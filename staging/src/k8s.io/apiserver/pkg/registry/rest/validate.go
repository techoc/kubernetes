/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package rest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	validationmetrics "k8s.io/apiserver/pkg/validation"
	"k8s.io/klog/v2"
)

// DeclarativeValidationStrategy defines how a strategy may opt-in to declarative validation.
//
// When strategies implements ValidateDeclaratively and handwritten validation (Validate / ValidateUpdate),
// the errors of both are merged and migration checks are performed.
type DeclarativeValidationStrategy interface {
	// ValidateDeclaratively runs declarative validation, merges the declarative validation errors with any
	// validationErrs returned from the strategy's Validate / ValidateUpdate functions (which implement hand-written validation)
	// and performs migration checks.
	ValidateDeclaratively(ctx context.Context, obj, oldObj runtime.Object, validationErrs field.ErrorList, opType operation.Type, config DeclarativeValidationConfig) field.ErrorList

	// DeclarativeValidationConfig configures declarative validation for a single request.
	DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) DeclarativeValidationConfig
}

// DeclarativeValidation is an implementation of DeclarativeValidationStrategy that
// provides a convenient way for a strategy to opt-in to declarative validation.
//
// 内嵌 DeclarativeValidation 即可让 strategy 自动获得声明式校验能力；
// 它只持有 *runtime.Scheme，所有方法都是值接收者，可安全地嵌入无状态 strategy。
//
// For example:
//
//		type podStrategy struct {
//		  rest.DeclarativeValidation
//		  names.NameGenerator
//		}
//	    var Strategy = podStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}
//
// Once a strategy opts-in this way, any generated declarative validation code is run automatically.
type DeclarativeValidation struct {
	*runtime.Scheme
}

// ValidateDeclaratively 把声明式校验错误与手写校验错误合并，
// 并执行迁移期的一致性检查（mismatch 日志与指标）。
// Scheme 为 nil 时返回内部错误而不是静默跳过，避免配置错误被掩盖。
func (d DeclarativeValidation) ValidateDeclaratively(ctx context.Context, obj, oldObj runtime.Object, validationErrs field.ErrorList, opType operation.Type, config DeclarativeValidationConfig) field.ErrorList {
	if d.Scheme == nil {
		validationErrs = append(validationErrs, field.InternalError(nil, fmt.Errorf("cannot validate declaratively without a scheme")))
		return validationErrs
	}
	return ValidateDeclarativelyWithMigrationChecks(ctx, d.Scheme, obj, oldObj, validationErrs, opType, config)
}

// DeclarativeValidationConfig 返回零值配置，即使用默认的选项、无路径归一化规则。
// 需要定制的 strategy 应覆写此方法。
func (d DeclarativeValidation) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) DeclarativeValidationConfig {
	// The zero value of DeclarativeValidationConfig is the default.
	return DeclarativeValidationConfig{}
}

// DeclarativeValidationConfig holds configuration for declarative validation.
// Strategies that need to customize declarative validation behavior implement
// DeclarativeValidationConfigurer and return this struct.
type DeclarativeValidationConfig struct {
	// Options contains the validation options that declarative validation tags expect,
	// mapping option name to whether it is enabled. Every option a resource's validation
	// tags reference must be defined here; an option that is not defined is reported as an
	// internal error rather than treated as disabled. Option names often correspond to
	// feature gates.
	Options map[string]bool

	// NormalizationRules are applied to field paths when comparing
	// handwritten and declarative validation errors.
	NormalizationRules []field.NormalizationRule

	// SubresourceGVKMapper maps a subresource request to the GVK of the
	// subresource type for polymorphic subresources like /scale.
	SubresourceGVKMapper GroupVersionKindProvider

	// ShortCircuitMismatch allows a short-circuit declarative validation error for a field
	// to match with any handwritten validation error on its subfields.
	ShortCircuitMismatch bool
}

// ValidationConfigOption 是 ValidateDeclarativelyWithMigrationChecks 使用的内部配置，
// 导出它仅为方便测试构造完整的选项集合。构造完整的选项集合。
type ValidationConfigOption struct {
	OpType               operation.Type
	ValidationIdentifier string
	DeclarativeValidationConfig
}

// validateDeclaratively validates obj and oldObj against declarative
// validation tags defined in its Go type. It uses the API version extracted from
// ctx and the provided scheme for validation.
//
// The ctx MUST contain requestInfo, which determines the target API for
// validation. The obj is converted to the API version using the provided scheme
// before validation occurs. The scheme MUST have the declarative validation
// registered for the requested resource/subresource.
//
// Returns a field.ErrorList containing any validation errors. An internal error
// is included if requestInfo is missing from the context or if version
// conversion fails.
//
// 必须先转换到请求所用的版本化类型：声明式校验 tag 由版本化类型生成，
// 直接校验内部类型会漏掉或误判 tag。
func validateDeclaratively(ctx context.Context, scheme *runtime.Scheme, obj, oldObj runtime.Object, o *ValidationConfigOption) field.ErrorList {
	// Find versionedGroupVersion, which identifies the API version to use for declarative validation.
	versionedGroupVersion, subresources, err := requestInfo(ctx, o.SubresourceGVKMapper)
	if err != nil {
		return field.ErrorList{field.InternalError(nil, err)}
	}
	versionedObj, err := scheme.ConvertToVersion(obj, versionedGroupVersion)
	if err != nil {
		return field.ErrorList{field.InternalError(nil, fmt.Errorf("unexpected error converting to versioned type: %w", err))}
	}
	var versionedOldObj runtime.Object

	switch o.OpType {
	case operation.Create:
		return scheme.Validate(ctx, o.Options, versionedObj, subresources...)
	case operation.Update:
		// 更新时需要同时转换旧对象，否则带 oldSelf 的 tag 无法校验。
		versionedOldObj, err = scheme.ConvertToVersion(oldObj, versionedGroupVersion)
		if err != nil {
			return field.ErrorList{field.InternalError(nil, fmt.Errorf("unexpected error converting to versioned type: %w", err))}
		}
		return scheme.ValidateUpdate(ctx, o.Options, versionedObj, versionedOldObj, subresources...)
	default:
		// 声明式校验目前只支持 create/update；其它操作属于内部调用错误。
		return field.ErrorList{field.InternalError(nil, fmt.Errorf("unknown operation type: %v", o.OpType))}
	}
}

// requestInfo 从上下文中取出请求的 group/version 与子资源路径；
// 子资源可能被 SubresourceGVKMapper 映射到不同的 GVK（如 /scale）。
func requestInfo(ctx context.Context, subresourceMapper GroupVersionKindProvider) (schema.GroupVersion, []string, error) {
	requestInfo, found := genericapirequest.RequestInfoFrom(ctx)
	if !found {
		return schema.GroupVersion{}, nil, fmt.Errorf("could not find requestInfo in context")
	}
	groupVersion := schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
	// 多态子资源（例如 scale）的 schema 属于另一个 GVK，需先做映射。
	if subresourceMapper != nil {
		groupVersion = subresourceMapper.GroupVersionKind(groupVersion).GroupVersion()
	}
	subresources, err := parseSubresourcePath(requestInfo.Subresource)
	if err != nil {
		return schema.GroupVersion{}, nil, fmt.Errorf("unexpected error parsing subresource path: %w", err)
	}
	return groupVersion, subresources, nil

}

// parseSubresourcePath 把 "status" 或 "a/b" 形式的子资源路径拆成段；
// 空段视为非法，防止出现空字符串子资源导致下游匹配歧义。
func parseSubresourcePath(subresourcePath string) ([]string, error) {
	if len(subresourcePath) == 0 {
		return nil, nil
	}
	parts := strings.Split(subresourcePath, "/")
	for _, part := range parts {
		if len(part) == 0 {
			return nil, fmt.Errorf("invalid subresource path: %s", subresourcePath)
		}
	}
	return parts, nil
}

// compareDeclarativeErrorsAndEmitMismatches 检查手写（imperative）校验与声明式校验之间的
// 不一致，并在发现差异时记录日志、上报指标。
// 每个不一致单独记日志与计数，便于按错误类型定位声明式校验实现的缺陷。
func compareDeclarativeErrorsAndEmitMismatches(ctx context.Context, imperativeErrs, declarativeErrs field.ErrorList, validationIdentifier string, enforced bool, opts ValidationConfigOption) {
	logger := klog.FromContext(ctx)
	mismatchDetails := gatherDeclarativeValidationMismatches(imperativeErrs, declarativeErrs, enforced, opts)
	for _, detail := range mismatchDetails {
		// Log information about the mismatch using contextual logger
		logger.Error(nil, detail)

		// Increment the metric for the mismatch
		validationmetrics.Metrics.IncDeclarativeValidationMismatchMetric(validationIdentifier)
	}
}

// gatherDeclarativeValidationMismatches compares imperative and declarative validation errors
// and returns detailed information about any mismatches found. Errors are compared via type, field, and origin
//
// 只比较被声明式校验“覆盖”（CoveredByDeclarative）的手写错误：新 API 的标准
// 声明式错误可能根本没有手写对应物，不该被当成缺失。
func gatherDeclarativeValidationMismatches(imperativeErrs, declarativeErrs field.ErrorList, enforced bool, opts ValidationConfigOption) []string {
	var mismatchDetails []string
	// short circuit here to minimize allocs for usual case of 0 validation errors
	if len(imperativeErrs) == 0 && len(declarativeErrs) == 0 {
		return mismatchDetails
	}
	// 根据是否强制执行给出默认建议。
	// 已强制时声明式结果会真正生效，因此建议关闭 beta 特性门以保持一致；
	// 未强制时手写校验仍是权威，差异不影响运行。
	const (
		authoritativeMsg = "This difference should not affect system operation since hand written validation is authoritative."
		disableBetaMsg   = "Consider disabling the DeclarativeValidationBeta feature gate to keep data persisted in etcd consistent with prior versions of Kubernetes."
	)

	defaultRecommendation := authoritativeMsg
	if enforced {
		defaultRecommendation = disableBetaMsg
	}

	fuzzyMatcher := field.ErrorMatcher{}.ByType().ByOrigin().RequireOriginWhenInvalid().ByFieldNormalized(opts.NormalizationRules)
	// 短路匹配用于声明式校验在父字段处提前返回、而手写校验逐个报告子字段的情况。
	fuzzyMatcherWithShortCircuit := fuzzyMatcher.MatchAncestorShortCircuit()

	// Dedupe imperative errors using the fuzzy matcher (type, field, and origin) as they are
	// not intended and come from (buggy) duplicate validation calls.
	// This is necessary as without deduping we could get unmatched
	// imperative errors for cases that are correct (matching).
	dedupedImperativeErrs := field.ErrorList{}
	for _, err := range imperativeErrs {
		found := false
		for _, existingErr := range dedupedImperativeErrs {
			if fuzzyMatcher.Matches(existingErr, err) {
				found = true
				break
			}
		}
		if !found {
			dedupedImperativeErrs = append(dedupedImperativeErrs, err)
		}
	}
	imperativeErrs = dedupedImperativeErrs

	matchedDeclarative := make([]bool, len(declarativeErrs))

	// Match each "covered" imperative error to declarative errors.
	// We use a fuzzy matching approach to find corresponding declarative errors
	// for each imperative error marked as CoveredByDeclarative.
	// They are matched with a "many:many" mapping: an imperative error can match multiple
	// declarative errors, and a declarative error can match multiple imperative errors.
	// This allows us to:
	// 1. Detect imperative errors that should have matching declarative errors but don't
	// 2. Identify extra declarative errors with no imperative counterpart
	// Both cases indicate issues with the declarative validation implementation.
	for _, iErr := range imperativeErrs {
		// 未标记为被声明式覆盖的错误不参与比较：它们本来就只由手写校验产生。
		if !iErr.CoveredByDeclarative {
			continue
		}

		matchCount := 0

		for dIdx, dErr := range declarativeErrs {
			if fuzzyMatcher.Matches(iErr, dErr) {
				matchCount++
				matchedDeclarative[dIdx] = true
			}
		}
		// see if the error matches with a short circuited DV error.
		if opts.ShortCircuitMismatch && matchCount == 0 {
			for _, dErr := range declarativeErrs {
				if fuzzyMatcherWithShortCircuit.Matches(iErr, dErr) {
					matchCount++
					break
				}
			}
		}

		if matchCount == 0 {
			rec := defaultRecommendation
			// If the imperative error is explicitly Alpha, it is never enforced, so HV is authoritative.
			if iErr.IsAlpha() {
				rec = authoritativeMsg
			}
			mismatchDetails = append(mismatchDetails,
				fmt.Sprintf(
					"Unexpected difference between hand written validation and declarative validation error results, unmatched error(s) found %s. "+
						"This indicates an issue with declarative validation. %s",
					fuzzyMatcher.Render(iErr),
					rec,
				),
			)
		}
	}

	// 其余未被匹配上的声明式错误都视为“多余错误”。
	// 多余的声明式错误同样意味着实现不一致：它会在强制后改变用户可见的校验结果。
	for dIdx, dErr := range declarativeErrs {
		if matchedDeclarative[dIdx] {
			continue
		}
		rec := defaultRecommendation
		// If the declarative error is Alpha, it is never enforced (shadowed), so HV is authoritative.
		if dErr.IsAlpha() {
			rec = authoritativeMsg
		}

		mismatchDetails = append(mismatchDetails,
			fmt.Sprintf(
				"Unexpected difference between hand written validation and declarative validation error results, extra error(s) found %s. "+
					"This indicates an issue with declarative validation. %s",
				fuzzyMatcher.Render(dErr),
				rec,
			),
		)
	}

	return mismatchDetails
}

// runDeclarativeValidationWithRecover 调用 validateDeclaratively，并带 panic 恢复。
// 发生 panic 时会累加 panic 指标，并在返回的错误列表中追加一个 InternalError。
// 声明式校验由生成代码执行，panic 不能拖垮整个请求；转为内部错误并计数以便发现缺陷。
func runDeclarativeValidationWithRecover(ctx context.Context, scheme *runtime.Scheme, obj, oldObj runtime.Object, o *ValidationConfigOption) (errs field.ErrorList) {
	defer func() {
		if r := recover(); r != nil {
			validationmetrics.Metrics.IncDeclarativeValidationPanicMetric(o.ValidationIdentifier)
			errs = append(errs, field.InternalError(nil, fmt.Errorf("panic during declarative validation: %v", r)))
		}
	}()
	return validateDeclaratively(ctx, scheme, obj, oldObj, o)
}

// metricIdentifier 构造稳定的指标标识：小写 Kind + 子资源 + 操作类型。
// 即使部分信息缺失也返回尽力而为的标识，调用方只需记录错误而不中断流程。
func metricIdentifier(ctx context.Context, scheme *runtime.Scheme, obj runtime.Object, opType operation.Type) (string, error) {
	var errs error
	var identifier string

	identifier = "unknown_resource"
	// Use kind for identifier.
	if obj != nil && scheme != nil {
		gvks, _, err := scheme.ObjectKinds(obj)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		if len(gvks) > 0 {
			identifier = strings.ToLower(gvks[0].Kind)
		}
	}

	// Use requestInfo for subresource.
	requestInfo, found := genericapirequest.RequestInfoFrom(ctx)
	if !found {
		errs = errors.Join(errs, fmt.Errorf("could not find requestInfo in context"))
	} else if len(requestInfo.Subresource) > 0 {
		// subresource can be a path, so replace '/' with '_'
		identifier += "_" + strings.ReplaceAll(requestInfo.Subresource, "/", "_")
	}

	switch opType {
	case operation.Create:
		identifier += "_create"
	case operation.Update:
		identifier += "_update"
	default:
		errs = errors.Join(errs, fmt.Errorf("unknown operation type: %v", opType))
		identifier += "_unknown_op"
	}
	return identifier, errs
}

// ValidateDeclarativelyWithMigrationChecks executes declarative validation and implements the Validation Lifecycle strategy.
// Declarative validation is always authoritative; the lifecycle prefix on each tag controls the visible behavior:
//   - Standard (no prefix): Enforced. HV counterparts are expected to be deleted from source.
//   - Beta (+k8s:beta): Enforced when DeclarativeValidationBeta is enabled. Otherwise shadowed (HV remains authoritative).
//   - Alpha (+k8s:alpha): Always shadowed; HV remains authoritative.
//
// Mismatches between HV and DV are logged when the DeclarativeValidation gate is enabled. Only Alpha and
// Beta errors are mismatch-checked, since Standard DV errors may have no HV counterpart in new APIs.
// WithAllDeclarativeEnforcedForTest returns a copy of parent context with allDeclarativeEnforcedKey set to true.
// This is used for testing to expose all declarative validation errors and filter all handwritten validation errors
// that are covered by declarative validation, regardless of the feature gate or maturity level.
//
// NOTE: This function is intended for testing purposes only and should not be used in production code.
//
// WithAllDeclarativeEnforcedForTest 在上下文里打上“全部强制”标记，仅用于测试：
// 让所有声明式错误可见并过滤掉被覆盖的手写错误，与特性门和成熟度无关。
func WithAllDeclarativeEnforcedForTest(ctx context.Context) context.Context {
	return validate.WithAllDeclarativeEnforcedForTest(ctx)
}

// ValidateDeclarativelyWithMigrationChecks executes declarative validation and implements the Validation Lifecycle strategy.
// Declarative validation is always authoritative; the lifecycle prefix on each tag controls the visible behavior:
//   - Standard (no prefix): Enforced. HV counterparts are expected to be deleted from source.
//   - Beta (+k8s:beta): Enforced when DeclarativeValidationBeta is enabled. Otherwise shadowed (HV remains authoritative).
//   - Alpha (+k8s:alpha): Always shadowed; HV remains authoritative.
//
// Mismatches between HV and DV are logged when the DeclarativeValidation gate is enabled. Only Alpha and
// Beta errors are mismatch-checked, since Standard DV errors may have no HV counterpart in new APIs.
//
// For testing purposes, WithAllDeclarativeEnforcedForTest enforces all declarative validations regardless
// of lifecycle and filters all covered handwritten validations.
//
// 执行顺序很关键：先做 mismatch 比较（使用未过滤的两侧错误），再按生命周期
// 过滤出真正生效的声明式错误，最后去掉被其覆盖的手写错误并追加声明式错误。
// 这样用户只会看到每个字段的一条权威错误，而指标仍能反映两侧差异。
func ValidateDeclarativelyWithMigrationChecks(ctx context.Context, scheme *runtime.Scheme, obj, oldObj runtime.Object, errs field.ErrorList, opType operation.Type, config DeclarativeValidationConfig) field.ErrorList {
	// 这里的 errs 必须来自手写校验返回的错误。标记来源后，后续匹配与过滤才能区分手写错误与声明式错误。
	errs = errs.MarkFromImperative()
	validationIdentifier, err := metricIdentifier(ctx, scheme, obj, opType)
	if err != nil {
		// Log the error, but continue with the best-effort identifier.
		klog.FromContext(ctx).Error(err, "failed to generate complete validation identifier for declarative validation")
	}

	cfg := &ValidationConfigOption{
		OpType:                      opType,
		ValidationIdentifier:        validationIdentifier,
		DeclarativeValidationConfig: config,
	}

	declarativeErrs := runDeclarativeValidationWithRecover(ctx, scheme, obj, oldObj, cfg)

	// betaEnabled 同时决定 Beta 错误是否生效，以及 mismatch 建议文案。
	betaEnabled := utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta)
	if utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidation) {
		// Standard errors are authoritative and may not have handwritten counterparts (e.g., in new APIs).
		// Only Alpha and Beta errors are eligible for mismatch checking.
		var mismatchCandidateErrs field.ErrorList
		for _, err := range declarativeErrs {
			if err.IsAlpha() || err.IsBeta() {
				mismatchCandidateErrs = append(mismatchCandidateErrs, err)
			}
		}
		compareDeclarativeErrorsAndEmitMismatches(ctx, errs, mismatchCandidateErrs, validationIdentifier, betaEnabled, *cfg)
	}

	// Collect the declarative errors that are enforced (i.e. surfaced to the user) in the current mode.
	enforcedDeclarativeErrs := validate.FilterEnforcedDeclarativeErrors(ctx, declarativeErrs, betaEnabled)
	// Remove handwritten errors that are superseded by an enforced declarative counterpart.
	errs = validate.FilterCoveredHandwrittenErrors(ctx, errs, enforcedDeclarativeErrs, betaEnabled, cfg.NormalizationRules...)

	// Append the enforced declarative errors.
	errs = append(errs, enforcedDeclarativeErrs...)

	return errs
}

// RecordDuplicateValidationErrors 在发现重复的校验错误时累加指标并记录日志。
// 重复错误通常来自重复调用校验函数，属于实现缺陷；只记录与计数，不影响返回结果。
func RecordDuplicateValidationErrors(ctx context.Context, qualifiedKind schema.GroupKind, errs field.ErrorList) {
	logger := klog.FromContext(ctx)
	seenErrs := make([]string, 0, len(errs))

	for _, err := range errs {
		errStr := fmt.Sprintf("%v", err)

		if slices.Contains(seenErrs, errStr) {
			logger.Info("Found duplicate validation error", "kind", qualifiedKind.String(), "error", errStr)
			validationmetrics.Metrics.IncDuplicateValidationErrorMetric()
		} else {
			seenErrs = append(seenErrs, errStr)
		}
	}
}
