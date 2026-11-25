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

// DeclarativeValidationStrategy 定义 strategy 可以如何启用声明式校验。
//
// 当 strategy 同时实现 ValidateDeclaratively 和手写校验（Validate / ValidateUpdate）时，
// 二者的错误会被合并，并执行迁移期检查。
type DeclarativeValidationStrategy interface {
	// ValidateDeclaratively 运行声明式校验，把声明式校验错误与 strategy 的
	// Validate / ValidateUpdate 函数（实现手写校验）返回的 validationErrs 合并，
	// 并执行迁移期检查。
	ValidateDeclaratively(ctx context.Context, obj, oldObj runtime.Object, validationErrs field.ErrorList, opType operation.Type, config DeclarativeValidationConfig) field.ErrorList

	// DeclarativeValidationConfig 为单个请求配置声明式校验。
	DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) DeclarativeValidationConfig
}

// DeclarativeValidation 是 DeclarativeValidationStrategy 的一种实现，
// 为 strategy 提供了启用声明式校验的便捷方式。
//
// 内嵌 DeclarativeValidation 即可让 strategy 自动获得声明式校验能力；
// 它只持有 *runtime.Scheme，所有方法都是值接收者，可安全地嵌入无状态 strategy。
//
// 例如：
//
//		type podStrategy struct {
//		  rest.DeclarativeValidation
//		  names.NameGenerator
//		}
//	    var Strategy = podStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}
//
// strategy 以此方式启用后，任何生成的声明式校验代码都会自动运行。
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
	// DeclarativeValidationConfig 的零值即为默认配置。
	return DeclarativeValidationConfig{}
}

// DeclarativeValidationConfig 保存声明式校验的配置。
// 需要定制声明式校验行为的 strategy 应实现 DeclarativeValidationConfigurer
// 并返回该结构体。
type DeclarativeValidationConfig struct {
	// Options 包含声明式校验 tag 所期望的校验选项，
	// 从选项名映射到其是否启用。资源校验 tag 引用的每个选项都必须在此定义；
	// 未定义的选项会报告为内部错误，而不是被视为关闭。选项名通常对应特性门。
	Options map[string]bool

	// NormalizationRules 在比较手写校验错误与声明式校验错误时
	// 应用于字段路径。
	NormalizationRules []field.NormalizationRule

	// SubresourceGVKMapper 针对 /scale 这类多态子资源，把子资源请求
	// 映射到该子资源类型的 GVK。
	SubresourceGVKMapper GroupVersionKindProvider

	// ShortCircuitMismatch 允许某字段的短路声明式校验错误与其
	// 子字段上的任意手写校验错误相匹配。
	ShortCircuitMismatch bool
}

// ValidationConfigOption 是 ValidateDeclarativelyWithMigrationChecks 使用的内部配置，
// 导出它仅为方便测试构造完整的选项集合。
type ValidationConfigOption struct {
	OpType               operation.Type
	ValidationIdentifier string
	DeclarativeValidationConfig
}

// validateDeclaratively 依据 obj 与 oldObj 的 Go 类型上定义的声明式校验 tag
// 进行校验。它使用从 ctx 提取的 API 版本和给定的 scheme 完成校验。
//
// ctx 必须包含 requestInfo，它决定校验的目标 API。
// 校验发生前，obj 会先用给定的 scheme 转换为对应的 API 版本。
// scheme 必须已为请求的资源/子资源注册声明式校验。
//
// 返回包含所有校验错误的 field.ErrorList。若上下文中缺少 requestInfo
// 或版本转换失败，其中会包含一个内部错误。
//
// 必须先转换到请求所用的版本化类型：声明式校验 tag 由版本化类型生成，
// 直接校验内部类型会漏掉或误判 tag。
func validateDeclaratively(ctx context.Context, scheme *runtime.Scheme, obj, oldObj runtime.Object, o *ValidationConfigOption) field.ErrorList {
	// 找出 versionedGroupVersion，它标识声明式校验要使用的 API 版本。
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
		// 使用上下文 logger 记录该 mismatch 的信息
		logger.Error(nil, detail)

		// 累加 mismatch 对应的指标
		validationmetrics.Metrics.IncDeclarativeValidationMismatchMetric(validationIdentifier)
	}
}

// gatherDeclarativeValidationMismatches 比较手写（imperative）校验错误与声明式校验错误，
// 并返回所发现差异的详细信息。错误通过类型、字段和 origin 进行比较。
//
// 只比较被声明式校验“覆盖”（CoveredByDeclarative）的手写错误：新 API 的标准
// 声明式错误可能根本没有手写对应物，不该被当成缺失。
func gatherDeclarativeValidationMismatches(imperativeErrs, declarativeErrs field.ErrorList, enforced bool, opts ValidationConfigOption) []string {
	var mismatchDetails []string
	// 此处短路返回，以在常见的 0 个校验错误场景下最小化内存分配
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

	// 使用模糊匹配器（类型、字段和 origin）对手写校验错误去重，因为它们并非有意产生，
	// 而是来自（有缺陷的）重复校验调用。
	// 这一步是必要的：若不先去重，本应正确（匹配）的情形可能会出现
	// 未匹配上的手写校验错误。
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

	// 把每个被“覆盖”的手写校验错误与声明式校验错误相匹配。
	// 我们采用模糊匹配的方式，为每个标记为 CoveredByDeclarative 的手写校验错误
	// 找到对应的声明式校验错误。
	// 二者以“多对多”映射匹配：一个手写校验错误可以匹配多个声明式校验错误，
	// 一个声明式校验错误也可以匹配多个手写校验错误。
	// 这使我们能够：
	// 1. 检测本应有对应声明式校验错误却没有的错误
	// 2. 识别没有手写校验对应物的多余声明式错误
	// 这两种情况都表明声明式校验实现存在问题。
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
		// 查看该错误能否与某个短路（short-circuit）的声明式校验错误匹配。
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
			// 若手写校验错误显式标记为 Alpha，则永远不会被强制执行，因此手写校验（HV）是权威。
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
		// 若声明式校验错误是 Alpha，则永远不会被强制执行（被遮蔽），因此手写校验（HV）是权威。
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
	// 用 Kind 构造标识符。
	if obj != nil && scheme != nil {
		gvks, _, err := scheme.ObjectKinds(obj)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		if len(gvks) > 0 {
			identifier = strings.ToLower(gvks[0].Kind)
		}
	}

	// 用 requestInfo 获取子资源。
	requestInfo, found := genericapirequest.RequestInfoFrom(ctx)
	if !found {
		errs = errors.Join(errs, fmt.Errorf("could not find requestInfo in context"))
	} else if len(requestInfo.Subresource) > 0 {
		// 子资源可能是一个路径，因此把 '/' 替换为 '_'
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

// ValidateDeclarativelyWithMigrationChecks 执行声明式校验并实现 Validation Lifecycle（校验生命周期）策略。
// 声明式校验始终是权威的；每个 tag 上的生命周期前缀控制可见行为：
//   - Standard（无前缀）：强制执行。对应的手写校验（HV）错误应当已从源码中删除。
//   - Beta（+k8s:beta）：启用 DeclarativeValidationBeta 时强制执行；否则被遮蔽（HV 仍是权威）。
//   - Alpha（+k8s:alpha）：始终被遮蔽；HV 仍是权威。
//
// 启用 DeclarativeValidation 特性门时，会记录 HV 与 DV 之间的 mismatch。仅对 Alpha 和
// Beta 错误做 mismatch 检查，因为新 API 中 Standard 的 DV 错误可能没有 HV 对应物。
//
// WithAllDeclarativeEnforcedForTest 返回父上下文的副本，并把 allDeclarativeEnforcedKey 置为 true，
// 仅用于测试：让所有声明式校验错误可见，并过滤掉所有被声明式校验覆盖的手写校验错误，
// 与特性门或成熟度级别无关。
//
// NOTE: 本函数仅供测试目的使用，不应当用于生产代码。
func WithAllDeclarativeEnforcedForTest(ctx context.Context) context.Context {
	return validate.WithAllDeclarativeEnforcedForTest(ctx)
}

// ValidateDeclarativelyWithMigrationChecks 执行声明式校验并实现 Validation Lifecycle（校验生命周期）策略。
// 声明式校验始终是权威的；每个 tag 上的生命周期前缀控制可见行为：
//   - Standard（无前缀）：强制执行。对应的手写校验（HV）错误应当已从源码中删除。
//   - Beta（+k8s:beta）：启用 DeclarativeValidationBeta 时强制执行；否则被遮蔽（HV 仍是权威）。
//   - Alpha（+k8s:alpha）：始终被遮蔽；HV 仍是权威。
//
// 启用 DeclarativeValidation 特性门时，会记录 HV 与 DV 之间的 mismatch。仅对 Alpha 和
// Beta 错误做 mismatch 检查，因为新 API 中 Standard 的 DV 错误可能没有 HV 对应物。
//
// 测试场景下，WithAllDeclarativeEnforcedForTest 会无视生命周期强制全部声明式校验，
// 并过滤掉全部被覆盖的手写校验。
//
// 执行顺序很关键：先做 mismatch 比较（使用未过滤的两侧错误），再按生命周期
// 过滤出真正生效的声明式错误，最后去掉被其覆盖的手写错误并追加声明式错误。
// 这样用户只会看到每个字段的一条权威错误，而指标仍能反映两侧差异。
func ValidateDeclarativelyWithMigrationChecks(ctx context.Context, scheme *runtime.Scheme, obj, oldObj runtime.Object, errs field.ErrorList, opType operation.Type, config DeclarativeValidationConfig) field.ErrorList {
	// 这里的 errs 必须来自手写校验返回的错误。标记来源后，后续匹配与过滤才能区分手写错误与声明式错误。
	errs = errs.MarkFromImperative()
	validationIdentifier, err := metricIdentifier(ctx, scheme, obj, opType)
	if err != nil {
		// 记录该错误，但继续使用尽力而为的标识符。
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
		// Standard 错误是权威的，可能没有手写对应物（例如新 API）。
		// 只有 Alpha 和 Beta 错误才符合 mismatch 检查的条件。
		var mismatchCandidateErrs field.ErrorList
		for _, err := range declarativeErrs {
			if err.IsAlpha() || err.IsBeta() {
				mismatchCandidateErrs = append(mismatchCandidateErrs, err)
			}
		}
		compareDeclarativeErrorsAndEmitMismatches(ctx, errs, mismatchCandidateErrs, validationIdentifier, betaEnabled, *cfg)
	}

	// 收集当前模式下会被强制执行（即呈现给用户）的声明式校验错误。
	enforcedDeclarativeErrs := validate.FilterEnforcedDeclarativeErrors(ctx, declarativeErrs, betaEnabled)
	// 移除已被某个生效的声明式校验对应物取代的手写校验错误。
	errs = validate.FilterCoveredHandwrittenErrors(ctx, errs, enforcedDeclarativeErrs, betaEnabled, cfg.NormalizationRules...)

	// 追加生效的声明式校验错误。
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
