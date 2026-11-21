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

package testing

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimetest "k8s.io/apimachinery/pkg/runtime/testing"
	"k8s.io/apimachinery/pkg/test/coverage"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	validationmetrics "k8s.io/apiserver/pkg/validation"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"sigs.k8s.io/randfill"
)

// skippedEquivalenceGroupVersions 是显式退出声明式校验（+k8s:validation-gen=false）
// 但又与其他版本共享同一 internal 类型的版本集合。
// 只应放入有意为之的退出项；其他缺失声明式校验的版本必须让扫描失败，而不是被跳过。
var skippedEquivalenceGroupVersions = sets.New("extensions/v1beta1", "events.k8s.io/v1beta1")

// VerifyVersionedValidationEquivalence 测试同一 API 的所有版本返回的校验错误是否等价。
// 可选配置用于处理不同版本结构差异带来的字段路径归一化。
func VerifyVersionedValidationEquivalence(t *testing.T, obj, old runtime.Object, testConfigs ...ValidationTestConfig) {
	t.Helper()

	opts := &validationOption{}
	for _, testcfg := range testConfigs {
		testcfg(opts)
	}

	// 按版本分别收集各版本校验产生的错误。
	all := map[string]field.ErrorList{}
	accumulate := func(t *testing.T, gv string, errs field.ErrorList) {
		// 跳过显式退出等价性扫描的版本（见 skippedEquivalenceGroupVersions）。
		if skippedEquivalenceGroupVersions.Has(gv) {
			return
		}
		// 若配置了归一化规则，先对错误中的字段路径做替换。
		// 这样即使版本间结构不同（例如扁平字段 vs 嵌套字段），也能比较错误。
		// 必须在排序前原地归一化。
		for i := range errs {
			currentPath := errs[i].Field
			for _, rule := range opts.NormalizationRules {
				normalized := rule.Regexp.ReplaceAllString(currentPath, rule.Replacement)
				if normalized != currentPath {
					errs[i].Field = normalized
					// 每个错误只应用第一条匹配规则。
					break
				}
			}
		}
		// 按归一化后的字段路径重新排序，保证逐项比较时错误能够对齐，
		// 不受原始结构差异影响。
		sort.Slice(errs, func(i, j int) bool {
			if errs[i].Field != errs[j].Field {
				return errs[i].Field < errs[j].Field
			}
			// 字段相同时再按完整错误字符串排序，保证结果确定。
			return errs[i].Error() < errs[j].Error()
		})
		all[gv] = errs
	}
	// runtimetest.RunValidationForEachVersion 要求输入是无版本的 internal 对象，
	// 所以先把带版本的入参转换到 internal。
	internalObj, err := convertToInternal(t, legacyscheme.Scheme, obj)
	if err != nil {
		t.Fatal(err)
	}
	if internalObj == nil {
		return
	}
	// fuzz 只针对 internal 版本：自定义 fuzz 函数只支持 internal 对象。
	if opts.Fuzzer != nil {
		opts.Fuzzer.Fill(internalObj)
	}
	if old == nil {
		// 创建场景：遍历所有版本执行校验，结果交给 accumulate 收集。
		runtimetest.RunValidationForEachVersion(t, legacyscheme.Scheme, opts.Options, internalObj, accumulate, opts.IgnoreObjectConversionErrors, opts.SubResources...)
	} else {
		// 更新场景：old 对象同样要先转成 internal。
		// runtimetest.RunUpdateValidationForEachVersion 也要求无版本输入。
		internalOld, err := convertToInternal(t, legacyscheme.Scheme, old)
		if err != nil {
			t.Fatal(err)
		}
		if internalOld == nil {
			return
		}
		if opts.Fuzzer != nil {
			opts.Fuzzer.Fill(internalOld)
		}
		runtimetest.RunUpdateValidationForEachVersion(t, legacyscheme.Scheme, opts.Options, internalObj, internalOld, accumulate, opts.IgnoreObjectConversionErrors, opts.SubResources...)
	}

	// 复制一份 map，用于两两比较时删除已比较的版本。
	other := map[string]field.ErrorList{}
	// 收集所有 key 并排序，让比较顺序确定、输出稳定。
	keys := []string{}
	for k, v := range all {
		other[k] = v
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// 两两比较各版本的错误列表。
	for _, lk := range keys {
		lv := all[lk]
		// 从 other 中删除当前版本，既避免自比较，也保证每对版本只比较一次：
		// [1,2,3] 会得到 1vs2、1vs3、2vs3。
		delete(other, lk)
		for _, rk := range keys {
			rv, found := other[rk]
			if !found {
				continue // 这一对已经比较过了
			}
			if len(lv) != len(rv) {
				t.Errorf("different error count (%d vs. %d)\n%s: %v\n%s: %v", len(lv), len(rv), lk, fmtErrs(lv), rk, fmtErrs(rv))
				continue
			}
			next := false
			for i := range lv {
				// 这里不用 reflect.DeepEqual：有版本与无版本校验的 BadValue
				// 可能是不同类型（如指针 vs 值）。
				// 也不用 ErrorMatcher：在此循环结构中，多错误场景下归一化后
				// 需要重新排序，它处理不了。
				l, r := lv[i], rv[i]
				// 比较字段（已归一化）、错误类型、详情和来源。
				if l.Type != r.Type || l.Field != r.Field || l.Detail != r.Detail || l.Origin != r.Origin {
					t.Errorf("different errors at index %d\n%s: %v\n%s: %v", i, lk, l.Error(), rk, r.Error())
					next = true
				}
			}
			if next {
				// 发现差异时打印完整错误列表，方便排查。
				t.Errorf("complete error lists for context:\n%s: %v\n%s: %v", lk, fmtErrs(lv), rk, fmtErrs(rv))
				continue
			}
		}
	}
}

// fmtErrs 把错误列表格式化成便于阅读的字符串。
func fmtErrs(errs field.ErrorList) string {
	if len(errs) == 0 {
		return "<no errors>"
	}
	if len(errs) == 1 {
		return strconv.Quote(errs[0].Error())
	}
	buf := bytes.Buffer{}
	for _, e := range errs {
		buf.WriteString("\n\t")
		buf.WriteString(strconv.Quote(e.Error()))
	}

	return buf.String()
}

// convertToInternal 把带版本的 obj 转换为 internal 版本。
// 对没有对应 internal 类型的 GroupKind 返回 (nil, nil)，调用方据此跳过。
func convertToInternal(t *testing.T, scheme *runtime.Scheme, obj runtime.Object) (runtime.Object, error) {
	t.Helper()

	gvks, _, err := scheme.ObjectKinds(obj)
	if err != nil {
		t.Fatal(err)
	}
	if len(gvks) == 0 {
		t.Fatal("no GVKs found for object")
	}
	gvk := gvks[0]
	// 已经是 internal 对象则原样返回。
	if gvk.Version == runtime.APIVersionInternal {
		return obj, nil
	}
	gvk.Version = runtime.APIVersionInternal
	if !scheme.Recognizes(gvk) {
		t.Logf("no internal object found for GroupKind %s", gvk.GroupKind().String())
		return nil, nil
	}
	return scheme.ConvertToVersion(obj, schema.GroupVersion{Group: gvk.Group, Version: runtime.APIVersionInternal})
}

// ValidationTestConfig 是等价性测试的可选配置函数类型。
type ValidationTestConfig func(*validationOption)

// validationOption 封装等价性测试的可选参数。
type validationOption struct {
	// SubResources 是需要校验的子资源列表。
	SubResources []string
	// NormalizationRules 是比较前对字段路径应用的归一化规则。
	NormalizationRules []field.NormalizationRule

	// IgnoreObjectConversionErrors 控制在 internal → 带版本对象转换失败时是否跳过测试。
	IgnoreObjectConversionErrors bool

	// Fuzzer 用于生成测试对象。
	Fuzzer *randfill.Filler

	// Options 是校验时使用的选项。
	Options map[string]bool
}

// WithOptions 设置校验选项。
func WithOptions(options map[string]bool) ValidationTestConfig {
	return func(o *validationOption) {
		o.Options = options
	}
}

// WithSubResources 设置要校验的子资源。
func WithSubResources(subResources ...string) ValidationTestConfig {
	return func(o *validationOption) {
		o.SubResources = subResources
	}
}

// WithNormalizationRules 设置字段路径归一化规则。
func WithNormalizationRules(rules ...field.NormalizationRule) ValidationTestConfig {
	return func(o *validationOption) {
		o.NormalizationRules = rules
	}
}

// WithIgnoreObjectConversionErrors 开启“对象转换失败时跳过测试”。
func WithIgnoreObjectConversionErrors() ValidationTestConfig {
	return func(o *validationOption) {
		o.IgnoreObjectConversionErrors = true
	}
}

// WithFuzzer 设置用于生成测试对象的 fuzzer。
func WithFuzzer(fuzzer *randfill.Filler) ValidationTestConfig {
	return func(o *validationOption) {
		o.Fuzzer = fuzzer
	}
}

// VerifyValidationEquivalence 用于测试从手写命令式校验迁移到声明式校验的过程，
// 保证不同执行模式下的校验逻辑一致。
//
// 该函数在三种场景下执行给定的校验逻辑：
//  1. DeclarativeValidation 与 DeclarativeValidationBeta 都开启，使用 Beta 阶段的声明式规则；
//  2. DeclarativeValidation 开启、DeclarativeValidationBeta 关闭，使用 Standard 阶段的声明式规则；
//  3. 强制所有声明式规则（含 Alpha）生效，确保完整的声明式校验都被正确实现（仅测试用）。
//
// 每种场景都会把错误与期望集合比对：按字段、来源和类型比较，三者都一致才算等价。
// 此外还会验证给定 API 的所有版本返回等价错误。
func VerifyValidationEquivalence(t *testing.T, ctx context.Context, obj runtime.Object, strategy rest.RESTCreateStrategy, expectedErrs field.ErrorList, testConfigs ...ValidationTestConfig) {
	t.Helper()
	opts := &validationOption{}
	for _, testcfg := range testConfigs {
		testcfg(opts)
	}

	// 把“调用 strategy 校验”包装成闭包，供三种场景复用。
	verifyValidationEquivalence(t, expectedErrs, func(c context.Context) field.ErrorList {
		return rest.ValidateCreate(c, obj, strategy)
	}, ctx, opts, obj)
	// 跨版本检查默认沿用 strategy 自己声明的 options，除非调用方显式提供。
	if opts.Options == nil {
		if dvs, ok := strategy.(rest.DeclarativeValidationStrategy); ok {
			testConfigs = append(testConfigs, WithOptions(dvs.DeclarativeValidationConfig(ctx, obj, nil).Options))
		}
	}
	VerifyVersionedValidationEquivalence(t, obj, nil, testConfigs...)
}

// VerifyUpdateValidationEquivalence 是 VerifyValidationEquivalence 的更新操作版本，
// 同样用于测试手写校验到声明式校验的迁移，保证各执行模式下逻辑一致。
//
// 三种场景与 VerifyValidationEquivalence 相同：
//  1. Beta 开关开启，使用 Beta 阶段声明式规则；
//  2. Beta 开关关闭，使用 Standard 阶段声明式规则；
//  3. 强制所有声明式规则（含 Alpha）生效（仅测试用）。
//
// 按字段、来源和类型比较错误，并验证所有 API 版本返回等价错误。
func VerifyUpdateValidationEquivalence(t *testing.T, ctx context.Context, obj, old runtime.Object, strategy rest.RESTUpdateStrategy, expectedErrs field.ErrorList, testConfigs ...ValidationTestConfig) {
	t.Helper()
	opts := &validationOption{}
	for _, testcfg := range testConfigs {
		testcfg(opts)
	}

	verifyValidationEquivalence(t, expectedErrs, func(c context.Context) field.ErrorList {
		return rest.ValidateUpdate(c, obj, old, strategy)
	}, ctx, opts, obj)
	// 跨版本检查默认沿用 strategy 声明的 options。
	if opts.Options == nil {
		if dvs, ok := strategy.(rest.DeclarativeValidationStrategy); ok {
			testConfigs = append(testConfigs, WithOptions(dvs.DeclarativeValidationConfig(ctx, obj, old).Options))
		}
	}
	VerifyVersionedValidationEquivalence(t, obj, old, testConfigs...)
}

// VerifyValidationEquivalenceFunc 是 VerifyValidationEquivalence 的变体：
// 调用方不通过 RESTCreateStrategy，而是直接提供生成手写与声明式校验错误的闭包。
// validate 闭包针对 (ctx, obj) 返回合并后的 ErrorList。
func VerifyValidationEquivalenceFunc(t *testing.T, ctx context.Context, obj runtime.Object, validate func(ctx context.Context, obj runtime.Object) field.ErrorList, expectedErrs field.ErrorList, testConfigs ...ValidationTestConfig) {
	t.Helper()
	opts := &validationOption{}
	for _, testcfg := range testConfigs {
		testcfg(opts)
	}

	verifyValidationEquivalence(t, expectedErrs, func(c context.Context) field.ErrorList {
		return validate(c, obj)
	}, ctx, opts, obj)
	VerifyVersionedValidationEquivalence(t, obj, nil, testConfigs...)
}

// VerifyUpdateValidationEquivalenceFunc 是 VerifyUpdateValidationEquivalence 的变体：
// 调用方直接提供针对 (ctx, obj, old) 返回合并 ErrorList 的闭包。
func VerifyUpdateValidationEquivalenceFunc(t *testing.T, ctx context.Context, obj, old runtime.Object, validate func(ctx context.Context, obj, old runtime.Object) field.ErrorList, expectedErrs field.ErrorList, testConfigs ...ValidationTestConfig) {
	t.Helper()
	opts := &validationOption{}
	for _, testcfg := range testConfigs {
		testcfg(opts)
	}

	verifyValidationEquivalence(t, expectedErrs, func(c context.Context) field.ErrorList {
		return validate(c, obj, old)
	}, ctx, opts, obj)
	VerifyVersionedValidationEquivalence(t, obj, old, testConfigs...)
}

// verifyValidationEquivalence 是通用辅助函数：跨声明式校验执行模式验证等价性。
func verifyValidationEquivalence(t *testing.T, expectedErrs field.ErrorList, runValidations func(context.Context) field.ErrorList, ctx context.Context, opt *validationOption, obj runtime.Object) {
	t.Helper()

	// 重置指标，保证不匹配检查从干净状态开始；测试结束后也重置。
	legacyregistry.Reset()
	defer legacyregistry.Reset()

	// errOutputMatcher 校验实际错误与用例期望一致。
	errOutputMatcher := field.ErrorMatcher{}.ByType().ByOrigin().ByFieldNormalized(opt.NormalizationRules)

	// 1. Beta 开关开启的声明式校验。
	t.Run("with declarative validation (Beta enabled)", func(t *testing.T) {
		validationmetrics.ResetValidationMetricsInstance()
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.DeclarativeValidationBeta: true,
		})
		errs := runValidations(ctx)

		if len(expectedErrs) > 0 {
			errOutputMatcher.Test(t, expectedErrs, errs)
		} else if len(errs) != 0 {
			t.Errorf("expected no errors, but got: %v", errs)
		}

		// 不应有不匹配被记录或指标被增加。
		testutil.AssertVectorCount(t, "apiserver_validation_declarative_validation_mismatch_total", nil, 0)
	})

	// 2. Beta 开关关闭的声明式校验。
	t.Run("with declarative validation (Beta disabled)", func(t *testing.T) {
		validationmetrics.ResetValidationMetricsInstance()
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.DeclarativeValidationBeta: false,
		})
		errs := runValidations(ctx)

		if len(expectedErrs) > 0 {
			errOutputMatcher.Test(t, expectedErrs, errs)
		} else if len(errs) != 0 {
			t.Errorf("expected no errors, but got: %v", errs)
		}

		testutil.AssertVectorCount(t, "apiserver_validation_declarative_validation_mismatch_total", nil, 0)
	})

	// 3. 强制生效所有声明式规则（仅测试用）。
	// 该子测试确保包括 Alpha 在内的所有声明式规则都正确实现、与期望错误一致。
	// 它使用特殊 context 强制所有规则生效，并过滤掉对应的手写校验错误。
	t.Run("with declarative validation (All Rules Enforced)", func(t *testing.T) {
		validationmetrics.ResetValidationMetricsInstance()
		// context 覆盖已经会强制执行，这里设置开关只是为了环境一致。
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			features.DeclarativeValidationBeta: true,
		})
		testCtx := validate.WithAllDeclarativeEnforcedForTest(ctx)
		allDeclarativeErrs := runValidations(testCtx)

		// 记录本子测试观察到的声明式规则，
		// 供 TestMain 中的 AssertDeclarativeCoverage 确认每条规则都被至少一个用例覆盖。
		recordObservedRules(testCtx, obj, allDeclarativeErrs)

		// 此模式下 strategy.go 的校验会移除所有“已被声明式覆盖”的手写校验错误，
		// 所以期望错误也要相应过滤：声明式校验因父节点短路可能不返回这些错误。
		filteredExpectedErrors := make(field.ErrorList, 0, len(expectedErrs))
		for _, err := range expectedErrs {
			if !err.ShortCircuitedInDeclarative {
				filteredExpectedErrors = append(filteredExpectedErrors, err)
			}
		}
		// 这个匹配器更严格，额外按稳定性级别和来源比较：
		// 既确保 Alpha 规则的错误被包含并正确匹配，也确保错误确实来自声明式校验。
		dvErrorMatcher := errOutputMatcher.ByValidationStabilityLevel().BySource()
		if len(filteredExpectedErrors) > 0 {
			dvErrorMatcher.Test(t, filteredExpectedErrors, allDeclarativeErrs)
		} else if len(allDeclarativeErrs) != 0 {
			t.Errorf("expected no errors, but got: %v", allDeclarativeErrs)
		}

		testutil.AssertVectorCount(t, "apiserver_validation_declarative_validation_mismatch_total", nil, 0)
	})
}

// recordObservedRules 提取 obj 的 GVK（优先使用 scheme 的规范 Kind，
// 否则回退到 Go 类型名），并把所有错误交给 coverage.RecordObservedRules 记录。
func recordObservedRules(ctx context.Context, obj runtime.Object, errs field.ErrorList) {
	info, ok := genericapirequest.RequestInfoFrom(ctx)
	if !ok {
		return
	}
	kind := reflect.TypeOf(obj).Elem().Name()
	if gvks, _, err := legacyscheme.Scheme.ObjectKinds(obj); err == nil && len(gvks) > 0 {
		kind = gvks[0].Kind
	}
	gvk := schema.GroupVersionKind{Group: info.APIGroup, Version: info.APIVersion, Kind: kind}
	coverage.RecordObservedRules(gvk, errs)
}
