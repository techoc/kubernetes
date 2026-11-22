/*
Copyright 2015 The Kubernetes Authors.

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

package validation

import (
	"context"
	"fmt"
	"regexp"
	"unicode"

	"k8s.io/apimachinery/pkg/api/operation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"k8s.io/utils/ptr"
)

// LabelSelectorValidationOptions 是传给 ValidateLabelSelector 的校验选项集合。
// 这些开关的存在主要是为了向后兼容：历史数据中可能已经存在不合规的 selector，
// 若在更新时严格校验会导致用户无法修改对象。
type LabelSelectorValidationOptions struct {
	// AllowInvalidLabelValueInSelector 允许 selector 中出现非法的 label value。
	// 仅在需要兼容已持久化的历史非法数据时才应置为 true。
	AllowInvalidLabelValueInSelector bool

	// AllowUnknownOperatorInRequirement 允许 requirement 使用无法识别的 operator。
	// 适用于只做宽松检查的场景，例如 *SubjectAccessReview。
	AllowUnknownOperatorInRequirement bool
}

// LabelSelectorHasInvalidLabelValue 判断给定 selector 的 match expression 中是否包含非法的 label value。
// 用于在更新已有对象时判断是否应把 AllowInvalidLabelValueInSelector 置为 true，
// 以放行那些在规则收紧前就已持久化的非法值。
func LabelSelectorHasInvalidLabelValue(ps *metav1.LabelSelector) bool {
	if ps == nil {
		return false
	}
	// 只检查 matchExpressions 的 Values；matchLabels 的键值校验在 ValidateLabels 中完成。
	for _, e := range ps.MatchExpressions {
		for _, v := range e.Values {
			if len(validation.IsValidLabelValue(v)) > 0 {
				return true
			}
		}
	}
	return false
}

// ValidateLabelSelector 按 opts 校验 LabelSelector 并返回所有校验错误。
// 只有在需要兼容已存在的非法数据时，才应把 opts.AllowInvalidLabelValueInSelector 置为 true。
func ValidateLabelSelector(ps *metav1.LabelSelector, opts LabelSelectorValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if ps == nil {
		return allErrs
	}
	// matchLabels 是简单的键值对，复用通用的 labels 校验。
	allErrs = append(allErrs, ValidateLabels(ps.MatchLabels, fldPath.Child("matchLabels"))...)
	// matchExpressions 需要按 operator 语义逐条校验。
	for i, expr := range ps.MatchExpressions {
		allErrs = append(allErrs, ValidateLabelSelectorRequirement(expr, opts, fldPath.Child("matchExpressions").Index(i))...)
	}
	return allErrs
}

// ValidateLabelSelectorRequirement 按 opts 校验单条 selector requirement 并返回所有校验错误。
// 只有在需要兼容已存在的非法数据时，才应把 opts.AllowInvalidLabelValueInSelector 置为 true。
func ValidateLabelSelectorRequirement(sr metav1.LabelSelectorRequirement, opts LabelSelectorValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// In/NotIn 必须给出取值集合；Exists/DoesNotExist 则不允许给出取值。
	switch sr.Operator {
	case metav1.LabelSelectorOpIn, metav1.LabelSelectorOpNotIn:
		if len(sr.Values) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("values"), "must be specified when `operator` is 'In' or 'NotIn'"))
		}
	case metav1.LabelSelectorOpExists, metav1.LabelSelectorOpDoesNotExist:
		if len(sr.Values) > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("values"), "may not be specified when `operator` is 'Exists' or 'DoesNotExist'"))
		}
	default:
		// 未知 operator 默认报错；宽松模式（如 SubjectAccessReview）下放行。
		if !opts.AllowUnknownOperatorInRequirement {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), sr.Operator, "not a valid selector operator"))
		}
	}
	// 无论 operator 是否合法，键名都必须满足 label 名称规则。
	allErrs = append(allErrs, ValidateLabelName(sr.Key, fldPath.Child("key"))...)
	// 仅在未开启兼容模式时校验取值，避免历史非法数据在更新时被卡住。
	if !opts.AllowInvalidLabelValueInSelector {
		for valueIndex, value := range sr.Values {
			for _, msg := range validation.IsValidLabelValue(value) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("values").Index(valueIndex), value, msg))
			}
		}
	}
	return allErrs
}

// ValidateLabelName 校验 label 名称（含可选的域名前缀）是否合法。
// origin 固定为 format=k8s-label-key，以便声明式校验对齐。
func ValidateLabelName(labelName string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(labelName) {
		allErrs = append(allErrs, field.Invalid(fldPath, labelName, msg).WithOrigin("format=k8s-label-key"))
	}
	return allErrs
}

// ValidateLabels 校验一组 labels 的键与值是否都合法。
// 注意：错误路径只到 labels 的父路径，具体是哪个键出错需看错误消息中的值。
func ValidateLabels(labels map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for k, v := range labels {
		// map 遍历顺序随机，错误顺序不稳定，调用方不应依赖顺序。
		allErrs = append(allErrs, ValidateLabelName(k, fldPath)...)
		for _, msg := range validation.IsValidLabelValue(v) {
			allErrs = append(allErrs, field.Invalid(fldPath, v, msg).WithOrigin("format=k8s-label-value"))
		}
	}
	return allErrs
}

// FieldSelectorValidationOptions 是传给 ValidateFieldSelectorRequirement 的校验选项。
type FieldSelectorValidationOptions struct {
	// AllowUnknownOperatorInRequirement 允许 requirement 使用无法识别的 operator。
	// 适用于只做宽松检查的场景，例如 *SubjectAccessReview。
	AllowUnknownOperatorInRequirement bool
}

// ValidateFieldSelectorRequirement 按 opts 校验单条 field selector requirement 并返回所有校验错误。
// 与 label selector 不同，field selector 的键没有统一的格式约束，只要求非空。
func ValidateFieldSelectorRequirement(requirement metav1.FieldSelectorRequirement, opts FieldSelectorValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// field selector 的键语义由各资源自行定义，这里只能保证其非空。
	if len(requirement.Key) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("key"), "must be specified"))
	}

	switch requirement.Operator {
	case metav1.FieldSelectorOpIn, metav1.FieldSelectorOpNotIn:
		if len(requirement.Values) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("values"), "must be specified when `operator` is 'In' or 'NotIn'"))
		}
	case metav1.FieldSelectorOpExists, metav1.FieldSelectorOpDoesNotExist:
		if len(requirement.Values) > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("values"), "may not be specified when `operator` is 'Exists' or 'DoesNotExist'"))
		}
	default:
		if !opts.AllowUnknownOperatorInRequirement {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("operator"), requirement.Operator, "not a valid selector operator"))
		}
	}

	return allErrs
}

// ValidateDeleteOptions 校验删除选项之间的互斥关系与取值范围。
func ValidateDeleteOptions(options *metav1.DeleteOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	//lint:file-ignore SA1019 Keep validation for deprecated OrphanDependents option until it's being removed
	// OrphanDependents 已废弃，但仍需与新的 PropagationPolicy 做互斥校验，避免两种语义同时生效。
	if options.OrphanDependents != nil && options.PropagationPolicy != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("propagationPolicy"), options.PropagationPolicy, "orphanDependents and deletionPropagation cannot be both set"))
	}
	// PropagationPolicy 只接受三个枚举值；nil 由默认值填充逻辑处理，因此消息中列出 "nil"。
	if options.PropagationPolicy != nil &&
		*options.PropagationPolicy != metav1.DeletePropagationForeground &&
		*options.PropagationPolicy != metav1.DeletePropagationBackground &&
		*options.PropagationPolicy != metav1.DeletePropagationOrphan {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("propagationPolicy"), options.PropagationPolicy, []string{string(metav1.DeletePropagationForeground), string(metav1.DeletePropagationBackground), string(metav1.DeletePropagationOrphan), "nil"}))
	}
	allErrs = append(allErrs, ValidateDryRun(field.NewPath("dryRun"), options.DryRun)...)
	allErrs = append(allErrs, ValidateIgnoreStoreReadError(field.NewPath("ignoreStoreReadErrorWithClusterBreakingPotential"), options)...)
	return allErrs
}

// ValidateCreateOptions 校验创建选项中的 fieldManager、dryRun 与 fieldValidation。
func ValidateCreateOptions(options *metav1.CreateOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateFieldManager(options.FieldManager, field.NewPath("fieldManager"))...)
	allErrs = append(allErrs, ValidateDryRun(field.NewPath("dryRun"), options.DryRun)...)
	allErrs = append(allErrs, ValidateFieldValidation(field.NewPath("fieldValidation"), options.FieldValidation)...)
	return allErrs
}

// ValidateUpdateOptions 校验更新选项；字段与创建选项相同。
func ValidateUpdateOptions(options *metav1.UpdateOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateFieldManager(options.FieldManager, field.NewPath("fieldManager"))...)
	allErrs = append(allErrs, ValidateDryRun(field.NewPath("dryRun"), options.DryRun)...)
	allErrs = append(allErrs, ValidateFieldValidation(field.NewPath("fieldValidation"), options.FieldValidation)...)
	return allErrs
}

// ValidatePatchOptions 校验补丁选项。apply 补丁与普通补丁的约束不同：
// apply 必须显式指定 fieldManager（便于服务端做字段归属），普通补丁则不允许使用 force。
func ValidatePatchOptions(options *metav1.PatchOptions, patchType types.PatchType) field.ErrorList {
	allErrs := field.ErrorList{}
	switch patchType {
	case types.ApplyYAMLPatchType, types.ApplyCBORPatchType:
		if options.FieldManager == "" {
			// kubectl 会默认填 "kubectl"，但控制器必须显式设置该字段——
			// 否则无法区分不同控制器的字段归属。
			allErrs = append(allErrs, field.Required(field.NewPath("fieldManager"), "is required for apply patch"))
		}
	default:
		// force 只对 apply 补丁有意义（用于强制解决冲突）。
		if options.Force != nil {
			allErrs = append(allErrs, field.Forbidden(field.NewPath("force"), "may not be specified for non-apply patch"))
		}
	}
	allErrs = append(allErrs, ValidateFieldManager(options.FieldManager, field.NewPath("fieldManager"))...)
	allErrs = append(allErrs, ValidateDryRun(field.NewPath("dryRun"), options.DryRun)...)
	allErrs = append(allErrs, ValidateFieldValidation(field.NewPath("fieldValidation"), options.FieldValidation)...)
	return allErrs
}

// FieldManagerMaxLength 是 fieldManager 允许的最大长度。
var FieldManagerMaxLength = 128

// ValidateFieldManager 校验 fieldManager 的长度合规，且只包含可打印字符。
func ValidateFieldManager(fieldManager string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 该字段无法用 `*string` 表达"未设置"，因此空字符串 "" 被视为未设置，
	// 由后续流程填入默认值（apply 场景除外，那里要求必填）。
	if len(fieldManager) > FieldManagerMaxLength {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, FieldManagerMaxLength))
	}
	// 逐个 rune 检查可打印性；用 range 遍历保证位置按字符而非字节计。
	for i, r := range fieldManager {
		if !unicode.IsPrint(r) {
			allErrs = append(allErrs, field.Invalid(fldPath, fieldManager, fmt.Sprintf("invalid character %#U (at position %d)", r, i)))
		}
	}

	return allErrs
}

// allowedDryRunValues 是 dryRun 查询参数允许的取值集合。
var allowedDryRunValues = sets.NewString(metav1.DryRunAll)

// ValidateDryRun 校验 dryRun 查询参数只包含允许的取值。
func ValidateDryRun(fldPath *field.Path, dryRun []string) field.ErrorList {
	allErrs := field.ErrorList{}
	if !allowedDryRunValues.HasAll(dryRun...) {
		allErrs = append(allErrs, field.NotSupported(fldPath, dryRun, allowedDryRunValues.List()))
	}
	return allErrs
}

// allowedFieldValidationValues 是 fieldValidation 查询参数允许的取值；
// 空字符串表示未指定，由服务端决定默认级别。
var allowedFieldValidationValues = sets.NewString("", metav1.FieldValidationIgnore, metav1.FieldValidationWarn, metav1.FieldValidationStrict)

// ValidateFieldValidation 校验 fieldValidation 查询参数只包含允许的取值。
func ValidateFieldValidation(fldPath *field.Path, fieldValidation string) field.ErrorList {
	allErrs := field.ErrorList{}
	if !allowedFieldValidationValues.Has(fieldValidation) {
		allErrs = append(allErrs, field.NotSupported(fldPath, fieldValidation, allowedFieldValidationValues.List()))
	}
	return allErrs

}

// UninitializedStatusUpdateErrorMsg 用于拒绝在对象尚未初始化时更新 status。
// 抽成常量是为了让服务端与测试共享同一文案。
const UninitializedStatusUpdateErrorMsg string = `must not update status when the object is uninitialized`

// ValidateTableOptions 返回 TableOptions 中任何非法的字段设置。
func ValidateTableOptions(opts *metav1.TableOptions) field.ErrorList {
	var allErrs field.ErrorList
	// 空字符串等价于 IncludeMetadata，属于允许的默认值。
	switch opts.IncludeObject {
	case metav1.IncludeMetadata, metav1.IncludeNone, metav1.IncludeObject, "":
	default:
		allErrs = append(allErrs, field.Invalid(field.NewPath("includeObject"), opts.IncludeObject, "must be 'Metadata', 'Object', 'None', or empty"))
	}
	return allErrs
}

// MaxSubresourceNameLength 是 subresource 名称允许的最大长度。
const MaxSubresourceNameLength = 256

// ManagedFieldsValidationOption 指定校验 managed fields 时的选项。
type ManagedFieldsValidationOption int

const (
	// CoveredByDeclarative 表示错误应被标记为由声明式校验覆盖，
	// 以便后续去重时与对应的声明式错误合并。
	CoveredByDeclarative ManagedFieldsValidationOption = iota + 1
)

// ValidateManagedFields 校验一组 managed fields 条目。
// 通过 opts 可选择把部分错误标记为"已由声明式校验覆盖"。
func ValidateManagedFields(fieldsList []metav1.ManagedFieldsEntry, fldPath *field.Path, opts ...ManagedFieldsValidationOption) field.ErrorList {
	// 选项以可变参数传入，便于后续扩展而不破坏调用方签名。
	coveredByDeclarative := false
	for _, opt := range opts {
		if opt == CoveredByDeclarative {
			coveredByDeclarative = true
		}
	}
	var allErrs field.ErrorList
	for i, fields := range fieldsList {
		fldPath := fldPath.Index(i)
		// operation 只能为空报错或取 Apply/Update；其余取值一律不支持。
		switch fields.Operation {
		case "":
			err := field.Required(fldPath.Child("operation"), "must not be empty")
			if coveredByDeclarative {
				err = err.MarkCoveredByDeclarative()
			}
			allErrs = append(allErrs, err)
		case metav1.ManagedFieldsOperationApply, metav1.ManagedFieldsOperationUpdate:
		default:
			err := field.NotSupported(fldPath.Child("operation"), fields.Operation, []metav1.ManagedFieldsOperationType{metav1.ManagedFieldsOperationApply, metav1.ManagedFieldsOperationUpdate})
			if coveredByDeclarative {
				err = err.MarkCoveredByDeclarative()
			}
			allErrs = append(allErrs, err)
		}
		// FieldsType 允许为空（兼容旧数据），但一旦设置就只能是 FieldsV1。
		if len(fields.FieldsType) > 0 && fields.FieldsType != "FieldsV1" {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("fieldsType"), fields.FieldsType, "must be `FieldsV1`"))
		}
		allErrs = append(allErrs, ValidateFieldManager(fields.Manager, fldPath.Child("manager"))...)

		// 限制 subresource 长度，防止超长名称在日志/索引中造成问题。
		if len(fields.Subresource) > MaxSubresourceNameLength {
			allErrs = append(allErrs, field.TooLong(fldPath.Child("subresource"), "" /*unused*/, MaxSubresourceNameLength))
		}
	}
	return allErrs
}

// ValidateConditions 校验一组 condition，并额外检查 Condition 的 Type 不能重复
// （同一对象同一时刻每种类型最多一个 condition）。
func ValidateConditions(conditions []metav1.Condition, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// 记录每个 Type 首次出现的位置，用于报告重复。
	conditionTypeToFirstIndex := map[string]int{}
	for i, condition := range conditions {
		if _, ok := conditionTypeToFirstIndex[condition.Type]; ok {
			allErrs = append(allErrs, field.Duplicate(fldPath.Index(i), condition.Type).MarkCoveredByDeclarative())
		} else {
			conditionTypeToFirstIndex[condition.Type] = i
		}

		allErrs = append(allErrs, ValidateCondition(condition, fldPath.Index(i))...)
	}

	return allErrs
}

// validConditionStatuses 用于内部合法性检查，并作为错误消息中的候选值列表。
var validConditionStatuses = sets.NewString(string(metav1.ConditionTrue), string(metav1.ConditionFalse), string(metav1.ConditionUnknown))

// condition 的 reason 与 message 长度上限：
// reason 应简短可枚举，message 可稍长但仍需限制，防止对象体积失控。
const (
	maxReasonLen  = 1 * 1024
	maxMessageLen = 32 * 1024
)

// ValidateCondition 校验单个 condition 的必填字段、取值与长度约束。
// 其中多数错误标记为 MarkCoveredByDeclarative，表示已有对应的声明式校验规则。
func ValidateCondition(condition metav1.Condition, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// type 必须存在且符合 label 名称格式。
	if len(condition.Type) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), "is required").MarkCoveredByDeclarative())
	} else {
		allErrs = append(allErrs, ValidateLabelName(condition.Type, fldPath.Child("type"))...)
	}

	// status 必须存在且取 True/False/Unknown 之一。
	if len(condition.Status) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("status"), "").MarkCoveredByDeclarative())
	} else if !validConditionStatuses.Has(string(condition.Status)) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("status"), condition.Status, validConditionStatuses.List()).MarkCoveredByDeclarative())
	}

	// observedGeneration 表示控制器观察到的最新一代，负数没有意义。
	if condition.ObservedGeneration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("observedGeneration"), condition.ObservedGeneration, "must be greater than or equal to zero").WithOrigin("minimum").MarkCoveredByDeclarative())
	}

	// lastTransitionTime 是必填项：没有它就无法判断状态变化的时间顺序。
	if condition.LastTransitionTime.IsZero() {
		allErrs = append(allErrs, field.Required(fldPath.Child("lastTransitionTime"), "").MarkCoveredByDeclarative())
	}

	// reason 必填，且需满足格式与长度约束。
	if len(condition.Reason) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("reason"), "").MarkCoveredByDeclarative())
	} else {
		for _, currErr := range IsValidConditionReason(condition.Reason) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("reason"), condition.Reason, currErr))
		}
		if len(condition.Reason) > maxReasonLen {
			allErrs = append(allErrs, field.TooLong(fldPath.Child("reason"), "" /*unused*/, maxReasonLen).WithOrigin("maxBytes").MarkCoveredByDeclarative())
		}
	}

	// message 可为空，但过长会被拒绝。
	if len(condition.Message) > maxMessageLen {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("message"), "" /*unused*/, maxMessageLen))
	}

	return allErrs
}

// condition reason 的格式：以字母开头，中间可含字母数字与 _,:，并以字母数字或 _ 结尾。
// 单字符 reason 也是合法的（因此尾部字符类整体可选）。
const conditionReasonFmt string = "[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?"
const conditionReasonErrMsg string = "a condition reason must start with alphabetic character, optionally followed by a string of alphanumeric characters or '_,:', and must end with an alphanumeric character or '_'"

var conditionReasonRegexp = regexp.MustCompile("^" + conditionReasonFmt + "$")

// IsValidConditionReason 检查字符串是否符合 condition reason 的格式规则。
// 只校验格式，不校验长度（长度由调用方单独检查）。
func IsValidConditionReason(value string) []string {
	if !conditionReasonRegexp.MatchString(value) {
		return []string{validation.RegexError(conditionReasonErrMsg, conditionReasonFmt, "my_name", "MY_NAME", "MyName", "ReasonA,ReasonB", "ReasonA:ReasonB")}
	}
	return nil
}

// ValidateIgnoreStoreReadError 校验当 ignoreStoreReadErrorWithClusterBreakingPotential
// 被启用时，删除选项是否合法。
// 该开关会跳过存储读取错误，因此与传播策略、优雅删除、前置条件等存在语义冲突，
// 不允许同时设置。
func ValidateIgnoreStoreReadError(fldPath *field.Path, options *metav1.DeleteOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	// 开关未启用时没有额外约束，直接返回。
	if enabled := ptr.Deref[bool](options.IgnoreStoreReadErrorWithClusterBreakingPotential, false); !enabled {
		return allErrs
	}

	if options.PropagationPolicy != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, true, "cannot be set together with .propagationPolicy"))
	}
	//nolint:staticcheck // Keep validation for deprecated OrphanDependents option until it's being removed
	// 已废弃的 OrphanDependents 同样与忽略读取错误冲突。
	if options.OrphanDependents != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, true, "cannot be set together with .orphanDependents"))
	}
	if options.GracePeriodSeconds != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, true, "cannot be set together with .gracePeriodSeconds"))
	}
	if options.Preconditions != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, true, "cannot be set together with .preconditions"))
	}

	return allErrs
}

// ValidateCustom_Condition_LastTransitionTime 由 Condition.LastTransitionTime 上的
// 自定义校验标记（+k8s:customValidation）接入生成的声明式校验。
// 它强制该字段必须设置，与 ValidateCondition 中的手写检查保持一致。
func ValidateCustom_Condition_LastTransitionTime(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *metav1.Time) field.ErrorList {
	// 零值时间表示未设置；value 为 nil 时 IsZero 也返回 true。
	if value.IsZero() {
		return field.ErrorList{field.Required(fldPath, "")}
	}
	return nil
}
