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

package validators

import (
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/validate"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	updateTagName = "k8s:update"
)

func init() {
	// 收集器与字段校验器必须共享同一张路径表：多个 +k8s:update 标签需要在
	// 延迟阶段合并成一次运行时调用。
	shared := map[string]*updateMetadata{}
	RegisterTagValidator(updateTagCollector{byFieldPath: shared, listByPath: globalListMeta})
}

// updateMetadata 收集字段的更新约束，同时支持普通校验与影子校验。
type updateMetadata struct {
	constraints    sets.Set[validate.UpdateConstraint]
	stabilityLevel ValidationStabilityLevel
}

// updateTagCollector 收集 +k8s:update 标签。
// 因为标签是逐个到达的，而运行时校验希望每个字段只调用一次，所以先累积、
// 再通过 Deferred 在上下文完整后统一生成。
type updateTagCollector struct {
	byFieldPath map[string]*updateMetadata
	listByPath  map[string]*listMetadata
}

// Init 无额外配置。
func (updateTagCollector) Init(_ Config) {}

// TagName 返回 +k8s:update。
func (updateTagCollector) TagName() string {
	return updateTagName
}

// updateTagValidScopes 除字段外也允许作用于 list/map 元素（通过 +k8s:eachVal）。
var updateTagValidScopes = sets.New(ScopeField, ScopeListVal, ScopeMapVal)

// ValidScopes 返回允许的作用域。
func (updateTagCollector) ValidScopes() sets.Set[Scope] {
	return updateTagValidScopes
}

// GetValidations 解析约束值，元素作用域立即生成校验，字段作用域则累积后延迟生成。
func (utc updateTagCollector) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// 从当前标签解析约束条件：
	// 只接受固定的五个语义化取值，拼写错误直接报错而不是静默忽略。
	var constraint validate.UpdateConstraint
	switch tag.Value {
	case "NoSet":
		constraint = validate.NoSet
	case "NoUnset":
		constraint = validate.NoUnset
	case "NoModify":
		constraint = validate.NoModify
	case "NoAddItem":
		constraint = validate.NoAddItem
	case "NoRemoveItem":
		constraint = validate.NoRemoveItem
	default:
		return Validations{}, fmt.Errorf("unknown +k8s:update constraint: %s", tag.Value)
	}

	// 元素作用域（经 +k8s:eachVal 抵达）：只有 NoModify 有效，且每次标签最多一个约束，
	// 直接输出校验即可。
	// 字段作用域：继续向下累积，让同一字段上的多个 +k8s:update 标签
	// 之后由 updateFieldValidator 合并为一次调用。
	if context.Scope == ScopeListVal || context.Scope == ScopeMapVal {
		if constraint != validate.NoModify {
			return Validations{}, fmt.Errorf("+k8s:update=%s does not apply to %s, attach it to the enclosing field", constraintName(constraint), context.Scope)
		}
		// 元素本身若是容器，NoModify 无法区分“整体替换”与“内部修改”，拒绝。
		nt := util.NonPointer(util.NativeType(context.Type))
		if nt.Kind == types.Slice || nt.Kind == types.Map {
			return Validations{}, fmt.Errorf("+k8s:update=NoModify cannot be applied to list/map elements that are themselves lists or maps")
		}
		// ScopeListVal 下，只有按键匹配元素的列表（listType=map 或 unique=map）
		// 才需要 NoModify。set/atomic 列表的内容变化会变成新的未匹配项，
		// 在 ratcheting 中被视为无变化，因此提前拒绝。
		if context.Scope == ScopeListVal && context.ParentPath != nil {
			if lm := utc.listByPath[context.ParentPath.String()]; lm != nil && lm.semantic != semanticMap {
				return Validations{}, fmt.Errorf("+k8s:eachVal=+k8s:update=NoModify requires the enclosing list to use listType=map or unique=map (got %s)", lm.semantic)
			}
		}
		v := emitScalarUpdate(context, []validate.UpdateConstraint{constraint})
		applyStabilityLevel(&v, context.StabilityLevel)
		return v, nil
	}

	// 元数据不存在时先初始化：字段第一次出现时创建累积槽；后续标签只做合并。
	fieldPath := context.Path.String()
	if utc.byFieldPath[fieldPath] == nil {
		utc.byFieldPath[fieldPath] = &updateMetadata{constraints: sets.New[validate.UpdateConstraint]()}
	}
	um := utc.byFieldPath[fieldPath]

	um.stabilityLevel = context.StabilityLevel

	// 把该约束加入该字段的约束集合。
	um.constraints.Insert(constraint)

	// 每次新增约束都重新做类型兼容性检查，让错误尽早指向具体的标签。
	if err := utc.validateConstraintsForType(context, um.constraints.UnsortedList()); err != nil {
		return Validations{}, err
	}

	// 这里只登记一个延迟生成任务，真正输出要等所有标签收集完，避免重复函数定义。
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ThisContext, func() (Validations, error) {
				return getUpdateValidations(utc.byFieldPath, utc.listByPath, context)
			}),
		},
	}, nil
}

// validateConstraintsForType 检查约束与字段类型是否相容：
// 列表/映射专属约束不能用于标量，反之 NoModify 用于容器则语义不明确。
func (utc updateTagCollector) validateConstraintsForType(context Context, constraints []validate.UpdateConstraint) error {
	t := util.NonPointer(util.NativeType(context.Type))
	isCompound := t.Kind == types.Slice || t.Kind == types.Map
	isPointer := context.Type.Kind == types.Pointer
	isStruct := t.Kind == types.Struct

	for _, constraint := range constraints {
		switch constraint {
		case validate.NoAddItem, validate.NoRemoveItem:
			if !isCompound {
				return fmt.Errorf("+k8s:update=%s can only be used on list or map fields", constraintName(constraint))
			}
		case validate.NoModify:
			if isCompound {
				return fmt.Errorf("+k8s:update=NoModify is not supported on list or map fields, use +k8s:eachVal=+k8s:update=NoModify for per-item immutability")
			}
		case validate.NoSet, validate.NoUnset:
			// 非指针结构体字段只适用 NoModify。
			if isStruct && !isPointer {
				return fmt.Errorf("+k8s:update=%s cannot be used on non-pointer struct fields (they cannot be unset)", constraintName(constraint))
			}
		}
	}

	return nil
}

// constraintName 把约束枚举渲染成标签里使用的名字，用于构造可操作的错误信息。
func constraintName(c validate.UpdateConstraint) string {
	switch c {
	case validate.NoSet:
		return "NoSet"
	case validate.NoUnset:
		return "NoUnset"
	case validate.NoModify:
		return "NoModify"
	case validate.NoAddItem:
		return "NoAddItem"
	case validate.NoRemoveItem:
		return "NoRemoveItem"
	default:
		return fmt.Sprintf("Unknown(%d)", c)
	}
}

// Docs 返回标签文档，说明各约束的语义与适用类型。
func (utc updateTagCollector) Docs() TagDoc {
	return TagDoc{
		Tag:            utc.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(utc.ValidScopes()),
		PayloadsType:   codetags.ValueTypeString,
		Description: "Provides constraints on the allowed update operations of a field. " +
			"Constraints: NoSet (prevents unset->set transitions), NoUnset (prevents set->unset transitions), " +
			"NoModify (prevents value changes but allows set/unset transitions), " +
			"NoAddItem (prevents adding items to a slice or map), NoRemoveItem (prevents removing items from a slice or map). " +
			"Multiple constraints can be specified using multiple tags. " +
			"For non-pointer structs, NoSet and NoUnset have no effect as these fields cannot be unset. " +
			"For slice and map fields, 'unset' means len == 0. Slice item identity for NoAddItem/NoRemoveItem comes from " +
			"+k8s:listType/+k8s:listMapKey/+k8s:unique, for maps the key is the item identity. " +
			"NoModify is not supported on slices or maps, use +k8s:eachVal=+k8s:update=NoModify for per-item immutability. " +
			"On lists, +k8s:eachVal=+k8s:update=NoModify requires listType=map or unique=map, otherwise content changes are not detectable. " +
			"Examples: +k8s:update=NoModify +k8s:update=NoUnset for set-once fields, " +
			"+k8s:update=NoSet for fields that must be set at creation or never, " +
			"+k8s:update=NoAddItem +k8s:update=NoRemoveItem on a listType=map field to freeze the structural shape of the list.",
	}
}

var (
	// 按目标 Go 形态选择运行时实现：可比较值、指针、结构体、不可比较值。
	updateValueValidator          = types.Name{Package: libValidationPkg, Name: "UpdateValueByCompare"}
	updatePointerValidator        = types.Name{Package: libValidationPkg, Name: "UpdatePointer"}
	updateValueByReflectValidator = types.Name{Package: libValidationPkg, Name: "UpdateValueByReflect"}
	updateStructValidator         = types.Name{Package: libValidationPkg, Name: "UpdateStruct"}
	valSliceUpdateValidator       = types.Name{Package: libValidationPkg, Name: "ValSliceUpdate"}
	ptrSliceUpdateValidator       = types.Name{Package: libValidationPkg, Name: "PtrSliceUpdate"}
	updateMapValidator            = types.Name{Package: libValidationPkg, Name: "UpdateMap"}

	// 用作实参的约束常量。
	noSetConstraint        = types.Name{Package: libValidationPkg, Name: "NoSet"}
	noUnsetConstraint      = types.Name{Package: libValidationPkg, Name: "NoUnset"}
	noModifyConstraint     = types.Name{Package: libValidationPkg, Name: "NoModify"}
	noAddItemConstraint    = types.Name{Package: libValidationPkg, Name: "NoAddItem"}
	noRemoveItemConstraint = types.Name{Package: libValidationPkg, Name: "NoRemoveItem"}
)

// getUpdateValidations 在延迟阶段为某字段生成合并后的更新校验。
func getUpdateValidations(byFieldPath map[string]*updateMetadata, listByPath map[string]*listMetadata, context Context) (Validations, error) {
	um := byFieldPath[context.Path.String()]

	if um == nil || um.constraints.Len() == 0 {
		return Validations{}, nil
	}
	// 处理后从 map 中删除条目，避免重复处理。
	delete(byFieldPath, context.Path.String())

	constraints := um.constraints.UnsortedList()

	v, err := generateUpdateValidation(listByPath, context, constraints)
	if err != nil {
		return Validations{}, err
	}

	level := um.stabilityLevel
	// 显式标签稳定性（如 +k8s:alpha 包裹）优先于收集时记录的包级默认值。
	if context.StabilityLevel != "" {
		level = context.StabilityLevel
	}

	applyStabilityLevel(&v, level)
	return v, nil
}

// generateUpdateValidation 按类型分派到 slice/map/标量三条生成路径。
func generateUpdateValidation(listByPath map[string]*listMetadata, context Context, constraints []validate.UpdateConstraint) (Validations, error) {
	// 对约束排序以保证顺序确定：约束集合本身无序，排序后生成的函数参数顺序才稳定。
	slices.Sort(constraints)

	t := util.NonPointer(util.NativeType(context.Type))
	switch t.Kind {
	case types.Slice:
		return generateSliceValidation(listByPath, context, constraints)
	case types.Map:
		return generateMapValidation(constraints), nil
	}
	return emitScalarUpdate(context, constraints), nil
}

// generateSliceValidation 输出 validate.ValSliceUpdate。NoAddItem/NoRemoveItem
// 需要匹配函数来配对新旧元素，具体用哪个函数取决于列表语义（set/map）。
// +k8s:listType/+k8s:listMapKey/+k8s:unique. For NoSet/NoUnset alone, a nil match is fine.
//
// 元数据查询会从字段路径回退到类型路径，让类型定义级列表注解生效，
// 与 listValidator 的做法一致。
func generateSliceValidation(listByPath map[string]*listMetadata, context Context, constraints []validate.UpdateConstraint) (Validations, error) {
	var matchArg any = Literal("nil")
	// NoAddItem/NoRemoveItem 需要匹配函数配对新旧元素；NoSet/NoUnset 只检查 len == 0。
	if slices.Contains(constraints, validate.NoAddItem) || slices.Contains(constraints, validate.NoRemoveItem) {
		// 字段路径（字段上的 listType 注解）优先，其次回退到类型路径（typedef 上的注解）。
		lm := listByPath[context.Path.String()]
		if lm == nil {
			lm = listByPath[context.Type.String()]
		}
		if lm == nil {
			return Validations{}, fmt.Errorf("+k8s:update=NoAddItem/+k8s:update=NoRemoveItem require list metadata (+k8s:listType with listMapKey, or +k8s:unique) to determine item identity")
		}
		elem := util.NativeType(context.Type).Elem
		switch lm.semantic {
		case semanticMap:
			// map 语义依赖 key 字段配对旧新元素，未声明 key 无法实现。
			if len(lm.keyMembers) == 0 {
				return Validations{}, fmt.Errorf("+k8s:update=NoAddItem/+k8s:update=NoRemoveItem require listMapKey to be set for listType=map")
			}
			matchArg = lm.makeListMapMatchFunc(elem)
		case semanticSet:
			// set 语义按元素相等配对；可比较类型直接用 ==，否则退化到语义深比较。
			if util.IsDirectComparable(util.NonPointer(util.NativeType(elem))) {
				matchArg = Identifier(validateDirectEqual)
			} else {
				matchArg = Identifier(validateSemanticDeepEqual)
			}
		default:
			return Validations{}, fmt.Errorf("+k8s:update=NoAddItem/+k8s:update=NoRemoveItem require listType=set, listType=map, unique=set, or unique=map to define item identity")
		}
	}

	args := append([]any{matchArg}, constraintIdentifierArgs(constraints)...)

	// 指针元素切片使用 PtrSliceUpdate，以便按值而非地址比较元素内容。
	validator := valSliceUpdateValidator
	nt := util.NativeType(context.Type)
	if nt.Elem.Kind == types.Pointer {
		validator = ptrSliceUpdateValidator
	}

	// 用 ShortCircuit 标志让这些校验与 +k8s:optional 同组。
	fn := Function(updateTagName, ShortCircuit, validator, args...).
		WithEmits(compoundUpdateEmissions(constraints, false)...)
	return Validations{Functions: []FunctionGen{fn}}, nil
}

// generateMapValidation 输出 validate.UpdateMap。map 键即元素标识，
// 因此不需要列表元数据或匹配函数。
func generateMapValidation(constraints []validate.UpdateConstraint) Validations {
	// 用 ShortCircuit 标志让这些校验与 +k8s:optional 同组。
	fn := Function(updateTagName, ShortCircuit, updateMapValidator, constraintIdentifierArgs(constraints)...).
		WithEmits(compoundUpdateEmissions(constraints, true)...)
	return Validations{Functions: []FunctionGen{fn}}
}

// emitScalarUpdate 根据 context.Type 的 Go 种类，输出对
// UpdateValueByCompare/UpdatePointer/UpdateStruct/UpdateValueByReflect 的调用。
// 字段作用域（标量/指针/结构体）与经 +k8s:eachVal 的列表/map 元素作用域都用它。
func emitScalarUpdate(context Context, constraints []validate.UpdateConstraint) Validations {
	// 根据字段类型选择合适的校验函数。
	t := util.NonPointer(util.NativeType(context.Type))
	isPointer := context.Type.Kind == types.Pointer
	isStruct := t.Kind == types.Struct
	isComparable := util.IsDirectComparable(t)

	var validatorFunc types.Name
	if isPointer {
		validatorFunc = updatePointerValidator
	} else if isStruct {
		validatorFunc = updateStructValidator
	} else if isComparable {
		// 可比较类型用 == 比较，避免反射开销。
		validatorFunc = updateValueValidator
	} else {
		validatorFunc = updateValueByReflectValidator
	}

	// 用 ShortCircuit 标志让这些校验与 +k8s:optional 同组。.
	// 标量/指针/结构体字段只接受 NoSet/NoUnset/NoModify
	// （其余会被 validateConstraintsForType 拒绝），它们都在字段路径上输出 field.Invalid。
	fn := Function(updateTagName, ShortCircuit, validatorFunc, constraintIdentifierArgs(constraints)...).
		WithEmits(Emission{field.ErrorTypeInvalid, "update", ""})
	return Validations{Functions: []FunctionGen{fn}}
}

// compoundUpdateEmissions 返回 ValSliceUpdate/UpdateMap 针对给定约束产生的
// Emission（已去重），顺序稳定以便复现。NoModify 在上游已被复合类型拒绝，
// 因此这里只处理 NoSet/NoUnset/NoAddItem/NoRemoveItem。
//
//   - NoSet/NoUnset       -> 在字段路径上报 Invalid
//   - NoAddItem           -> 在 fldPath.Index(i)/fldPath.Key(k) 上报 Forbidden（"[*]"）
//   - NoRemoveItem, slice -> 在 fldPath 上报 Forbidden（""）
//   - NoRemoveItem, map   -> 在 fldPath.Key(k) 上报 Forbidden（"[*]"），与 NoAddItem 共用
//
// 多个约束可能产生同一 Emission，这里按布尔聚合去重，避免测试生成器重复计数。
func compoundUpdateEmissions(constraints []validate.UpdateConstraint, isMap bool) []Emission {
	var hasInvalid, hasAdd, hasRemove bool
	for _, c := range constraints {
		switch c {
		case validate.NoSet, validate.NoUnset:
			hasInvalid = true
		case validate.NoAddItem:
			hasAdd = true
		case validate.NoRemoveItem:
			hasRemove = true
		}
	}
	var out []Emission
	if hasInvalid {
		out = append(out, Emission{field.ErrorTypeInvalid, "update", ""})
	}
	if hasAdd || (hasRemove && isMap) {
		out = append(out, Emission{field.ErrorTypeForbidden, "update", "[*]"})
	}
	if hasRemove && !isMap {
		out = append(out, Emission{field.ErrorTypeForbidden, "update", ""})
	}
	return out
}

// constraintIdentifierArgs 按确定顺序构建约束实参。
func constraintIdentifierArgs(constraints []validate.UpdateConstraint) []any {
	var args []any
	for _, constraint := range constraints {
		switch constraint {
		case validate.NoSet:
			args = append(args, Identifier(noSetConstraint))
		case validate.NoUnset:
			args = append(args, Identifier(noUnsetConstraint))
		case validate.NoModify:
			args = append(args, Identifier(noModifyConstraint))
		case validate.NoAddItem:
			args = append(args, Identifier(noAddItemConstraint))
		case validate.NoRemoveItem:
			args = append(args, Identifier(noRemoveItemConstraint))
		}
	}
	return args
}

// applyStabilityLevel 把稳定性级别下发给已生成的每个函数。
// 空字符串表示调用方未指定，保持函数原有级别。
func applyStabilityLevel(v *Validations, level ValidationStabilityLevel) {
	if level == "" {
		return
	}
	for i := range v.Functions {
		v.Functions[i] = v.Functions[i].WithStabilityLevel(level)
	}
}
