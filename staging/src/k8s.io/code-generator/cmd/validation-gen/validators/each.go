/*
Copyright 2024 The Kubernetes Authors.

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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	eachValTagName = "k8s:eachVal"
	eachKeyTagName = "k8s:eachKey"
)

// 保留 eachVal/eachKey 两个校验器实例，是因为主代码生成逻辑会直接调用它们
// （如对 []*T 注入 nil 检查）。把它们移到主包也可以，但留在这里更清晰。
// We keep the eachVal and eachKey validators around because the main
// code-generation logic calls them directly.  We could move them into the main
// pkg, but it's easier and cleaner to leave them here.
var globalEachVal *eachValTagValidator
var globalEachKey *eachKeyTagValidator

func init() {
	// 遍历列表/映射值是特殊标记，代码生成逻辑可直接调用。
	// Iterating values of lists and maps is a special tag, which can be called
	// directly by the code-generator logic.
	globalEachVal = &eachValTagValidator{byPath: globalListMeta, validator: nil}
	RegisterTagValidator(globalEachVal)

	// 遍历映射键同样是特殊标记。
	// Iterating keys of maps is a special tag, which can be called directly by
	// the code-generator logic.
	globalEachKey = &eachKeyTagValidator{validator: nil}
	RegisterTagValidator(globalEachKey)
}

// eachValTagValidator 实现 +k8s:eachVal：把负载校验施加到列表/映射的每个值。
// byPath 保存列表元数据（listType/listMapKey），用于生成 ratcheting 所需的
// 新旧元素关联逻辑。
type eachValTagValidator struct {
	byPath    map[string]*listMetadata
	validator TagValidationExtractor
}

// Init 注入 TagValidationExtractor，用于递归解析负载中的校验标记。
func (evtv *eachValTagValidator) Init(cfg Config) {
	evtv.validator = cfg.TagValidator
}

func (eachValTagValidator) TagName() string {
	return eachValTagName
}

func (eachValTagValidator) ValidScopes() sets.Set[Scope] {
	return listTagsValidScopes
}

var (
	validateEachValSliceVal = types.Name{Package: libValidationPkg, Name: "EachValSliceVal"}
	validateEachPtrSliceVal = types.Name{Package: libValidationPkg, Name: "EachPtrSliceVal"}
	validateEachMapVal      = types.Name{Package: libValidationPkg, Name: "EachMapVal"}
)

// GetValidations 解析 eachVal 的负载并构造元素上下文，返回包裹后的校验。
func (evtv eachValTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// 注意：列表/映射的指针不被支持，因此这里不应出现指针。
	// NOTE: pointers to lists and maps are not supported, so we should never see a pointer here.
	t := context.Type
	nt := util.NativeType(t)
	switch nt.Kind {
	case types.Slice, types.Array, types.Map:
	default:
		return Validations{}, fmt.Errorf("can only be used on list or map types (%s)", nt.Kind)
	}

	// 元素上下文：路径以 (vals) 结尾，ParentType 指向集合本身。
	elemContext := Context{
		// Scope is initialized below.
		Type:           nt.Elem,
		Path:           context.Path.Key("(vals)"),
		Member:         nil, // NA for list/map values
		ParentPath:     context.Path,
		ParentType:     context.Type,
		StabilityLevel: context.StabilityLevel,
	}
	switch nt.Kind {
	case types.Slice, types.Array:
		elemContext.Scope = ScopeListVal
		// 空 selector 表示“所有元素”。
		elemContext.ListSelector = []ListSelectorTerm{} // empty == "all"
	case types.Map:
		elemContext.Scope = ScopeMapVal
		// TODO: We may need map selectors at some point.
	}
	if tag.ValueTag == nil {
		return Validations{}, fmt.Errorf("missing validation tag")
	}

	validations, err := evtv.validator.ExtractTagValidations(elemContext, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	// 元素级校验不支持生成变量（变量是类型/包级产物）。
	if len(validations.Variables) > 0 {
		return Validations{}, fmt.Errorf("variable generation is not supported")
	}

	result := Validations{
		OpaqueValType: validations.OpaqueType, // 元素 opacity 映射为集合值的 opacity
	}
	result.Comments = append(result.Comments, validations.Comments...)

	if len(validations.Functions) > 0 {
		// 延迟处理：listType/listMapKey 需要先计算出 list key。
		// We defer this because we want listType and listMapKey to compute list keys first.
		result.AddDeferred(Deferred(ThisContext, func() (Validations, error) {
			return evtv.getValidations(context.Path, t, Validations{Functions: validations.Functions})
		}))
	}

	if len(validations.Deferred) > 0 {
		result.AddDeferred(Deferred(ThisContext, func() (Validations, error) {
			resolved := Validations{}
			for _, d := range validations.Deferred {
				inner, err := d.Callback()
				if err != nil {
					return Validations{}, err
				}
				resolved.Add(inner)
			}
			return evtv.getValidations(context.Path, t, resolved)
		}))
	}

	if result.Empty() {
		return Validations{}, fmt.Errorf("no validation functions found")
	}

	return result, nil
}

// t 期望是列表/映射的最外层类型。例如 typedef 到列表时，t 是别名类型
// 而不是底层类型。
// t is expected to be the top-most type of the list or map. For example, if
// this is a typedef to a list, this is the alias type, not the underlying
// type.
func (evtv eachValTagValidator) getValidations(fldPath *field.Path, t *types.Type, validations Validations) (Validations, error) {
	switch util.NativeType(t).Kind {
	case types.Slice, types.Array:
		return evtv.getListValidations(fldPath, t, validations)
	case types.Map:
		return evtv.getMapValidations(t, validations)
	}
	return Validations{}, fmt.Errorf("non-iterable type: %v", t)
}

// ForEachVal 返回“对列表/映射每个元素施加 fn”的校验。
// t 期望是列表/映射的最外层类型（typedef 时为别名类型）。
// ForEachVal returns a validation that applies a function to each element of
// a list or map. The type argument is expected to be the top-most type of the
// list or map. For example, if this is a typedef to a list, this is the alias
// type, not the underlying type.
func ForEachVal(fldPath *field.Path, t *types.Type, fn FunctionGen) (Validations, error) {
	return globalEachVal.getValidations(fldPath, t, Validations{Functions: []FunctionGen{fn}})
}

// getListValidations 生成列表的逐元素校验，并计算 ratcheting 所需的
// match/equiv 函数。
// t 期望是列表的最外层类型（typedef 时为别名类型）。
// t is expected to be the top-most type of the list. For example, if this is a
// typedef to a list, this is the alias type, not the underlying type.
func (evtv eachValTagValidator) getListValidations(fldPath *field.Path, t *types.Type, validations Validations) (Validations, error) {
	// 优先按字段路径查元数据，其次按类型查（类型级 listType 声明）。
	listMetadata := evtv.byPath[fldPath.String()]
	if listMetadata == nil {
		// If we don't have metadata for this field, we might have it for the
		// field's type.
		listMetadata = evtv.byPath[t.String()]
	}

	nt := util.NativeType(t)

	// matchArg 用于在旧列表中定位关联元素。
	// matchArg is the function that is used to lookup the correlated element in the old list.
	var matchArg any = Literal("nil")

	// equivArg 用于比较新旧列表中关联元素是否等价；
	// 当 matchArg 本身已是完整比较函数时为 "nil"。
	// equivArg is the function that is used to compare the correlated elements in the old and new lists.
	// It would be "nil" if the matchArg is a full comparison function.
	var equivArg any = Literal("nil")

	// directComparable 决定使用 "==" 还是语义 DeepEqual 来比较关联元素。
	// directComparable is used to determine whether we can use the direct
	// comparison operator "==" or need to use the semantic DeepEqual when
	// looking up and comparing correlated list elements for validation ratcheting.
	directComparable := util.IsDirectComparable(util.NonPointer(util.NativeType(nt.Elem)))

	if listMetadata != nil {
		switch listMetadata.semantic {
		case semanticMap:
			// listType=map：用 key 定位旧元素，用 equivFunc 比较其内容。
			// For listType=map, we use key to lookup the correlated element in the old list.
			// And use equivFunc to compare the correlated elements in the old and new lists.
			matchArg = listMetadata.makeListMapMatchFunc(nt.Elem)
			if directComparable {
				equivArg = Identifier(validateDirectEqual)
			} else {
				equivArg = Identifier(validateSemanticDeepEqual)
			}
		case semanticSet:
			// listType=set：matchArg 本身就是等价判断，故 equivArg 为 nil。
			// For listType=set, matchArg is the equivalence check, so equivArg is nil.
			if directComparable {
				matchArg = Identifier(validateDirectEqual)
			} else {
				matchArg = Identifier(validateSemanticDeepEqual)
			}
		default:
			// 普通列表无需在旧列表中查找关联元素，两个参数均为 nil。
			// For non-map and non-set list, we don't lookup the correlated element in the old list.
			// The matchArg and equivArg are both nil.
		}
	}

	// 元素是指针时使用专门的校验函数（需处理 nil 元素）。
	validateFunc := validateEachValSliceVal
	if nt.Elem.Kind == types.Pointer {
		validateFunc = validateEachPtrSliceVal
	}

	wrapped := WrapFunctions(validations, func(vfn FunctionGen, _ DeferredScope) FunctionGen {
		comm := vfn.Comments
		vfn.Comments = nil
		return Function(eachValTagName, vfn.Flags, validateFunc, matchArg, equivArg, WrapperFunction{Function: vfn, ObjType: nt.Elem, PathFragment: "[*]"}).WithComments(comm...)
	})
	// 只保留 Functions/Deferred；元素 opacity 转为集合值的 opacity。
	// Only Functions/Deferred carry forward; element opacity becomes value opacity.
	return Validations{
		Functions:     wrapped.Functions,
		Deferred:      wrapped.Deferred,
		OpaqueValType: validations.OpaqueType,
	}, nil
}

// getMapValidations 生成 map value 的逐值校验。
// t 期望是 map 的最外层类型（typedef 时为别名类型）。
// t is expected to be the top-most type of the map. For example, if this is a
// typedef to a map, this is the alias type, not the underlying type.
func (evtv eachValTagValidator) getMapValidations(t *types.Type, validations Validations) (Validations, error) {
	nt := util.NativeType(t)
	// map value 的 ratcheting 按值语义比较。
	equivArg := Identifier(validateSemanticDeepEqual)
	if util.IsDirectComparable(util.NonPointer(util.NativeType(nt.Elem))) {
		equivArg = Identifier(validateDirectEqual)
	}
	wrapped := WrapFunctions(validations, func(vfn FunctionGen, _ DeferredScope) FunctionGen {
		comm := vfn.Comments
		vfn.Comments = nil
		return Function(eachValTagName, vfn.Flags, validateEachMapVal, equivArg, WrapperFunction{Function: vfn, ObjType: nt.Elem, PathFragment: "[*]"}).WithComments(comm...)
	})
	return Validations{
		Functions:     wrapped.Functions,
		Deferred:      wrapped.Deferred,
		OpaqueValType: validations.OpaqueType,
	}, nil
}

// Docs 返回该标记的文档元数据（用于生成文档与 lint）。
func (evtv eachValTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            evtv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(evtv.ValidScopes()),
		Description:    "Declares a validation for each value in a map or list.",
		Payloads: []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        "The tag to evaluate for each value.",
		}},
		PayloadsType:     codetags.ValueTypeTag,
		PayloadsRequired: true,
	}
	return doc
}

// eachKeyTagValidator 实现 +k8s:eachKey：把负载校验施加到 map 的每个 key。
type eachKeyTagValidator struct {
	validator TagValidationExtractor
}

func (ektv *eachKeyTagValidator) Init(cfg Config) {
	ektv.validator = cfg.TagValidator
}

func (eachKeyTagValidator) TagName() string {
	return eachKeyTagName
}

func (eachKeyTagValidator) ValidScopes() sets.Set[Scope] {
	return listTagsValidScopes
}

var (
	validateEachMapKey = types.Name{Package: libValidationPkg, Name: "EachMapKey"}
)

// GetValidations 解析 eachKey 的负载并构造 key 上下文，返回包裹后的校验。
func (ektv eachKeyTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// 注意：列表/映射的指针不被支持，因此这里不应出现指针。
	// NOTE: pointers to lists are not supported, so we should never see a pointer here.
	t := context.Type
	nt := util.NativeType(t)
	if nt.Kind != types.Map {
		return Validations{}, fmt.Errorf("can only be used on map types (%s)", nt.Kind)
	}

	// key 上下文：路径以 (keys) 结尾，Type 为 map 的 key 类型。
	elemContext := Context{
		Scope:          ScopeMapKey,
		Type:           nt.Key,
		Path:           context.Path.Key("(keys)"),
		Member:         nil, // NA for map keys
		ParentPath:     context.Path,
		ParentType:     context.Type,
		StabilityLevel: context.StabilityLevel,
	}

	validations, err := ektv.validator.ExtractTagValidations(elemContext, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	// key 级校验同样不支持生成变量。
	if len(validations.Variables) > 0 {
		return Validations{}, fmt.Errorf("variable generation is not supported")
	}

	result := Validations{
		OpaqueKeyType: validations.OpaqueType,
	}
	result.Comments = append(result.Comments, validations.Comments...)

	if len(validations.Functions) > 0 {
		innerVals, err := ektv.getValidations(t, Validations{Functions: validations.Functions})
		if err != nil {
			return Validations{}, err
		}
		result.Add(innerVals)
	}

	if len(validations.Deferred) > 0 {
		result.AddDeferred(Deferred(ThisContext, func() (Validations, error) {
			resolved := Validations{}
			for _, d := range validations.Deferred {
				inner, err := d.Callback()
				if err != nil {
					return Validations{}, err
				}
				resolved.Add(inner)
			}
			return ektv.getValidations(t, resolved)
		}))
	}

	if result.Empty() {
		return Validations{}, fmt.Errorf("no validation functions found")
	}

	return result, nil
}

// getValidations 把内部校验函数包裹成 EachMapKey 形式。
func (ektv eachKeyTagValidator) getValidations(t *types.Type, validations Validations) (Validations, error) {
	nt := util.NativeType(t)
	wrapped := WrapFunctions(validations, func(vfn FunctionGen, _ DeferredScope) FunctionGen {
		comm := vfn.Comments
		vfn.Comments = nil
		return Function(eachKeyTagName, vfn.Flags, validateEachMapKey, WrapperFunction{Function: vfn, ObjType: nt.Key}).WithComments(comm...)
	})
	return Validations{
		Functions:     wrapped.Functions,
		Deferred:      wrapped.Deferred,
		OpaqueKeyType: validations.OpaqueType,
	}, nil
}

// ForEachKey 返回“对 map 每个 key 施加 fn”的校验。
// ForEachKey returns a validation that applies a function to each key of
// a map.
func ForEachKey(_ *field.Path, t *types.Type, fn FunctionGen) (Validations, error) {
	return globalEachKey.getValidations(t, Validations{Functions: []FunctionGen{fn}})
}

// Docs 返回该标记的文档元数据。
func (ektv eachKeyTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            ektv.TagName(),
		Scopes:         sets.List(ektv.ValidScopes()),
		StabilityLevel: TagStabilityLevelStable,
		Description:    "Declares a validation for each key in a map.",
		Payloads: []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        "The tag to evaluate for each key.",
		}},
		PayloadsType:     codetags.ValueTypeTag,
		PayloadsRequired: true,
	}
	return doc
}

var (
	validatePtrSliceNoNils = types.Name{Package: libValidationPkg, Name: "PtrSliceNoNils"}
)

// PtrSliceNoNils 返回一个合成校验，用于拒绝指针切片中的 nil 元素。
// 它不是由标签驱动的：生成器会为 []*T 字段自动注入该校验，
// 避免后续逐元素校验遇到 nil 而 panic。
// PtrSliceNoNils returns a synthetic validation that rejects nil elements of a
// pointer slice. It is not tag-driven; the generator injects it for []*T fields.
func PtrSliceNoNils(elemType types.Name) FunctionGen {
	return Function("PtrSliceNoNils", ShortCircuit, validatePtrSliceNoNils).WithTypeArgs(elemType)
}
