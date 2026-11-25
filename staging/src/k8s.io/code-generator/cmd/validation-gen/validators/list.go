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
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	listTypeTagName     = "k8s:listType"
	ListMapKeyTagName   = "k8s:listMapKey"
	uniqueTagName       = "k8s:unique"
	customUniqueTagName = "k8s:customUnique"
)

// globalListMeta 在列表相关的多个校验器之间共享，按字段或类型的路径索引。
// 之所以共享：listType、listMapKey、unique 可能分散在多条注释中，
// 需要先累积元数据，最后统一生成校验函数。
var globalListMeta = map[string]*listMetadata{} // keyed by the field or type path

func init() {
	// 注册四个会累积列表元数据的标签校验器；它们本身不产生校验代码，
	// 只负责填充 globalListMeta。
	// Accumulate list metadata via tags.
	RegisterTagValidator(listTypeTagValidator{byPath: globalListMeta})
	RegisterTagValidator(listMapKeyTagValidator{byPath: globalListMeta})
	RegisterTagValidator(uniqueTagValidator{byPath: globalListMeta})
	RegisterTagValidator(customUniqueTagValidator{byPath: globalListMeta})
}

// This applies to all tags in this file.
var listTagsValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)

// listOwnership 描述列表的所有权语义（是否允许共享/合并）。
type listOwnership string

const (
	ownershipSingle listOwnership = "single" // from listType=atomic
	ownershipShared listOwnership = "shared" // from listType=set/map
)

// listSemantic 描述列表的语义类型，决定是否做唯一性校验。
type listSemantic string

const (
	semanticAtomic listSemantic = "atomic" // No uniqueness check
	semanticSet    listSemantic = "set"    // uniqueness check
	semanticMap    listSemantic = "map"    // uniqueness check based on key(s)
)

// listMetadata 汇总一个 set/map 语义列表的信息。
type listMetadata struct {
	ownership  listOwnership // For now we don't use it for generation.
	semantic   listSemantic
	keyMembers []*types.Member // For semantic == map.
	keyNames   []string        // For semantic == map.

	// customUnique 表示该列表设置了 k8s:customUnique，
	// 因而禁用生成唯一性校验（由手写校验负责）。
	customUnique   bool
	stabilityLevel ValidationStabilityLevel
}

// check 校验列表标签之间的基本约束：listMapKey 必须搭配 map 语义，
// map 语义必须有 key，且 listType 必须显式声明。
func (lm *listMetadata) check() error {
	// Check some fundamental constraints on list tags.

	// If we have listMapKey but no map semantics, that's an error
	if len(lm.keyMembers) > 0 && lm.semantic != semanticMap {
		return fmt.Errorf("found listMapKey without listType=map or unique=map")
	}

	// If we have map semantics but no keys, that's an error
	if lm.semantic == semanticMap && len(lm.keyMembers) == 0 {
		return fmt.Errorf("found listType=map or unique=map without listMapKey")
	}

	// listType is mandatory.
	if lm.ownership == "" {
		return fmt.Errorf("listType must be specified - use listType=atomic, listType=set, or listType=map")
	}

	return nil
}

// makeListMapMatchFunc 生成一个按 list-map key 字段比较两个元素的函数字面量。
// 指针 key 的特殊处理是必要的：nil 与 nil 视为相等，只有双方非 nil 时才解引用比较。
func (lm *listMetadata) makeListMapMatchFunc(t *types.Type) FunctionLiteral {
	if lm.semantic != semanticMap {
		panic("makeListMapMatchFunc called on a non-map list")
	}
	// If no keys are defined, we will throw a good error later.

	// 两个参数都取非指针类型，保证生成的比较函数对 null 元素也有明确语义。
	matchFn := FunctionLiteral{
		Parameters: []ParamResult{{"a", types.PointerTo(util.NonPointer(t))}, {"b", types.PointerTo(util.NonPointer(t))}},
		Results:    []ParamResult{{"", types.Bool}},
	}
	buf := strings.Builder{}
	buf.WriteString("return ")

	// 多个 key 之间是逻辑与：所有 key 字段都相等才算同一个元素。
	for i, memb := range lm.keyMembers {
		if i > 0 {
			buf.WriteString(" && ")
		}
		fldName := memb.Name

		if memb.Type.Kind == types.Pointer {
			// Dereference pointers for comparison.
			// This is tricky because they could be nil.
			// Two keys are equal if all their fields are equal.
			// For pointer fields, that means either both are nil,
			// or neither is nil and the pointed-to values are equal.
			buf.WriteString(fmt.Sprintf("((a.%s == nil && b.%s == nil) || (a.%s != nil && b.%s != nil && *a.%s == *b.%s))", fldName, fldName, fldName, fldName, fldName, fldName))
		} else {
			buf.WriteString(fmt.Sprintf("a.%s == b.%s", fldName, fldName))
		}
	}
	matchFn.Body = buf.String()
	return matchFn
}

// listTypeTagValidator 处理 +k8s:listType 标签，记录列表所有权与语义。
type listTypeTagValidator struct {
	byPath map[string]*listMetadata
}

// Init 是该校验器的空初始化钩子。
func (listTypeTagValidator) Init(Config) {}

// TagName 返回该校验器处理的标签名。
func (listTypeTagValidator) TagName() string {
	return listTypeTagName
}

// ValidScopes 返回标签允许出现的上下文范围。
func (listTypeTagValidator) ValidScopes() sets.Set[Scope] {
	return listTagsValidScopes
}

// GetValidations 校验标签值并写入列表元数据，最后通过 Deferred 在
// 所有列表标签处理完后统一生成校验函数。
func (lttv listTypeTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// NOTE: pointers to lists are not supported, so we should never see a pointer here.
	t := util.NativeType(context.Type)
	if t.Kind != types.Slice && t.Kind != types.Array {
		return Validations{}, fmt.Errorf("can only be used on list types (%s)", t.Kind)
	}

	// 同一路径可能由多条标签共同描述，取已有元数据而非覆盖。
	lm := lttv.byPath[context.Path.String()]
	if lm == nil {
		lm = &listMetadata{}
		lttv.byPath[context.Path.String()] = lm
	}
	lm.stabilityLevel = context.StabilityLevel
	// listType 只能声明一次；重复声明会让语义不确定。
	if lm.ownership != "" {
		return Validations{}, fmt.Errorf("listType cannot be specified more than once")
	}

	switch tag.Value {
	case "atomic":
		lm.ownership = ownershipSingle
		// Do not overwrite a more specific semantic from uniqueTagValidator
		if lm.semantic == "" {
			lm.semantic = semanticAtomic
		}
	case "set":
		lm.ownership = ownershipShared
		// 若 uniqueTagValidator 已针对 `unique=set` 或 `unique=map` 运行过，
		// lm.semantic 就是非空且非 atomic 的。
		// 同时声明 listType=set 与 unique=set 是冗余配置。
		if lm.semantic != "" && lm.semantic != semanticAtomic {
			return Validations{}, fmt.Errorf("unique tag is redundant for listType=%q", tag.Value)
		}
		lm.semantic = semanticSet
	case "map":
		lm.ownership = ownershipShared
		// If uniqueTagValidator has run for `unique=set` or `unique=map`,
		// lm.semantic will be non-empty and non-atomic.
		if lm.semantic != "" && lm.semantic != semanticAtomic {
			return Validations{}, fmt.Errorf("unique tag is redundant for listType=%q", tag.Value)
		}
		// map 语义依赖元素的字段作为 key，因此只能是结构体列表。
		if util.NonPointer(util.NativeType(t.Elem)).Kind != types.Struct {
			return Validations{}, fmt.Errorf("only lists of structs can be list-maps")
		}
		lm.semantic = semanticMap
	default:
		return Validations{}, fmt.Errorf("unknown list type %q", tag.Value)
	}

	// 延迟到所有列表标签都处理完毕，再基于完整元数据生成唯一性校验。
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ThisContext, func() (Validations, error) {
				return getListValidations(lttv.byPath, context)
			}),
		},
	}, nil
}

// Docs 描述 listType 标签的稳定性、作用域与负载格式。
func (lttv listTypeTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            lttv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(lttv.ValidScopes()),
		Description:    "Declares a list field's semantic type and ownership behavior. atomic: single ownership, set: shared ownership with uniqueness, map: shared ownership with key-based uniqueness.",
		Payloads: []TagPayloadDoc{{
			Description: "<type>",
			Docs:        "atomic | map | set",
		}},
		PayloadsType:     codetags.ValueTypeString,
		PayloadsRequired: true,
	}
	return doc
}

// listMapKeyTagValidator 处理 +k8s:listMapKey 标签，记录 map 语义列表的 key 字段。
// 它本身不生成校验，只补齐 listTypeTagValidator 所需的 key 信息。
type listMapKeyTagValidator struct {
	byPath map[string]*listMetadata
}

// Init 是该校验器的空初始化钩子。
func (listMapKeyTagValidator) Init(Config) {}

// TagName 返回该校验器处理的标签名。
func (listMapKeyTagValidator) TagName() string {
	return ListMapKeyTagName
}

// ValidScopes 返回标签允许出现的上下文范围。
func (listMapKeyTagValidator) ValidScopes() sets.Set[Scope] {
	return listTagsValidScopes
}

// GetValidations 校验 key 字段存在且类型可作为 key，然后累积到元数据。
func (lmktv listMapKeyTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// NOTE: pointers to lists are not supported, so we should never see a pointer here.
	t := util.NativeType(context.Type)
	if t.Kind != types.Slice && t.Kind != types.Array {
		return Validations{}, fmt.Errorf("can only be used on list types (%s)", t.Kind)
	}
	structT := util.NonPointer(util.NativeType(t.Elem))
	if structT.Kind != types.Struct {
		return Validations{}, fmt.Errorf("only lists of structs can be list-maps")
	}

	var memb *types.Member
	// 标签值使用 JSON 字段名，而非 Go 字段名，以便与序列化后的 key 对应。
	if m := util.GetMemberByJSON(structT, tag.Value); m == nil {
		return Validations{}, fmt.Errorf("no field for JSON name %q", tag.Value)
	} else {
		keyType := m.Type
		// 指针 key 合法，只需检查其元素类型是否为基本类型。
		if keyType.Kind == types.Pointer {
			keyType = keyType.Elem
		}
		if util.NativeType(keyType).Kind != types.Builtin {
			return Validations{}, fmt.Errorf("only primitive types and pointers to primitive types can be list-map keys, not %s", m.Type.String())
		}
		memb = m
	}

	lm := lmktv.byPath[context.Path.String()]
	if lm == nil {
		lm = &listMetadata{}
		lmktv.byPath[context.Path.String()] = lm
	}
	// 支持复合 key：重复标签会按声明顺序追加多个 key 字段。
	lm.keyMembers = append(lm.keyMembers, memb)
	lm.keyNames = append(lm.keyNames, tag.Value)

	// Validations will be generated by listTypeTagValidator. This validator just computes the keys required for list-map.
	return Validations{}, nil
}

// Docs 描述 listMapKey 标签的稳定性、作用域与负载格式。
func (lmktv listMapKeyTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            lmktv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(lmktv.ValidScopes()),
		Description:    "Declares a named sub-field of a list's value-type to be part of the list-map key.",
		Payloads: []TagPayloadDoc{{
			Description: "<field-json-name>",
			Docs:        "The name of the field.",
		}},
		PayloadsType:     codetags.ValueTypeString,
		PayloadsRequired: true,
	}
	return doc
}

// uniqueTagValidator 处理 +k8s:unique 标签：为 listType=atomic 的列表补充唯一性语义，
// 或独立声明 set/map 语义。
type uniqueTagValidator struct {
	byPath map[string]*listMetadata
}

// Init 是该校验器的空初始化钩子。
func (uniqueTagValidator) Init(Config) {}

// TagName 返回该校验器处理的标签名。
func (uniqueTagValidator) TagName() string {
	return uniqueTagName
}

// ValidScopes 返回标签允许出现的上下文范围。
func (uniqueTagValidator) ValidScopes() sets.Set[Scope] {
	return listTagsValidScopes
}

// GetValidations 校验 unique 与 listType 的组合合法性并更新元数据。
func (utv uniqueTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// NOTE: pointers to lists are not supported, so we should never see a pointer here.
	t := util.NativeType(context.Type)
	if t.Kind != types.Slice && t.Kind != types.Array {
		return Validations{}, fmt.Errorf("can only be used on list types (%s)", t.Kind)
	}

	lm := utv.byPath[context.Path.String()]
	if lm == nil {
		lm = &listMetadata{}
		utv.byPath[context.Path.String()] = lm
	}

	lm.stabilityLevel = context.StabilityLevel

	// 若 listType 已经运行并设置了非 atomic 的归属，这里就算错误：
	// set/map 已隐含唯一性，再写 unique 只是重复。
	if lm.ownership != "" && lm.ownership != ownershipSingle {
		return Validations{}, fmt.Errorf("unique tag may not be used with listType=set or listType=map")
	}

	// 同一语义只能声明一次，避免多标签互相矛盾。
	if lm.semantic != "" && lm.semantic != semanticAtomic {
		return Validations{}, fmt.Errorf("unique tag cannot be specified more than once")
	}

	switch tag.Value {
	case "set":
		lm.semantic = semanticSet
	case "map":
		// map 唯一性依赖元素字段，因此只能是结构体列表。
		if util.NonPointer(util.NativeType(t.Elem)).Kind != types.Struct {
			return Validations{}, fmt.Errorf("only lists of structs can be list-maps")
		}
		lm.semantic = semanticMap
	default:
		return Validations{}, fmt.Errorf("unknown unique type %q", tag.Value)
	}

	// 与 listType 一样延迟到元数据完整后再生成校验。
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ThisContext, func() (Validations, error) {
				return getListValidations(utv.byPath, context)
			}),
		},
	}, nil
}

// Docs 描述 unique 标签的稳定性、作用域与负载格式。
func (utv uniqueTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            utv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(utv.ValidScopes()),
		Description:    "Declares that a list field's elements are unique. This tag can be used with listType=atomic to add uniqueness constraints, or independently to specify uniqueness semantics.",
		Payloads: []TagPayloadDoc{{
			Description: "<type>",
			Docs:        "map | set",
		}},
		PayloadsType:     codetags.ValueTypeString,
		PayloadsRequired: true,
	}
	return doc
}

// customUniqueTagValidator 处理 +k8s:customUnique：声明唯一性由手写校验实现，
// 从而关闭生成器为该列表生成唯一性检查。
type customUniqueTagValidator struct {
	byPath map[string]*listMetadata
}

// Init 是该校验器的空初始化钩子。
func (customUniqueTagValidator) Init(Config) {}

// TagName 返回该校验器处理的标签名。
func (customUniqueTagValidator) TagName() string {
	return customUniqueTagName
}

// ValidScopes 返回标签允许出现的上下文范围。
func (customUniqueTagValidator) ValidScopes() sets.Set[Scope] {
	return listTagsValidScopes
}

// GetValidations 只设置 customUnique 标志，不直接产生校验。
func (cutv customUniqueTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// NOTE: pointers to lists are not supported, so we should never see a pointer here.
	t := util.NativeType(context.Type)
	if t.Kind != types.Slice && t.Kind != types.Array {
		return Validations{}, fmt.Errorf("can only be used on list types (%s)", t.Kind)
	}

	lm := cutv.byPath[context.Path.String()]
	if lm == nil {
		lm = &listMetadata{}
		cutv.byPath[context.Path.String()] = lm
	}

	lm.customUnique = true

	// Configures metadata, Validations are emitted by the ListValidator.
	return Validations{}, nil
}

// Docs 描述 customUnique 标签的稳定性、作用域与负载格式。
func (cutv customUniqueTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            cutv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(cutv.ValidScopes()),
		Description:    "Indicates that uniqueness validation for this list is implemented via custom, handwritten validation. This disables generation of uniqueness validation for this list.",
		Payloads:       nil,
	}
	return doc
}

// 生成代码时引用的 apimachinery 运行时校验函数。
var (
	validateValSliceUnique    = types.Name{Package: libValidationPkg, Name: "ValSliceUnique"}
	validatePtrSliceUnique    = types.Name{Package: libValidationPkg, Name: "PtrSliceUnique"}
	validateSemanticDeepEqual = types.Name{Package: libValidationPkg, Name: "SemanticDeepEqual"}
	validateDirectEqual       = types.Name{Package: libValidationPkg, Name: "DirectEqual"}
)

// getListMeta 是查找给定上下文对应列表元数据的辅助函数。
func getListMeta(byPath map[string]*listMetadata, context Context) (*listMetadata, error) {
	// 先查字段自身的元数据。
	// Look up the list metadata which is defined on this field.
	lm := byPath[context.Path.String()]

	// NOTE: We don't really support list-of-list or map-of-list, so this does
	// not consider the case of ScopeListVal or ScopeMapVal. If we want to
	// support those, we need to look at this and make sure the paths work the
	// way we need.
	if context.Scope == ScopeField {
		// 字段本身没有时，回退到其类型上声明的元数据。
		// If this is a field, look up the list metadata for the type.
		tm := byPath[context.Type.String()]
		// 字段与类型同时声明元数据会让语义冲突，直接报错。
		if lm != nil && tm != nil {
			return nil, fmt.Errorf("found list metadata for both a field and its type: %s", context.Path)
		}
		if tm != nil {
			return tm, nil
		}
		// TODO(thockin): enable this once the whole codebase is converted or
		// if we only run against fields which are opted-in.
		// if lm == nil && tm == nil {
		//       return Validations{}, fmt.Errorf("found a list field without list metadata")
		// }
	}
	return lm, nil
}

// 记录已经处理过的项：去重表；同一个路径会因多个 key 或 listType+listMapKey 组合被多次触发延迟生成，
// 但唯一性校验函数只应生成一次。
var listsGenerated = map[string]bool{}

// 基于累积下来的 list 元数据收尾。
// getListValidations 根据累积的元数据生成 set/map 语义的唯一性校验函数。
func getListValidations(byPath map[string]*listMetadata, context Context) (Validations, error) {
	// 非列表类型不适用，静默跳过。
	nt := util.NativeType(context.Type)
	if nt.Kind != types.Slice && nt.Kind != types.Array {
		return Validations{}, nil
	}

	lm, err := getListMeta(byPath, context)
	if err != nil {
		return Validations{}, err
	}

	if lm == nil {
		// If we don't have metadata for this field, we might have it for the
		// field's type.
		return Validations{}, nil
	}

	// If we have processed this path before, we don't want to do it again.
	// This could be because there are multiple keys or even just a
	// listType=map and listMapKey=field.
	if p := context.Path.String(); listsGenerated[p] {
		return Validations{}, nil
	}
	listsGenerated[context.Path.String()] = true

	// Do this after the above - if we only get one error, the one(s) above
	// this are more important.
	if err := lm.check(); err != nil {
		return Validations{}, err
	}
	result := Validations{}
	if lm.customUnique {
		// Uniqueness validation is disabled in generated validation for this list.
		// It would defer to handwritten validation to check the uniqueness.
		result.AddComment("Uniqueness validation is implemented via custom, handwritten validation")
		return result, nil
	}

	// Generate uniqueness checks for lists with higher-order semantics.
	if lm.semantic == semanticSet {
		// Only compare primitive values when possible. Slices and maps are not
		// comparable, and structs might hold pointer fields, which are directly
		// comparable but not what we need.
		//
		// 可直接比较的元素用 DirectEqual 更快；否则退回语义深比较。
		matchArg := validateSemanticDeepEqual
		if util.IsDirectComparable(util.NonPointer(util.NativeType(nt.Elem))) {
			matchArg = validateDirectEqual
		}
		// 指针元素需要解引用后再比较，因此使用 PtrSliceUnique。
		validateFunc := validateValSliceUnique
		if nt.Elem.Kind == types.Pointer {
			validateFunc = validatePtrSliceUnique
		}
		comment := "lists with set semantics require unique values"
		f := Function("listValidator", DefaultFlags, validateFunc, Identifier(matchArg)).
			WithComment(comment).
			WithEmits(Emission{field.ErrorTypeDuplicate, "", "[*]"})
		// 仅当标签显式指定了稳定性级别时才覆盖默认级别。
		if lm.stabilityLevel != "" {
			f = f.WithStabilityLevel(lm.stabilityLevel)
		}
		result.AddFunction(f)
	}
	if lm.semantic == semanticMap {
		// TODO: There are some fields which are declared as maps which do not
		// enforce uniqueness in manual validation. Those either need to not be
		// maps or we need to allow types to opt-out from this validation.  SSA
		// is also not able to handle these well.
		// map 语义的比较函数由 key 字段拼装而成。
		matchArg := lm.makeListMapMatchFunc(nt.Elem)
		validateFunc := validateValSliceUnique
		if nt.Elem.Kind == types.Pointer {
			validateFunc = validatePtrSliceUnique
		}
		comment := "lists with map semantics require unique keys"

		f := Function("listValidator", DefaultFlags, validateFunc, matchArg).
			WithComment(comment).
			WithStabilityLevel(lm.stabilityLevel).
			WithEmits(Emission{field.ErrorTypeDuplicate, "", "[*]"})
		result.AddFunction(f)
	}

	return result, nil
}
