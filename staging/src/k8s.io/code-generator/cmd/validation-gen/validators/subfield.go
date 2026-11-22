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

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	subfieldTagName = "k8s:subfield"
)

// 注册在全局注册表中；子标签的实际校验通过注入的 TagValidator 递归解析。
func init() {
	RegisterTagValidator(&subfieldTagValidator{})
}

// subfieldTagValidator 把校验标签转发到“指定 JSON 名的直接子字段”上，
// 并负责在父类型上补齐 nil 安全的值提取与 ratcheting 所需的比较函数。
type subfieldTagValidator struct {
	validator              TagValidationExtractor
	processedShortCircuits map[string]bool // 按子字段路径去重，见 extractMemberShortCircuits
}

// Init 注入全局 TagValidator；子标签的解析完全委托给它。
func (stv *subfieldTagValidator) Init(cfg Config) {
	stv.validator = cfg.TagValidator
	stv.processedShortCircuits = make(map[string]bool)
}

// TagName 返回标签名，供注册表分派。
func (*subfieldTagValidator) TagName() string {
	return subfieldTagName
}

// 作用于类型、字段以及列表/映射的元素位置，因为这些位置都可能出现结构体。
var subfieldTagValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)

// ValidScopes 返回该标签允许的作用域集合。
func (*subfieldTagValidator) ValidScopes() sets.Set[Scope] {
	return subfieldTagValidScopes
}

// 运行时库中的 Subfield 包装函数：校验前先安全地取出子字段值。
var (
	validateSubfield = types.Name{Package: libValidationPkg, Name: "Subfield"}
)

// GetValidations 解析 +k8s:subfield(name)=<sub-tag>，把子标签的校验包一层
// nil 安全的字段提取，并在需要 ratcheting 时选择 == 或 DeepEqual。
func (stv *subfieldTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// TODO: Support nested subfields once the generated validation code can
	// prevent nil pointer dereferences by checking optionality or requiredness
	// for intermediate fields.
	for t := tag.ValueTag; t != nil; t = t.ValueTag {
		if t.Name == subfieldTagName {
			return Validations{}, fmt.Errorf("nested +%s tags are unsupported; apply validations directly to the nested type instead", subfieldTagName)
		}
	}

	args := tag.Args
	// This tag can apply to value and pointer fields, as well as typedefs
	// (which should never be pointers). We need to check the concrete type.
	// 剥掉指针和别名后必须是 struct，否则没有“子字段”可访问。
	t := context.Type
	nt := util.NonPointer(util.NativeType(t))
	if nt.Kind != types.Struct {
		return Validations{}, fmt.Errorf("can only be used on struct types: %v", nt.Kind)
	}
	// 参数是 JSON 名，避免生成代码依赖 Go 字段名的改写规则。
	subname := args[0].Value
	submemb := util.GetMemberByJSON(nt, subname)
	if submemb == nil {
		return Validations{}, fmt.Errorf("no field for json name %q", subname)
	}

	// 子上下文模拟“直接位于该字段上”的语义，路径与 parent 都同步调整。
	subContext := Context{
		Scope:          ScopeField,
		Type:           submemb.Type,
		Path:           context.Path.Child(subname),
		Member:         submemb,
		ParentPath:     context.Path,
		ParentType:     context.Type,
		StabilityLevel: context.StabilityLevel,
	}

	// 提取函数的两端都统一成可空类型：父结构体不可空时取地址传入，
	// 子字段不可空时返回指针，保证生成的函数可以安全地检查 nil。
	nilableStructType := context.Type
	if !util.IsNilableType(nilableStructType) {
		nilableStructType = types.PointerTo(nilableStructType)
	}

	nilableFieldType := submemb.Type
	fieldExprPrefix := ""
	if !util.IsNilableType(nilableFieldType) {
		nilableFieldType = types.PointerTo(nilableFieldType)
		fieldExprPrefix = "&"
	}

	// getFn is the function that retrieves the subfield value from the
	// struct.
	getFn := FunctionLiteral{
		Parameters: []ParamResult{{"o", nilableStructType}},
		Results:    []ParamResult{{"", nilableFieldType}},
	}
	getFn.Body = fmt.Sprintf("return %so.%s", fieldExprPrefix, submemb.Name)

	// equivArg compares the correlated elements in the old and new lists, for
	// ratcheting. directComparable selects "==" vs semantic DeepEqual.
	var equivArg any = Identifier(validateSemanticDeepEqual)
	if util.IsDirectComparable(util.NonPointer(util.NativeType(submemb.Type))) {
		// It must be a pointer, since other nilable types are not directly
		// comparable.
		equivArg = Identifier(validateDirectEqual)
	}

	tagValidations, err := stv.validator.ExtractTagValidations(subContext, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	// Only the parent-member short-circuit inheritance depends on opaque-tag
	// globals (see globalOpaqueTypes / globalOpaqueMembers in opaque.go), so
	// defer just that decision until all opaque tags have been registered. The
	// rest of the wrapping is independent and runs eagerly.
	result := Validations{}
	result.AddDeferred(Deferred(ThisContext, func() (Validations, error) {
		// Subfield's own validations (which contain its short-circuits) are added first,
		// and inherited parent member short-circuits are added second. The cohort-based
		// partitioner (sortIntoCohorts) will then sort them so that all short-circuits
		// run first, preserving the relative addition order (subfield short-circuits
		// first, then inherited short-circuits). Finally, non-short-circuits will run.
		var combined Validations
		combined.Add(tagValidations)
		if !isFieldOpaque(context) {
			fieldValidations, err := stv.extractMemberShortCircuits(subContext)
			if err != nil {
				return Validations{}, err
			}
			combined.Add(fieldValidations)
		}

		mapped := WrapFunctions(combined, func(fn FunctionGen, scope DeferredScope) FunctionGen {
			// ParentContext functions (e.g. Union) emit without a cohort.
			if scope == ParentContext {
				return fn
			}
			f := Function(subfieldTagName, fn.Flags, validateSubfield, subname, getFn, equivArg,
				WrapperFunction{Function: fn, ObjType: submemb.Type, PathFragment: "." + subname})
			f.Cohort = subname
			return f
		})

		for i := range mapped.Deferred {
			// The validations belong to the subfield, so re-scope ParentContext
			// (which would target the enclosing struct) to ThisContext.
			if mapped.Deferred[i].Scope == ParentContext {
				mapped.Deferred[i].Scope = ThisContext
			}
		}

		return mapped, nil
	}))
	return result, nil
}

// extractMemberShortCircuits extracts short-circuit validations (e.g. required,
// immutable) from the child member's own comment lines so that the subfield
// validator can inherit them. Each subfield path is processed at most once;
// without this dedup, multiple subfield tags for the same child field would emit
// duplicate short-circuit validations.
func (stv *subfieldTagValidator) extractMemberShortCircuits(subContext Context) (Validations, error) {
	key := subContext.Path.String()
	if stv.processedShortCircuits[key] {
		return Validations{}, nil
	}
	stv.processedShortCircuits[key] = true

	ve, ok := stv.validator.(ValidationExtractor)
	if !ok {
		return Validations{}, fmt.Errorf("validator does not implement ValidationExtractor")
	}
	tags, err := ve.ExtractTags(subContext, subContext.Member.CommentLines)
	if err != nil {
		return Validations{}, err
	}
	memberValidations, err := ve.ExtractValidations(subContext, tags...)
	if err != nil {
		return Validations{}, err
	}
	return copyShortCircuitsAsNonError(memberValidations), nil
}

// Docs 返回标签文档；payload 必须是另一个校验标签。
func (stv *subfieldTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            stv.TagName(),
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(stv.ValidScopes()),
		Description:    "Declares a validation for a subfield of a struct.",
		Args: []TagArgDoc{{
			Description: "<field-json-name>",
			Type:        codetags.ArgTypeString,
			Required:    true,
		}},
		Docs: "The named subfield must be a direct field of the struct, or of an embedded struct.",
		Payloads: []TagPayloadDoc{{
			Description: "<validation-tag>",
			Docs:        "The tag to evaluate for the subfield.",
		}},
		PayloadsType:     codetags.ValueTypeTag,
		PayloadsRequired: true,
	}
}

// copyShortCircuitsAsNonError recursively traverses the validations (and deferred ones)
// and returns a new Validations object containing only the functions that have
// ShortCircuit flag set. The returned functions are also marked as NonError.
func copyShortCircuitsAsNonError(v Validations) Validations {
	res := Validations{}
	for _, fn := range v.Functions {
		if fn.Flags.IsSet(ShortCircuit) {
			fn.Flags |= NonError
			res.AddFunction(fn)
		}
	}
	for _, d := range v.Deferred {
		// deferred 项同样递归过滤：父上下文返回的校验可能包含 short-circuit。
		res.AddDeferred(Deferred(d.Scope, func() (Validations, error) {
			inner, err := d.Callback()
			if err != nil {
				return Validations{}, err
			}
			return copyShortCircuitsAsNonError(inner), nil
		}))
	}
	return res
}
