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
	// TODO: 等生成的校验代码能通过检查中间字段的可选性/必填性
	// 来避免 nil 指针解引用后，再支持嵌套子字段。
	// for intermediate fields.
	for t := tag.ValueTag; t != nil; t = t.ValueTag {
		if t.Name == subfieldTagName {
			return Validations{}, fmt.Errorf("nested +%s tags are unsupported; apply validations directly to the nested type instead", subfieldTagName)
		}
	}

	args := tag.Args
	// 该标签可用于值字段、指针字段以及 typedef
	// （typedef 不应为指针），因此需检查具体类型。
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

	// getFn 是从结构体中取出子字段值的函数。
	// struct.
	getFn := FunctionLiteral{
		Parameters: []ParamResult{{"o", nilableStructType}},
		Results:    []ParamResult{{"", nilableFieldType}},
	}
	getFn.Body = fmt.Sprintf("return %so.%s", fieldExprPrefix, submemb.Name)

	// equivArg 用于比较新旧列表中对应的元素，
	// ratcheting；directComparable 决定用 "==" 还是语义 DeepEqual。
	var equivArg any = Identifier(validateSemanticDeepEqual)
	if util.IsDirectComparable(util.NonPointer(util.NativeType(submemb.Type))) {
		// 必须是指针类型，因为其他可为 nil 的类型不可直接比较。
		// comparable.
		equivArg = Identifier(validateDirectEqual)
	}

	tagValidations, err := stv.validator.ExtractTagValidations(subContext, *tag.ValueTag)
	if err != nil {
		return Validations{}, err
	}

	// 只有从父成员继承短路校验依赖 opaque 标签的全局信息
	// （见 opaque.go 的 globalOpaqueTypes / globalOpaqueMembers），
	// 因此只把这一步推迟到所有 opaque 标签注册完；
	// 其余包装逻辑相互独立，立即执行。
	result := Validations{}
	result.AddDeferred(Deferred(ThisContext, func() (Validations, error) {
		// 先加入子字段自身的校验（含其短路校验），
		// 再添加从父成员继承的短路校验。随后基于 cohort 的
		// 分区器 sortIntoCohorts 会排序，使所有短路校验先运行，
		// 短路校验先执行，保持加入顺序（子字段短路校验在前，
		// 继承的短路校验在后）。最后运行非短路校验。
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
			// ParentContext 函数（如 Union）不带 cohort 输出。
			if scope == ParentContext {
				return fn
			}
			f := Function(subfieldTagName, fn.Flags, validateSubfield, subname, getFn, equivArg,
				WrapperFunction{Function: fn, ObjType: submemb.Type, PathFragment: "." + subname})
			f.Cohort = subname
			return f
		})

		for i := range mapped.Deferred {
			// 这些校验属于子字段，因此把 ParentContext 重设为
			// ThisContext；否则会作用到外层结构体。
			if mapped.Deferred[i].Scope == ParentContext {
				mapped.Deferred[i].Scope = ThisContext
			}
		}

		return mapped, nil
	}))
	return result, nil
}

// extractMemberShortCircuits 从子成员自身的注释中提取短路校验
// （如 required、immutable），供子字段校验器继承。
// 每条子字段路径至多处理一次；
// 否则同一子字段的多个标签会输出重复的
// 短路校验。
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

// copyShortCircuitsAsNonError 递归遍历校验（含延迟校验），
// 返回一个新的 Validations，仅含设置了 ShortCircuit 标志
// 的函数；返回的函数同时标记为 NonError。
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
