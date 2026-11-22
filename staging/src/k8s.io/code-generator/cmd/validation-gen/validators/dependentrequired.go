/*
Copyright The Kubernetes Authors.

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
	"k8s.io/gengo/v2/parser/tags"
	"k8s.io/gengo/v2/types"
)

const (
	// 两个标签共用同一套实现，差异只在“要求存在”还是“要求不存在”。
	dependentRequiredTagName  = "k8s:dependentRequired"
	dependentForbiddenTagName = "k8s:dependentForbidden"
)

// 运行时校验库中的具体实现（生成的代码会引用这些符号）。
var (
	dependentRequiredValidator  = types.Name{Package: libValidationPkg, Name: "DependentRequired"}
	dependentForbiddenValidator = types.Name{Package: libValidationPkg, Name: "DependentForbidden"}
)

// 两种模式分别注册，因此同一个依赖关系可以同时用 required 和 forbidden 表达。
func init() {
	RegisterTagValidator(dependencyTagValidator{dependencyRequired})
	RegisterTagValidator(dependencyTagValidator{dependencyForbidden})
}

// dependencyTagValidator implements conditional set-ness dependencies between
// sibling fields: when the tagged (trigger) field is set, a named sibling must
// also be set (required mode) or must not be set (forbidden mode).
type dependencyTagValidator struct {
	mode dependencyMode
}

// dependencyMode 区分 required/forbidden，同时复用调用路径。
type dependencyMode string

const (
	dependencyRequired  dependencyMode = dependentRequiredTagName
	dependencyForbidden dependencyMode = dependentForbiddenTagName
)

// Init 无需额外状态，依赖信息全部来自标签参数。
func (dependencyTagValidator) Init(_ Config) {}

// TagName 返回当前模式对应的标签名，注册表据此分派。
func (dtv dependencyTagValidator) TagName() string {
	return string(dtv.mode)
}

// 只允许出现在 struct 字段上：校验需要同时读取触发字段和依赖字段。
var dependencyTagValidScopes = sets.New(ScopeField)

// ValidScopes 返回该标签允许的作用域集合。
func (dependencyTagValidator) ValidScopes() sets.Set[Scope] {
	return dependencyTagValidScopes
}

// GetValidations 定位依赖字段并生成校验函数。
// 返回值以 Deferred 形式挂在父类型上，因为生成的函数需要同时访问两个兄弟字段。
func (dtv dependencyTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	if context.Member == nil {
		return Validations{}, fmt.Errorf("must be used on a struct field")
	}

	parentType := util.NonPointer(util.NativeType(context.ParentType))
	if parentType.Kind != types.Struct {
		return Validations{}, fmt.Errorf("parent type must be a struct (got %s)", parentType.Kind)
	}

	if len(tag.Args) != 1 {
		return Validations{}, fmt.Errorf("expected exactly one argument naming the dependent field, got %d", len(tag.Args))
	}
	// 参数是 JSON 名而非 Go 字段名，生成的校验从不依赖 Go 命名细节。
	dependentJSONName := tag.Args[0].Value

	dependentMember := util.GetMemberByJSON(parentType, dependentJSONName)
	if dependentMember == nil {
		return Validations{}, fmt.Errorf("no sibling field with JSON name %q", dependentJSONName)
	}
	if dependentMember.Name == context.Member.Name {
		return Validations{}, fmt.Errorf("trigger and dependent field must be different (both are %q)", dependentJSONName)
	}

	triggerJSONTag, ok := tags.LookupJSON(*context.Member)
	if !ok || triggerJSONTag.Name == "" {
		return Validations{}, fmt.Errorf("trigger field %q has no JSON name", context.Member.Name)
	}
	triggerJSONName := triggerJSONTag.Name

	// "Is set" has an obvious meaning for these kinds: pointer non-nil,
	// slice/map non-empty, builtin non-zero. We don't restrict what a
	// pointer points to — in practice it's a builtin or a struct.
	for _, m := range []struct {
		role   string
		member *types.Member
	}{{"trigger", context.Member}, {"dependent", dependentMember}} {
		nt := util.NativeType(m.member.Type)
		switch nt.Kind {
		case types.Pointer, types.Map, types.Slice, types.Builtin:
		default:
			return Validations{}, fmt.Errorf("%s field %q has unsupported type kind %s",
				m.role, m.member.Name, nt.Kind)
		}
	}

	tagName := string(dtv.mode)
	var validator types.Name
	var emitType field.ErrorType
	var origin string
	// 错误类型与 origin 决定用户看到的报错语义，必须与模式严格对应。
	switch dtv.mode {
	case dependencyRequired:
		validator, emitType, origin = dependentRequiredValidator, field.ErrorTypeRequired, "dependentRequired"
	case dependencyForbidden:
		validator, emitType, origin = dependentForbiddenValidator, field.ErrorTypeForbidden, "dependentForbidden"
	default:
		panic(fmt.Sprintf("unknown dependency mode: %q", dtv.mode))
	}

	ptrType := types.PointerTo(context.ParentType)
	triggerExtractor := createMemberExtractor(ptrType, context.Member)
	dependentExtractor := createMemberExtractor(ptrType, dependentMember)

	// Emit at the parent — the check needs both sibling fields.
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ParentContext, func() (Validations, error) {
				fn := Function(tagName, DefaultFlags, validator,
					triggerJSONName, triggerExtractor,
					dependentJSONName, dependentExtractor,
				).WithEmits(Emission{
					Type:         emitType,
					Origin:       origin,
					PathFragment: "." + dependentJSONName,
				})
				return Validations{Functions: []FunctionGen{fn}}, nil
			}),
		},
	}, nil
}

// Docs 返回标签的用法文档；两个模式共享参数格式，只是描述与错误语义不同。
func (dtv dependencyTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            dtv.TagName(),
		StabilityLevel: TagStabilityLevelAlpha,
		Scopes:         sets.List(dtv.ValidScopes()),
		Args: []TagArgDoc{{
			Description: "<sibling-field-json-name>",
			Type:        codetags.ArgTypeString,
			Required:    true,
		}},
	}
	switch dtv.mode {
	case dependencyRequired:
		doc.Description = "Indicates that when this field is set, the named sibling field must also be set."
		doc.Docs = "When the tagged field is set (non-nil pointer, non-empty slice/map, or non-zero " +
			"builtin), the named sibling must also be set. Dependencies are one-directional. " +
			"Repeat the tag to require multiple siblings."
	case dependencyForbidden:
		doc.Description = "Indicates that when this field is set, the named sibling field must not be set."
		doc.Docs = "When the tagged field is set (non-nil pointer, non-empty slice/map, or non-zero " +
			"builtin), the named sibling must not be set. Dependencies are one-directional. " +
			"Repeat the tag to forbid multiple siblings."
	default:
		panic(fmt.Sprintf("unknown dependency mode: %q", dtv.mode))
	}
	return doc
}
