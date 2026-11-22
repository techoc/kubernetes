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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

var zeroOrOneOfUnionValidator = types.Name{Package: libValidationPkg, Name: "ZeroOrOneOfUnion"}
var zeroOrOneOfVariablePrefix = "zeroOrOneOfMembershipFor"

// init 注册 zeroOrOneOf 成员校验器。
// zeroOrOneOf union 由多个标记共同描述，它们需要共享信息：
// 标记写在结构体字段上，但校验实际作用于结构体本身。
func init() {
	// zeroOrOneOf union 由多个需要互相共享信息的标记组成。
	// 标记位于结构体字段上，但校验实际针对结构体本身。
	shared := map[string]unions{}
	RegisterTagValidator(zeroOrOneOfMemberTagValidator{shared})
}

// getZeroOrOneOfValidations 在成员标记收集完毕后生成 union 校验。
func getZeroOrOneOfValidations(shared map[string]unions, context Context) (Validations, error) {
	// union 以 ParentPath 为键（字段作用域），其他作用域则用 Path。
	structPath := context.ParentPath.String()

	// Gengo 不把结构体定义当作别名，这与直觉不一致但基本不会改变。
	// 因此这里并非真的需要处理，但为稳妥起见仍提取尽可能具体的类型。
	if k := util.NonPointer(util.NativeType(context.ParentType)).Kind; k != types.Struct && k != types.Slice {
		return Validations{}, nil
	}

	unions := shared[structPath]
	if len(unions) == 0 {
		return Validations{}, nil
	}
	// 消费后删除，避免同一路径被重复生成。
	delete(shared, structPath)

	result, err := processUnionValidations(context.ParentPath, context.ParentType, unions, zeroOrOneOfVariablePrefix,
		zeroOrOneOfMemberTagName, zeroOrOneOfUnionValidator, types.Name{},
		Emission{field.ErrorTypeInvalid, "zeroOrOneOf", ""})
	return result, err
}

const (
	// 该标记只应作用于列表元素类型，绝不直接用于结构体字段。
	// 若用于结构体字段，“or one of” 行为在当下即被冻结、永远无法扩展。
	// 为什么？旧版本客户端无法区分“没有指定任何成员”与“指定了它不认识的字段”。
	zeroOrOneOfMemberTagName = "k8s:zeroOrOneOfMember"
)

// zeroOrOneOfMemberTagValidator 处理 +k8s:zeroOrOneOfMember。
type zeroOrOneOfMemberTagValidator struct {
	shared map[string]unions
}

// Init 无状态，无需初始化。
func (zeroOrOneOfMemberTagValidator) Init(_ Config) {}

// TagName 返回标记名。
func (zeroOrOneOfMemberTagValidator) TagName() string {
	return zeroOrOneOfMemberTagName
}

// ValidScopes 返回允许的作用域：字段与列表元素。
func (zeroOrOneOfMemberTagValidator) ValidScopes() sets.Set[Scope] {
	return sets.New(ScopeField, ScopeListVal)
}

// GetValidations 记录成员信息，并注册延迟校验（等父上下文收集完所有成员）。
func (zmtv zeroOrOneOfMemberTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	err := processMemberValidations(zmtv.shared, context, tag)
	if err != nil {
		return Validations{}, err
	}
	return Validations{
		Deferred: []DeferredGen{
			Deferred(ParentContext, func() (Validations, error) {
				return getZeroOrOneOfValidations(zmtv.shared, context)
			}),
		},
	}, nil
}

// Docs 返回 zeroOrOneOf 成员标记的文档。
func (zmtv zeroOrOneOfMemberTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            zmtv.TagName(),
		Scopes:         sets.List(zmtv.ValidScopes()),
		StabilityLevel: TagStabilityLevelStable,
		Description:    "Indicates that this field is a member of a zero-or-one-of union.",
		Docs:           "A zero-or-one-of union allows at most one member to be set. Unlike regular unions, having no members set is valid.",
		Warning:        "This tag should only be used on sets of list items, and never on struct fields directly.",
		Args: []TagArgDoc{{
			Name:        "union",
			Description: "<string>",
			Docs:        "the name of the union, if more than one exists",
			Type:        codetags.ArgTypeString,
		}, {
			Name:        "memberName",
			Description: "<string>",
			Docs:        "the custom member name for this member",
			Default:     "the field's name",
			Type:        codetags.ArgTypeString,
		}},
	}
}
