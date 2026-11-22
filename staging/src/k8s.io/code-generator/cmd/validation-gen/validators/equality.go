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
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	neqTagName = "k8s:neq"
)

// init 注册 neq 校验器；注册必须在 InitGlobalValidator 之前完成。
func init() {
	RegisterTagValidator(neqTagValidator{})
}

// neqTagValidator 实现 +k8s:neq 标记：字段值不得等于给定的不允许值。
type neqTagValidator struct{}

// Init 无状态，无需初始化。
func (neqTagValidator) Init(_ Config) {}

// TagName 返回标记名。
func (neqTagValidator) TagName() string {
	return neqTagName
}

// neqTagValidScopes 列出 neq 可用的位置：
// 类型、字段、列表元素、map 键与 map 值。
var neqTagValidScopes = sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)

// ValidScopes 返回 neq 允许的作用域。
func (neqTagValidator) ValidScopes() sets.Set[Scope] {
	return neqTagValidScopes
}

var (
	neqValidator = types.Name{Package: libValidationPkg, Name: "NEQ"}
)

// GetValidations 校验标记载荷与字段类型是否匹配，并生成一个校验函数调用。
// 载荷类型必须与字段类型一致（string/bool/integer），否则拒绝生成：
// 生成代码里直接比较不同类型的值无法编译。
func (v neqTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	// 先剥掉指针与别名，得到实际参与比较的原生类型。
	t := util.NonPointer(util.NativeType(context.Type))
	if !util.IsDirectComparable(t) {
		return Validations{}, fmt.Errorf("can only be used on comparable types (e.g. string, int, bool), but got %s", rootTypeString(context.Type, t))
	}

	if tag.ValueType == codetags.ValueTypeNone {
		return Validations{}, fmt.Errorf("missing required payload")
	}

	var disallowedValue any
	var err error

	switch {
	case t == types.String:
		if tag.ValueType != codetags.ValueTypeString {
			return Validations{}, fmt.Errorf("type mismatch: field is a string, but payload is of type %s", tag.ValueType)
		}
		disallowedValue = tag.Value
	case t == types.Bool:
		if tag.ValueType != codetags.ValueTypeBool {
			return Validations{}, fmt.Errorf("type mismatch: field is a bool, but payload is of type %s", tag.ValueType)
		}
		disallowedValue, err = util.ParseBool(tag.Value)
		if err != nil {
			return Validations{}, fmt.Errorf("invalid bool value for payload: %w", err)
		}
	case types.IsInteger(t):
		if tag.ValueType != codetags.ValueTypeInt {
			return Validations{}, fmt.Errorf("type mismatch: field is an integer, but payload is of type %s", tag.ValueType)
		}
		disallowedValue, err = util.ParseInt(tag.Value)
		if err != nil {
			return Validations{}, fmt.Errorf("invalid integer value for payload: %w", err)
		}
	default:
		return Validations{}, fmt.Errorf("unsupported type for 'neq' tag: %s", t.Name)
	}

	// 载荷作为常量内联进生成代码；Emission 声明该规则产生的错误类型。
	fn := Function(v.TagName(), DefaultFlags, neqValidator, disallowedValue).
		WithEmits(Emission{field.ErrorTypeInvalid, "neq", ""})
	return Validations{Functions: []FunctionGen{fn}}, nil
}

// Docs 返回 neq 标记的文档，供 --docs 输出。
func (v neqTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:              v.TagName(),
		StabilityLevel:   TagStabilityLevelAlpha,
		Scopes:           sets.List(v.ValidScopes()),
		Description:      "Verifies the field's value is not equal to a specific disallowed value. Supports string, integer, and boolean types.",
		PayloadsRequired: true,
		PayloadsType:     codetags.ValueTypeRaw,
		Payloads: []TagPayloadDoc{{
			Description: "<value>",
			Docs:        "The disallowed value. The parser will infer the type (string, int, bool).",
		}},
	}
}
