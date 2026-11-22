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
	"encoding/json"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	requiredTagName  = "k8s:required"
	optionalTagName  = "k8s:optional"
	forbiddenTagName = "k8s:forbidden"
	defaultTagName   = "default" // TODO: this should eventually be +k8s:default
)

// init 为三种 requiredness 模式各注册一个校验器。
// 它们共享实现，但以不同标记名区分，从而复用分派逻辑。
func init() {
	RegisterTagValidator(requirednessTagValidator{requirednessRequired})
	RegisterTagValidator(requirednessTagValidator{requirednessOptional})
	RegisterTagValidator(requirednessTagValidator{requirednessForbidden})
}

// requirednessTagValidator 实现 required/optional/forbidden 三种必需性模式。
type requirednessTagValidator struct {
	mode requirednessMode
}

// requirednessMode 标识当前校验器对应的标记。
type requirednessMode string

const (
	requirednessRequired  requirednessMode = requiredTagName
	requirednessOptional  requirednessMode = optionalTagName
	requirednessForbidden requirednessMode = forbiddenTagName
)

// Init 无状态，无需初始化。
func (requirednessTagValidator) Init(_ Config) {}

// TagName 返回当前模式对应的标记名。
func (rtv requirednessTagValidator) TagName() string {
	return string(rtv.mode)
}

// requirednessTagValidScopes 限定这些标记只能用于字段：
// 必需性描述的是字段是否被客户端设置，类型层面没有对应语义。
var requirednessTagValidScopes = sets.New(ScopeField)

// ValidScopes 返回允许的作用域。
func (requirednessTagValidator) ValidScopes() sets.Set[Scope] {
	return requirednessTagValidScopes
}

// GetValidations 按模式分派到具体实现。
func (rtv requirednessTagValidator) GetValidations(context Context, _ codetags.Tag) (Validations, error) {
	switch rtv.mode {
	case requirednessRequired:
		return rtv.doRequired(context)
	case requirednessOptional:
		return rtv.doOptional(context)
	case requirednessForbidden:
		return rtv.doForbidden(context)
	}
	panic(fmt.Sprintf("unknown requiredness mode: %q", rtv.mode))
}

var (
	requiredValueValidator   = types.Name{Package: libValidationPkg, Name: "RequiredValue"}
	requiredPointerValidator = types.Name{Package: libValidationPkg, Name: "RequiredPointer"}
	requiredSliceValidator   = types.Name{Package: libValidationPkg, Name: "RequiredSlice"}
	requiredMapValidator     = types.Name{Package: libValidationPkg, Name: "RequiredMap"}
)

// TODO: It might be valuable to have a string payload for when requiredness is
// conditional (e.g. required when <otherfield> is specified).
//
// doRequired 为 +k8s:required 生成校验。
func (rtv requirednessTagValidator) doRequired(context Context) (Validations, error) {
	// 大多数校验器不关心被校验值是值类型还是指针类型，但这个校验器关心。
	// 由于 Go 不支持模板偏特化，这里按类型手动分派到对应的运行时函数。
	emits := Emission{field.ErrorTypeRequired, "", ""}
	switch util.NativeType(context.Type).Kind {
	case types.Slice:
		return Validations{Functions: []FunctionGen{Function(requiredTagName, ShortCircuit, requiredSliceValidator).WithEmits(emits)}}, nil
	case types.Map:
		return Validations{Functions: []FunctionGen{Function(requiredTagName, ShortCircuit, requiredMapValidator).WithEmits(emits)}}, nil
	case types.Pointer:
		return Validations{Functions: []FunctionGen{Function(requiredTagName, ShortCircuit, requiredPointerValidator).WithEmits(emits)}}, nil
	case types.Struct:
		// 不支持对非指针结构体使用 +k8s:required：值类型结构体永远“存在”，
		// 无法区分“未设置”和“设置为零值”。
		// If you encounter this error and believe you have a valid use case
		// for forbiddening a non-pointer struct, please let us know! We need
		// to understand your scenario to determine if we need to adjust
		// this behavior or provide alternative validation mechanisms.
		return Validations{}, fmt.Errorf("non-pointer structs cannot use the %q tag", requiredTagName)
	}
	return Validations{Functions: []FunctionGen{Function(requiredTagName, ShortCircuit, requiredValueValidator).WithEmits(emits)}}, nil
}

var (
	optionalValueValidator   = types.Name{Package: libValidationPkg, Name: "OptionalValue"}
	optionalPointerValidator = types.Name{Package: libValidationPkg, Name: "OptionalPointer"}
	optionalSliceValidator   = types.Name{Package: libValidationPkg, Name: "OptionalSlice"}
	optionalMapValidator     = types.Name{Package: libValidationPkg, Name: "OptionalMap"}
)

// doOptional 为 +k8s:optional 生成校验。
//
// 所有标记都是从 API 客户端视角表达的，而生成代码运行在服务端，
// optional 因此比较微妙。
//
// 标记为 optional 且没有默认值的字段是严格可选的：客户端可以不设置，
// 服务端不会补默认值，消费该字段的代码必须处理“完全没有值”的情况。
//
// 标记为 optional 但有默认值的字段对客户端可选，对服务端却是必需的：
// 客户端可以不设置，但服务端会填默认值，消费代码可以假定它总有值。
//
// 有一个特例必须处理：带默认值的 optional 非指针字段。若默认值不等于该类型
// 零值，零值可用于判断是否需要赋默认值，因此零值必须越界，可按上述规则处理。
// 但若默认值就是零值，则零值显然合法，“optional” 也就失去意义——无法区分
// 客户端未设置（得到零值）与显式设为零值。
//
// TODO: handle default=ref(...)
// TODO: handle manual defaulting
func (rtv requirednessTagValidator) doOptional(context Context) (Validations, error) {
	if hasDefault, zeroDefault, err := rtv.hasZeroDefault(context); err != nil {
		return Validations{}, err
	} else if hasDefault {
		// 值类型 + 零值默认：optional 只是文档说明，无法校验，直接返回注释。
		if !util.IsNilableType(context.Type) && zeroDefault {
			return Validations{Comments: []string{"optional value-type fields with zero-value defaults are purely documentation"}}, nil
		}
		// 有默认值时按 required 处理：服务端最终总会看到非零值。
		validations, err := rtv.doRequired(context)
		if err != nil {
			return Validations{}, err
		}
		for i, fn := range validations.Functions {
			validations.Functions[i] = fn.WithComment("optional fields with default values are effectively required")
		}
		return validations, nil
	}

	// 大多数校验器不关心值类型还是指针类型，但这个校验器关心。
	// 由于 Go 不支持模板偏特化，这里按类型手动分派。
	// NonError 标志表示这些函数只用于短路，不产生错误。
	switch util.NativeType(context.Type).Kind {
	case types.Slice:
		return Validations{Functions: []FunctionGen{Function(optionalTagName, ShortCircuit|NonError, optionalSliceValidator)}}, nil
	case types.Map:
		return Validations{Functions: []FunctionGen{Function(optionalTagName, ShortCircuit|NonError, optionalMapValidator)}}, nil
	case types.Pointer:
		return Validations{Functions: []FunctionGen{Function(optionalTagName, ShortCircuit|NonError, optionalPointerValidator)}}, nil
	case types.Struct:
		// 不支持对非指针结构体使用 +k8s:optional，原因同 required。
		// If you encounter this error and believe you have a valid use case
		// for forbiddening a non-pointer struct, please let us know! We need
		// to understand your scenario to determine if we need to adjust
		// this behavior or provide alternative validation mechanisms.
		return Validations{}, fmt.Errorf("non-pointer structs cannot use the %q tag", optionalTagName)
	}
	return Validations{Functions: []FunctionGen{Function(optionalTagName, ShortCircuit|NonError, optionalValueValidator)}}, nil
}

// hasZeroDefault 返回字段是否有默认值，以及该默认值是否等于字段类型的零值。
func (rtv requirednessTagValidator) hasZeroDefault(context Context) (bool, bool, error) {
	// 该校验器只作用于字段，因此 Member 必定有效。
	tagsByName, err := gengo.ExtractFunctionStyleCommentTags("+", []string{defaultTagName}, context.Member.CommentLines)
	if err != nil {
		return false, false, fmt.Errorf("failed to read tags: %w", err)
	}

	tags, hasDefault := tagsByName[defaultTagName]
	if !hasDefault {
		return false, false, nil
	}
	if len(tags) == 0 {
		return false, false, fmt.Errorf("+default tag with no value")
	}
	if len(tags) > 1 {
		return false, false, fmt.Errorf("+default tag with multiple values: %q", tags)
	}

	payload := tags[0].Value
	var defaultValue any
	// 默认值以 JSON 形式书写，因此按 JSON 解析以便与零值比较。
	if err := json.Unmarshal([]byte(payload), &defaultValue); err != nil {
		return false, false, fmt.Errorf("failed to parse default value %q: %w", payload, err)
	}
	if defaultValue == nil {
		return false, false, fmt.Errorf("failed to parse default value %q: unmarshalled to nil", payload)
	}

	// 对可空类型（指针、切片、map、interface），调用方忽略 zeroDefault，
	// 一律把“带默认值的字段”视为有效 required，因此跳过零值比较。
	if util.IsNilableType(context.Type) {
		return true, false, nil
	}

	t := util.NonPointer(util.NativeType(context.Type))
	zero, found := typeZeroValue[t.String()]
	if !found {
		return false, false, fmt.Errorf("unknown zero-value for type %s", t.String())
	}

	return true, reflect.DeepEqual(defaultValue, zero), nil
}

// 以下零值表从 defaulter-gen 复制而来，用于判断 +default 是否为零值。
// TODO: move this back to gengo as Type.ZeroValue()?
var typeZeroValue = map[string]any{
	"uint":        0.,
	"uint8":       0.,
	"uint16":      0.,
	"uint32":      0.,
	"uint64":      0.,
	"int":         0.,
	"int8":        0.,
	"int16":       0.,
	"int32":       0.,
	"int64":       0.,
	"byte":        0.,
	"float64":     0.,
	"float32":     0.,
	"bool":        false,
	"time.Time":   "",
	"string":      "",
	"integer":     0.,
	"number":      0.,
	"boolean":     false,
	"[]byte":      "", // base64 encoded characters
	"interface{}": interface{}(nil),
	"any":         interface{}(nil),
}

var (
	forbiddenValueValidator   = types.Name{Package: libValidationPkg, Name: "ForbiddenValue"}
	forbiddenPointerValidator = types.Name{Package: libValidationPkg, Name: "ForbiddenPointer"}
	forbiddenSliceValidator   = types.Name{Package: libValidationPkg, Name: "ForbiddenSlice"}
	forbiddenMapValidator     = types.Name{Package: libValidationPkg, Name: "ForbiddenMap"}
)

// TODO: It might be valuable to have a string payload for when forbidden is
// conditional (e.g. forbidden when <option> is disabled).
//
// doForbidden 为 +k8s:forbidden 生成校验。
func (requirednessTagValidator) doForbidden(context Context) (Validations, error) {
	// Forbidden 比较特殊：每个分支都生成两个互为相反的检查。
	// 若字段未通过 forbidden 检查，则短路，不再运行 optional 检查。
	// 若通过 forbidden 检查，说明字段必须未被指定，于是会“失败” optional
	// 检查并短路（但不报错）。为什么要这样？例如，这能防止后续校验
	// 在 nil 指针上运行。
	// optional* 系列带 NonError 标志（只短路、不产生错误），因此没有 Emission。
	forbids := Emission{field.ErrorTypeForbidden, "", ""}
	switch util.NativeType(context.Type).Kind {
	case types.Slice:
		return Validations{
			Functions: []FunctionGen{
				Function(forbiddenTagName, ShortCircuit, forbiddenSliceValidator).WithEmits(forbids),
				Function(forbiddenTagName, ShortCircuit|NonError, optionalSliceValidator),
			},
		}, nil
	case types.Map:
		return Validations{
			Functions: []FunctionGen{
				Function(forbiddenTagName, ShortCircuit, forbiddenMapValidator).WithEmits(forbids),
				Function(forbiddenTagName, ShortCircuit|NonError, optionalMapValidator),
			},
		}, nil
	case types.Pointer:
		return Validations{
			Functions: []FunctionGen{
				Function(forbiddenTagName, ShortCircuit, forbiddenPointerValidator).WithEmits(forbids),
				Function(forbiddenTagName, ShortCircuit|NonError, optionalPointerValidator),
			},
		}, nil
	case types.Struct:
		// 不支持对非指针结构体使用 +k8s:forbidden，原因同 required。
		// If you encounter this error and believe you have a valid use case
		// for forbiddening a non-pointer struct, please let us know! We need
		// to understand your scenario to determine if we need to adjust
		// this behavior or provide alternative validation mechanisms.
		return Validations{}, fmt.Errorf("non-pointer structs cannot use the %q tag", forbiddenTagName)
	}
	return Validations{
		Functions: []FunctionGen{
			Function(forbiddenTagName, ShortCircuit, forbiddenValueValidator).WithEmits(forbids),
			Function(forbiddenTagName, ShortCircuit|NonError, optionalValueValidator),
		},
	}, nil
}

// Docs 返回当前模式对应标记的文档。
func (rtv requirednessTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:    rtv.TagName(),
		Scopes: sets.List(rtv.ValidScopes()),
	}

	switch rtv.mode {
	case requirednessRequired:
		doc.StabilityLevel = TagStabilityLevelStable
		doc.Description = "Indicates that a field must be specified by clients."
	case requirednessOptional:
		doc.StabilityLevel = TagStabilityLevelStable
		doc.Description = "Indicates that a field is optional to clients."
	case requirednessForbidden:
		doc.StabilityLevel = TagStabilityLevelBeta
		doc.Description = "Indicates that a field may not be specified."
	default:
		panic(fmt.Sprintf("unknown requiredness mode: %q", rtv.mode))
	}

	return doc
}
