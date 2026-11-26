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

// Package dynamic 实现无 schema 情况下 CEL 表达式中的 Object{} 字面量。
//
// 当 MutatingAdmissionPolicy 等特性尚未绑定具体类型时，用本包提供的动态 Object
// 类型让表达式仍能构造结构化值，并在求值后再做字段路径与类型名一致性校验。
package dynamic

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"google.golang.org/protobuf/types/known/structpb"
)

// ObjectType 是在没有对象 schema 信息时，用于编译 CEL 表达式的 Object 类型实现。
// 它为 CEL 表达式提供 Object{} 类型构造器。
// 为 CEL 表达式提供 Object{} 类型构造器。
//
// 该类型不校验字段名与类型，任何字段都按 DynType 处理；
// 严格的类型名检查推迟到求值后的 CheckTypeNamesMatchFieldPathNames。
type ObjectType struct {
	objectType *types.Type
}

func (o *ObjectType) HasTrait(trait int) bool {
	return o.objectType.HasTrait(trait)
}

// TypeName 返回该 ObjectType 的名字。
func (o *ObjectType) TypeName() string {
	return o.objectType.TypeName()
}

// Val 根据给定字段返回一个实例。
func (o *ObjectType) Val(fields map[string]ref.Val) ref.Val {
	return NewObjectVal(o.objectType, fields)
}

func (o *ObjectType) Type() *types.Type {
	return o.objectType
}

// Field 按名字查找字段。
// 这是非结构化版本，允许任意字段名。
// 返回的字段是 DynType 类型。
func (o *ObjectType) Field(name string) (*types.FieldType, bool) {
	return &types.FieldType{
		// 非结构化场景不检查字段类型，
		// 所有字段都用 DynType。
		Type: types.DynType,
		// IsSet 支撑 CEL 的 has() 语义：只有显式赋值的字段才视为已设置。
		IsSet: func(target any) bool {
			if m, ok := target.(map[string]any); ok {
				_, isSet := m[name]
				return isSet
			}
			return false
		},
		GetFrom: func(target any) (any, error) {
			if m, ok := target.(map[string]any); ok {
				return m[name], nil
			}
			return nil, fmt.Errorf("cannot get field %q", name)
		},
	}, true
}

func (o *ObjectType) FieldNames() ([]string, bool) {
	return nil, true // 动态类型的字段名未知，因此允许所有字段名。
}

// NewObjectType 根据给定字段名创建一个 ObjectType。
func NewObjectType(name string) *ObjectType {
	return &ObjectType{
		objectType: types.NewObjectType(name),
	}
}

// ObjectVal 是在没有对象 schema 信息时，由 CEL 表达式中的 Object{} 构造出的对象的 CEL Val。
// 在没有对象 schema 信息时由 CEL 表达式中的 Object{} 构造。
type ObjectVal struct {
	objectType *types.Type
	fields     map[string]ref.Val
}

// NewObjectVal 根据 ResolvedType 和字段创建一个 ObjectVal。
func NewObjectVal(objectType *types.Type, fields map[string]ref.Val) *ObjectVal {
	return &ObjectVal{
		objectType: objectType,
		fields:     fields,
	}
}

var _ ref.Val = (*ObjectVal)(nil)
var _ traits.Zeroer = (*ObjectVal)(nil)

// ConvertToNative 把对象转换为 map[string]any。
// 所有嵌套列表转换为 []any 原生类型。
//
// 目标类型不是 map[string]any，或递归转换中任一步失败时返回错误。
// 或递归转换中任一步失败。
//
// 支持 structpb 目标类型是为了让 Object 字面量能直接作为 JSON patch 的值被序列化。
func (v *ObjectVal) ConvertToNative(typeDesc reflect.Type) (any, error) {
	result := make(map[string]any, len(v.fields))
	for k, v := range v.fields {
		converted, err := convertField(v)
		if err != nil {
			return nil, fmt.Errorf("fail to convert field %q: %w", k, err)
		}
		result[k] = converted
	}
	switch typeDesc {
	case reflect.TypeFor[map[string]any]():
		return result, nil
	// CEL 内置的数据字面量都支持转换为 structpb.Value，进而序列化为 JSON。
	// 这便于 CEL 表达式返回任意 JSON 值，例如 MutatingAdmissionPolicy 的 JSON Patch
	// valueExpression 字段；因此这里也为 Object 数据字面量支持该转换。
	//
	case reflect.TypeFor[*structpb.Struct]():
		return structpb.NewStruct(result)
	case reflect.TypeFor[*structpb.Value]():
		return structpb.NewValue(result)
	default:
		return nil, fmt.Errorf("unable to convert to %v", typeDesc)
	}
}

// ConvertToType 支持表达式语言所支持的 CEL 值类型之间的转换。
func (v *ObjectVal) ConvertToType(typeValue ref.Type) ref.Val {
	if v.objectType.TypeName() == typeValue.TypeName() {
		return v
	}
	// 允许转换为 CEL 的 type 值，用于类型比较与断言。
	if typeValue == types.TypeType {
		return types.NewTypeTypeWithParam(v.objectType)
	}
	return types.NewErr("unsupported conversion into %v", typeValue)
}

// Equal 当 `other` 与实现结构体类型和内容相同时返回 true。
// 类型名不同即不相等；字段比较使用 DeepEqual，嵌套 Object 会递归比较。
func (v *ObjectVal) Equal(other ref.Val) ref.Val {
	if rhs, ok := other.(*ObjectVal); ok {
		if v.objectType.Equal(rhs.objectType) != types.True {
			return types.False
		}
		return types.Bool(reflect.DeepEqual(v.fields, rhs.fields))
	}
	return types.False
}

// Type 返回该值的 TypeValue。
func (v *ObjectVal) Type() ref.Type {
	return types.NewObjectType(v.objectType.TypeName())
}

// Value 以 map[string]any 形式返回其值。
func (v *ObjectVal) Value() any {
	var result any
	var object map[string]any
	result, err := v.ConvertToNative(reflect.TypeOf(object))
	if err != nil {
		return types.WrapErr(err)
	}
	return result
}

// CheckTypeNamesMatchFieldPathNames 递归检查 ObjectVal 的 CEL 对象类型名，返回所有
// 发现的类型名不匹配错误。
// 该 ObjectVal 下或 <field> 下的子 ObjectVal 类型名
// 必须是 "<ObjectVal.TypeName>.<field>" 形式，其子元素再追加 ".<field>"，依此类推。
//
// 中间的 map 和 list 没有名字，予以忽略。
//
// 该约束保证 Object{} 构造出的类型名与字段路径一致，防止表达式伪造任意类型。
func (v *ObjectVal) CheckTypeNamesMatchFieldPathNames() error {
	return errors.Join(typeCheck(v, []string{v.Type().TypeName()})...)

}

// typeCheck 递归校验子对象的类型名是否等于其从根开始的字段路径。
func typeCheck(v ref.Val, typeNamePath []string) []error {
	var errs []error
	if ov, ok := v.(*ObjectVal); ok {
		tn := ov.objectType.TypeName()
		if strings.Join(typeNamePath, ".") != tn {
			errs = append(errs, fmt.Errorf("unexpected type name %q, expected %q, which matches field name path from root Object type", tn, strings.Join(typeNamePath, ".")))
		}
		for k, f := range ov.fields {
			errs = append(errs, typeCheck(f, append(typeNamePath, k))...)
		}
	}
	value := v.Value()
	if listOfVal, ok := value.([]ref.Val); ok {
		for _, v := range listOfVal {
			errs = append(errs, typeCheck(v, typeNamePath)...)
		}
	}

	if mapOfVal, ok := value.(map[ref.Val]ref.Val); ok {
		for _, v := range mapOfVal {
			errs = append(errs, typeCheck(v, typeNamePath)...)
		}
	}
	return errs
}

// IsZeroValue 指示对象是否为该类型的零值。
// 对于 ObjectVal，当且仅当 fields map 为空时是零值。
func (v *ObjectVal) IsZeroValue() bool {
	return len(v.fields) == 0
}

// convertField 把被引用的 ref.Val 转换为期望类型。
// 对象期望 map[string]any。
// 列表期望 []any。
// map 期望 map[string]any。
// 其他类型通过 value.Value() 转换。
//
// 若请求类型是 map 但键不是字符串，返回错误。
// 键不是字符串。
func convertField(value ref.Val) (any, error) {
	// 列表特殊处理：元素用 Value() 而非 ConvertToNative 转换，
	// 以允许它们变成任意原生类型的值。
	if listOfVal, ok := value.Value().([]ref.Val); ok {
		var result []any
		for _, v := range listOfVal {
			result = append(result, v.Value())
		}
		return result, nil
	}
	// 非结构化 map，例如 annotations。
	// map 的键必须是字符串。
	if mapOfVal, ok := value.Value().(map[ref.Val]ref.Val); ok {
		result := make(map[string]any, len(mapOfVal))
		for k, v := range mapOfVal {
			stringKey, ok := k.Value().(string)
			if !ok {
				return nil, fmt.Errorf("map key %q is of type %T, not string", k, k)
			}
			result[stringKey] = v.Value()
		}
		return result, nil
	}
	if lister, ok := value.(traits.Lister); ok {
		var result []any
		it := lister.Iterator()
		for it.HasNext() == types.True {
			val := it.Next()
			conv, err := convertField(val)
			if err != nil {
				return nil, err
			}
			result = append(result, conv)
		}
		return result, nil
	}
	if mapper, ok := value.(traits.Mapper); ok {
		// ObjectVal 自己处理快速路径，直接通过 ConvertToNative 返回 map[string]any。
		if objVal, isObj := value.(*ObjectVal); isObj {
			native, err := objVal.ConvertToNative(reflect.TypeFor[map[string]any]())
			if err != nil {
				return nil, err
			}
			return native, nil
		}

		result := make(map[string]any)
		it := mapper.Iterator()
		for it.HasNext() == types.True {
			k := it.Next()
			stringKey, ok := k.Value().(string)
			if !ok {
				return nil, fmt.Errorf("map key %q is of type %T, not string", k, k)
			}
			v := mapper.Get(k)
			conv, err := convertField(v)
			if err != nil {
				return nil, err
			}
			result[stringKey] = conv
		}
		return result, nil
	}
	return value.Value(), nil
}
