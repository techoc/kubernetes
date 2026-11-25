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

package mutation

import (
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"reflect"
)

// jsonPatchType 是所有 JSONPatch 值共享的类型对象，作为 CEL 类型系统中的单例。
var jsonPatchType = types.NewObjectType(JSONPatchTypeName)

// JSONPatch 字段名常量；集中定义避免拼写漂移导致 Val/Field/Get 不一致。
var (
	jsonPatchOp    = "op"
	jsonPatchPath  = "path"
	jsonPatchFrom  = "from"
	jsonPatchValue = "value"
)

// JSONPatchType and JSONPatchVal are defined entirely from scratch here because JSONPatchVal
// has a dynamic 'value' field which can not be defined with an OpenAPI schema,
// preventing us from using DeclType and UnstructuredToVal.
//
// 换言之，value 可以是任意 JSON 值，无法用静态 schema 描述，只能手写 ref.Val 实现。

// JSONPatchType provides a CEL type for "JSONPatch" operations.
type JSONPatchType struct{}

// HasTrait 直接委托给底层 types.Type，保证与 CEL 运行时的特性判断一致。
func (r *JSONPatchType) HasTrait(trait int) bool {
	return jsonPatchType.HasTrait(trait)
}

// TypeName returns the name of this ObjectType.
func (r *JSONPatchType) TypeName() string {
	return jsonPatchType.TypeName()
}

// Val returns an instance given the fields.
// 字段类型在构造时校验：op/path/from 必须是字符串，value 保留原始 ref.Val，
// 未知字段直接报错，避免拼写错误被静默忽略。
func (r *JSONPatchType) Val(fields map[string]ref.Val) ref.Val {
	result := &JSONPatchVal{}
	for name, value := range fields {
		switch name {
		case jsonPatchOp:
			if s, ok := value.Value().(string); ok {
				result.Op = s
			} else {
				return types.NewErr("unexpected type %T for JSONPatchType 'op' field", value.Value())
			}
		case jsonPatchPath:
			if s, ok := value.Value().(string); ok {
				result.Path = s
			} else {
				return types.NewErr("unexpected type %T for JSONPatchType 'path' field", value.Value())
			}
		case jsonPatchFrom:
			if s, ok := value.Value().(string); ok {
				result.From = s
			} else {
				return types.NewErr("unexpected type %T for JSONPatchType 'from' field", value.Value())
			}
		case jsonPatchValue:
			// value 保持原样，由消费方按 JSON Patch 语义处理。
			result.Val = value
		default:
			return types.NewErr("unexpected JSONPatchType field: %s", name)
		}
	}
	return result
}

// Type 返回该类型实例对应的 types.Type。
func (r *JSONPatchType) Type() *types.Type {
	return jsonPatchType
}

// Field 描述字段类型：三个字符串字段为 string，value 为 dyn。
// 始终返回 true，因为 JSONPatch 不区分“字段不存在”与“值为空”。
func (r *JSONPatchType) Field(name string) (*types.FieldType, bool) {
	var fieldType *types.Type
	switch name {
	case jsonPatchOp, jsonPatchFrom, jsonPatchPath:
		fieldType = cel.StringType
	case jsonPatchValue:
		fieldType = types.DynType
	}
	return &types.FieldType{
		Type: fieldType,
	}, true
}

// FieldNames 列出可访问的字段名，供 CEL 进行字段选择检查。
func (r *JSONPatchType) FieldNames() ([]string, bool) {
	return []string{jsonPatchOp, jsonPatchFrom, jsonPatchPath, jsonPatchValue}, true
}

// JSONPatchVal is the ref.Val for a JSONPatch.
type JSONPatchVal struct {
	Op, From, Path string
	Val            ref.Val
}

// ConvertToNative 仅支持转换为 *JSONPatchVal 本身，其余类型一律拒绝，
// 防止 CEL 把内部表示意外暴露给外部代码。
func (p *JSONPatchVal) ConvertToNative(typeDesc reflect.Type) (any, error) {
	if typeDesc == reflect.TypeOf(&JSONPatchVal{}) {
		return p, nil
	}
	return nil, fmt.Errorf("cannot convert to native type: %v", typeDesc)
}

// ConvertToType 支持转换为自身类型或 type 类型；其他转换返回错误而非静默 nil。
func (p *JSONPatchVal) ConvertToType(typeValue ref.Type) ref.Val {
	if typeValue == jsonPatchType {
		return p
	} else if typeValue == types.TypeType {
		return types.NewTypeTypeWithParam(jsonPatchType)
	}
	return types.NewErr("unsupported type: %s", typeValue.TypeName())
}

// Equal 逐字段比较；value 需要处理 nil 与“nil 不等于非 nil”的边界。
func (p *JSONPatchVal) Equal(other ref.Val) ref.Val {
	if o, ok := other.(*JSONPatchVal); ok && p != nil && o != nil {
		if p.Op != o.Op || p.From != o.From || p.Path != o.Path {
			return types.False
		}
		if (p.Val == nil) != (o.Val == nil) {
			return types.False
		}
		if p.Val == nil {
			return types.True
		}
		return p.Val.Equal(o.Val)
	}
	return types.False
}

// Get 实现 CEL 的下标访问：patch["op"] 等；未知字段返回错误。
func (p *JSONPatchVal) Get(index ref.Val) ref.Val {
	if name, ok := index.Value().(string); ok {
		switch name {
		case jsonPatchOp:
			return types.String(p.Op)
		case jsonPatchPath:
			return types.String(p.Path)
		case jsonPatchFrom:
			return types.String(p.From)
		case jsonPatchValue:
			return p.Val
		default:

		}
	}
	return types.NewErr("unsupported indexer: %s", index)
}

// IsSet 实现 CEL 的 has() 语义：字符串字段非空、value 非 nil 即视为已设置。
func (p *JSONPatchVal) IsSet(field ref.Val) ref.Val {
	if name, ok := field.Value().(string); ok {
		switch name {
		case jsonPatchOp:
			return types.Bool(len(p.Op) > 0)
		case jsonPatchPath:
			return types.Bool(len(p.Path) > 0)
		case jsonPatchFrom:
			return types.Bool(len(p.From) > 0)
		case jsonPatchValue:
			return types.Bool(p.Val != nil)
		}
	}
	return types.NewErr("unsupported field: %s", field)
}

// Type 返回该值的 CEL 类型。
func (p *JSONPatchVal) Type() ref.Type {
	return jsonPatchType
}

// Value 返回底层 Go 值，即 JSONPatchVal 自身。
func (p *JSONPatchVal) Value() any {
	return p
}

// 编译期断言：JSONPatchVal 必须实现 ref.Val。
var _ ref.Val = &JSONPatchVal{}
