/*
Copyright 2023 The Kubernetes Authors.

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

// Package lazy 提供按字段惰性求值的 CEL map 值，用于代价高昂的派生字段：
// 只有表达式真正访问某个字段时才调用其回调，避免为未使用的字段付出计算成本。
package lazy

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"

	"k8s.io/apiserver/pkg/cel"
)

// GetFieldFunc 计算某个字段的 CEL 值。接收整个 MapValue 是为了允许字段之间
// 相互引用（例如某字段由其他字段派生），实现时需注意不要形成循环依赖。
type GetFieldFunc func(*MapValue) ref.Val

var _ ref.Val = (*MapValue)(nil)
var _ traits.Mapper = (*MapValue)(nil)

// MapValue is a map that lazily evaluate its value when a field is first accessed.
// The map value is not designed to be thread-safe.
//
// 求值结果写入 values 后即被复用，因此同一个 MapValue 只能用于一次 CEL 求值。
type MapValue struct {
	typeValue *types.Type

	// values are previously evaluated values obtained from callbacks
	values map[string]ref.Val
	// callbacks are a map of field name to the function that returns the field Val
	callbacks map[string]GetFieldFunc
	// knownValues are registered names, used for iteration
	// 保留注册顺序，使迭代结果稳定；不能直接遍历 callbacks（map 顺序随机）。
	knownValues []string
}

// NewMapValue 创建指定 CEL 类型的惰性 map；初始不含任何字段，需通过 Append 注册。
func NewMapValue(objectType ref.Type) *MapValue {
	return &MapValue{
		typeValue: types.NewTypeValue(objectType.TypeName(), traits.IndexerType|traits.FieldTesterType|traits.IterableType),
		values:    map[string]ref.Val{},
		callbacks: map[string]GetFieldFunc{},
	}
}

// Append adds the given field with its name and callback.
// 同名重复 Append 会覆盖回调但保留重复的名称项；调用方应保证字段名唯一。
func (m *MapValue) Append(name string, callback GetFieldFunc) {
	m.knownValues = append(m.knownValues, name)
	m.callbacks[name] = callback
}

// Contains checks if the key is known to the map
// 注意它会触发字段求值：求值出错时按 CEL 语义返回错误而不是 false。
func (m *MapValue) Contains(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}
	return types.Bool(found)
}

// Iterator returns an iterator to traverse the map.
func (m *MapValue) Iterator() traits.Iterator {
	return &iterator{parent: m, index: 0}
}

// Size returns the number of currently known fields
func (m *MapValue) Size() ref.Val {
	return types.Int(len(m.callbacks))
}

// ConvertToNative returns an error because it is disallowed
// 惰性 map 没有可导出的原生表示，禁止转换可避免在 CEL 之外意外触发全部求值。
func (m *MapValue) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("disallowed conversion from %q to %q", m.typeValue.TypeName(), typeDesc.Name())
}

// ConvertToType converts the map to the given type.
// Only its own type and "Type" type are allowed.
func (m *MapValue) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case m.typeValue:
		return m
	case types.TypeType:
		return m.typeValue
	}
	return types.NewErr("disallowed conversion from %q to %q", m.typeValue.TypeName(), typeVal.TypeName())
}

// Equal returns true if the other object is the same pointer-wise.
// 指针相等是唯一可行的比较方式：逐个字段比较会触发全部惰性求值。
func (m *MapValue) Equal(other ref.Val) ref.Val {
	otherMap, ok := other.(*MapValue)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(m == otherMap)
}

// Type returns its registered type.
func (m *MapValue) Type() ref.Type {
	return m.typeValue
}

// Value is not allowed.
func (m *MapValue) Value() any {
	return types.NoSuchOverloadErr()
}

// resolveField resolves the field. Calls the callback if the value is not yet stored.
// 回调只在首次访问时执行一次；nil 值也会被缓存，避免重复求值。
func (m *MapValue) resolveField(name string) ref.Val {
	v, seen := m.values[name]
	if seen {
		return v
	}
	f := m.callbacks[name]
	v = f(m)
	m.values[name] = v
	return v
}

// Find 查找字段并触发惰性求值。
// key 需先做 CEL 名称反转义（如 __underscore__ 还原），因为字段名可能包含非法标识符字符。
func (m *MapValue) Find(key ref.Val) (ref.Val, bool) {
	n, ok := key.(types.String)
	if !ok {
		return types.MaybeNoSuchOverloadErr(n), true
	}
	name, ok := cel.Unescape(n.Value().(string))
	if !ok {
		return nil, false
	}
	// 未注册的字段直接返回未找到，绝不调用回调，避免执行任意未知逻辑。
	if _, exists := m.callbacks[name]; !exists {
		return nil, false
	}
	return m.resolveField(name), true
}

// Get 在字段不存在时返回 CEL 错误。
func (m *MapValue) Get(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if found {
		return v
	}
	return types.ValOrErr(key, "no such key: %v", key)
}

// iterator 按注册顺序遍历字段名；Next 返回的是字段名而非值，
// 符合 CEL map 迭代语义（迭代 key）。
type iterator struct {
	parent *MapValue
	index  int
}

// ConvertToNative 不允许转换迭代器。
func (i *iterator) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("disallowed conversion to %q", typeDesc.Name())
}

// ConvertToType 不允许转换迭代器。
func (i *iterator) ConvertToType(typeValue ref.Type) ref.Val {
	return types.NewErr("disallowed conversion o %q", typeValue.TypeName())
}

// Equal 以指针身份比较迭代器。
func (i *iterator) Equal(other ref.Val) ref.Val {
	otherIterator, ok := other.(*iterator)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(otherIterator == i)
}

// Type 返回 CEL 的 iterator 类型。
func (i *iterator) Type() ref.Type {
	return types.IteratorType
}

// Value 无原生表示。
func (i *iterator) Value() any {
	return nil
}

// HasNext 判断是否还有未遍历的字段名。
func (i *iterator) HasNext() ref.Val {
	return types.Bool(i.index < len(i.parent.knownValues))
}

// Next 返回下一个字段名；调用方需自行保证 HasNext 为真，否则会越界 panic。
func (i *iterator) Next() ref.Val {
	ret := i.parent.Get(types.String(i.parent.knownValues[i.index]))
	i.index++
	return ret
}

var _ traits.Iterator = (*iterator)(nil)
