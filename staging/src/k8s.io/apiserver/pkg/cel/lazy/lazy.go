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

// MapValue 是一个在字段首次访问时才惰性求值其值的 map。
// 该 map 不保证线程安全。
//
// 求值结果写入 values 后即被复用，因此同一个 MapValue 只能用于一次 CEL 求值。
type MapValue struct {
	typeValue *types.Type

	// values 是此前从 callback 求值得到的结果
	values map[string]ref.Val
	// callbacks 是字段名到返回字段 Val 的函数的映射
	callbacks map[string]GetFieldFunc
	// knownValues 保存已注册的字段名，供迭代使用：保留注册顺序使迭代结果稳定；不能直接遍历 callbacks（map 顺序随机）。
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

// Append 以给定名称与回调添加字段。同名重复 Append 会覆盖回调但保留重复的名称项；调用方应保证字段名唯一。
func (m *MapValue) Append(name string, callback GetFieldFunc) {
	m.knownValues = append(m.knownValues, name)
	m.callbacks[name] = callback
}

// Contains 检查 map 是否已知该 key。注意它会触发字段求值：求值出错时按 CEL 语义返回错误而不是 false。
func (m *MapValue) Contains(key ref.Val) ref.Val {
	v, found := m.Find(key)
	if v != nil && types.IsUnknownOrError(v) {
		return v
	}
	return types.Bool(found)
}

// Iterator 返回遍历该 map 的迭代器。
func (m *MapValue) Iterator() traits.Iterator {
	return &iterator{parent: m, index: 0}
}

// Size 返回当前已知字段的数量
func (m *MapValue) Size() ref.Val {
	return types.Int(len(m.callbacks))
}

// ConvertToNative 一律返回错误，因为惰性 map 禁止原生转换：它没有可导出的原生表示，禁止转换可避免在 CEL 之外意外触发全部求值。
func (m *MapValue) ConvertToNative(typeDesc reflect.Type) (any, error) {
	return nil, fmt.Errorf("disallowed conversion from %q to %q", m.typeValue.TypeName(), typeDesc.Name())
}

// ConvertToType 把 map 转换为给定类型。
// 只允许转换到自身类型与 "Type" 类型。
func (m *MapValue) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case m.typeValue:
		return m
	case types.TypeType:
		return m.typeValue
	}
	return types.NewErr("disallowed conversion from %q to %q", m.typeValue.TypeName(), typeVal.TypeName())
}

// Equal 仅在 other 指针相同时返回 true：指针相等是唯一可行的比较方式，逐字段比较会触发全部惰性求值。
func (m *MapValue) Equal(other ref.Val) ref.Val {
	otherMap, ok := other.(*MapValue)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(m == otherMap)
}

// Type 返回其注册的类型。
func (m *MapValue) Type() ref.Type {
	return m.typeValue
}

// 不允许使用 Value。
func (m *MapValue) Value() any {
	return types.NoSuchOverloadErr()
}

// resolveField 解析字段：若值尚未保存则调用回调。回调只在首次访问时执行一次；nil 值也会被缓存，避免重复求值。
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
