/*
Copyright 2014 The Kubernetes Authors.

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

package conversion

import (
	"fmt"
	"reflect"
)

// typePair 用源/目标反射类型作为转换函数的查找键。
type typePair struct {
	source reflect.Type
	dest   reflect.Type
}

// NameFunc 根据反射类型返回一个名字，用于日志与调试。
type NameFunc func(t reflect.Type) string

// DefaultNameFunc 返回类型的 Go 名称，是 NameFunc 的默认实现。
var DefaultNameFunc = func(t reflect.Type) string { return t.Name() }

// ConversionFunc 定义将对象 a 转换并写入对象 b 的函数签名。实现可以按需复用目标对象中的
// 数组、切片、映射或指针，以减少内存分配；无法转换或发现数据非法时必须返回错误。
// 如果调用方不希望源对象与目标对象共享字段或嵌套对象，调用前必须先复制源对象。
// a、b 应传入与注册类型匹配的指针，scope 用于继续当前转换流程。
type ConversionFunc func(a, b interface{}, scope Scope) error

// Converter 负责在不同 Go 类型之间查找并执行转换。
// 它维护类型对到转换函数的映射。查找优先级：
// ignored（无操作） > 手动注册 > 生成代码注册；都不命中则报"unknown conversion"。
type Converter struct {
	// 手动注册的转换函数表。相同类型对再次注册时，新函数会覆盖旧函数。
	conversionFuncs          ConversionFuncs
	generatedConversionFuncs ConversionFuncs

	// 应被视为无操作的类型对集合；命中后 Convert 直接返回，不检查或修改目标对象。
	ignoredUntypedConversions map[typePair]struct{}
}

// NewConverter 创建一个空的转换器，并初始化手动注册、生成代码注册和忽略转换三张表。
// NameFunc 参数仅为兼容旧 API 而保留，当前实现不使用它。
func NewConverter(NameFunc) *Converter {
	c := &Converter{
		conversionFuncs:           NewConversionFuncs(),
		generatedConversionFuncs:  NewConversionFuncs(),
		ignoredUntypedConversions: make(map[typePair]struct{}),
	}
	// 预注册 []byte 的转换：默认反射拷贝会逐字节递归，代价高且无意义。
	c.RegisterUntypedConversionFunc(
		(*[]byte)(nil), (*[]byte)(nil),
		func(a, b interface{}, s Scope) error {
			return Convert_Slice_byte_To_Slice_byte(a.(*[]byte), b.(*[]byte), s)
		},
	)
	return c
}

// WithConversions 返回基于当前转换器派生的新转换器，并将 fns 合并到手动转换表中。
// 新的手动转换表由 Merge 创建，因此添加或覆盖其中的条目不会修改原转换器的该表；
// 但这是浅拷贝，生成代码转换表和忽略转换表仍与原对象共享，不能据此认为所有状态都隔离。
func (c *Converter) WithConversions(fns ConversionFuncs) *Converter {
	copied := *c
	copied.conversionFuncs = c.conversionFuncs.Merge(fns)
	return &copied
}

// DefaultMeta 为指定类型创建默认元数据。当前实现不根据 t 填充字段，返回空 Meta。
func (c *Converter) DefaultMeta(t reflect.Type) *Meta {
	return &Meta{}
}

// Convert_Slice_byte_To_Slice_byte 直接复制字节切片，避免反射转换逐个递归处理每个字节。
func Convert_Slice_byte_To_Slice_byte(in *[]byte, out *[]byte, s Scope) error {
	if *in == nil {
		// 保持 nil 与空切片的区别。
		*out = nil
		return nil
	}
	*out = make([]byte, len(*in))
	copy(*out, *in)
	return nil
}

// Scope 会传给每个转换函数，使自定义转换逻辑能够继续当前转换流程。
// 当系统中存在多个 Converter 时，Scope 会保留发起当前转换的那个实例，
// 子对象转换应通过 Scope.Convert 完成，避免误用其他转换器。
type Scope interface {
	// Convert 用于递归转换子对象。必须传入不同的源/目标子对象；若再次传入当前转换的原始参数，
	// 会重复进入同一转换函数并导致无限递归，最终耗尽调用栈。
	//
	// 用相同参数递归调用会无限递归，最终栈溢出。
	Convert(src, dest interface{}) error

	// Meta 返回最初调用 Converter.Convert 时传入的元数据，供嵌套转换函数读取。
	Meta() *Meta
}

// NewConversionFuncs 创建一个空的转换函数表。
func NewConversionFuncs() ConversionFuncs {
	return ConversionFuncs{
		untyped: make(map[typePair]ConversionFunc),
	}
}

// ConversionFuncs 是类型对到转换函数的映射集合。
type ConversionFuncs struct {
	untyped map[typePair]ConversionFunc
}

// AddUntyped 将 fn 注册为 a、b 对应类型之间的转换函数。a 和 b 必须传入指针，
// 注册时只根据指针的反射类型建立查找键，不会验证 fn 的参数签名；
// 相同类型对重复注册时会覆盖已有函数。
func (c ConversionFuncs) AddUntyped(a, b interface{}, fn ConversionFunc) error {
	tA, tB := reflect.TypeOf(a), reflect.TypeOf(b)
	if tA.Kind() != reflect.Pointer {
		return fmt.Errorf("the type %T must be a pointer to register as an untyped conversion", a)
	}
	if tB.Kind() != reflect.Pointer {
		return fmt.Errorf("the type %T must be a pointer to register as an untyped conversion", b)
	}
	c.untyped[typePair{tA, tB}] = fn
	return nil
}

// Merge 创建新的转换函数表，先复制当前表，再复制 other；因此 other 中相同类型对的函数优先级更高。
// 原表不会被修改，两个输入表也不会与结果共享底层 map。
func (c ConversionFuncs) Merge(other ConversionFuncs) ConversionFuncs {
	merged := NewConversionFuncs()
	for k, v := range c.untyped {
		merged.untyped[k] = v
	}
	// 后写入 other，实现"other 优先"的覆盖语义。
	for k, v := range other.untyped {
		merged.untyped[k] = v
	}
	return merged
}

// Meta 由 Scheme 调用 Convert 时传入，用于向转换函数传递调用方上下文。
type Meta struct {
	// Context 是可选的调用方上下文，可携带转换函数所需的附加信息；Converter 本身不会解释该值。
	Context interface{}
}

// scope 保存一次转换过程的上下文，使嵌套转换沿用同一个 Converter 和 Meta。
type scope struct {
	converter *Converter
	meta      *Meta
}

// Convert 使用当前 scope 中的 Converter 和 Meta 继续转换子对象。
func (s *scope) Convert(src, dest interface{}) error {
	return s.converter.Convert(src, dest, s.meta)
}

// Meta 返回创建当前转换 scope 时传入的 Meta；嵌套转换共享同一份元数据。
func (s *scope) Meta() *Meta {
	return s.meta
}

// RegisterUntypedConversionFunc 注册 a、b 两种指针类型之间的手动转换函数。
// 调用时会把实际对象直接传给 fn；注册机制只校验 a、b 是指针，不会校验 fn 的签名或转换结果，
// 因此函数实现必须自行保证类型断言正确，并按约定写入目标对象。
func (c *Converter) RegisterUntypedConversionFunc(a, b interface{}, fn ConversionFunc) error {
	return c.conversionFuncs.AddUntyped(a, b, fn)
}

// RegisterGeneratedUntypedConversionFunc 注册由代码生成器提供的类型转换函数。
// 调用时会把对应类型的对象直接传给 fn；注册机制只校验 a、b 是指针，不会校验 fn 的签名或转换结果。
// 生成函数与手动函数分开存储，查找时手动注册优先，便于业务代码覆盖生成的默认转换逻辑。
func (c *Converter) RegisterGeneratedUntypedConversionFunc(a, b interface{}, fn ConversionFunc) error {
	return c.generatedConversionFuncs.AddUntyped(a, b, fn)
}

// RegisterIgnoredConversion 将 from 到 to 的类型对登记为忽略转换。
// 后续 Convert 命中该类型对时会直接返回 nil，不调用转换函数，也不会修改目标对象。
// from 和 to 必须是指针；该规则优先于手动注册和生成代码注册的转换函数。
func (c *Converter) RegisterIgnoredConversion(from, to interface{}) error {
	typeFrom := reflect.TypeOf(from)
	typeTo := reflect.TypeOf(to)
	if typeFrom.Kind() != reflect.Pointer {
		return fmt.Errorf("expected pointer arg for 'from' param 0, got: %v", typeFrom)
	}
	if typeTo.Kind() != reflect.Pointer {
		return fmt.Errorf("expected pointer arg for 'to' param 1, got: %v", typeTo)
	}
	c.ignoredUntypedConversions[typePair{typeFrom, typeTo}] = struct{}{}
	return nil
}

// Convert 根据 src、dest 的具体指针类型查找并执行转换函数；两者通常都应是非 nil 指针。
// 查找顺序为：忽略转换、手动注册函数、生成代码注册函数。当前实现没有通用反射复制兜底，
// 未找到匹配函数时会校验指针并返回 unknown conversion 错误。
// meta 不参与函数选择或转换逻辑，只会存入 Scope，供转换函数读取。
// 转换逻辑不支持对象图中的循环引用；自定义转换函数必须避免递归遍历循环结构。
func (c *Converter) Convert(src, dest interface{}, meta *Meta) error {
	pair := typePair{reflect.TypeOf(src), reflect.TypeOf(dest)}
	scope := &scope{
		converter: c,
		meta:      meta,
	}

	// 忽略规则优先级最高：命中后直接返回，不执行任何转换。
	if _, ok := c.ignoredUntypedConversions[pair]; ok {
		return nil
	}
	// 手动注册的函数优先于生成代码，便于调用方覆盖特定行为。
	if fn, ok := c.conversionFuncs.untyped[pair]; ok {
		return fn(src, dest, scope)
	}
	if fn, ok := c.generatedConversionFuncs.untyped[pair]; ok {
		return fn(src, dest, scope)
	}

	// 没有注册函数时只校验 src、dest 是否为有效指针，然后返回明确错误；
	// 不尝试通用反射拷贝，避免对不同结构的字段语义、别名和所有权作出错误假设。
	dv, err := EnforcePtr(dest)
	if err != nil {
		return err
	}
	sv, err := EnforcePtr(src)
	if err != nil {
		return err
	}
	return fmt.Errorf("converting (%s) to (%s): unknown conversion", sv.Type(), dv.Type())
}
