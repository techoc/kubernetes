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

// ConversionFunc converts the object a into the object b, reusing arrays or objects
// or pointers if necessary. It should return an error if the object cannot be converted
// or if some data is invalid. If you do not wish a and b to share fields or nested
// objects, you must copy a before calling this function.
//
// 约定：a、b 均为指针；函数应尽量复用 b 中已有的容器以减少分配，
// 因此若调用方不希望共享底层数据，需自行先深拷贝 a。
type ConversionFunc func(a, b interface{}, scope Scope) error

// Converter knows how to convert one type to another.
//
// Converter 维护类型对到转换函数的映射。查找优先级：
// ignored（无操作） > 手动注册 > 生成代码注册；都不命中则报"unknown conversion"。
type Converter struct {
	// Map from the conversion pair to a function which can
	// do the conversion.
	conversionFuncs          ConversionFuncs
	generatedConversionFuncs ConversionFuncs

	// Set of conversions that should be treated as a no-op
	ignoredUntypedConversions map[typePair]struct{}
}

// NewConverter creates a new Converter object.
// Arg NameFunc is just for backward compatibility.
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

// WithConversions returns a Converter that is a copy of c but with the additional
// fns merged on top.
//
// 返回浅拷贝：映射表通过 Merge 生成新表，因此原 Converter 不会被修改，
// 可安全地作为模板派生出多个变体。
func (c *Converter) WithConversions(fns ConversionFuncs) *Converter {
	copied := *c
	copied.conversionFuncs = c.conversionFuncs.Merge(fns)
	return &copied
}

// DefaultMeta returns meta for a given type.
func (c *Converter) DefaultMeta(t reflect.Type) *Meta {
	return &Meta{}
}

// Convert_Slice_byte_To_Slice_byte prevents recursing into every byte
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

// Scope is passed to conversion funcs to allow them to continue an ongoing conversion.
// If multiple converters exist in the system, Scope will allow you to use the correct one
// from a conversion function--that is, the one your conversion function was called by.
type Scope interface {
	// Call Convert to convert sub-objects. Note that if you call it with your own exact
	// parameters, you'll run out of stack space before anything useful happens.
	//
	// 用相同参数递归调用会无限递归，最终栈溢出。
	Convert(src, dest interface{}) error

	// Meta returns any information originally passed to Convert.
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

// AddUntyped adds the provided conversion function to the lookup table for the types that are
// supplied as a and b. a and b must be pointers or an error is returned. This method overwrites
// previously defined functions.
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

// Merge returns a new ConversionFuncs that contains all conversions from
// both other and c, with other conversions taking precedence.
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

// Meta is supplied by Scheme, when it calls Convert.
type Meta struct {
	// Context is an optional field that callers may use to pass info to conversion functions.
	Context interface{}
}

// scope contains information about an ongoing conversion.
type scope struct {
	converter *Converter
	meta      *Meta
}

// Convert continues a conversion.
func (s *scope) Convert(src, dest interface{}) error {
	return s.converter.Convert(src, dest, s.meta)
}

// Meta returns the meta object that was originally passed to Convert.
func (s *scope) Meta() *Meta {
	return s.meta
}

// RegisterUntypedConversionFunc registers a function that converts between a and b by passing objects of those
// types to the provided function. The function *must* accept objects of a and b - this machinery will not enforce
// any other guarantee.
func (c *Converter) RegisterUntypedConversionFunc(a, b interface{}, fn ConversionFunc) error {
	return c.conversionFuncs.AddUntyped(a, b, fn)
}

// RegisterGeneratedUntypedConversionFunc registers a function that converts between a and b by passing objects of those
// types to the provided function. The function *must* accept objects of a and b - this machinery will not enforce
// any other guarantee.
//
// 生成代码单独存放，避免手动注册覆盖生成函数时难以排查；查找时手动注册优先。
func (c *Converter) RegisterGeneratedUntypedConversionFunc(a, b interface{}, fn ConversionFunc) error {
	return c.generatedConversionFuncs.AddUntyped(a, b, fn)
}

// RegisterIgnoredConversion registers a "no-op" for conversion, where any requested
// conversion between from and to is ignored.
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

// Convert will translate src to dest if it knows how. Both must be pointers.
// If no conversion func is registered and the default copying mechanism
// doesn't work on this type pair, an error will be returned.
// 'meta' is given to allow you to pass information to conversion functions,
// it is not used by Convert() other than storing it in the scope.
// Not safe for objects with cyclic references!
func (c *Converter) Convert(src, dest interface{}, meta *Meta) error {
	pair := typePair{reflect.TypeOf(src), reflect.TypeOf(dest)}
	scope := &scope{
		converter: c,
		meta:      meta,
	}

	// ignore conversions of this type
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

	// 没有注册函数时只做参数校验，然后返回明确错误；不做反射拷贝，
	// 因为任意结构的反射拷贝容易产生错误语义。
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
