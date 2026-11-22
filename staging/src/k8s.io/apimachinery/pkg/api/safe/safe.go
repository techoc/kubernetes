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

// Package safe 提供一组泛型辅助函数，用于安全地访问可能为 nil 的指针与接口，
// 以表达式形式替代冗长的显式判空代码。
package safe

// Field 接收任意值的指针（可能为 nil）和一个把指针遍历为目标类型 R 的函数
// （典型用法是解引用某个字段），返回遍历结果；若指针为 nil 则返回 R 的零值。
//
// 大致等价于支持三元运算符的语言中的：
//
//	value != nil ? fn(value) : zero-value
func Field[V any, R any](value *V, fn func(*V) R) R {
	if value == nil {
		var zero R
		return zero
	}
	o := fn(value)
	return o
}

// Cast 尝试把任意值断言为 T，成功时返回该值，否则返回 T 的零值。
// 有意吞掉断言失败的 ok 标志，用于“类型不匹配就当作缺失”的场景。
func Cast[T any](value any) T {
	result, _ := value.(T)
	return result
}

// Value 接收任意值的指针（可能为 nil）和一个返回同类型指针的函数。
// 若值非 nil 则返回该值，否则返回函数的结果。
//
// 大致等价于支持三元运算符的语言中的：
//
//	value != nil ? value : fn()
func Value[T any](value *T, fn func() *T) *T {
	if value != nil {
		return value
	}
	return fn()
}
