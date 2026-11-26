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

// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// // 本包是测试夹具，用于验证生成器的行为。
// slices 验证递归切片类型（T5/T6 持有自身切片）的发现顺序：切片元素类型
// 在字段处理过程中可能尚未完成发现，生成器必须能正确处理。
// +k8s:validation-gen-nolint
package slices

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 这个用例专门构造来测递归。不想再加
// `validateFalse` 标签，因为促成此测试的那个 bug
// 就是递归树看得不够深。
//
// 预期：
// * T1 应生成校验，因为 T3 有校验。
// * T2 应生成校验，因为它使用了有校验的 T1。
// * T3 应生成校验，因为它自身有校验。
// * T4 应生成校验，因为它使用了有校验的 T3。
// * T1 应调用 T2 和 T3。
// * T2 应调用 eachVal(T1)。
// * T3 应调用 T4。
// * T4 应调用 eachVal(T3)。
// * T5 和 T6 各持有自身类型的 slice，且经 T1 可达，因此处理其字段时
// 处理其自身字段时仍在发现中。

type T1 struct {
	T2 T2   `json:"t2"`
	T3 T3   `json:"t3"`
	T5 []T5 `json:"t5"`
	T6 []T6 `json:"t6"`
}

type T2 struct {
	ST1 []T1 `json:"st1"`
}

// +k8s:validateFalse="type T3"
type T3 struct {
	T4 T4 `json:"t4"`
}

type T4 struct {
	ST3 []T3 `json:"st3"`
}

type T5 struct {
	// +k8s:validateFalse="type T5.T5"
	T5 []T5 `json:"t5"`
}

type T6 struct {
	// +k8s:validateFalse="type T6.S"
	S  string `json:"s"`
	T6 []T6   `json:"t6"`
}
