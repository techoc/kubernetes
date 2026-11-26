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

// 这是一个测试包。
// pointers 验证校验代码发现阶段对递归指针类型的处理：既不能无限递归，
// 也要为真正可达的校验生成调用。
// +k8s:validation-gen-nolint
package pointers

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 该测试用例刻意构造用于验证递归。为了保持递归行为，不添加更多 `validateFalse` 标签。
//
// 预期：
// * 应为 T1 生成校验：它使用 T2，T2 使用带校验的 T3。
// * 应为 T2 生成校验：它使用带校验的 T3。
// * 应为 T3 生成校验：它自身带校验。
// * 不应为 T4 生成校验。
// * T1 应调用 optional(T1)、T2 与 optional(T2)。
// * T2 应调用 optional(T1)、optional(T2) 与 optional(T3)。

type T1 struct {
	// +k8s:optional
	PT1 *T1 `json:"pt1"`

	T2 T2 `json:"t2"`

	// +k8s:optional
	PT2 *T2 `json:"pt2"`
}

type T2 struct {
	// +k8s:optional
	PT1 *T1 `json:"pt1"`

	// +k8s:optional
	PT2 *T2 `json:"pt2"`

	// +k8s:optional
	PT3 *T3 `json:"pt3"`
}

// +k8s:validateFalse="type T3"
type T3 struct {
	I int `json:"i"`
}

// 注意：没有校验。
type T4 struct {
	// 注意：没有校验。
	PT4 *T4 `json:"pt4"`
}
