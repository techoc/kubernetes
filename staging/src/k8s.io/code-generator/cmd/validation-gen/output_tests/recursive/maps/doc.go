/*
Copyright 2025 The Kubernetes Authors.

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
// +k8s:validation-gen-nolint
package maps

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 该用例经过精心构造以测试递归机制。不要再添加更多 `validateFalse` 标签，因为
// 触发该测试的那个 bug 是对递归树探查得不够深。
//
// 期望：
// * T3 带有验证，因此应为 T1 生成验证。
// * T2 使用了带验证的 T1，因此应为 T2 生成验证。
// * T3 自身带有验证，因此应为 T3 生成验证。
// * T4 使用了带验证的 T3，因此应为 T4 生成验证。
// * T1 应调用 T2 和 T3。
// * T2 应调用 eachVal(T1)。
// * T3 应调用 T4。
// * T4 应调用 eachVal(T3)。
// * T5 和 T6 持有以自身为值类型的 map，且经由 T1 到达，因此该 map 类型在
//   其自身字段被处理时尚处于发现过程中。
//
// 该用例针对递归 map 类型的验证传播。核心风险是类型发现与字段处理
// 交错时可能提前判定“无验证”，因此期望值明确要求 T2/T4 等间接类型也被生成，
// 并且自引用 map（T5/T6）在自身字段处理期间仍能被发现。

type T1 struct {
	T2 T2            `json:"t2"`
	T3 T3            `json:"t3"`
	T5 map[string]T5 `json:"t5"`
	T6 map[string]T6 `json:"t6"`
}

type T2 struct {
	MT1 map[string]T1 `json:"mt1"`
}

// +k8s:validateFalse="type T3"
type T3 struct {
	T4 T4 `json:"t4"`
}

type T4 struct {
	MT3 map[string]T3 `json:"mt3"`
}

type T5 struct {
	// +k8s:validateFalse="type T5.T5"
	T5 map[string]T5 `json:"t5"`
}

type T6 struct {
	// +k8s:validateFalse="type T6.S"
	S  string        `json:"s"`
	T6 map[string]T6 `json:"t6"`
}
