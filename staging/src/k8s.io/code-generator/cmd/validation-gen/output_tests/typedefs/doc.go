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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// 这是一个测试包。
// +k8s:validation-gen-nolint
// 验证校验标记在 typedef 链（typedef 的 typedef）上的传播，
// 以及字段级校验与类型级校验的叠加。
package typedefs

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// E1 是字符串 typedef，类型级校验应传播到使用它的字段。
// +k8s:validateFalse="type E1"
type E1 string

// +k8s:validateFalse="type E2"
type E2 int

// E3 是 typedef 的 typedef（E1 的别名类型），用于验证多级传播。
// +k8s:validateFalse="type E3"
type E3 E1

// E4 是基于结构体 typedef 的 typedef。
// +k8s:validateFalse="type E4"
type E4 T2

// T1 用值/指针两种方式引用上述 typedef，验证两路径都生效。
// +k8s:validateFalse="type T1"
type T1 struct {
	TypeMeta int

	// +k8s:validateFalse="field T1.E1"
	E1 E1 `json:"e1"`
	// +k8s:validateFalse="field T1.PE1"
	PE1 *E1 `json:"pe1"`

	// +k8s:validateFalse="field T1.E2"
	E2 E2 `json:"e2"`
	// +k8s:validateFalse="field T1.PE2"
	PE2 *E2 `json:"pe2"`

	// +k8s:validateFalse="field T1.E3"
	E3 E3 `json:"e3"`
	// +k8s:validateFalse="field T1.PE3"
	PE3 *E3 `json:"pe3"`

	// +k8s:validateFalse="field T1.E4"
	E4 E4 `json:"e4"`
	// +k8s:validateFalse="field T1.PE4"
	PE4 *E4 `json:"pe4"`

	// +k8s:validateFalse="field T1.T2"
	T2 T2 `json:"t2"`
	// +k8s:validateFalse="field T1.PT2"
	PT2 *T2 `json:"pt2"`
}

// T2 是带类型级校验的结构体 typedef 目标。
// +k8s:validateFalse="type T2"
type T2 struct {
	// +k8s:validateFalse="field T2.S"
	S string `json:"s"`
}
