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

// // 本包是测试夹具，用于验证生成器的行为。
//
// 覆盖校验标签的 typeArg 参数：显式指定类型参数后，生成代码应以该类型
// 渲染（且必须是指针，见下方 T1 的说明）。
// +k8s:validation-gen-nolint
package typeargs

import (
	"k8s.io/code-generator/cmd/validation-gen/output_tests/primitives"
	"k8s.io/code-generator/cmd/validation-gen/testscheme"
)

var localSchemeBuilder = testscheme.New()

// 显式指定类型实参，以验证渲染正确。
// 注意：受校验代码生成方式所限，这些必须始终是指针，
// 因为传递的就是指针。
type T1 struct {
	TypeMeta int

	// +k8s:validateFalse(typeArg: "k8s.io/code-generator/cmd/validation-gen/output_tests/primitives.T1")="T1.S1"
	S1 *primitives.T1 `json:"s1"`
	// +k8s:validateFalse(typeArg: "k8s.io/code-generator/cmd/validation-gen/output_tests/primitives.T1")="PT1.PS1"
	PS1 *primitives.T1 `json:"ps1"`

	// +k8s:validateFalse(typeArg: "k8s.io/code-generator/cmd/validation-gen/output_tests/type_args.E1")="T1.E1"
	E1 E1 `json:"e1"`
	// +k8s:validateTrue(typeArg: "k8s.io/code-generator/cmd/validation-gen/output_tests/type_args.E1")="T1.PE1"
	PE1 *E1 `json:"pe1"`

	// +k8s:validateFalse(typeArg: "int")="T1.I1"
	I1 int `json:"i1"`
	// +k8s:validateTrue(typeArg: "int")="T1.PI1"
	PI1 *int `json:"pi1"`
}

// +k8s:validateFalse="type E1"
type E1 string
