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

// Package withfieldvalidations 是 validation-gen 的测试包：只有带 TypeMeta
// 字段的类型才会被选中（TypesWithField=TypeMeta），用于验证字段级校验
// 如何被内联到类型图中，以及未被引用的类型（T4/T5/E3/E4）不产生代码。
// +k8s:validation-gen-nolint
package withfieldvalidations

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type T1 struct {
	TypeMeta int

	// +k8s:validateFalse="field T1.S"
	S string `json:"s"`
	// +k8s:validateFalse="field T1.T2"
	T2 T2 `json:"t2"`
	// +k8s:validateFalse="field T1.T3"
	T3 T3 `json:"t3"`

	// +k8s:validateFalse="field T1.E1"
	E1 E1 `json:"e1"`
	// +k8s:validateFalse="field T1.E2"
	E2 E2 `json:"e2"`
}

// 注意：此类型有校验，且被 T1 引用。
type T2 struct {
	// +k8s:validateFalse="field T2.S"
	S string `json:"s"`
}

// 注意：此类型无校验，但被 T1 引用。
type T3 struct {
	S string `json:"s"`
}

// 注意：此类型有校验，但未被 T1 引用。
type T4 struct {
	// +k8s:validateFalse="field T4.S"
	S string `json:"s"`
}

// 注意：此类型无校验，也未被 T1 引用。
type T5 struct {
	S string `json:"s"`
}

// 注意：此类型有校验，且被 T1 引用。
// +k8s:validateFalse="type E1"
type E1 string

// 注意：此类型无校验，但被 T1 引用。
type E2 string

// 注意：此类型有校验，但未被 T1 引用。
// +k8s:validateFalse="field type E3"
type E3 string

// 注意：此类型无校验，也未被 T1 引用。
type E4 string
