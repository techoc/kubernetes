/*
Copyright The Kubernetes Authors.

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

// +k8s:validation-gen=TypesWithSuffix=Request
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
// 覆盖 +k8s:validation-gen=TypesWithSuffix=Request：只处理以后缀结尾的类型，
// 且只有进入 StructRequest 类型图的类型才应生成校验。
package primitives

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type StructRequest struct {
	// +k8s:validateFalse="field StructRequest.S"
	S string `json:"s"`

	// +k8s:validateFalse="field StructRequest.T2"
	T2 T2 `json:"t2"`

	// 没有内部校验。
	T3 T3 `json:"t3"`
}

// 注意：此类型有校验，且被 StructRequest 的类型图引用。
type T2 struct {
	// +k8s:validateFalse="field T2.I"
	I int `json:"i"`
}

// 注意：此类型无校验，但被 StructRequest 的类型图引用。
type T3 struct {
	B bool    `json:"b"`
	F float64 `json:"f"`
}

// 注意：此类型有校验，但未被以下类型图引用：
// StructRequest。
type T4 struct {
	// +k8s:validateFalse="field T4.S"
	S string `json:"s"`
	// +k8s:validateFalse="field T4.I"
	I int `json:"i"`
	// +k8s:validateFalse="field T4.B"
	B bool `json:"b"`
	// +k8s:validateFalse="field T4.F"
	F float64 `json:"f"`
}

// +k8s:validateFalse="type EnumRequest"
type EnumRequest string
