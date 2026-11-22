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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// 这是一个测试包：验证 ratcheting（已存对象跳过既有非法值）与 subfield
// 标签的组合——subfield 校验在旧值相同的情况下不应再次触发。
// This is a test package.
// +k8s:validation-gen-nolint
package subfield

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 通过 subfield 把校验下沉到子结构体字段。
type Struct struct {
	TypeMeta int

	// +k8s:subfield(intField)=+k8s:validateFalse="field IntField"
	// +k8s:subfield(intPtrField)=+k8s:validateFalse="field IntPtrField"
	SubStructField SubStruct `json:"subStructField"`
}

type SubStruct struct {
	IntField    int  `json:"intField"`
	IntPtrField *int `json:"intPtrField"`
}

// +k8s:subfield(intField)=+k8s:validateFalse="field IntField"
// +k8s:subfield(intPtrField)=+k8s:validateFalse="field IntPtrField"
// StructWithSubfield 把 subfield 标签直接写在类型上，用来对比
// 标签写在字段上的情形；两者在生成的校验路径上应保持一致。
type StructWithSubfield struct {
	TypeMeta    int
	IntField    int  `json:"intField"`
	IntPtrField *int `json:"intPtrField"`
}
