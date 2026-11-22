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

// This is a test package for complex neq compositions.
// +k8s:validation-gen-nolint
// 验证 neq 与 subfield/eachVal/eachKey 的链式组合，以及声明在 typedef 上的
// 校验如何传播到使用该类型的字段。
package neqchained

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// subfield：只对结构体的 stringField 做 neq 校验。
	// +k8s:subfield(stringField)=+k8s:neq="disallowed-subfield"
	StructField InnerStruct `json:"structField"`

	// 指针结构体同样支持 subfield 链。
	// +k8s:optional
	// +k8s:subfield(stringField)=+k8s:neq="disallowed-subfield-ptr"
	StructPtrField *InnerStruct `json:"structPtrField"`

	// eachVal：校验切片每个元素。
	// +k8s:eachVal=+k8s:neq="disallowed-slice"
	StringSliceField []string `json:"stringSliceField"`

	// eachVal 作用在 map value 上。
	// +k8s:eachVal=+k8s:neq="disallowed-map-val"
	StringMapField map[string]string `json:"stringMapField"`

	// eachKey 作用在 map key 上。
	// +k8s:eachKey=+k8s:neq="disallowed-key"
	StringMapKeyField map[string]string `json:"stringMapKeyField"`

	// 校验声明在 typedef 上，字段继承。
	ValidatedSliceField ValidatedStringSlice `json:"validatedSliceField"`

	ValidatedStructField ValidatedInnerStruct `json:"validatedStructField"`
}

// InnerStruct 是被 subfield 选中的结构体。
type InnerStruct struct {
	StringField string `json:"stringField"`
}

// ValidatedStringSlice 在类型级声明 eachVal+neq。
// +k8s:eachVal=+k8s:neq="disallowed-typedef"
type ValidatedStringSlice []string

// ValidatedInnerStruct 在类型级声明 subfield+neq。
// +k8s:subfield(stringField)=+k8s:neq="disallowed-typedef-struct"
type ValidatedInnerStruct InnerStruct
