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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// 这是一个测试包。
//
// 验证 +k8s:customValidation 与声明式标签可共存、可被功能开关
// （+k8s:ifEnabled）门控，并沿 typedef 字段深度传播。
// +k8s:validation-gen-nolint
package customvalidation

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// +k8s:customValidation
type Struct struct {
	TypeMeta int

	// +k8s:customValidation
	StringField string `json:"stringField"`

	// 与声明式标签配合：两种校验都会在该字段上运行。
	// +k8s:maxLength=3
	// +k8s:customValidation
	MaxLengthField string `json:"maxLengthField"`

	// StringType 在遍历中无论出现在哪里都会执行自定义校验。
	TypedefField      StringType            `json:"typedefField"`
	TypedefPtrField   *StringType           `json:"typedefPtrField,omitempty"`
	TypedefSliceField []StringType          `json:"typedefSliceField,omitempty"`
	TypedefMapField   map[string]StringType `json:"typedefMapField,omitempty"`

	StructField OtherStruct `json:"structField"`
}

// +k8s:customValidation
type StringType string

type OtherStruct struct {
	// +k8s:customValidation
	StringField string `json:"stringField"`
}

// OptionStruct 演示受特性选项控制的自定义校验。
type OptionStruct struct {
	TypeMeta int

	// +k8s:ifEnabled(FeatureX)=+k8s:customValidation
	StringField string `json:"stringField"`
}

// EachStruct 演示自定义校验（经 StringType 元素类型）与 eachVal 的声明式逐元素检查共存。
type EachStruct struct {
	TypeMeta int

	// +k8s:eachVal=+k8s:maxLength=3
	SliceField []StringType `json:"sliceField"`
}
