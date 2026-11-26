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

// Package shallow 包含用于测试 subfield 字段校验标签的类型。
//
// 覆盖浅层子字段校验：通过 +k8s:subfield(name) 把校验绑定到结构体
// （或其指针）的指定子字段上。
// +k8s:validation-gen-nolint
package shallow

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 演示结构体 subfield 字段的校验。
type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// +k8s:subfield(stringField)=+k8s:validateFalse="subfield Struct.StructField.StringField 1"
	// +k8s:subfield(stringField)=+k8s:validateFalse="subfield Struct.StructField.StringField 2"
	// +k8s:subfield(pointerField)=+k8s:validateFalse="subfield Struct.StructField.PointerField"
	// +k8s:subfield(structField)=+k8s:validateFalse="subfield Struct.StructField.StructField"
	// +k8s:subfield(sliceField)=+k8s:validateFalse="subfield Struct.StructField.SliceField"
	// +k8s:subfield(mapField)=+k8s:validateFalse="subfield Struct.StructField.MapField"
	StructField OtherStruct `json:"structField"`

	// +k8s:subfield(stringField)=+k8s:validateFalse="subfield Struct.StructPtrField.StringField 1"
	// +k8s:subfield(stringField)=+k8s:validateFalse="subfield Struct.StructPtrField.StringField 2"
	// +k8s:subfield(pointerField)=+k8s:validateFalse="subfield Struct.StructPtrField.PointerField"
	// +k8s:subfield(structField)=+k8s:validateFalse="subfield Struct.StructPtrField.StructField"
	// +k8s:subfield(sliceField)=+k8s:validateFalse="subfield Struct.StructPtrField.SliceField"
	// +k8s:subfield(mapField)=+k8s:validateFalse="subfield Struct.StructPtrField.MapField"
	StructPtrField *OtherStruct `json:"structPtrField"`
}

type OtherStruct struct {
	StringField  string            `json:"stringField"`
	PointerField *string           `json:"pointerField"`
	StructField  SmallStruct       `json:"structField"`
	SliceField   []string          `json:"sliceField"`
	MapField     map[string]string `json:"mapField"`
}

type SmallStruct struct {
	StringField string `json:"stringField"`
}
