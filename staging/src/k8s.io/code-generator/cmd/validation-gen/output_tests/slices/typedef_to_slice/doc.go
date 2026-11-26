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
// 覆盖“typedef 到 slice”的映射：slice 类型本身可以携带 +k8s:eachVal，
// 也可以作为没有校验的普通类型（Unvalidated*）。
// +k8s:validation-gen-nolint
package typedeftoslice

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 注意：此处没有校验。
type UnvalidatedType []string

// 注意：此处没有校验。
type UnvalidatedPtrType []*string

// +k8s:validateFalse="type ListType"
// +k8s:eachVal=+k8s:validateFalse="type ListType[*]"
type ListType []string

// +k8s:validateFalse="type ListPtrType"
// +k8s:eachVal=+k8s:validateFalse="type ListPtrType[*]"
type ListPtrType []*string

// +k8s:validateFalse="type ListTypedefType"
// +k8s:eachVal=+k8s:validateFalse="type ListTypedefType[*]"
type ListTypedefType []StringType

// +k8s:validateFalse="type ListPtrTypedefType"
// +k8s:eachVal=+k8s:validateFalse="type ListPtrTypedefType[*]"
type ListPtrTypedefType []*StringType

// +k8s:validateFalse="type StringType"
type StringType string

// +k8s:validateFalse="type Struct"
type Struct struct {
	TypeMeta int

	// +k8s:validateFalse="field Struct.ListField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListField[*]"
	ListField ListType `json:"listField"`

	// +k8s:validateFalse="field Struct.ListTypedefField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListTypedefField[*]"
	ListTypedefField ListTypedefType `json:"listTypedefField"`

	UnvalidatedListField UnvalidatedType `json:"UnvalidatedListField"`

	// +k8s:validateFalse="field Struct.ListPtrField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListPtrField[*]"
	ListPtrField ListPtrType `json:"listPtrField"`

	// +k8s:validateFalse="field Struct.ListPtrTypedefField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListPtrTypedefField[*]"
	ListPtrTypedefField ListPtrTypedefType `json:"listPtrTypedefField"`

	UnvalidatedListPtrField UnvalidatedPtrType `json:"UnvalidatedListPtrField"`
}
