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

// This is a test package.
// +k8s:validation-gen-nolint
//
// 本包验证 eachVal 作用在结构体元素上：包括普通结构体、typedef 结构体
// 以及不可比较结构体（ratcheting 判等需要深比较）。
package sliceofstruct

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListField[*]"
	ListField []OtherStruct `json:"listField"`

	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListTypedefField[*]"
	ListTypedefField []OtherTypedefStruct `json:"listTypedefField"`

	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListNonComparableField[*]"
	ListNonComparableField []NonComparableStruct `json:"listNonComparableField"`
}

type OtherStruct struct{}

type OtherTypedefStruct OtherStruct

// NonComparableStruct 含 slice 字段，不可用 == 比较。
type NonComparableStruct struct {
	SliceField []string `json:"sliceField"`
}
