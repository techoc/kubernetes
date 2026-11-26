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

// 这是一个测试包。
// +k8s:validation-gen-nolint
//
// 本包验证 minItems 作用在结构体 slice 上，并对比普通元素与 typedef 元素；
// minItems=0 不应产生任何校验。
package sliceofstruct

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:minItems=0
	Min0Field []OtherStruct `json:"min0Field"`

	// +k8s:minItems=10
	Min10Field []OtherStruct `json:"min10Field"`

	// +k8s:minItems=0
	Min0TypedefField []OtherTypedefStruct `json:"min0TypedefField"`

	// +k8s:minItems=10
	Min10TypedefField []OtherTypedefStruct `json:"min10TypedefField"`
}

type OtherStruct struct{}

type OtherTypedefStruct OtherStruct
