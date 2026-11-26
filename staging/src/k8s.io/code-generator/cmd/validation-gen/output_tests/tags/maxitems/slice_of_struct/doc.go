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

// 这是一个测试包：验证 maxItems 作用于结构体切片及 typedef 切片。
// 这是一个测试包。
// +k8s:validation-gen-nolint
package sliceofstruct

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 同时覆盖 maxItems=0 与 maxItems=10、普通元素与 typedef 元素。
type Struct struct {
	TypeMeta int

	// +k8s:maxItems=0
	Max0Field []OtherStruct `json:"max0Field"`

	// +k8s:maxItems=10
	Max10Field []OtherStruct `json:"max10Field"`

	// +k8s:maxItems=0
	Max0TypedefField []OtherTypedefStruct `json:"max0TypedefField"`

	// +k8s:maxItems=10
	Max10TypedefField []OtherTypedefStruct `json:"max10TypedefField"`
}

type OtherStruct struct{}

type OtherTypedefStruct OtherStruct
