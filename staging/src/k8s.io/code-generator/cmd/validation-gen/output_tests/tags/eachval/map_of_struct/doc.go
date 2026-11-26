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
// 覆盖 +k8s:eachVal 作用于 map 的结构体值：值本身与 typedef 值类型都应逐元素校验，
// 路径使用 [*] 而不是具体 key。
package mapofstruct

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:eachVal=+k8s:validateFalse="field Struct.MapField[*]"
	MapField map[string]OtherStruct `json:"mapField"`

	// +k8s:eachVal=+k8s:validateFalse="field Struct.MapTypedefField[*]"
	MapTypedefField map[string]OtherTypedefStruct `json:"mapTypedefField"`
}

type OtherStruct struct{}

type OtherTypedefStruct OtherStruct
