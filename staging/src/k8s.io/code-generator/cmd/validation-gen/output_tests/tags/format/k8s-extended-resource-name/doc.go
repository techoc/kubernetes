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

// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
//
// 本包验证 k8s-extended-resource-name 格式校验在字段、指针字段和
// typedef 三种写法下的生成结果。
package k8sextendedresourcename

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// MyType 是含 extended-resource-name 格式字段的结构体。
// +k8s:validation:Required
type MyType struct {
	TypeMeta int
	// +k8s:optional
	// +k8s:format=k8s-extended-resource-name
	NameField string `json:"nameField"`
	// 指针字段需先判空再校验格式。
	// +k8s:optional
	// +k8s:format=k8s-extended-resource-name
	NamePtrField *string `json:"namePtrField"`
	// 注意：此处没有校验。
	NameTypedefField NameStringType `json:"nameTypedefField"`
}

// +k8s:format=k8s-extended-resource-name
type NameStringType string
