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

// 这是一个测试包：验证 neq 标签对字符串、指针、typedef 及其指针的处理，
// 以及类型级 neq 对引用它的字段生效。
// This is a test package.
// +k8s:validation-gen-nolint
package neqstring

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 覆盖字段级 neq 与类型级 neq（ValidatedStringType）两种来源。
type Struct struct {
	TypeMeta int

	// +k8s:neq="disallowed-string"
	StringField string `json:"stringField"`

	// +k8s:neq="disallowed-pointer"
	StringPtrField *string `json:"stringPtrField"`

	// +k8s:neq="disallowed-typedef"
	StringTypedefField StringType `json:"stringTypedefField"`

	// +k8s:neq="disallowed-typedef-pointer"
	StringTypedefPtrField *StringType `json:"stringTypedefPtrField"`

	ValidatedTypedefField    ValidatedStringType  `json:"validatedTypedefField"`
	ValidatedTypedefPtrField *ValidatedStringType `json:"validatedTypedefPtrField"`
}

type StringType string

// +k8s:neq="disallowed-on-type"
type ValidatedStringType string
