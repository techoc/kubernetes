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

// This is a test package.
//
// 覆盖 +k8s:neq 对 int：普通字段、指针字段以及类型级声明的 typedef 都要
// 拒绝等于指定值的输入。
// +k8s:validation-gen-nolint
package neqint

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:neq=0
	IntField int `json:"intField"`

	// +k8s:neq=-1
	IntPtrField *int `json:"intPtrField"`

	// +k8s:neq=42
	IntTypedefField IntType `json:"intTypedefField"`

	ValidatedTypedefField ValidatedIntType `json:"validatedTypedefField"`
}

type IntType int

// +k8s:neq=100
type ValidatedIntType int
