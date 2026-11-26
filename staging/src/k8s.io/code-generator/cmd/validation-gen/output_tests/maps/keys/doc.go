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

// Package keys 是 validation-gen 的测试包，覆盖 map 键上的校验：
// 通过 +k8s:eachKey 把校验施加到键类型，包括内建键类型与各种 typedef 键类型，
// 用于确认键与值的作用域不会互相串扰。
// +k8s:validation-gen-nolint
package keys

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// +k8s:validateFalse="type Struct"
type Struct struct {
	TypeMeta int

	// +k8s:validateFalse="field Struct.MapField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapField(keys)"
	MapField map[string]string `json:"mapField"`

	// +k8s:validateFalse="field Struct.MapTypedefField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapTypedefField(keys)"
	MapTypedefField map[UnvalidatedStringType]string `json:"mapTypedefField"`

	// +k8s:validateFalse="field Struct.MapValidatedTypedefField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapValidatedTypedefField(keys)"
	MapValidatedTypedefField map[ValidatedStringType]string `json:"mapValidatedTypedefField"`

	// +k8s:validateFalse="field Struct.MapTypeField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapTypeField(keys)"
	MapTypeField UnvalidatedMapType `json:"mapTypeField"`

	// +k8s:validateFalse="field Struct.ValidatedMapTypeField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.ValidatedMapTypeField(keys)"
	ValidatedMapTypeField ValidatedMapType `json:"validatedMapTypeField"`
}

// 注意：没有校验。
type UnvalidatedStringType string

// +k8s:validateFalse="ValidatedStringType"
type ValidatedStringType string

// 注意：没有校验。
type UnvalidatedMapType map[string]string

// +k8s:validateFalse="ValidatedMapType"
// +k8s:eachKey=+k8s:validateFalse="type ValidatedMapType(keys)"
type ValidatedMapType map[string]string
