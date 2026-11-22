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
// +k8s:validation-gen-test-targets

// This is a test package.
// +k8s:validation-gen-nolint
//
// 本包验证“无校验时不产出空壳校验函数”的裁剪逻辑：
// 未被链接（NotLinked）的类型不应进入生成结果，而字段/类型级校验混用时
// 生成器需要沿 typedef 链正确下钻。
package elidenovalidations

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// T1 是测试入口类型，内嵌 TypeMeta 以满足 TypesWithField=TypeMeta 的筛选条件。
type T1 struct {
	TypeMeta int

	HasTypeVal HasTypeVal `json:"hasTypeVal"`

	HasFieldVal HasFieldVal `json:"hasFieldVal"`

	HasNoVal HasNoVal `json:"hasNoVal"`

	// +k8s:validateFalse="field T1.HasNoValFieldVal"
	HasNoValFieldVal HasNoVal `json:"hasNoValFieldVal"`

	ValidatedSlice  TypedefSliceWithValidations  `json:"validatedSlice"`
	ValidatedMap    TypedefMapWithValidations    `json:"validatedMap"`
	ValidatedMapKey TypedefMapWithKeyValidations `json:"validatedMapKey"`

	DeepValidatedSlice DeepTypedefSlice `json:"deepValidatedSlice"`
	DeepValidatedMap   DeepTypedefMap   `json:"deepValidatedMap"`

	DoubleDeepValidatedSlice DoubleDeepTypedefSlice `json:"doubleDeepValidatedSlice           "`
	DoubleDeepValidatedMap   DoubleDeepTypedefMap   `json:"doubleDeepValidatedMap"`
}

// +k8s:validateFalse="type HasTypeVal"
type HasTypeVal struct {
	// Note: no field validation.
	S string `json:"s"`
}

// Note: no type validation.
type HasFieldVal struct {
	// +k8s:validateFalse="field HasFieldVal.S"
	S string `json:"s"`
}

// Note: no type validation.
type HasNoVal struct {
	// Note: no field validation.
	S string `json:"s"`
}

// +k8s:validateFalse="type HasTypeValNotLinked"
type HasTypeValNotLinked struct {
	// Note: no field validation.
	S string `json:"s"`
}

// Note: no type validation.
type HasFieldValNotLinked struct {
	// +k8s:validateFalse="field HasFieldValNotLinked.S"
	S string `json:"s"`
}

// Note: no type validation.
type HasNoValNotLinked struct {
	// Note: no field validation.
	S string `json:"s"`
}

// +k8s:validateFalse="type OtherStruct"
type OtherStruct struct {
	S string `json:"s"`
}

// +k8s:validateFalse="type ValidatedKeyType"
type ValidatedKeyType string

type TypedefSliceWithValidations []OtherStruct

type TypedefMapWithValidations map[string]OtherStruct

type TypedefMapWithKeyValidations map[ValidatedKeyType]string

// FIXME: The following validation is not being generated for DoubleDeepTypedefSlice.
// Validation-gen is ignoring this validation, because Go directly translates
// DoubleDeepTypedefSlice to TypedefSliceWithValidations.
// +k8s:eachVal=+k8s:validateFalse="type DeepTypedefSlice"
type DeepTypedefSlice TypedefSliceWithValidations

type DeepTypedefMap TypedefMapWithValidations

type DoubleDeepTypedefSlice DeepTypedefSlice

type DoubleDeepTypedefMap DeepTypedefMap
