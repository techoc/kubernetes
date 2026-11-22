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
// +k8s:validation-gen-nolint
//
// 本包验证 map 值上的 eachVal 校验在 ratcheting（旧对象放宽）模式下
// 对不同值类型的行为：基本类型、typedef、可比较与不可比较结构体。
package eachval

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// StructWithMaps 覆盖 eachVal 作用在各类 map value 上的情形。
type StructWithMaps struct {
	TypeMeta int

	// +k8s:eachVal=+k8s:validateFalse="field MapTest.MapPrimitiveField[*]"
	MapPrimitiveField map[string]string `json:"mapPrimitiveField"`

	// +k8s:eachVal=+k8s:validateFalse="field MapTest.MapTypedefField[*]"
	MapTypedefField map[string]StringType `json:"mapTypedefField"`

	// +k8s:eachVal=+k8s:validateFalse="field MapTest.MapComparableStructField[*]"
	MapComparableStructField map[string]ComparableStruct `json:"mapComparableStructField"`

	// +k8s:eachVal=+k8s:validateFalse="field MapTest.MapNonComparableStructField[*]"
	MapNonComparableStructField map[string]NonComparableStruct `json:"mapNonComparableStructField"`
}

type StringType string

// ComparableStruct 可比较，可直接用 == 判等。
type ComparableStruct struct {
	IntField int `json:"intField"`
}

// NonComparableStruct 含指针字段导致不可比较，ratcheting 比较必须走
// 深比较而非 ==，否则运行时会 panic。
type NonComparableStruct struct {
	IntPtrField *int `json:"intPtrField"`
}
