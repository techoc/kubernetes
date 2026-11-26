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

// 这是一个测试包：验证 enum 标签对没有枚举值、只有一个值、多个值
// 以及“看起来像 enum 但实际不是”的类型分别如何生成。
// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
package enum

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 覆盖各枚举类型的值与指针字段形态。
type Struct struct {
	TypeMeta int

	Enum0Field    Enum0  `json:"enum0Field"`
	Enum0PtrField *Enum0 `json:"enum0PtrField"`

	Enum1Field    Enum1  `json:"enum1Field"`
	Enum1PtrField *Enum1 `json:"enum1PtrField"`

	Enum2Field    Enum2  `json:"enum2Field"`
	Enum2PtrField *Enum2 `json:"enum2PtrField"`

	NotEnumField    NotEnum  `json:"notEnumField"`
	NotEnumPtrField *NotEnum `json:"notEnumPtrField"`
}

// +k8s:enum
// Enum0 没有枚举值，用于检验生成器对空枚举集合的处理。
type Enum0 string // Note: this enum has no values

// +k8s:enum
// Enum1 只有一个合法取值。
type Enum1 string // Note: this enum has 1 value

const (
	E1V1 Enum1 = "e1v1"
)

// +k8s:enum
// Enum2 有两个合法取值。
type Enum2 string // Note: this enum has 2 values

const (
	E2V1 Enum2 = "e2v1"
	E2V2 Enum2 = "e2v2"
)

// 注意：这不是枚举，因为常量值的类型是 Enum2，而且
// 因为 Go 会省略中间 typedef（这里建模为 "NotEnum" ->
// "string"）。
type NotEnum Enum2
