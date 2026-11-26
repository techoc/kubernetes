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

// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
// 验证 +k8s:eachVal 作用在“切片的 typedef”上：校验标记定义在类型上，
// 使用该类型的字段应继承逐元素校验。
package typedeftoslice

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// UnvalidatedType 没有校验，用于对照。
// 注意：此处没有校验。
type UnvalidatedType []string

// ListType 在类型上声明 eachVal，[]string 的每个元素都会被校验。
// +k8s:eachVal=+k8s:validateFalse="type ListType[*]"
type ListType []string

// UnvalidatedPtrType 是指针元素切片且无校验，用于对照。
// 注意：此处没有校验。
type UnvalidatedPtrType []*string

// StringType 作为元素 typedef，用于验证元素为 typedef 时的类型映射。
type StringType string

// ListTypedefType 在 typedef 切片上声明 eachVal，元素是另一个 typedef。
// +k8s:eachVal=+k8s:validateFalse="type ListTypedefType[*]"
type ListTypedefType []StringType

// Struct 把上述 typedef 用作字段，验证类型级校验会传播到字段。
type Struct struct {
	TypeMeta int

	// 字段级 eachVal 叠加在类型级校验之上。
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListField[*]"
	ListField ListType `json:"listField"`

	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListTypedefField[*]"
	ListTypedefField ListTypedefType `json:"listTypedefField"`
}
