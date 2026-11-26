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

// 这是一个测试 +k8s:update 标签的包。
// +k8s:validation-gen-nolint
// 覆盖 NoSet/NoUnset/NoModify 及其组合在所有标量、指针、typedef 与不可比较结构体
// 上的生成；多个 update 标签必须合并为单次调用，而不是重复校验。
package primitives

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type UpdateTestStruct struct {
	TypeMeta int

	// +k8s:update=NoSet
	StringNoSet string `json:"stringNoSet"`

	// +k8s:update=NoUnset
	StringNoUnset string `json:"stringNoUnset"`

	// +k8s:update=NoModify
	StringNoModify string `json:"stringNoModify"`

	// +k8s:update=NoSet
	// +k8s:update=NoModify
	// +k8s:update=NoUnset
	StringFullyRestricted string `json:"stringFullyRestricted"`

	// +k8s:update=NoModify
	// +k8s:update=NoUnset
	StringSetOnce string `json:"stringSetOnce"`

	// +k8s:update=NoModify
	IntNoModify int `json:"intNoModify"`

	// +k8s:update=NoModify
	Int32NoModify int32 `json:"int32NoModify"`

	// +k8s:update=NoModify
	Int64NoModify int64 `json:"int64NoModify"`

	// +k8s:update=NoModify
	UintNoModify uint `json:"uintNoModify"`

	// +k8s:update=NoModify
	BoolNoModify bool `json:"boolNoModify"`

	// +k8s:update=NoModify
	Float32NoModify float32 `json:"float32NoModify"`

	// +k8s:update=NoModify
	Float64NoModify float64 `json:"float64NoModify"`

	// +k8s:update=NoModify
	ByteNoModify byte `json:"byteNoModify"`

	// +k8s:update=NoModify
	StructNoModify TestStruct `json:"structNoModify"`

	// +k8s:update=NoModify
	NonComparableStructNoModify NonComparableStruct `json:"nonComparableStructNoModify"`

	// 指针字段测试

	// +k8s:update=NoSet
	PointerNoSet *string `json:"pointerNoSet"`

	// +k8s:update=NoUnset
	PointerNoUnset *string `json:"pointerNoUnset"`

	// +k8s:update=NoModify
	PointerNoModify *string `json:"pointerNoModify"`

	// +k8s:update=NoSet
	// +k8s:update=NoModify
	// +k8s:update=NoUnset
	PointerFullyRestricted *string `json:"pointerFullyRestricted"`

	// +k8s:update=NoModify
	IntPointerNoModify *int `json:"intPointerNoModify"`

	// +k8s:update=NoModify
	BoolPointerNoModify *bool `json:"boolPointerNoModify"`

	// +k8s:update=NoModify
	StructPointerNoModify *TestStruct `json:"structPointerNoModify"`

	// 类型别名测试

	// +k8s:update=NoModify
	CustomTypeNoModify CustomString `json:"customTypeNoModify"`

	// +k8s:update=NoSet
	CustomTypeNoSet CustomInt `json:"customTypeNoSet"`
}

type TestStruct struct {
	StringField string `json:"stringField"`
	IntField    int    `json:"intField"`
}

// NonComparableStruct 含 slice，因此不可比较
type NonComparableStruct struct {
	SliceField []string `json:"sliceField"`
	IntField   int      `json:"intField"`
}

// 用于测试类型别名的自定义类型
type CustomString string
type CustomInt int
