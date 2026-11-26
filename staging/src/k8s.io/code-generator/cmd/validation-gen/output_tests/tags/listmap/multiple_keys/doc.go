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
//
// 本包验证多 listMapKey 下的 item 匹配与 eachVal 语义：
// 包括 typedef 列表、指针 key（需解引用）、可比较与不可比较元素。
package multiplekeys

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 的每个字段覆盖一种 listType=map 组合。
type Struct struct {
	TypeMeta int

	// +k8s:listType=map
	// +k8s:listMapKey=key1Field
	// +k8s:listMapKey=key2Field
	// +k8s:eachVal=+k8s:immutable
	ListField []OtherStruct `json:"listField"`

	// +k8s:listType=map
	// +k8s:listMapKey=key1Field
	// +k8s:listMapKey=key2Field
	// +k8s:eachVal=+k8s:immutable
	ListTypedefField []OtherTypedefStruct `json:"listTypedefField"`

	// 类型级 listMapKey 已定义在 ListType 上，字段只需补充 eachVal。
	// +k8s:eachVal=+k8s:immutable
	TypedefField ListType `json:"typedefField"`

	// +k8s:listType=map
	// +k8s:listMapKey=key1Field
	// +k8s:listMapKey=key2Field
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListComparableField[*]"
	ListComparableField []OtherStruct `json:"listComparableField"`

	// 不可比较元素要求 ratcheting 比较走深比较。
	// +k8s:listType=map
	// +k8s:listMapKey=key1Field
	// +k8s:listMapKey=key2Field
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListNonComparableField[*]"
	ListNonComparableField []NonComparableStruct `json:"listNonComparableField"`

	// item 匹配条件里的指针 key 需按解引用后的值比较。
	// +k8s:listType=map
	// +k8s:listMapKey=key1Field
	// +k8s:listMapKey=key2Field
	// +k8s:item(key1Field: "target-ptr", key2Field: 42)=+k8s:validateFalse="item ListPtrKeyField[key1Field=target-ptr,key2Field=42]"
	ListPtrKeyField []PtrKeyStruct `json:"listPtrKeyField"`

	// +k8s:listType=map
	// +k8s:listMapKey=stringPtrKey
	// +k8s:listMapKey=stringKey
	// +k8s:item(stringPtrKey: "target-ptr", stringKey: "target")=+k8s:validateFalse="item ListMixedPtrKeyField"
	ListMixedPtrKeyField []MixedPtrKeyStruct `json:"listMixedPtrKeyField"`
}

type OtherStruct struct {
	Key1Field string `json:"key1Field"`
	Key2Field int    `json:"key2Field"`
	DataField string `json:"dataField"`
}

type OtherTypedefStruct OtherStruct

// NonComparableStruct 含有 slice 字段，不能直接用 == 比较。
type NonComparableStruct struct {
	Key1Field string   `json:"key1Field"`
	Key2Field int      `json:"key2Field"`
	DataField []string `json:"dataField"`
}

// +k8s:listType=map
// +k8s:listMapKey=key1Field
// +k8s:listMapKey=key2Field
type ListType []OtherStruct

type PtrKeyStruct struct {
	Key1Field *string `json:"key1Field"`
	Key2Field int     `json:"key2Field"`
	DataField string  `json:"dataField"`
}

type MixedPtrKeyStruct struct {
	StringPtrKey *string `json:"stringPtrKey"`
	StringKey    string  `json:"stringKey"`
	DataField    string  `json:"dataField"`
}
