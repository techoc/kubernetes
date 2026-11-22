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
// 验证 +k8s:unique 的 set/map 两种模式，覆盖基本类型、结构体、指针 key、
// 混合 key，以及与 listType/customUnique 的组合。
package unique

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// 基本类型切片按值去重。
	// Basic unique=set on primitive slice
	// +k8s:listType=atomic
	// +k8s:unique=set
	PrimitiveListUniqueSet []string `json:"primitiveListUniqueSet"`

	// 多 key 的 map 唯一性：按 key1+key2 组合判断重复。
	// unique=map with multiple keys
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=key1
	// +k8s:listMapKey=key2
	SliceMapFieldWithMultipleKeys []ItemWithMultipleKeys `json:"sliceMapFieldWithMultipleKeys"`

	// atomic + unique=set 组合。
	// atomic + unique=set combination
	// +k8s:listType=atomic
	// +k8s:unique=set
	AtomicListUniqueSet []Item `json:"atomicListUniqueSet"`

	// atomic + unique=map 组合。
	// atomic + unique=map combination
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=key
	AtomicListUniqueMap []Item `json:"atomicListUniqueMap"`

	// customUnique 与 listType=set 组合：由自定义函数判定唯一性。
	// customUnique with listType=set
	// +k8s:listType=set
	// +k8s:customUnique
	CustomUniqueListWithTypeSet []string `json:"customUniqueListWithTypeSet"`

	// customUnique 与 listType=map 组合。
	// customUnique with listType=map
	// +k8s:listType=map
	// +k8s:listMapKey=key
	// +k8s:customUnique
	CustomUniqueListWithTypeMap []Item `json:"customUniqueListWithTypeMap"`

	// 指针 key：按指针指向的值比较。
	// unique=map with pointer key
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=key
	SliceMapFieldWithPtrKey []PtrKeyStruct `json:"sliceMapFieldWithPtrKey"`

	// 混合（指针 + 基本类型）key。
	// unique=map with mixed (pointer and primitive) keys
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=key1
	// +k8s:listMapKey=key2
	SliceMapFieldWithMixedKeys []ItemWithMixedKeys `json:"sliceMapFieldWithMixedKeys"`

	// 多个指针 key。
	// unique=map with multiple pointer keys
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=key1
	// +k8s:listMapKey=key2
	SliceMapFieldWithMultiplePtrKeys []ItemWithMultiplePtrKeys `json:"sliceMapFieldWithMultiplePtrKeys"`

	// 指向基本类型的指针切片，按值去重。
	// unique=set on slice of pointers to primitive
	// +k8s:listType=atomic
	// +k8s:unique=set
	PrimitivePointerListUniqueSet []*string `json:"primitivePointerListUniqueSet"`

	// 指向结构体的指针切片，按 map key 去重。
	// unique=map on slice of pointers to struct
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=key
	PointerListUniqueMap []*Item `json:"pointerListUniqueMap"`
}

// Item 是单 key 的元素类型。
type Item struct {
	Key  string `json:"key"`
	Data string `json:"data"`
}

// ItemWithMultipleKeys 提供两个可作为组合 key 的字段。
type ItemWithMultipleKeys struct {
	Key1 string `json:"key1"`
	Key2 string `json:"key2"`
	Data string `json:"data"`
}

// PtrKeyStruct 的 key 是指针。
type PtrKeyStruct struct {
	Key  *string `json:"key"`
	Data string  `json:"data"`
}

// ItemWithMixedKeys 混合指针与非指针 key。
type ItemWithMixedKeys struct {
	Key1 *string `json:"key1"`
	Key2 string  `json:"key2"`
	Data string  `json:"data"`
}

// ItemWithMultiplePtrKeys 提供两个指针 key。
type ItemWithMultiplePtrKeys struct {
	Key1 *string `json:"key1"`
	Key2 *string `json:"key2"`
	Data string  `json:"data"`
}
