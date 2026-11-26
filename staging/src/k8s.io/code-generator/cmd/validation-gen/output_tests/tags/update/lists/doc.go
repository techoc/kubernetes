/*
Copyright The Kubernetes Authors.

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
//
// 覆盖 +k8s:update 的 NoSet/NoUnset/NoAddItem/NoRemoveItem/NoModify：
// 不同 listType（atomic/set/map）选择不同的元素匹配策略，
// 复合键与 typedef 列表也在覆盖范围内。
// +k8s:validation-gen-nolint
package lists

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type UpdateListStruct struct {
	TypeMeta int

	// slice 的 NoSet/NoUnset（len == 0 语义，无需匹配函数）。

	// +k8s:listType=atomic
	// +k8s:update=NoSet
	StringSliceNoSet []string `json:"stringSliceNoSet"`

	// +k8s:listType=atomic
	// +k8s:update=NoUnset
	StringSliceNoUnset []string `json:"stringSliceNoUnset"`

	// listType=set → 可直接比较的元素使用 DirectEqual 匹配。

	// +k8s:listType=set
	// +k8s:update=NoAddItem
	StringSetNoAdd []string `json:"stringSetNoAdd"`

	// +k8s:listType=set
	// +k8s:update=NoRemoveItem
	StringSetNoRemove []string `json:"stringSetNoRemove"`

	// +k8s:listType=set
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	StringSetFrozenShape []string `json:"stringSetFrozenShape"`

	// listType=map → 内联 listMapKey 比较闭包。

	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:update=NoAddItem
	MapListNoAdd []UpdateItem `json:"mapListNoAdd"`

	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	MapListFrozenShape []UpdateItem `json:"mapListFrozenShape"`

	// listType=map 且键为复合键（所有键字段按 AND 组合）。

	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:listMapKey=priority
	// +k8s:update=NoRemoveItem
	CompositeKeyList []CompositeKeyItem `json:"compositeKeyList"`

	// listType=atomic + unique={set,map} → 与非 atomic 变体相同。

	// +k8s:listType=atomic
	// +k8s:unique=set
	// +k8s:update=NoAddItem
	AtomicUniqueSetNoAdd []string `json:"atomicUniqueSetNoAdd"`

	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=name
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	AtomicUniqueMapFrozenShape []UpdateItem `json:"atomicUniqueMapFrozenShape"`

	// listType=set 且元素不可直接比较 → SemanticDeepEqual。

	// +k8s:listType=set
	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	NonComparableSetFrozenShape []NonComparableItem `json:"nonComparableSetFrozenShape"`

	// typedef 列表：列表元数据定义在类型上，字段继承它。

	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	TypedefFrozenList FrozenUserList `json:"typedefFrozenList"`

	// eachVal 组合：逐项 NoModify，列表整体结构仍可变化。

	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:eachVal=+k8s:update=NoModify
	EachValNoModifyList []UpdateItem `json:"eachValNoModifyList"`

	// 指针 slice 的 NoSet
	// +k8s:listType=atomic
	// +k8s:update=NoSet
	PointerSliceNoSet []*string `json:"pointerSliceNoSet"`

	// 指针 listType=set + NoAddItem
	// +k8s:listType=set
	// +k8s:update=NoAddItem
	PointerSetNoAdd []*string `json:"pointerSetNoAdd"`

	// 指针 listType=map + NoRemoveItem
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:update=NoRemoveItem
	PointerMapListNoRemove []*UpdateItem `json:"pointerMapListNoRemove"`
}

type UpdateItem struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type CompositeKeyItem struct {
	Name     string `json:"name"`
	Priority int    `json:"priority"`
	Value    string `json:"value"`
}

// NonComparableItem 含 slice，无法用 Go 的 == 比较。
type NonComparableItem struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

// +k8s:listType=map
// +k8s:listMapKey=name
type FrozenUserList []UpdateItem
