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

// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
// 验证 +k8s:item 多 key 选择器：listMapKey 可声明多个 key，
// 且 item 的 key 参数顺序与 listMapKey 不一致时也应正确匹配。
package multiplekeys

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// 三个 key 的组合定位到唯一元素；同一选择器可重复出现多条校验。
	// +k8s:listType=map
	// +k8s:listMapKey=stringKey
	// +k8s:listMapKey=intKey
	// +k8s:listMapKey=boolKey
	// +k8s:item(stringKey: "target", intKey: 42, boolKey: true)=+k8s:validateFalse="item Items[stringKey=target,intKey=42,boolKey=true] 1"
	// +k8s:item(stringKey: "target", intKey: 42, boolKey: true)=+k8s:validateFalse="item Items[stringKey=target,intKey=42,boolKey=true] 2"
	Items []Item `json:"items"`

	// item 参数的书写顺序与 listMapKey 不同，仍应按 key 名匹配。
	// +k8s:listType=map
	// +k8s:listMapKey=stringKey
	// +k8s:listMapKey=intKey
	// +k8s:listMapKey=boolKey
	// +k8s:item(boolKey: true, stringKey: "target", intKey: 42)=+k8s:validateFalse="item OutOfOrder[boolKey=42,stringKey=target,intKey=42]"
	OutOfOrder []Item `json:"outOfOrder"`

	// 指针 key 字段也参与选择器匹配。
	// +k8s:listType=map
	// +k8s:listMapKey=stringKey
	// +k8s:listMapKey=intKey
	// +k8s:listMapKey=boolKey
	// +k8s:item(stringKey: "target-ptr", intKey: 42, boolKey: true)=+k8s:validateFalse="item PtrItems[stringKey=target-ptr,intKey=42,boolKey=true]"
	PtrItems []PtrItem `json:"ptrItems"`

	// 混合指针与非指针 key。
	// +k8s:listType=map
	// +k8s:listMapKey=stringPtrKey
	// +k8s:listMapKey=stringKey
	// +k8s:item(stringPtrKey: "target-ptr", stringKey: "target")=+k8s:validateFalse="item MixedPtrItems"
	MixedPtrItems []MixedPtrItem `json:"mixedPtrItems"`
}

// Item 是同时拥有三个可作为 map key 的字段的元素类型。
type Item struct {
	StringKey string `json:"stringKey"`
	IntKey    int    `json:"intKey"`
	BoolKey   bool   `json:"boolKey"`
	Data      string `json:"data"`
}

// PtrItem 的 StringKey 为指针，用于测试指针 key 的选择器匹配。
type PtrItem struct {
	StringKey *string `json:"stringKey"`
	IntKey    int     `json:"intKey"`
	BoolKey   bool    `json:"boolKey"`
	Data      string  `json:"data"`
}

// MixedPtrItem 同时含指针与非指针 key。
type MixedPtrItem struct {
	StringPtrKey *string `json:"stringPtrKey"`
	StringKey    string  `json:"stringKey"`
	Data         string  `json:"data"`
}
