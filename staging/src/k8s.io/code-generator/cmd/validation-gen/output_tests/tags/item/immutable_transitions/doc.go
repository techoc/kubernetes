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

// 这是一个测试包：验证 item 标签与 immutable 组合时的更新/状态迁移语义，
// 包括对指定 key 的整体不可变以及对 item 子字段的不可变。
// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
package transitions

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 是一个 map 语义列表：key1=a 的元素整体不可变，
// key1=b 的元素只要求 stringField 不可变。
type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// +k8s:listType=map
	// +k8s:listMapKey=key1
	// +k8s:item(key1: a)=+k8s:immutable
	// +k8s:item(key1: b)=+k8s:subfield(stringField)=+k8s:immutable
	ListField []Item `json:"listField"`
}

// Item 是列表元素类型；Key1 用 JSON 名参与 key 匹配。
type Item struct {
	Key1        string `json:"key1"`
	StringField string `json:"stringField"`
}
