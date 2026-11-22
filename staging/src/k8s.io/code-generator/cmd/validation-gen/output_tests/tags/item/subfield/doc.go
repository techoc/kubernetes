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
// 验证 item 选择器与 subfield 的组合：先用 key 选中元素，再对元素的
// 子字段施加校验或不可变约束。
package subfield

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// 选中 key=target 的元素，仅校验其 stringField 子字段。
	// +k8s:listType=map
	// +k8s:listMapKey=key
	// +k8s:item(key: "target")=+k8s:subfield(stringField)=+k8s:validateFalse="item Items[key=target].stringField"
	Items []Item `json:"items"`

	// 选中 key=ratchet 的元素，对其 status 子字段施加 neq 校验，
	// 用于验证 item+subfield 组合下的 ratcheting 行为。
	// +k8s:listType=map
	// +k8s:listMapKey=key
	// +k8s:item(key: "ratchet")=+k8s:subfield(status)=+k8s:neq="forbidden"
	RatchetItems []RatchetItem `json:"ratchetItems"`
}

// Item 是带 key 与 stringField 的简单元素。
type Item struct {
	Key         string `json:"key"`
	StringField string `json:"stringField"`
}

// RatchetItem 含 status/version，用于测试子字段校验在更新时的 ratcheting。
type RatchetItem struct {
	Key     string `json:"key"`
	Status  string `json:"status"`
	Version int    `json:"version"`
}
