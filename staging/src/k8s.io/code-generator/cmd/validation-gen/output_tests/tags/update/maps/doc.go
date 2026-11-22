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

// Package maps 是 validation-gen 的测试包，覆盖 map 的更新约束
// （+k8s:update=NoSet/NoUnset/NoAddItem/NoRemoveItem/NoModify）及其组合，
// 用于确认生成的 update 校验能正确区分“整体设置”与“逐项增删改”。
// +k8s:validation-gen-nolint
package maps

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type UpdateMapStruct struct {
	TypeMeta int

	// +k8s:update=NoSet
	MapNoSet map[string]string `json:"mapNoSet"`

	// +k8s:update=NoUnset
	MapNoUnset map[string]string `json:"mapNoUnset"`

	// +k8s:update=NoAddItem
	MapNoAdd map[string]string `json:"mapNoAdd"`

	// +k8s:update=NoRemoveItem
	MapNoRemove map[string]string `json:"mapNoRemove"`

	// +k8s:update=NoAddItem
	// +k8s:update=NoRemoveItem
	MapFrozenShape map[string]string `json:"mapFrozenShape"`

	// +k8s:update=NoSet
	// +k8s:update=NoAddItem
	MapSetThenFreeze map[string]MapItem `json:"mapSetThenFreeze"`

	// eachVal composition: per-value NoModify, key set can still change.

	// +k8s:eachVal=+k8s:update=NoModify
	EachValNoModifyMap map[string]MapItem `json:"eachValNoModifyMap"`
}

type MapItem struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
