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

// +k8s:validation-gen-nolint
// 验证 alpha/beta 稳定性级别标记在结构体字段上的行为，以及条件启用
// （ifEnabled）与递归嵌套标记的传播。
package structs

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// MixedStruct 对比 alpha/beta 对同一字段施加的不同阈值。
type MixedStruct struct {
	TypeMeta int

	// 基础 minimum=5，alpha 级别额外要求 minimum=10。
	// +k8s:minimum=5
	// +k8s:alpha=+k8s:minimum=10
	IntField int `json:"intField"`

	// 同上，但使用 beta 级别。
	// +k8s:minimum=5
	// +k8s:beta=+k8s:minimum=10
	IntFieldBeta int `json:"intFieldBeta"`

	// alpha 级别收紧列表长度上限。
	// +k8s:maxItems=5
	// +k8s:alpha=+k8s:maxItems=3
	ListField []string `json:"listField"`

	// beta 级别收紧列表长度上限。
	// +k8s:maxItems=5
	// +k8s:beta=+k8s:maxItems=3
	ListFieldBeta []string `json:"listFieldBeta"`
}

// ConditionalStruct 验证 ifEnabled 条件与稳定性级别、递归标记的组合。
type ConditionalStruct struct {
	TypeMeta int

	// 仅在特性 MyFeature 启用时生效的 alpha 校验。
	// +k8s:alpha=+k8s:ifEnabled(MyFeature)=+k8s:minimum=10
	ConditionalField int `json:"conditionalField"`

	// 仅在特性 MyFeature 启用时生效的 beta 校验。
	// +k8s:beta=+k8s:ifEnabled(MyFeature)=+k8s:minimum=10
	ConditionalFieldBeta int `json:"conditionalFieldBeta"`

	// 嵌套的 alpha 标记：内层级别应被外层覆盖/合并。
	// +k8s:alpha=+k8s:alpha=+k8s:minimum=20
	RecursiveAlpha int `json:"recursiveAlpha"`

	// 嵌套的 beta 标记。
	// +k8s:beta=+k8s:beta=+k8s:minimum=20
	RecursiveBeta int `json:"recursiveBeta"`
}
