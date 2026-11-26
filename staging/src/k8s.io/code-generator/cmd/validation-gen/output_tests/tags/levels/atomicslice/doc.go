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

// Package atomicslice 是 validation-gen 的测试包，覆盖原子切片的校验等级：
// alpha/beta 包装可分别作用于 eachVal 或作用于 eachVal 内部的校验本身。
// +k8s:validation-gen-nolint
package atomicslice

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type AtomicSliceStruct struct {
	TypeMeta int

	// 用例：标准 eachVal
	// +k8s:listType=atomic
	// +k8s:eachVal=+k8s:minimum=10
	Standard []int `json:"standard"`

	// 用例：alpha eachVal
	// +k8s:listType=atomic
	// +k8s:alpha=+k8s:eachVal=+k8s:minimum=10
	Alpha []int `json:"Alpha"`

	// 用例：beta eachVal
	// +k8s:listType=atomic
	// +k8s:beta=+k8s:eachVal=+k8s:minimum=10
	Beta []int `json:"Beta"`

	// 用例：标准 eachVal + alpha 校验
	// +k8s:listType=atomic
	// +k8s:eachVal=+k8s:alpha=+k8s:minimum=10
	AlphaValidation []int `json:"AlphaValidation"`

	// 用例：标准 eachVal + beta 校验
	// +k8s:listType=atomic
	// +k8s:eachVal=+k8s:beta=+k8s:minimum=10
	BetaValidation []int `json:"BetaValidation"`
}
