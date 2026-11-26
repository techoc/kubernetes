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
// +k8s:validation-gen-nolint
// 覆盖 +k8s:maxBytes：边界值 0 与 10、值/指针/typedef 的排列组合，
// 并验证类型定义上的标记会传递到所有使用该类型的字段。
package maxbytes

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:maxBytes=0
	Max0Field string `json:"max0Field"`

	// +k8s:maxBytes=0
	Max0PtrField *string `json:"max0PtrField"`

	// +k8s:maxBytes=10
	Max10Field string `json:"max10Field"`

	// +k8s:maxBytes=10
	Max10PtrField *string `json:"max10PtrField"`

	// +k8s:maxBytes=0
	Max0UnvalidatedTypedefField UnvalidatedStringType `json:"max0UnvalidatedTypedefField"`

	// +k8s:maxBytes=0
	Max0UnvalidatedTypedefPtrField *UnvalidatedStringType `json:"max0UnvalidatedTypedefPtrField"`

	// +k8s:maxBytes=10
	Max10UnvalidatedTypedefField UnvalidatedStringType `json:"max10UnvalidatedTypedefField"`

	// +k8s:maxBytes=10
	Max10UnvalidatedTypedefPtrField *UnvalidatedStringType `json:"max10UnvalidatedTypedefPtrField"`

	// 注意：这里没有校验。
	Max0ValidatedTypedefField Max0Type `json:"max0ValidatedTypedefField"`

	// 注意：这里没有校验。
	Max0ValidatedTypedefPtrField *Max0Type `json:"max0ValidatedTypedefPtrField"`

	// 注意：这里没有校验。
	Max10ValidatedTypedefField Max10Type `json:"max10ValidatedTypedefField"`

	// 注意：这里没有校验。
	Max10ValidatedTypedefPtrField *Max10Type `json:"max10ValidatedTypedefPtrField"`
}

// 注意：这里没有校验。
type UnvalidatedStringType string

// 验证类型定义上的标签会贯穿到使用该类型的字段校验中。
// +k8s:maxBytes=0
type Max0Type string

// 验证类型定义上的标签会贯穿到使用该类型的字段校验中。
// +k8s:maxBytes=10
type Max10Type string
