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
// 覆盖 +k8s:maxItems 在切片 typedef 上的传递：类型标记应作用于所有使用它的字段，
// 未标记的 typedef 与指针元素 typedef 不应产生校验。
package typedeftoslice

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 注意：这里没有校验。
type UnvalidatedType []int

// +k8s:maxItems=0
type Max0Type []int

// +k8s:maxItems=10
type Max10Type []int

// 注意：这里没有校验。
type UnvalidatedPtrType []*int

type SliceType []int

// +k8s:maxItems=0
type Max0TypedefType SliceType

// +k8s:maxItems=10
type Max10TypedefType SliceType

type Struct struct {
	TypeMeta int

	UnvalidatedField UnvalidatedType `json:"unvalidatedField"`

	Max0Field Max0Type `json:"max0Field"`

	Max10Field Max10Type `json:"max10Field"`

	Max0TypedefField Max0TypedefType `json:"max0TypedefField"`

	Max10TypedefField Max10TypedefType `json:"max10TypedefField"`
}
