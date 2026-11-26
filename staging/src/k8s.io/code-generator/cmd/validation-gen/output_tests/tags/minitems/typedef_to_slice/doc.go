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
// typedeftoslice 验证 +k8s:minItems 声明在切片类型定义上的生成行为，
// 以及未声明校验的切片别名不会被误加校验。
// +k8s:validation-gen-nolint
package typedeftoslice

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 注意：这里没有校验。
type UnvalidatedType []int

// +k8s:minItems=0
type Min0Type []int

// +k8s:minItems=10
type Min10Type []int

// 注意：这里没有校验。
type UnvalidatedPtrType []*int

type SliceType []int

// +k8s:minItems=0
type Min0TypedefType SliceType

// +k8s:minItems=10
type Min10TypedefType SliceType

type Struct struct {
	TypeMeta int

	UnvalidatedField UnvalidatedType `json:"unvalidatedField"`

	Min0Field Min0Type `json:"min0Field"`

	Min10Field Min10Type `json:"min10Field"`

	Min0TypedefField Min0TypedefType `json:"min0TypedefField"`

	Min10TypedefField Min10TypedefType `json:"min10TypedefField"`
}
