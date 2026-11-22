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

// 这是一个测试包：无判别器联合体——所有成员都可选，但同一时间至多一个被设置，
// 覆盖结构体、字符串、指针、slice、map 等成员形态。
// This is a test package.
// +k8s:validation-gen-nolint
package simple

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Non-discriminated union
// 无判别器联合体：M1~M6 为互斥成员，NonUnionField 不参与联合体。
type Struct struct {
	TypeMeta int

	NonUnionField string `json:"nonUnionField"`

	// +k8s:unionMember
	// +k8s:optional
	M1 *M1 `json:"m1"`

	// +k8s:unionMember
	// +k8s:optional
	M2 *M2 `json:"m2"`

	// +k8s:unionMember
	// +k8s:optional
	M3 string `json:"m3"`

	// +k8s:unionMember
	// +k8s:optional
	M4 *string `json:"m4"`

	// +k8s:unionMember
	// +k8s:optional
	M5 []string `json:"m5"`

	// +k8s:unionMember
	// +k8s:optional
	M6 map[string]string `json:"m6"`
}

type M1 struct{}

type M2 struct{}
