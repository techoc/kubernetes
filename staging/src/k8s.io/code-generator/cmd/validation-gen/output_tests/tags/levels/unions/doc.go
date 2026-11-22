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
// 验证 alpha/beta 稳定性级别标记在 union 相关标记（unionDiscriminator、
// unionMember、zeroOrOneOfMember）上的传播。
package unions

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 是 alpha 级别的 discriminated union 测试。
type Struct struct {
	TypeMeta int

	// 判别字段为 alpha 级别的 unionDiscriminator。
	// +k8s:alpha=+k8s:unionDiscriminator
	D D `json:"d"`

	// +k8s:alpha=+k8s:unionMember
	// +k8s:optional
	M1 *M1 `json:"m1"`

	// 带 since 版本的 alpha 成员。
	// +k8s:alpha(since:"1.35")=+k8s:unionMember
	// +k8s:optional
	M2 *M2 `json:"m2"`
}

// UnionStructBeta 是 beta 级别的 discriminated union 测试。
type UnionStructBeta struct {
	TypeMeta int

	// +k8s:beta=+k8s:unionDiscriminator
	DBeta BetaD `json:"dBeta"`

	// +k8s:beta=+k8s:unionMember
	// +k8s:optional
	M1Beta *BetaM1 `json:"m1Beta"`

	// +k8s:beta(since:"1.35")=+k8s:unionMember
	// +k8s:optional
	M2Beta *BetaM2 `json:"m2Beta"`
}

// MyStruct 是 alpha 级别的 zero-or-one-of union 测试。
type MyStruct struct {
	TypeMeta int

	// +k8s:alpha=+k8s:zeroOrOneOfMember
	// +k8s:optional
	Z1 *Z1 `json:"z1"`

	// +k8s:alpha=+k8s:zeroOrOneOfMember
	// +k8s:optional
	Z2 *Z2 `json:"z2"`
}

// MyStructBeta 是 beta 级别的 zero-or-one-of union 测试。
type MyStructBeta struct {
	TypeMeta int

	// +k8s:beta=+k8s:zeroOrOneOfMember
	// +k8s:optional
	Z1Beta *BetaZ1 `json:"z1Beta"`

	// +k8s:beta=+k8s:zeroOrOneOfMember
	// +k8s:optional
	Z2Beta *BetaZ2 `json:"z2Beta"`
}

// D 是 alpha union 的判别值类型。
type D string

const (
	DM1 D = "M1"
	DM2 D = "M2"
)

type M1 struct{}
type M2 struct{}

// BetaD 是 beta union 的判别值类型。
type BetaD string

const (
	BetaDM1 BetaD = "M1Beta"
	BetaDM2 BetaD = "M2Beta"
)

type BetaM1 struct{}
type BetaM2 struct{}

type Z1 struct{}
type Z2 struct{}

type BetaZ1 struct{}
type BetaZ2 struct{}

// MyListStruct 用 item 选择器在列表元素上声明 alpha zeroOrOneOfMember。
type MyListStruct struct {
	TypeMeta int

	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:alpha=+k8s:item(name: "succeeded")=+k8s:zeroOrOneOfMember
	// +k8s:alpha=+k8s:item(name: "failed")=+k8s:zeroOrOneOfMember
	Tasks []Task `json:"tasks"`
}

// MyListStructBeta 与 MyListStruct 对应，但使用 beta 级别。
type MyListStructBeta struct {
	TypeMeta int

	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:beta=+k8s:item(name: "succeeded")=+k8s:zeroOrOneOfMember
	// +k8s:beta=+k8s:item(name: "failed")=+k8s:zeroOrOneOfMember
	TasksBeta []Task `json:"tasksBeta"`
}

type Task struct {
	Name  string `json:"name"`
	State string `json:"state"`
}
