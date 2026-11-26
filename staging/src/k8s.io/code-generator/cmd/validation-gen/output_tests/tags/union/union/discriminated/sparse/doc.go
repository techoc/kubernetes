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

// 这是一个测试包。
// sparse 是“稀疏”discriminated union：存在没有对应成员字段的
// discriminator 取值，验证选中该值时不会错误地要求字段存在。
// +k8s:validation-gen-nolint
package sparse

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 判别式联合体
type Struct struct {
	TypeMeta int

	// +k8s:unionDiscriminator
	D D `json:"d"`

	// +k8s:unionMember
	// +k8s:optional
	M1 *M1 `json:"m1"`

	// 注意：没有与 M2 discriminator 对应的 M2 字段。
}

type D string

const (
	DM1 D = "M1"
	DM2 D = "M2"
)

type M1 struct{}
