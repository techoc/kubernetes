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

// Package unions contains test types for testing subfield union validation tags.
// 中文说明：通过 subfield 把 unionDiscriminator/unionMember 作用到子结构体字段上。
// +k8s:validation-gen-nolint
package unions

import (
	"k8s.io/code-generator/cmd/validation-gen/testscheme"
)

var localSchemeBuilder = testscheme.New()

// Struct 的 Subfield 字段携带整套 union 标签，等价于在 SubStruct 内部声明。
type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// +k8s:subfield(d)=+k8s:unionDiscriminator
	// +k8s:subfield(m1)=+k8s:unionMember
	// +k8s:subfield(m2)=+k8s:unionMember
	Subfield SubStruct `json:"subfield"`
}

// SubStruct 是被 subfield 定位的子结构体，D 为判别器，M1/M2 为成员。
type SubStruct struct {
	D  string `json:"d"`
	M1 *int   `json:"m1,omitempty"`
	M2 *int   `json:"m2,omitempty"`
}
