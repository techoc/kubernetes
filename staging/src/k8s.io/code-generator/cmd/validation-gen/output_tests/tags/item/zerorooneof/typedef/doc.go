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

// Package zeroroneooftypedef 是 validation-gen 的测试包，覆盖在命名列表类型上
// 通过 +k8s:item 声明 zero-or-one-of 成员：列表最多只能有一个成员的 key 命中。
// +k8s:validation-gen-nolint
package zeroroneooftypedef

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int `json:"typeMeta"`

	Tasks TaskList `json:"tasks"`
}

// +k8s:listType=map
// +k8s:listMapKey=name
// +k8s:item(name: "succeeded")=+k8s:zeroOrOneOfMember
// +k8s:item(name: "failed")=+k8s:zeroOrOneOfMember
type TaskList []Task

type Task struct {
	Name  string `json:"name"`
	State string `json:"state"`
}
