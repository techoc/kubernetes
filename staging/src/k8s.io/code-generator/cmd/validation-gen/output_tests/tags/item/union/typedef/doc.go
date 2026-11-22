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

// This is a test package.
// +k8s:validation-gen-nolint
// 验证在切片 typedef 上用 item 选择器声明 unionMember：列表元素按 key 匹配，
// 选中的元素成为无 discriminator union 的成员。
package uniontypedef

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int `json:"typeMeta"`

	Tasks TaskList `json:"tasks"`
}

// TaskList 的每个元素按 name 定位；name 为 succeeded/failed 的元素
// 被声明为 union 成员。
// +k8s:listType=map
// +k8s:listMapKey=name
// +k8s:item(name: "succeeded")=+k8s:unionMember
// +k8s:item(name: "failed")=+k8s:unionMember
type TaskList []Task

// Task 是 union 成员的元素类型。
type Task struct {
	Name  string `json:"name"`
	State string `json:"state"`
}
