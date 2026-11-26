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

// Package dependentrequired 是 validation-gen 的测试包，覆盖 +k8s:dependentRequired：
// 一个触发字段被设置时，其依赖字段也必须被设置；并覆盖一对多、多对一以及
// 指针/切片/map/内建类型四种“是否已设置”判定方式。
// +k8s:validation-gen-nolint
package dependentrequired

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 基本的一对一依赖。
type Struct struct {
	TypeMeta int

	// +k8s:optional
	// +k8s:dependentRequired("dependent")
	Trigger *string `json:"trigger"`

	// +k8s:optional
	Dependent *string `json:"dependent"`

	// +k8s:optional
	OtherField *string `json:"otherField"`
}

// 一个触发项、多个依赖项（重复标签）。
type MultiDependent struct {
	TypeMeta int

	// +k8s:optional
	// +k8s:dependentRequired("dependentA")
	// +k8s:dependentRequired("dependentB")
	Trigger *string `json:"trigger"`

	// +k8s:optional
	DependentA *string `json:"dependentA"`

	// +k8s:optional
	DependentB *string `json:"dependentB"`
}

// 多个触发项、一个依赖项。
type MultiTrigger struct {
	TypeMeta int

	// +k8s:optional
	// +k8s:dependentRequired("dependent")
	TriggerA *string `json:"triggerA"`

	// +k8s:optional
	// +k8s:dependentRequired("dependent")
	TriggerB *string `json:"triggerB"`

	// +k8s:optional
	Dependent *string `json:"dependent"`
}

// 四种“是否设置”提取器。
type AllKinds struct {
	TypeMeta int

	// +k8s:optional
	// +k8s:dependentRequired("ptrDep")
	PtrTrigger *string `json:"ptrTrigger"`

	// +k8s:optional
	PtrDep *string `json:"ptrDep"`

	// +k8s:optional
	// +k8s:dependentRequired("sliceDep")
	SliceTrigger []string `json:"sliceTrigger"`

	// +k8s:optional
	SliceDep []string `json:"sliceDep"`

	// +k8s:optional
	// +k8s:dependentRequired("mapDep")
	MapTrigger map[string]string `json:"mapTrigger"`

	// +k8s:optional
	MapDep map[string]string `json:"mapDep"`

	// +k8s:optional
	// +k8s:dependentRequired("intDep")
	IntTrigger int `json:"intTrigger"`

	// +k8s:optional
	IntDep int `json:"intDep"`
}
