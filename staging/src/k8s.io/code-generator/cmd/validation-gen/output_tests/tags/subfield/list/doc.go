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

// Package  contains test types for testing subfield field validation tags.
// list 验证 +k8s:subfield 对嵌套 list/map 语义（listType、eachKey 等）
// 的覆盖方式，并配合 opaqueType 跳过外部类型。
// +k8s:validation-gen-nolint
package list

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/code-generator/cmd/validation-gen/testscheme"
)

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// +k8s:subfield(ownerReferences)=+k8s:listType=map
	// +k8s:subfield(ownerReferences)=+k8s:listMapKey=uid
	// +k8s:subfield(finalizers)=+k8s:listType=set
	// +k8s:subfield(labels)=+k8s:eachKey=+k8s:validateFalse="labels key error"
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"objectMeta"`
}
