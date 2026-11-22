/*
Copyright 2017 The Kubernetes Authors.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:noStatus
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Example 是 Example 资源的结构定义。
type Example struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata"`

	Spec   ExampleSpec   `json:"spec"`
	Status ExampleStatus `json:"status,omitempty"`
}

// ExampleSpec 是 Example 资源的 spec 部分。
type ExampleSpec struct {
	Foo string `json:"foo"`
	Bar bool   `json:"bar"`
}

// ExampleStatus 是 Example 资源的 status 部分。
type ExampleStatus struct {
	State   ExampleState `json:"state,omitempty"`
	Message string       `json:"message,omitempty"`
}

// ExampleState 表示 Example 资源所处的状态。
type ExampleState string

// Example 资源可能处于的状态。
const (
	// ExampleStateCreated 表示 Example 已创建。
	ExampleStateCreated ExampleState = "Created"
	// ExampleStateProcessed 表示 Example 已处理完成。
	ExampleStateProcessed ExampleState = "Processed"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExampleList 是 Example 资源的列表。
type ExampleList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata"`

	Items []Example `json:"items"`
}
