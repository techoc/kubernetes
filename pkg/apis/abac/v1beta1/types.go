/*
Copyright 2015 The Kubernetes Authors.

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

// +k8s:openapi-gen=true

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Policy 包含一条 ABAC 策略规则。
type Policy struct {
	metav1.TypeMeta `json:""`

	// Spec 描述策略规则。
	Spec PolicySpec `json:"spec"`
}

// PolicySpec 包含策略规则的属性。
type PolicySpec struct {
	// User 是该规则适用的用户名。
	// user 或 group 至少有一个要匹配请求。
	// "*" 匹配所有用户。
	// +optional
	User string `json:"user,omitempty"`

	// Group 是该规则适用的用户组。
	// user 或 group 至少有一个要匹配请求。
	// "*" 匹配所有用户组。
	// +optional
	Group string `json:"group,omitempty"`

	// 为 true 时只匹配只读请求，为 false 时匹配所有请求。
	// +optional
	Readonly bool `json:"readonly,omitempty"`

	// APIGroup 是 API 组的名称。匹配资源请求时需要 APIGroup、Resource 和 Namespace。
	// "*" 匹配所有 API 组。
	// +optional
	APIGroup string `json:"apiGroup,omitempty"`

	// Resource 是资源名称。匹配资源请求时需要 APIGroup、Resource 和 Namespace。
	// "*" 匹配所有资源。
	// +optional
	Resource string `json:"resource,omitempty"`

	// Namespace 是命名空间名称。匹配资源请求时需要 APIGroup、Resource 和 Namespace。
	// "*" 匹配所有命名空间（包括无命名空间的请求）。
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// NonResourcePath 匹配非资源请求路径。
	// "*" 匹配所有路径。
	// "/foo/*" 匹配 foo 的所有子路径。
	// +optional
	NonResourcePath string `json:"nonResourcePath,omitempty"`
}
