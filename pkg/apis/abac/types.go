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

package abac

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Policy 包含一条 ABAC 策略规则。
type Policy struct {
	metav1.TypeMeta

	// Spec 描述策略规则。
	Spec PolicySpec
}

// PolicySpec 包含策略规则的属性。
type PolicySpec struct {

	// User 是该规则适用的用户名。
	// user 或 group 至少有一个要匹配请求。
	// "*" 匹配所有用户。
	User string

	// Group 是该规则适用的用户组。
	// user 或 group 至少有一个要匹配请求。
	// "*" 匹配所有用户组。
	Group string

	// 为 true 时只匹配只读请求，为 false 时匹配所有请求。
	Readonly bool

	// APIGroup 是 API 组的名称。匹配资源请求时需要 APIGroup、Resource 和 Namespace。
	// "*" 匹配所有 API 组。
	APIGroup string

	// Resource 是资源名称。匹配资源请求时需要 APIGroup、Resource 和 Namespace。
	// "*" 匹配所有资源。
	Resource string

	// Namespace 是命名空间名称。匹配资源请求时需要 APIGroup、Resource 和 Namespace。
	// "*" 匹配所有命名空间（包括无命名空间的请求）。
	Namespace string

	// NonResourcePath 匹配非资源请求路径。
	// "*" 匹配所有路径。
	// "/foo/*" 匹配 foo 的所有子路径。
	NonResourcePath string

	// TODO: "expires" string in RFC3339 format.

	// TODO: want a way to allow some users to restart containers of a pod but
	// 不得删除或修改它。

	// TODO: want a way to allow a controller to create a pod based only on a
	// 特定的 podTemplate。

}
