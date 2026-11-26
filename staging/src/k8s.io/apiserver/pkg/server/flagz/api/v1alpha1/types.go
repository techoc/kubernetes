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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// Flagz 是 /flagz 端点的结构化响应。
//
// Flagz 是 /flagz 端点的响应对象；它借用 apimachinery 的 TypeMeta/ObjectMeta，
// 以便复用通用的序列化与 discovery 机制，而不是定义专用的响应格式。
type Flagz struct {
	// TypeMeta 是对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 对象的标准元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Flags 包含命令行 flag 及其取值。
	// key 是 flag 名称，value 是 flag 值，
	// 敏感值可能已被脱敏。
	// +optional
	//
	// 值可能被脱敏（例如认证令牌、证书口令），因此消费方不能假设这里
	// 拿到的一定是原始命令行；只应用于诊断展示。
	Flags map[string]string `json:"flags,omitempty"`
}
