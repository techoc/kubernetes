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
// Flagz is the structured response for the /flagz endpoint.
//
// Flagz 是 /flagz 端点的响应对象；它借用 apimachinery 的 TypeMeta/ObjectMeta，
// 以便复用通用的序列化与 discovery 机制，而不是定义专用的响应格式。
type Flagz struct {
	// TypeMeta is the type metadata for the object.
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// Flags contains the command-line flags and their values.
	// The keys are the flag names and the values are the flag values,
	// possibly with confidential values redacted.
	// +optional
	//
	// 值可能被脱敏（例如认证令牌、证书口令），因此消费方不能假设这里
	// 拿到的一定是原始命令行；只应用于诊断展示。
	Flags map[string]string `json:"flags,omitempty"`
}
