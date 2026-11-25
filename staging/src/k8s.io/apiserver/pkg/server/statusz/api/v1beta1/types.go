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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// Statusz 是用于版本化 statusz 端点的结构体。
// 这是 /statusz 的结构化响应体。字段一旦发布就属于 API 契约，
// 新增字段必须可选，避免破坏旧客户端。
type Statusz struct {
	// TypeMeta is the type metadata for the object.
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// StartTime is the time the component process was initiated.
	StartTime metav1.Time `json:"startTime"`
	// UptimeSeconds 是组件连续运行的秒数。
	// 由 StartTime 推导而非独立维护，避免时钟回拨时两者互相矛盾。
	UptimeSeconds int64 `json:"uptimeSeconds"`
	// GoVersion is the version of the Go programming language used to build the binary.
	// The format is not guaranteed to be consistent across different Go builds.
	// +optional
	GoVersion string `json:"goVersion"`
	// BinaryVersion is the version of the component's binary.
	// The format is not guaranteed to be semantic versioning and may be an arbitrary string.
	BinaryVersion string `json:"binaryVersion"`
	// EmulationVersion 是该组件正在模拟的 Kubernetes API 版本。
	// 存在时，格式为 "<major>.<minor>"
	// +optional
	// 仅在使用版本模拟（emulation）时出现；为空表示按真实构建版本行为运行。
	EmulationVersion string `json:"emulationVersion,omitempty"`
	// Paths contains relative URLs to other essential read-only endpoints for debugging and troubleshooting.
	// +optional
	// +listType=set
	Paths []string `json:"paths"`
}
