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
// Statusz 是带版本的 statusz 端点所用结构。
//
// statusz 面向运维排障：只暴露进程级的只读信息，不包含任何用户数据。
type Statusz struct {
	// 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// 组件进程的启动时间。
	StartTime metav1.Time `json:"startTime"`
	// 组件连续运行的时长（秒）。
	// 以秒为整数便于消费方直接做阈值判断，无需解析时间戳。
	UptimeSeconds int64 `json:"uptimeSeconds"`
	// 构建二进制所用的 Go 版本。不同 Go 构建之间格式不保证一致。
	// +optional
	GoVersion string `json:"goVersion"`
	// 组件二进制的版本。不一定是语义化版本，也可能是任意字符串。
	BinaryVersion string `json:"binaryVersion"`
	// 组件正在模拟的 Kubernetes API 版本，格式为 "<major>.<minor>"。
	// +optional
	EmulationVersion string `json:"emulationVersion,omitempty"`
	// 指向其他用于排障的只读端点的相对 URL。
	// +optional
	// +listType=set
	// listType=set 保证同一路径不会重复出现，也便于生成服务端 apply 的
	// 语义；顺序无意义。
	Paths []string `json:"paths"`
}
