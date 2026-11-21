/*
Copyright 2018 The Kubernetes Authors.

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

package node

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RuntimeClass 定义了集群中支持的容器运行时类别（class）。
// RuntimeClass 用于决定用哪个容器运行时来运行 Pod 中的全部容器。
// RuntimeClass 目前由用户或集群 provisioning 方手工定义，并在 PodSpec 中引用。
// Kubelet 负责在运行 Pod 之前解析 RuntimeClassName 引用。
// 更多细节参见：
// https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class/README.md
type RuntimeClass struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

	// handler 指定 CRI 实现在处理该类别的 Pod 时所使用的底层运行时与配置。
	// 可能的取值取决于节点与 CRI 的配置。这里假定所有节点上都有这些 handler，
	// 且同名 handler 在每个节点上的语义等价。
	// 例如名为 "runc" 的 handler 可能表示使用 runc OCI 运行时（原生 Linux 容器）来运行 Pod 中的容器。
	// Handler 必须符合 DNS Label（RFC 1123）要求，且不可变更。
	Handler string

	// overhead 表示以给定 RuntimeClass 运行 Pod 时相关的资源开销。
	// 更多细节参见：
	// https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates
	// +optional
	Overhead *Overhead

	// scheduling 保存调度约束，确保使用该 RuntimeClass 的 Pod 被调度到支持它的节点上。
	// 如果 scheduling 为 nil，则认为该 RuntimeClass 被所有节点支持。
	// +optional
	Scheduling *Scheduling
}

// Overhead 结构体表示运行一个 Pod 相关的资源开销。
type Overhead struct {
	// podFixed 表示运行一个 Pod 相关的固定资源开销。
	// +optional
	PodFixed core.ResourceList
}

// Scheduling 指定支持某个 RuntimeClass 的节点的调度约束。
type Scheduling struct {
	// nodeSelector 列出支持该 RuntimeClass 的节点上必须存在的标签。
	// 使用该 RuntimeClass 的 Pod 只能被调度到与该 selector 匹配的节点上。
	// RuntimeClass 的 nodeSelector 会与 Pod 自身的 nodeSelector 合并；
	// 任何冲突都会在准入（admission）阶段导致 Pod 被拒绝。
	// +optional
	NodeSelector map[string]string

	// tolerations 会在准入阶段追加到使用该 RuntimeClass 的 Pod（去除重复项），
	// 实际上是对 Pod 与 RuntimeClass 所能容忍的节点集合取并集。
	// +optional
	Tolerations []core.Toleration
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RuntimeClassList 是 RuntimeClass 对象的列表。
type RuntimeClassList struct {
	metav1.TypeMeta

	// +optional
	metav1.ListMeta

	// items 是 schema 对象的列表。
	Items []RuntimeClass
}
