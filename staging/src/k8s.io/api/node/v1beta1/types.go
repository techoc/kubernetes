/*
Copyright 2019 The Kubernetes Authors.

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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.13
// +k8s:prerelease-lifecycle-gen:deprecated=1.22

// RuntimeClass 定义集群支持的一类容器运行时。
// RuntimeClass 用于确定以何种容器运行时运行 Pod 中的所有容器。它目前由用户或集群提供方手动定义，
// 并在 PodSpec 中引用。Kubelet 负责在运行 Pod 前解析 RuntimeClassName 引用。
// 详见 https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class
type RuntimeClass struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// handler 指定 CRI 实现用于处理该类 Pod 的底层运行时及配置。
	// 可能的取值取决于节点和 CRI 配置。假定所有 handler 在每个节点上均可用，
	// 且同名 handler 在各节点上等价。
	// 例如名为 "runc" 的 handler 可指定使用 runc OCI 运行时（原生 Linux 容器）运行 Pod 中的容器。
	// handler 必须为小写、符合 DNS Label（RFC 1123）要求，且不可变。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:format="k8s-short-name"
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:required
	Handler string `json:"handler" protobuf:"bytes,2,opt,name=handler"`

	// overhead 表示使用给定 RuntimeClass 运行 Pod 时的资源开销。
	// 详见 https://git.k8s.io/enhancements/keps/sig-node/688-pod-overhead/README.md
	// +optional
	Overhead *Overhead `json:"overhead,omitempty" protobuf:"bytes,3,opt,name=overhead"`

	// scheduling 保存调度约束，确保使用该 RuntimeClass 的 Pod 被调度到支持它的节点上。
	// 若 scheduling 为 nil，则认为所有节点都支持该 RuntimeClass。
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	Scheduling *Scheduling `json:"scheduling,omitempty" protobuf:"bytes,4,opt,name=scheduling"`
}

// Overhead 表示运行 Pod 相关的资源开销。
type Overhead struct {
	// podFixed 表示运行 Pod 相关的固定资源开销。
	// +optional
	PodFixed corev1.ResourceList `json:"podFixed,omitempty" protobuf:"bytes,1,opt,name=podFixed,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}

// Scheduling 指定支持某 RuntimeClass 的节点的调度约束。
type Scheduling struct {
	// nodeSelector 列出支持该 RuntimeClass 的节点必须具备的标签。
	// 使用该 RuntimeClass 的 Pod 只能被调度到与此选择器匹配的节点。
	// RuntimeClass 的 nodeSelector 会与 Pod 已有的 nodeSelector 合并；
	// 任何冲突都会导致该 Pod 在准入阶段被拒绝。
	// +optional
	// +mapType=atomic
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,1,opt,name=nodeSelector"`

	// tolerations 会在准入阶段追加到使用该 RuntimeClass 的 Pod 上（去重），
	// 实际效果是取 Pod 与 RuntimeClass 所能容忍节点集合的并集。
	// +optional
	// +listType=atomic
	// +k8s:alpha(since: "1.37")=+k8s:optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty" protobuf:"bytes,2,rep,name=tolerations"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.13
// +k8s:prerelease-lifecycle-gen:deprecated=1.22

// RuntimeClassList 是 RuntimeClass 对象的列表。
type RuntimeClassList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 schema 对象的列表。
	Items []RuntimeClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}
