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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +resourceName=nodes
// +genclient:readonly
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetrics 定义节点的资源使用指标。
// v1beta1 仍用于兼容尚未迁移的旧客户端。
type NodeMetrics struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 以下字段定义指标采集的时间区间 [Timestamp-Window, Timestamp]。
	// Timestamp 与 Window 共同界定统计窗口，用量是该窗口内的聚合值，而非瞬时读数。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	// 指标采集窗口的长度。
	Window metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// 内存用量为工作集（working set），而非 RSS，以便 HPA 等消费者识别真实内存压力。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,4,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetricsList 是节点指标列表。
// 节点是非命名空间资源，因此该列表通常来自集群级 GET。
type NodeMetricsList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 节点指标列表。
	// 列表项的元数据里带节点名，调用方按 Name 关联到具体节点。
	Items []NodeMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +resourceName=pods
// +genclient:readonly
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetrics 定义 Pod 的资源使用指标。
// Pod 按命名空间隔离查询。
type PodMetrics struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 以下字段定义指标采集的时间区间 [Timestamp-Window, Timestamp]。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	// 指标采集窗口的长度。
	Window metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// 所有容器的指标都在同一时间窗口内采集，便于把各部分用量直接相加。
	// 该列表具有原子性，整体替换而非逐项合并。
	// +listType=atomic
	Containers []ContainerMetrics `json:"containers" protobuf:"bytes,4,rep,name=containers"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetricsList 是 Pod 指标列表。
// 通常对应某个命名空间下的全部 Pod，也可由 labelSelector 过滤。
type PodMetricsList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Pod 指标列表。
	Items []PodMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ContainerMetrics 定义容器的资源使用指标。
// 容器级指标是采集的最小粒度，Pod 与节点指标由此向上汇总。
type ContainerMetrics struct {
	// 与 pod.spec.containers 对应的容器名称。
	// 用名字而非索引关联，容器顺序在 Pod 重启后可能变化。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 内存用量为工作集（working set）。
	// 与 NodeMetrics 保持一致，内存口径同为工作集。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,2,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}
