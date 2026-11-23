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

// NodeMetrics sets resource usage metrics of a node.
// 中文说明：这是节点快照式的资源用量，Windows 版本之前为兼容旧客户端仍保留在 v1beta1。
type NodeMetrics struct {
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// The following fields define time interval from which metrics were
	// collected from the interval [Timestamp-Window, Timestamp].
	// 中文说明：Timestamp 与 Window 共同界定统计窗口；
	// 用量是这段时间窗口内的聚合值，而非某一瞬间的读数。
	Timestamp metav1.Time     `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	Window    metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// The memory usage is the memory working set.
	// 中文说明：取 working set 而非 RSS，是为了让 HPA 等消费者看到可回收前的真实压力。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,4,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetricsList is a list of NodeMetrics.
// 中文说明：节点是非命名空间资源，因此该列表通常来自集群级 GET。
type NodeMetricsList struct {
	metav1.TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of node metrics.
	// 中文说明：列表项的元数据里带节点名，调用方按 Name 关联到具体节点。
	Items []NodeMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +resourceName=pods
// +genclient:readonly
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetrics sets resource usage metrics of a pod.
// 中文说明：Pod 的用量是所有容器用量之和，按命名空间隔离查询。
type PodMetrics struct {
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// The following fields define time interval from which metrics were
	// collected from the interval [Timestamp-Window, Timestamp].
	Timestamp metav1.Time     `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	Window    metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// Metrics for all containers are collected within the same time window.
	// 中文说明：同一 Pod 内所有容器共用窗口，便于把各部分用量直接相加。
	// 指令 +listType=atomic 表示整个列表作为一个整体替换，不做逐项合并。
	// +listType=atomic
	Containers []ContainerMetrics `json:"containers" protobuf:"bytes,4,rep,name=containers"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetricsList is a list of PodMetrics.
// 中文说明：通常对应某个命名空间下的全部 Pod，也可由 labelSelector 过滤。
type PodMetricsList struct {
	metav1.TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of pod metrics.
	Items []PodMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ContainerMetrics sets resource usage metrics of a container.
// 中文说明：容器级指标是底层采集的最小粒度，Pod 与节点指标由此向上汇总。
type ContainerMetrics struct {
	// Container name corresponding to the one from pod.spec.containers.
	// 中文说明：用名字而非索引关联，容器顺序在 Pod 重启后可能变化。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// The memory usage is the memory working set.
	// 中文说明：与 NodeMetrics 保持一致，内存口径同为 working set。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,2,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}
