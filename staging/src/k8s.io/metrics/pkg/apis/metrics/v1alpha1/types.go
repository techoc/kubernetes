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

package v1alpha1

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
// 本结构描述的是某个时间窗口内的节点资源使用快照，不是累计计数。
type NodeMetrics struct {
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// The following fields define time interval from which metrics were
	// collected from the interval [Timestamp-Window, Timestamp].
	// 两个字段成对出现：单看 Timestamp 无法知道指标是瞬时值还是窗口内的平均值。
	Timestamp metav1.Time     `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	Window    metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// The memory usage is the memory working set.
	// 选用 working set 而非 RSS，是为了让 HPA 在存在可回收页时不会误判为内存紧张。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,4,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetricsList is a list of NodeMetrics.
// 列表类型需要独立注册，kubectl 等客户端按 List kind 解码批量查询响应。
type NodeMetricsList struct {
	metav1.TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of node metrics.
	Items []NodeMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +resourceName=pods
// +genclient:readonly
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetrics sets resource usage metrics of a pod.
// 容器内用量汇总成 Pod 用量时由服务端完成，客户端不应再次累加。
type PodMetrics struct {
	metav1.TypeMeta `json:""`
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// The following fields define time interval from which metrics were
	// collected from the interval [Timestamp-Window, Timestamp].
	// 窗口信息放在 Pod 层，所有容器共享同一采集周期，避免每个容器重复携带。
	Timestamp metav1.Time     `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	Window    metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// 指令 +listType=atomic：容器列表是一次性快照，合并补丁时整体替换而非按名字归并。
	// Metrics for all containers are collected within the same time window.
	// +listType=atomic
	Containers []ContainerMetrics `json:"containers" protobuf:"bytes,4,rep,name=containers"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetricsList is a list of PodMetrics.
// 批量查询按 namespace 聚合返回，List 结构便于一次解码多个 Pod 的快照。
type PodMetricsList struct {
	metav1.TypeMeta `json:""`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of pod metrics.
	Items []PodMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ContainerMetrics sets resource usage metrics of a container.
// 本类型内嵌在 PodMetrics 中，不单独作为 API 资源暴露。
type ContainerMetrics struct {
	// Container name corresponding to the one from pod.spec.containers.
	// 用名字而非序号关联，容器重启或列表顺序变化时仍能对上 spec。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// The memory usage is the memory working set.
	// 语义与 NodeMetrics.Usage 保持一致，方便上层统一处理各层级的内存指标。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,2,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}
