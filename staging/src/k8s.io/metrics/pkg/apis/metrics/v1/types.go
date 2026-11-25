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

package v1

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
type NodeMetrics struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 以下字段定义指标采集的时间区间 [Timestamp-Window, Timestamp]。
	// 速率指标必须结合窗口才能还原原始累积量，窗口为零表示瞬时采样。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	// 指标采集窗口的长度。
	Window metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// 内存用量为工作集（working set），排除可回收缓存，比 RSS 更贴近实际内存压力。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,4,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetricsList 是节点指标列表。
// 列表不保证节点顺序，消费者应按名字自行匹配。
type NodeMetricsList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 节点指标列表。
	Items []NodeMetrics `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +resourceName=pods
// +genclient:readonly
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetrics 定义 Pod 的资源使用指标。
// Pod 的采样窗口对所有容器一致，便于把容器用量加总到 Pod 级别。
type PodMetrics struct {
	// API 对象的类型元数据。
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 以下字段定义指标采集的时间区间 [Timestamp-Window, Timestamp]。
	// 速率指标必须结合窗口才能还原原始累积量，窗口为零表示瞬时采样。
	Timestamp metav1.Time `json:"timestamp" protobuf:"bytes,2,opt,name=timestamp"`
	// 指标采集窗口的长度。
	Window metav1.Duration `json:"window" protobuf:"bytes,3,opt,name=window"`

	// 所有容器的指标都在同一时间窗口内采集。
	// 该列表具有原子性，更新时整体替换，避免部分更新混用不同窗口。
	// +listType=atomic
	Containers []ContainerMetrics `json:"containers" protobuf:"bytes,4,rep,name=containers"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetricsList 是 Pod 指标列表。
// 同样不保证顺序；跨 Pod 汇总时注意时间戳是否对齐。
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
type ContainerMetrics struct {
	// 与 pod.spec.containers 对应的容器名称。
	// 名字与 pod.spec.containers 对齐，便于按名关联用量和容器定义。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 内存用量为工作集（working set），排除可回收缓存，比 RSS 更贴近实际内存压力。
	Usage v1.ResourceList `json:"usage" protobuf:"bytes,2,rep,name=usage,casttype=k8s.io/api/core/v1.ResourceList,castkey=k8s.io/api/core/v1.ResourceName,castvalue=k8s.io/apimachinery/pkg/api/resource.Quantity"`
}
