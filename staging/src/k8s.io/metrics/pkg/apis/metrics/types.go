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

package metrics

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +resourceName=nodes
// +genclient:readonly
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetrics 定义节点的资源使用指标。
// 内部版本不参与线上编解码，仅作为各外部版本转换的中枢。
type NodeMetrics struct {
	// API 对象的类型元数据。
	metav1.TypeMeta
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// 以下字段定义指标采集的时间区间 [Timestamp-Window, Timestamp]。
	// 内部版本保留完整时间窗口，供转换层决定如何向各外部版本呈现。
	Timestamp metav1.Time
	// 指标采集窗口的长度。
	Window metav1.Duration

	// 内存用量为工作集（working set）。
	// 统一按工作集记账，避免不同版本各自解释内存口径。
	Usage corev1.ResourceList
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetricsList 是节点指标列表。
// 列表类型主要服务于 API 的 list 动词，内部不额外做聚合计算。
type NodeMetricsList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta

	// 节点指标列表。
	// 列表项用 metadata.name 标识节点，转换到外部版本时原样保留。
	Items []NodeMetrics
}

// +genclient
// +resourceName=pods
// +genclient:readonly
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetrics 定义 Pod 的资源使用指标。
// 内部版本不涉及序列化细节。
type PodMetrics struct {
	// API 对象的类型元数据。
	metav1.TypeMeta
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// 以下字段定义指标采集的时间区间 [Timestamp-Window, Timestamp]。
	Timestamp metav1.Time
	// 指标采集窗口的长度。
	Window metav1.Duration

	// 所有容器的指标都在同一时间窗口内采集。
	// 共享窗口保证容器间用量可直接相加得到 Pod 总量。
	Containers []ContainerMetrics
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetricsList 是 Pod 指标列表。
// 对应某个命名空间（或全部命名空间）下的一组 Pod 指标快照。
type PodMetricsList struct {
	// API 对象的类型元数据。
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta

	// Pod 指标列表。
	Items []PodMetrics
}

// ContainerMetrics 定义容器的资源使用指标。
// 内部版本是转换源，所有外部版本的容器指标都由此映射而来。
type ContainerMetrics struct {
	// 与 pod.spec.containers 对应的容器名称。
	// 名字与 pod.spec.containers 对齐，便于按声明关联指标。
	Name string
	// 内存用量为工作集（working set）。
	// 内存统一按工作集统计，外部版本不再重复定义口径。
	Usage corev1.ResourceList
}
