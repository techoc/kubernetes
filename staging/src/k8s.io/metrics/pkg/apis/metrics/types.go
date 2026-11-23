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

// NodeMetrics sets resource usage metrics of a node.
// 中文说明：内部版本不参与线上编解码，只作为各外部版本转换的中枢。
type NodeMetrics struct {
	metav1.TypeMeta
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// The following fields define time interval from which metrics were
	// collected from the interval [Timestamp-Window, Timestamp].
	// 中文说明：内部版本保留完整时间窗口，供转换层决定如何向各外部版本呈现。
	Timestamp metav1.Time
	Window    metav1.Duration

	// The memory usage is the memory working set.
	// 中文说明：统一按 working set 记账，避免不同版本各自解释内存口径。
	Usage corev1.ResourceList
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeMetricsList is a list of NodeMetrics.
// 中文说明：列表类型主要服务于 API 的 list 动词，内部不额外做聚合计算。
type NodeMetricsList struct {
	metav1.TypeMeta
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta

	// List of node metrics.
	// 中文说明：列表项用 metadata.name 标识节点，转换到外部版本时原样保留。
	Items []NodeMetrics
}

// +genclient
// +resourceName=pods
// +genclient:readonly
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetrics sets resource usage metrics of a pod.
// 中文说明：Pod 级用量是容器用量之和，内部版本不涉及序列化细节。
type PodMetrics struct {
	metav1.TypeMeta
	// Standard object's metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// The following fields define time interval from which metrics were
	// collected from the interval [Timestamp-Window, Timestamp].
	Timestamp metav1.Time
	Window    metav1.Duration

	// Metrics for all containers are collected within the same time window.
	// 中文说明：共享窗口保证容器间用量可直接相加得到 Pod 总量。
	Containers []ContainerMetrics
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodMetricsList is a list of PodMetrics.
// 中文说明：对应某个命名空间（或全部命名空间）下的一组 Pod 指标快照。
type PodMetricsList struct {
	metav1.TypeMeta
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	metav1.ListMeta

	// List of pod metrics.
	Items []PodMetrics
}

// ContainerMetrics sets resource usage metrics of a container.
// 中文说明：内部版本是转换源，所有外部版本的容器指标都由此映射而来。
type ContainerMetrics struct {
	// Container name corresponding to the one from pod.spec.containers.
	// 中文说明：名字与 pod.spec.containers 对齐，便于按声明关联指标。
	Name string
	// The memory usage is the memory working set.
	// 中文说明：内存统一按 working set 统计，外部版本不再重复定义口径。
	Usage corev1.ResourceList
}
