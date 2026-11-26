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

package example

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下类型是示例 API 中的字符串枚举，取值含义与 core/v1 保持一致。
type (
	ConditionStatus  string
	PodConditionType string
	PodPhase         string
	RestartPolicy    string
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pod 是容器的集合，既可作为输入（create、update），也可作为输出（list、get）。
type Pod struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 Pod 的行为
	// +optional
	Spec PodSpec

	// Status 表示 Pod 的当前信息。这些数据可能并非最新。
	//
	// +optional
	Status PodStatus
}

// PodStatus 表示 Pod 的状态信息。状态可能滞后于系统的实际状态。
//
// 示例 API 不实现状态机，字段仅用于验证序列化与默认值处理路径。
type PodStatus struct {
	// +optional
	Phase PodPhase
	// +optional
	Conditions []PodCondition
	// 人类可读的消息，说明 Pod 处于该状态的原因。
	// +optional
	Message string
	// 简短的 CamelCase 消息，说明 Pod 处于该状态的原因，如 'DiskPressure'。
	// +optional
	Reason string

	// +optional
	HostIP string
	// +optional
	PodIP string

	// Kubelet 确认该对象的时间。
	// 该时间早于 Kubelet 为 Pod 拉取容器镜像的时间。
	// +optional
	StartTime *metav1.Time
}

// PodCondition 描述 Pod 的单个状态条件。
type PodCondition struct {
	Type   PodConditionType
	Status ConditionStatus
	// +optional
	LastProbeTime metav1.Time
	// +optional
	LastTransitionTime metav1.Time
	// +optional
	Reason string
	// +optional
	Message string
}

// PodSpec 是对 Pod 的描述
//
// 这些字段是从 core/v1 复制的子集，用于让示例 API 具备足够的结构
// 深度以覆盖 conversion 与 field selector 等测试。
type PodSpec struct {
	// +optional
	RestartPolicy RestartPolicy
	// 优雅终止 Pod 的可选秒数。删除请求可将其调小。
	// 必须是非负整数；0 表示立即删除。
	// 若为 nil，则使用默认宽限期。
	// 宽限期是从向 Pod 内进程发送终止信号，到进程被强制杀死之间的秒数。
	//
	// 该值应大于进程的预期清理时间。
	// +optional
	TerminationGracePeriodSeconds *int64
	// Pod 在节点上可保持活动的可选秒数，从 StartTime 起算；
	// 超过后系统会主动终止 Pod；必须是正整数。
	// +optional
	ActiveDeadlineSeconds *int64
	// NodeSelector 是 Pod 能调度到某节点必须满足的选择器
	// +optional
	NodeSelector map[string]string

	// ServiceAccountName 是运行此 Pod 所用的 ServiceAccount 名称
	// Pod 将被允许使用该 ServiceAccount 引用的 secret
	ServiceAccountName string

	// NodeName 是请求将 Pod 调度到指定节点。若非空，调度器会直接
	// 将其调度到该节点，前提是满足资源
	// 要求。
	// +optional
	NodeName string
	// 指定 Pod 的 hostname。
	// 未指定时，Pod 的 hostname 由系统设定。
	// +optional
	Hostname string
	// 若指定，则 Pod 的完整主机名格式为 "<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>"。
	// 未指定时，Pod 完全没有域名。
	// +optional
	Subdomain string
	// 若指定，则由指定调度器分发该 Pod。
	// 未指定时，由默认调度器分发。
	// +optional
	SchedulerName string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodList 是 Pod 列表。
type PodList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Pod
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicaSet 确保任意时刻都有指定数量的 Pod 副本在运行。
type ReplicaSet struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义此 ReplicaSet 的期望行为。
	// +optional
	Spec ReplicaSetSpec

	// Status 是此 ReplicaSet 的当前状态。这些数据可能
	// 滞后一段时间。
	// +optional
	Status ReplicaSetStatus
}

// ReplicaSetSpec 是 ReplicaSet 的规格。
// 作为 ReplicaSet 的内部表示，必须设置
// Template。
//
// 注意 Replicas 在内部版本是值类型，而 v1 外部版本用指针以区分
// “未设置”与“显式 0”，这正是 conversion.go 需要特殊处理的原因。
type ReplicaSetSpec struct {
	// Replicas 是期望的副本数。
	Replicas int32
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是实际的副本数。
	Replicas int32
}
