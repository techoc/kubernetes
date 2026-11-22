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

// Pod is a collection of containers, used as either input (create, update) or as output (list, get).
type Pod struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the behavior of a pod.
	// +optional
	Spec PodSpec

	// Status represents the current information about a pod. This data may not be up
	// to date.
	// +optional
	Status PodStatus
}

// PodStatus represents information about the status of a pod. Status may trail the actual
// state of a system.
//
// 示例 API 不实现状态机，字段仅用于验证序列化与默认值处理路径。
type PodStatus struct {
	// +optional
	Phase PodPhase
	// +optional
	Conditions []PodCondition
	// A human readable message indicating details about why the pod is in this state.
	// +optional
	Message string
	// A brief CamelCase message indicating details about why the pod is in this state. e.g. 'DiskPressure'
	// +optional
	Reason string

	// +optional
	HostIP string
	// +optional
	PodIP string

	// Date and time at which the object was acknowledged by the Kubelet.
	// This is before the Kubelet pulled the container image(s) for the pod.
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

// PodSpec is a description of a pod
//
// 这些字段是从 core/v1 复制的子集，用于让示例 API 具备足够的结构
// 深度以覆盖 conversion 与 field selector 等测试。
type PodSpec struct {
	// +optional
	RestartPolicy RestartPolicy
	// Optional duration in seconds the pod needs to terminate gracefully. May be decreased in delete request.
	// Value must be non-negative integer. The value zero indicates delete immediately.
	// If this value is nil, the default grace period will be used instead.
	// The grace period is the duration in seconds after the processes running in the pod are sent
	// a termination signal and the time when the processes are forcibly halted with a kill signal.
	// Set this value longer than the expected cleanup time for your process.
	// +optional
	TerminationGracePeriodSeconds *int64
	// Optional duration in seconds relative to the StartTime that the pod may be active on a node
	// before the system actively tries to terminate the pod; value must be positive integer
	// +optional
	ActiveDeadlineSeconds *int64
	// NodeSelector is a selector which must be true for the pod to fit on a node
	// +optional
	NodeSelector map[string]string

	// ServiceAccountName is the name of the ServiceAccount to use to run this pod
	// The pod will be allowed to use secrets referenced by the ServiceAccount
	ServiceAccountName string

	// NodeName is a request to schedule this pod onto a specific node.  If it is non-empty,
	// the scheduler simply schedules this pod onto that node, assuming that it fits resource
	// requirements.
	// +optional
	NodeName string
	// Specifies the hostname of the Pod.
	// If not specified, the pod's hostname will be set to a system-defined value.
	// +optional
	Hostname string
	// If specified, the fully qualified Pod hostname will be "<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>".
	// If not specified, the pod will not have a domainname at all.
	// +optional
	Subdomain string
	// If specified, the pod will be dispatched by specified scheduler.
	// If not specified, the pod will be dispatched by default scheduler.
	// +optional
	SchedulerName string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodList is a list of Pods.
type PodList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Pod
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicaSet ensures that a specified number of pod replicas are running at any given time.
type ReplicaSet struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the desired behavior of this ReplicaSet.
	// +optional
	Spec ReplicaSetSpec

	// Status is the current status of this ReplicaSet. This data may be
	// out of date by some window of time.
	// +optional
	Status ReplicaSetStatus
}

// ReplicaSetSpec is the specification of a ReplicaSet.
// As the internal representation of a ReplicaSet, it must have
// a Template set.
//
// 注意 Replicas 在内部版本是值类型，而 v1 外部版本用指针以区分
// “未设置”与“显式 0”，这正是 conversion.go 需要特殊处理的原因。
type ReplicaSetSpec struct {
	// Replicas is the number of desired replicas.
	Replicas int32
}

// ReplicaSetStatus represents the current status of a ReplicaSet.
type ReplicaSetStatus struct {
	// Replicas is the number of actual replicas.
	Replicas int32
}
