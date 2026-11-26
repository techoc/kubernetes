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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicaSet 确保任意时刻都有指定数量的 Pod 副本在运行。
//
// 该类型只用于测试：apiserver 的 endpoint/storage 测试需要一个独立于
// core/v1 的 API 组，避免测试数据与真实内置类型互相污染。
type ReplicaSet struct {
	metav1.TypeMeta `json:""`

	// Labels 为空时，默认取其所管理 Pod 的标签。
	// 标准对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义 ReplicaSet 期望的行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ReplicaSetSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是 ReplicaSet 最近一次观测到的状态。
	// 数据可能存在一定延迟。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ReplicaSetStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ReplicaSetSpec 描述 ReplicaSet 的规格。
type ReplicaSetSpec struct {
	// Replicas 是期望的副本数。
	// 使用指针以区分显式设为 0 与未指定。
	// 默认为 1。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller/#what-is-a-replicationcontroller
	// +optional
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`
}

// ReplicaSetStatus 表示 ReplicaSet 的当前状态。
type ReplicaSetStatus struct {
	// Replicas 是最近一次观测到的副本数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller/#what-is-a-replicationcontroller
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`
}
