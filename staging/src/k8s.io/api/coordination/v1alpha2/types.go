/*
Copyright 2024 The Kubernetes Authors.

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

package v1alpha2

import (
	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// LeaseCandidate 定义一个 Lease 对象的候选者。
// 创建候选者是为了让协调式 leader election 从候选列表中选出最佳 leader。
type LeaseCandidate struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含 Lease 的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec LeaseCandidateSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// LeaseCandidateSpec 是 Lease 的规格定义。
type LeaseCandidateSpec struct {
	// leaseName 是该候选者所竞争的 lease 的名称。
	// 此字段不可变。
	// +required
	LeaseName string `json:"leaseName" protobuf:"bytes,1,name=leaseName"`
	// pingTime 是服务端上次请求该 LeaseCandidate 续约的时间。
	// 仅在 leader election 期间执行，用于检查是否有 LeaseCandidate 已不再合格。
	// 当 PingTime 更新时，LeaseCandidate 会通过更新 RenewTime 作出响应。
	// +optional
	PingTime *metav1.MicroTime `json:"pingTime,omitempty" protobuf:"bytes,2,opt,name=pingTime"`
	// renewTime 是该 LeaseCandidate 上次更新的时间。
	// 每当 Lease 需要进行 leader election 时，都会更新 PingTime 字段，
	// 以通知 LeaseCandidate 更新 RenewTime。
	// 如果距上次续约已有数小时，旧的 LeaseCandidate 对象也会被垃圾回收。
	// 定期更新 PingTime 字段可避免仍活跃的 LeaseCandidate 被垃圾回收。
	// +optional
	RenewTime *metav1.MicroTime `json:"renewTime,omitempty" protobuf:"bytes,3,opt,name=renewTime"`
	// binaryVersion 是二进制版本，必须为不含前导 `v` 的 semver 格式。
	// 此字段为必填。
	// +required
	BinaryVersion string `json:"binaryVersion" protobuf:"bytes,4,name=binaryVersion"`
	// emulationVersion 是模拟版本，必须为不含前导 `v` 的 semver 格式。
	// EmulationVersion 必须小于或等于 BinaryVersion。
	// 当 strategy 为 "OldestEmulationVersion" 时，此字段为必填。
	// +optional
	EmulationVersion string `json:"emulationVersion,omitempty" protobuf:"bytes,5,opt,name=emulationVersion"`
	// strategy 是协调式 leader election 用于挑选 leader 的策略。
	// 若同一 Lease 的多个候选者返回不同策略，则采用 BinaryVersion 最新的候选者所提供的策略。
	// 若仍有冲突，则属于用户错误；在问题解决前，协调式 leader election 不会操作该 Lease。
	// +required
	Strategy v1.CoordinatedLeaseStrategy `json:"strategy,omitempty" protobuf:"bytes,6,opt,name=strategy"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// LeaseCandidateList 是 Lease 对象的列表。
type LeaseCandidateList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 schema 对象的列表。
	Items []LeaseCandidate `json:"items" protobuf:"bytes,2,rep,name=items"`
}
