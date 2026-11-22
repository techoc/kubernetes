/*
Copyright 2018 The Kubernetes Authors.

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

// CoordinatedLeaseStrategy 表示协同选主中选择 leader 的策略。
type CoordinatedLeaseStrategy string

// CoordinatedLeaseStrategy 定义协同选主时挑选 leader 的策略。
const (
	// OldestEmulationVersion 选择“最老”的 LeaseCandidate。“最老”按如下方式判定：
	// 1) 选择模拟版本最低的候选者；
	// 2) 若有多个候选者模拟版本相同，选择二进制版本最低的候选者（注意二进制版本必须大于或等于模拟版本）；
	// 3) 若仍有多个候选者二进制版本相同，选择 creationTimestamp 最早的候选者。
	// 未指定 emulationVersion 和 binaryVersion 字段的候选者不会被纳入租约候选。
	OldestEmulationVersion CoordinatedLeaseStrategy = "OldestEmulationVersion"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14

// Lease 定义租约概念。
type Lease struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含 Lease 的规格。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec LeaseSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// LeaseSpec 是 Lease 的规格。
type LeaseSpec struct {
	// holderIdentity 包含当前租约持有者的标识。
	// 如果使用协同选主，持有者标识必须等于选出的 LeaseCandidate.metadata.name 字段。
	// +optional
	HolderIdentity *string `json:"holderIdentity,omitempty" protobuf:"bytes,1,opt,name=holderIdentity"`
	// leaseDurationSeconds 是租约候选者在强制获取租约前需要等待的时长。
	// 该时长相对于最近一次观测到的 renewTime 计算。
	// +optional
	LeaseDurationSeconds *int32 `json:"leaseDurationSeconds,omitempty" protobuf:"varint,2,opt,name=leaseDurationSeconds"`
	// acquireTime 是当前租约被获取的时间。
	// +optional
	AcquireTime *metav1.MicroTime `json:"acquireTime,omitempty" protobuf:"bytes,3,opt,name=acquireTime"`
	// renewTime 是当前租约持有者最近一次更新租约的时间。
	// +optional
	RenewTime *metav1.MicroTime `json:"renewTime,omitempty" protobuf:"bytes,4,opt,name=renewTime"`
	// leaseTransitions 是租约在持有者之间切换的次数。
	// +optional
	LeaseTransitions *int32 `json:"leaseTransitions,omitempty" protobuf:"varint,5,opt,name=leaseTransitions"`
	// strategy 表示协同选主时挑选 leader 的策略。
	// 若未指定该字段，则此租约没有激活的协同。
	// (Alpha) 使用该字段需要启用 CoordinatedLeaderElection 特性门控。
	// +featureGate=CoordinatedLeaderElection
	// +optional
	Strategy *CoordinatedLeaseStrategy `json:"strategy,omitempty" protobuf:"bytes,6,opt,name=strategy"`
	// preferredHolder 向租约持有者表明存在更合适的持有者，应放弃租约。
	// 只有在同时设置了 Strategy 时才能设置此字段。
	// +featureGate=CoordinatedLeaderElection
	// +optional
	PreferredHolder *string `json:"preferredHolder,omitempty" protobuf:"bytes,7,opt,name=preferredHolder"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// LeaseList 是 Lease 对象的列表。
type LeaseList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 schema 对象的列表。
	Items []Lease `json:"items" protobuf:"bytes,2,rep,name=items"`
}
