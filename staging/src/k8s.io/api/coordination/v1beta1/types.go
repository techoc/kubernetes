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

package v1beta1

import (
	v1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.12
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=coordination.k8s.io,v1,Lease

// Lease 定义一个租约（lease）概念。
type Lease struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含 Lease 的规格定义。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec LeaseSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// LeaseSpec 是 Lease 的规格定义。
type LeaseSpec struct {
	// holderIdentity 保存当前租约持有者的身份。
	// 如果启用了协同 leader 选举（Coordinated Leader Election），该身份必须等于
	// 当选的 LeaseCandidate.metadata.name 字段。
	// +optional
	HolderIdentity *string `json:"holderIdentity,omitempty" protobuf:"bytes,1,opt,name=holderIdentity"`
	// leaseDurationSeconds 是租约候选者在强制获取租约前需要等待的时长，
	// 以最后一次观测到的 renewTime 为基准计算。
	// +optional
	LeaseDurationSeconds *int32 `json:"leaseDurationSeconds,omitempty" protobuf:"varint,2,opt,name=leaseDurationSeconds"`
	// acquireTime 是当前租约被获取的时间。
	// +optional
	AcquireTime *metav1.MicroTime `json:"acquireTime,omitempty" protobuf:"bytes,3,opt,name=acquireTime"`
	// renewTime 是当前租约持有者最后一次更新租约的时间。
	// +optional
	RenewTime *metav1.MicroTime `json:"renewTime,omitempty" protobuf:"bytes,4,opt,name=renewTime"`
	// leaseTransitions 是租约在持有者之间发生切换的次数。
	// +optional
	LeaseTransitions *int32 `json:"leaseTransitions,omitempty" protobuf:"varint,5,opt,name=leaseTransitions"`
	// strategy 指明协同 leader 选举选取 leader 的策略。
	// （Alpha）使用该字段需要启用 CoordinatedLeaderElection 特性门控。
	// +featureGate=CoordinatedLeaderElection
	// +optional
	Strategy *v1.CoordinatedLeaseStrategy `json:"strategy,omitempty" protobuf:"bytes,6,opt,name=strategy"`
	// preferredHolder 用于告知当前租约持有者：存在更合适的持有者，应当放弃租约。
	// +featureGate=CoordinatedLeaderElection
	// +optional
	PreferredHolder *string `json:"preferredHolder,omitempty" protobuf:"bytes,7,opt,name=preferredHolder"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.12
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=coordination.k8s.io,v1,LeaseList

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

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// LeaseCandidate 定义 Lease 对象的一个候选者。
// 创建候选者后，协同 leader 选举会从候选者列表中选出最合适的 leader。
type LeaseCandidate struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 包含 Lease 的规格定义。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec LeaseCandidateSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// LeaseCandidateSpec 是 Lease 候选者的规格定义。
type LeaseCandidateSpec struct {
	// leaseName 是本候选者所竞争的租约名称。
	// 该字段的约束与 Lease.name 相同。多个候选者可以引用同一个 Lease.name。
	// 本字段不可变。
	// +required
	LeaseName string `json:"leaseName" protobuf:"bytes,1,name=leaseName"`
	// pingTime 是服务端上次请求该 LeaseCandidate 续约的时间。
	// 仅在 leader 选举期间使用，用于检查是否有候选者已不再符合条件。
	// PingTime 更新后，LeaseCandidate 会通过更新 RenewTime 作出响应。
	// +optional
	PingTime *metav1.MicroTime `json:"pingTime,omitempty" protobuf:"bytes,2,opt,name=pingTime"`
	// renewTime 是该 LeaseCandidate 上次更新的时间。
	// 每当 Lease 需要进行 leader 选举时，都会更新 PingTime 字段，
	// 通知 LeaseCandidate 应当更新 RenewTime。
	// 若距上次续约已过去数小时，旧的 LeaseCandidate 对象也会被垃圾回收；
	// 定期更新 PingTime 可以避免仍活跃的候选者被回收。
	// +optional
	RenewTime *metav1.MicroTime `json:"renewTime,omitempty" protobuf:"bytes,3,opt,name=renewTime"`
	// binaryVersion 是二进制版本，必须为不带前导 `v` 的 semver 格式。
	// 本字段必填。
	// +required
	BinaryVersion string `json:"binaryVersion" protobuf:"bytes,4,name=binaryVersion"`
	// emulationVersion 是模拟版本，必须为不带前导 `v` 的 semver 格式。
	// EmulationVersion 必须小于或等于 BinaryVersion。
	// 当 strategy 为 "OldestEmulationVersion" 时本字段必填。
	// +optional
	EmulationVersion string `json:"emulationVersion,omitempty" protobuf:"bytes,5,opt,name=emulationVersion"`
	// strategy 是协同 leader 选举用于挑选 leader 的策略。
	// 若同一 Lease 的多个候选者返回不同策略，则采用 BinaryVersion 最新的候选者所提供的策略。
	// 若仍然存在冲突，则属于用户错误，协同 leader 选举在问题解决前不会操作该 Lease。
	// +required
	Strategy v1.CoordinatedLeaseStrategy `json:"strategy,omitempty" protobuf:"bytes,6,opt,name=strategy"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// LeaseCandidateList 是 Lease 候选者对象的列表。
type LeaseCandidateList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 schema 对象的列表。
	Items []LeaseCandidate `json:"items" protobuf:"bytes,2,rep,name=items"`
}
