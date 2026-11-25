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

package coordination

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Lease 定义租约概念。
type Lease struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// spec 包含 Lease 的规格定义。
	// +optional
	Spec LeaseSpec
}

// LeaseSpec 是 Lease 的规格。
type LeaseSpec struct {
	// holderIdentity 包含当前租约持有者的标识。
	// 如果使用协同选主，持有者标识必须等于选出的 LeaseCandidate.metadata.name 字段。
	// +optional
	HolderIdentity *string
	// leaseDurationSeconds 是租约候选者在强制获取租约前需要等待的时长，
	// 以最后一次观测到的 renewTime 为基准计算。
	// +optional
	LeaseDurationSeconds *int32
	// acquireTime 是当前租约被获取的时间。
	// +optional
	AcquireTime *metav1.MicroTime
	// renewTime 是当前租约持有者最近一次更新租约的时间。
	// +optional
	RenewTime *metav1.MicroTime
	// leaseTransitions 是租约在持有者之间切换的次数。
	// +optional
	LeaseTransitions *int32
	// strategy 表示协同选主时挑选 leader 的策略。
	// 若未指定该字段，则此租约没有激活的协同。
	// (Alpha) 使用该字段需要启用 CoordinatedLeaderElection 特性门控。
	// +featureGate=CoordinatedLeaderElection
	// +optional
	Strategy *CoordinatedLeaseStrategy
	// PreferredHolder signals to a lease holder that the lease has a
	// more optimal holder and should be given up.
	// This field can only be set if Strategy is also set.
	// +featureGate=CoordinatedLeaderElection
	// +optional
	PreferredHolder *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LeaseList 是 Lease 对象的列表。
type LeaseList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 schema 对象列表。
	Items []Lease
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LeaseCandidate 定义一个 Lease 对象的候选者。
// 创建候选者是为了让协调式 leader election 从候选列表中选出最佳 leader。
type LeaseCandidate struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	Spec LeaseCandidateSpec
}

// LeaseCandidateSpec 是 Lease 的规格定义。
type LeaseCandidateSpec struct {
	// LeaseName is the name of the lease for which this candidate is contending.
	// The limits on this field are the same as on Lease.name. Multiple lease candidates
	// may reference the same Lease.name.
	// 该字段不可变。
	// +required
	LeaseName string
	// PingTime is the last time that the server has requested the LeaseCandidate
	// to renew. It is only done during leader election to check if any
	// LeaseCandidates have become ineligible. When PingTime is updated, the
	// LeaseCandidate will respond by updating RenewTime.
	// +optional
	PingTime *metav1.MicroTime
	// RenewTime is the time that the LeaseCandidate was last updated. Any time
	// a Lease needs to do leader election, the PingTime field is updated to
	// signal to the LeaseCandidate that they should update the RenewTime. The
	// PingTime field is also updated regularly and LeaseCandidates must update
	// RenewTime to prevent garbage collection for still active LeaseCandidates.
	// Old LeaseCandidate objects are periodically garbage collected.
	// +optional
	RenewTime *metav1.MicroTime
	// BinaryVersion is the binary version. It must be in a semver format without leading `v`.
	// 该字段必填。
	// +required
	BinaryVersion string
	// EmulationVersion is the emulation version. It must be in a semver format without leading `v`.
	// EmulationVersion must be less than or equal to BinaryVersion.
	// This field is required when strategy is "OldestEmulationVersion"
	// +optional
	EmulationVersion string
	// strategy 是协调式 leader election 用于挑选 leader 的策略。
	// 若同一 Lease 的多个候选者返回不同策略，则采用 BinaryVersion 最新的候选者所提供的策略。
	// 若仍有冲突，则属于用户错误；在问题解决前，协调式 leader election 不会操作该 Lease。
	// +listType=atomic
	// +required
	Strategy CoordinatedLeaseStrategy
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LeaseCandidateList is a list of LeaseCandidate objects.
type LeaseCandidateList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 schema 对象列表。
	Items []LeaseCandidate
}
