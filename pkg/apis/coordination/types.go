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
	// PreferredHolder 向 lease 持有者发出信号：该 lease 存在
	// 更优的持有者，应当让出。
	// 仅当 Strategy 也已设置时，才可以设置此字段。
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
	// LeaseName 是该 candidate 所竞争的 lease 的名称。
	// 该字段的限制与 Lease.name 相同。多个 lease candidate
	// 可以引用同一个 Lease.name。
	// 该字段不可变。
	// +required
	LeaseName string
	// PingTime 是服务端最后一次要求该 LeaseCandidate 续期的时间。
	// 它仅在 leader election 期间进行，用于检查是否有 LeaseCandidate
	// 已不再符合条件。PingTime 更新后，LeaseCandidate 会通过
	// 更新 RenewTime 作出响应。
	// +optional
	PingTime *metav1.MicroTime
	// RenewTime 是该 LeaseCandidate 最近一次被更新的时间。每当某个
	// Lease 需要进行 leader election 时，PingTime 字段会被更新，以向
	// LeaseCandidate 发出应当更新 RenewTime 的信号。PingTime 字段也会
	// 定期更新，LeaseCandidates 必须更新 RenewTime，以防止仍活跃的
	// LeaseCandidates 被垃圾回收。
	// 旧的 LeaseCandidate 对象会被周期性地垃圾回收。
	// +optional
	RenewTime *metav1.MicroTime
	// BinaryVersion 是二进制版本，必须为不带前导 `v` 的 semver 格式。
	// 该字段必填。
	// +required
	BinaryVersion string
	// EmulationVersion 是仿真版本，必须为不带前导 `v` 的 semver 格式。
	// EmulationVersion 必须小于或等于 BinaryVersion。
	// 当 strategy 为 "OldestEmulationVersion" 时该字段必填。
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

// LeaseCandidateList 是 LeaseCandidate 对象的列表。
type LeaseCandidateList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// items 是 schema 对象列表。
	Items []LeaseCandidate
}
