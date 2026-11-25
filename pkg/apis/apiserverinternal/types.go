/*
Copyright 2019 The Kubernetes Authors.

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

package apiserverinternal

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageVersion 表示特定资源的存储版本。
type StorageVersion struct {
	metav1.TypeMeta
	// 名称格式为 <group>.<resource>。
	metav1.ObjectMeta

	// spec 为空，仅为符合 Kubernetes API 规范而保留。
	Spec StorageVersionSpec

	// API server 实例会上报其能够解码的版本，以及
	// 持久化对象到后端时所使用的编码方式。
	Status StorageVersionStatus
}

// StorageVersionSpec 是空规格。
type StorageVersionSpec struct{}

// StorageVersionStatus 是 API server 实例上报的、其能够解码的版本，
// 以及持久化对象到后端时所使用的编码方式。
type StorageVersionStatus struct {
	// 按 API server 实例上报的各自版本。
	// +optional
	StorageVersions []ServerStorageVersion
	// 若所有 API server 实例对同一编码存储版本达成一致，
	// 则此字段设置为该版本；否则此字段留空。
	// API server 应当在处理写操作之前完成对其 storageVersionStatus 条目的更新，
	// 以保证此字段与实际状态同步。
	// +optional
	CommonEncodingVersion *string

	// conditions 记录 storageVersion 状态的最新观测结果。
	// +optional
	Conditions []StorageVersionCondition
}

// ServerStorageVersion 是 API server 实例上报的其能够解码的版本，
// 以及持久化对象到后端时所使用的编码版本。
type ServerStorageVersion struct {
	// 上报该信息的 API server 的 ID。
	APIServerID string

	// API server 在后端（例如 etcd）持久化对象时，
	// 会将对象编码为该版本。
	EncodingVersion string

	// API server 可以解码以这些版本编码的对象。
	// encodingVersion 必须包含在 decodableVersions 中。
	DecodableVersions []string

	// API server 可以提供（serve）这些版本。
	// decodableVersions 必须涵盖所有 servedVersions。
	ServedVersions []string
}

// StorageVersionConditionType 表示 storage version 的 condition 类型
type StorageVersionConditionType string

const (
	//AllEncodingVersionsEqual 表示所有服务器上报的编码存储版本一致。
	AllEncodingVersionsEqual StorageVersionConditionType = "AllEncodingVersionsEqual"
)

// ConditionStatus 表示 condition 的状态，取值为 "True"、"False" 或 "Unknown"
type ConditionStatus string

const (
	// ConditionTrue 表示 condition 为 "True"
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse 表示 condition 为 "False"
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown 表示 condition 为 "Unknown"
	ConditionUnknown ConditionStatus = "Unknown"
)

// StorageVersionCondition 描述某一时间点 storageVersion 的状态。
type StorageVersionCondition struct {
	// condition 的类型。
	// +optional
	Type StorageVersionConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	// +required
	Status ConditionStatus
	// 若设置，表示该 condition 设置时所依据的 .metadata.generation。
	// +optional
	ObservedGeneration int64
	// 条件最近一次从一种状态变为另一种状态的时间。
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	// +required
	Reason string
	// 用自然语言描述状态变化的详细信息。
	// +required
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageVersionList 是 StorageVersions 的列表。
type StorageVersionList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []StorageVersion
}
