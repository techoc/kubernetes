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

// StorageVersion of a specific resource.
type StorageVersion struct {
	metav1.TypeMeta
	// The name is <group>.<resource>.
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
	// The reported versions per API server instance.
	// +optional
	StorageVersions []ServerStorageVersion
	// If all API server instances agree on the same encoding storage version,
	// then this field is set to that version. Otherwise this field is left empty.
	// API servers should finish updating its storageVersionStatus entry before
	// serving write operations, so that this field will be in sync with the reality.
	// +optional
	CommonEncodingVersion *string

	// conditions 记录 storageVersion 状态的最新观测结果。
	// +optional
	Conditions []StorageVersionCondition
}

// ServerStorageVersion An API server instance reports the version it can decode and the version it
// encodes objects to when persisting objects in the backend.
type ServerStorageVersion struct {
	// The ID of the reporting API server.
	APIServerID string

	// The API server encodes the object to this version when persisting it in
	// the backend (e.g., etcd).
	EncodingVersion string

	// The API server can decode objects encoded in these versions.
	// The encodingVersion must be included in the decodableVersions.
	DecodableVersions []string

	// API server 可以提供（serve）这些版本。
	// decodableVersions 必须涵盖所有 servedVersions。
	ServedVersions []string
}

// StorageVersionConditionType Indicates the storage version condition type
type StorageVersionConditionType string

const (
	//AllEncodingVersionsEqual Indicates that encoding storage versions reported by all servers are equal.
	AllEncodingVersionsEqual StorageVersionConditionType = "AllEncodingVersionsEqual"
)

// ConditionStatus indicates status of condition from "True", "False", or "Unknown"
type ConditionStatus string

const (
	// ConditionTrue indicates condition as "True"
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse indicates condition as "False"
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown indicates condition as "Unknown"
	ConditionUnknown ConditionStatus = "Unknown"
)

// StorageVersionCondition Describes the state of the storageVersion at a certain point.
type StorageVersionCondition struct {
	// Type of the condition.
	// +optional
	Type StorageVersionConditionType
	// 条件的状态，取值为 True、False 或 Unknown。
	// +required
	Status ConditionStatus
	// If set, this represents the .metadata.generation that the condition was set based upon.
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

// StorageVersionList A list of StorageVersions.
type StorageVersionList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []StorageVersion
}
