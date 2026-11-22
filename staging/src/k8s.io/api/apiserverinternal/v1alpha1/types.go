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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageVersion 记录某个特定资源的存储版本信息。
// +k8s:supportsSubresource="/status"
type StorageVersion struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据，name 形如 <group>.<resource>。
	// +required
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 为空，仅为符合 Kubernetes API 规范而保留。
	// +optional
	Spec StorageVersionSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 记录 API server 实例在持久化对象时可解码和存储编码的版本。
	// +optional
	Status StorageVersionStatus `json:"status" protobuf:"bytes,3,opt,name=status"`
}

// StorageVersionSpec 是空规格。
type StorageVersionSpec struct{}

// StorageVersionStatus 汇总各 API server 实例上报的可解码版本和持久化编码版本。
type StorageVersionStatus struct {
	// storageVersions 列出各 API server 实例上报的版本信息。
	// +optional
	// +listType=map
	// +listMapKey=apiServerID
	StorageVersions []ServerStorageVersion `json:"storageVersions,omitempty" protobuf:"bytes,1,opt,name=storageVersions"`
	// 所有实例使用同一编码版本时记录该版本，否则留空。
	// API server 应在处理写请求前更新自己的 storageVersionStatus，
	// 以保证该字段与实际状态一致。
	// +optional
	CommonEncodingVersion *string `json:"commonEncodingVersion,omitempty" protobuf:"bytes,2,opt,name=commonEncodingVersion"`

	// conditions 记录 storageVersion 状态的最新观测结果。
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []StorageVersionCondition `json:"conditions,omitempty" protobuf:"bytes,3,opt,name=conditions"`
}

// ServerStorageVersion 是单个 API server 实例上报的版本信息。
type ServerStorageVersion struct {
	// apiServerID 为上报实例的 ID。
	// +required
	APIServerID string `json:"apiServerID,omitempty" protobuf:"bytes,1,opt,name=apiServerID"`

	// 该实例持久化对象（如写入 etcd）时使用的编码版本。
	// +required
	EncodingVersion string `json:"encodingVersion,omitempty" protobuf:"bytes,2,opt,name=encodingVersion"`

	// 该实例可解码的编码版本列表。
	// encodingVersion 必须包含在 decodableVersions 中。
	// +listType=set
	// +required
	DecodableVersions []string `json:"decodableVersions,omitempty" protobuf:"bytes,3,opt,name=decodableVersions"`

	// 该实例可对外服务的版本列表。
	// decodableVersions 必须涵盖所有 servedVersions。
	// +listType=set
	// +optional
	ServedVersions []string `json:"servedVersions,omitempty" protobuf:"bytes,4,opt,name=servedVersions"`
}

// StorageVersionConditionType 表示 storageVersion 条件类型。
type StorageVersionConditionType string

const (
	// 所有实例上报的编码存储版本一致。
	AllEncodingVersionsEqual StorageVersionConditionType = "AllEncodingVersionsEqual"
)

// ConditionStatus 表示条件的当前状态。
type ConditionStatus string

const (
	// 条件状态为 True。
	ConditionTrue ConditionStatus = "True"
	// 条件状态为 False。
	ConditionFalse ConditionStatus = "False"
	// 条件状态未知。
	ConditionUnknown ConditionStatus = "Unknown"
)

// StorageVersionCondition 描述某一时刻 storageVersion 的状态。
type StorageVersionCondition struct {
	// 条件类型。
	// +required
	Type StorageVersionConditionType `json:"type" protobuf:"bytes,1,opt,name=type"`
	// 条件状态，取值为 True、False 或 Unknown。
	// +required
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status"`
	// 设置该条件时所依据的 .metadata.generation（若设置）。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`
	// condition 上次发生状态变更的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// 上次状态变更的原因。
	// +required
	Reason string `json:"reason" protobuf:"bytes,5,opt,name=reason"`
	// 可读的变更详情。
	// +required
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageVersionList 是 StorageVersion 的列表。
type StorageVersionList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Items 为 StorageVersion 列表。
	Items []StorageVersion `json:"items" protobuf:"bytes,2,rep,name=items"`
}
