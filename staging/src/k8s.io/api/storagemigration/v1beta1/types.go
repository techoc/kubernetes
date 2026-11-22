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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.35
// +k8s:prerelease-lifecycle-gen:deprecated=1.37
// +k8s:prerelease-lifecycle-gen:replacement=storagemigration.k8s.io,v1,StorageVersionMigration

// StorageVersionMigration 表示将存量数据迁移到最新存储版本的任务。
// +k8s:supportsSubresource="/status"
type StorageVersionMigration struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// 迁移任务的具体规格。
	// +optional
	Spec StorageVersionMigrationSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// 迁移任务的当前状态。
	// +optional
	Status StorageVersionMigrationStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// StorageVersionMigrationSpec 描述存储版本迁移任务的规格。
type StorageVersionMigrationSpec struct {
	// 待迁移的资源。迁移器向服务该资源的 endpoint 发送请求。
	// 该字段不可变。
	Resource metav1.GroupResource `json:"resource" protobuf:"bytes,1,opt,name=resource"`
}

// MigrationConditionType 表示迁移任务所处的状态类别。
type MigrationConditionType string

const (
	// 迁移正在执行。
	MigrationRunning MigrationConditionType = "Running"
	// 迁移已成功完成。
	MigrationSucceeded MigrationConditionType = "Succeeded"
	// 迁移失败。
	MigrationFailed MigrationConditionType = "Failed"
)

// StorageVersionMigrationStatus 记录存储版本迁移的状态。
type StorageVersionMigrationStatus struct {
	// 迁移当前状态的最新观测结果。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
	// 与 GC 缓存比对以决定是否执行迁移的 resourceVersion。
	// 取自 kube-controller-manager 首次观测到该 StorageVersionMigration 时，
	// 对应 group/version/resource 的当前版本。
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.35
// +k8s:prerelease-lifecycle-gen:deprecated=1.37
// +k8s:prerelease-lifecycle-gen:replacement=storagemigration.k8s.io,v1,StorageVersionMigrationList

// StorageVersionMigrationList 是存储版本迁移任务的集合。
type StorageVersionMigrationList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 详见 https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Items 为 StorageVersionMigration 列表。
	Items []StorageVersionMigration `json:"items" protobuf:"bytes,2,rep,name=items"`
}
