/*
Copyright The Kubernetes Authors.

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

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// StorageVersionMigration 表示将存储的数据迁移到最新存储版本。
// +k8s:supportsSubresource="/status"
type StorageVersionMigration struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// 迁移的规格定义。
	// +optional
	Spec StorageVersionMigrationSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// 迁移的状态。
	// +optional
	Status StorageVersionMigrationStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// StorageVersionMigrationSpec 描述存储版本迁移的规格。
type StorageVersionMigrationSpec struct {
	// 正在迁移的资源。迁移器会向服务该资源的端点发送请求。
	// 不可变。
	Resource metav1.GroupResource `json:"resource" protobuf:"bytes,1,opt,name=resource"`
}

// MigrationConditionType 表示存储版本迁移的状态类型。
type MigrationConditionType string

const (
	// 表示迁移正在进行。
	MigrationRunning MigrationConditionType = "Running"
	// 表示迁移已成功完成。
	MigrationSucceeded MigrationConditionType = "Succeeded"
	// 表示迁移已失败。
	MigrationFailed MigrationConditionType = "Failed"
)

// StorageVersionMigrationStatus 描述存储版本迁移的状态。
type StorageVersionMigrationStatus struct {
	// 关于迁移当前状态的最新可用观测信息。
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
	// 执行迁移时与 GC 缓存进行比较的 ResourceVersion。
	// 这是 kube-controller-manager 首次观察到该 StorageVersionMigration 资源时，
	// 给定 group、version 和 resource 的当前资源版本。
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// StorageVersionMigrationList 是存储版本迁移对象的集合。
type StorageVersionMigrationList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Items 为 StorageVersionMigration 对象的列表。
	Items []StorageVersionMigration `json:"items" protobuf:"bytes,2,rep,name=items"`
}
