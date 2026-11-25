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

package storagemigration

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// StorageVersionMigration represents a migration of stored data to the latest
// storage version.
type StorageVersionMigration struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta
	// Specification of the migration.
	// +optional
	Spec StorageVersionMigrationSpec
	// Status of the migration.
	// +optional
	Status StorageVersionMigrationStatus
}

// StorageVersionMigrationSpec 描述存储版本迁移的规格。
type StorageVersionMigrationSpec struct {
	// 待迁移的资源。迁移器向服务该资源的 endpoint 发送请求。
	// 该字段不可变。
	Resource metav1.GroupResource
}

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
	// The latest available observations of the migration's current state.
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition
	// 执行迁移时与 GC 缓存进行比较的 ResourceVersion。
	// 这是 kube-controller-manager 首次观察到该 StorageVersionMigration 资源时，
	// 给定 group、version 和 resource 的当前资源版本。
	ResourceVersion string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.30

// StorageVersionMigrationList 是存储版本迁移对象的集合。
type StorageVersionMigrationList struct {
	metav1.TypeMeta

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta
	// Items 为 StorageVersionMigration 对象的列表。
	Items []StorageVersionMigration
}
