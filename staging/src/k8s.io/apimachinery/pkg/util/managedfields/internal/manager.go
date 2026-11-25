/*
Copyright 2022 The Kubernetes Authors.

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

package internal

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// Managed 将 fieldpath.ManagedFields 与各操作的时间戳组合在一起。
type Managed interface {
	// Fields 获取 fieldpath.ManagedFields。
	Fields() fieldpath.ManagedFields

	// Times 获取各操作关联的时间戳。
	Times() map[string]*metav1.Time
}

// Manager 更新托管字段并合并应用配置。
type Manager interface {
	// Update 用于对象已完成合并的非 apply 场景。
	// * `liveObj` 不会被此函数修改。
	// * `newObj` 可能被此函数修改。
	// 返回移除 managedFields 后的新对象，以及单独返回的对象新 proposed managedFields。
	Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error)

	// Apply 用于服务端应用，负责合并对象并更新托管字段。
	// * `liveObj` 不会被此函数修改。
	// * `newObj` 可能被此函数修改。
	// 返回移除 managedFields 后的新对象，以及单独返回的对象新 proposed managedFields。
	Apply(liveObj, appliedObj runtime.Object, managed Managed, fieldManager string, force bool) (runtime.Object, Managed, error)
}
