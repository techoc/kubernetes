/*
Copyright 2020 The Kubernetes Authors.

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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// managedFieldsUpdater 包装底层 Manager，在每次更新后维护 managedFieldsEntry 的时间戳，
// 并把临时 manager 名下的字段归属回真实 manager。
type managedFieldsUpdater struct {
	fieldManager Manager
}

var _ Manager = &managedFieldsUpdater{}

// NewManagedFieldsUpdater 负责更新对象中的 managedFields，并按需更新操作时间。
// 对于更新操作，它使用一个硬编码的 manager 来检测字段是否发生变化，
// 操作完成后再把字段交换回正确的 manager 名下。
//
// 这样做的原因：底层 Manager.Update 需要区分"本次操作"与其他 manager 的字段，
// 用一个固定名字可以避免与调用方传入的 manager 重名，同时便于事后归并。
func NewManagedFieldsUpdater(fieldManager Manager) Manager {
	return &managedFieldsUpdater{
		fieldManager: fieldManager,
	}
}

// Update 实现 Manager 接口。
func (f *managedFieldsUpdater) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	// 用占位名让底层 Manager 记录本次操作触碰的字段，随后再改名归并。
	self := "current-operation"
	object, managed, err := f.fieldManager.Update(liveObj, newObj, managed, self)
	if err != nil {
		return object, managed, err
	}

	// 若本次操作从其他 manager 处取得了字段，说明对象确实发生了变化，
	// 此时才更新 managedFieldsEntry 的时间戳，并与同一 manager 之前的记录合并。
	if vs, ok := managed.Fields()[self]; ok {
		delete(managed.Fields(), self)

		if previous, ok := managed.Fields()[manager]; ok {
			// 同一 manager 已有记录时取字段并集，保持其 APIVersion 与 Applied 标记，
			// 以维持 fieldpath 集合的版本语义。
			managed.Fields()[manager] = fieldpath.NewVersionedSet(vs.Set().Union(previous.Set()), vs.APIVersion(), vs.Applied())
		} else {
			managed.Fields()[manager] = vs
		}

		// 时间戳统一用 UTC，避免不同节点的时区差异影响比较。
		managed.Times()[manager] = &metav1.Time{Time: time.Now().UTC()}
	}

	return object, managed, nil
}

// Apply 实现 Manager 接口。
func (f *managedFieldsUpdater) Apply(liveObj, appliedObj runtime.Object, managed Managed, fieldManager string, force bool) (runtime.Object, Managed, error) {
	object, managed, err := f.fieldManager.Apply(liveObj, appliedObj, managed, fieldManager, force)
	if err != nil {
		return object, managed, err
	}
	if object != nil {
		// 应用产生了新对象，说明该 manager 确实改动了字段，刷新其时间戳。
		managed.Times()[fieldManager] = &metav1.Time{Time: time.Now().UTC()}
	} else {
		// 底层返回 nil 表示无需变更；这里返回 liveObj 的副本并剥离 managedFields，
		// 使调用方得到与真实存储一致、不含管理元数据的对象，同时避免修改原对象。
		object = liveObj.DeepCopyObject()
		RemoveObjectManagedFields(object)
	}
	return object, managed, nil
}
