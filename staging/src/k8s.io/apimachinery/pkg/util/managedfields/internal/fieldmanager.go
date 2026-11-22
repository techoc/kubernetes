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

// Package internal 实现服务端 apply 的字段管理逻辑：解码/编码
// managedFields、合并 apply 配置，并通过一系列 Manager 包装叠加版本检查、
// last-applied 记录、创建时跟踪概率等语义。
package internal

import (
	"fmt"
	"reflect"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
)

// DefaultMaxUpdateManagers defines the default maximum retained number of managedFields entries from updates
// if the number of update managers exceeds this, the oldest entries will be merged until the number is below the maximum.
// TODO(jennybuckley): Determine if this is really the best value. Ideally we wouldn't unnecessarily merge too many entries.
const DefaultMaxUpdateManagers int = 10

// DefaultTrackOnCreateProbability defines the default probability that the field management of an object
// starts being tracked from the object's creation, instead of from the first time the object is applied to.
// 中文说明：设为 1 表示完全从创建时开始跟踪，避免为省开销而随机跳过，
// 从而保证 apply 语义确定。
const DefaultTrackOnCreateProbability float32 = 1

// atMostEverySecond 限制错误日志频率，避免 managedFields 失败时刷屏。
var atMostEverySecond = NewAtMostEvery(time.Second)

// FieldManager updates the managed fields and merges applied
// configurations.
//
// 中文说明：它把内层 Manager 链与实际 API 对象的编解码隔离开——调用方只需
// 传入 live/new 对象，FieldManager 负责取用、合并与回写 managedFields。
type FieldManager struct {
	fieldManager Manager
	subresource  string
}

// NewFieldManager creates a new FieldManager that decodes, manages, then re-encodes managedFields
// on update and apply requests.
func NewFieldManager(f Manager, subresource string) *FieldManager {
	return &FieldManager{fieldManager: f, subresource: subresource}
}

// newDefaultFieldManager is a helper function which wraps a Manager with certain default logic.
//
// 中文说明：包装顺序即语义叠加顺序，从内到外依次为：剥离类型元信息、
// 按 manager 归属更新、容量上限、创建时跟踪概率、last-applied 维护、
// 版本校验。顺序有依赖，不能随意调整。
func NewDefaultFieldManager(f Manager, typeConverter TypeConverter, objectConverter runtime.ObjectConvertor, objectCreater runtime.ObjectCreater, kind schema.GroupVersionKind, subresource string) *FieldManager {
	return NewFieldManager(
		NewVersionCheckManager(
			NewLastAppliedUpdater(
				NewLastAppliedManager(
					NewProbabilisticSkipNonAppliedManager(
						NewCapManagersManager(
							NewBuildManagerInfoManager(
								NewManagedFieldsUpdater(
									NewStripMetaManager(f),
								), kind.GroupVersion(), subresource,
							), DefaultMaxUpdateManagers,
						), objectCreater, DefaultTrackOnCreateProbability,
					), typeConverter, objectConverter, kind.GroupVersion(),
				),
			), kind,
		), subresource,
	)
}

// decodeLiveOrNew 决定本次操作应以哪一份 managedFields 为基础。
// ignoreManagedFieldsFromRequestObject 为 true（子资源请求）时信任 live 对象；
// 否则优先使用请求对象，但若其为空或解码失败则退回 live，避免不感知
// managedFields 的客户端误删数据。
func decodeLiveOrNew(liveObj, newObj runtime.Object, ignoreManagedFieldsFromRequestObject bool) (Managed, error) {
	liveAccessor, err := meta.Accessor(liveObj)
	if err != nil {
		return nil, err
	}

	// We take the managedFields of the live object in case the request tries to
	// manually set managedFields via a subresource.
	if ignoreManagedFieldsFromRequestObject {
		return emptyManagedFieldsOnErr(DecodeManagedFields(liveAccessor.GetManagedFields()))
	}

	// If the object doesn't have metadata, we should just return without trying to
	// set the managedFields at all, so creates/updates/patches will work normally.
	newAccessor, err := meta.Accessor(newObj)
	if err != nil {
		return nil, err
	}

	// 显式的空列表（或单个空条目）是“重置 managedFields”的约定信号。
	if isResetManagedFields(newAccessor.GetManagedFields()) {
		return NewEmptyManaged(), nil
	}

	// If the managed field is empty or we failed to decode it,
	// let's try the live object. This is to prevent clients who
	// don't understand managedFields from deleting it accidentally.
	managed, err := DecodeManagedFields(newAccessor.GetManagedFields())
	if err != nil || len(managed.Fields()) == 0 {
		return emptyManagedFieldsOnErr(DecodeManagedFields(liveAccessor.GetManagedFields()))
	}
	return managed, nil
}

// emptyManagedFieldsOnErr 在解码失败时降级为空的 managedFields 而不是报错，
// 保证写入路径在数据损坏时仍可继续（旧数据会被下一次写入覆盖）。
func emptyManagedFieldsOnErr(managed Managed, err error) (Managed, error) {
	if err != nil {
		return NewEmptyManaged(), nil
	}
	return managed, nil
}

// Update is used when the object has already been merged (non-apply
// use-case), and simply updates the managed fields in the output
// object.
func (f *FieldManager) Update(liveObj, newObj runtime.Object, manager string) (object runtime.Object, err error) {
	// First try to decode the managed fields provided in the update,
	// This is necessary to allow directly updating managed fields.
	isSubresource := f.subresource != ""
	managed, err := decodeLiveOrNew(liveObj, newObj, isSubresource)
	if err != nil {
		return newObj, nil
	}

	RemoveObjectManagedFields(newObj)

	if object, managed, err = f.fieldManager.Update(liveObj, newObj, managed, manager); err != nil {
		return nil, err
	}

	if err = EncodeObjectManagedFields(object, managed); err != nil {
		return nil, fmt.Errorf("failed to encode managed fields: %v", err)
	}

	return object, nil
}

// UpdateNoErrors is the same as Update, but it will not return
// errors. If an error happens, we preserve the managedFields from
// liveObj.
func (f *FieldManager) UpdateNoErrors(liveObj, newObj runtime.Object, manager string) runtime.Object {
	obj, err := f.Update(liveObj, newObj, manager)
	if err != nil {
		// Preserve the managedFields from the live object rather than
		// stripping them entirely, to avoid silent data loss when the
		// managedFields update fails (e.g. due to an unavailable
		// conversion webhook).
		// Note: meta.Accessor for liveObj and newObj below never return an error in this code branch,
		// because if they would f.Update above would return "newObj, nil". Accordingly, the case
		// where one of the accessors returns an error is not handled here.
		if liveAccessor, aErr := meta.Accessor(liveObj); aErr == nil {
			if newAccessor, aErr := meta.Accessor(newObj); aErr == nil {
				atMostEverySecond.Do(func() {
					//nolint:logcheck // Should not be reached.
					klog.ErrorS(err, "[SHOULD NOT HAPPEN] failed to update managedFields (restored previous managedFields from live object)", "versionKind",
						newObj.GetObjectKind().GroupVersionKind(), "namespace", newAccessor.GetNamespace(), "name", newAccessor.GetName())
				})
				newAccessor.SetManagedFields(liveAccessor.GetManagedFields())
			}
		}
		return newObj
	}
	return obj
}

// Returns true if the managedFields indicate that the user is trying to
// reset the managedFields, i.e. if the list is non-nil but empty, or if
// the list has one empty item.
func isResetManagedFields(managedFields []metav1.ManagedFieldsEntry) bool {
	if len(managedFields) == 0 {
		return managedFields != nil
	}

	if len(managedFields) == 1 {
		return reflect.DeepEqual(managedFields[0], metav1.ManagedFieldsEntry{})
	}

	return false
}

// Apply is used when server-side apply is called, as it merges the
// object and updates the managed fields.
func (f *FieldManager) Apply(liveObj, appliedObj runtime.Object, manager string, force bool) (object runtime.Object, err error) {
	// If the object doesn't have metadata, apply isn't allowed.
	accessor, err := meta.Accessor(liveObj)
	if err != nil {
		return nil, fmt.Errorf("couldn't get accessor: %v", err)
	}

	// Decode the managed fields in the live object, since it isn't allowed in the patch.
	managed, err := DecodeManagedFields(accessor.GetManagedFields())
	if err != nil {
		return nil, fmt.Errorf("failed to decode managed fields: %v", err)
	}

	object, managed, err = f.fieldManager.Apply(liveObj, appliedObj, managed, manager, force)
	if err != nil {
		if conflicts, ok := err.(merge.Conflicts); ok {
			return nil, NewConflictError(conflicts)
		}
		return nil, err
	}

	if err = EncodeObjectManagedFields(object, managed); err != nil {
		return nil, fmt.Errorf("failed to encode managed fields: %v", err)
	}

	return object, nil
}
