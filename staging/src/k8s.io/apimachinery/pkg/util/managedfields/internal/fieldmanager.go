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

// DefaultMaxUpdateManagers 定义默认保留的 update 类型 managedFields 条目数上限；
// 超过上限时合并最旧条目，直到数量低于上限。
// TODO(jennybuckley): 确认这是否为最佳值；理想情况下不应不必要地合并过多条目。
const DefaultMaxUpdateManagers int = 10

// DefaultTrackOnCreateProbability 定义从对象创建时开始跟踪字段管理的默认概率，
// 而不是从对象首次被 apply 时开始。
// 中文说明：设为 1 表示完全从创建时开始跟踪，避免为省开销而随机跳过，
// 从而保证 apply 语义确定。
const DefaultTrackOnCreateProbability float32 = 1

// atMostEverySecond 限制错误日志频率，避免 managedFields 失败时刷屏。
var atMostEverySecond = NewAtMostEvery(time.Second)

// FieldManager 更新托管字段并合并应用配置。
//
// 中文说明：它把内层 Manager 链与实际 API 对象的编解码隔离开——调用方只需
// 传入 live/new 对象，FieldManager 负责取用、合并与回写 managedFields。
type FieldManager struct {
	fieldManager Manager
	subresource  string
}

// NewFieldManager 创建 FieldManager，在 update 和 apply 请求中解码、管理并重新编码 managedFields。
func NewFieldManager(f Manager, subresource string) *FieldManager {
	return &FieldManager{fieldManager: f, subresource: subresource}
}

// newDefaultFieldManager 为 Manager 包装默认逻辑。
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

	//请求尝试通过子资源手动设置 managedFields 时，使用线上对象的 managedFields。
	if ignoreManagedFieldsFromRequestObject {
		return emptyManagedFieldsOnErr(DecodeManagedFields(liveAccessor.GetManagedFields()))
	}

	//对象没有元数据时直接返回，不设置 managedFields，
	//使 create、update 和 patch 正常工作。
	newAccessor, err := meta.Accessor(newObj)
	if err != nil {
		return nil, err
	}

	// 显式的空列表（或单个空条目）是“重置 managedFields”的约定信号。
	if isResetManagedFields(newAccessor.GetManagedFields()) {
		return NewEmptyManaged(), nil
	}

	//如果托管字段为空或解码失败，
	//则尝试使用线上对象，防止不理解 managedFields 的客户端意外删除它。
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

// Update 用于对象已完成合并的非 apply 场景，仅更新输出对象中的托管字段。
func (f *FieldManager) Update(liveObj, newObj runtime.Object, manager string) (object runtime.Object, err error) {
	//首先尝试解码 update 提供的托管字段，
	//以允许直接更新托管字段。
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

// UpdateNoErrors 与 Update 相同，但不返回错误；
// 发生错误时保留 liveObj 的 managedFields。
func (f *FieldManager) UpdateNoErrors(liveObj, newObj runtime.Object, manager string) runtime.Object {
	obj, err := f.Update(liveObj, newObj, manager)
	if err != nil {
		//出错时保留线上对象的 managedFields，而不是全部剥离，
		//从而避免因转换 webhook 不可用等更新失败导致静默数据丢失。
		//注意：在此代码分支中，liveObj 和 newObj 的 meta.Accessor 不会返回错误，
		//否则前面的 f.Update 会返回 "newObj, nil"。因此这里不处理访问器返回错误的情况。
		if liveAccessor, aErr := meta.Accessor(liveObj); aErr == nil {
			if newAccessor, aErr := meta.Accessor(newObj); aErr == nil {
				atMostEverySecond.Do(func() {
					//nolint:logcheck // 不应执行到这里。
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

// 如果 managedFields 表明用户试图重置托管字段，则返回 true，
// 即列表非 nil 但为空，
// 或列表仅含一个空条目。
func isResetManagedFields(managedFields []metav1.ManagedFieldsEntry) bool {
	if len(managedFields) == 0 {
		return managedFields != nil
	}

	if len(managedFields) == 1 {
		return reflect.DeepEqual(managedFields[0], metav1.ManagedFieldsEntry{})
	}

	return false
}

// Apply 用于服务端应用，负责合并对象并更新托管字段。
func (f *FieldManager) Apply(liveObj, appliedObj runtime.Object, manager string, force bool) (object runtime.Object, err error) {
	//对象没有元数据时不允许 apply。
	accessor, err := meta.Accessor(liveObj)
	if err != nil {
		return nil, fmt.Errorf("couldn't get accessor: %v", err)
	}

	//解码线上对象的 managedFields，因为补丁中不允许携带它。
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
