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

package internal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// stripMetaManager 装饰 Manager，从记录的字段集合中移除元数据和类型元数据，
// 避免每个写入者都看似拥有 apiVersion、kind 和通用元数据。
type stripMetaManager struct {
	fieldManager Manager

	//stripSet 是绝不应出现在 managedFields 中的字段列表。
	stripSet *fieldpath.Set
}

var _ Manager = &stripMetaManager{}

// NewStripMetaManager 创建 Manager，从管理器的字段集合中剥离元数据和类型元数据字段。
// 这些路径构成固定拒绝列表；未列出的字段（如 labels 和 annotations）
// 仍会被跟踪，因为它们可以被合法拥有。
func NewStripMetaManager(fieldManager Manager) Manager {
	return &stripMetaManager{
		fieldManager: fieldManager,
		stripSet: fieldpath.NewSet(
			fieldpath.MakePathOrDie("apiVersion"),
			fieldpath.MakePathOrDie("kind"),
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("metadata", "name"),
			fieldpath.MakePathOrDie("metadata", "namespace"),
			fieldpath.MakePathOrDie("metadata", "creationTimestamp"),
			fieldpath.MakePathOrDie("metadata", "selfLink"),
			fieldpath.MakePathOrDie("metadata", "uid"),
			fieldpath.MakePathOrDie("metadata", "clusterName"),
			fieldpath.MakePathOrDie("metadata", "generation"),
			fieldpath.MakePathOrDie("metadata", "managedFields"),
			fieldpath.MakePathOrDie("metadata", "resourceVersion"),
		),
	}
}

// Update 实现 Manager 接口。
func (f *stripMetaManager) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	newObj, managed, err := f.fieldManager.Update(liveObj, newObj, managed, manager)
	if err != nil {
		return nil, nil, err
	}
	f.stripFields(managed.Fields(), manager)
	return newObj, managed, nil
}

// Apply 实现 Manager 接口。
func (f *stripMetaManager) Apply(liveObj, appliedObj runtime.Object, managed Managed, manager string, force bool) (runtime.Object, Managed, error) {
	newObj, managed, err := f.fieldManager.Apply(liveObj, appliedObj, managed, manager, force)
	if err != nil {
		return nil, nil, err
	}
	f.stripFields(managed.Fields(), manager)
	return newObj, managed, nil
}

// stripFields 从 managed 中移除 typed 内预定义的一组路径。
func (f *stripMetaManager) stripFields(managed fieldpath.ManagedFields, manager string) {
	vs, ok := managed[manager]
	if ok {
		if vs == nil {
			panic(fmt.Sprintf("Found unexpected nil manager which should never happen: %s", manager))
		}
		//减去拒绝列表后如果没有剩余字段，说明管理器不拥有有效字段，
		//应完全删除其条目。
		newSet := vs.Set().Difference(f.stripSet)
		if newSet.Empty() {
			delete(managed, manager)
		} else {
			managed[manager] = fieldpath.NewVersionedSet(newSet, vs.APIVersion(), vs.Applied())
		}
	}
}
