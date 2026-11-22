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
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// capManagersManager 包装 FieldManager，限制来自 update 的 manager 条目数量，
// 防止 managedFields 随更新次数无限增长。
type capManagersManager struct {
	fieldManager          Manager
	maxUpdateManagers     int
	oldUpdatesManagerName string
}

var _ Manager = &capManagersManager{}

// NewCapManagersManager 创建一个包装后的 FieldManager：保证来自 update 的 manager
// 数量不超过 maxUpdateManagers，超出时在每次更新中合并最旧的条目。
func NewCapManagersManager(fieldManager Manager, maxUpdateManagers int) Manager {
	return &capManagersManager{
		fieldManager:          fieldManager,
		maxUpdateManagers:     maxUpdateManagers,
		oldUpdatesManagerName: "ancient-changes",
	}
}

// Update 实现 Manager：先执行真实更新，再对结果做条目数量上限合并。
func (f *capManagersManager) Update(liveObj, newObj runtime.Object, managed Managed, manager string) (runtime.Object, Managed, error) {
	object, managed, err := f.fieldManager.Update(liveObj, newObj, managed, manager)
	if err != nil {
		return object, managed, err
	}
	if managed, err = f.capUpdateManagers(managed); err != nil {
		return nil, nil, fmt.Errorf("failed to cap update managers: %v", err)
	}
	return object, managed, nil
}

// Apply 实现 Manager：apply 产生的条目不参与上限合并，直接透传。
func (f *capManagersManager) Apply(liveObj, appliedObj runtime.Object, managed Managed, fieldManager string, force bool) (runtime.Object, Managed, error) {
	return f.fieldManager.Apply(liveObj, appliedObj, managed, fieldManager, force)
}

// capUpdateManagers 把最旧的一批 update 条目合并进按版本划分的桶条目，
// 使 update 来源的条目数不超过 f.maxUpdateManagers。
func (f *capManagersManager) capUpdateManagers(managed Managed) (newManaged Managed, err error) {
	// Gather all entries from updates
	// 收集所有非 apply 的（即来自更新的）manager 条目。
	updaters := []string{}
	for manager, fields := range managed.Fields() {
		if !fields.Applied() {
			updaters = append(updaters, manager)
		}
	}
	if len(updaters) <= f.maxUpdateManagers {
		return managed, nil
	}

	// If we have more than the maximum, sort the update entries by time, oldest first.
	// 条目超限时按更新时间排序，最旧的排在前面；时间相同时用 manager 名作为稳定排序的平局裁决。
	sort.Slice(updaters, func(i, j int) bool {
		iTime, jTime, iSeconds, jSeconds := managed.Times()[updaters[i]], managed.Times()[updaters[j]], int64(0), int64(0)
		if iTime != nil {
			iSeconds = iTime.Unix()
		}
		if jTime != nil {
			jSeconds = jTime.Unix()
		}
		if iSeconds != jSeconds {
			return iSeconds < jSeconds
		}
		return updaters[i] < updaters[j]
	})

	// Merge the oldest updaters with versioned bucket managers until the number of updaters is under the cap
	// 从最旧的条目开始，把它们合并到按 API 版本划分的桶 manager，直到数量降到上限以下。
	versionToFirstManager := map[string]string{}
	for i, length := 0, len(updaters); i < len(updaters) && length > f.maxUpdateManagers; i++ {
		manager := updaters[i]
		vs := managed.Fields()[manager]
		time := managed.Times()[manager]
		version := string(vs.APIVersion())

		// Create a new manager identifier for the versioned bucket entry.
		// The version for this manager comes from the version of the update being merged into the bucket.
		// 为版本桶生成新的 manager 标识，版本取自被合并的更新条目。
		bucket, err := BuildManagerIdentifier(&metav1.ManagedFieldsEntry{
			Manager:    f.oldUpdatesManagerName,
			Operation:  metav1.ManagedFieldsOperationUpdate,
			APIVersion: version,
		})
		if err != nil {
			return managed, fmt.Errorf("failed to create bucket manager for version %v: %v", version, err)
		}

		// Merge the fieldets if this is not the first time the version was seen.
		// Otherwise just record the manager name in versionToFirstManager
		// 同一版本第二次及以后出现时才真正合并；首次出现只记录其 manager 名，
		// 这样可以在不新建桶的情况下尽量少改动条目。
		if first, ok := versionToFirstManager[version]; ok {
			// If the bucket doesn't exists yet, create one.
			// 该版本还没有桶时，把首个条目改造成桶（复用其字段集合）。
			if _, ok := managed.Fields()[bucket]; !ok {
				s := managed.Fields()[first]
				delete(managed.Fields(), first)
				managed.Fields()[bucket] = s
			}

			managed.Fields()[bucket] = fieldpath.NewVersionedSet(vs.Set().Union(managed.Fields()[bucket].Set()), vs.APIVersion(), vs.Applied())
			delete(managed.Fields(), manager)
			length--

			// Use the time from the update being merged into the bucket, since it is more recent.
			// 桶的时间取最近一次被合并更新的时间，反映桶内最新的活动。
			managed.Times()[bucket] = time
		} else {
			versionToFirstManager[version] = manager
		}
	}

	return managed, nil
}
