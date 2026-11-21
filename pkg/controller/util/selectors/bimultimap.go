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

package selectors

import (
	"fmt"
	"strings"
	"sync"

	pkglabels "k8s.io/apimachinery/pkg/labels"
)

// BiMultimap 维护对象 key 之间的高效双向映射：附带标签的对象与带有选择器的对象
// 通过标签匹配自动建立关联，可同时按选择器正向查询和被选择对象反向查询。
type BiMultimap struct {
	mux sync.RWMutex

	// 按 key 索引的对象表。
	labeledObjects   map[Key]*labeledObject
	selectingObjects map[Key]*selectingObject

	// 按选择器/标签序列化值缓存的关联表，通过引用计数管理生命周期。
	labeledBySelecting map[selectorKey]*labeledObjects
	selectingByLabeled map[labelsKey]*selectingObjects
}

// NewBiMultimap 创建一个空的双向映射。
func NewBiMultimap() *BiMultimap {
	return &BiMultimap{
		labeledObjects:     make(map[Key]*labeledObject),
		selectingObjects:   make(map[Key]*selectingObject),
		labeledBySelecting: make(map[selectorKey]*labeledObjects),
		selectingByLabeled: make(map[labelsKey]*selectingObjects),
	}
}

// Key 是命名空间与名称组成的对象标识。
type Key struct {
	Name      string
	Namespace string
}

// Parse 将 "namespace/name" 格式的字符串解析为 Key；没有斜杠时视为无命名空间的名称。
func Parse(s string) (key Key) {
	ns := strings.SplitN(s, "/", 2)
	if len(ns) == 2 {
		key.Namespace = ns[0]
		key.Name = ns[1]
	} else {
		key.Name = ns[0]
	}
	return key
}

// String 返回 "namespace/name" 形式的字符串表示。
func (k Key) String() string {
	return fmt.Sprintf("%v/%v", k.Namespace, k.Name)
}

type selectorKey struct {
	key       string
	namespace string
}

type selectingObject struct {
	key      Key
	selector pkglabels.Selector
	// selectorKey is a stable serialization of selector for
	// association caching.
	selectorKey selectorKey
}

type selectingObjects struct {
	objects  map[Key]*selectingObject
	refCount int
}

type labelsKey struct {
	key       string
	namespace string
}

type labeledObject struct {
	key    Key
	labels map[string]string
	// labelsKey is a stable serialization of labels for association
	// caching.
	labelsKey labelsKey
}

type labeledObjects struct {
	objects  map[Key]*labeledObject
	refCount int
}

// Put 插入或更新带标签的对象，并根据其标签重建与选择器对象的关联（持写锁）。
func (m *BiMultimap) Put(key Key, labels map[string]string) {
	m.mux.Lock()
	defer m.mux.Unlock()

	labelsKey := labelsKey{
		key:       pkglabels.Set(labels).String(),
		namespace: key.Namespace,
	}
	if l, ok := m.labeledObjects[key]; ok {
		// Update labeled object.
		if labelsKey == l.labelsKey {
			// 标签未变化则关联无需更新，直接返回。
			return
		}
		// 标签变化会改变匹配关系，先清理旧关联再重建。
		m.delete(key)
	}
	// Add labeled object.
	labels = copyLabels(labels)
	labeledObject := &labeledObject{
		key:       key,
		labels:    labels,
		labelsKey: labelsKey,
	}
	m.labeledObjects[key] = labeledObject
	// Add associations.
	if _, ok := m.selectingByLabeled[labelsKey]; !ok {
		// 缓存未命中：需扫描全部同命名空间的选择器对象，计算匹配集合并缓存。
		selecting := &selectingObjects{
			objects: make(map[Key]*selectingObject),
		}
		set := pkglabels.Set(labels)
		for _, s := range m.selectingObjects {
			if s.key.Namespace != key.Namespace {
				continue
			}
			if s.selector.Matches(set) {
				selecting.objects[s.key] = s
			}
		}
		// Associate selecting with labeled.
		m.selectingByLabeled[labelsKey] = selecting
	}
	selecting := m.selectingByLabeled[labelsKey]
	selecting.refCount++
	for _, sObject := range selecting.objects {
		// Associate labeled with selecting.
		labeled := m.labeledBySelecting[sObject.selectorKey]
		labeled.objects[labeledObject.key] = labeledObject
	}
}

// Delete removes a labeled object and incoming associations.
func (m *BiMultimap) Delete(key Key) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.delete(key)
}

// delete 是 Delete 的无锁内部实现，供持锁的批量操作复用。
func (m *BiMultimap) delete(key Key) {
	if _, ok := m.labeledObjects[key]; !ok {
		// 对象不存在，无需处理。
		return
	}
	labeledObject := m.labeledObjects[key]
	labelsKey := labeledObject.labelsKey
	defer delete(m.labeledObjects, key)
	if _, ok := m.selectingByLabeled[labelsKey]; !ok {
		// 该标签值没有关联缓存，直接返回。
		return
	}
	// Remove associations.
	for _, selectingObject := range m.selectingByLabeled[labelsKey].objects {
		selectorKey := selectingObject.selectorKey
		// Delete selectingObject to labeledObject association.
		delete(m.labeledBySelecting[selectorKey].objects, key)
	}
	m.selectingByLabeled[labelsKey].refCount--
	// 引用计数归零时回收关联缓存，避免无效条目长期占用内存。
	if m.selectingByLabeled[labelsKey].refCount == 0 {
		delete(m.selectingByLabeled, labelsKey)
	}
}

// Exists 判断带标签的对象是否存在于映射中。
func (m *BiMultimap) Exists(key Key) bool {
	m.mux.RLock()
	defer m.mux.RUnlock()

	_, exists := m.labeledObjects[key]
	return exists
}

// PutSelector 插入或更新带选择器的对象，并依据选择器重建与带标签对象的关联（持写锁）。
func (m *BiMultimap) PutSelector(key Key, selector pkglabels.Selector) {
	m.mux.Lock()
	defer m.mux.Unlock()

	selectorKey := selectorKey{
		key:       selector.String(),
		namespace: key.Namespace,
	}
	if s, ok := m.selectingObjects[key]; ok {
		// Update selecting object.
		if selectorKey == s.selectorKey {
			// 选择器未变化则关联无需更新，直接返回。
			return
		}
		// Delete before readding.
		m.deleteSelector(key)
	}
	// Add selecting object.
	selectingObject := &selectingObject{
		key:         key,
		selector:    selector,
		selectorKey: selectorKey,
	}
	m.selectingObjects[key] = selectingObject
	// Add associations.
	if _, ok := m.labeledBySelecting[selectorKey]; !ok {
		// 缓存未命中：需扫描全部同命名空间的带标签对象，计算匹配集合并缓存。
		labeled := &labeledObjects{
			objects: make(map[Key]*labeledObject),
		}
		for _, l := range m.labeledObjects {
			if l.key.Namespace != key.Namespace {
				continue
			}
			set := pkglabels.Set(l.labels)
			if selector.Matches(set) {
				labeled.objects[l.key] = l
			}
		}
		// Associate labeled with selecting.
		m.labeledBySelecting[selectorKey] = labeled
	}
	labeled := m.labeledBySelecting[selectorKey]
	labeled.refCount++
	for _, labeledObject := range labeled.objects {
		// Associate selecting with labeled.
		selecting := m.selectingByLabeled[labeledObject.labelsKey]
		selecting.objects[selectingObject.key] = selectingObject
	}
}

// DeleteSelector 删除带选择器的对象及其创建的所有关联（持写锁）。
func (m *BiMultimap) DeleteSelector(key Key) {
	m.mux.Lock()
	defer m.mux.Unlock()
	m.deleteSelector(key)
}

// deleteSelector 是 DeleteSelector 的无锁内部实现，供持锁的批量操作复用。
func (m *BiMultimap) deleteSelector(key Key) {
	if _, ok := m.selectingObjects[key]; !ok {
		// 对象不存在，无需处理。
		return
	}
	selectingObject := m.selectingObjects[key]
	selectorKey := selectingObject.selectorKey
	defer delete(m.selectingObjects, key)
	if _, ok := m.labeledBySelecting[selectorKey]; !ok {
		// 该标签值没有关联缓存，直接返回。
		return
	}
	// Remove associations.
	for _, labeledObject := range m.labeledBySelecting[selectorKey].objects {
		labelsKey := labeledObject.labelsKey
		// Delete labeledObject to selectingObject association.
		delete(m.selectingByLabeled[labelsKey].objects, key)
	}
	m.labeledBySelecting[selectorKey].refCount--
	// 引用计数归零时回收关联缓存。
	if m.labeledBySelecting[selectorKey].refCount == 0 {
		delete(m.labeledBySelecting, selectorKey)
	}
}

// SelectorExists 判断带选择器的对象是否存在于映射中。
func (m *BiMultimap) SelectorExists(key Key) bool {
	m.mux.RLock()
	defer m.mux.RUnlock()

	_, exists := m.selectingObjects[key]
	return exists
}

// KeepOnly 仅保留指定的带标签对象，删除其余对象；等价于对未列出的 key 逐个调用 Delete，但在同一把锁内完成。
func (m *BiMultimap) KeepOnly(keys []Key) {
	m.mux.Lock()
	defer m.mux.Unlock()

	keyMap := make(map[Key]bool)
	for _, k := range keys {
		keyMap[k] = true
	}
	for k := range m.labeledObjects {
		if !keyMap[k] {
			m.delete(k)
		}
	}
}

// KeepOnlySelectors 仅保留指定的带选择器对象，删除其余对象；等价于对未列出的 key 逐个调用 DeleteSelector。
func (m *BiMultimap) KeepOnlySelectors(keys []Key) {
	m.mux.Lock()
	defer m.mux.Unlock()

	keyMap := make(map[Key]bool)
	for _, k := range keys {
		keyMap[k] = true
	}
	for k := range m.selectingObjects {
		if !keyMap[k] {
			m.deleteSelector(k)
		}
	}
}

// Select 返回与给定选择器对象关联的带标签对象列表；对象存在时 ok 为 true。
func (m *BiMultimap) Select(key Key) (keys []Key, ok bool) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	selectingObject, ok := m.selectingObjects[key]
	if !ok {
		// 对象不存在，无需处理。
		return nil, false
	}
	keys = make([]Key, 0)
	if labeled, ok := m.labeledBySelecting[selectingObject.selectorKey]; ok {
		for _, labeledObject := range labeled.objects {
			keys = append(keys, labeledObject.key)
		}
	}
	return keys, true
}

// ReverseSelect 返回选择了给定带标签对象的选择器对象列表；对象存在时 ok 为 true。
func (m *BiMultimap) ReverseSelect(key Key) (keys []Key, ok bool) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	labeledObject, ok := m.labeledObjects[key]
	if !ok {
		// 对象不存在，无需处理。
		return []Key{}, false
	}
	keys = make([]Key, 0)
	if selecting, ok := m.selectingByLabeled[labeledObject.labelsKey]; ok {
		for _, selectingObject := range selecting.objects {
			keys = append(keys, selectingObject.key)
		}
	}
	return keys, true
}

// copyLabels 复制标签映射，防止调用方后续修改原 map 影响内部缓存的一致性。
func copyLabels(labels map[string]string) map[string]string {
	l := make(map[string]string)
	for k, v := range labels {
		l[k] = v
	}
	return l
}
