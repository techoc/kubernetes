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

package podautoscaler

import (
	"maps"
	"slices"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/controller/util/selectors"
)

// hpaSelectorTracker 有两种实现，由 HPAOptimizedSelectorStore 特性门控决定：
// 门控开启时使用 hpaSelectorStore（优化实现），关闭时使用 biMultimapSelectorTracker。
// TODO: HPAOptimizedSelectorStore 正式发布后，
// 移除 Bimultimap 实现。
// 接口只暴露选择器的增删以及按 Pod 反查 HPA 的能力，
// 使控制器无需关心底层存储结构。
type hpaSelectorTracker interface {
	PutIfAbsent(namespace string, key selectors.Key, selector labels.Selector) bool

	PutIfPresent(namespace string, key selectors.Key, selector labels.Selector) bool

	Delete(namespace string, key selectors.Key)

	HPAsMatchingPods(namespace string, pods []*v1.Pod, limit int) []selectors.Key
}

// 编译期断言：优化实现必须满足 hpaSelectorTracker 接口。
var _ hpaSelectorTracker = &hpaSelectorStore{}

// hpaSelectorStore 按命名空间保存 HPA 标签选择器，
// 读写锁使并发只读的重叠检测可以在所有 worker 间并行进行。
type hpaSelectorStore struct {
	mu         sync.RWMutex
	namespaces map[string]map[selectors.Key]labels.Selector
}

// newHPASelectorStore 创建一个空的 hpaSelectorStore。
func newHPASelectorStore() *hpaSelectorStore {
	return &hpaSelectorStore{
		namespaces: make(map[string]map[selectors.Key]labels.Selector),
	}
}

// PutIfAbsent 仅在键不存在时插入 HPA 选择器；
// 返回 true 表示确实插入了新键，插入失败说明该 HPA 的选择器已存在。
// 写操作持有独占锁，防止与查询并发读写同一 map。
func (s *hpaSelectorStore) PutIfAbsent(namespace string, key selectors.Key, selector labels.Selector) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 命名空间已存在时只判断该键是否已登记。
	if ns := s.namespaces[namespace]; ns != nil {
		if _, exists := ns[key]; exists {
			return false
		}
		ns[key] = selector
	} else {
		s.namespaces[namespace] = map[selectors.Key]labels.Selector{key: selector}
	}
	return true
}

// PutIfPresent 仅在键已存在时更新 HPA 选择器；
// 返回 true 表示更新成功，避免为已删除的 HPA 重新插入陈旧选择器。
func (s *hpaSelectorStore) PutIfPresent(namespace string, key selectors.Key, selector labels.Selector) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ns := s.namespaces[namespace]; ns != nil {
		if _, exists := ns[key]; exists {
			ns[key] = selector
			return true
		}
	}
	return false
}

// Delete 移除指定 HPA 的选择器；命名空间空了之后一并删除，避免 map 无限膨胀。
func (s *hpaSelectorStore) Delete(namespace string, key selectors.Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 最后一个选择器被删除后回收命名空间条目。
	if ns := s.namespaces[namespace]; ns != nil {
		delete(ns, key)
		if len(ns) == 0 {
			delete(s.namespaces, namespace)
		}
	}
}

// HPAsMatchingPods 返回指定命名空间中选择器与任一给定 Pod 匹配的所有 HPA 键。
// 该操作只读且仅持有共享读锁，允许所有 worker 完全并发执行。
// limit > 0 时，找到足够数量的不同匹配项即可提前返回。
func (s *hpaSelectorStore) HPAsMatchingPods(namespace string, pods []*v1.Pod, limit int) []selectors.Key {
	// 只读路径使用读锁，不阻塞其他查询。
	s.mu.RLock()
	defer s.mu.RUnlock()

	nsSelectors := s.namespaces[namespace]
	if len(nsSelectors) == 0 {
		return nil
	}

	// 同一 ReplicaSet 的 Pod 通常标签完全相同；
	// 先按标签集合去重，避免对同一标签集重复跑选择器匹配。
	uniqueLabels := make(map[string]labels.Set)
	for _, pod := range pods {
		key := labels.Set(pod.Labels).String()
		if _, seen := uniqueLabels[key]; !seen {
			uniqueLabels[key] = labels.Set(pod.Labels)
		}
	}

	// 用集合收集结果，确保同一 HPA 命中多个 Pod 时只返回一次。
	hpas := make(map[selectors.Key]struct{})
	for _, set := range uniqueLabels {
		for key, sel := range nsSelectors {
			if sel.Matches(set) {
				hpas[key] = struct{}{}
				// 达到上限立即返回：调用方只关心是否有足够数量的 HPA 需要处理。
				if limit > 0 && len(hpas) >= limit {
					return slices.Collect(maps.Keys(hpas))
				}
			}
		}
	}
	return slices.Collect(maps.Keys(hpas))
}

// 编译期断言：旧实现同样满足 hpaSelectorTracker 接口。
var _ hpaSelectorTracker = &biMultimapSelectorTracker{}

// biMultimapSelectorTracker 是基于 BiMultimap 的旧版 hpaSelectorTracker，
// 在 HPAOptimizedSelectorStore 门控关闭时使用。
// TODO: HPAOptimizedSelectorStore 正式发布后移除旧实现。
type biMultimapSelectorTracker struct {
	mu        sync.Mutex
	selectors *selectors.BiMultimap
}

// newBiMultimapSelectorTracker 创建一个空的旧版选择器追踪器。
func newBiMultimapSelectorTracker() *biMultimapSelectorTracker {
	return &biMultimapSelectorTracker{
		selectors: selectors.NewBiMultimap(),
	}
}

// PutIfAbsent 仅在 HPA 键不存在时写入选择器，返回是否插入。
func (t *biMultimapSelectorTracker) PutIfAbsent(namespace string, key selectors.Key, selector labels.Selector) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.selectors.SelectorExists(key) {
		return false
	}
	t.selectors.PutSelector(key, selector)
	return true
}

// PutIfPresent 仅在 HPA 键已存在时更新选择器，返回是否更新。
func (t *biMultimapSelectorTracker) PutIfPresent(namespace string, key selectors.Key, selector labels.Selector) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.selectors.SelectorExists(key) {
		return false
	}
	t.selectors.PutSelector(key, selector)
	return true
}

// Delete 从双向映射中移除指定 HPA 的选择器。
func (t *biMultimapSelectorTracker) Delete(namespace string, key selectors.Key) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.selectors.DeleteSelector(key)
}

// HPAsMatchingPods 通过临时把 Pod 标签放入 BiMultimap 后反向查询，
// 返回选择器匹配任一 Pod 的 HPA 键；查询结束后清空临时 Pod 条目。
func (t *biMultimapSelectorTracker) HPAsMatchingPods(namespace string, pods []*v1.Pod, limit int) []selectors.Key {
	t.mu.Lock()
	defer t.mu.Unlock()

	hpas := map[selectors.Key]struct{}{}
	for _, p := range pods {
		podKey := selectors.Key{Name: p.Name, Namespace: p.Namespace}
		// 只在查询期间登记 Pod 标签，避免把 Pod 长期留在选择器索引中。
		t.selectors.Put(podKey, p.Labels)

		selectingHpas, ok := t.selectors.ReverseSelect(podKey)
		if !ok {
			continue
		}
		for _, hpa := range selectingHpas {
			hpas[hpa] = struct{}{}
		}
	}
	// 清空本次查询加入的全部 Pod，防止后续查询被陈旧数据污染。
	t.selectors.KeepOnly([]selectors.Key{})

	hpaList := []selectors.Key{}
	for hpa := range hpas {
		hpaList = append(hpaList, hpa)
	}
	return hpaList
}
