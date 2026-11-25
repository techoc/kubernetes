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

package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// makeMutationTestPod 构造仅含名字、UID 与 resourceVersion 的 pod，用于驱动 mutation cache 的版本比较。
func makeMutationTestPod(name string, uid types.UID, rv string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             uid,
			ResourceVersion: rv,
		},
	}
}

// TestMutationCacheOnAddOrUpdate 验证 informer 推送 OnAddOrUpdate 时，只有当 store 的版本
// 不旧于 mutation 的版本，缓存中的 mutation 才会被清除（即 mutation 已过时）。
func TestMutationCacheOnAddOrUpdate(t *testing.T) {
	tests := map[string]struct {
		mutationRV  string
		storeUID    types.UID // UID passed to OnAddOrUpdate
		storeRV     string    // RV passed to OnAddOrUpdate
		wantCleared bool
	}{
		"equal-rv-clears": {
			mutationRV:  "2",
			storeUID:    "uid-1",
			storeRV:     "2",
			wantCleared: true,
		},
		"newer-store-clears": {
			mutationRV:  "2",
			storeUID:    "uid-1",
			storeRV:     "3",
			wantCleared: true,
		},
		"older-store-keeps": {
			mutationRV:  "2",
			storeUID:    "uid-1",
			storeRV:     "1",
			wantCleared: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewStore(MetaNamespaceKeyFunc)
			indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
			mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Minute, false)

			mutated := makeMutationTestPod("pod", "uid-1", tc.mutationRV)
			mc.Mutation(mutated)

			mc.OnAddOrUpdate(makeMutationTestPod("pod", tc.storeUID, tc.storeRV))

			// Add the pod to the backing store at RV "1" (always older than the
			// mutation's RV "2"), so we can tell whether GetByKey returns the
			// mutation or the backing copy.
			require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "1")))

			got, exists, err := mc.GetByKey("pod")
			require.NoError(t, err)
			require.True(t, exists)

			gotRV := got.(*v1.Pod).ResourceVersion
			if tc.wantCleared {
				assert.Equal(t, "1", gotRV, "backing store version expected after mutation cleared")
			} else {
				assert.Equal(t, tc.mutationRV, gotRV, "mutation version expected")
			}
		})
	}
}

// TestMutationCacheOnDelete 检查在 Mutation 之后调用 OnDelete 的行为。
// 验证 OnDelete 之后哪些情况下 mutation 会被清除：同一 UID 的旧版本（可能来自 tombstone）
// 以及新 UID 都应清除；只有"旧版本且不同 UID"这一无法确认已删除的情况会保留 mutation。
func TestMutationCacheOnDelete(t *testing.T) {
	tests := map[string]struct {
		deleteUID  types.UID
		deleteRV   string
		wantExists bool
	}{
		"old-rv-same-UID": {
			deleteUID:  "uid-1",
			deleteRV:   "1", // Could be from a stale object in DeletedFinalStateUnknown.
			wantExists: false,
		},
		"new-rv-different-UID": {
			deleteUID:  "uid-2",
			deleteRV:   "3",
			wantExists: false,
		},
		"old-rv-different-uid": {
			deleteUID:  "uid-other",
			deleteRV:   "1",
			wantExists: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewStore(MetaNamespaceKeyFunc)
			indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
			mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Minute, true)

			// Backing store has the pod at RV "2".
			require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "2")))

			// Mutation brings it to RV "3".
			mc.Mutation(makeMutationTestPod("pod", "uid-1", "3"))
			deletedPod := makeMutationTestPod("pod", tc.deleteUID, tc.deleteRV)
			require.NoError(t, store.Delete(deletedPod))
			mc.OnDelete(deletedPod)

			_, exists, err := mc.GetByKey("pod")
			require.NoError(t, err)
			require.Equal(t, tc.wantExists, exists)
		})
	}
}

// TestMutationCacheUpdateConcurrentDelete 检查在一些 informer 事件之后调用
// Mutation 的行为。
// 模拟"更新后立即被删除"的竞态：informer 事件晚于实际变更到达，
// 过期的 Mutation 因缓存中存在 tombstone 而被丢弃。
func TestMutationCacheUpdateConcurrentDelete(t *testing.T) {
	store := NewStore(MetaNamespaceKeyFunc)
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Hour, true)

	// Backing store has the pod at RV "1".
	require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "1")))

	// Client starts an update leading to RV "2", which immediately gets
	// deleted by some other client. That deletion is received before the
	// client finishes its update.
	updatedPod := makeMutationTestPod("pod", "uid-1", "2")
	require.NoError(t, store.Update(updatedPod))
	require.NoError(t, store.Delete(updatedPod))

	// Informer events get delivered with a delay.
	mc.OnAddOrUpdate(updatedPod)
	mc.OnDelete(updatedPod)

	// This Mutation call is stale, which gets detected because
	// the mutation cache contains a tombstone object.
	mc.Mutation(updatedPod)

	_, exists, err := mc.GetByKey("pod")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, []any{"pod"}, mc.(*mutationCache).mutationCache.Keys())
}

// TestMutationCacheUpdateConcurrentRecreate 检查在一些 informer 事件之后调用
// Mutation 的行为。
// 模拟"对象被删除后同名重建"的竞态：store 中已有更新的同名对象，
// 过期的 Mutation 应被识别并丢弃。
func TestMutationCacheUpdateConcurrentRecreate(t *testing.T) {
	store := NewStore(MetaNamespaceKeyFunc)
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{})
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Hour, true)

	// Backing store has the pod at RV "1".
	require.NoError(t, store.Add(makeMutationTestPod("pod", "uid-1", "1")))

	// Client starts an update leading to RV "2", which immediately gets
	// replaced by some other pod using the same name. Those changes are
	// received before the client finishes its update.
	updatedPod := makeMutationTestPod("pod", "uid-1", "2")
	require.NoError(t, store.Update(updatedPod))
	require.NoError(t, store.Delete(updatedPod))
	replacementPod := makeMutationTestPod("pod", "uid-2", "3")
	require.NoError(t, store.Add(replacementPod))

	// Informer events get delivered with a delay.
	mc.OnAddOrUpdate(updatedPod)
	mc.OnDelete(updatedPod)
	mc.OnAddOrUpdate(replacementPod)

	// This Mutation call is stale, which gets detected because there is a more
	// recent object in the store.
	mc.Mutation(updatedPod)

	got, exists, err := mc.GetByKey("pod")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, replacementPod, got)
	require.Empty(t, mc.(*mutationCache).mutationCache.Keys())
}

// TestMutationCacheOnDeleteClearsStaleMutation 验证促成 OnDelete 方法的核心缺陷：
// 更新之后存入的 mutation，一旦 informer 报告对象已删除，就不应继续存活。
//
// 测试场景：
//  1. 调用方添加对象（includeAdds=true），使 mutation cache 即便后端 store 为空
//     也会返回它。
//  2. 对象在服务端被删除，后端 store 此时为空。
//  3. 若没有 OnDelete，mutation 会残留在缓存中，ByIndex 仍会返回它，
//     导致调用方无法重建该对象。
//
// 核心回归点：OnDelete 必须清除 update 之后残留的 mutation，否则 ByIndex 会持续返回
// 已删除的对象，导致调用方以为对象仍存在而无法重建。
func TestMutationCacheOnDeleteClearsStaleMutation(t *testing.T) {
	store := NewStore(MetaNamespaceKeyFunc)
	byNameIndex := "by-name"
	indexer := NewIndexer(MetaNamespaceKeyFunc, Indexers{
		byNameIndex: func(obj interface{}) ([]string, error) {
			return []string{obj.(*v1.Pod).Name}, nil
		},
	})
	mc := NewIntegerResourceVersionMutationCache(klog.Background(), store, indexer, time.Minute, true /* includeAdds */)

	// Simulate an update: mutation cache holds the pod at RV "2" while the
	// backing store is still empty (informer hasn't caught up yet).
	mc.Mutation(makeMutationTestPod("pod", "uid-1", "2"))

	items, err := mc.ByIndex(byNameIndex, "pod")
	require.NoError(t, err)
	assert.Len(t, items, 1, "mutation should be visible via ByIndex when backing store is empty")

	// Informer reports the delete: OnDelete must clear the stale mutation so
	// that the next ByIndex call returns nothing and lets the caller recreate
	// the object.
	mc.OnDelete(makeMutationTestPod("pod", "uid-1", "1"))

	items, err = mc.ByIndex(byNameIndex, "pod")
	require.NoError(t, err)
	assert.Empty(t, items, "OnDelete must clear the mutation; ByIndex must return nothing")
}
