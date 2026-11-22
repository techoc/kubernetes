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

package store

import (
	"fmt"
	"sort"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage/cacher/key"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
)

// NewWatchCacheStorage 构造底层存储；快照相关结构仅在特性门控开启时创建，
// 关闭时保持 nil，使所有快照方法天然退化为"不支持精确 RV"。
func NewWatchCacheStorage(keyFunc func(runtime.Object) (string, error), indexers *cache.Indexers) *WatchCacheStorage {
	storage := &WatchCacheStorage{
		keyFunc:             keyFunc,
		store:               NewIndexer(indexers),
		listResourceVersion: 0,
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
		storage.snapshottingEnabled.Store(true)
		storage.snapshots = NewSnapshotter()
	}
	return storage
}

// WatchCacheStorage 是 watch cache 的存储层，负责元素索引与历史快照。
type WatchCacheStorage struct {
	keyFunc func(runtime.Object) (string, error)

	// store will effectively support LIST operation from the "end of cache
	// history" i.e. from the moment just after the newest cached watched event.
	// It is necessary to effectively allow clients to start watching at now.
	// NOTE: We assume that <store> is thread-safe.
	store Indexer

	// ResourceVersion of the last list result (populated via Replace() method).
	listResourceVersion uint64

	// Stores previous snapshots of orderedLister to allow serving requests from previous revisions.
	//
	// snapshottingEnabled 之所以用 atomic.Bool 而不是受外部锁保护的普通字段：
	// 一致性检查器可能从独立 goroutine 把它置为 false，读写双方不共享同一把锁。
	snapshots           Snapshotter
	snapshottingEnabled atomic.Bool
}

// StoreLocked returns the live store as a Snapshot.
// Unlike GetExactSnapshotLocked this is not an immutable point-in-time copy.
// The caller must hold the lock for the duration of use.
func (w *WatchCacheStorage) StoreLocked() Snapshot {
	return w.store
}

// SnapshottingEnabled 报告当前是否允许基于快照服务 list。
// 一致性检查失败后会被关闭，直到再次确认一致。
func (w *WatchCacheStorage) SnapshottingEnabled() bool {
	return w.snapshots != nil && w.snapshottingEnabled.Load()
}

// CanServeExactRV 判断是否存在不晚于 rv 的快照；没有则调用方需回退到 etcd。
func (w *WatchCacheStorage) CanServeExactRV(rv uint64) bool {
	if w.snapshots == nil {
		return false
	}
	_, canServe := w.snapshots.GetLessOrEqual(rv)
	return canServe
}

// UpdateListResourceVersion 记录最近一次 list 的版本，供无条件 list 的结果使用。
func (w *WatchCacheStorage) UpdateListResourceVersion(rv uint64) {
	w.listResourceVersion = rv
}

// Compact 按 etcd 压缩进度回收更旧的快照，防止快照无限增长。
func (w *WatchCacheStorage) Compact(rev uint64) {
	if w.snapshots == nil {
		return
	}
	w.snapshots.RemoveLess(rev)
}

// MarkConsistent 由一致性检查器回调：不一致时停用快照并清空，避免继续用坏数据服务请求。
func (w *WatchCacheStorage) MarkConsistent(consistent bool) {
	if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
		w.snapshottingEnabled.Store(consistent)
		if !consistent && w.snapshots != nil {
			w.snapshots.Reset()
		}
	}
}

// LatestSnapshotLocked 返回最新快照；调用方需持有队列锁。
func (w *WatchCacheStorage) LatestSnapshotLocked() (Snapshot, bool) {
	if w.SnapshottingEnabled() {
		return w.snapshots.Latest()
	}
	return nil, false
}

// GetLatestSnapshotOrBuildLocked 优先复用最新快照，缺失时从当前 store 现场构建。
//
// 现场构建而非直接返回 live store，是为了让调用方拿到不可变视图，
// 后续写入不会改变正在遍历的列表。
func (w *WatchCacheStorage) GetLatestSnapshotOrBuildLocked(key, continueKey string) (Snapshot, error) {
	if snap, ok := w.LatestSnapshotLocked(); ok {
		// Snapshots are added in order as we update store, so the
		// latest snapshot match latest store state and latest revision.
		return snap, nil
	}
	// TODO: Consider using Indexer Clone() after benchmarking.
	return orderedSnapshotResponseFromIndexer(w.store, key, continueKey)
}

// orderedSnapshotResponseFromIndexer 通过有序前缀遍历构造一个只读快照。
func orderedSnapshotResponseFromIndexer(indexer Indexer, key, continueKey string) (Snapshot, error) {
	items, err := indexer.OrderedListPrefix(key, continueKey)
	if err != nil {
		return nil, err
	}
	return orderedListSnapshot{Items: items}, nil
}

// orderedListSnapshot 是已经按 key 排好序的列表快照。
type orderedListSnapshot struct {
	Items []interface{}
}

var _ Snapshot = (*orderedListSnapshot)(nil)

// GetByKey 在快照内做线性查找；快照通常只用于 list，单点查询不常走这里。
func (o orderedListSnapshot) GetByKey(key string) (interface{}, bool, error) {
	for _, item := range o.Items {
		elem, ok := item.(*Element)
		if ok && elem.Key == key {
			return item, true, nil
		}
	}
	return nil, false, nil
}

// OrderedListPrefix 直接返回已排序结果，忽略参数：数据在构建时已按 key 截取。
func (o orderedListSnapshot) OrderedListPrefix(prefix, continueKey string) ([]interface{}, error) {
	return o.Items, nil
}

// listSnapshot 是未排序的列表快照（来自索引查询），需要在使用时过滤与排序。
type listSnapshot struct {
	Items []interface{}
}

var _ Snapshot = (*listSnapshot)(nil)

// GetByKey 线性查找指定 key。
func (l listSnapshot) GetByKey(key string) (interface{}, bool, error) {
	for _, item := range l.Items {
		elem, ok := item.(*Element)
		if ok && elem.Key == key {
			return item, true, nil
		}
	}
	return nil, false, nil
}

// OrderedListPrefix 在读取时应用前缀与续传过滤并排序，因为索引结果原本无序。
func (l listSnapshot) OrderedListPrefix(prefix string, continueKey string) ([]interface{}, error) {
	var result []interface{}
	for _, item := range l.Items {
		elem, ok := item.(*Element)
		if !ok {
			return nil, fmt.Errorf("non *Element returned from storage: %v", item)
		}
		// 续传语义是"严格大于 continueKey"，与 etcd 的分页行为保持一致。
		if len(continueKey) > 0 && continueKey >= elem.Key {
			continue
		}
		if !key.HasPathPrefix(elem.Key, prefix) {
			continue
		}
		result = append(result, item)
	}
	sort.Sort(sortableStoreElements(result))
	return result, nil
}

// sortableStoreElements 让 []interface{} 按 Element.Key 排序。
type sortableStoreElements []interface{}

func (s sortableStoreElements) Len() int {
	return len(s)
}

func (s sortableStoreElements) Less(i, j int) bool {
	return s[i].(*Element).Key < s[j].(*Element).Key
}

func (s sortableStoreElements) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Get takes runtime.Object as a parameter. However, it returns
// pointer to <storeElement>.
func (w *WatchCacheStorage) Get(obj interface{}) (interface{}, bool, error) {
	object, ok := obj.(runtime.Object)
	if !ok {
		return nil, false, fmt.Errorf("obj does not implement runtime.Object interface: %v", obj)
	}
	key, err := w.keyFunc(object)
	if err != nil {
		return nil, false, fmt.Errorf("couldn't compute key: %w", err)
	}

	return w.store.Get(&Element{Key: key, Object: object})
}

// GetByKey returns pointer to <storeElement>.
func (w *WatchCacheStorage) GetByKey(key string) (interface{}, bool, error) {
	return w.store.GetByKey(key)
}

// ListKeys 返回全部 key，供 list 时判定续传起点。
func (w *WatchCacheStorage) ListKeys() []string {
	return w.store.ListKeys()
}

// List returns list of pointers to <Element> objects.
func (w *WatchCacheStorage) List() []interface{} {
	return w.store.List()
}

// UpdateStoreLocked executes a mutation (Add, Update, Delete) on the underlying store.
//
// 每次变更后追加一份快照，使快照序列与事件序列一一对应；
// 快照仅在开关打开时维护，关闭后不再累积以节省内存。
func (w *WatchCacheStorage) UpdateStoreLocked(eventType watch.EventType, elem *Element, resourceVersion uint64) (err error) {
	switch eventType {
	case watch.Added:
		err = w.store.Add(elem)
	case watch.Modified:
		err = w.store.Update(elem)
	case watch.Deleted:
		err = w.store.Delete(elem)
	default:
		err = fmt.Errorf("unexpected event type: %v", eventType)
	}
	if err != nil {
		return err
	}
	if w.snapshots != nil && w.snapshottingEnabled.Load() {
		w.snapshots.Add(resourceVersion, w.store)
	}
	return nil
}

// CompactSnapshotsLocked prunes snapshots older than the oldest history version.
func (w *WatchCacheStorage) CompactSnapshotsLocked(oldestRV uint64) {
	if w.snapshots != nil && w.snapshottingEnabled.Load() {
		w.snapshots.RemoveLess(oldestRV)
	}
}

// ReplaceLocked replaces the elements in the underlying store and resets snapshots.
//
// Replace 代表一次完整重同步，旧快照已与新的 store 内容无关，必须先清空再登记新快照。
func (w *WatchCacheStorage) ReplaceLocked(toReplace []interface{}, resourceVersion string, version uint64) error {
	if err := w.store.Replace(toReplace, resourceVersion); err != nil {
		return err
	}
	if w.snapshots != nil {
		w.snapshots.Reset()
		if w.snapshottingEnabled.Load() {
			w.snapshots.Add(version, w.store)
		}
	}
	w.listResourceVersion = version
	return nil
}

// GetExactSnapshotLocked retrieves a snapshot less than or equal to the given resource version.
//
// 找不到时返回 ResourceExpired：调用方应据此让客户端重新 list，而不是回退到更旧的视图。
func (w *WatchCacheStorage) GetExactSnapshotLocked(resourceVersion uint64) (Snapshot, error) {
	if w.snapshots == nil {
		return nil, errors.NewResourceExpired(fmt.Sprintf("too old resource version: %d", resourceVersion))
	}
	snap, ok := w.snapshots.GetLessOrEqual(resourceVersion)
	if !ok {
		return nil, errors.NewResourceExpired(fmt.Sprintf("too old resource version: %d", resourceVersion))
	}
	return snap, nil
}

// GetByIndexSnapshot retrieves elements by index and wraps them in a Snapshot.
// 索引查询结果无序，包装成 listSnapshot 让后续前缀遍历负责排序。
func (w *WatchCacheStorage) GetByIndexSnapshot(indexName, value string) (Snapshot, error) {
	result, err := w.store.ByIndex(indexName, value)
	if err != nil {
		return nil, err
	}
	return listSnapshot{Items: result}, nil
}

// ListResourceVersion returns the list resource version.
func (w *WatchCacheStorage) ListResourceVersion() uint64 {
	return w.listResourceVersion
}
