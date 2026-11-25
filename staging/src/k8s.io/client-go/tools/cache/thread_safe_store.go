/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgofeaturegate "k8s.io/client-go/features"
	utiltrace "k8s.io/utils/trace"
)

// ThreadSafeStore 是允许并发地按索引访问存储后端的接口。它与 Indexer 类似，
// 但（未必）知道如何从给定对象中提取 Store 的 key。
//
// 简言之的注意事项：不得修改 Get 或 List 返回的任何对象，否则不仅线程不安全，
// 还会破坏索引功能。
//
// List/Get 提供的线程安全保证只有在调用方把返回的条目当作只读时才成立。例如，
// 通过 `Add` 存入的指针会被 `Get` 原样返回。多个客户端可能对同一个 key 调用
// `Get`，并以非线程安全的方式修改该指针。另请注意，修改 indexer 存储的对象
// （如果存在）*不会*自动触发重新索引。因此一般而言，直接修改 Get/List 返回的
// 对象并不是好主意。
//
// 所有方法都通过 RWMutex 保护内部 map；读操作返回的是对象指针本身，
// 因此“线程安全”只覆盖容器操作，不覆盖对象内容。
type ThreadSafeStore interface {
	// Add 以 key 插入对象；key 已存在时等同于 Update。
	Add(key string, obj interface{})
	// Update 替换 key 对应的对象，并同步更新所有索引。
	Update(key string, obj interface{})
	// Delete 等价于调用 DeleteWithObject(key, nil)，但不推荐使用：它不会更新
	// store 的 resource version，可能导致其过期。
	Delete(key string)
	// DeleteWithObject 删除对象；提供 obj 时会用其 resourceVersion 更新 store 的 RV。
	DeleteWithObject(key string, obj interface{})
	Get(key string) (item interface{}, exists bool)
	List() []interface{}
	ListKeys() []string
	// Replace 整体替换存储内容，并按新内容重建所有索引。
	Replace(map[string]interface{}, string)
	Index(indexName string, obj interface{}) ([]interface{}, error)
	IndexKeys(indexName, indexedValue string) ([]string, error)
	// Bookmark 仅推进 store 记录的最新 resourceVersion，不改变对象集合。
	Bookmark(rv string)
	LastStoreSyncResourceVersion() string
	ListIndexFuncValues(name string) []string
	ByIndex(indexName, indexedValue string) ([]interface{}, error)
	GetIndexers() Indexers

	// AddIndexers 向该 store 追加索引器；支持在 store 已有对象之后再添加索引。
	AddIndexers(newIndexers Indexers) error
	// Resync 是空操作，且已废弃。
	Resync() error
}

// ThreadSafeStoreWithTransaction 是可以批量执行多个事务的 store。
// 批量写入在同一把锁内完成，避免消费者看到中间状态，也减少锁开销。
type ThreadSafeStoreWithTransaction interface {
	ThreadSafeStore
	// Transaction 允许在一次调用中执行多个写操作。
	Transaction(fns ...ThreadSafeStoreTransaction)
}

// ThreadSafeStoreTransaction 内嵌一个 Transaction，并包含标识受影响对象的具体 Key。
type ThreadSafeStoreTransaction struct {
	Transaction
	Key string
}

// ThreadSafeStoreOption 用于在构造 store 时注入可选能力（如指标）。
type ThreadSafeStoreOption = func(*threadSafeMap)

// WithThreadSafeStoreMetrics 注入 informer 指标；未提供时构造器会补上 no-op 实现。
func WithThreadSafeStoreMetrics(identifier InformerNameAndResource, metricsProvider InformerMetricsProvider) ThreadSafeStoreOption {
	return func(c *threadSafeMap) {
		c.metrics = newStoreMetrics(identifier, metricsProvider)
	}
}

// storeIndex 实现 Store 接口的索引功能。
type storeIndex struct {
	// indexers 将名称映射到 IndexFunc。
	indexers Indexers
	// indices 将名称映射到 Index。
	indices Indices
}

// reset 清空所有索引；Replace 后需要基于新对象集合重建。
func (i *storeIndex) reset() {
	i.indices = Indices{}
}

// getKeysFromIndex 计算对象在一个索引上命中的全部 store key。
func (i *storeIndex) getKeysFromIndex(indexName string, obj interface{}) (sets.Set[string], error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	indexedValues, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}
	index := i.indices[indexName]

	var storeKeySet sets.Set[string]
	if len(indexedValues) == 1 {
		// 多数情况下恰好只有一个匹配值。
		// 对最常见的路径做优化——这里无需去重。
		storeKeySet = index[indexedValues[0]]
	} else {
		// 需要对返回列表去重。
		// 由于允许多个 key，这种情况是可能发生的。
		storeKeySet = sets.Set[string]{}
		for _, indexedValue := range indexedValues {
			for key := range index[indexedValue] {
				storeKeySet.Insert(key)
			}
		}
	}

	return storeKeySet, nil
}

// getKeysByIndex 返回某个索引值下登记的全部 key。
func (i *storeIndex) getKeysByIndex(indexName, indexedValue string) (sets.Set[string], error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	index := i.indices[indexName]
	return index[indexedValue], nil
}

// getIndexValues 返回索引中出现过的所有索引值（用于 ListIndexFuncValues）。
func (i *storeIndex) getIndexValues(indexName string) []string {
	index := i.indices[indexName]
	names := make([]string, 0, len(index))
	for key := range index {
		names = append(names, key)
	}
	return names
}

// addIndexers 注册新的索引函数；索引名冲突时整体失败，不部分生效。
func (i *storeIndex) addIndexers(newIndexers Indexers) error {
	oldKeys := sets.KeySet(i.indexers)
	newKeys := sets.KeySet(newIndexers)

	if oldKeys.HasAny(sets.List(newKeys)...) {
		return fmt.Errorf("indexer conflict: %v", oldKeys.Intersection(newKeys))
	}

	for k, v := range newIndexers {
		i.indexers[k] = v
	}
	return nil
}

// updateSingleIndex 修改对象在指定索引中的位置：
// - 创建时只需提供 newObj
// - 更新时必须同时提供 oldObj 和 newObj
// - 删除时只需提供 oldObj
// 必须在已持有缓存锁的函数中调用 updateSingleIndex。
func (i *storeIndex) updateSingleIndex(name string, oldObj interface{}, newObj interface{}, key string) {
	var oldIndexValues, indexValues []string
	indexFunc, ok := i.indexers[name]
	if !ok {
		// 本不该发生。调用方负责确保其存在，并应在持锁时调用以避免竞态。
		panic(fmt.Errorf("indexer %q does not exist", name))
	}
	if oldObj != nil {
		var err error
		oldIndexValues, err = indexFunc(oldObj)
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}
	} else {
		oldIndexValues = oldIndexValues[:0]
	}

	if newObj != nil {
		var err error
		indexValues, err = indexFunc(newObj)
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}
	} else {
		indexValues = indexValues[:0]
	}

	idx := i.indices[name]
	if idx == nil {
		idx = index{}
		i.indices[name] = idx
	}

	if len(indexValues) == 1 && len(oldIndexValues) == 1 && indexValues[0] == oldIndexValues[0] {
		// 针对最常见情况做优化：indexFunc 返回单个值且该值未变化。
		// 绝大多数 Update 不改变索引值，此时无需触碰索引结构。
		return
	}

	for _, value := range oldIndexValues {
		i.deleteKeyFromIndex(key, value, idx)
	}
	for _, value := range indexValues {
		i.addKeyToIndex(key, value, idx)
	}
}

// updateIndices 修改对象在受管索引中的位置：
// - 创建时只需提供 newObj
// - 更新时必须同时提供 oldObj 和 newObj
// - 删除时只需提供 oldObj
// 必须在已持有缓存锁的函数中调用 updateIndices。
func (i *storeIndex) updateIndices(oldObj interface{}, newObj interface{}, key string) {
	for name := range i.indexers {
		i.updateSingleIndex(name, oldObj, newObj, key)
	}
}

// addKeyToIndex 把 key 加入索引值对应的集合，集合不存在时懒创建。
func (i *storeIndex) addKeyToIndex(key, indexValue string, index index) {
	set := index[indexValue]
	if set == nil {
		set = sets.Set[string]{}
		index[indexValue] = set
	}
	set.Insert(key)
}

// deleteKeyFromIndex 从索引集合中移除 key；集合为空时连同索引值一起删除，防止内存累积。
func (i *storeIndex) deleteKeyFromIndex(key, indexValue string, index index) {
	set := index[indexValue]
	if set == nil {
		return
	}
	set.Delete(key)
	// 若集合清空时不将其删除，高基数、短生命周期资源的索引会因
	// 遗留的空集合而随时间增长内存占用。见 `kubernetes/kubernetes/issues/84959`。
	if len(set) == 0 {
		delete(index, indexValue)
	}
}

// threadSafeMap 实现 ThreadSafeStore。
type threadSafeMap struct {
	// lock 保护 items 与 index：所有 *_locked 方法都假定调用方已持锁。
	lock  sync.RWMutex
	items map[string]interface{}

	// index 实现索引功能。
	index *storeIndex
	rv    string

	// metrics 用于暴露 store 的指标，必须非 nil。
	// 未提供时会使用 no-op 实现。
	metrics *storeMetrics
}

// Transaction 在一把锁内依次应用多个写操作，最后统一更新 resourceVersion。
func (c *threadSafeMap) Transaction(txns ...ThreadSafeStoreTransaction) {
	if len(txns) == 0 {
		return
	}
	// 以最后一个对象的 RV 作为事务结束后的 store RV。
	finalObj := txns[len(txns)-1].Object
	rv, rvErr := rvFromObject(finalObj)
	rvInt, parseErr := parseRVForMetricsWithTruncation(rv)
	c.lock.Lock()
	defer c.lock.Unlock()
	trace := utiltrace.New("ThreadSafeMap Transaction Process",
		utiltrace.Field{Key: "Size", Value: len(txns)},
		utiltrace.Field{Key: "Reason", Value: "Slow batch process due to too many items"})
	defer trace.LogIfLong(min(500*time.Millisecond*time.Duration(len(txns)), 5*time.Second))

	for _, txn := range txns {
		switch txn.Type {
		case TransactionTypeAdd:
			c.addLocked(txn.Key, txn.Object)
		case TransactionTypeUpdate:
			c.updateLocked(txn.Key, txn.Object)
		case TransactionTypeDelete:
			c.deleteLocked(txn.Key)
		}
	}
	if rvErr == nil {
		c.rv = rv
		if parseErr == nil {
			c.metrics.storeResourceVersion.Set(float64(rvInt))
		}
	}
}

// Add 等价于 Update：若 key 已存在会被覆盖，同时更新索引。
func (c *threadSafeMap) Add(key string, obj interface{}) {
	c.Update(key, obj)
}

// addLocked 是持锁版本的 Add。
func (c *threadSafeMap) addLocked(key string, obj interface{}) {
	c.updateLocked(key, obj)
}

// Update 写入对象并推进 store 的 resourceVersion（解析失败时忽略指标）。
func (c *threadSafeMap) Update(key string, obj interface{}) {
	rv, rvErr := rvFromObject(obj)
	rvInt, parseErr := parseRVForMetricsWithTruncation(rv)
	c.lock.Lock()
	defer c.lock.Unlock()
	c.updateLocked(key, obj)
	if rvErr == nil {
		c.rv = rv
		if parseErr == nil {
			c.metrics.storeResourceVersion.Set(float64(rvInt))
		}
	}
}

// updateLocked 替换 map 条目并增量更新索引；调用方必须持锁。
func (c *threadSafeMap) updateLocked(key string, obj interface{}) {
	oldObject := c.items[key]
	c.items[key] = obj
	c.index.updateIndices(oldObject, obj, key)
}

// Delete 删除 key，但不像 DeleteWithObject 那样能推进 RV。
func (c *threadSafeMap) Delete(key string) {
	c.DeleteWithObject(key, nil)
}

// DeleteWithObject 删除 key；obj 非 nil 时用它携带的 RV 更新 store。
func (c *threadSafeMap) DeleteWithObject(key string, obj interface{}) {
	var rv string
	var rvInt int64
	var rvErr, parseErr error
	if obj != nil {
		rv, rvErr = rvFromObject(obj)
		rvInt, parseErr = parseRVForMetricsWithTruncation(rv)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.deleteLocked(key)
	if obj != nil && rvErr == nil {
		c.rv = rv
		if parseErr == nil {
			c.metrics.storeResourceVersion.Set(float64(rvInt))
		}
	}
}

// deleteLocked 从 map 和所有索引中移除 key；不存在时为空操作。
func (c *threadSafeMap) deleteLocked(key string) {
	if obj, exists := c.items[key]; exists {
		c.index.updateIndices(obj, nil, key)
		delete(c.items, key)
	}
}

// Get 返回对象本身（不是副本），调用方不得修改。
func (c *threadSafeMap) Get(key string) (item interface{}, exists bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	item, exists = c.items[key]
	return item, exists
}

// List 返回所有对象；顺序不确定，且元素与缓存共享，调用方不得修改。
func (c *threadSafeMap) List() []interface{} {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]interface{}, 0, len(c.items))
	for _, item := range c.items {
		list = append(list, item)
	}
	return list
}

// ListKeys 返回 threadSafeMap 中当前所有对象的 key 列表。
func (c *threadSafeMap) ListKeys() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]string, 0, len(c.items))
	for key := range c.items {
		list = append(list, key)
	}
	return list
}

// Replace 整体替换 items，并用给定的 resourceVersion 覆盖 store 的 RV。
// 索引无法增量推导，因此先 reset 再全量重建。
func (c *threadSafeMap) Replace(items map[string]interface{}, resourceVersion string) {
	var rvInt int64
	var parseErr error
	if resourceVersion != "" {
		rvInt, parseErr = parseRVForMetricsWithTruncation(resourceVersion)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.items = items
	c.rv = resourceVersion
	if parseErr == nil {
		c.metrics.storeResourceVersion.Set(float64(rvInt))
	}
	// 重建所有索引
	c.index.reset()
	for key, item := range c.items {
		c.index.updateIndices(nil, item, key)
	}
}

// rvFromObject 通过 meta.Accessor 读取 resourceVersion；非 API 对象会返回错误。
func rvFromObject(obj interface{}) (rv string, err error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}
	rv = meta.GetResourceVersion()
	return rv, nil
}

// Index 返回在索引函数上与给定对象匹配的条目列表。
// 只要把所有条目视为不可变，Index 就是线程安全的。
func (c *threadSafeMap) Index(indexName string, obj interface{}) ([]interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	storeKeySet, err := c.index.getKeysFromIndex(indexName, obj)
	if err != nil {
		return nil, err
	}

	list := make([]interface{}, 0, storeKeySet.Len())
	for storeKey := range storeKeySet {
		list = append(list, c.items[storeKey])
	}
	return list, nil
}

// LastStoreSyncResourceVersion 返回 store 所见过的最新 resource version。
func (c *threadSafeMap) LastStoreSyncResourceVersion() string {
	// AtomicFIFO 特性门控未开启时无法返回 resource version。
	// 关闭 AtomicFIFO 时 store 的 RV 更新路径不保证完整，返回空串以避免误用。
	if !clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.AtomicFIFO) {
		return ""
	}
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.rv
}

// Bookmark 设置 store 所见过的最新 resource version。
// 只记录 RV，不触发任何事件或索引变化。
func (c *threadSafeMap) Bookmark(rv string) {
	var rvInt int64
	var parseErr error
	if rv != "" {
		rvInt, parseErr = parseRVForMetricsWithTruncation(rv)
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	c.rv = rv
	if parseErr == nil {
		c.metrics.storeResourceVersion.Set(float64(rvInt))
	}
}

// ByIndex 返回在给定索引中其索引值包含指定索引值的条目列表。
// 返回顺序由 map 迭代决定，不保证稳定。
func (c *threadSafeMap) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	set, err := c.index.getKeysByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	list := make([]interface{}, 0, set.Len())
	for key := range set {
		list = append(list, c.items[key])
	}

	return list, nil
}

// IndexKeys 返回在给定索引中其索引值包含指定索引值的对象的 Store key 列表。
// 只要把所有条目视为不可变，IndexKeys 就是线程安全的。
func (c *threadSafeMap) IndexKeys(indexName, indexedValue string) ([]string, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	// 索引可能不存在：getKeysByIndex 对未知索引名返回错误。
	set, err := c.index.getKeysByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return sets.List(set), nil
}

// ListIndexFuncValues 返回索引中出现的所有索引值。
func (c *threadSafeMap) ListIndexFuncValues(indexName string) []string {
	c.lock.RLock()
	defer c.lock.RUnlock()

	return c.index.getIndexValues(indexName)
}

// GetIndexers 返回索引函数映射本身；调用方只应读取，不应修改。
func (c *threadSafeMap) GetIndexers() Indexers {
	return c.index.indexers
}

// AddIndexers 注册新索引并为已有对象补建索引。
func (c *threadSafeMap) AddIndexers(newIndexers Indexers) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if err := c.index.addIndexers(newIndexers); err != nil {
		return err
	}

	// 若已有对象，为它们补建索引
	for key, item := range c.items {
		for name := range newIndexers {
			c.index.updateSingleIndex(name, nil, item, key)
		}
	}

	return nil
}

// Resync 是空操作：store 的内容已由 informer 维护，无需重新计算。
func (c *threadSafeMap) Resync() error {
	// 无需任何操作
	return nil
}

// NewThreadSafeStore 构造线程安全的索引存储；未显式注入指标时使用 no-op 实现。
func NewThreadSafeStore(indexers Indexers, indices Indices, opts ...ThreadSafeStoreOption) ThreadSafeStore {
	store := &threadSafeMap{
		items: map[string]interface{}{},
		index: &storeIndex{
			indexers: indexers,
			indices:  indices,
		},
	}
	for _, opt := range opts {
		opt(store)
	}
	if store.metrics == nil {
		store.metrics = newStoreMetrics(InformerNameAndResource{}, noopInformerMetricsProvider{})
	}
	return store
}

// parseRVForMetricsWithTruncation 把 RV 转成可安全放入 float64 的整数。
// etcd RV 可能超出 2^53，直接转换会丢精度，因此只保留末 15 位。
func parseRVForMetricsWithTruncation(rv string) (int64, error) {
	if rv == "" {
		return 0, nil
	}
	// 截断为末 15 位，确保指标值始终小于 2^53-1，
	// 避免 float64 精度丢失。
	if len(rv) > 15 {
		rv = rv[len(rv)-15:]
	}
	return strconv.ParseInt(rv, 10, 64)
}
