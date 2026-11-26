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

package store

import (
	"fmt"
	"strings"
	"sync"

	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/third_party/forked/golang/btree"
)

// newThreadedBtreeStoreIndexer 返回为 cacher 提供存储的结构，它在两个数据结构上统一加锁：
// * 基于 btree 的存储，用于高效执行按前缀的 LIST 操作
// * 基于 map 的索引器，用于按索引取值
// 分开维护两者是为将来可对这两个存储分别做快照预留空间；
// 利用 btree 快照廉价的特点——不修改数据时快照无需加锁。
// 所有公开方法统一由一把 RWMutex 保护，
// 保证两个结构在任意观察点保持一致。
func newThreadedBtreeStoreIndexer(indexers cache.Indexers, degree int) *threadedStoreIndexer {
	return &threadedStoreIndexer{
		store:   newBtreeStore(degree),
		indexer: newIndexer(indexers),
	}
}

// threadedStoreIndexer 用一把读写锁同时保护 btree 存储与索引器。
// 两结构必须原子更新，否则并发读可能看到「对象已加入 btree 但索引未更新」
// 的中间状态，导致按索引查询遗漏对象。
type threadedStoreIndexer struct {
	lock    sync.RWMutex
	store   btreeStore
	indexer indexer
}

var _ Snapshot = (*threadedStoreIndexer)(nil)

// Count 返回前缀下（含 continueKey 起）的对象数。
func (si *threadedStoreIndexer) Count(prefix, continueKey string) (count int) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.store.Count(prefix, continueKey)
}

// Clone 返回 btree 的惰性快照。
// btree 的 Clone 是写时复制，代价低；但底层实现要求调用时不并发修改，
// 因此这里必须持有写锁而非读锁——这也是注释「Clone 不应并发调用」的由来。
func (si *threadedStoreIndexer) Clone() Snapshot {
	// Clone 不应被并发调用。
	si.lock.Lock()
	defer si.lock.Unlock()
	return si.store.Clone()
}

// Add 与 Update 语义相同：btree 按 key 替换，重复添加等价于更新。
func (si *threadedStoreIndexer) Add(obj interface{}) error {
	return si.addOrUpdate(obj)
}

// Update 见 Add。
func (si *threadedStoreIndexer) Update(obj interface{}) error {
	return si.addOrUpdate(obj)
}

// addOrUpdate 先更新 btree 取得旧值，再用旧值/新值同步更新索引。
// 索引更新需要旧值才能正确移除旧索引项；先 btree 后索引的顺序
// 保证在锁内两者最终一致。
func (si *threadedStoreIndexer) addOrUpdate(obj interface{}) error {
	if obj == nil {
		return fmt.Errorf("obj cannot be nil")
	}
	newElem, ok := obj.(*Element)
	if !ok {
		return fmt.Errorf("obj not a storeElement: %#v", obj)
	}
	si.lock.Lock()
	defer si.lock.Unlock()
	oldElem := si.store.addOrUpdateElem(newElem)
	return si.indexer.updateElem(newElem.Key, oldElem, newElem)
}

// Delete 从 btree 与索引中移除对象；对象本不存在时视为成功（幂等）。
func (si *threadedStoreIndexer) Delete(obj interface{}) error {
	storeElem, ok := obj.(*Element)
	if !ok {
		return fmt.Errorf("obj not a storeElement: %#v", obj)
	}
	si.lock.Lock()
	defer si.lock.Unlock()
	oldObj, existed := si.store.deleteElem(storeElem)
	if !existed {
		return nil
	}
	return si.indexer.updateElem(storeElem.Key, oldObj, nil)
}

// List 返回全部对象，顺序按 key 升序。
func (si *threadedStoreIndexer) List() []interface{} {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.store.List()
}

// OrderedListPrefix 返回前缀匹配的对象，从 continueKey 开始，用于分页。
func (si *threadedStoreIndexer) OrderedListPrefix(prefix, continueKey string) ([]interface{}, error) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.store.OrderedListPrefix(prefix, continueKey)
}

// ListKeys 返回全部 key，按升序。
func (si *threadedStoreIndexer) ListKeys() []string {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.store.ListKeys()
}

// Get 按元素（key）查找。
func (si *threadedStoreIndexer) Get(obj interface{}) (item interface{}, exists bool, err error) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.store.Get(obj)
}

// GetByKey 按 key 字符串查找。
func (si *threadedStoreIndexer) GetByKey(key string) (item interface{}, exists bool, err error) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.store.GetByKey(key)
}

// Replace 用给定对象整体替换内容。
// 必须持写锁并在同一临界区内同时替换 btree 与索引，否则并发读会看到
// 新旧混合的视图。resourceVersion 在此实现中被忽略——快照版本由 Snapshotter 管理。
func (si *threadedStoreIndexer) Replace(objs []interface{}, resourceVersion string) error {
	si.lock.Lock()
	defer si.lock.Unlock()
	err := si.store.Replace(objs, resourceVersion)
	if err != nil {
		return err
	}
	return si.indexer.Replace(objs, resourceVersion)
}

// ByIndex 按索引名与值查询；不存在的索引返回错误而非空列表，便于发现配置错误。
func (si *threadedStoreIndexer) ByIndex(indexName, indexValue string) ([]interface{}, error) {
	si.lock.RLock()
	defer si.lock.RUnlock()
	return si.indexer.ByIndex(indexName, indexValue)
}

// newBtreeStore 创建以 Element.Key 为比较键的 btree。
func newBtreeStore(degree int) btreeStore {
	return btreeStore{
		tree: btree.New(degree, func(a, b *Element) bool {
			return a.Key < b.Key
		}),
	}
}

// btreeStore 是未加锁的 btree 包装，仅由 threadedStoreIndexer 在持锁时调用。
type btreeStore struct {
	tree *btree.BTree[*Element]
}

// Clone 不应并发调用。
// Ref: https://github.com/kubernetes/kubernetes/blob/4a8f617f3ca/vendor/k8s.io/utils/third_party/forked/golang/btree/btree.go#L586-L588
// 惰性克隆，后续对原树的写会复制受影响路径，因此快照读无需加锁且与写互不影响。
func (s *btreeStore) Clone() Snapshot {
	return &btreeStore{
		tree: s.tree.Clone(),
	}
}

// Add 将元素插入树；同 key 覆盖旧值。
func (s *btreeStore) Add(obj interface{}) error {
	if obj == nil {
		return fmt.Errorf("obj cannot be nil")
	}
	storeElem, ok := obj.(*Element)
	if !ok {
		return fmt.Errorf("obj not a storeElement: %#v", obj)
	}
	s.addOrUpdateElem(storeElem)
	return nil
}

// Update 与 Add 等价（按 key 替换）。
func (s *btreeStore) Update(obj interface{}) error {
	if obj == nil {
		return fmt.Errorf("obj cannot be nil")
	}
	storeElem, ok := obj.(*Element)
	if !ok {
		return fmt.Errorf("obj not a storeElement: %#v", obj)
	}
	s.addOrUpdateElem(storeElem)
	return nil
}

// Delete 按元素 key 删除。
func (s *btreeStore) Delete(obj interface{}) error {
	if obj == nil {
		return fmt.Errorf("obj cannot be nil")
	}
	storeElem, ok := obj.(*Element)
	if !ok {
		return fmt.Errorf("obj not a storeElement: %#v", obj)
	}
	s.deleteElem(storeElem)
	return nil
}

// deleteElem 删除并返回被删除的旧元素，调用方用它更新索引。
func (s *btreeStore) deleteElem(storeElem *Element) (*Element, bool) {
	return s.tree.Delete(storeElem)
}

// List 按 key 升序遍历并收集全部元素。
func (s *btreeStore) List() []interface{} {
	items := make([]interface{}, 0, s.tree.Len())
	s.tree.Ascend(func(item *Element) bool {
		items = append(items, item)
		return true
	})
	return items
}

// ListKeys 按 key 升序收集全部键。
func (s *btreeStore) ListKeys() []string {
	items := make([]string, 0, s.tree.Len())
	s.tree.Ascend(func(item *Element) bool {
		items = append(items, item.Key)
		return true
	})
	return items
}

// Get 按元素 key 查找；非 Element 入参返回错误。
func (s *btreeStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	storeElem, ok := obj.(*Element)
	if !ok {
		return nil, false, fmt.Errorf("obj is not a storeElement")
	}
	item, exists = s.tree.Get(storeElem)
	return item, exists, nil
}

// GetByKey 以仅含 key 的临时元素作为查询条件。
func (s *btreeStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	return s.getByKey(key)
}

// Replace 清空后重新插入；Clear(false) 表示保留已分配的节点以复用内存。
func (s *btreeStore) Replace(objs []interface{}, _ string) error {
	s.tree.Clear(false)
	for _, obj := range objs {
		storeElem, ok := obj.(*Element)
		if !ok {
			return fmt.Errorf("obj not a storeElement: %#v", obj)
		}
		s.addOrUpdateElem(storeElem)
	}
	return nil
}

// addOrUpdateElem 假定调用时已持有锁，用于 Add 和 Update 操作。
// ReplaceOrInsert 返回旧值（若存在），供索引器计算差异。
func (s *btreeStore) addOrUpdateElem(storeElem *Element) *Element {
	oldObj, _ := s.tree.ReplaceOrInsert(storeElem)
	return oldObj
}

// getByKey 构造只带 key 的探针元素进行查找。
func (s *btreeStore) getByKey(key string) (item interface{}, exists bool, err error) {
	keyElement := &Element{Key: key}
	item, exists = s.tree.Get(keyElement)
	return item, exists, nil
}

// OrderedListPrefix 从 continueKey（空则从 prefix）开始升序遍历，
// 遇到第一个前缀不匹配的键即停止——因为 btree 有序，后续键必然也不匹配。
func (s *btreeStore) OrderedListPrefix(prefix, continueKey string) ([]interface{}, error) {
	if continueKey == "" {
		continueKey = prefix
	}
	var result []interface{}
	s.tree.AscendGreaterOrEqual(&Element{Key: continueKey}, func(item *Element) bool {
		if !strings.HasPrefix(item.Key, prefix) {
			return false
		}
		result = append(result, item)
		return true
	})
	return result, nil
}

// Count 与 OrderedListPrefix 遍历方式相同，但只计数不收集，避免分配。
func (s *btreeStore) Count(prefix, continueKey string) (count int) {
	if continueKey == "" {
		continueKey = prefix
	}
	s.tree.AscendGreaterOrEqual(&Element{Key: continueKey}, func(item *Element) bool {
		if !strings.HasPrefix(item.Key, prefix) {
			return false
		}
		count++
		return true
	})
	return count
}

// newIndexer 返回一个与 client-go/tools/cache 中 storeIndex 类似的索引器。
// TODO: 与 client-go/cache 包中的 indexer 代码统一。
// 主要区别在于存储值的类型及其可变性：
// * client-go 中的 Indexer 存储对象 key，且 key 不可变。
// * cacher 中的 Indexer 存储整个对象，且对象可变。
// client-go 的 Indexer 用 key 是因为它配合 map[key]value 使用，可快速取值，
// 而 cacher 中使用 btree 会带来额外开销。
// 存储值可变性的差异被用于优化 client-go Indexer 的部分操作。
// 三级 map 的结构为 索引名 → 索引值 → key → 元素，
// 这样 ByIndex 能 O(1) 定位到目标集合，而删除单个元素时也只需操作对应集合。
func newIndexer(indexers cache.Indexers) indexer {
	return indexer{
		indices:  map[string]map[string]map[string]*Element{},
		indexers: indexers,
	}
}

// indexer 维护多索引的内存映射；所有访问由外层锁保护，自身不加锁。
type indexer struct {
	indices  map[string]map[string]map[string]*Element
	indexers cache.Indexers
}

// ByIndex 返回索引值对应的全部元素。
func (i *indexer) ByIndex(indexName, indexValue string) ([]interface{}, error) {
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}
	index := i.indices[indexName]
	set := index[indexValue]
	list := make([]interface{}, 0, len(set))
	for _, obj := range set {
		list = append(list, obj)
	}
	return list, nil
}

// Replace 重建全部索引。
// 直接丢弃旧 map 而非逐项删除，避免大对象集合下逐个清理的开销；
// 旧 map 由 GC 回收。
func (i *indexer) Replace(objs []interface{}, resourceVersion string) error {
	i.indices = map[string]map[string]map[string]*Element{}
	for _, obj := range objs {
		storeElem, ok := obj.(*Element)
		if !ok {
			return fmt.Errorf("obj not a storeElement: %#v", obj)
		}
		err := i.updateElem(storeElem.Key, nil, storeElem)
		if err != nil {
			return err
		}
	}
	return nil
}

// updateElem 根据旧值/新值重新计算每个索引的归属并增删。
// oldObj/newObj 为 nil 分别表示「新增」与「删除」。
// 常见快路径是「单值且未变化」——此时无需先删后加，直接 add 覆盖即可，
// 避免无谓的 map 操作。索引函数返回错误时立即中止，防止部分更新造成不一致。
func (i *indexer) updateElem(key string, oldObj, newObj *Element) (err error) {
	var oldIndexValues, indexValues []string
	for name, indexFunc := range i.indexers {
		if oldObj != nil {
			oldIndexValues, err = indexFunc(oldObj)
		} else {
			oldIndexValues = oldIndexValues[:0]
		}
		if err != nil {
			return fmt.Errorf("unable to calculate an index entry for key %q on index %q: %w", key, name, err)
		}
		if newObj != nil {
			indexValues, err = indexFunc(newObj)
		} else {
			indexValues = indexValues[:0]
		}
		if err != nil {
			return fmt.Errorf("unable to calculate an index entry for key %q on index %q: %w", key, name, err)
		}
		index := i.indices[name]
		if index == nil {
			index = map[string]map[string]*Element{}
			i.indices[name] = index
		}
		if len(indexValues) == 1 && len(oldIndexValues) == 1 && indexValues[0] == oldIndexValues[0] {
			// 针对最常见情况优化：indexFunc 返回单个未改变的值。
			i.add(key, indexValues[0], newObj, index)
			continue
		}
		for _, value := range oldIndexValues {
			i.delete(key, value, index)
		}
		for _, value := range indexValues {
			i.add(key, value, newObj, index)
		}
	}
	return nil
}

// add 把元素放入指定索引值的集合，必要时创建集合。
func (i *indexer) add(key, value string, obj *Element, index map[string]map[string]*Element) {
	set := index[value]
	if set == nil {
		set = map[string]*Element{}
		index[value] = set
	}
	set[key] = obj
}

// delete 从集合中移除 key，并在集合为空时删除整个索引值条目。
func (i *indexer) delete(key, value string, index map[string]map[string]*Element) {
	set := index[value]
	if set == nil {
		return
	}
	delete(set, key)
	// 若不在集合为空时删除它，高基数、短生命周期资源的索引会残留大量
	// 未使用的空集合，导致内存随时间增长。见 `kubernetes/kubernetes/issues/84959`。
	if len(set) == 0 {
		delete(index, value)
	}
}

// NewSnapshotter 返回一个 storeSnapshotter，用于存储快照以支持精确
// resourceVersion（RV）的读请求与分页。
//
// 快照通过调用 orderedLister 的 Clone 方法创建，得益于 B-tree 的使用，
// 这应当快速且高效：B-tree 可以惰性复制树结构，将开销降到最低。
//
// 假设 watch cache 能观察所有事件并在每个事件后快照缓存，
// 对特定 resourceVersion 的请求可以通过取出
// RV 小于等于请求 RV 的最大快照来服务。要高效地检索快照，
// 需要一个有序数据结构，例如树。
//
// 初始实现使用 B-tree 以获得以下性能特征（n 为存储的快照数量）：
//   - `Add`：添加新快照。
//     复杂度：O(log n)。
//     cache 每观察到一个 watch 事件执行一次。
//   - `GetLessOrEqual`：取出 RV 小于等于请求 RV 的最大快照。
//     复杂度：O(log n)。
//     每个 match=Exact 或带 continuation 的 LIST 请求执行一次。
//   - `RemoveLess`：清理超出 watch 历史窗口的快照。
//     复杂度：O(k log n)，k 为要删除的快照数；若 watch 容量未缩减通常只有 1 个。
//     缓存已满时，每观察到一个 watch 事件执行一次。
//   - `Reset`：清理所有快照。
//     复杂度：O(1)。
//     watch cache 重新初始化时执行。
//
// 还可以利用「添加只会抬高最大 RV、删除只会抬高最小 RV」的性质进一步优化。
// 例如，在 (RV, snapshot) 循环缓冲上做二分查找可以减少分配次数并改善
// 删除复杂度。但该方案更复杂，留待将来实现。
//
// TODO: 重写为环形缓冲区。
// 以 RV 为键的 btree 让「找到 ≤ 请求 RV 的最新快照」成为一次有序查找，
// 这是支持 resourceVersion 精确读与分页续读的基础。
func NewSnapshotter() Snapshotter {
	s := &storeSnapshotter{
		snapshots: btree.New(btreeDegree, func(a, b rvSnapshot) bool {
			return a.resourceVersion < b.resourceVersion
		}),
	}
	return s
}

var _ Snapshotter = (*storeSnapshotter)(nil)

// Snapshotter 按 RV 管理快照集合。
type Snapshotter interface {
	Reset()
	GetLessOrEqual(rv uint64) (Snapshot, bool)
	Latest() (Snapshot, bool)
	Add(rv uint64, indexer Indexer)
	RemoveLess(rv uint64)
	Len() int
}

// storeSnapshotter 用 RWMutex 保护 btree；读路径（GetLessOrEqual/Latest/Len）
// 并发无阻塞，写路径串行。
type storeSnapshotter struct {
	mux       sync.RWMutex
	snapshots *btree.BTree[rvSnapshot]
}

// rvSnapshot 把资源版本与对应快照绑定；RV 相同则视为同一快照（ReplaceOrInsert 覆盖）。
type rvSnapshot struct {
	resourceVersion uint64
	snapshot        Snapshot
}

// Reset 清空全部快照，保留已分配的树节点。
func (s *storeSnapshotter) Reset() {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.snapshots.Clear(false)
}

// GetLessOrEqual 返回 RV ≤ 给定值的最新快照。
// DescendLessOrEqual 从大到小遍历，回调返回 false 即停止，
// 因此只访问第一个（也是最大的）匹配项。
func (s *storeSnapshotter) GetLessOrEqual(rv uint64) (Snapshot, bool) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	var result *rvSnapshot
	s.snapshots.DescendLessOrEqual(rvSnapshot{resourceVersion: rv}, func(rvs rvSnapshot) bool {
		result = &rvs
		return false
	})
	if result == nil {
		return nil, false
	}
	return result.snapshot, true
}

// Latest 返回 RV 最大的快照。
func (s *storeSnapshotter) Latest() (Snapshot, bool) {
	s.mux.RLock()
	defer s.mux.RUnlock()

	max, ok := s.snapshots.Max()
	if !ok {
		return nil, false
	}
	return max.snapshot, true
}

// Add 克隆索引器并挂到给定 RV 下。
// Clone 在写锁内完成，保证快照与当前状态一致；
// btree 的惰性克隆让该操作在后续写入前几乎零成本。
func (s *storeSnapshotter) Add(rv uint64, indexer Indexer) {
	s.mux.Lock()
	defer s.mux.Unlock()
	s.snapshots.ReplaceOrInsert(rvSnapshot{resourceVersion: rv, snapshot: indexer.Clone()})
}

// RemoveLess 删除所有 RV 小于给定值的快照，即清理超出 watch 历史窗口的部分。
// 循环删除最小项，遇到第一个 ≥ rv 的即停止；由于有序，
// 至多删除 k 个（通常为 1）。
func (s *storeSnapshotter) RemoveLess(rv uint64) {
	s.mux.Lock()
	defer s.mux.Unlock()
	for s.snapshots.Len() > 0 {
		oldest, ok := s.snapshots.Min()
		if !ok {
			break
		}
		if rv <= oldest.resourceVersion {
			break
		}
		s.snapshots.DeleteMin()
	}
}

// Len 返回当前快照数量。
func (s *storeSnapshotter) Len() int {
	s.mux.RLock()
	defer s.mux.RUnlock()

	return s.snapshots.Len()
}
