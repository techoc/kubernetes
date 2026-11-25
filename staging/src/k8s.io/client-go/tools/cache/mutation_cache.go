/*
Copyright 2017 The Kubernetes Authors.

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

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilcache "k8s.io/apimachinery/pkg/util/cache"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
)

// MutationCache 会把更新操作的结果存入一个 LRU 缓存，从而为请求的对象提供更
// 新鲜的视图；比较时需要对 resourceVersion 进行解读。实现必须是线程安全的。
// OnAddOrUpdate 和 OnDelete 应当由 informer 事件处理器调用，以提高缓存的准确度。
//
// 读路径总是返回底层 store 与 mutation 缓存中较新的版本，因此调用方在自己
// 刚写完对象后能立即读到新值，而不必等待 watch 事件回灌。MutationCache 只是
// “更可能新鲜”的视图，不提供强一致保证。
type MutationCache interface {
	// GetByKey 返回指定 key 的最新视图。
	GetByKey(key string) (interface{}, bool, error)
	// ByIndex 按索引查询最新视图；未配置 Indexer 时返回错误。
	ByIndex(indexName, indexKey string) ([]interface{}, error)
	// Mutation 记录一次本地写入，供后续读取优先返回。
	Mutation(interface{})
	// OnAddOrUpdate 由 informer 的 add/update handler 调用，清理已过时的条目。
	OnAddOrUpdate(obj runtime.Object)
	// OnDelete 由 informer 的 delete handler 调用，写入 tombstone。
	OnDelete(obj runtime.Object)

	// 该接口不打算供他处实现。将其标记为 internal 可以在不触发 apidiff 的前提下
	// 进行未来的修改。
	//
	// internal 通过未导出方法封闭实现集合；由于是未导出方法，包外无法实现该接口。
	internal()
}

// ResourceVersionComparator 用于比较对象版本。
//
// 返回值语义与 strings.Compare 一致：负数表示 lhs 更旧，0 表示相同，正数表示更新。
type ResourceVersionComparator interface {
	CompareResourceVersion(lhs, rhs runtime.Object) int
}

// MutationCacheOptions 配置 NewIntegerResourceVersionMutationCacheWithOptions 返回的 MutationCache。
type MutationCacheOptions struct {
	// Indexer 供 ByIndex 查找对象时使用。可以为 nil，此时 ByIndex 返回错误。
	Indexer Indexer

	// TTL 控制条目在 mutation 缓存中保留多久后被移除。
	// 0 表示默认的 5 分钟。
	TTL time.Duration

	// IncludeAdds 让缓存在对象不存在于底层 store 时依然返回它。只有当缓存的使用方
	// 能够容忍“mutation 与删除在极短时间内相继发生”时条目仍在缓存中保留最长 TTL
	// 的情况时，这样才是安全的。
	IncludeAdds bool

	// MaxCacheSize 限制跟踪的 mutation 对象数量；达到上限后按 LRU 淘汰。
	// 0 表示默认的 100。
	MaxCacheSize int
}

// NewIntegerResourceVersionMutationCacheWithOptions 返回一个 MutationCache，它能处理
// resource version 满足以下条件的对象：
//
//   - 是整数
//   - 更新时递增
//   - 在同一命名空间的同一类资源之间可比较
//
// 多数后端都具备上述语义。indexer 可以为 nil。ttl 控制条目在 mutation 缓存中
// 保留多久后被移除。
//
// 若 includeAdds 为 true，即使对象不存在于底层 store，也会返回 mutation 缓存中的对象。
// 只有当缓存的使用方能够容忍“mutation 与删除在极短时间内相继发生”时条目仍在缓存中
// 保留最长 ttl 的情况时，这样才是安全的。
//
// 注意：这不仅适用于调用方新增的对象，也适用于其更新的对象。因为当对象在 apiserver 中
// 于更新之后被删除、底层 store 已将其移除时，MutationCache 仍可能持有一个更新后的对象。
// 为解决该问题，调用方可以在 informer 事件处理器中调用 OnAddOrUpdate 和 OnDelete。
//
// 这会把 store 观察到的所有变更告知 MutationCache，使其能够在本地新增或更新的对象
// 出现在 store 中后立即将其移除，从而避免“过期的更新对象”问题。
//
// 但这无法可靠地解决“过期的新增对象”问题：当新增后很快发生删除时，informer 可能
// 永远看不到该新增对象。调用方必须自行处理过期的新增对象，即在 TTL 到期后检查
// 它们是否仍然存在。
func NewIntegerResourceVersionMutationCacheWithOptions(logger klog.Logger, backingCache Store, options MutationCacheOptions) MutationCache {
	// 默认值与注释保持一致：100 条、5 分钟。
	var (
		maxCacheSize = 100
		ttl          = 5 * time.Minute
	)

	// 0 表示“使用默认值”，因此无法显式配置为 0。
	if options.MaxCacheSize != 0 {
		maxCacheSize = options.MaxCacheSize
	}
	if options.TTL != 0 {
		ttl = options.TTL
	}

	return &mutationCache{
		backingCache:  backingCache,
		indexer:       options.Indexer,
		mutationCache: utilcache.NewLRUExpireCache(maxCacheSize),
		comparator:    etcdObjectVersioner{},
		ttl:           ttl,
		includeAdds:   options.IncludeAdds,
		logger:        logger,
	}
}

// NewIntegerResourceVersionMutationCache 与 NewIntegerResourceVersionMutationCacheWithOptions 相同，只是缓存大小固定为 100。
func NewIntegerResourceVersionMutationCache(logger klog.Logger, backingCache Store, indexer Indexer, ttl time.Duration, includeAdds bool) MutationCache {
	return NewIntegerResourceVersionMutationCacheWithOptions(logger, backingCache, MutationCacheOptions{
		TTL:          ttl,
		Indexer:      indexer,
		IncludeAdds:  includeAdds,
		MaxCacheSize: 100,
	})
}

// mutationCache 不保证返回通过 Mutation 写入的值：它们可能被换出；并且无法区分
// “未观察到创建”与“创建后被删除”，因此当 key 在 backing cache 中缺失时，一律按缺失返回。
//
// lock 同时保护 backingCache 的读取与 mutationCache 的读写，保证“比较后删除”
// 这一序列相对其他写操作是原子的。
type mutationCache struct {
	logger        klog.Logger
	lock          sync.Mutex
	backingCache  Store
	indexer       Indexer
	mutationCache *utilcache.LRUExpireCache
	includeAdds   bool
	ttl           time.Duration

	comparator ResourceVersionComparator
}

// GetByKey 不保证返回 Mutation 设置的值：它可能被换出、可能比另一份副本更旧、
// backingCache 可能更新，或者你对同一个 key 写过两次。
// 返回的是在某次快照时刻有效的值，且总是 backingCache 与 mutationCache 中较新的那个。
func (c *mutationCache) GetByKey(key string) (interface{}, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	obj, exists, err := c.backingCache.GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		if !c.includeAdds {
			// 无法区分“未观察到创建”与“创建后被删除”，
			// 因此 key 缺失时一律按缺失返回
			return nil, false, nil
		}
		obj, exists = c.mutationCache.Get(key)
		if !exists {
			return nil, false, nil
		}
		// 不要返回 tombstone。
		if _, ok := obj.(*tombstone); ok {
			return nil, false, nil
		}
	}
	objRuntime, ok := obj.(runtime.Object)
	if !ok {
		return obj, true, nil
	}
	// 如果对象来自 store，这一步会移除已过时的 tombstone。
	//
	// 较新的对象绝不可能是 tombstone，因为 a) 若仅存在 tombstone
	// 时上面已提前返回，b) store 中的对象一定比 tombstone 更新
	// （tombstone 由更早的一次 store 删除产生）。
	return c.newerObject(key, objRuntime), true, nil
}

// ByIndex 返回与给定索引名和 index key 匹配的较新对象。
// 未提供 indexer 时返回错误。
func (c *mutationCache) ByIndex(name string, indexKey string) ([]interface{}, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	// 索引查询依赖 Indexer；没有配置时无法推断对象的归属，直接报错。
	if c.indexer == nil {
		return nil, fmt.Errorf("no indexer has been provided to the mutation cache")
	}
	keys, err := c.indexer.IndexKeys(name, indexKey)
	if err != nil {
		return nil, err
	}
	var items []interface{}
	keySet := sets.NewString()
	for _, key := range keys {
		keySet.Insert(key)
		obj, exists, err := c.indexer.GetByKey(key)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		if objRuntime, ok := obj.(runtime.Object); ok {
			items = append(items, c.newerObject(key, objRuntime))
		} else {
			items = append(items, obj)
		}
	}

	if c.includeAdds {
		// 索引中不存在的 key 可能对应尚未被 informer 观察到的本地新增，
		// 需要额外遍历 mutation 缓存，用同一索引函数判断其是否匹配。
		fn := c.indexer.GetIndexers()[name]
		// Keys() 按从旧到新的顺序返回，因此完整遍历不会改变 LRU 行为
		for _, key := range c.mutationCache.Keys() {
			updated, ok := c.mutationCache.Get(key)
			if !ok {
				continue
			}
			if keySet.Has(key.(string)) {
				continue
			}
			if _, ok := updated.(*tombstone); ok {
				continue
			}
			elements, err := fn(updated)
			if err != nil {
				c.logger.V(4).Info("Unable to calculate an index entry for mutation cache entry", "key", key, "err", err)
				continue
			}
			for _, inIndex := range elements {
				if inIndex != indexKey {
					continue
				}
				items = append(items, updated)
				break
			}
		}
	}

	return items, nil
}

// newerObject 检查 mutation 缓存中是否有更新的对象，若有则返回。若 mutated 对象
// 比 backing 对象更旧，则将其从缓存中移除。必须在持有锁时调用。
//
// 返回较新的一方，并顺带淘汰已过时的 mutation 条目，控制缓存规模。
func (c *mutationCache) newerObject(key string, backing runtime.Object) runtime.Object {
	mutatedObj, exists := c.mutationCache.Get(key)
	if !exists {
		return backing
	}
	// mutation 缓存中也可能存有 tombstone 等非 runtime.Object，忽略之。
	mutatedObjRuntime, ok := mutatedObj.(runtime.Object)
	if !ok {
		return backing
	}
	// 相等也视为 backing 更新：避免本地缓存长期压制 store 中的同版本对象。
	if c.comparator.CompareResourceVersion(backing, mutatedObjRuntime) >= 0 {
		c.mutationCache.Remove(key)
		return backing
	}
	return mutatedObjRuntime
}

// Mutation 向缓存添加一个变更；若它比 backingCache 中的副本更新，GetByKey 会返回它。
// 若在不同线程对同一对象调用两次 Mutation，只会保留其中一个，且不保证是哪一个。
// 这不影响正确性，因为 GetByKey 的“两个缓存中较晚的那个”保证仍然成立；
// 但你未必能得到你想要的版本。返回的对象只保证“在某时刻曾经有效”，
// 不保证“正是我想要的那个”。
func (c *mutationCache) Mutation(obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		// 这只是“锦上添花”，失败时不做任何特殊处理
		utilruntime.HandleErrorWithLogger(c.logger, err, "DeletionHandlingMetaNamespaceKeyFunc")
		return
	}

	if objRuntime, ok := obj.(runtime.Object); ok {
		if mutatedObj, exists := c.mutationCache.Get(key); exists {
			if mutatedObjRuntime, ok := mutatedObj.(runtime.Object); ok {
				// 已有更新的本地写入时丢弃本次；乱序写入不会让视图回退。
				cmp := c.comparator.CompareResourceVersion(objRuntime, mutatedObjRuntime)
				if cmp < 0 {
					return
				}
				if t, ok := mutatedObj.(*tombstone); ok {
					if cmp == 0 {
						// 已明确知道该对象实例被删除。
						// 不存储它，保留 tombstone。
						return
					}
					// UID 相同意味着同一实例的另一个版本；既然已删除，
					// 任何版本都不应复活，保留 tombstone。
					objMeta, err := meta.Accessor(obj)
					if err == nil && t.GetUID() == objMeta.GetUID() {
						// 该对象实例的其他某个版本已被确认删除。
						// 同样不存储。
						return
					}
				}
			}
		}
	}
	c.mutationCache.Add(key, obj, c.ttl)
}

// OnAddOrUpdate 可由调用方用来通知缓存：某个对象已加入 store 或已在 store 中更新。
// 若该对象与缓存中的对象同样新或更新，缓存对象会被移除，因为它已不再需要。
// 这有助于保持缓存小巧。
//
// 这是纯粹的优化：即使调用方从不调用，正确性也不受影响，只是缓存会更大。
func (c *mutationCache) OnAddOrUpdate(obj runtime.Object) {
	key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		// 这只是“锦上添花”，失败时不做任何特殊处理
		utilruntime.HandleErrorWithLogger(c.logger, err, "DeletionHandlingMetaNamespaceKeyFunc")
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if mutatedObj, exists := c.mutationCache.Get(key); exists {
		if mutatedObj, ok := mutatedObj.(runtime.Object); ok {
			// 这里无需比较 UID：不同实例之间的 resource version 也可以比较。
			// 若它更新或相等，则 mutation 缓存已过时。
			if c.comparator.CompareResourceVersion(obj, mutatedObj) >= 0 {
				c.mutationCache.Remove(key)
			}
		}
	}
}

// OnDelete 可由调用方用来通知缓存：某个对象已在 store 中被删除。
// 若缓存中存在 UID 相同或 RV 更旧的对象，则将其移除。
func (c *mutationCache) OnDelete(obj runtime.Object) {
	// 无法获取元数据的对象无法比较，静默忽略（该通知是可选的）。
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return
	}

	key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		// 这只是“锦上添花”，失败时不做任何特殊处理
		utilruntime.HandleErrorWithLogger(c.logger, err, "DeletionHandlingMetaNamespaceKeyFunc")
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if mutatedObj, exists := c.mutationCache.Get(key); exists {
		if mutatedObj, ok := mutatedObj.(runtime.Object); ok {
			mutatedObjMeta, err := meta.Accessor(mutatedObj)
			if err != nil {
				return
			}
			// 若已知被删除的对象比 mutated 对象更新，那么 mutated 对象
			// 必然已被移除，因为 resource version 是按类型递增的，
			// 而不是按对象实例。
			//
			// 若 UID 相同，也可以确定它已被移除。
			//
			// 若两者都不成立，则说明调用方写入的 mutated 对象可能仍然
			// 存在，也可能已不存在。调用方需要在 TTL 之后自行检查。
			if c.comparator.CompareResourceVersion(obj, mutatedObj) >= 0 ||
				mutatedObjMeta.GetUID() == objMeta.GetUID() {
				c.mutationCache.Remove(key)
			} else {
				return
			}
		}
	}

	// 在 mutation 缓存中存入一个 tombstone 对象。
	// 之后若某次 Mutation 调用的 RV 更低或相等，或 UID 相同，
	// 则视为过时。
	t := &tombstone{
		namespace: objMeta.GetNamespace(),
		name:      objMeta.GetName(),
		uid:       objMeta.GetUID(),
		rv:        objMeta.GetResourceVersion(),
	}
	c.mutationCache.Add(key, t, c.ttl)
}

func (c *mutationCache) internal() {}

// etcdObjectVersioner 为内嵌 ObjectMeta 或 ListMeta 字段的对象实现版本信息的
// 提取与比较。
//
// 该比较器假设 resourceVersion 是单调递增的十进制整数，这适用于 etcd 后端，
// 但不适用于所有存储实现。
type etcdObjectVersioner struct{}

// ObjectResourceVersion 实现 Versioner 接口。
func (a etcdObjectVersioner) ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	version := accessor.GetResourceVersion()
	if len(version) == 0 {
		// 空 resourceVersion 视为 0，便于比较尚未持久化的对象。
		return 0, nil
	}
	return strconv.ParseUint(version, 10, 64)
}

// CompareResourceVersion 比较 etcd 的 resource version。在本 API 之外它们都是字符串，
// 但 etcd 的 resource version 很特殊，实际是整数，因此可以直接比较。
func (a etcdObjectVersioner) CompareResourceVersion(lhs, rhs runtime.Object) int {
	lhsVersion, err := a.ObjectResourceVersion(lhs)
	if err != nil {
		// 编码错误：无法解析说明对象类型或数据有误，属于调用方 bug，直接 panic。
		panic(err)
	}
	rhsVersion, err := a.ObjectResourceVersion(rhs)
	if err != nil {
		// 编码错误
		panic(err)
	}

	if lhsVersion == rhsVersion {
		return 0
	}
	if lhsVersion < rhsVersion {
		return -1
	}

	return 1
}

// tombstone 是被删除对象的替身。它与该对象 key 相同，并记录检测到删除时的
// ResourceVersion。
//
// 它实现了 GetName、GetNamespace、GetResourceVersion 和 GetUID，因此上面的代码
// 可以把它与其他普通对象比较。区别在于它永远不会通过 ByIndex 或 GetByKey 返回。
//
// tombstone 用“同 key 的墓碑”阻止已被删除对象通过并发到达的 Put/Update
// 重新出现在缓存视图里。
type tombstone struct {
	namespace, name, rv string
	uid                 types.UID
}

// 编译期断言：tombstone 必须能参与与其他对象的比较。
var _ metav1.Object = &tombstone{}
var _ runtime.Object = &tombstone{}

// DeepCopyObject 返回浅拷贝即可：所有字段均为值类型。
func (t *tombstone) DeepCopyObject() runtime.Object {
	clone := *t
	return &clone
}

// 除 GetNamespace/GetName/GetUID/GetResourceVersion 外，metav1.Object 的其余
// 方法都不会被比较逻辑使用，因此以 panic 显式标记为不支持，避免误用。
func (t *tombstone) GetObjectKind() schema.ObjectKind { panic("not implemented") }

func (t *tombstone) GetNamespace() string                          { return t.namespace }
func (t *tombstone) SetNamespace(namespace string)                 { panic("not implemented") }
func (t *tombstone) GetName() string                               { return t.name }
func (t *tombstone) SetName(name string)                           { panic("not implemented") }
func (t *tombstone) GetGenerateName() string                       { panic("not implemented") }
func (t *tombstone) SetGenerateName(name string)                   { panic("not implemented") }
func (t *tombstone) GetUID() types.UID                             { return t.uid }
func (t *tombstone) SetUID(uid types.UID)                          { panic("not implemented") }
func (t *tombstone) GetResourceVersion() string                    { return t.rv }
func (t *tombstone) SetResourceVersion(version string)             { panic("not implemented") }
func (t *tombstone) GetGeneration() int64                          { panic("not implemented") }
func (t *tombstone) SetGeneration(generation int64)                { panic("not implemented") }
func (t *tombstone) GetSelfLink() string                           { panic("not implemented") }
func (t *tombstone) SetSelfLink(selfLink string)                   { panic("not implemented") }
func (t *tombstone) GetCreationTimestamp() metav1.Time             { panic("not implemented") }
func (t *tombstone) SetCreationTimestamp(timestamp metav1.Time)    { panic("not implemented") }
func (t *tombstone) GetDeletionTimestamp() *metav1.Time            { panic("not implemented") }
func (t *tombstone) SetDeletionTimestamp(timestamp *metav1.Time)   { panic("not implemented") }
func (t *tombstone) GetDeletionGracePeriodSeconds() *int64         { panic("not implemented") }
func (t *tombstone) SetDeletionGracePeriodSeconds(*int64)          { panic("not implemented") }
func (t *tombstone) GetLabels() map[string]string                  { panic("not implemented") }
func (t *tombstone) SetLabels(labels map[string]string)            { panic("not implemented") }
func (t *tombstone) GetAnnotations() map[string]string             { panic("not implemented") }
func (t *tombstone) SetAnnotations(annotations map[string]string)  { panic("not implemented") }
func (t *tombstone) GetFinalizers() []string                       { panic("not implemented") }
func (t *tombstone) SetFinalizers(finalizers []string)             { panic("not implemented") }
func (t *tombstone) GetOwnerReferences() []metav1.OwnerReference   { panic("not implemented") }
func (t *tombstone) SetOwnerReferences([]metav1.OwnerReference)    { panic("not implemented") }
func (t *tombstone) GetManagedFields() []metav1.ManagedFieldsEntry { panic("not implemented") }
func (t *tombstone) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) {
	panic("not implemented")
}
