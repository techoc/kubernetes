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

// MutationCache is able to take the result of update operations and stores them in an LRU
// that can be used to provide a more current view of a requested object.  It requires interpreting
// resourceVersions for comparisons.
// Implementations must be thread-safe.
// OnAddOrupdate and OnDelete should be called from informer event handlers to increase
// the accuracy of the cache.
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

	// This interface is not meant to be implemented elsewhere.
	// Marking it as internal enables future changes without
	// triggering apidiff.
	//
	// internal 通过未导出方法封闭实现集合；由于是未导出方法，包外无法实现该接口。
	internal()
}

// ResourceVersionComparator is able to compare object versions.
//
// 返回值语义与 strings.Compare 一致：负数表示 lhs 更旧，0 表示相同，正数表示更新。
type ResourceVersionComparator interface {
	CompareResourceVersion(lhs, rhs runtime.Object) int
}

// MutationCacheOptions configures the MutationCache returned by NewIntegerResourceVersionMutationCacheWithOptions.
type MutationCacheOptions struct {
	// Indexer is used by ByIndex to look up objects. May be nil, in which case ByIndex returns an error.
	Indexer Indexer

	// TTL controls how long an item remains in the mutation cache before it is removed.
	// Zero means the default of 5 minutes.
	TTL time.Duration

	// IncludeAdds makes the cache return objects even if they don't exist
	// in the underlying store. This is only safe if your use of the cache
	// can handle mutation entries remaining in the cache for up to TTL
	// when mutations and deletes occur very closely in time.
	IncludeAdds bool

	// MaxCacheSize limits how many mutated objects are tracked; the least
	// recently used entries are evicted once the limit is reached.
	// Zero means the default of 100.
	MaxCacheSize int
}

// NewIntegerResourceVersionMutationCacheWithOptions returns a MutationCache that understands how to
// deal with objects that have a resource version that:
//
//   - is an integer
//   - increases when updated
//   - is comparable across the same resource in a namespace
//
// Most backends will have these semantics. Indexer may be nil. ttl controls how long an item
// remains in the mutation cache before it is removed.
//
// If includeAdds is true, objects in the mutation cache will be returned even if they don't exist
// in the underlying store. This is only safe if your use of the cache can handle mutation entries
// remaining in the cache for up to ttl when mutations and deletes occur very closely in time.
//
// Note that this also applies to objects updated by the caller, not just
// objects added by it.  That's because the MutationCache may hold an updated
// object at a time when the underlying store already removed it because it was
// deleted in the apiserver after the update. To address this, the caller
// can call OnAddOrUpdate and OnDelete from informer event handlers.
//
// This informs the MutationCache about all changes observed by the store
// and enables it to remove a locally added or updated object immediately once
// it appears in the store, which prevents the "stale updated object" problem.
//
// This does not solve the "stale added object" problem reliably because
// the informer might never see the added object when the add is followed
// quickly by a delete. The caller has to handle stale added objects by
// checking whether they still exist once the TTL is over.
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

// NewIntegerResourceVersionMutationCache is like NewIntegerResourceVersionMutationCacheWithOptions with a fixed cache size of 100.
func NewIntegerResourceVersionMutationCache(logger klog.Logger, backingCache Store, indexer Indexer, ttl time.Duration, includeAdds bool) MutationCache {
	return NewIntegerResourceVersionMutationCacheWithOptions(logger, backingCache, MutationCacheOptions{
		TTL:          ttl,
		Indexer:      indexer,
		IncludeAdds:  includeAdds,
		MaxCacheSize: 100,
	})
}

// mutationCache doesn't guarantee that it returns values added via Mutation since they can page out and
// since you can't distinguish between, "didn't observe create" and "was deleted after create",
// if the key is missing from the backing cache, we always return it as missing
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

// GetByKey is never guaranteed to return back the value set in Mutation.  It could be paged out, it could
// be older than another copy, the backingCache may be more recent or, you might have written twice into the same key.
// You get a value that was valid at some snapshot of time and will always return the newer of backingCache and mutationCache.
func (c *mutationCache) GetByKey(key string) (interface{}, bool, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	obj, exists, err := c.backingCache.GetByKey(key)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		if !c.includeAdds {
			// we can't distinguish between, "didn't observe create" and "was deleted after create", so
			// if the key is missing, we always return it as missing
			return nil, false, nil
		}
		obj, exists = c.mutationCache.Get(key)
		if !exists {
			return nil, false, nil
		}
		// Don't return the tombstone.
		if _, ok := obj.(*tombstone); ok {
			return nil, false, nil
		}
	}
	objRuntime, ok := obj.(runtime.Object)
	if !ok {
		return obj, true, nil
	}
	// If the object is from the store,
	// then this will remove the obsolete tombstone.
	//
	// The newer object can never be the tombstone
	// because a) we don't get here if only the tombstone
	// exists (early return above) and b) the store's
	// object must be more recent than the tombstone
	// (the tombstone was created by an earlier store
	// deletion).
	return c.newerObject(key, objRuntime), true, nil
}

// ByIndex returns the newer objects that match the provided index and indexer key.
// Will return an error if no indexer was provided.
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
		// Keys() is returned oldest to newest, so full traversal does not alter the LRU behavior
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

// newerObject checks the mutation cache for a newer object and returns one if found. If the
// mutated object is older than the backing object, it is removed from the  Must be
// called while the lock is held.
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

// Mutation adds a change to the cache that can be returned in GetByKey if it is newer than the backingCache
// copy.  If you call Mutation twice with the same object on different threads, one will win, but its not defined
// which one.  This doesn't affect correctness, since the GetByKey guaranteed of "later of these two caches" is
// preserved, but you may not get the version of the object you want.  The object you get is only guaranteed to
// "one that was valid at some point in time", not "the one that I want".
func (c *mutationCache) Mutation(obj interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		// this is a "nice to have", so failures shouldn't do anything weird
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
						// Exactly this object instance is known to be deleted.
						// Don't store it, keep the tombstone.
						return
					}
					// UID 相同意味着同一实例的另一个版本；既然已删除，
					// 任何版本都不应复活，保留 tombstone。
					objMeta, err := meta.Accessor(obj)
					if err == nil && t.GetUID() == objMeta.GetUID() {
						// Some other revision of this object instance
						// is known to be deleted. Also don't store it.
						return
					}
				}
			}
		}
	}
	c.mutationCache.Add(key, obj, c.ttl)
}

// OnAddOrUpdate can be called to informer the cache about an object added to
// the store or updated in in. If the object is as recent as the cached object
// or newer, the cached object gets removed because it is no longer
// needed. This keeps the cache smaller.
//
// 这是纯粹的优化：即使调用方从不调用，正确性也不受影响，只是缓存会更大。
func (c *mutationCache) OnAddOrUpdate(obj runtime.Object) {
	key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		// this is a "nice to have", so failures shouldn't do anything weird
		utilruntime.HandleErrorWithLogger(c.logger, err, "DeletionHandlingMetaNamespaceKeyFunc")
		return
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if mutatedObj, exists := c.mutationCache.Get(key); exists {
		if mutatedObj, ok := mutatedObj.(runtime.Object); ok {
			// No need to compare the UID here: resource versions can be compared
			// between different instances. If it's newer or equal, then the mutation
			// cache is out-dated.
			if c.comparator.CompareResourceVersion(obj, mutatedObj) >= 0 {
				c.mutationCache.Remove(key)
			}
		}
	}
}

// OnDelete can be called to informer the cache about an object deleted in the store.
// If there is a cached object with the same UID or an older RV, it gets removed.
func (c *mutationCache) OnDelete(obj runtime.Object) {
	// 无法获取元数据的对象无法比较，静默忽略（该通知是可选的）。
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return
	}

	key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		// this is a "nice to have", so failures shouldn't do anything weird
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
			// If the deleted object is known to be more recent than the
			// mutated one, then the mutated one must have been removed
			// because resource versions are incremented by type, not by
			// object instance.
			//
			// If the UID matches, then we can also be sure that it got
			// removed.
			//
			// If neither of this is true, then the caller has added
			// a mutated object that may or may not still exist. They
			// have to check after the TTL.
			if c.comparator.CompareResourceVersion(obj, mutatedObj) >= 0 ||
				mutatedObjMeta.GetUID() == objMeta.GetUID() {
				c.mutationCache.Remove(key)
			} else {
				return
			}
		}
	}

	// Store a tombstone object in the mutation cache.
	// A future Mutation call is outdated if it has a RV
	// which is lower or equal or has the same UID.
	t := &tombstone{
		namespace: objMeta.GetNamespace(),
		name:      objMeta.GetName(),
		uid:       objMeta.GetUID(),
		rv:        objMeta.GetResourceVersion(),
	}
	c.mutationCache.Add(key, t, c.ttl)
}

func (c *mutationCache) internal() {}

// etcdObjectVersioner implements versioning and extracting etcd node information
// for objects that have an embedded ObjectMeta or ListMeta field.
//
// 该比较器假设 resourceVersion 是单调递增的十进制整数，这适用于 etcd 后端，
// 但不适用于所有存储实现。
type etcdObjectVersioner struct{}

// ObjectResourceVersion implements Versioner
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

// CompareResourceVersion compares etcd resource versions.  Outside this API they are all strings,
// but etcd resource versions are special, they're actually ints, so we can easily compare them.
func (a etcdObjectVersioner) CompareResourceVersion(lhs, rhs runtime.Object) int {
	lhsVersion, err := a.ObjectResourceVersion(lhs)
	if err != nil {
		// coder error
		// 无法解析说明对象类型或数据有误，属于调用方 bug，直接 panic。
		panic(err)
	}
	rhsVersion, err := a.ObjectResourceVersion(rhs)
	if err != nil {
		// coder error
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

// tombstone is a replacement for a deleted object. It has the same key as it
// and the ResourceVersion at which the deletion was detected.
//
// It implements GetName, GetNamespace, GetResourceVersion and GetUID and thus
// the code above can compare it against other, normal objects. The difference
// is that it never gets returned by ByIndex or GetByKey.
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
