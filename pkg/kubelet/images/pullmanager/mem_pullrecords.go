/*
Copyright 2025 The Kubernetes Authors.

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

package pullmanager

import (
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/klog/v2"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/utils/lru"
)

// lruCache 是带权威标记的 LRU 缓存：发生过驱逐后不再可信。
type lruCache[K comparable, V any] struct {
	cache   *lru.Cache
	maxSize int

	// authoritative 表示缓存内容是否可作为权威数据源：
	// 容量不足发生驱逐或初始化出错时置为 false。
	authoritative atomic.Bool

	// deletingKeys 让驱逐函数区分“显式删除”与“容量/清空导致的移除”，
	// 只有 lruCache.Delete() 会修改它。
	deletingKeys sync.Map
}

// newLRUCache 创建 LRU 缓存并注册驱逐回调。
func newLRUCache[K comparable, V any](size int) *lruCache[K, V] {
	c := lru.New(size)
	l := &lruCache[K, V]{
		maxSize:      size,
		cache:        c,
		deletingKeys: sync.Map{},
	}
	if err := c.SetEvictionFunc(func(key lru.Key, _ any) {
		if _, shouldIgnore := l.deletingKeys.Load(key); shouldIgnore {
			return
		}
		// 发生驱逐说明缓存不完整，不再作为权威源。
		l.authoritative.Store(false)
	}); err != nil {
		panic(fmt.Sprintf("failed to set eviction function to the LRU cache: %v", err))
	}
	return l
}

// Get 返回缓存值；nil 值也会以 (nil, true) 表示命中。
func (c *lruCache[K, V]) Get(key K) (*V, bool) {
	value, found := c.cache.Get(key)
	if !found {
		return nil, false
	}
	if value == nil {
		return nil, true
	}
	return value.(*V), true
}

// Set 写入缓存。
func (c *lruCache[K, V]) Set(key K, value *V) { c.cache.Add(key, value) }

// Len 返回当前缓存条目数。
func (c *lruCache[K, V]) Len() int { return c.cache.Len() }

// Clear 清空缓存。
func (c *lruCache[K, V]) Clear() { c.cache.Clear() }

// Delete 删除键且不改变权威标记；
// 调用方必须已对 key 持有外部写锁。
func (c *lruCache[K, V]) Delete(key K) {
	c.deletingKeys.Store(key, struct{}{})
	defer c.deletingKeys.Delete(key)
	c.cache.Remove(key)
}

// cachedPullRecordsAccessor 在另一个 PullRecordsAccessor 之上实现写穿透缓存。
type cachedPullRecordsAccessor struct {
	delegate PullRecordsAccessor

	intentsLocks       *StripedLockSet
	intents            *lruCache[string, kubeletconfiginternal.ImagePullIntent]
	pulledRecordsLocks *StripedLockSet
	pulledRecords      *lruCache[string, kubeletconfiginternal.ImagePulledRecord]
}

// NewCachedPullRecordsAccessor 创建带缓存与指标包装的访问器，并预热缓存。
func NewCachedPullRecordsAccessor(logger klog.Logger, delegate PullRecordsAccessor, intentsCacheSize, pulledRecordsCacheSize, stripedLocksSize int32) PullRecordsAccessor {
	intentsCacheSize = min(intentsCacheSize, 1024)
	pulledRecordsCacheSize = min(pulledRecordsCacheSize, 2000)

	c := &cachedPullRecordsAccessor{
		delegate: delegate,

		intentsLocks:       NewStripedLockSet(stripedLocksSize),
		intents:            newLRUCache[string, kubeletconfiginternal.ImagePullIntent](int(intentsCacheSize)),
		pulledRecordsLocks: NewStripedLockSet(stripedLocksSize),
		pulledRecords:      newLRUCache[string, kubeletconfiginternal.ImagePulledRecord](int(pulledRecordsCacheSize)),
	}
	// 预热缓存：列表成功且不超过容量时才标记为权威。
	_, err := c.ListImagePullIntents()
	if err != nil {
		logger.Info("there was an error initializing the image pull intents cache, the cache will work in a non-authoritative mode until the intents are listed successfully", "error", err)
	}
	_, err = c.ListImagePulledRecords()
	if err != nil {
		logger.Info("there was an error initializing the image pulled records cache, the cache will work in a non-authoritative mode until the pulled records are listed successfully", "error", err)
	}
	return NewMeteringRecordsAccessor(c, inMemIntentsPercent, inMemPulledRecordsPercent)
}

// ListImagePullIntents 从委托方列出 intent，并刷新缓存。
func (c *cachedPullRecordsAccessor) ListImagePullIntents() ([]*kubeletconfiginternal.ImagePullIntent, error) {
	return cacheRefreshingList(
		c.intents,
		c.intentsLocks,
		c.delegate.ListImagePullIntents,
		pullIntentToCacheKey,
	)
}

// ImagePullIntentExists 先查缓存，未命中再在锁内查一次，最后才回源。
func (c *cachedPullRecordsAccessor) ImagePullIntentExists(image string) (bool, error) {
	// 先做无锁的快速查询。
	if _, exists := c.intents.Get(image); exists {
		return true, nil
	}

	// 未命中时对镜像加锁，避免并发回源。
	c.intentsLocks.Lock(image)
	defer c.intentsLocks.Unlock(image)

	// 持锁后再次检查，可能已被其他 goroutine 填充。
	if _, exists := c.intents.Get(image); exists {
		return true, nil
	}
	// 缓存权威时未命中即可确定不存在，无需回源。
	if c.intents.authoritative.Load() {
		return false, nil
	}

	// 回退到开销更大的委托查询。
	exists, err := c.delegate.ImagePullIntentExists(image)
	if err == nil && exists {
		c.intents.Set(image, &kubeletconfiginternal.ImagePullIntent{
			Image: image,
		})
	}
	return exists, err
}

// WriteImagePullIntent 写穿透到委托方，成功后更新缓存。
func (c *cachedPullRecordsAccessor) WriteImagePullIntent(logger klog.Logger, image string) error {
	c.intentsLocks.Lock(image)
	defer c.intentsLocks.Unlock(image)

	if err := c.delegate.WriteImagePullIntent(logger, image); err != nil {
		return err
	}
	c.intents.Set(image, &kubeletconfiginternal.ImagePullIntent{
		Image: image,
	})

	return nil
}

// DeleteImagePullIntent 删除委托方记录并同步清理缓存。
func (c *cachedPullRecordsAccessor) DeleteImagePullIntent(logger klog.Logger, image string) error {
	c.intentsLocks.Lock(image)
	defer c.intentsLocks.Unlock(image)

	if err := c.delegate.DeleteImagePullIntent(logger, image); err != nil {
		return err
	}
	c.intents.Delete(image)
	return nil
}

// ListImagePulledRecords 从委托方列出拉取记录，并刷新缓存。
func (c *cachedPullRecordsAccessor) ListImagePulledRecords() ([]*kubeletconfiginternal.ImagePulledRecord, error) {
	return cacheRefreshingList(
		c.pulledRecords,
		c.pulledRecordsLocks,
		c.delegate.ListImagePulledRecords,
		pulledRecordToCacheKey,
	)
}

// GetImagePulledRecord 先查缓存，未命中再持锁回源。
func (c *cachedPullRecordsAccessor) GetImagePulledRecord(imageRef string) (*kubeletconfiginternal.ImagePulledRecord, bool, error) {
	// 先做无锁的快速查询。
	pulledRecord, exists := c.pulledRecords.Get(imageRef)
	if exists {
		return pulledRecord, true, nil
	}

	// 未命中时对 imageRef 加锁，避免并发回源。
	c.pulledRecordsLocks.Lock(imageRef)
	defer c.pulledRecordsLocks.Unlock(imageRef)

	// 持锁后再次检查，可能已被其他 goroutine 填充。
	pulledRecord, exists = c.pulledRecords.Get(imageRef)
	if exists {
		return pulledRecord, true, nil
	}
	// 缓存权威时未命中即可确定不存在，无需回源。
	if c.pulledRecords.authoritative.Load() {
		return nil, false, nil
	}

	// 回退到开销更大的委托查询。
	pulledRecord, exists, err := c.delegate.GetImagePulledRecord(imageRef)
	if err == nil && exists {
		c.pulledRecords.Set(imageRef, pulledRecord)
	}
	return pulledRecord, exists, err
}

// WriteImagePulledRecord 写穿透到委托方，成功后更新缓存。
func (c *cachedPullRecordsAccessor) WriteImagePulledRecord(logger klog.Logger, record *kubeletconfiginternal.ImagePulledRecord) error {
	c.pulledRecordsLocks.Lock(record.ImageRef)
	defer c.pulledRecordsLocks.Unlock(record.ImageRef)

	if err := c.delegate.WriteImagePulledRecord(logger, record); err != nil {
		return err
	}
	c.pulledRecords.Set(record.ImageRef, record)
	return nil
}

// DeleteImagePulledRecord 删除委托方记录并同步清理缓存。
func (c *cachedPullRecordsAccessor) DeleteImagePulledRecord(logger klog.Logger, imageRef string) error {
	c.pulledRecordsLocks.Lock(imageRef)
	defer c.pulledRecordsLocks.Unlock(imageRef)

	if err := c.delegate.DeleteImagePulledRecord(logger, imageRef); err != nil {
		return err
	}
	c.pulledRecords.Delete(imageRef)
	return nil
}

// intentsSize 返回 pull intent 缓存使用率百分比。
func (c *cachedPullRecordsAccessor) intentsSize() (uint, error) {
	intentsUsage := c.intents.Len() * 100 / c.intents.maxSize
	return uint(intentsUsage), nil
}

// pulledRecordsSize 返回拉取记录缓存使用率百分比。
func (c *cachedPullRecordsAccessor) pulledRecordsSize() (uint, error) {
	pulledRecordsUsage := c.pulledRecords.Len() * 100 / c.pulledRecords.maxSize
	return uint(pulledRecordsUsage), nil
}

// cacheRefreshingList 是通用的列表+缓存刷新逻辑：
// 非权威缓存先全局加锁，列表成功后按容量重建缓存。
func cacheRefreshingList[K comparable, V any](
	cache *lruCache[K, V],
	delegateLocks *StripedLockSet,
	listRecordsFunc func() ([]*V, error),
	recordToKey func(*V) K,
) ([]*V, error) {
	wasAuthoritative := cache.authoritative.Load()
	if !wasAuthoritative {
		// 完整列表且结果能放进缓存时，有机会重新变为权威。
		delegateLocks.GlobalLock()
		defer delegateLocks.GlobalUnlock()
	}

	results, err := listRecordsFunc()
	if wasAuthoritative {
		return results, err
	}

	resultsAreAuthoritative := err == nil && len(results) < cache.maxSize
	// 能让缓存变为权威或缓存为空时，才重建缓存。
	if resultsAreAuthoritative || cache.Len() == 0 {
		cache.Clear()
		// 最多写入 maxSize 条，超出部分不缓存。
		for _, record := range results[:min(len(results), cache.maxSize)] {
			cache.Set(recordToKey(record), record)
		}
		cache.authoritative.Store(resultsAreAuthoritative)
	}

	return results, err
}

// pullIntentToCacheKey 以镜像名作为 pull intent 的缓存键。
func pullIntentToCacheKey(intent *kubeletconfiginternal.ImagePullIntent) string {
	return intent.Image
}

// pulledRecordToCacheKey 以 ImageRef 作为拉取记录的缓存键。
func pulledRecordToCacheKey(record *kubeletconfiginternal.ImagePulledRecord) string {
	return record.ImageRef
}
