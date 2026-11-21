/*
Copyright 2018 The Kubernetes Authors.

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

package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/utils/clock"
)

// listObjectFunc 按命名空间与列表选项获取对象列表。
type listObjectFunc func(string, metav1.ListOptions) (runtime.Object, error)

// watchObjectFunc 按命名空间与列表选项建立对象 watch。
type watchObjectFunc func(string, metav1.ListOptions) (watch.Interface, error)

// newObjectFunc 创建用于解码的空对象。
type newObjectFunc func() runtime.Object

// isImmutableFunc 判断对象是否不可变（不可变对象无需继续 watch）。
type isImmutableFunc func(runtime.Object) bool

// listWatcherWithWatchListSemanticsWrapperFunc 为 ListWatch 包装 watchlist 语义支持。
type listWatcherWithWatchListSemanticsWrapperFunc func(lw *cache.ListWatch) cache.ListerWatcher

// objectCacheItem 是 objectCache 中存储的单个缓存项。
type objectCacheItem struct {
	// refMap 记录引用该对象的 Pod UID 及引用计数。
	refMap    map[types.UID]int
	store     *cacheStore
	reflector *cache.Reflector

	hasSynced func() (bool, error)

	// waitGroup 保证不会有两个并发的 reflector.Run 调用。
	waitGroup sync.WaitGroup

	// lock 保护 lastAccessTime、stopped、immutable 的并发访问，
	// 并防止 stopCh 被重复关闭。
	lock           sync.Mutex
	lastAccessTime time.Time
	stopped        bool
	immutable      bool
	stopCh         chan struct{}
}

// stop 在加锁状态下停止该缓存项的 reflector，返回是否真正执行了停止。
func (i *objectCacheItem) stop() bool {
	i.lock.Lock()
	defer i.lock.Unlock()
	return i.stopThreadUnsafe()
}

// stopThreadUnsafe 在调用方已持有锁的前提下停止 reflector；
// 重复调用返回 false，避免重复关闭通道。
func (i *objectCacheItem) stopThreadUnsafe() bool {
	if i.stopped {
		return false
	}
	i.stopped = true
	close(i.stopCh)
	// 可变对象在停止 watch 后需清除初始化标记，下次读取时重新同步。
	if !i.immutable {
		i.store.unsetInitialized()
	}
	return true
}

// setLastAccessTime 更新该缓存项的最后访问时间。
func (i *objectCacheItem) setLastAccessTime(time time.Time) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.lastAccessTime = time
}

// setImmutable 将缓存项标记为不可变，此后不再重启 reflector。
func (i *objectCacheItem) setImmutable() {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.immutable = true
}

// stopIfIdle 在缓存项空闲超过 maxIdleTime 且已完成同步时停止其 reflector。
func (i *objectCacheItem) stopIfIdle(now time.Time, maxIdleTime time.Duration) bool {
	i.lock.Lock()
	defer i.lock.Unlock()
	// 确保不会停止尚未完成初始化的 reflector。
	// 当 kube-apiserver 过载时，若列表请求仍在处理中，
	// 此时停止会让已完成的工作白费，只能重试。
	if !i.stopped && i.store.hasSynced() && now.After(i.lastAccessTime.Add(maxIdleTime)) {
		return i.stopThreadUnsafe()
	}
	return false
}

// restartReflectorIfNeeded 在缓存项此前被停止且仍可变时重启其 reflector。
func (i *objectCacheItem) restartReflectorIfNeeded() {
	i.lock.Lock()
	defer i.lock.Unlock()
	if i.immutable || !i.stopped {
		return
	}
	i.stopCh = make(chan struct{})
	i.stopped = false
	go i.startReflector()
}

// startReflector 等待之前的 reflector 退出后，启动新的 reflector。
func (i *objectCacheItem) startReflector() {
	i.waitGroup.Wait()
	i.waitGroup.Add(1)
	defer i.waitGroup.Done()
	i.reflector.Run(i.stopCh)
}

// cacheStore 重写 Replace 方法，以便在首次替换后标记缓存已初始化。
type cacheStore struct {
	cache.Store
	lock        sync.Mutex
	initialized bool
}

// Replace 替换缓存内容，并在成功后把缓存标记为已初始化。
func (c *cacheStore) Replace(list []interface{}, resourceVersion string) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	err := c.Store.Replace(list, resourceVersion)
	if err != nil {
		return err
	}
	c.initialized = true
	return nil
}

// hasSynced 返回缓存是否已完成首次同步。
func (c *cacheStore) hasSynced() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.initialized
}

// unsetInitialized 清除初始化标记，使后续读取重新等待同步。
func (c *cacheStore) unsetInitialized() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.initialized = false
}

// objectCache 是通过独立 watch 传播的对象的本地缓存。
type objectCache struct {
	listObject                               listObjectFunc
	watchObject                              watchObjectFunc
	newObject                                newObjectFunc
	isImmutable                              isImmutableFunc
	listWatcherWithWatchListSemanticsWrapper listWatcherWithWatchListSemanticsWrapperFunc
	groupResource                            schema.GroupResource
	clock                                    clock.Clock
	maxIdleTime                              time.Duration

	// lock 保护 items 与 stopped 的并发访问。
	lock    sync.RWMutex
	items   map[objectKey]*objectCacheItem
	stopped bool
}

// minIdleTime 是空闲回收时间的最小值，避免过于频繁地停止与重建 watch。
const minIdleTime = 1 * time.Minute

// NewObjectCache 返回一个基于 watch 的 Store 实现。
func NewObjectCache(
	listObject listObjectFunc,
	watchObject watchObjectFunc,
	newObject newObjectFunc,
	isImmutable isImmutableFunc,
	listWatcherWithWatchListSemanticsWrapper listWatcherWithWatchListSemanticsWrapperFunc,
	groupResource schema.GroupResource,
	clock clock.Clock,
	maxIdleTime time.Duration,
	stopCh <-chan struct{}) Store {

	// 对空闲时间设置下限，防止配置过小导致 watch 频繁抖动。
	if maxIdleTime < minIdleTime {
		maxIdleTime = minIdleTime
	}

	store := &objectCache{
		listObject:                               listObject,
		watchObject:                              watchObject,
		newObject:                                newObject,
		isImmutable:                              isImmutable,
		listWatcherWithWatchListSemanticsWrapper: listWatcherWithWatchListSemanticsWrapper,
		groupResource:                            groupResource,
		clock:                                    clock,
		maxIdleTime:                              maxIdleTime,
		items:                                    make(map[objectKey]*objectCacheItem),
	}

	// 周期性回收空闲 watch，并在收到停止信号时统一关闭。
	go wait.Until(store.startRecycleIdleWatch, time.Minute, stopCh)
	go store.shutdownWhenStopped(stopCh)
	return store
}

// newStore 创建一个带初始化标记的缓存存储。
func (c *objectCache) newStore() *cacheStore {
	// TODO: 可以考虑实现只保存单个对象的专用存储，替代通用 Store。
	// 但基准测试显示内存开销仅从每对象约 600B 降至约 300B，
	// 在出现明确收益前暂不优化。
	store := cache.NewStore(cache.MetaNamespaceKeyFunc)
	return &cacheStore{store, sync.Mutex{}, false}
}

// newReflectorLocked 在调用方已持有锁的前提下，为单个对象创建 reflector 与缓存项。
func (c *objectCache) newReflectorLocked(namespace, name string) *objectCacheItem {
	fieldSelector := fields.Set{"metadata.name": name}.AsSelector().String()
	listFunc := func(options metav1.ListOptions) (runtime.Object, error) {
		options.FieldSelector = fieldSelector
		return c.listObject(namespace, options)
	}
	watchFunc := func(options metav1.ListOptions) (watch.Interface, error) {
		options.FieldSelector = fieldSelector
		return c.watchObject(namespace, options)
	}
	store := c.newStore()
	reflector := cache.NewReflectorWithOptions(
		c.listWatcherWithWatchListSemanticsWrapper(&cache.ListWatch{ListFunc: listFunc, WatchFunc: watchFunc}),
		c.newObject(),
		store,
		cache.ReflectorOptions{
			Name: fmt.Sprintf("object-%q/%q", namespace, name),
			// 将默认的 5 分钟 MinWatchTimeout 调大，避免 watch 被过于频繁地重建。
			MinWatchTimeout: 30 * time.Minute,
		},
	)
	item := &objectCacheItem{
		refMap:    make(map[types.UID]int),
		store:     store,
		reflector: reflector,
		hasSynced: func() (bool, error) { return store.hasSynced(), nil },
		stopCh:    make(chan struct{}),
	}

	// kubelet 已在关闭中时不再启动新的 reflector。
	if !c.stopped {
		go item.startReflector()
	}
	return item
}

// AddReference 为指定对象增加一个来自 referencedFrom 的引用，首次引用时启动 watch。
func (c *objectCache) AddReference(namespace, name string, referencedFrom types.UID) {
	key := objectKey{namespace: namespace, name: name}

	// AddReference 由 RegisterPod 调用，必须足够高效：
	// 这里只递增引用计数，并在首次注册时启动对应的 reflector。
	// 等待 reflector 同步完成由第一次 Get 操作负责。
	c.lock.Lock()
	defer c.lock.Unlock()
	item, exists := c.items[key]
	if !exists {
		item = c.newReflectorLocked(namespace, name)
		c.items[key] = item
	}
	item.refMap[referencedFrom]++
}

// DeleteReference 移除一个引用；引用计数归零时停止 watch 并删除缓存项。
func (c *objectCache) DeleteReference(namespace, name string, referencedFrom types.UID) {
	key := objectKey{namespace: namespace, name: name}

	c.lock.Lock()
	defer c.lock.Unlock()
	if item, ok := c.items[key]; ok {
		item.refMap[referencedFrom]--
		if item.refMap[referencedFrom] == 0 {
			delete(item.refMap, referencedFrom)
		}
		if len(item.refMap) == 0 {
			// 没有任何引用后停止底层 reflector，释放资源。
			item.stop()
			delete(c.items, key)
		}
	}
}

// key 返回给定命名空间与名称的对象键。
// 该格式必须与 cache.MetaNamespaceKeyFunc 保持一致。
func (c *objectCache) key(namespace, name string) string {
	if len(namespace) > 0 {
		return namespace + "/" + name
	}
	return name
}

// isStopped 返回缓存是否已进入停止状态。
func (c *objectCache) isStopped() bool {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.stopped
}

// Get 从本地缓存获取对象，必要时等待 reflector 完成同步。
func (c *objectCache) Get(namespace, name string) (runtime.Object, error) {
	key := objectKey{namespace: namespace, name: name}

	c.lock.RLock()
	item, exists := c.items[key]
	c.lock.RUnlock()

	if !exists {
		return nil, fmt.Errorf("object %q/%q not registered", namespace, name)
	}
	// 无论后续成功与否都记录访问时间，避免 reflector 因竞态被过早关闭。
	item.setLastAccessTime(c.clock.Now())

	// kubelet 已在关闭中时不再重启 reflector。
	if !c.isStopped() {
		item.restartReflectorIfNeeded()
	}
	// 轮询等待缓存同步完成；超时说明缓存不可用，返回错误让调用方重试。
	if err := wait.PollImmediate(10*time.Millisecond, time.Second, item.hasSynced); err != nil {
		return nil, fmt.Errorf("failed to sync %s cache: %v", c.groupResource.String(), err)
	}
	obj, exists, err := item.store.GetByKey(c.key(namespace, name))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, apierrors.NewNotFound(c.groupResource, name)
	}
	if object, ok := obj.(runtime.Object); ok {
		// 若返回的对象不可变，则停止 reflector。
		//
		// 注意：理论上对象在创建时即不可变的话可以完全不启动 reflector，
		// 但考虑到：
		// - 需要处理对象稍后才被标记为不可变的情况；
		// - Secret 与 ConfigMap 本来就会被 volume manager 周期性读取；
		// - 提前判断并不能带来可见的扩展性或性能收益——这里的处理已经足够；
		// - 提前判断需要对 reflector 做较大重构；
		// 因此这里只做快速停止 reflector 的处理。
		if c.isImmutable(object) {
			item.setImmutable()
			if item.stop() {
				// TODO: 未来应替换为合适的上下文。
				ctx := context.TODO()
				logger := klog.FromContext(ctx)
				logger.V(4).Info("Stopped watching for changes - object is immutable", "obj", klog.KRef(namespace, name))
			}
		}
		return object, nil
	}
	return nil, fmt.Errorf("unexpected object type: %v", obj)
}

// startRecycleIdleWatch 遍历所有缓存项，回收空闲超时的 watch。
func (c *objectCache) startRecycleIdleWatch() {
	// TODO: 未来应替换为合适的上下文。
	ctx := context.TODO()
	logger := klog.FromContext(ctx)
	c.lock.Lock()
	defer c.lock.Unlock()

	for key, item := range c.items {
		if item.stopIfIdle(c.clock.Now(), c.maxIdleTime) {
			logger.V(4).Info("Not acquired for long time, Stopped watching for changes", "objectKey", key, "maxIdleTime", c.maxIdleTime)
		}
	}
}

// shutdownWhenStopped 在收到停止信号后标记缓存为已停止，并停止所有 reflector。
func (c *objectCache) shutdownWhenStopped(stopCh <-chan struct{}) {
	<-stopCh

	c.lock.Lock()
	defer c.lock.Unlock()

	c.stopped = true
	for _, item := range c.items {
		item.stop()
	}
}

// NewWatchBasedManager 创建一个管理器，为已注册 Pod 所需的全部对象维护缓存。
// 其逻辑如下：
//   - 每当 Pod 被创建或更新时，为所有尚未被其他已注册 Pod 引用的对象
//     启动独立的 watch；
//   - 每次 GetObject() 都返回由 watch 传播到本地缓存的值。
func NewWatchBasedManager(
	listObject listObjectFunc,
	watchObject watchObjectFunc,
	newObject newObjectFunc,
	isImmutable isImmutableFunc,
	listWatcherWithWatchListSemanticsWrapper listWatcherWithWatchListSemanticsWrapperFunc,
	groupResource schema.GroupResource,
	resyncInterval time.Duration,
	getReferencedObjects func(*v1.Pod) sets.Set[string]) Manager {

	// 若 ConfigMap/Secret 作为卷使用，volumeManager 每个 resyncInterval 周期
	// 都会访问对应的 objectCacheItem；我们只想停止仅被环境变量引用的缓存项，
	// 因此把 maxIdleTime 设为 resyncInterval 的整数倍，目前取 5 倍。
	maxIdleTime := resyncInterval * 5

	// TODO: 应从更高层传递 stopCh，而不是使用 NeverStop。
	objectStore := NewObjectCache(listObject, watchObject, newObject, isImmutable, listWatcherWithWatchListSemanticsWrapper, groupResource, clock.RealClock{}, maxIdleTime, wait.NeverStop)
	return NewCacheBasedManager(objectStore, getReferencedObjects)
}
