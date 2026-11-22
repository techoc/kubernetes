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

// Package assumecache 提供覆盖在 informer 之上的 AssumeCache：
// 允许在 informer 事件之外临时更新对象（Assume），并在后续恢复（Restore）
// 到 informer 的版本，用于调度器"先假定已绑定，失败时回滚"的场景。
package assumecache

import (
	"errors"
	"fmt"
	"strconv"
	"sync"

	"k8s.io/klog/v2"
	"k8s.io/utils/buffer"

	"k8s.io/apimachinery/pkg/api/meta"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
)

// Informer 是 NewAssumeCache 所需的 [cache.SharedInformer] 最小接口子集。
// 仅依赖注册事件处理器这一能力，便于测试时用轻量实现替代完整 informer。
type Informer interface {
	AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error)
}

// AddTestObject 向 assume cache 添加一个对象。
// 仅供单元测试使用！
func AddTestObject(cache *AssumeCache, obj interface{}) {
	cache.add(obj)
}

// UpdateTestObject 更新 assume cache 中的对象。
// 仅供单元测试使用！
func UpdateTestObject(cache *AssumeCache, obj interface{}) {
	cache.update(nil, obj)
}

// DeleteTestObject 从 assume cache 删除一个对象。
// 仅供单元测试使用！
func DeleteTestObject(cache *AssumeCache, obj interface{}) {
	cache.delete(obj)
}

// 这些哨兵错误可通过 errors.Is 进行判定。
var (
	// ErrWrongType 表示对象类型与缓存期望的不一致。
	ErrWrongType = errors.New("object has wrong type")
	// ErrNotFound 表示按 key 未找到对象。
	ErrNotFound = errors.New("object not found")
	// ErrObjectName 表示无法从对象中解析出名称。
	ErrObjectName = errors.New("cannot determine object name")
)

// WrongTypeError 表示对象无法转换为期望类型。
type WrongTypeError struct {
	TypeName string
	Object   interface{}
}

// Error 返回类型不匹配的错误描述。
func (e WrongTypeError) Error() string {
	return fmt.Sprintf("could not convert object to type %v: %+v", e.TypeName, e.Object)
}

// Is 使该错误可通过 errors.Is 匹配 ErrWrongType。
func (e WrongTypeError) Is(err error) bool {
	return err == ErrWrongType
}

// NotFoundError 表示按指定 key 找不到对象。
type NotFoundError struct {
	TypeName  string
	ObjectKey string
}

// Error 返回未找到对象的错误描述。
func (e NotFoundError) Error() string {
	return fmt.Sprintf("could not find %v %q", e.TypeName, e.ObjectKey)
}

// Is 使该错误可通过 errors.Is 匹配 ErrNotFound。
func (e NotFoundError) Is(err error) bool {
	return err == ErrNotFound
}

// ObjectNameError 封装解析对象名称时的底层错误。
type ObjectNameError struct {
	DetailedErr error
}

// Error 返回名称解析失败的错误描述。
func (e ObjectNameError) Error() string {
	return fmt.Sprintf("failed to get object name: %v", e.DetailedErr)
}

// Is 使该错误可通过 errors.Is 匹配 ErrObjectName。
func (e ObjectNameError) Is(err error) bool {
	return err == ErrObjectName
}

// AssumeCache 是覆盖在 informer 之上的缓存，支持在 informer 事件之外更新对象，
// 并可将对象恢复为 informer 版本。对象需为 [meta.Accessor] 支持的 Kubernetes API 对象，
// 以 [cache.MetaNamespaceKeyFunc] 生成的 key 索引。
//
// 每个对象保存两个指针：informer 中的对象，以及最新版本（可能与 informer 对象相同，
// 也可能是内存中的假定对象）：
//   - informer 更新总是覆盖两个指针。
//   - Assume() 只更新最新版本指针。
//   - Restore() 将最新版本指针重置回 informer 对象。
//   - Get/List() 始终返回最新版本指针。
type AssumeCache struct {
	// 创建缓存时选定的日志器，所有操作共用。
	logger klog.Logger

	// 保护以下所有字段的并发访问。
	rwMutex sync.RWMutex

	// cond 供 emitEvents 等待/通知使用。
	cond *sync.Cond

	// 已注册的所有事件处理器。
	eventHandlers       []cache.ResourceEventHandler
	handlerRegistration cache.ResourceEventHandlerRegistration

	// eventQueue 保存向单个处理器投递事件的函数。
	//
	// 这些函数必须在*不持有* rwMutex 时调用：处理器允许回查 assume cache，
	// 持锁调用会死锁。
	//
	// 新函数在持锁处理缓存更新时加入队列；每个向队列写入的函数都会在返回前
	// 排空队列，因此能保证处理器立即收到通知（便于单元测试）。
	//
	// 这里不能用 channel：其容量有限，写满时会阻塞持锁的写方，
	// 而读方处理器又可能尝试获取 rwMutex，形成死锁。
	// 不持锁写入则无法保证事件按序投递。
	eventQueue buffer.Ring[func()]

	// emittingEvents 为 true 表示当前有 emitEvents 正在投递事件。
	emittingEvents bool

	// 所存对象的描述，用于日志与错误信息。
	description string

	// 实际存储 objInfo 的索引器。
	store cache.Indexer

	// 对象的索引函数与索引名。
	indexFunc cache.IndexFunc
	indexName string
}

// objInfo 保存同一对象的两个版本：informer 版本与最新版本。
type objInfo struct {
	// 对象的 key（namespace/name）。
	name string

	// 最新版本，可能是假定对象或 informer 对象。
	latestObj interface{}

	// informer 提供的最新对象。
	apiObj interface{}
}

// objInfoKeyFunc 从 objInfo 中提取 key，供 Indexer 索引使用。
func objInfoKeyFunc(obj interface{}) (string, error) {
	objInfo, ok := obj.(*objInfo)
	if !ok {
		return "", &WrongTypeError{TypeName: "objInfo", Object: obj}
	}
	return objInfo.name, nil
}

// objInfoIndexFunc 基于对象的最新版本计算索引键。
func (c *AssumeCache) objInfoIndexFunc(obj interface{}) ([]string, error) {
	objInfo, ok := obj.(*objInfo)
	if !ok {
		return []string{""}, &WrongTypeError{TypeName: "objInfo", Object: obj}
	}
	return c.indexFunc(objInfo.latestObj)
}

// NewAssumeCache 为通用对象创建 assume cache。
// informer 为 nil 时不注册事件处理器（单元测试场景）。
func NewAssumeCache(logger klog.Logger, informer Informer, description, indexName string, indexFunc cache.IndexFunc) *AssumeCache {
	c := &AssumeCache{
		logger:      logger,
		description: description,
		indexFunc:   indexFunc,
		indexName:   indexName,
		eventQueue:  *buffer.NewRing[func()](buffer.RingOptions{InitialSize: 0, NormalSize: 4}),
	}
	c.cond = sync.NewCond(&c.rwMutex)
	indexers := cache.Indexers{}
	if indexName != "" && indexFunc != nil {
		indexers[indexName] = c.objInfoIndexFunc
	}
	c.store = cache.NewIndexer(objInfoKeyFunc, indexers)

	// 单元测试不使用 informer，此时跳过事件注册。
	if informer != nil {
		// 实际上不会失败，调用方也无人检查该错误。
		c.handlerRegistration, _ = informer.AddEventHandler(
			cache.ResourceEventHandlerFuncs{
				AddFunc:    c.add,
				UpdateFunc: c.update,
				DeleteFunc: c.delete,
			},
		)
	}
	return c
}

// add 处理 informer 的新增/更新事件，用 informer 版本覆盖缓存中的两个版本。
func (c *AssumeCache) add(obj interface{}) {
	if obj == nil {
		return
	}

	name, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, &ObjectNameError{err}, "Add failed")
		return
	}

	defer c.emitEvents()
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	var oldObj interface{}
	if objInfo, _ := c.getObjInfo(name); objInfo != nil {
		newVersion, err := c.getObjVersion(name, obj)
		if err != nil {
			utilruntime.HandleErrorWithLogger(c.logger, err, "Add failed: couldn't get object version")
			return
		}

		storedVersion, err := c.getObjVersion(name, objInfo.latestObj)
		if err != nil {
			utilruntime.HandleErrorWithLogger(c.logger, err, "Add failed: couldn't get stored object version")
			return
		}

		// 只接受更高版本的更新，避免 informer 全量 resync
		// 用旧数据覆盖正在使用的假定对象。
		if newVersion <= storedVersion {
			c.logger.V(10).Info("Skip adding object to assume cache because version is not newer than storedVersion", "description", c.description, "cacheKey", name, "newVersion", newVersion, "storedVersion", storedVersion)
			return
		}
		oldObj = objInfo.latestObj
	}

	objInfo := &objInfo{name: name, latestObj: obj, apiObj: obj}
	if err = c.store.Update(objInfo); err != nil {
		c.logger.Info("Error occurred while updating stored object", "err", err)
	} else {
		c.logger.V(10).Info("Adding object to assume cache", "description", c.description, "cacheKey", name, "assumeCache", obj)
		c.pushEvent(oldObj, obj)
	}
}

// update 处理 informer 的更新事件，语义与 add 相同。
func (c *AssumeCache) update(oldObj interface{}, newObj interface{}) {
	c.add(newObj)
}

// delete 处理 informer 的删除事件，移除缓存中的对象并通知处理器。
func (c *AssumeCache) delete(obj interface{}) {
	if obj == nil {
		return
	}

	name, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, &ObjectNameError{err}, "Failed to delete")
		return
	}

	defer c.emitEvents()
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	var oldObj interface{}
	if len(c.eventHandlers) > 0 {
		if objInfo, _ := c.getObjInfo(name); objInfo != nil {
			oldObj = objInfo.latestObj
		}
	}

	objInfo := &objInfo{name: name}
	err = c.store.Delete(objInfo)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "Failed to delete", "description", c.description, "cacheKey", name)
	}

	c.pushEvent(oldObj, nil)
}

// pushEvent 在调用方持有写锁时调用，为所有已注册的处理器排队通知；
// 实际投递由 emitEvents 在解锁后完成。
//
// 删除事件 newObj 为 nil，新增事件 oldObj 为 nil，更新事件两者均非 nil。
func (c *AssumeCache) pushEvent(oldObj, newObj interface{}) {
	for _, handler := range c.eventHandlers {
		if oldObj == nil {
			c.eventQueue.WriteOne(func() {
				handler.OnAdd(newObj, false)
			})
		} else if newObj == nil {
			c.eventQueue.WriteOne(func() {
				handler.OnDelete(oldObj)
			})
		} else {
			c.eventQueue.WriteOne(func() {
				handler.OnUpdate(oldObj, newObj)
			})
		}
	}
}

// getObjVersion 解析对象的 ResourceVersion 为整数，便于版本比较。
func (c *AssumeCache) getObjVersion(name string, obj interface{}) (int64, error) {
	objAccessor, err := meta.Accessor(obj)
	if err != nil {
		return -1, err
	}

	objResourceVersion, err := strconv.ParseInt(objAccessor.GetResourceVersion(), 10, 64)
	if err != nil {
		//nolint:errorlint // 故意不包装底层错误，它只是实现细节。
		return -1, fmt.Errorf("error parsing ResourceVersion %q for %v %q: %v", objAccessor.GetResourceVersion(), c.description, name, err)
	}
	return objResourceVersion, nil
}

// getObjInfo 按 key 从索引器中取出 objInfo，调用方需持有适当的锁。
func (c *AssumeCache) getObjInfo(key string) (*objInfo, error) {
	obj, ok, err := c.store.GetByKey(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, &NotFoundError{TypeName: c.description, ObjectKey: key}
	}

	objInfo, ok := obj.(*objInfo)
	if !ok {
		return nil, &WrongTypeError{"objInfo", obj}
	}
	return objInfo, nil
}

// Get 按 key 返回对象的最新版本（可能是假定对象）。
func (c *AssumeCache) Get(key string) (interface{}, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	objInfo, err := c.getObjInfo(key)
	if err != nil {
		return nil, err
	}
	return objInfo.latestObj, nil
}

// GetAPIObj 按 key 返回 informer 缓存中的版本，忽略任何假定更新。
func (c *AssumeCache) GetAPIObj(key string) (interface{}, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	objInfo, err := c.getObjInfo(key)
	if err != nil {
		return nil, err
	}
	return objInfo.apiObj, nil
}

// List 返回缓存中所有（或匹配 indexObj 索引的）对象的最新版本。
func (c *AssumeCache) List(indexObj interface{}) []interface{} {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	return c.listLocked(indexObj)
}

// listLocked 是 List 的内部实现，调用方必须持有 rwMutex（读锁或写锁）。
func (c *AssumeCache) listLocked(indexObj interface{}) []interface{} {
	allObjs := []interface{}{}
	var objs []interface{}
	if c.indexName != "" {
		o, err := c.store.Index(c.indexName, &objInfo{latestObj: indexObj})
		if err != nil {
			utilruntime.HandleErrorWithLogger(c.logger, err, "List index error")
			return nil
		}
		objs = o
	} else {
		objs = c.store.List()
	}

	for _, obj := range objs {
		objInfo, ok := obj.(*objInfo)
		if !ok {
			utilruntime.HandleErrorWithLogger(c.logger, &WrongTypeError{TypeName: "objInfo", Object: obj}, "List error")
			continue
		}
		allObjs = append(allObjs, objInfo.latestObj)
	}
	return allObjs
}

// Assume 只在内存中更新对象，不改 informer 版本。
//
// 新对象的版本必须大于等于当前版本，否则返回错误。
//
// 允许假定相同版本的对象，但存在竞态：若此时 informer 收到更新，
// 该更新会被丢弃而采用 apiserver 返回的较新对象。
// 因此只有对 apiserver 操作（Update、Patch）返回的对象执行 Assume 才是安全的。
func (c *AssumeCache) Assume(obj interface{}) error {
	name, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return &ObjectNameError{err}
	}

	defer c.emitEvents()
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	objInfo, err := c.getObjInfo(name)
	if err != nil {
		return err
	}

	newVersion, err := c.getObjVersion(name, obj)
	if err != nil {
		return err
	}

	storedVersion, err := c.getObjVersion(name, objInfo.latestObj)
	if err != nil {
		return err
	}

	if newVersion < storedVersion {
		return fmt.Errorf("%v %q is out of sync (stored: %d, assume: %d)", c.description, name, storedVersion, newVersion)
	}

	c.pushEvent(objInfo.latestObj, obj)

	// 只替换最新版本，保留 informer 版本以便后续 Restore
	oldObj := objInfo.latestObj
	objInfo.latestObj = obj
	if loggerV := c.logger.V(6); loggerV.Enabled() {
		loggerV.Info("Assumed object", "description", c.description, "cacheKey", name, "version", newVersion, "oldObj", klog.Format(oldObj), "newObj", klog.Format(obj))
	} else {
		c.logger.V(5).Info("Assumed object", "description", c.description, "cacheKey", name, "version", newVersion)
	}
	return nil
}

// Restore 将对象恢复为 informer 缓存中的版本。
// 对象已被删除时忽略错误，这属于预期情况。
func (c *AssumeCache) Restore(objName string) {
	defer c.emitEvents()
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	objInfo, err := c.getObjInfo(objName)
	if err != nil {
		// 对象可能已被删除，属于预期情况
		c.logger.V(5).Info("Restore object", "description", c.description, "cacheKey", objName, "err", err)
	} else {
		if objInfo.latestObj != objInfo.apiObj {
			c.pushEvent(objInfo.latestObj, objInfo.apiObj)
			objInfo.latestObj = objInfo.apiObj
		}
		c.logger.V(5).Info("Restored object", "description", c.description, "cacheKey", objName)
	}
}

// AddEventHandler 向缓存添加事件处理器。
// 同一处理器的事件按序投递，但不同处理器之间没有协调顺序；
// 处理器可以回查缓存。
//
// 返回值可用于等待缓存同步完成。
func (c *AssumeCache) AddEventHandler(handler cache.ResourceEventHandler) cache.ResourceEventHandlerRegistration {
	defer c.emitEvents()
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	c.eventHandlers = append(c.eventHandlers, handler)
	allObjs := c.listLocked(nil)
	for _, obj := range allObjs {
		c.eventQueue.WriteOne(func() {
			handler.OnAdd(obj, true)
		})
	}

	if c.handlerRegistration == nil {
		// 没有 informer 时缓存一开始就是同步状态
		s := syncedHandlerRegistration{
			synced: make(chan struct{}),
		}
		close(s.synced)
		return s
	}

	return c.handlerRegistration
}

// emitEvents 按 FIFO 顺序投递队列中所有待处理事件。
// 同一时刻只允许一个 goroutine 投递，以保证事件顺序正确。
func (c *AssumeCache) emitEvents() {
	c.rwMutex.Lock()
	for c.emittingEvents {
		// 等待当前正在投递的调用方结束。它结束时可能已经、
		// 也可能尚未排空本次调用方推入的事件，下面自行再检查队列。
		c.cond.Wait()
	}
	c.emittingEvents = true
	c.rwMutex.Unlock()

	defer func() {
		c.rwMutex.Lock()
		c.emittingEvents = false
		// 将"投递权"交给另一个等待中的 goroutine。
		// 只唤醒一个即可，因为只有一个能拿到 emittingEvents 的责任。
		c.cond.Signal()
		c.rwMutex.Unlock()
	}()

	// 执行到这里说明本实例是当前的活跃投递者。
	for {
		c.rwMutex.Lock()
		deliver, ok := c.eventQueue.ReadOne()
		c.rwMutex.Unlock()

		if !ok {
			return
		}
		func() {
			defer utilruntime.HandleCrash()
			deliver()
		}()
	}
}

// syncedHandlerRegistration 是 ResourceEventHandlerRegistration 的实现，
// 始终报告已同步。用于没有 informer（单元测试）的场景。
type syncedHandlerRegistration struct {
	synced chan struct{}
}

// HasSynced 始终返回 true。
func (syncedHandlerRegistration) HasSynced() bool { return true }

// HasSyncedChecker 返回自身作为 DoneChecker。
func (s syncedHandlerRegistration) HasSyncedChecker() cache.DoneChecker { return s }

// Name 返回注册名。
func (s syncedHandlerRegistration) Name() string { return "AssumeCache" }

// Done 返回已关闭的 channel，表示同步已完成。
func (s syncedHandlerRegistration) Done() <-chan struct{} { return s.synced }
