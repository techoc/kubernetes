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

// Package volumebinding 实现卷绑定调度插件，在调度过程中处理 Pod 的持久卷声明（PVC）。
package volumebinding

import (
	"fmt"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// informer 是 newAssumeCache 所需 [cache.SharedInformer] 的最小子集。
type informer interface {
	AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error)
	GetIndexer() cache.Indexer
}

// passiveAssumeCache 是构建在 informer 之上的缓存，既允许在 informer 事件之外更新对象，
// 也允许恢复到 informer 缓存中的对象版本。
//
// informer 的更新总会覆盖 assumed 对象。
//
// 它与 pkg/scheduler/util/assumecache 的区别在于：
//   - 不派发事件
//   - 始终与 informer 保持同步
//   - 只允许 assume 尚未发送到 apiserver 的对象，不允许 assume 从 apiserver 返回的对象
type passiveAssumeCache[T v1.Object] struct {
	// 创建缓存时选定的 logger，所有操作都使用它。
	logger klog.Logger
	gr     schema.GroupResource

	// 同步对以下所有字段的更新。
	// 虽然 [store] 自带锁，但比较两者的 ResourceVersion 时，
	// 仍需先持有本锁再读取 [store] 或 [assumed]。
	rwMutex sync.RWMutex

	// 来自 informer 的对象。
	store   cache.Indexer
	assumed map[string]T
}

// newAssumeCache 为类型 T 的对象创建一个 assume 缓存。
func newAssumeCache[T v1.Object](logger klog.Logger, informer informer, gr schema.GroupResource) (*passiveAssumeCache[T], error) {
	c := &passiveAssumeCache[T]{
		logger:  logger,
		gr:      gr,
		store:   informer.GetIndexer(),
		assumed: make(map[string]T),
	}

	_, err := informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    c.add,
			UpdateFunc: c.update,
			DeleteFunc: c.delete,
		},
	)
	return c, err
}

// mayExpire 接收来自 informer 的事件；若 assumed 对象已过期，则使其失效。
func (c *passiveAssumeCache[T]) mayExpire(key string) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	assumed, ok := c.assumed[key]
	if !ok {
		return
	}

	// 取最新版本，避免覆盖 [Assume] 写入的更新对象。
	obj, exists, err := c.store.GetByKey(key)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "mayExpire get", "key", key)
		return
	}

	expire := true
	if exists {
		newMeta, err := meta.Accessor(obj)
		if err != nil {
			utilruntime.HandleErrorWithLogger(c.logger, err, "mayExpire meta", "key", key)
			return
		}

		// 仅当版本更新时才覆盖 assumed 对象，避免 resync 造成误失效。
		if assumed.GetResourceVersion() == newMeta.GetResourceVersion() {
			c.logger.V(10).Info("ignoring resync of assumed object", "key", key, "version", assumed.GetResourceVersion())
			expire = false
		} else {
			c.logger.V(4).Info("assumed object expired", "newVersion", newMeta.GetResourceVersion(),
				"key", key, "version", assumed.GetResourceVersion())
		}
	} else {
		c.logger.V(4).Info("assumed object expired", "key", key, "version", assumed.GetResourceVersion())
	}
	if expire {
		delete(c.assumed, key)
	}
}

// add 处理 informer 的添加事件：可能的过期判定依赖对象 key。
func (c *passiveAssumeCache[T]) add(obj any) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "Add object get key")
		return
	}
	c.mayExpire(key)
}

// update 处理 informer 的更新事件；旧对象内容无关紧要，复用 add 即可。
func (c *passiveAssumeCache[T]) update(_, obj any) {
	c.add(obj)
}

// delete 处理 informer 的删除事件。
func (c *passiveAssumeCache[T]) delete(obj any) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleErrorWithLogger(c.logger, err, "Delete object get key")
		return
	}
	c.mayExpire(key)
}

// ByIndex 返回指定索引中索引值匹配给定值存储对象。
//
// 索引只在 store 中的对象上计算。[Assume] 的对象会出现在结果中，但不参与索引。
func (c *passiveAssumeCache[T]) ByIndex(indexName, indexedValue string) ([]T, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	objs, err := c.store.ByIndex(indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return c.replaceAssumed(objs), nil
}

// Get 根据 key 获取对象。
func (c *passiveAssumeCache[T]) Get(key string) (T, error) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()

	obj, err := c.GetAPIObj(key)
	if err != nil {
		return obj, err
	}

	assumed, ok := c.assumed[key]
	// 未 assume，或 informer 对象更新时，以 informer 对象为准。
	if !ok || assumed.GetResourceVersion() != obj.GetResourceVersion() { // not assumed or Informer object is newer
		return obj, nil
	}
	return assumed, nil
}

// GetAPIObj 根据 key 获取 informer 缓存中的对象版本。
func (c *passiveAssumeCache[T]) GetAPIObj(key string) (T, error) {
	obj, ok, err := c.store.GetByKey(key)
	var zero T
	if err != nil {
		return zero, err
	}
	if !ok {
		return zero, apierrors.NewNotFound(c.gr, key)
	}
	v, ok := obj.(T)
	if !ok {
		return zero, fmt.Errorf("object is not of type %T", zero)
	}
	return v, nil
}

// keyOf 返回对象的 namespace/name 键。
func keyOf[T v1.Object](obj T) string {
	return cache.MetaObjectToName(obj).String()
}

// replaceAssumed 把列表中的对象替换为版本匹配的 assumed 对象。
func (c *passiveAssumeCache[T]) replaceAssumed(objs []any) []T {
	allObjs := make([]T, 0, len(objs))
	for _, obj := range objs {
		v, ok := obj.(T)
		if !ok {
			utilruntime.HandleErrorWithLogger(c.logger, nil, "listed object has wrong type", "type", fmt.Sprintf("%T", obj))
			continue
		}
		assumed, ok := c.assumed[keyOf(v)]
		// 版本一致才替换，说明 assumed 对象尚未进入 informer。
		if ok && assumed.GetResourceVersion() == v.GetResourceVersion() {
			v = assumed
		}
		allObjs = append(allObjs, v)
	}
	return allObjs
}

// Assume 只在内存中更新对象。
//
// 对象的版本必须与当前对象一致，否则返回错误。
// 若 assume 期间收到 informer 更新，则丢弃 assumed 对象，采用 apiserver 的更新版本。
func (c *passiveAssumeCache[T]) Assume(obj T) error {
	key := keyOf(obj)

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	stored, err := c.GetAPIObj(key)
	if err != nil {
		return err
	}

	if stored.GetResourceVersion() != obj.GetResourceVersion() {
		return fmt.Errorf("%q is out of sync (stored: %s, assume: %s)", key, stored.GetResourceVersion(), obj.GetResourceVersion())
	}
	c.assumed[key] = obj
	c.logger.V(4).Info("Assumed object", "key", key, "version", obj.GetResourceVersion())
	return nil
}

// Restore 恢复对象到 informer 缓存中的版本。
func (c *passiveAssumeCache[T]) Restore(obj T) {
	key := keyOf(obj)

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	assumed, ok := c.assumed[key]
	if ok && assumed.GetResourceVersion() == obj.GetResourceVersion() {
		delete(c.assumed, key)
		c.logger.V(4).Info("Restored object", "key", key, "version", obj.GetResourceVersion())
	}
}
