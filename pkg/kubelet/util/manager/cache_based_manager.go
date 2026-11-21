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
	"strconv"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/kubernetes/pkg/kubelet/util"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
)

// GetObjectTTLFunc 定义获取 TTL 值的函数类型。
type GetObjectTTLFunc func() (time.Duration, bool)

// GetObjectFunc 定义按命名空间和名称获取对象的函数类型。
type GetObjectFunc func(string, string, metav1.GetOptions) (runtime.Object, error)

// objectKey 是对象缓存的键，包含命名空间、名称和 UID。
type objectKey struct {
	namespace string
	name      string
	uid       types.UID
}

// objectStoreItem 是 objectStore 中存储的单个条目。
type objectStoreItem struct {
	refCount int
	data     *objectData
}

// objectData 缓存单个对象的数据、错误与最近一次更新时间，并内嵌互斥锁保护这些字段。
type objectData struct {
	sync.Mutex

	object         runtime.Object
	err            error
	lastUpdateTime time.Time
}

// objectStore 是对象的本地缓存。
type objectStore struct {
	getObject GetObjectFunc
	clock     clock.Clock

	lock  sync.Mutex
	items map[objectKey]*objectStoreItem

	defaultTTL time.Duration
	getTTL     GetObjectTTLFunc
}

// NewObjectStore 返回基于 TTL 的 Store 接口实例。
func NewObjectStore(getObject GetObjectFunc, clock clock.Clock, getTTL GetObjectTTLFunc, ttl time.Duration) Store {
	return &objectStore{
		getObject:  getObject,
		clock:      clock,
		items:      make(map[objectKey]*objectStoreItem),
		defaultTTL: ttl,
		getTTL:     getTTL,
	}
}

// isObjectOlder 判断 newObject 的资源版本是否比 oldObject 更旧；任一对象为 nil 时返回 false。
func isObjectOlder(newObject, oldObject runtime.Object) bool {
	if newObject == nil || oldObject == nil {
		return false
	}
	newVersion, _ := storage.APIObjectVersioner{}.ObjectResourceVersion(newObject)
	oldVersion, _ := storage.APIObjectVersioner{}.ObjectResourceVersion(oldObject)
	return newVersion < oldVersion
}

// AddReference 为指定对象增加引用计数；实际拉取推迟到下一次 Get 调用。
func (s *objectStore) AddReference(namespace, name string, _ types.UID) {
	key := objectKey{namespace: namespace, name: name}

	// AddReference 由 RegisterPod 调用，必须足够高效：这里只增加引用计数并
	// 使对象失效，真正拉取对象交给 Get() 负责。
	s.lock.Lock()
	defer s.lock.Unlock()
	item, exists := s.items[key]
	if !exists {
		item = &objectStoreItem{
			refCount: 0,
			data:     &objectData{},
		}
		s.items[key] = item
	}

	item.refCount++
	// 将缓存数据置空，使下一次 Get() 触发重新拉取。
	item.data = nil
}

// DeleteReference 减少指定对象的引用计数，计数归零时从缓存中删除。
func (s *objectStore) DeleteReference(namespace, name string, _ types.UID) {
	key := objectKey{namespace: namespace, name: name}

	s.lock.Lock()
	defer s.lock.Unlock()
	if item, ok := s.items[key]; ok {
		item.refCount--
		// 引用计数归零说明已无 Pod 使用该对象，可以安全移除。
		if item.refCount == 0 {
			delete(s.items, key)
		}
	}
}

// GetObjectTTLFromNodeFunc 返回一个从 Node 对象读取 TTL 值的函数。
func GetObjectTTLFromNodeFunc(ctx context.Context, getNode func(context.Context) (*v1.Node, error)) GetObjectTTLFunc {
	return func() (time.Duration, bool) {
		node, err := getNode(ctx)
		if err != nil {
			return time.Duration(0), false
		}
		// 读取失败或未设置注解时返回 false，调用方会回退到默认 TTL。
		if node != nil && node.Annotations != nil {
			// TTL 注解以秒为单位的整数形式存储。
			if value, ok := node.Annotations[v1.ObjectTTLAnnotationKey]; ok {
				if intValue, err := strconv.Atoi(value); err == nil {
					return time.Duration(intValue) * time.Second, true
				}
			}
		}
		return time.Duration(0), false
	}
}

// isObjectFresh 判断缓存对象是否仍在 TTL 有效期内。
func (s *objectStore) isObjectFresh(data *objectData) bool {
	objectTTL := s.defaultTTL
	// Node 注解中的 TTL 优先于默认 TTL。
	if ttl, ok := s.getTTL(); ok {
		objectTTL = ttl
	}
	return s.clock.Now().Before(data.lastUpdateTime.Add(objectTTL))
}

// Get 返回指定对象：优先使用未过期的缓存，否则从 apiserver 重新拉取。
func (s *objectStore) Get(namespace, name string) (runtime.Object, error) {
	key := objectKey{namespace: namespace, name: name}

	data := func() *objectData {
		s.lock.Lock()
		defer s.lock.Unlock()
		item, exists := s.items[key]
		if !exists {
			return nil
		}
		if item.data == nil {
			item.data = &objectData{}
		}
		return item.data
	}()
	// 未注册的对象直接报错，避免对未知对象发起请求。
	if data == nil {
		return nil, fmt.Errorf("object %q/%q not registered", namespace, name)
	}

	// 取到对象数据后单独加锁：按需拉取并返回数据。
	// 每个对象独立加锁，避免不同对象的拉取相互阻塞。
	data.Lock()
	defer data.Unlock()
	// 缓存有错误或已过期时重新拉取。
	if data.err != nil || !s.isObjectFresh(data) {
		opts := metav1.GetOptions{}
		if data.object != nil && data.err == nil {
			// 这只是对之前成功拉取过的对象做周期性刷新。此时从 apiserver 缓存读取，
			// 以降低 etcd 与 apiserver 的负载（该缓存是最终一致的）。
			util.FromApiserverCache(&opts)
		}

		object, err := s.getObject(namespace, name, opts)
		if err != nil && !apierrors.IsNotFound(err) && data.object == nil && data.err == nil {
			// 无法拉取最新对象，且没有可返回的缓存数据时，直接返回拉取结果。
			return object, err
		}
		if (err == nil && !isObjectOlder(object, data.object)) || apierrors.IsNotFound(err) {
			// 若成功拉到更新版本的对象，或对象在 apiserver 中已不存在，
			// 则更新缓存数据以反映最新状态；更旧的响应不会覆盖缓存。
			data.object = object
			data.err = err
			data.lastUpdateTime = s.clock.Now()
		}
	}
	return data.object, data.err
}

// cacheBasedManager 维护已注册 Pod 所需对象的存储。不同的 Store 实现
// 对对象新鲜度有不同语义（例如基于 TTL 的实现与基于 watch 的实现）。
type cacheBasedManager struct {
	objectStore          Store
	getReferencedObjects func(*v1.Pod) sets.Set[string]

	lock           sync.Mutex
	registeredPods map[objectKey]*v1.Pod
}

// GetObject 从底层对象存储中获取对象。
func (c *cacheBasedManager) GetObject(namespace, name string) (runtime.Object, error) {
	return c.objectStore.Get(namespace, name)
}

// RegisterPod 记录 Pod，并为其引用的对象增加引用计数。
func (c *cacheBasedManager) RegisterPod(pod *v1.Pod) {
	names := c.getReferencedObjects(pod)
	c.lock.Lock()
	defer c.lock.Unlock()
	var prev *v1.Pod
	key := objectKey{namespace: pod.Namespace, name: pod.Name, uid: pod.UID}
	prev = c.registeredPods[key]
	c.registeredPods[key] = pod
	// 为了尽量减少对 apiserver 的 configmap/secret 请求，
	// 只在首次为该 Pod 调用 RegisterPod 时增加引用。
	// 首次注册时一次性增加全部引用。
	if prev == nil {
		for name := range names {
			c.objectStore.AddReference(pod.Namespace, name, pod.UID)
		}
	} else {
		prevNames := c.getReferencedObjects(prev)
		// 为新增的引用增加计数。
		for name := range names {
			if !prevNames.Has(name) {
				c.objectStore.AddReference(pod.Namespace, name, pod.UID)
			}
		}
		// 为不再被引用的对象减少计数。
		for prevName := range prevNames {
			if !names.Has(prevName) {
				c.objectStore.DeleteReference(pod.Namespace, prevName, pod.UID)
			}
		}
	}
}

// UnregisterPod 移除 Pod 记录，并释放其引用的全部对象。
func (c *cacheBasedManager) UnregisterPod(pod *v1.Pod) {
	var prev *v1.Pod
	key := objectKey{namespace: pod.Namespace, name: pod.Name, uid: pod.UID}
	c.lock.Lock()
	defer c.lock.Unlock()
	prev = c.registeredPods[key]
	delete(c.registeredPods, key)
	// 只对已注册过的 Pod 释放引用，避免重复递减计数。
	if prev != nil {
		for name := range c.getReferencedObjects(prev) {
			c.objectStore.DeleteReference(prev.Namespace, name, prev.UID)
		}
	}
}

// NewCacheBasedManager 创建一个管理器，缓存已注册 Pod 所需的所有对象。
// 它实现以下逻辑：
//   - 每当 Pod 被创建或更新时，使其引用的所有对象的缓存版本失效
//   - 每次 GetObject() 调用都优先尝试从本地缓存获取；若不存在、已失效或过旧，
//     则从 apiserver 拉取并刷新缓存；否则直接返回缓存值
func NewCacheBasedManager(objectStore Store, getReferencedObjects func(*v1.Pod) sets.Set[string]) Manager {
	return &cacheBasedManager{
		objectStore:          objectStore,
		getReferencedObjects: getReferencedObjects,
		registeredPods:       make(map[objectKey]*v1.Pod),
	}
}
