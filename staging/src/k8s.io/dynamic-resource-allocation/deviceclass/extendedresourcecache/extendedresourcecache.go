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

package extendedresourcecache

import (
	"cmp"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
)

// ExtendedResourceCache 基于 informer 事件维护“扩展资源名 → DeviceClass”的全局缓存，
// 为此实现了 cache.ResourceEventHandler 接口，可注册为 DeviceClass 事件的处理器。
type ExtendedResourceCache struct {
	// logger 用于记录日志，handlers 是缓存处理后需要依次通知的附加事件处理器。
	logger   klog.Logger
	handlers []cache.ResourceEventHandler

	// mutex 保护下面两个映射的并发读写。
	mutex sync.RWMutex
	// resourceName2class 保存扩展资源名到 DeviceClass 的正向映射。
	resourceName2class map[v1.ResourceName]*resourceapi.DeviceClass
	// class2ResourceName 保存 DeviceClass 名称到显式扩展资源名的反向映射。
	class2ResourceName map[string]string
}

// 编译期断言：ExtendedResourceCache 必须实现 cache.ResourceEventHandler 接口。
var _ cache.ResourceEventHandler = &ExtendedResourceCache{}

// NewExtendedResourceCache 创建一个缓存实例并初始化两个映射；调用方需负责把它注册为 DeviceClass 事件处理器。
// 附加事件处理器可在此处传入，也可随后通过 AddEventHandler 注册。
func NewExtendedResourceCache(logger klog.Logger, handlers ...cache.ResourceEventHandler) *ExtendedResourceCache {
	cache := &ExtendedResourceCache{
		// 保存日志器与初始处理器列表，并初始化双向映射。
		logger:             logger,
		handlers:           handlers,
		resourceName2class: make(map[v1.ResourceName]*resourceapi.DeviceClass),
		class2ResourceName: make(map[string]string),
	}

	return cache
}

// AddEventHandler 追加一个在缓存处理完事件后被调用的处理器；多个处理器按注册顺序调用，
// 处理器内可调用 GetDeviceClass。该方法非线程安全，必须在把缓存注册到 informer 之前调用。
func (c *ExtendedResourceCache) AddEventHandler(handler cache.ResourceEventHandler) {
	c.handlers = append(c.handlers, handler)
}

// GetDeviceClass 按扩展资源名返回对应的 DeviceClass，找不到时返回 nil。
// 这是唯一可在 nil 缓存实例上调用的方法：nil 实例恒返回 nil。
func (c *ExtendedResourceCache) GetDeviceClass(resourceName v1.ResourceName) *resourceapi.DeviceClass {
	// nil 接收者保护，便于调用方无条件使用缓存实例。
	if c == nil {
		return nil
	}
	// 读操作加读锁，支持并发读取。
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.resourceName2class[resourceName]
}

// GetExtendedResource 按 DeviceClass 名称返回其显式扩展资源名，找不到（或接收者为 nil）时返回空字符串。
func (c *ExtendedResourceCache) GetExtendedResource(className string) string {
	if c == nil {
		return ""
	}
	// 读反向映射同样需要读锁。
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.class2ResourceName[className]
}

// OnAdd 处理 DeviceClass 新增事件：先同步两张映射，再按注册顺序通知附加处理器。
func (c *ExtendedResourceCache) OnAdd(obj interface{}, isInInitialList bool) {
	// 类型断言失败时记录错误并忽略该事件。
	deviceClass, ok := obj.(*resourceapi.DeviceClass)
	if !ok {
		utilruntime.HandleErrorWithLogger(c.logger, nil, "Expected DeviceClass", "actual", fmt.Sprintf("%T", obj))
		return
	}
	c.logger.V(5).Info("DeviceClass added", "deviceClass", klog.KObj(deviceClass))
	// 新增时无需清理旧映射，传入 nil 作为旧对象。
	c.updateResourceName2class(deviceClass, nil)
	c.updateClass2ResourceName(deviceClass)

	// 缓存更新完成后再通知附加处理器，保证其中可查到最新映射。
	for _, handler := range c.handlers {
		handler.OnAdd(obj, isInInitialList)
	}
}

// OnUpdate 处理 DeviceClass 更新事件：用新旧对象同步映射（旧对象用于清理已变更的映射），再通知附加处理器。
func (c *ExtendedResourceCache) OnUpdate(oldObj, newObj interface{}) {
	// 新旧对象都必须是 DeviceClass，否则记录错误并忽略。
	deviceClass, ok := newObj.(*resourceapi.DeviceClass)
	if !ok {
		utilruntime.HandleErrorWithLogger(c.logger, nil, "Expected DeviceClass", "actual", fmt.Sprintf("%T", newObj))
		return
	}
	oldDeviceClass, ok := oldObj.(*resourceapi.DeviceClass)
	if !ok {
		utilruntime.HandleErrorWithLogger(c.logger, nil, "Expected DeviceClass", "actual", fmt.Sprintf("%T", oldObj))
		return
	}
	c.logger.V(5).Info("DeviceClass updated", "deviceClass", klog.KObj(deviceClass))
	// 先按旧对象清理、再按新对象写入，保证映射变更原子生效。
	c.updateResourceName2class(deviceClass, oldDeviceClass)
	c.updateClass2ResourceName(deviceClass)

	for _, handler := range c.handlers {
		handler.OnUpdate(oldObj, newObj)
	}
}

// OnDelete 处理 DeviceClass 删除事件：先从两张映射中移除相关条目，再通知附加处理器。
func (c *ExtendedResourceCache) OnDelete(obj interface{}) {
	// informer 可能以墓碑对象交付删除事件，这里取出其中的真实对象。
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	deviceClass, ok := obj.(*resourceapi.DeviceClass)
	if !ok {
		utilruntime.HandleErrorWithLogger(c.logger, nil, "Expected DeviceClass", "actual", fmt.Sprintf("%T", obj))
		return
	}
	c.logger.V(5).Info("DeviceClass deleted", "deviceClass", klog.KObj(deviceClass))
	c.removeResourceName2class(deviceClass)
	c.removeClass2ResourceName(deviceClass)

	for _, handler := range c.handlers {
		handler.OnDelete(obj)
	}
}

// updateResourceName2class 更新“扩展资源名 → DeviceClass”映射：先清理旧对象对应的映射以应对扩展资源名变更，
// 再写入新映射。若多个类声明同一扩展资源名，则优先创建时间更新者，时间相同取名称较小者。
func (c *ExtendedResourceCache) updateResourceName2class(newDeviceClass, oldDeviceClass *resourceapi.DeviceClass) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 多个 DeviceClass 可能声明同一个显式扩展资源名（迁移期常见）：优先创建时间较新者，名称作平局判据。
	// 隐式映射 deviceclass.resource.kubernetes.io/<类名> 天然唯一，且校验保证不会被其他类用作显式名，
	// 因此无需为隐式映射做同样的冲突检查。
	var classWithSameExtendedResourceName *resourceapi.DeviceClass
	if newDeviceClass.Spec.ExtendedResourceName != nil {
		classWithSameExtendedResourceName = c.resourceName2class[v1.ResourceName(*newDeviceClass.Spec.ExtendedResourceName)]
	}
	if classWithSameExtendedResourceName != nil {
		// 按创建时间比较：新类更旧或同名同时间且名称不更小时，放弃本次更新。
		switch cmp.Compare(newDeviceClass.CreationTimestamp.UnixNano(), classWithSameExtendedResourceName.CreationTimestamp.UnixNano()) {
		case -1:
			return
		case 0:
			if newDeviceClass.Name >= classWithSameExtendedResourceName.Name {
				return
			}
		}
	}

	// 删除旧对象的隐式映射与旧的显式映射，避免残留过期条目。
	if oldDeviceClass != nil {
		delete(c.resourceName2class, v1.ResourceName(resourceapi.ResourceDeviceClassPrefix+oldDeviceClass.Name))
		if oldDeviceClass.Spec.ExtendedResourceName != nil {
			delete(c.resourceName2class, v1.ResourceName(*oldDeviceClass.Spec.ExtendedResourceName))
		}
	}

	// 写入新对象的显式映射。
	if newDeviceClass.Spec.ExtendedResourceName != nil {
		c.resourceName2class[v1.ResourceName(*newDeviceClass.Spec.ExtendedResourceName)] = newDeviceClass
		c.logger.V(5).Info("Updated extended resource cache for explicit mapping",
			"extendedResource", *newDeviceClass.Spec.ExtendedResourceName,
			"deviceClass", newDeviceClass.Name)
	}
	// 无论是否声明显式名，都要写入默认的隐式映射。
	defaultResourceName := v1.ResourceName(resourceapi.ResourceDeviceClassPrefix + newDeviceClass.Name)
	c.resourceName2class[defaultResourceName] = newDeviceClass
	c.logger.V(5).Info("Updated extended resource cache for default mapping",
		"extendedResource", defaultResourceName,
		"deviceClass", newDeviceClass.Name)
}

// updateClass2ResourceName 更新“DeviceClass 名 → 扩展资源名”反向映射；若类不再声明扩展资源名，则删除对应条目。
func (c *ExtendedResourceCache) updateClass2ResourceName(deviceClass *resourceapi.DeviceClass) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 未声明显式扩展资源名时清理旧映射。
	if deviceClass.Spec.ExtendedResourceName == nil {
		delete(c.class2ResourceName, deviceClass.Name)
		return
	}

	c.class2ResourceName[deviceClass.Name] = *deviceClass.Spec.ExtendedResourceName
	c.logger.V(5).Info("Updated device class mapping", "deviceClass", deviceClass.Name, "extendedResource", *deviceClass.Spec.ExtendedResourceName)
}

// removeResourceName2class 从正向映射中删除该 DeviceClass 的隐式映射与显式映射；
// 由于传入对象上的扩展资源名可能已过期，删除时不依赖它遍历，而是按名称直接删除。
func (c *ExtendedResourceCache) removeResourceName2class(deviceClass *resourceapi.DeviceClass) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.resourceName2class, v1.ResourceName(resourceapi.ResourceDeviceClassPrefix+deviceClass.Name))
	// 若对象上仍带有扩展资源名，也一并删除该显式映射。
	if deviceClass.Spec.ExtendedResourceName != nil {
		delete(c.resourceName2class, v1.ResourceName(*deviceClass.Spec.ExtendedResourceName))
	}
	c.logger.V(5).Info("Removed extended resource from cache",
		"deviceClass", deviceClass.Name)
}

// removeClass2ResourceName 从反向映射中删除指定 DeviceClass 的条目。
func (c *ExtendedResourceCache) removeClass2ResourceName(deviceClass *resourceapi.DeviceClass) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.class2ResourceName, deviceClass.Name)
	c.logger.V(5).Info("Removed device class", "deviceClass", deviceClass.Name)
}
