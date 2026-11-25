/*
Copyright The Kubernetes Authors.

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

// Package cache 实现客户端缓存：Reflector 从服务器 watch 并把对象写入 Store，
// 上层控制器从本地缓存读取，避免频繁访问 API Server。
package cache

import (
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// informerNameRegistry 跟踪所有已注册的 InformerName 实例以检测重名。
// 名称在进程内必须全局唯一。
var informerNameRegistry = struct {
	sync.Mutex
	names map[string]*InformerName
}{
	names: make(map[string]*InformerName),
}

// InformerName 表示用于指标的具名 informer 身份。它在启动时创建一次
// （例如在 cmd/kube-controller-manager 中）并传给 SharedInformerFactory，
// 名称在进程内必须全局唯一。
//
// InformerName 记录该名称下注册过哪些 GVR：informer 申请某个 name+GVR 组合时，
// 先到者获得指标发布资格，后续重复注册同一 GVR 会被静默关闭指标。
type InformerName struct {
	name string
	// lock 保护 gvrs map 的修改
	lock sync.Mutex
	// 调用 Release() 后 reserved 会被置为 false
	reserved *atomic.Bool
	// gvrs 把每个已注册的 GVR 映射到其原子 bool，供 Reserved() 做无锁检查
	gvrs map[schema.GroupVersionResource]*atomic.Bool
}

// NewInformerName 用给定名称创建 InformerName。名称在进程内必须全局唯一，
// 检测到重名时返回错误。
//
// informer 名称不再需要时（例如进程退出时），调用方应调用 Release()
// 以便该名称可以被复用。
func NewInformerName(name string) (*InformerName, error) {
	if name == "" {
		return nil, fmt.Errorf("informer name cannot be empty")
	}

	informerNameRegistry.Lock()
	defer informerNameRegistry.Unlock()

	if existing, ok := informerNameRegistry.names[name]; ok {
		// 检查已有条目是否仍被占用
		// 仍被持有时拒绝重复注册；已 Release 的条目允许复用同名。
		if existing.reserved.Load() {
			return nil, fmt.Errorf("informer name %q is already registered", name)
		}
		// 前一个已释放，可以复用该名称
		delete(informerNameRegistry.names, name)
	}

	reserved := &atomic.Bool{}
	reserved.Store(true)

	n := &InformerName{
		name:     name,
		reserved: reserved,
		gvrs:     make(map[schema.GroupVersionResource]*atomic.Bool),
	}

	informerNameRegistry.names[name] = n
	return n, nil
}

// WithResource 在该 InformerName 下注册一个 GVR，并返回可传给
// FIFO/SharedIndexInformer 的 InformerNameAndResource。
//
// 若该 GVR 是首次在此名称下注册，返回值的 Reserved() 为 true；
// 若已注册过，返回值的 Reserved() 为 false，以避免重复发布指标。
func (n *InformerName) WithResource(gvr schema.GroupVersionResource) InformerNameAndResource {
	// nil 接收者表示调用方未配置名称，返回零值即可，Reserved() 恒为 false。
	if n == nil {
		return InformerNameAndResource{gvr: gvr}
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	retval := InformerNameAndResource{name: n.name, gvr: gvr, reserved: &atomic.Bool{}}
	if n.reserved.Load() {
		if _, gvrExists := n.gvrs[gvr]; !gvrExists {
			// 首个注册者获得指标发布权，后续同名同 GVR 的注册静默关闭指标。
			retval.reserved.Store(true)
			n.gvrs[gvr] = retval.reserved
		} else {
			// WithResource 由生成的 informer 代码调用，改成上下文日志需要
			// 改动全部生成 API，收益不大。
			klog.TODO().Error(nil, "Duplicate informer registration - metrics will not be published", "informerName", n.name, "group", gvr.Group, "version", gvr.Version, "resource", gvr.Resource)
		}
	}
	return retval
}

// Release 将该 InformerName 标记为不再使用：此后由它创建的所有
// InformerNameAndResource 的 Reserved() 都会返回 false，
// 名称也可以被后续 NewInformerName 调用复用。
func (n *InformerName) Release() {
	if n == nil {
		return
	}

	n.lock.Lock()
	defer n.lock.Unlock()

	// 逐个翻转 GVR 级别的标志，使 WithResource() 已返回的
	// InformerNameAndResource 的 Reserved() 变为 false。
	// 这些实例持有同一批原子 bool 的指针，必须在清空 map 之前翻转。
	//
	// 已发出的 InformerNameAndResource 持有这些 bool 的指针，
	// 必须在清空 map 之前逐个置 false，否则它们仍会发布指标。
	for _, reserved := range n.gvrs {
		reserved.Store(false)
	}

	// 清空 map
	n.gvrs = make(map[schema.GroupVersionResource]*atomic.Bool)

	// 翻转主 reserved 标志
	n.reserved.Store(false)

	// 从全局注册表移除
	informerNameRegistry.Lock()
	defer informerNameRegistry.Unlock()
	delete(informerNameRegistry.names, n.name)
}

// Name 返回该 InformerName 的名称。
func (n *InformerName) Name() string {
	// 容忍 nil，让未配置名称的调用方无需额外判空。
	if n == nil {
		return ""
	}
	return n.name
}

// InformerNameAndResource 表示一个同时具有名称和 GVR 的具体 informer 身份，
// 传给 FIFO 与 SharedIndexInformer 用于指标。
//
// Reserved() 提供无锁检查，决定是否应发布指标；由于每次队列操作都会调用它，
// 必须足够快。
type InformerNameAndResource struct {
	name     string
	gvr      schema.GroupVersionResource
	reserved *atomic.Bool
}

// Reserved 在该 informer 身份获得指标发布资格时返回 true。
// 内部是无锁原子读取，可安全用于热路径。
//
// 以下情况返回 false：
// - InformerNameAndResource 为零值（未配置名称）
// - 父 InformerName 已被 Release
// - 这是一次重复的 GVR 注册
func (n InformerNameAndResource) Reserved() bool {
	// 零值实例没有 reserved 指针，等价于“未配置名称”。
	if n.reserved == nil {
		return false
	}
	return n.reserved.Load()
}

// Name 返回 informer 名称。
func (n InformerNameAndResource) Name() string {
	return n.name
}

// GroupVersionResource 返回该 informer 身份对应的 GVR。
func (n InformerNameAndResource) GroupVersionResource() schema.GroupVersionResource {
	return n.gvr
}

// ResetInformerNamesForTesting 清空 informer 名称注册表，仅出于测试目的导出。
func ResetInformerNamesForTesting() {
	// 先在锁内拷贝快照，再逐个 Release：Release 自身也需要获取全局注册表锁。
	informerNameRegistry.Lock()
	names := make([]*InformerName, 0, len(informerNameRegistry.names))
	for _, name := range informerNameRegistry.names {
		names = append(names, name)
	}
	informerNameRegistry.Unlock()

	for _, name := range names {
		name.Release()
	}
}
