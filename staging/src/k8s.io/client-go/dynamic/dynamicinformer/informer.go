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

package dynamicinformer

import (
	"context"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamiclister"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// NewDynamicSharedInformerFactory 创建面向所有命名空间的动态共享 informer 工厂。
//
// 等价于以 NamespaceAll、无 tweak 函数调用 NewFilteredDynamicSharedInformerFactory；
// 工厂本身不启动任何 goroutine，需要随后调用 Start。
func NewDynamicSharedInformerFactory(client dynamic.Interface, defaultResync time.Duration) DynamicSharedInformerFactory {
	return NewFilteredDynamicSharedInformerFactory(client, defaultResync, metav1.NamespaceAll, nil)
}

// NewFilteredDynamicSharedInformerFactory 创建带过滤条件的工厂；此后由它创建的
// lister 都会遵守这里的 namespace 与 ListOptions 过滤条件。
//
// namespace 和 tweakListOptions 会固化进此后创建的每个 informer，因此 List/Watch
// 与服务端过滤条件保持一致；tweakListOptions 每次 List/Watch 调用都会执行一次，
// 必须是无副作用、可并发调用的。
func NewFilteredDynamicSharedInformerFactory(client dynamic.Interface, defaultResync time.Duration, namespace string, tweakListOptions TweakListOptionsFunc) DynamicSharedInformerFactory {
	return &dynamicSharedInformerFactory{
		client:           client,
		defaultResync:    defaultResync,
		namespace:        namespace,
		informers:        map[schema.GroupVersionResource]informers.GenericInformer{},
		startedInformers: make(map[schema.GroupVersionResource]bool),
		tweakListOptions: tweakListOptions,
	}
}

// dynamicSharedInformerFactory 按 GroupVersionResource 惰性创建并共享 informer。
//
// 同一 GVR 只构造一次 informer，所有调用方共享同一个缓存和事件分发器，
// 这样可以避免为同一资源重复建立 List/Watch 连接。
type dynamicSharedInformerFactory struct {
	client        dynamic.Interface
	defaultResync time.Duration
	namespace     string

	// lock 保护 informers、startedInformers 和 shuttingDown，也保证 Start 与
	// ForResource 并发时不会重复启动同一个 informer。
	lock      sync.Mutex
	informers map[schema.GroupVersionResource]informers.GenericInformer
	// startedInformers 记录已启动的 informer，使 Start() 可被安全地多次调用。
	startedInformers map[schema.GroupVersionResource]bool
	tweakListOptions TweakListOptionsFunc

	// wg 跟踪已启动的 goroutine 数量，供 Shutdown 等待。
	wg sync.WaitGroup
	// shuttingDown 在 Shutdown 调用后为 true；此时它可能仍在运行，因为要等待
	// goroutine 退出。
	//
	// 置位后 Start 不再启动新的 informer，避免 Shutdown 等待期间又拉起 goroutine。
	shuttingDown bool
}

var _ DynamicSharedInformerFactory = &dynamicSharedInformerFactory{}

// ForResource 返回指定 GVR 的 GenericInformer，必要时创建并缓存。
//
// 创建过程在锁内完成，因此并发调用同一 GVR 也只会得到一个实例；
// 这里只构造对象，真正运行由 Start 负责。
func (f *dynamicSharedInformerFactory) ForResource(gvr schema.GroupVersionResource) informers.GenericInformer {
	f.lock.Lock()
	defer f.lock.Unlock()

	key := gvr
	informer, exists := f.informers[key]
	if exists {
		return informer
	}

	informer = NewFilteredDynamicInformer(f.client, gvr, f.namespace, f.defaultResync, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
	f.informers[key] = informer

	return informer
}

// Start 初始化所有被请求过的 informer。
//
// Start 可被多次调用：已启动的 informer 会被跳过，因此它只负责“补齐”尚未
// 启动的实例。调用方通常先 ForResource 注册所需资源，再统一 Start。
func (f *dynamicSharedInformerFactory) Start(stopCh <-chan struct{}) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// 已进入关闭流程，不再启动新的 goroutine，否则 Shutdown 的等待会落空。
	if f.shuttingDown {
		return
	}

	for informerType, informer := range f.informers {
		if !f.startedInformers[informerType] {
			f.wg.Add(1)
			// 每次循环都要声明新变量，否则 goroutine 会捕获不断变化的循环变量。
			//
			// 显式复制一份接口值，确保闭包捕获的是当前 informer。
			informer := informer.Informer()
			go func() {
				defer f.wg.Done()
				informer.Run(stopCh)
			}()
			f.startedInformers[informerType] = true
		}
	}
}

// WaitForCacheSync 等待所有已启动 informer 的缓存完成同步。
//
// 只等待已经由 Start 启动的 informer；未启动的实例没有活动的 Reflector，
// 永远无法达到 synced。返回 map 便于调用方按 GVR 判断具体哪个缓存没同步成功。
func (f *dynamicSharedInformerFactory) WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool {
	// 先在锁内取出已启动 informer 的快照，再在锁外等待：
	// HasSynced 可能阻塞到 Reflector 处理完初始列表，不能持锁等待。
	informers := func() map[schema.GroupVersionResource]cache.SharedIndexInformer {
		f.lock.Lock()
		defer f.lock.Unlock()

		informers := map[schema.GroupVersionResource]cache.SharedIndexInformer{}
		for informerType, informer := range f.informers {
			if f.startedInformers[informerType] {
				informers[informerType] = informer.Informer()
			}
		}
		return informers
	}()

	res := map[schema.GroupVersionResource]bool{}
	for informType, informer := range informers {
		res[informType] = cache.WaitForCacheSync(stopCh, informer.HasSynced)
	}
	return res
}

// Shutdown 标记工厂不再接受新的启动请求，并等待所有已启动的 informer 退出。
//
// defer wg.Wait() 先于解锁执行，但 Wait 本身不持锁；标记 shuttingDown 后
// Start 会立即返回，因此不会再有新的 goroutine 被加入等待组。
func (f *dynamicSharedInformerFactory) Shutdown() {
	// 没有 goroutine 需要等待时立即返回。
	defer f.wg.Wait()

	f.lock.Lock()
	defer f.lock.Unlock()
	f.shuttingDown = true
}

// NewFilteredDynamicInformer 为动态类型创建新的 informer。
//
// 返回的 informer 以 *unstructured.Unstructured 作为对象类型，因此 List/Watch
// 得到的任意资源都能进入同一个缓存，无需为每种类型生成专门的 informer。
func NewFilteredDynamicInformer(client dynamic.Interface, gvr schema.GroupVersionResource, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions TweakListOptionsFunc) informers.GenericInformer {
	return &dynamicInformer{
		gvr: gvr,
		informer: cache.NewSharedIndexInformerWithOptions(
			// ToListWatcherWithWatchListSemantics 让 reflector 能识别 dynamic client
			// 是否支持 WatchList 语义，从而决定是否走流式初始列表。
			cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return client.Resource(gvr).Namespace(namespace).List(context.Background(), options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return client.Resource(gvr).Namespace(namespace).Watch(context.Background(), options)
				},
				// 带 context 的版本供支持上下文取消的 reflector 使用；
				// 上面的旧函数保留以兼容老接口。
				ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return client.Resource(gvr).Namespace(namespace).List(ctx, options)
				},
				WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
					if tweakListOptions != nil {
						tweakListOptions(&options)
					}
					return client.Resource(gvr).Namespace(namespace).Watch(ctx, options)
				},
			}, client),
			&unstructured.Unstructured{},
			cache.SharedIndexInformerOptions{
				ResyncPeriod: resyncPeriod,
				Indexers:     indexers,
				// 资源描述用于日志和指标，便于区分不同 GVR 的 informer。
				ObjectDescription: gvr.String(),
			},
		),
	}
}

// dynamicInformer 把非类型化的 SharedIndexInformer 适配为 GenericInformer。
type dynamicInformer struct {
	informer cache.SharedIndexInformer
	gvr      schema.GroupVersionResource
}

var _ informers.GenericInformer = &dynamicInformer{}

// Informer 返回底层共享 informer；多次调用返回同一实例。
func (d *dynamicInformer) Informer() cache.SharedIndexInformer {
	return d.informer
}

// Lister 返回读取该 informer 缓存的 lister。
//
// RuntimeObjectShim 把 dynamiclister 返回的 *unstructured.Unstructured 适配成
// 任意 runtime.Object，保证 GenericLister 接口的通用性；lister 直接读索引器，
// 不发起服务端请求。
func (d *dynamicInformer) Lister() cache.GenericLister {
	return dynamiclister.NewRuntimeObjectShim(dynamiclister.New(d.informer.GetIndexer(), d.gvr))
}
