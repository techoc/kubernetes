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

// Package metadatainformer 为仅需对象元数据（PartialObjectMetadata）的场景提供共享
// informer 工厂。
//
// 相比完整的类型化 informer，这里只拉取 metadata，显著降低带宽与内存占用；代价是
// 缓存中拿不到 spec/status，只适合做列表展示、依赖关系等元数据驱动的逻辑。
package metadatainformer

import (
	"context"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/metadata/metadatalister"
	"k8s.io/client-go/tools/cache"
)

// SharedInformerOption defines the functional option type for metadataSharedInformerFactory.
type SharedInformerOption func(*metadataSharedInformerFactory) *metadataSharedInformerFactory

// WithTransform sets a transform on all informers.
//
// 中文说明：transform 在对象进入 indexer 前执行，可用于去掉不需要的字段以节省内存。
// 它必须返回同类型对象且不得修改共享缓存中的原对象，否则会破坏 informer 的一致性假设。
func WithTransform(transform cache.TransformFunc) SharedInformerOption {
	return func(factory *metadataSharedInformerFactory) *metadataSharedInformerFactory {
		factory.transform = transform
		return factory
	}
}

// NewSharedInformerFactory constructs a new instance of metadataSharedInformerFactory for all namespaces.
func NewSharedInformerFactory(client metadata.Interface, defaultResync time.Duration) SharedInformerFactory {
	return NewFilteredSharedInformerFactory(client, defaultResync, metav1.NamespaceAll, nil)
}

// NewFilteredSharedInformerFactory constructs a new instance of metadataSharedInformerFactory.
// Listers obtained via this factory will be subject to the same filters as specified here.
//
// 中文说明：namespace 与 tweakListOptions 会作用于该工厂创建的所有 informer，
// 因此 lister 看到的视图与 List/Watch 请求的过滤条件保持一致。
func NewFilteredSharedInformerFactory(client metadata.Interface, defaultResync time.Duration, namespace string, tweakListOptions TweakListOptionsFunc) SharedInformerFactory {
	return &metadataSharedInformerFactory{
		client:           client,
		defaultResync:    defaultResync,
		namespace:        namespace,
		informers:        map[schema.GroupVersionResource]informers.GenericInformer{},
		startedInformers: make(map[schema.GroupVersionResource]bool),
		tweakListOptions: tweakListOptions,
	}
}

// NewSharedInformerFactoryWithOptions constructs a new instance of metadataSharedInformerFactory with additional options.
//
// 中文：默认监听所有命名空间，options 在构造后依次应用，可覆盖该默认值或注入 transform。
func NewSharedInformerFactoryWithOptions(client metadata.Interface, defaultResync time.Duration, options ...SharedInformerOption) SharedInformerFactory {
	factory := &metadataSharedInformerFactory{
		client:           client,
		namespace:        v1.NamespaceAll,
		defaultResync:    defaultResync,
		informers:        map[schema.GroupVersionResource]informers.GenericInformer{},
		startedInformers: make(map[schema.GroupVersionResource]bool),
	}

	// Apply all options
	for _, opt := range options {
		factory = opt(factory)
	}

	return factory
}

// metadataSharedInformerFactory 是 SharedInformerFactory 的实现，按 GVR 缓存 informer。
type metadataSharedInformerFactory struct {
	client        metadata.Interface
	defaultResync time.Duration
	namespace     string
	transform     cache.TransformFunc

	lock      sync.Mutex
	informers map[schema.GroupVersionResource]informers.GenericInformer
	// startedInformers is used for tracking which informers have been started.
	// This allows Start() to be called multiple times safely.
	// 中文：Start 幂等性依赖该标记；同一 informer 只会启动一次。
	startedInformers map[schema.GroupVersionResource]bool
	tweakListOptions TweakListOptionsFunc
	// wg tracks how many goroutines were started.
	wg sync.WaitGroup
	// shuttingDown is true when Shutdown has been called. It may still be running
	// because it needs to wait for goroutines.
	// 中文：置位后 Start 不再启动新 informer，但 Shutdown 仍可能阻塞在等待已启动者退出。
	shuttingDown bool
}

var _ SharedInformerFactory = &metadataSharedInformerFactory{}

// ForResource 返回指定 GVR 的 informer，不存在则创建并缓存。
//
// 中文说明：整个查找与创建过程持锁，保证同一 GVR 在并发调用下只创建一个 informer；
// 缓存 key 就是 GVR，工厂的 namespace/tweak 已固化在 informer 内，调用方无需再传。
func (f *metadataSharedInformerFactory) ForResource(gvr schema.GroupVersionResource) informers.GenericInformer {
	f.lock.Lock()
	defer f.lock.Unlock()

	key := gvr
	informer, exists := f.informers[key]
	if exists {
		return informer
	}

	// 中文：内置 NamespaceIndex，使 lister 支持按命名空间高效查询。
	informer = NewFilteredMetadataInformer(f.client, gvr, f.namespace, f.defaultResync, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
	informer.Informer().SetTransform(f.transform)
	f.informers[key] = informer

	return informer
}

// Start is a legacy wrapper that initializes all requested informers.
//
// 中文：把 stop channel 适配为 context，仅用于兼容旧调用方。
//
//logcheck:context // StartWithContext should be used instead of Start in code which supports contextual logging.
func (f *metadataSharedInformerFactory) Start(stopCh <-chan struct{}) {
	f.StartWithContext(wait.ContextForChannel(stopCh))
}

// StartWithContext initializes all requested informers.
//
// 中文说明：为每个尚未启动的 informer 各起一个 goroutine，并立即更新 startedInformers，
// 因此重复调用 StartWithContext 不会重复启动。持锁时间仅覆盖遍历与标记，实际运行在锁外。
func (f *metadataSharedInformerFactory) StartWithContext(ctx context.Context) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.shuttingDown {
		return
	}

	for informerType, informer := range f.informers {
		if !f.startedInformers[informerType] {
			f.wg.Add(1)
			// We need a new variable in each loop iteration,
			// otherwise the goroutine would use the loop variable
			// and that keeps changing.
			// 中文：与 Go 1.22 之前的循环变量语义兼容，显式捕获本次迭代的 informer。
			informer := informer.Informer()
			go func() {
				defer f.wg.Done()
				informer.RunWithContext(ctx)
			}()
			f.startedInformers[informerType] = true
		}
	}
}

// WaitForCacheSync waits for all started informers' cache were synced.
//
// 中文说明：先在锁内快照已启动的 informer，随后在锁外逐个等待同步，避免长时间持锁
// 阻塞其它调用方创建或启动 informer。未启动的 informer 不出现在结果中。
func (f *metadataSharedInformerFactory) WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool {
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

// Shutdown 停止接受新的启动请求，并等待已启动的 informer goroutine 退出。
//
// 中文：defer 注册顺序保证先释放锁再 wg.Wait，否则等待期间持锁会阻塞其它调用方；
// 置位 shuttingDown 后 StartWithContext 直接返回，新的 informer 不会再被启动。
func (f *metadataSharedInformerFactory) Shutdown() {
	// Will return immediately if there is nothing to wait for.
	defer f.wg.Wait()

	f.lock.Lock()
	defer f.lock.Unlock()
	f.shuttingDown = true
}

// NewFilteredMetadataInformer constructs a new informer for a metadata type.
//
// 中文说明：以 PartialObjectMetadata 作为缓存对象类型，只保留 metadata 字段。
// 每个 List/Watch 回调都会应用 tweakListOptions，确保工厂级别的过滤语义一致；
// 同时提供带 context 与不带 context 的版本，后者仅为兼容旧 reflector 路径。
func NewFilteredMetadataInformer(client metadata.Interface, gvr schema.GroupVersionResource, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions TweakListOptionsFunc) informers.GenericInformer {
	return &metadataInformer{
		gvr: gvr,
		informer: cache.NewSharedIndexInformer(
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
			&metav1.PartialObjectMetadata{},
			resyncPeriod,
			indexers,
		),
	}
}

// metadataInformer 把通用 informer 适配为 informers.GenericInformer。
type metadataInformer struct {
	informer cache.SharedIndexInformer
	gvr      schema.GroupVersionResource
}

var _ informers.GenericInformer = &metadataInformer{}

// Informer 返回底层的 SharedIndexInformer。
func (d *metadataInformer) Informer() cache.SharedIndexInformer {
	return d.informer
}

// Lister 返回基于该 informer 索引器的 lister。
//
// 中文：RuntimeObjectShim 把 PartialObjectMetadata 适配为通用 runtime.Object 视图，
// 使调用方无需感知元数据专用的 lister 类型。
func (d *metadataInformer) Lister() cache.GenericLister {
	return metadatalister.NewRuntimeObjectShim(metadatalister.New(d.informer.GetIndexer(), d.gvr))
}
