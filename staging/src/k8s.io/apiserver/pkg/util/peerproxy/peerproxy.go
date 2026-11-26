/*
Copyright 2023 The Kubernetes Authors.

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

package peerproxy

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apiserver/pkg/reconcilers"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/transport"
	"k8s.io/client-go/util/workqueue"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	coordinationv1informers "k8s.io/client-go/informers/coordination/v1"
)

const (
	// localDiscoveryRefreshInterval 是本地发现缓存的刷新间隔。
	// 该缓存只用于混版本代理的路由决策（shouldServeLocally），不用于提供发现响应。
	// 定期刷新保证尽可能在本机处理请求。若不刷新，
	// 过时的缓存可能把请求代理给对端，即便本机已能处理，造成
	// 不必要的网络跳转。升级期间新内置 API 在本机上线时尤其重要。
	//
	localDiscoveryRefreshInterval = 30 * time.Minute
	// defaultExclusionGracePeriod 是 CRD 与聚合 API 删除 GV 后，
	// 将其从排除集移除前的默认等待时长。
	//
	// 留出时间让所有对端 apiserver 也能观察到
	// 被删除的 CRD 或聚合 API，然后本机才停止排除它。
	//
	//
	// 宽限期的意义：各 apiserver 观察 CRD/APIService 删除的时间点不一致，
	// 若立即取消排除，可能把请求代理到一个仍认为该 GV 存在的对端。
	defaultExclusionGracePeriod = 5 * time.Minute
)

// Interface 定义混版本代理过滤器与底层系统的交互方式。
//
// 混版本代理的目标：客户端请求的版本本机不支持、但某个对端支持时，把请求转发给对端，
// 从而在滚动升级期间保持 API 可用。
type Interface interface {
	WrapHandler(handler http.Handler) http.Handler
	WaitForCacheSync(stopCh <-chan struct{}) error
	HasFinishedSync() bool
	RunLocalDiscoveryCacheSync(stopCh <-chan struct{}) error
	RunPeerDiscoveryCacheSync(ctx context.Context, workers int)
	GetPeerResources() map[string][]apidiscoveryv2.APIGroupDiscovery
	RegisterCacheInvalidationCallback(cb func())

	// RegisterCRDInformerHandlers 在 CRD informer 上注册事件处理，跟踪哪些 GroupVersion
	// 由 CRD 在本机提供。CRD 创建或更新时把 GV 加入排除集；删除时在宽限期内继续排除，
	// 让对端有时间观察到删除。extractor 函数
	// 从 CRD 对象中提取 GroupVersion。
	//
	//
	// 排除是必要的：本机 CRD 删除时不会刷新对端发现数据。若不排除，已删除的 GV 可能仍出现在
	// 缓存的对端发现数据中，导致请求被错误路由到本地已不存在的 GV。因此从一开始
	// 就把 CRD GV 从对端发现中排除，只依赖本机 apiserver 对 CRD 的视图
	// 在对端聚合发现中提供它。
	//
	// 在对端聚合发现中提供它。
	RegisterCRDInformerHandlers(crdInformer cache.SharedIndexInformer, extractor GVExtractor) error

	// RegisterAPIServiceInformerHandlers 在 APIService informer 上注册事件处理，
	// 跟踪哪些 GroupVersion 由聚合 APIService 在本机提供。APIService 创建或更新时把 GV 加入排除集，
	// 删除时在宽限期内继续排除。
	//
	//
	// 排除是必要的：本机聚合 APIService 删除时不会刷新对端发现数据。若不排除，已删除的 GV 可能仍出现在
	// 缓存的对端发现数据中，导致请求被错误路由到对端。因此从一开始就把聚合 APIService 的 GV
	// 从对端发现中排除，只依赖本机 apiserver 的视图
	// 在对端聚合发现中提供它们。
	//
	// peer-aggregated discovery.
	RegisterAPIServiceInformerHandlers(apiServiceInformer cache.SharedIndexInformer, extractor GVExtractor) error

	// RunPeerDiscoveryActiveGVTracker 启动一个 worker，处理 CRD/APIService informer 事件，
	// 重建当前活跃提供的 GroupVersion 集合。CRD 或 APIService 新增/更新时触发该 worker 并更新排除集；
	// GV 删除时会自动安排一次延迟同步，
	// 在宽限期后清理排除集中过期的 GV。
	//
	RunPeerDiscoveryActiveGVTracker(ctx context.Context)

	// RunPeerDiscoveryRefilter 启动一个 worker，在排除集变化时重新对缓存的对端发现数据应用过滤。
	// 这保证已缓存的对端发现响应会立即排除新增/更新的本地 GV，
	// 而无需等待下一次 peer lease 事件触发发现缓存刷新。
	//
	// 发现缓存刷新。
	RunPeerDiscoveryRefilter(ctx context.Context)
}

// New 创建一个实现未知版本代理的新实例。
// 该方法服务于 alpha 特性 UnknownVersionInteroperabilityProxy，
// 未来可能修改。
//
// identityLeaseLabelSelector 用于从 Lease 中筛选出 apiserver 身份租约；
// loopbackClientConfig 用于访问本机 discovery（NoPeerDiscovery=true，避免代理回环）。
func NewPeerProxyHandler(
	serverId string,
	identityLeaseLabelSelector string,
	leaseInformer coordinationv1informers.LeaseInformer,
	reconciler reconcilers.PeerEndpointLeaseReconciler,
	ser runtime.NegotiatedSerializer,
	loopbackClientConfig *rest.Config,
	proxyClientConfig *transport.Config,
) (*peerProxyHandler, error) {
	h := &peerProxyHandler{
		name:                             "PeerProxyHandler",
		serverID:                         serverId,
		reconciler:                       reconciler,
		serializer:                       ser,
		localDiscoveryInfoCache:          atomic.Value{},
		localDiscoveryCacheTicker:        time.NewTicker(localDiscoveryRefreshInterval),
		localDiscoveryInfoCachePopulated: make(chan struct{}),
		rawPeerDiscoveryCache:            atomic.Value{},
		peerLeaseQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: peerDiscoveryControllerName,
			}),
		apiserverIdentityInformer: leaseInformer,
	}

	h.gvExclusionManager = NewGVExclusionManager(
		defaultExclusionGracePeriod,
		&h.rawPeerDiscoveryCache,
		&h.cacheInvalidationCallback,
	)

	if parts := strings.Split(identityLeaseLabelSelector, "="); len(parts) != 2 {
		return nil, fmt.Errorf("invalid identityLeaseLabelSelector provided, must be of the form key=value, received: %v", identityLeaseLabelSelector)
	}
	selector, err := labels.Parse(identityLeaseLabelSelector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse label selector: %w", err)
	}
	h.identityLeaseLabelSelector = selector

	// 单独的 scheme 只用于编解码 apidiscovery 类型，避免污染主 scheme。
	discoveryScheme := runtime.NewScheme()
	utilruntime.Must(apidiscoveryv2.AddToScheme(discoveryScheme))
	h.discoverySerializer = serializer.NewCodecFactory(discoveryScheme)

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(loopbackClientConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating discovery client: %w", err)
	}

	// 始终使用本地发现来获取本机资源视图。
	//
	// 关键：必须关闭 peer discovery，否则本地视图会包含对端资源，
	// 导致 shouldServeLocally 判断错误、形成代理回环。
	discoveryClient.NoPeerDiscovery = true
	h.discoveryClient = discoveryClient
	h.localDiscoveryInfoCache.Store(map[schema.GroupVersionResource]bool{})
	h.rawPeerDiscoveryCache.Store(map[string]PeerDiscoveryCacheEntry{})

	proxyTransport, err := transport.New(proxyClientConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create proxy transport: %w", err)
	}
	h.proxyTransport = proxyTransport

	// 监听 apiserver 身份 Lease：新增/删除/持有者变化都意味着对端集合变了，
	// 需要重新拉取其 discovery 信息。
	peerDiscoveryRegistration, err := h.apiserverIdentityInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if lease, ok := h.isValidPeerIdentityLease(obj); ok {
				h.enqueueLease(lease)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldLease, oldLeaseOk := h.isValidPeerIdentityLease(oldObj)
			newLease, newLeaseOk := h.isValidPeerIdentityLease(newObj)
			if oldLeaseOk && newLeaseOk &&
				oldLease.Name == newLease.Name && *oldLease.Spec.HolderIdentity != *newLease.Spec.HolderIdentity {
				h.enqueueLease(newLease)
			}
		},
		DeleteFunc: func(obj interface{}) {
			if lease, ok := h.isValidPeerIdentityLease(obj); ok {
				h.enqueueLease(lease)
			}
		},
	})
	if err != nil {
		return nil, err
	}

	h.leaseRegistration = peerDiscoveryRegistration
	return h, nil
}

// RegisterCRDInformerHandlers 为 CRD informer 注册事件处理。
func (h *peerProxyHandler) RegisterCRDInformerHandlers(crdInformer cache.SharedIndexInformer, extractor GVExtractor) error {
	// gvExclusionManager 为 nil 表示未启用排除逻辑，此时静默跳过而不是报错。
	if h.gvExclusionManager != nil {
		return h.gvExclusionManager.RegisterCRDInformerHandlers(crdInformer, extractor)
	}
	return nil
}

// RegisterAPIServiceInformerHandlers 为 APIService informer 注册事件处理。
func (h *peerProxyHandler) RegisterAPIServiceInformerHandlers(apiServiceInformer cache.SharedIndexInformer, extractor GVExtractor) error {
	if h.gvExclusionManager != nil {
		return h.gvExclusionManager.RegisterAPIServiceInformerHandlers(apiServiceInformer, extractor)
	}
	return nil
}

// RunPeerDiscoveryActiveGVTracker 启动从 CRD/APIService 跟踪活跃 GV 的 worker。
func (h *peerProxyHandler) RunPeerDiscoveryActiveGVTracker(ctx context.Context) {
	if h.gvExclusionManager != nil {
		h.gvExclusionManager.RunPeerDiscoveryActiveGVTracker(ctx)
	}
}

// RunPeerDiscoveryRefilter 启动重新过滤对端发现缓存的 worker。
func (h *peerProxyHandler) RunPeerDiscoveryRefilter(ctx context.Context) {
	if h.gvExclusionManager != nil {
		h.gvExclusionManager.RunPeerDiscoveryRefilter(ctx)
	}
}
