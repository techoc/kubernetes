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
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	"k8s.io/apiserver/pkg/reconcilers"
	"k8s.io/apiserver/pkg/util/peerproxy/metrics"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/transport"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	epmetrics "k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	apiserverproxyutil "k8s.io/apiserver/pkg/util/proxy"
	coordinationv1informers "k8s.io/client-go/informers/coordination/v1"
)

const (
	// PeerProxiedHeader 标记请求已经过一次转发；接收方据此只在本机处理，
	// 防止滚动升级期间多个 apiserver 互相转发形成环路。
	PeerProxiedHeader = "x-kubernetes-peer-proxied"
)

// peerProxyHandler 在混合版本集群中把请求转发给真正能提供该资源的 apiserver。
//
// 典型场景：滚动升级后新版本的 CRD/聚合 API 只在部分实例上存在，
// 客户端连到任一实例都期望成功，因此需要按"谁有该 GVR"来路由。
type peerProxyHandler struct {
	name string
	// Identity for this server.
	serverID     string
	finishedSync atomic.Bool

	// Label to check against in identity leases to make sure
	// we are working with apiserver identity leases only.
	identityLeaseLabelSelector labels.Selector
	apiserverIdentityInformer  coordinationv1informers.LeaseInformer
	leaseRegistration          cache.ResourceEventHandlerRegistration
	// Reconciler that is used to fetch host port of peer apiserver when proxying request to a peer.
	reconciler reconcilers.PeerEndpointLeaseReconciler
	// Client to make discovery calls locally.
	discoveryClient     *discovery.DiscoveryClient
	discoverySerializer serializer.CodecFactory
	// Cache that stores resources served by this apiserver. Refreshed periodically.
	// We always look up in the local discovery cache first, to check whether the
	// request can be served by this apiserver instead of proxying it to a peer.
	//
	// 先查本地、再考虑转发，可以避免"本机能服务却绕一圈"的额外延迟与故障面。
	localDiscoveryInfoCache              atomic.Value
	localDiscoveryCacheTicker            *time.Ticker
	localDiscoveryInfoCachePopulated     chan struct{}
	localDiscoveryInfoCachePopulatedOnce sync.Once
	// rawPeerDiscoveryCache stores unfiltered resources and groups served by peer apiservers.
	// The map is from string (serverID) to PeerDiscoveryCacheEntry.
	// Written ONLY by peerLeaseQueue worker when peer leases change.
	//
	// 用 atomic.Value 发布整张 map：读者永远看到某个完整版本，不会观察到写一半的状态。
	rawPeerDiscoveryCache atomic.Value // map[string]PeerDiscoveryCacheEntry
	proxyTransport        http.RoundTripper
	// Worker queue that keeps the rawPeerDiscoveryCache up-to-date.
	peerLeaseQueue            workqueue.TypedRateLimitingInterface[string]
	serializer                runtime.NegotiatedSerializer
	cacheInvalidationCallback atomic.Pointer[func()]
	// Manager for GV exclusions (CRDs/APIServices)
	gvExclusionManager *GVExclusionManager
}

// PeerDiscoveryCacheEntry holds the GVRs and group-level discovery info for a peer.
type PeerDiscoveryCacheEntry struct {
	GVRs           map[schema.GroupVersionResource]bool
	GroupDiscovery []apidiscoveryv2.APIGroupDiscovery
}

// responder implements rest.Responder for assisting a connector in writing objects or errors.
type responder struct {
	w   http.ResponseWriter
	ctx context.Context
}

// RegisterCacheInvalidationCallback 注册回调，供上层在发现信息变化时清空代理缓存。
func (h *peerProxyHandler) RegisterCacheInvalidationCallback(cb func()) {
	h.cacheInvalidationCallback.Store(&cb)
}

// HasFinishedSync 报告所有依赖的 informer 是否已完成初次同步。
func (h *peerProxyHandler) HasFinishedSync() bool {
	return h.finishedSync.Load()
}

// WaitForCacheSync 等待身份租约、处理注册、GV 排除与本地发现缓存全部就绪。
//
// 任一环节等待失败都返回错误而不是继续运行：在信息不全时转发请求，
// 会导致合法请求被误判为"无实例可服务"而失败。
func (h *peerProxyHandler) WaitForCacheSync(stopCh <-chan struct{}) error {
	ok := cache.WaitForNamedCacheSync("mixed-version-proxy", stopCh, h.apiserverIdentityInformer.Informer().HasSynced)
	if !ok {
		return fmt.Errorf("error while waiting for initial cache sync")
	}

	if !cache.WaitForNamedCacheSync(peerDiscoveryControllerName, stopCh, h.leaseRegistration.HasSynced) {
		return fmt.Errorf("error while waiting for peer-identity-lease event handler registration sync")
	}

	if h.gvExclusionManager != nil {
		if !h.gvExclusionManager.WaitForCacheSync(stopCh) {
			return fmt.Errorf("error while waiting for gv exclusion manager cache sync")
		}
	}

	// Wait for localDiscoveryInfoCache to be populated.
	// 本地发现缓存由独立 ticker 填充，必须等到首份数据，否则所有请求都会走转发路径。
	select {
	case <-h.localDiscoveryInfoCachePopulated:
	case <-stopCh:
		return fmt.Errorf("stop signal received while waiting for local discovery cache population")
	}

	h.finishedSync.Store(true)
	return nil
}

// WrapHandler will fetch the apiservers that can serve the request and either serve it locally
// or route it to a peer
func (h *peerProxyHandler) WrapHandler(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if !ok {
			responsewriters.InternalError(w, r, errors.New("no RequestInfo found in the context"))
			return
		}

		// Allow non-resource requests
		// /healthz、/version 等请求不涉及具体资源，本机即可应答。
		if !requestInfo.IsResourceRequest {
			klog.V(3).Infof("Not a resource request skipping proxying")
			handler.ServeHTTP(w, r)
			return
		}

		// Request has already been proxied once, it must be served locally
		// 环路防护：已经转发过的请求若再转发，会在集群内无限乒乓。
		if r.Header.Get(PeerProxiedHeader) == "true" {
			klog.V(3).Infof("Already rerouted once, skipping proxying to peer")
			handler.ServeHTTP(w, r)
			return
		}

		// Apiserver Identity Informers is not synced yet, pass request to next handler
		// This will happen for self requests from the kube-apiserver because we have a poststarthook
		// to ensure that external requests are not served until the ApiserverIdentity Informer has synced
		if !h.HasFinishedSync() {
			handler.ServeHTTP(w, r)
			return
		}

		gvr := schema.GroupVersionResource{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion, Resource: requestInfo.Resource}

		if h.shouldServeLocally(gvr) {
			handler.ServeHTTP(w, r)
			return
		}

		// find servers that are capable of serving this request
		peerServerIDs := h.findServiceableByPeerFromPeerDiscoveryCache(gvr)
		if len(peerServerIDs) == 0 {
			// 没有任何实例声明可服务该 GVR：交给本地处理，让原生逻辑返回 404 等标准错误。
			klog.V(3).Infof("gvr %v is not served by anything in this cluster", gvr)
			handler.ServeHTTP(w, r)
			return
		}

		peerEndpoints, err := h.resolveServingLocation(peerServerIDs)
		if err != nil {
			metrics.IncPeerProxyError(ctx, metrics.ProxyErrorEndpointResolution, gvr.Group, gvr.Version, gvr.Resource)
			gv := schema.GroupVersion{Group: gvr.Group, Version: gvr.Version}
			klog.ErrorS(err, "error finding serviceable-by apiservers for the requested resource", "gvr", gvr)
			responsewriters.ErrorNegotiated(apierrors.NewServiceUnavailable("Error getting ip and port info of the remote server while proxying"), h.serializer, gv, w, r)
			return
		}

		// 随机选择一个对等端，避免所有实例都转发到同一台造成热点。
		endpointIndex := rand.Intn(len(peerEndpoints))
		peerEndpoint := peerEndpoints[endpointIndex]
		h.proxyRequestToDestinationAPIServer(r, w, peerEndpoint, gvr)
	})
}

// resolveServingLocation resolves the host:port addresses for the given peer IDs.
//
// 部分对等端解析失败不影响可用性：只要还有至少一个可解析地址就继续转发，
// 仅在全部失败时才返回错误。
func (h *peerProxyHandler) resolveServingLocation(peerIDs []string) ([]string, error) {
	var peerServerEndpoints []string
	var errs []error

	for _, id := range peerIDs {
		hostPort, err := h.hostportInfo(id)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		peerServerEndpoints = append(peerServerEndpoints, hostPort)
	}

	// reset err if there was atleast one valid peer server found.
	if len(peerServerEndpoints) > 0 {
		errs = nil
	}

	return peerServerEndpoints, errors.Join(errs...)
}

// hostportInfo 从身份租约 reconciler 查询对等端地址，并校验其格式合法。
func (h *peerProxyHandler) hostportInfo(apiserverKey string) (string, error) {
	hostPort, err := h.reconciler.GetEndpoint(apiserverKey)
	if err != nil {
		return "", err
	}

	_, _, err = net.SplitHostPort(hostPort)
	if err != nil {
		return "", err
	}

	return hostPort, nil
}

// proxyRequestToDestinationAPIServer 构造指向对等端的请求并完成转发。
//
// 使用 AuthProxyRoundTripper 以原用户身份转发：授权仍在对等端重新执行一次，
// 代理本身不构成权限提升路径。
func (h *peerProxyHandler) proxyRequestToDestinationAPIServer(req *http.Request, rw http.ResponseWriter, host string, gvr schema.GroupVersionResource) {
	// write a new location based on the existing request pointed at the target service
	location := &url.URL{}
	location.Scheme = "https"
	location.Host = host
	location.Path = req.URL.Path
	location.RawQuery = req.URL.Query().Encode()

	newReq, cancelFn := apiserverproxyutil.NewRequestForProxy(location, req)
	newReq.Header.Add(PeerProxiedHeader, "true")
	defer cancelFn()

	proxyRoundTripper, err := h.buildProxyRoundtripper(req)
	if err != nil {
		metrics.IncPeerProxyError(req.Context(), metrics.ProxyErrorTransport, gvr.Group, gvr.Version, gvr.Resource)
		klog.Errorf("failed to build proxy round tripper: %v", err)
		return
	}

	// 包装响应写入器以捕获状态码，供成功指标记录。
	delegate := &epmetrics.ResponseWriterDelegator{ResponseWriter: rw}
	w := responsewriter.WrapForHTTP1Or2(delegate)
	handler := proxy.NewUpgradeAwareHandler(location, proxyRoundTripper, true, false, &responder{w: w, ctx: req.Context()})
	klog.Infof("Proxying request for %s from %s to %s", req.URL.Path, req.Host, location.Host)
	handler.ServeHTTP(w, newReq)
	metrics.IncPeerProxiedRequest(req.Context(), strconv.Itoa(delegate.Status()), gvr.Group, gvr.Version, gvr.Resource)
}

// buildProxyRoundtripper 用请求中的用户身份构造转发用的 RoundTripper。
// 缺少用户信息说明过滤器顺序有误，直接拒绝而不是匿名转发。
func (h *peerProxyHandler) buildProxyRoundtripper(req *http.Request) (http.RoundTripper, error) {
	user, ok := apirequest.UserFrom(req.Context())
	if !ok {
		return nil, apierrors.NewBadRequest("no user details present in request")
	}

	return transport.NewAuthProxyRoundTripper(user.GetName(), user.GetUID(), user.GetGroups(), user.GetExtra(), h.proxyTransport), nil
}

// Error 在转发失败时向客户端返回 503，表示对等端暂时不可用。
func (r *responder) Error(w http.ResponseWriter, req *http.Request, err error) {
	klog.ErrorS(err, "Error while proxying request to destination apiserver")
	http.Error(w, err.Error(), http.StatusServiceUnavailable)
}

// GetPeerResources implements PeerDiscoveryProvider interface
// Returns a map of serverID -> []apidiscoveryv2.APIGroupDiscovery served by peer servers
//
// 排除本机：调用方聚合的是"其他实例"的发现信息，混入自身会重复。
func (h *peerProxyHandler) GetPeerResources() map[string][]apidiscoveryv2.APIGroupDiscovery {
	result := make(map[string][]apidiscoveryv2.APIGroupDiscovery)
	cacheMap := h.gvExclusionManager.GetFilteredPeerDiscoveryCache()
	if len(cacheMap) == 0 {
		klog.V(4).Infof("GetPeerResources: peer cache is empty")
		return result
	}

	for serverID, peerData := range cacheMap {
		if serverID == h.serverID {
			klog.V(4).Infof("GetPeerResources: skipping local server %s", serverID)
			continue // Skip local server
		}
		result[serverID] = peerData.GroupDiscovery
	}

	return result
}
