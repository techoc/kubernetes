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
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/user"
	peerproxymetrics "k8s.io/apiserver/pkg/util/peerproxy/metrics"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	v1 "k8s.io/api/coordination/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	responsewriterutil "k8s.io/apiserver/pkg/util/responsewriter"
)

const (
	peerDiscoveryControllerName = "peer-discovery-cache-sync"
	// maxRetries is the maximum number of retry attempts per lease, set to 20 to handle
	// the race condition during API server startup where identity leases are created before
	// endpoint leases. During initialization, peer discovery sync may attempt to fetch discovery
	// from a peer before that peer has created its endpoint lease, resulting in "missing port in
	// address" errors. With the default rate limiting (exponential backoff starting at 5ms),
	// 20 retries per lease provides approximately 60-90 seconds of retry window, which is
	// sufficient for both identity and endpoint leases to be established during normal startup.
	//
	// 启动期 identity lease 可能先于 endpoint lease 创建，此时解析对端地址会
	// 得到 "missing port in address"。20 次、从 5ms 开始的指数退避约覆盖
	// 60-90 秒，足以等到两类 lease 都就绪。
	maxRetries = 20
)

// RunPeerDiscoveryCacheSync 启动 peer 发现缓存的同步 worker，并在退出时
// 注销 informer 事件处理器、关闭队列。workers 并发处理同一个 lease 队列，
// 但每次同步都是全量重建，重复处理是幂等的。
func (h *peerProxyHandler) RunPeerDiscoveryCacheSync(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer h.peerLeaseQueue.ShutDown()
	defer func() {
		err := h.apiserverIdentityInformer.Informer().RemoveEventHandler(h.leaseRegistration)
		if err != nil {
			klog.Warning("error removing leaseInformer eventhandler")
		}
	}()

	klog.Infof("Workers: %d", workers)
	for i := 0; i < workers; i++ {
		klog.Infof("Starting worker")
		go wait.UntilWithContext(ctx, h.runWorker, time.Second)
	}
	<-ctx.Done()
}

// enqueueLease 把 lease 名放入队列；同步时以名称重新从 lister 读取，
// 避免持有可能过期的对象引用。
func (h *peerProxyHandler) enqueueLease(lease *v1.Lease) {
	h.peerLeaseQueue.Add(lease.Name)
}

// runWorker 持续从队列取任务直到队列关闭。
func (h *peerProxyHandler) runWorker(ctx context.Context) {
	for h.processNextElectionItem(ctx) {
	}
}

// processNextElectionItem 处理一个队列项并保证 Done 被调用，使队列的
// 进行中计数正确；syncPeerDiscoveryCache 失败时由
// handleSyncPeerDiscoveryCacheErr 决定重试或丢弃。
func (h *peerProxyHandler) processNextElectionItem(ctx context.Context) bool {
	key, shutdown := h.peerLeaseQueue.Get()
	if shutdown {
		return false
	}
	defer h.peerLeaseQueue.Done(key)

	err := h.syncPeerDiscoveryCache(ctx)
	h.handleSyncPeerDiscoveryCacheErr(err, key)
	return true
}

// syncPeerDiscoveryCache 根据当前 identity lease 全量重建对等发现缓存。
// 先写入 raw cache，再由单写者的 refilter 应用排除规则，避免并发读写
// 过滤后的缓存。单个 peer 拉取失败不会中止整体重建，返回值仅用于触发
// 重试。
func (h *peerProxyHandler) syncPeerDiscoveryCache(ctx context.Context) error {
	var fetchDiscoveryErr error
	// Rebuild the peer discovery cache from available leases.
	leases, err := h.apiserverIdentityInformer.Lister().List(h.identityLeaseLabelSelector)
	if err != nil {
		peerproxymetrics.IncPeerDiscoverySyncError(ctx, peerproxymetrics.DiscoveryErrorLeaseList)
		utilruntime.HandleError(err)
		return err
	}

	newCache := map[string]PeerDiscoveryCacheEntry{}
	for _, l := range leases {
		// 过滤掉非本集群的、已删除 tombstone 或 holder 信息缺失的 lease。
		_, ok := h.isValidPeerIdentityLease(l)
		if !ok {
			continue
		}

		discoveryEntry, err := h.fetchNewDiscoveryFor(ctx, l.Name)
		if err != nil {
			fetchDiscoveryErr = err
		}
		// Only add if there is at least one GVR or group
		// 空发现结果不缓存：可能对端尚未就绪，下次同步会重试。
		if len(discoveryEntry.GVRs) > 0 || len(discoveryEntry.GroupDiscovery) > 0 {
			newCache[l.Name] = discoveryEntry
		}
	}

	// Store unfiltered data to raw cache and trigger refilter.
	// The refilter worker (single writer to filtered cache) will apply exclusions.
	h.rawPeerDiscoveryCache.Store(newCache)
	h.gvExclusionManager.TriggerRefilter()
	return fetchDiscoveryErr
}

// fetchNewDiscoveryFor 解析对端地址并分别拉取 /api 与 /apis 的发现
// 文档，汇总 GVR 集合与按组去重的发现项。core/v1 会记录在 GVR 集合中
// 以便路由，但不写入聚合发现文档，因为它不由 /apis 提供。
func (h *peerProxyHandler) fetchNewDiscoveryFor(ctx context.Context, serverID string) (PeerDiscoveryCacheEntry, error) {
	hostport, err := h.hostportInfo(serverID)
	if err != nil {
		peerproxymetrics.IncPeerDiscoverySyncError(ctx, peerproxymetrics.DiscoveryErrorHostPortResolution)
		return PeerDiscoveryCacheEntry{}, fmt.Errorf("failed to get host port info from identity lease for server %s: %w", serverID, err)
	}

	klog.V(4).Infof("Proxying an agg-discovery call from %s to %s", h.serverID, serverID)
	gvrMap := make(map[schema.GroupVersionResource]bool)
	var discoveryErr error
	var discoveryResponse *apidiscoveryv2.APIGroupDiscoveryList
	discoveryPaths := []string{"/api", "/apis"}

	// Use a slice to preserve order from the peer.
	// Use a map to track seen groups to avoid duplicates.
	// 保留对端返回顺序（切片）同时去重（map），兼得稳定输出与正确性。
	groupList := make([]apidiscoveryv2.APIGroupDiscovery, 0)
	seenGroups := make(map[string]struct{})

	for _, path := range discoveryPaths {
		discoveryResponse, discoveryErr = h.aggregateDiscovery(ctx, path, hostport)
		if discoveryErr != nil {
			peerproxymetrics.IncPeerDiscoverySyncError(ctx, peerproxymetrics.DiscoveryErrorFetch)
			klog.ErrorS(discoveryErr, "error querying discovery endpoint for serverID", "path", path, "serverID", serverID)
			continue
		}

		for _, groupDiscovery := range discoveryResponse.Items {
			for _, version := range groupDiscovery.Versions {
				for _, resource := range version.Resources {
					gvr := schema.GroupVersionResource{
						Group:    groupDiscovery.Name,
						Version:  version.Version,
						Resource: resource.Resource,
					}
					gvrMap[gvr] = true
				}
			}
			// Skip core/v1 group from peer-aggregated discovery since its not served from /apis.
			// We still want to re-route core/v1 requests to the peer, but we don't want it
			// to appear in the peer-aggregated discovery document.
			if groupDiscovery.Name == "" {
				continue
			}
			if _, ok := seenGroups[groupDiscovery.Name]; !ok {
				groupList = append(groupList, groupDiscovery)
				seenGroups[groupDiscovery.Name] = struct{}{}
			}
		}
	}

	klog.V(4).Infof("Agg discovery done successfully by %s for %s", h.serverID, serverID)
	return PeerDiscoveryCacheEntry{
		GVRs:           gvrMap,
		GroupDiscovery: groupList,
	}, discoveryErr
}

// aggregateDiscovery 以 apiserver 自身身份向对端发起发现请求。Accept
// 头优先请求不含 peer 信息的 V2（AcceptV2NoPeer），避免对端把我们的
// 对等聚合再嵌套进来，形成循环或重复。
func (h *peerProxyHandler) aggregateDiscovery(ctx context.Context, path string, hostport string) (*apidiscoveryv2.APIGroupDiscoveryList, error) {
	req, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}

	apiServerUser := &user.DefaultInfo{
		Name:   user.APIServerUser,
		UID:    user.APIServerUser,
		Groups: []string{user.AllAuthenticated},
	}

	ctx = apirequest.WithUser(ctx, apiServerUser)
	req = req.WithContext(ctx)

	// Fallback to V2 and V1 in that order if V2Local is not recognized.
	req.Header.Add("Accept", discovery.AcceptV2NoPeer+","+discovery.AcceptV2+","+discovery.AcceptV1)

	// 用内存 ResponseWriter 承接代理响应，便于在进程内直接解码，无需
	// 建立回环连接。
	writer := responsewriterutil.NewInMemoryResponseWriter()
	h.proxyRequestToDestinationAPIServer(req, writer, hostport, schema.GroupVersionResource{})
	if writer.RespCode() != http.StatusOK {
		return nil, fmt.Errorf("discovery request failed with status: %d", writer.RespCode())
	}

	parsed := &apidiscoveryv2.APIGroupDiscoveryList{}
	if err := runtime.DecodeInto(h.discoverySerializer.UniversalDecoder(), writer.Data(), parsed); err != nil {
		return nil, fmt.Errorf("error decoding discovery response: %w", err)
	}

	return parsed, nil
}

// handleSyncPeerDiscoveryCacheErr checks if an error happened during peer discovery sync and makes sure we will retry later.
//
// 有界重试：未超过 maxRetries 时按速率限制重新入队；超过后丢弃并
// 上报，避免一个永久失败的对端拖住队列。
func (h *peerProxyHandler) handleSyncPeerDiscoveryCacheErr(err error, key string) {
	if err == nil {
		h.peerLeaseQueue.Forget(key)
		return
	}

	if h.peerLeaseQueue.NumRequeues(key) < maxRetries {
		klog.Infof("Error syncing discovery for peer lease %v: %v", key, err)
		h.peerLeaseQueue.AddRateLimited(key)
		return
	}

	h.peerLeaseQueue.Forget(key)
	utilruntime.HandleError(err)
	klog.Infof("Dropping lease %s out of the queue: %v", key, err)
}

// isValidPeerIdentityLease 校验 lease 是否代表一个可代理的同集群对端：
// 需能处理 tombstone、匹配标签选择器、不是自身、且 holderIdentity 非空。
// 任一条件不满足都返回 false，调用方据此跳过而不是报错。
func (h *peerProxyHandler) isValidPeerIdentityLease(obj interface{}) (*v1.Lease, bool) {
	lease, ok := obj.(*v1.Lease)
	if !ok {
		// 删除事件可能以 tombstone 到达，需要从中还原 Lease。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", obj))
			return nil, false
		}
		if lease, ok = tombstone.Obj.(*v1.Lease); !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", obj))
			return nil, false
		}
	}

	if lease == nil {
		klog.Error(fmt.Errorf("nil lease object provided"))
		return nil, false
	}

	// 目前只支持单一 key=value 形式的标签选择器；更复杂的表达式会被
	// 拒绝，因为这些 lease 由 apiserver 自身以固定标签创建。
	if h.identityLeaseLabelSelector != nil && h.identityLeaseLabelSelector.String() != "" {
		identityLeaseLabel := strings.Split(h.identityLeaseLabelSelector.String(), "=")
		if len(identityLeaseLabel) != 2 {
			klog.Errorf("invalid identityLeaseLabelSelector format: %s", h.identityLeaseLabelSelector.String())
			return nil, false
		}

		if lease.Labels == nil || lease.Labels[identityLeaseLabel[0]] != identityLeaseLabel[1] {
			klog.V(4).Infof("lease %s/%s does not match label selector: %s=%s", lease.Namespace, lease.Name, identityLeaseLabel[0], identityLeaseLabel[1])
			return nil, false
		}

	}

	// Ignore self.
	// 代理给自己会造成请求回环，必须排除。
	if lease.Name == h.serverID {
		return nil, false
	}

	if lease.Spec.HolderIdentity == nil {
		klog.Error(fmt.Errorf("invalid lease object provided, missing holderIdentity in lease obj"))
		return nil, false
	}

	return lease, true
}

// findServiceableByPeerFromPeerDiscoveryCache 返回声明可服务该 GVR 的
// 对端 ID。排除规则已在 filtered cache 中应用，这里再剔除自身作为兜底。
func (h *peerProxyHandler) findServiceableByPeerFromPeerDiscoveryCache(gvr schema.GroupVersionResource) []string {
	var serviceableByIDs []string
	cacheMap := h.gvExclusionManager.GetFilteredPeerDiscoveryCache()
	if len(cacheMap) == 0 {
		return serviceableByIDs
	}

	for peerID, peerData := range cacheMap {
		// Ignore local apiserver.
		if peerID == h.serverID {
			continue
		}
		if _, exists := peerData.GVRs[gvr]; exists {
			serviceableByIDs = append(serviceableByIDs, peerID)
		}
	}
	return serviceableByIDs
}
