/*
Copyright 2016 The Kubernetes Authors.

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

package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

const (
	// ToBeDeletedTaint 是 Cluster Autoscaler 在标记节点待删除前使用的 taint。定义于
	// https://github.com/kubernetes/autoscaler/blob/e80ab518340f88f364fe3ef063f8303755125971/cluster-autoscaler/utils/deletetaint/delete.go#L36
	ToBeDeletedTaint = "ToBeDeletedByClusterAutoscaler"
)

// ProxierHealth 表示处理单一 IP 族的 proxier 的健康状态。
type ProxierHealth struct {
	LastUpdated time.Time `json:"lastUpdated"`
	Healthy     bool      `json:"healthy"`
}

// ProxyHealth 表示 kube-proxy 的健康状态，其中包含各个 proxier 的健康状态。
type ProxyHealth struct {
	// LastUpdated 是最近一次更新的 proxier 的更新时间。
	// 保留此字段是为了向后兼容。
	LastUpdated  time.Time `json:"lastUpdated"`
	CurrentTime  time.Time `json:"currentTime"`
	NodeEligible *bool     `json:"nodeEligible,omitempty"`
	// 所有 proxier 都健康时 Healthy 为 true，否则为 false。
	Healthy bool `json:"healthy"`
	// 每个 IP 族的健康检查状态。
	Status map[v1.IPFamily]ProxierHealth `json:"status,omitempty"`
}

// ProxyHealthServer 允许调用方：
//  1. 运行一个带有 /healthz 和 /livez endpoint handler 的 HTTP server。
//  2. 在数据平面同步前后更新 healthz 时间戳。
//  3. 同步节点状态；若节点被 autoscaler 标记为待删除，则在 /healthz 响应中报告不健康。
//  4. 通过检查 QueuedUpdate() 与 Updated() 调用之间的延迟是否超过 healthTimeout
//     来获取 proxy 健康状态。
type ProxyHealthServer struct {
	listener    listener
	httpFactory httpServerFactory
	clock       clock.Clock

	nodeManager nodeManager

	addr          string
	healthTimeout time.Duration

	lock                   sync.RWMutex
	lastUpdatedMap         map[v1.IPFamily]time.Time
	oldestPendingQueuedMap map[v1.IPFamily]time.Time
}

// 该接口用于切断 pkg/proxy 与 pkg/proxy/healthcheck 之间的导入链。
type nodeManager interface {
	Node() *v1.Node
}

// NewProxyHealthServer 返回一个 proxy 健康检查 HTTP server。
func NewProxyHealthServer(addr string, healthTimeout time.Duration, nodeManager nodeManager) *ProxyHealthServer {
	return newProxyHealthServer(stdNetListener{}, stdHTTPServerFactory{}, clock.RealClock{}, addr, healthTimeout, nodeManager)
}

// newProxyHealthServer 使用指定的依赖构造 ProxyHealthServer，便于测试中注入 fake 的依赖。
func newProxyHealthServer(listener listener, httpServerFactory httpServerFactory, c clock.Clock, addr string, healthTimeout time.Duration, nodeManager nodeManager) *ProxyHealthServer {
	return &ProxyHealthServer{
		listener:      listener,
		httpFactory:   httpServerFactory,
		clock:         c,
		addr:          addr,
		healthTimeout: healthTimeout,
		nodeManager:   nodeManager,

		lastUpdatedMap:         make(map[v1.IPFamily]time.Time),
		oldestPendingQueuedMap: make(map[v1.IPFamily]time.Time),
	}
}

// 当指定 IP 族的 proxier 已成功更新 service 规则以反映当前状态、可以被视为健康时，
// 应调用 Updated。
func (hs *ProxyHealthServer) Updated(ipFamily v1.IPFamily) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	// 规则已同步，清除最早待处理更新的时间戳，避免 Health() 误判超时。
	delete(hs.oldestPendingQueuedMap, ipFamily)
	hs.lastUpdatedMap[ipFamily] = hs.clock.Now()
}

// 当 proxier 收到 API Server 发来的、需要更新 service 规则的 Service 或 Endpoints 事件时，
// 应调用 QueuedUpdate。它表示指定 IP 族的 proxier 已收到变更，但尚未将其推送到后端。
// 如果 proxier 未在 healthTimeout 时间内调用 Updated，则会被视为不健康。
func (hs *ProxyHealthServer) QueuedUpdate(ipFamily v1.IPFamily) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	// 只在尚未设置时记录最早待处理更新的时间，以免后续排队更新重置超时计时。
	if _, set := hs.oldestPendingQueuedMap[ipFamily]; !set {
		hs.oldestPendingQueuedMap[ipFamily] = hs.clock.Now()
	}
}

// Health 返回 proxy 的健康状态。
func (hs *ProxyHealthServer) Health() ProxyHealth {
	var health = ProxyHealth{
		Healthy: true,
		Status:  make(map[v1.IPFamily]ProxierHealth),
	}
	hs.lock.RLock()
	defer hs.lock.RUnlock()

	var lastUpdated time.Time
	for ipFamily, proxierLastUpdated := range hs.lastUpdatedMap {
		if proxierLastUpdated.After(lastUpdated) {
			lastUpdated = proxierLastUpdated
		}
		// 先将每个 proxier 的健康状态初始化为 healthy=true，并记录其最近更新时间；
		// 后面若发现超时再改写为不健康。
		health.Status[ipFamily] = ProxierHealth{
			LastUpdated: proxierLastUpdated,
			Healthy:     true,
		}
	}

	currentTime := hs.clock.Now()
	health.CurrentTime = currentTime
	for ipFamily, proxierLastUpdated := range hs.lastUpdatedMap {
		if _, set := hs.oldestPendingQueuedMap[ipFamily]; !set {
			// 没有待处理的更新：proxier 仍在启动中或已完全同步，视为健康。
			continue
		}

		if currentTime.Sub(hs.oldestPendingQueuedMap[ipFamily]) < hs.healthTimeout {
			// 有未处理的更新在排队，但尚未超时。
			continue
		}

		// 排队更新已超时，将该 proxier 标记为不健康。
		health.Healthy = false
		health.Status[ipFamily] = ProxierHealth{
			LastUpdated: proxierLastUpdated,
			Healthy:     false,
		}
	}
	health.LastUpdated = lastUpdated
	return health
}

// NodeEligible 返回节点是否合格。合格的定义是：没有被 ToBeDeletedTaint 污染，且未被删除。
func (hs *ProxyHealthServer) NodeEligible() bool {
	hs.lock.Lock()
	defer hs.lock.Unlock()

	node := hs.nodeManager.Node()
	// 节点信息尚不可用时保守地视为不合格，避免误报健康。
	if node == nil {
		return false
	}
	if !node.DeletionTimestamp.IsZero() {
		return false
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == ToBeDeletedTaint {
			return false
		}
	}
	return true
}

// Run 启动 healthz HTTP server，并阻塞直到其退出。
func (hs *ProxyHealthServer) Run(ctx context.Context) error {
	serveMux := http.NewServeMux()
	serveMux.Handle("/healthz", healthzHandler{hs: hs})
	serveMux.Handle("/livez", livezHandler{hs: hs})
	server := hs.httpFactory.New(serveMux)

	// 使用 MultiListen 以同时支持 IPv4 和 IPv6 地址。
	listener, err := hs.listener.Listen(ctx, "tcp", hs.addr)
	if err != nil {
		return fmt.Errorf("failed to start proxy healthz on %s: %w", hs.addr, err)
	}

	klog.V(3).InfoS("Starting healthz HTTP server", "address", hs.addr)

	if err := server.Serve(listener); err != nil {
		return fmt.Errorf("proxy healthz closed with error: %w", err)
	}
	return nil
}

type healthzHandler struct {
	hs *ProxyHealthServer
}

// ServeHTTP 处理 /healthz 请求：综合 proxier 健康状态和节点是否合格返回响应。
func (h healthzHandler) ServeHTTP(resp http.ResponseWriter, _ *http.Request) {
	health := h.hs.Health()
	nodeEligible := h.hs.NodeEligible()
	healthy := health.Healthy && nodeEligible
	// 在此处（而非 Health() 中）写入节点合格性，因为只有 /healthz 的响应才需要该字段，
	// /livez 的响应不应包含它。
	health.NodeEligible = ptr.To(nodeEligible)

	resp.Header().Set("Content-Type", "application/json")
	resp.Header().Set("X-Content-Type-Options", "nosniff")
	if !healthy {
		metrics.ProxyHealthzTotal.WithLabelValues("503").Inc()
		resp.WriteHeader(http.StatusServiceUnavailable)
	} else {
		metrics.ProxyHealthzTotal.WithLabelValues("200").Inc()
		resp.WriteHeader(http.StatusOK)
		// 旧版本中返回的 "lastUpdated" 表示 proxier 同步循环最后一次运行的时间，
		// 即使没有任何变更也会更新。为保持兼容性，此处沿用相同语义：server 健康时
		// 返回的 lastUpdated 是“最近”的。更详细的信息由 kube-proxy metrics 提供。
		health.LastUpdated = h.hs.clock.Now()
	}

	output, _ := json.Marshal(health)
	_, _ = fmt.Fprint(resp, string(output))
}

type livezHandler struct {
	hs *ProxyHealthServer
}

// ServeHTTP 处理 /livez 请求：仅依据 proxier 健康状态返回响应，不检查节点是否合格。
func (h livezHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	health := h.hs.Health()

	resp.Header().Set("Content-Type", "application/json")
	resp.Header().Set("X-Content-Type-Options", "nosniff")
	if !health.Healthy {
		metrics.ProxyLivezTotal.WithLabelValues("503").Inc()
		resp.WriteHeader(http.StatusServiceUnavailable)
	} else {
		metrics.ProxyLivezTotal.WithLabelValues("200").Inc()
		resp.WriteHeader(http.StatusOK)
		// 旧版本中返回的 "lastUpdated" 表示 proxier 同步循环最后一次运行的时间，
		// 即使没有任何变更也会更新。为保持兼容性，此处沿用相同语义：server 健康时
		// 返回的 lastUpdated 是“最近”的。更详细的信息由 kube-proxy metrics 提供。
		health.LastUpdated = h.hs.clock.Now()
	}
	output, _ := json.Marshal(health)
	_, _ = fmt.Fprint(resp, string(output))
}
