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

// Package metrics 定义 apiserver 之间请求转发（peer proxy）与
// peer 发现同步相关的指标。
//
// 这些指标用于定位版本偏斜期间哪个组件在持续重路由或同步失败。
package metrics

import (
	"context"
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	subsystem  = "apiserver"
	statuscode = "code"
	group      = "group"
	version    = "version"
	resource   = "resource"
	errorType  = "type"

	// ProxyErrorEndpointResolution 表示解析对等 apiserver 网络地址失败。
	// 以下错误原因作为指标 type 标签，取值必须稳定，否则会导致看板断档。
	ProxyErrorEndpointResolution = "endpoint_resolution"
	// ProxyErrorTransport 表示为请求构建代理传输失败。
	ProxyErrorTransport = "proxy_transport"

	// DiscoveryErrorLeaseList 表示列举 apiserver 身份 lease 失败。
	DiscoveryErrorLeaseList = "lease_list"
	// DiscoveryErrorHostPortResolution 表示从身份 lease 解析 host/port 失败。
	DiscoveryErrorHostPortResolution = "hostport_resolution"
	// DiscoveryErrorFetch 表示从 peer 拉取发现文档失败。
	DiscoveryErrorFetch = "fetch_discovery"
)

var registerMetricsOnce sync.Once

// 指标用 sync.Once 注册，重复调用安全；标签刻意不包含 peer 地址，
// 以防止基数爆炸。
var (
	// peerProxiedRequestsTotal 统计被代理到 peer kube-apiserver 的请求数。
	peerProxiedRequestsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "rerouted_request_total",
			Help:           `Total number of requests that were proxied to a peer kube-apiserver because the local apiserver was not capable of serving it, broken down by 'group', 'version', and 'resource' indicating the GVR of the request. If all three are empty (""), the request is a discovery request.`,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{statuscode, group, version, resource},
	)

	// peerProxyErrorsTotal 统计代理请求到 peer kube-apiserver 时遇到的错误数。
	peerProxyErrorsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "peer_proxy_errors_total",
			Help:           "Total number of errors encountered while proxying requests to a peer kube apiserver",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{errorType, group, version, resource},
	)

	// peerDiscoverySyncErrorsTotal 统计从 peer kube-apiserver 同步发现信息时的错误数。
	peerDiscoverySyncErrorsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "peer_discovery_sync_errors_total",
			Help:           "Total number of errors encountered while syncing discovery information from a peer kube-apiserver",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{errorType},
	)
)

// Register 把 peer proxy 指标注册到 legacy registry，幂等。
func Register() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(peerProxiedRequestsTotal)
		legacyregistry.MustRegister(peerProxyErrorsTotal)
		legacyregistry.MustRegister(peerDiscoverySyncErrorsTotal)
	})
}

// Reset 清空全局 registry，仅供测试使用；生产路径调用会破坏指标注册状态。
func Reset() {
	legacyregistry.Reset()
}

// IncPeerProxiedRequest 增加代理到 peer kube-apiserver 的请求数
func IncPeerProxiedRequest(ctx context.Context, status, g, v, r string) {
	peerProxiedRequestsTotal.WithContext(ctx).WithLabelValues(status, g, v, r).Add(1)
}

// IncPeerProxyError 增加 peer 代理期间遇到的错误数
func IncPeerProxyError(ctx context.Context, e, g, v, r string) {
	peerProxyErrorsTotal.WithContext(ctx).WithLabelValues(e, g, v, r).Add(1)
}

// IncPeerDiscoverySyncError 增加 peer 发现同步期间遇到的错误数
func IncPeerDiscoverySyncError(ctx context.Context, e string) {
	peerDiscoverySyncErrorsTotal.WithContext(ctx).WithLabelValues(e).Add(1)
}
