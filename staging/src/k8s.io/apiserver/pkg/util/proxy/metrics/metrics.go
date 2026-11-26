/*
Copyright 2024 The Kubernetes Authors.

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

// Package metrics 定义 apiserver 流式代理（RemoteCommand/PortForward 及 WebSocket）的指标。
// 指标按状态码或子资源/代理类型打标签，用于观察流式连接的成败与路由方式。
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
)

var registerMetricsOnce sync.Once

var (
	// streamTranslatorRequestsTotal 统计由
	// StreamTranslatorProxy（RemoteCommand 子协议）处理的请求数。
	streamTranslatorRequestsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "stream_translator_requests_total",
			Help:           "Total number of requests that were handled by the StreamTranslatorProxy, which processes streaming RemoteCommand/V5",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{statuscode},
	)
	// streamTunnelRequestsTotal 统计由
	// StreamTunnelProxy（PortForward 子协议）处理的请求数。
	streamTunnelRequestsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "stream_tunnel_requests_total",
			Help:           "Total number of requests that were handled by the StreamTunnelProxy, which processes streaming PortForward/V2",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{statuscode},
	)
	// websocketStreamingRequestsTotal 统计 apiserver 路由的
	// WebSocket 流式请求，按 subresource 与 proxy_type 标记。
	websocketStreamingRequestsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: subsystem,
			Name:      "websocket_streaming_requests_total",
			Help: "Total number of WebSocket streaming requests (exec/attach/portforward) routed by the API server, " +
				"labeled by subresource and proxy_type. proxy_type is proxied_to_kubelet when the kubelet " +
				"handles the request directly; otherwise translated_at_apiserver.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"subresource", "proxy_type"},
	)
)

// Register 把本包的指标注册到全局 registry；sync.Once 保证重复调用安全。
func Register() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(streamTranslatorRequestsTotal)
		legacyregistry.MustRegister(streamTunnelRequestsTotal)
		legacyregistry.MustRegister(websocketStreamingRequestsTotal)
	})
}

// ResetForTest 清空所有计数器，仅供测试使用。
func ResetForTest() {
	streamTranslatorRequestsTotal.Reset()
	streamTunnelRequestsTotal.Reset()
	websocketStreamingRequestsTotal.Reset()
}

// IncStreamTranslatorRequest 增加 StreamTranslatorProxy 处理的请求计数。
func IncStreamTranslatorRequest(ctx context.Context, status string) {
	streamTranslatorRequestsTotal.WithContext(ctx).WithLabelValues(status).Add(1)
}

// IncStreamTunnelRequest 增加 StreamTunnelProxy 处理的请求计数。
func IncStreamTunnelRequest(ctx context.Context, status string) {
	streamTunnelRequestsTotal.WithContext(ctx).WithLabelValues(status).Add(1)
}

// IncWebSocketStreamingRequest 增加 apiserver 路由的 WebSocket 流式请求计数，
// 按给定 subresource（exec/attach/portforward）与
// proxy_type 区分。
func IncWebSocketStreamingRequest(ctx context.Context, subresource, proxyType string) {
	websocketStreamingRequestsTotal.WithContext(ctx).WithLabelValues(subresource, proxyType).Add(1)
}
