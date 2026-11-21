/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	kubeletSubsystem = "kubelet"

	// CAdvisorMetricsProvider 表示通过 cAdvisor 采集容器统计指标。
	CAdvisorMetricsProvider MetricsProviderType = "cadvisor"
	// CRIMetricsProvider 表示通过 CRI 采集容器统计指标。
	CRIMetricsProvider MetricsProviderType = "cri"
)

var (
	// HTTPRequests 统计服务器启动以来收到的 HTTP 请求数。
	HTTPRequests = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeletSubsystem,
			Name:           "http_requests_total",
			Help:           "Number of the http requests received since the server started",
			StabilityLevel: metrics.ALPHA,
		},
		// server_type 区分只读服务器与读写服务器。
		// long_running 标记请求是否为长连接，目前包括 exec/attach/portforward/debug。
		[]string{"method", "path", "server_type", "long_running"},
	)
	// HTTPRequestsDuration 记录处理 HTTP 请求的耗时（秒）。
	HTTPRequestsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: kubeletSubsystem,
			Name:      "http_requests_duration_seconds",
			Help:      "Duration in seconds to serve http requests",
			// 暂时使用默认桶；如有需要再定制。
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"method", "path", "server_type", "long_running"},
	)
	// HTTPInflightRequests 记录当前正在处理的 HTTP 请求数。
	HTTPInflightRequests = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeletSubsystem,
			Name:           "http_inflight_requests",
			Help:           "Number of the inflight http requests",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"method", "path", "server_type", "long_running"},
	)
	// VolumeStatCalDuration 记录计算卷统计信息的耗时（秒），
	// 主要用于比较 fsquota 监控与 `du` 统计磁盘用量的差异。
	VolumeStatCalDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      kubeletSubsystem,
			Name:           "volume_metric_collection_duration_seconds",
			Help:           "Duration in seconds to calculate volume stats",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"metric_source"},
	)
	// MetricsProvider 记录 kubelet 当前使用的容器统计指标来源（cadvisor 或 cri）。
	MetricsProvider = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      kubeletSubsystem,
			Name:           "metrics_provider",
			Help:           "Metrics provider used by kubelet to collect container stats. Values can be 'cadvisor' and 'cri'",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"provider"},
	)
	// WebSocketStreamingRequests 按子资源统计 kubelet 收到的 WebSocket 流式请求数。
	WebSocketStreamingRequests = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      kubeletSubsystem,
			Name:           "websocket_streaming_requests_total",
			Help:           "Total number of WebSocket streaming requests (exec/attach/portforward) received by the kubelet.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"subresource"},
	)
)

// registerMetrics 保证指标只注册一次，避免重复注册触发 panic。
var registerMetrics sync.Once

// Register 将 kubelet 的全部指标注册到 legacy registry，且只执行一次。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(HTTPRequests)
		legacyregistry.MustRegister(HTTPRequestsDuration)
		legacyregistry.MustRegister(HTTPInflightRequests)
		legacyregistry.MustRegister(VolumeStatCalDuration)
		legacyregistry.MustRegister(MetricsProvider)
		legacyregistry.MustRegister(WebSocketStreamingRequests)
	})
}

// ResetForTest 重置可重置的指标，仅供单元测试使用。
func ResetForTest() {
	WebSocketStreamingRequests.Reset()
}

// SinceInSeconds 返回从 start 到现在的秒数。
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// CollectVolumeStatCalDuration 记录一次卷统计计算的耗时。
func CollectVolumeStatCalDuration(metricSource string, start time.Time) {
	VolumeStatCalDuration.WithLabelValues(metricSource).Observe(SinceInSeconds(start))
}

// MetricsProviderType 标识 kubelet 采集容器统计指标时使用的提供方。
type MetricsProviderType string

// SetMetricsProvider 设置当前生效的指标提供方。
func SetMetricsProvider(provider MetricsProviderType) {
	// 先清空再设置，保证同一时刻只有一个 provider 处于激活状态。
	MetricsProvider.Reset()
	MetricsProvider.WithLabelValues(string(provider)).Set(1)
}

// IncWebSocketStreamingRequest 将指定子资源（exec/attach/portforward）的 WebSocket 流式请求计数加一。
func IncWebSocketStreamingRequest(subresource string) {
	WebSocketStreamingRequests.WithLabelValues(subresource).Inc()
}
