/*
Copyright 2017 The Kubernetes Authors.

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

// Package metrics 记录 apiserver 出站连接（egress dialer）的拨号次数、延迟与失败，
// 用于区分故障发生在连接代理服务器阶段还是代理转发阶段。
package metrics

import (
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/utils/clock"
)

const (
	namespace = "apiserver"
	subsystem = "egress_dialer"

	// ProtocolHTTPConnect 表示代理协议为 http-connect。
	ProtocolHTTPConnect = "http_connect"
	// ProtocolGRPC 表示代理协议为 GRPC。
	ProtocolGRPC = "grpc"
	// TransportTCP 表示传输层为 TCP。
	TransportTCP = "tcp"
	// TransportUDS 表示传输层为 UDS。
	TransportUDS = "uds"
	// StageConnect 表示连接代理服务器时拨号失败。
	StageConnect = "connect"
	// StageProxy 表示请求代理服务器进行代理时失败。
	StageProxy = "proxy"
)

var (
	// 使用 5ms 到 12.5s 的分布桶：桶的跨度覆盖本机代理（毫秒级）到跨网络代理（十秒级）两种典型场景。
	latencyBuckets = []float64{0.005, 0.025, 0.1, 0.5, 2.5, 12.5}

	// Metrics 提供对所有 dial 指标的访问；进程级单例，在包初始化时注册到 legacy registry。
	Metrics = newDialMetrics()
)

// DialMetrics 用 prometheus 指标记录到代理服务器的拨号。
type DialMetrics struct {
	clock     clock.Clock
	starts    *metrics.CounterVec
	latencies *metrics.HistogramVec
	failures  *metrics.CounterVec
}

// newDialMetrics 创建一个使用默认指标名配置的 DialMetrics。
// 指标以 ALPHA 稳定性注册，名称与标签集合一旦发布即属于兼容性契约。
func newDialMetrics() *DialMetrics {
	starts := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dial_start_total",
			Help:           "Dial starts, labeled by the protocol (http-connect or grpc) and transport (tcp or uds).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"protocol", "transport"},
	)

	latencies := metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dial_duration_seconds",
			Help:           "Dial latency histogram in seconds, labeled by the protocol (http-connect or grpc), transport (tcp or uds)",
			Buckets:        latencyBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"protocol", "transport"},
	)

	failures := metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "dial_failure_count",
			Help:           "Dial failure count, labeled by the protocol (http-connect or grpc), transport (tcp or uds), and stage (connect or proxy). The stage indicates at which stage the dial failed",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"protocol", "transport", "stage"},
	)

	legacyregistry.MustRegister(starts)
	legacyregistry.MustRegister(latencies)
	legacyregistry.MustRegister(failures)
	return &DialMetrics{starts: starts, latencies: latencies, failures: failures, clock: clock.RealClock{}}
}

// Clock 返回指标使用的时钟：让调用方与指标共用同一时间基准，避免延迟统计出现负值。
func (m *DialMetrics) Clock() clock.Clock {
	return m.clock
}

// SetClock sets the clock. 仅供测试替换为假时钟。
func (m *DialMetrics) SetClock(c clock.Clock) {
	m.clock = c
}

// Reset resets the metrics. 用于测试之间隔离状态。
func (m *DialMetrics) Reset() {
	m.starts.Reset()
	m.latencies.Reset()
	m.failures.Reset()
}

// ObserveDialStart 记录一次 dial 尝试的开始，按 protocol 与 transport 打标。
// 注意：start 与 latency/failure 是不同指标，调用方需自行保证成对记录。
func (m *DialMetrics) ObserveDialStart(protocol, transport string) {
	m.starts.WithLabelValues(protocol, transport).Inc()
}

// ObserveDialLatency 记录 dial 的延迟，按 protocol 与 transport 打标。
// elapsed 以秒为单位入桶，与指标名中的 _seconds 保持一致。
func (m *DialMetrics) ObserveDialLatency(elapsed time.Duration, protocol, transport string) {
	m.latencies.WithLabelValues(protocol, transport).Observe(elapsed.Seconds())
}

// ObserveDialFailure 记录一次失败的 dial，按 protocol、transport 以及失败所处阶段（stage）打标。
// stage 用于定位故障点：connect 表示连不上代理，proxy 表示代理拒绝了转发请求。
func (m *DialMetrics) ObserveDialFailure(protocol, transport, stage string) {
	m.failures.WithLabelValues(protocol, transport, stage).Inc()
}
