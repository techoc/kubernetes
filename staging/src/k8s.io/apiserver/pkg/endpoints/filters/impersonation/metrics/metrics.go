/*
Copyright The Kubernetes Authors.

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
	namespace = "apiserver"
	subsystem = "impersonation"
)

var (
	impersonationAttemptsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "attempts_total",
			Help:           "Total number of impersonation attempts split by mode and decision.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"mode", "decision"},
	)

	impersonationAttemptsDurationSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "attempts_duration_seconds",
			Help:           "Latency of impersonation attempts in seconds split by mode and decision.",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"mode", "decision"},
	)

	impersonationAuthorizationAttemptsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "authorization_attempts_total",
			Help:           "Total number of authorization checks made by the impersonation handler split by mode and decision.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"mode", "decision"},
	)

	impersonationAuthorizationAttemptsDurationSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "authorization_attempts_duration_seconds",
			Help:           "Latency of authorization checks made by the impersonation handler in seconds split by mode and decision.",
			StabilityLevel: metrics.ALPHA,
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"mode", "decision"},
	)
)

var registerMetrics sync.Once

// RegisterMetrics 把模拟相关指标注册到 legacy registry。
// 用 sync.Once 保证重复调用（例如测试中多次构造 server）不会因重复注册而 panic。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(impersonationAttemptsTotal)
		legacyregistry.MustRegister(impersonationAttemptsDurationSeconds)
		legacyregistry.MustRegister(impersonationAuthorizationAttemptsTotal)
		legacyregistry.MustRegister(impersonationAuthorizationAttemptsDurationSeconds)
	})
}

// RecordImpersonationAttempt 记录一次模拟请求的计数与耗时。
// mode 与 decision 是有限的枚举值，避免指标基数失控。
func RecordImpersonationAttempt(mode, decision string, duration time.Duration) {
	impersonationAttemptsTotal.WithLabelValues(mode, decision).Inc()
	impersonationAttemptsDurationSeconds.WithLabelValues(mode, decision).Observe(duration.Seconds())
}

// RecordImpersonationAuthorizationCall 记录模拟流程内部发起的单次授权检查。
// 与整体尝试分开统计，便于区分"请求慢"与"授权慢"。
func RecordImpersonationAuthorizationCall(mode, decision string, duration time.Duration) {
	impersonationAuthorizationAttemptsTotal.WithLabelValues(mode, decision).Inc()
	impersonationAuthorizationAttemptsDurationSeconds.WithLabelValues(mode, decision).Observe(duration.Seconds())
}
