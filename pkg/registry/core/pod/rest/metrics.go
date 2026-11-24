/*
Copyright 2020 The Kubernetes Authors.

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

package rest

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "kube_apiserver"
	subsystem = "pod_logs"

	usageEnforce     = "enforce_tls"
	usageSkipAllowed = "skip_tls_allowed"
)

var (
	// podLogsUsage 统计 Pod 日志后端 TLS 校验策略的使用情况，帮助识别跳过验证配置的实际使用量。
	podLogsUsage = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "insecure_backend_total",
			Help:           "Total number of requests for pods/logs sliced by usage type: enforce_tls, skip_tls_allowed, skip_tls_denied",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"usage"},
	)

	// podLogsTLSFailure 统计 API Server 连接 Kubelet 日志端点时因 TLS 证书验证失败的次数。
	podLogsTLSFailure = metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "backend_tls_failure_total",
			Help:           "Total number of requests for pods/logs that failed due to kubelet server TLS verification",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

var registerMetricsOnce sync.Once

// registerMetrics 使用 sync.Once 注册全局指标，避免多个 LogREST 实例重复注册相同指标名。
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(podLogsUsage)
		legacyregistry.MustRegister(podLogsTLSFailure)
	})
}
