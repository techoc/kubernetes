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

package app

import (
	"context"
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// subSystemName is the name of this subsystem name used for prometheus metrics.
	subSystemName = "cloud_provider_webhook"
)

// registerables 只是 Registerable 的切片别名，用于统一批量注册。
type registerables []metrics.Registerable

// registerMetricsOnce 保证注册只执行一次；重复注册会触发 legacyregistry panic。
var registerMetricsOnce sync.Once

// RegisterMetrics registers the cloud-provider webhook metrics with the legacy
// registry. It is invoked from webhook server setup at runtime rather than from
// an init() function, so that metric feature gates (e.g. NativeHistograms) have
// already been parsed and applied by the time the histogram metrics are created.
func RegisterMetrics() {
	registerMetricsOnce.Do(func() {
		for _, metric := range toRegister {
			legacyregistry.MustRegister(metric)
		}
	})
}

var (
	// requestTotal 按状态码与 webhook 名称统计请求数。标签取值有限，
	// 不会随请求内容增长，因而适合做长期指标。
	requestTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "request_total",
			Subsystem:      subSystemName,
			Help:           "Number of HTTP requests partitioned by status code.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code", "webhook"},
	)

	// requestLatency 的桶针对 webhook 调用的典型耗时分布设计，
	// 便于区分本地快速失败与真实的云侧慢调用。
	requestLatency = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Name:           "request_duration_seconds",
			Subsystem:      subSystemName,
			Help:           "Request latency in seconds. Broken down by status code.",
			Buckets:        []float64{0.25, 0.5, 0.7, 1, 1.5, 3, 5, 10},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code", "webhook"},
	)

	// toRegister 是延迟注册的指标集合，顺序无关紧要。
	toRegister = registerables{
		requestTotal,
		requestLatency,
	}
)

// recordRequestTotal 记录一次 webhook 请求。使用 ctx 以便在请求取消时仍能正确归属到当前上下文。
// RecordRequestTotal increments the total number of requests for the webhook.
func recordRequestTotal(ctx context.Context, code string, webhookName string) {
	requestTotal.WithContext(ctx).With(map[string]string{"code": code, "webhook": webhookName}).Add(1)
}

// recordRequestLatency 记录一次 webhook 请求耗时；code 用于区分成功与各类失败路径。
// 指标只做观测，调用方不应因记录失败而改变请求处理结果。
// RecordRequestLatency measures request latency in seconds for the delegated authorization. Broken down by status code.
func recordRequestLatency(ctx context.Context, code string, webhookName string, latency float64) {
	requestLatency.WithContext(ctx).With(map[string]string{"code": code, "webhook": webhookName}).Observe(latency)
}
