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

// Package legacyregistry 维护进程级全局指标注册表，并暴露 /metrics 处理器。
// 它保留 Kubernetes 早期组件习惯的包级注册函数（MustRegister 等），
// 内部仍由 metrics 包执行 stability level 与弃用过滤，因此是“legacy”接口而非裸 prometheus registry。
package legacyregistry

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"k8s.io/component-base/metrics"
)

var (
	defaultRegistry = metrics.NewKubeRegistry()
	// DefaultGatherer 暴露全局注册表的收集器。
	// 供需要直接 Gather 的调用方使用，避免再引用 prometheus 全局 registry。
	DefaultGatherer metrics.Gatherer = defaultRegistry
	// Reset 调用全局注册表的重置操作。
	// 主要供测试在用例间清空全局状态。
	Reset = defaultRegistry.Reset
	// MustRegister 使用全局注册表注册可注册指标。
	MustRegister = defaultRegistry.MustRegister
	// RawMustRegister 使用全局注册表注册 prometheus collector，并绕过指标稳定性框架。
	//
	// Deprecated
	// 绕过稳定性框架，仅用于第三方 collector；新代码应使用 MustRegister。
	RawMustRegister = defaultRegistry.RawMustRegister

	// Register 使用全局注册表注册可收集指标。
	Register = defaultRegistry.Register

	// Registerer 暴露全局注册器。
	Registerer = defaultRegistry.Registerer

	// processStart 在 init 时固定，随后作为响应头暴露给抓取端。
	processStart time.Time
)

func init() {
	// 进程级与 Go 运行时指标由 prometheus 官方 collector 提供，直接注册以保留其原有指标名。
	RawMustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	RawMustRegister(collectors.NewGoCollector(collectors.WithGoCollectorRuntimeMetrics(collectors.MetricsAll)))
	// 注册 registry 自身的元指标（如注册指标数量）。
	defaultRegistry.RegisterMetaMetrics()
	processStart = time.Now()
}

// Handler 返回 DefaultGatherer 的 HTTP handler，已使用 InstrumentHandler 插桩，handler 名为 "prometheus"。
// 通过 ProcessStartTime 把启动时间放进响应头，抓取端据此计算进程 uptime。
func Handler() http.Handler {
	return promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, promhttp.HandlerFor(defaultRegistry, promhttp.HandlerOpts{ProcessStartTime: processStart}))
}

// HandlerWithReset 返回 DefaultGatherer 的 HTTP handler，并在 HTTP 方法为 DELETE 时重置注册表。
// 提供 DELETE 复位能力，便于测试或需要清理指标的组件。
func HandlerWithReset() http.Handler {
	return promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer,
		metrics.HandlerWithReset(defaultRegistry, metrics.HandlerOpts{ProcessStartTime: processStart}))
}

// CustomRegister 使用全局注册表注册自定义 collector。
// 同时向 prometheus 默认 registry 注册，以兼容直接使用全局 registry 的代码；
// 返回值只反映 KubeRegistry 的错误。
func CustomRegister(c metrics.StableCollector) error {
	err := defaultRegistry.CustomRegister(c)

	// TODO(RainbowMango): 可以考虑用错误包装处理此错误（Golang 1.13）。
	_ = prometheus.Register(c)

	return err
}

// CustomMustRegister 使用全局注册表注册自定义 collector。
// 与 CustomRegister 相同，但任一注册失败时 panic。
func CustomMustRegister(cs ...metrics.StableCollector) {
	defaultRegistry.CustomMustRegister(cs...)

	for _, c := range cs {
		prometheus.MustRegister(c)
	}
}

// GetProcessStart 返回 processStart 值。
// 返回 init 时固定的进程启动时间。
func GetProcessStart() time.Time {
	return processStart
}
