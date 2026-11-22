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

package slis

import (
	"net/http"
	"sync"

	"k8s.io/component-base/metrics"
)

var (
	// 注册只应发生一次；重复注册到同一个 Registry 会 panic。
	installOnce          = sync.Once{}
	installWithResetOnce = sync.Once{}
)

// mux 抽象出 http.ServeMux 的最小能力，便于测试替换。
type mux interface {
	Handle(path string, handler http.Handler)
}

// SLIMetrics 以只读方式暴露 SLI 指标。
type SLIMetrics struct{}

// Install adds the DefaultMetrics handler
// Install 在 /metrics/slis 路径挂载 SLI 指标；Register 通过 sync.Once 保证只执行一次。
func (s SLIMetrics) Install(m mux) {
	installOnce.Do(func() {
		Register(Registry)
	})
	m.Handle("/metrics/slis", metrics.HandlerFor(Registry, metrics.HandlerOpts{}))
}

// SLIMetricsWithReset 在 DELETE 请求时重置 SLI 指标，供测试或运维清理使用。
type SLIMetricsWithReset struct{}

// Install adds the DefaultMetrics handler
// Install 与 SLIMetrics.Install 类似，但使用支持 DELETE 复位的 handler。
func (s SLIMetricsWithReset) Install(m mux) {
	installWithResetOnce.Do(func() {
		Register(Registry)
	})
	m.Handle("/metrics/slis", metrics.HandlerWithReset(Registry, metrics.HandlerOpts{}))
}
