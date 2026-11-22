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

package route

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	// subsystem is the name of this subsystem used for prometheus metrics.
	subsystem = "route_controller"

	// outcome label values for route_sync_total.
	// 取值固定为三态，便于计算变更率（changed）与失败率（error）。
	outcomeChanged = "changed"
	outcomeNoop    = "noop"
	outcomeError   = "error"
)

// registration 保证指标只注册一次。
var registration sync.Once

// routeSyncCount 记录每次路由对账的结果。trigger 区分周期触发与节点变更触发，
// 便于判断某类云 API 错误是否只出现在特定触发路径。
var routeSyncCount = metrics.NewCounterVec(&metrics.CounterOpts{
	Name:           "route_sync_total",
	Subsystem:      subsystem,
	Help:           "A metric counting the number of route reconciles with the cloud provider, labeled by the trigger (periodic or node_change) and the outcome (error if the reconcile failed, changed if at least one route was created or deleted, otherwise noop).",
	StabilityLevel: metrics.ALPHA,
}, []string{"outcome", "trigger"})

// registerMetrics 幂等注册；由控制器启动路径调用，避免 init 阶段过早创建指标。
func registerMetrics() {
	registration.Do(func() {
		legacyregistry.MustRegister(routeSyncCount)
	})
}

// recordRouteSync increments route_sync_total for a completed reconcile,
// deriving the outcome label from the reconcile error and whether any route
// was created or deleted.
// 错误优先于变更：一次部分成功的对账仍计为 error，避免掩盖失败。
func recordRouteSync(routesChanged bool, err error, trigger string) {
	outcome := outcomeNoop
	switch {
	case err != nil:
		outcome = outcomeError
	case routesChanged:
		outcome = outcomeChanged
	}
	routeSyncCount.WithLabelValues(outcome, trigger).Inc()
}
