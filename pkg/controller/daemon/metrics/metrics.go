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

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// DaemonControllerSubsystem 是 DaemonSet 控制器指标的子系统名前缀。
const DaemonControllerSubsystem = "daemonset_controller"

var (
	// DaemonsetRequeueSkips 统计因监听缓存过期而跳过的同步次数。
	// 缓存中的对象版本落后于队列中的键时，直接处理会基于陈旧状态做出错误决策，
	// 因此跳过并等待重新入队；该指标用于监控跳过的频率。
	DaemonsetRequeueSkips = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      DaemonControllerSubsystem,
			Name:           "stale_sync_skips_total",
			Help:           "Total number of DaemonSet syncs skipped due to a stale watch cache.",
			StabilityLevel: metrics.ALPHA,
		},
		// 按 group/resource 维度拆分，便于定位哪个资源的 watch 缓存经常落后。
		[]string{"group", "resource"},
	)
)

var registerMetrics sync.Once

// Register 注册 DaemonSet 控制器指标。
// sync.Once 防止控制器重启或多次初始化时重复注册导致 panic。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(DaemonsetRequeueSkips)
	})
}
