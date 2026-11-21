/*
Copyright 2021 The Kubernetes Authors.

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

// GarbageCollectorControllerSubsystem 定义垃圾回收控制器指标的子系统命名空间，
// 避免与其他控制器的同名指标发生冲突。
const GarbageCollectorControllerSubsystem = "garbagecollector_controller"

var (
	// GarbageCollectorResourcesSyncError 统计资源同步过程中的错误次数。
	// 该指标为 ALPHA 稳定性级别，用于观测垃圾回收器能否正常完成图重建。
	GarbageCollectorResourcesSyncError = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      GarbageCollectorControllerSubsystem,
			Name:           "resources_sync_error_total",
			Help:           "Number of garbage collector resources sync errors",
			StabilityLevel: metrics.ALPHA,
		})
)

// registerMetrics 保证指标只注册一次；重复注册同一指标会导致 legacyregistry  panic。
var registerMetrics sync.Once

// Register 向 legacyregistry 注册垃圾回收控制器的指标。
// 使用 sync.Once 包装是因为同一进程内该函数可能被调用多次，而重复注册会触发 panic。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(GarbageCollectorResourcesSyncError)
	})
}
