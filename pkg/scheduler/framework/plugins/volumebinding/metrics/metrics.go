/*
Copyright 2018 The Kubernetes Authors.

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
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// VolumeSchedulerSubsystem 是调度器上报卷相关指标时使用的子系统名。
const VolumeSchedulerSubsystem = "scheduler_volume"

var (
	// VolumeBindingRequestSchedulerBinderCache 统计卷绑定器缓存的操作次数。
	VolumeBindingRequestSchedulerBinderCache = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      VolumeSchedulerSubsystem,
			Name:           "binder_cache_requests_total",
			Help:           "Total number for request volume binding cache",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation"},
	)
	// VolumeSchedulingStageFailed 统计卷调度阶段失败的操作次数。
	VolumeSchedulingStageFailed = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      VolumeSchedulerSubsystem,
			Name:           "scheduling_stage_error_total",
			Help:           "Volume scheduling stage error count",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation"},
	)
)

// RegisterVolumeSchedulingMetrics 由调度器调用，把卷调度指标注册到全局 registry。
// 卷绑定缓存本身只是一个库，无法自行决定何时注册指标。
func RegisterVolumeSchedulingMetrics() {
	legacyregistry.MustRegister(VolumeBindingRequestSchedulerBinderCache)
	legacyregistry.MustRegister(VolumeSchedulingStageFailed)
}
