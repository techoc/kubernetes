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

// TTLAfterFinishedSubsystem 是本控制器在指标体系中使用的子系统名称，
// 用于区分不同控制器上报的指标。
const TTLAfterFinishedSubsystem = "ttl_after_finished_controller"

var (
	// JobDeletionDurationSeconds 记录 Job 从符合删除条件到实际被删除所花费的时间，
	// 用于观测清理延迟。
	JobDeletionDurationSeconds = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      TTLAfterFinishedSubsystem,
			Name:           "job_deletion_duration_seconds",
			Help:           "The time it took to delete the job since it became eligible for deletion",
			StabilityLevel: metrics.ALPHA,
			// 从 100ms 起的指数分桶，最后一个桶约为 [27m, Inf)，覆盖从快速删除到长时间滞留的范围。
			Buckets: metrics.ExponentialBuckets(0.1, 2, 14),
		},
	)
)

// registerMetrics 保证注册逻辑只执行一次，避免重复注册同一指标导致 panic。
var registerMetrics sync.Once

// Register 向 legacyregistry 注册 TTLAfterFinished 控制器的指标；
// 借助 sync.Once，多次调用是安全的。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(JobDeletionDurationSeconds)
	})
}
