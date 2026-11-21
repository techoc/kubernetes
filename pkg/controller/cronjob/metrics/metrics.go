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

// CronJobControllerSubsystem 是 CronJob 控制器所有指标共用的 subsystem 名称。
const CronJobControllerSubsystem = "cronjob_controller"

var (
	// CronJobCreationSkew 记录 CronJob 计划运行时间与实际创建 Job 时间之间的偏差，
	// 用于观测控制器的调度延迟。
	CronJobCreationSkew = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      CronJobControllerSubsystem,
			Name:           "job_creation_skew_duration_seconds",
			Help:           "Time between when a cronjob is scheduled to be run, and when the corresponding job is created",
			StabilityLevel: metrics.STABLE,
			Buckets:        metrics.ExponentialBuckets(1, 2, 10),
		},
	)
)

var registerMetrics sync.Once

// Register 注册 CronJobController 的指标。
// 使用 sync.Once 是因为同一进程可能被多次调用（如测试中重建控制器），
// 而向 legacyregistry 重复注册同名指标会直接 panic。
func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(CronJobCreationSkew)
	})
}
