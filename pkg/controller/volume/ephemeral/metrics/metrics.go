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

// EphemeralVolumeSubsystem 是本控制器所有指标共用的 Prometheus 子系统名，用于区分其他控制器的同名指标。
const EphemeralVolumeSubsystem = "ephemeral_volume_controller"

var (
	// EphemeralVolumeCreateAttempts 统计 PersistentVolumeClaims().Create 的调用次数，
	// 无论成功还是失败都计数；与失败数配合即可算出创建成功率。
	EphemeralVolumeCreateAttempts = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      EphemeralVolumeSubsystem,
			Name:           "create_total",
			Help:           "Number of PersistentVolumeClaim creation requests",
			StabilityLevel: metrics.ALPHA,
		})
	// EphemeralVolumeCreateFailures 只统计失败的 PersistentVolumeClaims().Create 调用，
	// 用于监控临时卷 PVC 创建异常，例如配额不足或 API 拒绝。
	EphemeralVolumeCreateFailures = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      EphemeralVolumeSubsystem,
			Name:           "create_failures_total",
			Help:           "Number of PersistentVolumeClaim creation requests",
			StabilityLevel: metrics.ALPHA,
		})
)

var registerMetrics sync.Once

// RegisterMetrics 把本控制器的指标注册到 legacyregistry。
// 用 sync.Once 包裹是必需的：控制器可能被重复初始化，而 Prometheus 不支持重复注册同名指标。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(EphemeralVolumeCreateAttempts)
		legacyregistry.MustRegister(EphemeralVolumeCreateFailures)
	})
}
