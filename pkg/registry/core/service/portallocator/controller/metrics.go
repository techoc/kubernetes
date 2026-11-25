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

package controller

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "nodeport_repair"
)

var (
	// nodePortRepairPortErrors 记录修复循环发现的错误数量，按错误类型区分：
	// leak、repair、full、outOfRange、duplicate、unknown。
	nodePortRepairPortErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "port_errors_total",
			Help:           "Number of errors detected on ports by the repair loop broken down by type of error: leak, repair, full, outOfRange, duplicate, unknown",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"type"},
	)
	// nodePortRepairReconcileErrors 记录修复循环未能修复其所发现错误的次数。
	nodePortRepairReconcileErrors = metrics.NewCounter(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "reconcile_errors_total",
			Help:           "Number of reconciliation failures on the nodeport repair reconcile loop",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

var registerMetricsOnce sync.Once

// registerMetrics 注册 NodePort 修复控制器的协调与错误统计指标。
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(nodePortRepairPortErrors)
		legacyregistry.MustRegister(nodePortRepairReconcileErrors)
	})
}
