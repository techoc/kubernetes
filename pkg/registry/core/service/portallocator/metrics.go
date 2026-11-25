/*
Copyright 2022 The Kubernetes Authors.

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

package portallocator

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "kube_apiserver"
	subsystem = "nodeport_allocator"
)

var (
	// nodePortAllocated 记录 NodePort Service 已分配的端口数量。
	nodePortAllocated = metrics.NewGauge(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "allocated_ports",
			Help:           "Gauge measuring the number of allocated NodePorts for Services",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// nodePortAvailable 记录 NodePort Service 可分配的端口数量。
	nodePortAvailable = metrics.NewGauge(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "available_ports",
			Help:           "Gauge measuring the number of available NodePorts for Services",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// nodePortAllocations 按静态或动态分配模式统计端口分配总次数。
	nodePortAllocations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "allocation_total",
			Help:           "Number of NodePort allocations",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"scope"},
	)
	// nodePortAllocationErrors 按静态或动态分配模式统计端口分配失败次数。
	nodePortAllocationErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "allocation_errors_total",
			Help:           "Number of errors trying to allocate NodePort",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"scope"},
	)
)

var registerMetricsOnce sync.Once

// registerMetrics 通过 sync.Once 注册 NodePort 分配/释放/容量指标，避免多个分配器重复注册。
func registerMetrics() {
	registerMetricsOnce.Do(func() {
		legacyregistry.MustRegister(nodePortAllocated)
		legacyregistry.MustRegister(nodePortAvailable)
		legacyregistry.MustRegister(nodePortAllocations)
		legacyregistry.MustRegister(nodePortAllocationErrors)
	})
}

// metricsRecorderInterface 定义 NodePort 分配器指标记录接口。
type metricsRecorderInterface interface {
	setAllocated(allocated int)
	setAvailable(available int)
	incrementAllocations(scope string)
	incrementAllocationErrors(scope string)
}

// metricsRecorder 实现 metricsRecorderInterface。
type metricsRecorder struct{}

func (m *metricsRecorder) setAllocated(allocated int) {
	nodePortAllocated.Set(float64(allocated))
}

func (m *metricsRecorder) setAvailable(available int) {
	nodePortAvailable.Set(float64(available))
}

func (m *metricsRecorder) incrementAllocations(scope string) {
	nodePortAllocations.WithLabelValues(scope).Inc()
}

func (m *metricsRecorder) incrementAllocationErrors(scope string) {
	nodePortAllocationErrors.WithLabelValues(scope).Inc()
}

// emptyMetricsRecorder 是 metricsRecorderInterface 的空实现，用于禁用指标记录。
type emptyMetricsRecorder struct{}

func (*emptyMetricsRecorder) setAllocated(allocated int)             {}
func (*emptyMetricsRecorder) setAvailable(available int)             {}
func (*emptyMetricsRecorder) incrementAllocations(scope string)      {}
func (*emptyMetricsRecorder) incrementAllocationErrors(scope string) {}
