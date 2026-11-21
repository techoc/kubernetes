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

const (
	// Subsystem 是所有指标共用的子系统名，构成指标名的前缀。
	Subsystem = "resourcepoolstatusrequest_controller"
)

var (
	// RequestsProcessed 统计已处理的 ResourcePoolStatusRequest 总数，成功与失败均计入，
	// 用作吞吐量和请求量的基准；按 driver_name 拆分以定位不同驱动的负载差异。
	RequestsProcessed = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      Subsystem,
			Name:           "requests_processed_total",
			Help:           "Total number of ResourcePoolStatusRequests processed",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name"},
	)

	// RequestProcessingErrors 统计请求处理过程中发生的错误数。
	// 与总数结合即可计算各驱动的错误率。
	RequestProcessingErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      Subsystem,
			Name:           "request_processing_errors_total",
			Help:           "Total number of errors encountered while processing ResourcePoolStatusRequests",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name"},
	)

	// RequestProcessingDuration 记录单个请求的处理耗时分布。
	// 使用指数桶（0.001 秒起、公比 2、共 15 桶）覆盖毫秒到数十秒的范围，
	// 便于同时观察正常延迟与慢请求长尾。
	RequestProcessingDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      Subsystem,
			Name:           "request_processing_duration_seconds",
			Help:           "Time taken to process a ResourcePoolStatusRequest",
			Buckets:        metrics.ExponentialBuckets(0.001, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name"},
	)
)

var registerOnce sync.Once

// Register 将所有指标注册到 legacy registry。
// sync.Once 保证即使控制器被重复启动，也不会因重复注册同一指标而 panic。
func Register() {
	registerOnce.Do(func() {
		legacyregistry.MustRegister(RequestsProcessed)
		legacyregistry.MustRegister(RequestProcessingErrors)
		legacyregistry.MustRegister(RequestProcessingDuration)
	})
}
