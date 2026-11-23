/*
Copyright 2019 The Kubernetes Authors.

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

package aggregator

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// regeneration 指标用于观察 OpenAPI v2 规范重建的频率与耗时；
// 计数按 APIService 和触发原因拆分，便于定位是哪个聚合 API 的变动引发了重建，
// 而耗时只按原因拆分，避免高基数标签。
var (
	regenerationCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "aggregator_openapi_v2_regeneration_count",
			Help:           "Counter of OpenAPI v2 spec regeneration count broken down by causing APIService name and reason.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"apiservice", "reason"},
	)
	regenerationDurationGauge = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Name:           "aggregator_openapi_v2_regeneration_duration",
			Help:           "Gauge of OpenAPI v2 spec regeneration duration in seconds.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)
)

func init() {
	// 在包初始化时注册，保证使用该包的二进制只要导入就暴露这些指标，
	// 不需要每个调用方显式注册；MustRegister 在重复注册时直接 panic 以便尽早发现错误。
	legacyregistry.MustRegister(regenerationCounter)
	legacyregistry.MustRegister(regenerationDurationGauge)
}
