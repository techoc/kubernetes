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

// Package openapiv3 负责为 CRD 生成并维护 OpenAPI v3 规范。
package openapiv3

import (
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// regenerationCounter 统计 OpenAPI v3 规范的重建次数，按 group、version、
// 触发重建的 CRD 与原因拆分，用于定位频繁重建的来源。
var (
	regenerationCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Name:           "apiextensions_openapi_v3_regeneration_count",
			Help:           "Counter of OpenAPI v3 spec regeneration count broken down by group, version, causing CRD and reason.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "version", "crd", "reason"},
	)
)

func init() {
	// 注册到 legacy registry，随 apiserver 统一的 metrics 端点暴露。
	legacyregistry.MustRegister(regenerationCounter)
}
