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

// Package metrics 是 Kubernetes 组件统一的指标门面：封装 prometheus 客户端，
// 在注册时按 stability level（ALPHA/BETA/STABLE）与弃用版本过滤指标，
// 并提供计时直方图、原生直方图等扩展。
// 组件通过 metrics 与 legacyregistry 暴露 /metrics，避免直接依赖 prometheus 的注册语义。
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Labels 表示标签名到标签值的映射集合。
// 底层类型与 prometheus.Labels 一致，便于在 metrics 与 prometheus 两套 API 间传递；
// 但它是独立类型，跨包传递时需显式转换。
type Labels prometheus.Labels
