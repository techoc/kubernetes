/*
Copyright 2025 The Kubernetes Authors.

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

// +k8s:deepcopy-gen=package

// Package v1 包含指标配置 API。
//
// 该 API 只保留单一版本，仅以向后兼容的方式逐步新增字段。alpha 或 beta 特性的字段也可以加入，
// 但必须保证不修改默认值时这些特性保持禁用。
//
// "v1" 包名仅提醒使用者遵守 API 兼容规则，并不表示其中所有特性都具有稳定保证。
//
// 注意：建议组件维护者通过 `k8s.io/component-base/metrics` 操作
// `k8s.io/component-base/metrics/api/v1.MetricsConfiguration`，因为前者包含应用和校验配置的函数；
// 这些函数依赖本包成员，而本包无法移动或导入它们，否则会形成循环依赖。
//
// 中文补充：本包只定义配置数据结构，不包含校验与生效逻辑；
// 后者位于 k8s.io/component-base/metrics，以免形成循环依赖。

package v1
