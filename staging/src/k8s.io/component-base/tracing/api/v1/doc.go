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

// +k8s:deepcopy-gen=package
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.component-base.tracing.api.v1

// Package v1 是 tracing 的配置 API。这里只保留一个版本，后续新增字段必须
// 向后兼容；alpha/beta 字段可以存在，但默认值必须让特性保持关闭，
// 这样未显式配置的组件升级后行为不变。
// 包名中的 v1 只表示适用 API 兼容性规则，并不代表其中所有特性都已稳定。

package v1
