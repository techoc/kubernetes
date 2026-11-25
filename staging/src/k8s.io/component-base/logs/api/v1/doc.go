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
// +k8s:openapi-model-package=io.k8s.component-base.logs.api.v1

// Package v1 是日志组件的配置 API。单一版本、只做向后兼容的字段追加；
// 不稳定能力由 LoggingAlphaOptions、LoggingBetaOptions 两个特性门控把关，
// 避免命令行参数在无意中依赖尚未稳定的行为。
package v1
