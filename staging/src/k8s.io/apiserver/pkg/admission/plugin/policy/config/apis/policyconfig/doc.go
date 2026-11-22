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

// +k8s:deepcopy-gen=package

// Package policyconfig defines the configuration for the validating and mutating admission policy plugins.
//
// 本包是策略类准入插件（ValidatingAdmissionPolicy / MutatingAdmissionPolicy）
// 的配置 API 内部版本，仅用于 apiserver 的配置解码与转换，不作为集群 API 暴露。
package policyconfig
