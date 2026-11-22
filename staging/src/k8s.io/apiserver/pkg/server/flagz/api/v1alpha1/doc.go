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
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.apiserver.pkg.server.flagz.api.v1alpha1

// Package v1alpha1 contains API Schema definitions for the flagz v1alpha1 API group
//
// Package v1alpha1 定义 flagz 端点的 v1alpha1 API。
// flagz 用于在运行时以只读方式暴露 apiserver 进程的启动参数（flags），
// 便于排查配置漂移；因此该组只包含读取语义的类型，且被标记为 alpha，
// 字段可能在不保证兼容的情况下调整。
package v1alpha1
