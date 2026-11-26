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

// Package v1alpha1 是 DRA 健康 gRPC 接口的 alpha 实现。
// 它已由 k8s.io/kubelet/pkg/apis/dra-health/v1 取代。
// 两者使用同一份 proto。本包仅为过渡期兼容保留，
// 过渡期中使用，1.40 左右移除。
//
// 这里刻意不使用 "Deprecated:" 标记：过渡期内 kubelet 与
// kubeletplugin 辅助包仍须使用它，加了标记会让 linter 报错。
// 否则该标记会让 linter 报出这些导入。
//
// 已弃用：v1alpha1 与 v1 使用相同的 proto，仅保留用于兼容仍在跑旧版的 kubelet 与驱动。
// 过渡期结束后整包删除；此处刻意不加 Deprecated 标记，否则会被 linter 误报。
package v1alpha1
