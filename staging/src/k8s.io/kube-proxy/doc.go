/*
Copyright 2021 The Kubernetes Authors.

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

// Package kubeproxy 是 kube-proxy 组件的 Go 库入口。
//
// 这里几乎不放逻辑：真正可复用的 API 在 config/v1alpha1 子包中，kube-proxy 命令本身在
// cmd/kube-proxy 下。之所以保留这个顶层包，是为了让 staging/src/k8s.io/kube-proxy
// 作为独立的 k8s.io 模块存在，便于被其他仓库按版本依赖，而不必反向依赖 k8s.io/kubernetes。
//
// 子包各自声明自己的包文档，这里不与它们重复描述模块用途。
package kubeproxy
