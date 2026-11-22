/*
Copyright 2015 The Kubernetes Authors.

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

// Package server contains the plumbing to create kubernetes-like API server command.
//
// 本包提供组装 Kubernetes 风格 API Server 所需的骨架：配置与默认值、鉴权/准入链、
// 处理器安装、优雅关闭，以及后台动态证书加载等。它只负责“通用”服务端装配，
// 具体资源与存储由调用方（如 kube-apiserver）注入，因此不依赖 k8s.io/kubernetes。
package server
