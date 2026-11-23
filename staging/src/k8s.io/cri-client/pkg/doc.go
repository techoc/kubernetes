/*
Copyright 2016 The Kubernetes Authors.

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

// Package remote contains gRPC implementation of internalapi.RuntimeService
// and internalapi.ImageManagerService.
//
// Package cri 提供 CRI（Container Runtime Interface，容器运行时接口）的远程 gRPC 客户端实现。
// 主要入口包括：
//   - remote_runtime.go：RuntimeService 的远程客户端，管理 PodSandbox 与容器的生命周期；
//   - remote_image.go：ImageManagerService 的远程客户端，负责镜像查询与拉取；
//   - logs：基于 CRI 流式接口获取容器日志；
//   - util：按平台解析运行时端点（unix socket / npipe / tcp）并建立连接。
package cri
