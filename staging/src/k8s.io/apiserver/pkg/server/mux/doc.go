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

// Package mux 包含 API 的 http 路径复用（多路复用）抽象。
// mux 包封装了 API Server 的 HTTP 路径复用（路由）能力。核心是 PathRecorderMux：
// 它在标准库 ServeMux 之上记录已注册路径，用于启动时生成 /paths、/metrics 等自省端点，
// 并支持在注册阶段检测路径冲突。之所以不直接暴露 http.ServeMux，是因为 API Server
// 需要在运行期枚举路由（例如 discovery 与健康检查），而标准库不提供该能力。
package mux
