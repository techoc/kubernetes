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

// Package probe 提供健康探针的工具和状态信息。
//
// Kubernetes 使用探针来检测容器的健康状态，支持三种类型：
//   - LivenessProbe（存活探针）：判断容器是否存活，失败时重启容器
//   - ReadinessProbe（就绪探针）：判断容器是否就绪，失败时从 Service 端点中移除
//   - StartupProbe（启动探针）：判断容器是否启动完成，失败时重启容器
//
// 探针支持四种执行方式：HTTP GET、TCP Socket、Exec（执行命令）、gRPC。
// 每种方式对应一个子包（http、tcp、exec、grpc），实现各自的 Prober 接口。
//
// 核心类型：
//   - Result：探针执行结果（Success/Warning/Failure/Unknown）
//   - ProbeDialer：优化的 TCP 拨号器，减少 TIME-WAIT 状态的 socket 残留
package probe
