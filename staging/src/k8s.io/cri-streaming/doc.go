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

// Package cristreaming contains the staged module root for Kubernetes CRI streaming.
//
// Package cristreaming 是 Kubernetes CRI 流式服务模块的根目录包。
// CRI 流式服务负责为 exec（执行命令）、attach（附着容器）和 portforward（端口转发）
// 这类需要长连接和数据流的功能提供 HTTP/WebSocket 服务端实现。
// 本包本身只作为模块入口，具体实现位于 pkg/streaming 及其子包中。
package cristreaming
