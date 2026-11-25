/*
Copyright 2024 The Kubernetes Authors.

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

// Among other files, this directory contains functionality for two
// stream proxies: streamtranslator.go and streamtunnel.go. Both of
// these proxies allow the inter-connection of WebSocket and SPDY
// streaming connections.
//
// The stream translator proxy is used for the RemoteCommand
// subprotocol (e.g. kubectl exec, cp, and attach), and it connects
// the output streams of a WebSocket connection (e.g. STDIN, STDOUT,
// STDERR, TTY resize, and error streams) to the input streams of a
// SPDY connection.
//
// 流隧道代理（stream tunnel proxy）把 SPDY 帧封装进 WebSocket 连接中传输，
// 用于 PortForward 子协议（例如 kubectl port-forward）。该代理透明地把 SPDY
// 帧数据编解码进 WebSocket 数据帧的 payload 来实现隧道。隧道的核心结构是
// TunnelingConnection，streamtunnel.go 中的其余大量代码用于在开始流传输之前
// 正确升级上游 SPDY 连接与下游 WebSocket 连接。
// 本包中的代理都位于 apiserver 与后端（kubelet/peer apiserver）之间，
// 负责协议适配与连接升级。WebSocket 侧的握手与流封装必须严格遵循对应子协议，
// 否则 kubectl 等客户端会出现难以诊断的挂起。
package proxy
