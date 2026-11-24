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

// Package streaming 提供 CRI 流式服务的服务端接口与实现。
// 工作流程大致如下：
//  1. kubelet 调用容器运行时的 GetExec/GetAttach/GetPortForward RPC，拿到一个流式 URL 和 token；
//  2. kubelet 用该 URL 向本服务的 HTTP 端点发起请求；
//  3. server.go 解析 token 并从 requestCache 中取出缓存的原始请求；
//  4. 请求被路由到 remotecommand（exec/attach）或 portforward 子包的具体处理器；
//  5. 处理器与客户端协商 HTTP Upgrade 或 WebSocket 协议后，开始双向数据流转发。
package streaming
