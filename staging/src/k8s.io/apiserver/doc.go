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

// Package apiserver 提供构建 Kubernetes 风格 API Server 的基础设施。
//
// 该库既是 kube-apiserver 的实现基础，也是第三方构建聚合 API Server（extension API server）
// 的主要框架：聚合服务器通过 APIService 对象注册到主 apiserver，由主 apiserver 反向代理
// 对应 API 组的请求。绝大多数扩展场景应优先选择 CRD，而不是自建聚合服务器。
//
// 聚合服务器是用户自行部署的独立 Web 服务，向主 kube-apiserver 注册后接管特定 API 组。
// 这样一来，自定义 API 可以拥有与内置 API 一致的体验：类型化客户端、认证、授权与发现。
//
// # 关键包
//
// `apiserver` 库由以下几个关键包组成：
//
//   - `pkg/server`：库的核心，提供 `GenericAPIServer` 及搭建服务器的主要机制。
//   - `pkg/admission`：准入控制框架。开发者可编写自定义准入插件，校验或修改请求以执行
//     自定义策略；这是无需引入完整 API Server 即可扩展 Kubernetes 行为的常用方式。
//   - `pkg/authentication`：请求认证框架。
//   - `pkg/authorization`：请求授权框架。
//   - `pkg/endpoints`：构建 API Server REST 端点的机制。
//   - `pkg/registry`：API Server 的存储接口。
//
// # 实例化 GenericAPIServer
//
// `GenericAPIServer` 是扩展服务器的核心，负责组装并运行 HTTP 服务栈。可运行示例展示了
// 如何实例化 `GenericAPIServer`。
//
// # 构建扩展 API Server（API 聚合）
//
// 扩展 API Server 依托 API 聚合机制：主 apiserver（通常是 kube-apiserver）充当代理，
// 将特定 API 组（如 /apis/myextension.io/v1）的请求转发给已注册的扩展服务器，
// 并通过 APIService 对象完成配置。
//
// 多数场景下，扩展 Kubernetes API 的首选仍是自定义资源（CustomResourceDefinitions）。
//
// # 构建准入插件
//
// `pkg/admission` 提供将准入策略直接嵌入 apiserver 的方式。准入插件可在写操作期间
// 校验或修改对象；kube-apiserver 自身也借此实现多项核心系统能力。
//
// 多数扩展场景下，扩展准入控制的首选是策略驱动的动态准入
// （ValidatingAdmissionPolicies 或 MutatingAdmissionPolicies），
// 或 Webhook（ValidatingWebhookConfiguration 与 MutatingWebhookConfiguration）。
package apiserver
