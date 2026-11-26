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

// Package clientgo 是 Kubernetes API 的官方 Go 客户端。它提供一组标准的客户端与
// 工具，用于构建与 Kubernetes 集群交互的应用、控制器和 Operator。
//
// clientgo 是 Kubernetes API 的官方 Go 客户端。它以一组可组合的包提供从底层
// REST 传输、类型化/动态客户端到 Informer、Lister、工作队列等控制器基础
// 设施，既可在集群内 Pod 中运行，也可在集群外作为 CLI 或本地开发工具使用。
//
// # 主要包
//
//   - kubernetes：提供类型化 Clientset，用于操作内置的版本化 API 对象（如 Pod、
//     Deployment）。通常从这里入手。
//
//   - dynamic：提供动态客户端，可操作任意 Kubernetes 对象（包括自定义资源 CR）。
//     编写面向 CRD 的控制器时离不开它。
//
//   - discovery：发现集群支持的 API 组、版本和资源。
//
//   - tools/cache：控制器模式的基础，提供 Informer 与 Lister 等高效的缓存和同步
//     机制。
//
//   - tools/clientcmd：从 kubeconfig 文件加载客户端配置，集群外应用与 CLI 工具必备。
//
//   - rest：更底层的 RESTClient，封装与 API server 通信的细节。适合需要精细控制
//     请求的高级场景，例如使用非标准 REST 动词。
//
// # 连接 API
//
// 配置客户端连接 API server 主要有两种方式：
//
//  1. 集群内配置：运行在 Pod 中的应用可直接使用 `rest.InClusterConfig()`。它会自动
//     使用 Pod 的 ServiceAccount 完成认证，是控制器和 Operator 的推荐方式。
//
//  2. 集群外配置：本地开发或命令行工具通过 `clientcmd` 从 kubeconfig 文件加载配置。
//
// `rest.Config` 可精细控制客户端的性能与可靠性，关键设置包括：
//
//   - QPS：向 API server 发起请求的每秒最大次数。
//   - Burst：单次突发允许发出的最大请求数。
//   - Timeout：单个请求的超时时间。
//
// # 操作 API 对象
//
// 配置完成后，即可用客户端操作集群中的对象。
//
//   - 类型化 Clientset（`kubernetes` 包）为内置对象提供强类型接口。
//
//   - 动态客户端（`dynamic` 包）通过 `unstructured.Unstructured` 操作任意对象，
//     包括自定义资源。
//
//   - 自定义资源（CRD）可用 `k8s.io/code-generator` 生成类型化客户端、Informer 和
//     Lister，`sample-controller` 是该模式的参考示例。
//
//   - Server-Side Apply 通过跟踪字段所有权让多方共同管理同一对象，避免相互覆盖，
//     并提供冲突解决机制。`applyconfigurations` 包提供了这种声明式方式所需的类型。
//
// # 处理 API 错误
//
// 与 API 交互时健壮的错误处理必不可少。`k8s.io/apimachinery/pkg/api/errors` 提供
// 检查错误的工具函数，可判断资源不存在或已存在等常见情况，帮助控制器实现稳健、
// 幂等的 reconcile 逻辑。
//
// # 编写控制器
//
// 控制器模式是 Kubernetes 的核心：控制器观察集群状态，并推动其趋近期望状态。
//
//   - `tools/cache` 提供该模式的构件：Informer 监听 API server 并维护本地缓存，
//     Lister 提供只读访问，Workqueue 将事件发现与处理解耦。
//
//   - 高可用部署下会有多个控制器实例，通过 leader election（`tools/leaderelection`）
//     保证同一时刻只有一个实例处于活跃状态。
//
//   - 客户端特性门控可开关 `client-go` 中的实验特性，通过 `rest.Config` 配置。
package clientgo
