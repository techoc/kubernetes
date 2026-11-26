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

// Package testing 提供用于单元测试的 fake Kubernetes 客户端。它无需运行真实
// kube-apiserver 即可模拟交互，使测试快速且自包含。
//
// # 范围与局限
//
// 该 fake 客户端刻意简化，不会也不可能完整复刻真实 kube-apiserver 的行为。
// 字段默认值、校验、status 管理、战略合并补丁、服务端 Apply 语义、高级字段选择器
// 等服务端行为均不支持，且没有支持的计划。
//
// 这是有意的设计取舍：为测试工具维护一个覆盖 apiserver 全部 API 面的高保真
// mock，复杂度过高，收益难以 justify。
//
// # 何时使用本包
//
// 适合验证代码在结构层面如何与 Kubernetes API 交互，例如：
//
//   - 断言发出了正确的 API 调用。
//   - 用预置响应驱动特定代码路径。
//
// # 何时不使用本包
//
// 若测试依赖 apiserver 的真实行为（例如执行校验、准确持久化资源、处理 apply
// 语义或产生真实的 watch 事件），应改用针对真实 kube-apiserver 的集成测试。
//
// # 贡献指南
//
// 要求 fake 客户端更贴近 apiserver 行为的 issue，应局限于它在其明确支持的
// 单元测试场景中的行为 bug。改进 fake 客户端的 PR 只有同时满足以下条件才会
// 被接受：
//
//   - 让 fake 客户端更易用于常见单元测试模式。
//   - 不引入显著复杂度。
//   - 所针对的场景确实更适合 fake 客户端，而非针对真实 apiserver 的集成测试。
//
// 我们对这类改动门槛很高：若相关测试场景能通过集成测试合理覆盖，就更倾向于
// 那条路径，而不是扩充 fake 客户端。
//
// 我们知道这种立场可能带来不便，感谢理解。目标是让本包保持简单、可维护，
// 并对自身能力边界诚实，从而在其设计覆盖的场景里始终可靠。
package testing
