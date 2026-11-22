/*
Copyright 2014 The Kubernetes Authors.

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

// Package securitycontext 包含安全上下文（Security Context）的 API 实现。
// 主要功能：
//   - 提供 Pod 和 Container 级别的 SecurityContext 访问器（Accessor）和变更器（Mutator）接口
//   - 计算容器的有效安全上下文（合并 Pod 和 Container 级别的配置，Container 优先级更高）
//   - 转换 ProcMount 类型为运行时 MaskedPaths 和 ReadonlyPaths
//   - 检测 Windows HostProcess 容器的请求
//   - 提供测试用的伪造安全上下文
package securitycontext
