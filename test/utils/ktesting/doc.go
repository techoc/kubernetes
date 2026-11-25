/*
Copyright 2023 The Kubernetes Authors.

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

// ktesting 包有多个变体，每个变体都在前一个变体的基础上
// 增加更多功能：
//   - [k8s.io/klog/v2/ktesting]: 上下文日志，调用方决定
//     所有细节
//   - [k8s.io/kubernetes/test/utils/ktesting]: 对 Go testing 和
//     Ginkgo 的抽象
//   - [k8s.io/kubernetes/test/utils/client-go/ktesting]: 针对
//     Kubernetes 测试定制
//
// 使用者只需修改 import 路径，即可从较简单的变体切换到
// 较复杂的变体。较复杂的变体有额外的包
// 依赖。
//
// 此变体有自己的主张。导入它会：
// - 增加 -v 命令行 flag
// - 启用对复杂数据类型的更好转储
// - 将默认详细程度设为 5（可通过 [SetDefaultVerbosity] 修改）
//
// 它还为单元测试和集成测试添加了额外的 API 和类型，
// 这些对 klog 来说过于实验性，和/或与日志无关。
// ktesting 包本身负责管理测试上下文，并将截止时间、
// 超时、取消以及一些常用属性作为 API 的
// 一等成员。
package ktesting
