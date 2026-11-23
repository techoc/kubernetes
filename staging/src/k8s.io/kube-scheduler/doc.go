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

// Package kubescheduler 是 k8s.io/kube-scheduler 模块的根包，本身不提供任何逻辑，
// 存在的意义是给 staging 目录一个可声明的 Go 包路径。
//
// 可复用的代码都在子包里：config/v1 放调度器配置的 API 类型，
// extender/v1 放 scheduler extender 的 HTTP 交互协议，
// framework 放调度框架的接口和共享数据结构。
// 这些类型被 k/k 主仓库的调度器引用，放 staging 而非直接放主仓库，
// 是为了让外部实现只依赖稳定的 API 契约，同时避免循环依赖。
package kubescheduler
