/*
Copyright 2020 The Kubernetes Authors.

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

// Package corev1 定义满足以下条件的函数：
//
// - 被多个核心组件（kube-scheduler、kubelet、kube-apiserver 等）共同使用；
// - 或者被一个核心组件与另一个 Kubernetes 项目（cluster-autoscaler、descheduler）共同使用。
//
// 并且这些函数都属于调度（scheduling）相关的特性。
//
// 这里强调“被多个调用方复用”的门槛，是为了防止把只服务单一组件的逻辑塞进公共包；
// 否则后续任何一处改动都要重新验证所有使用方，维护成本会被无限放大。
//
// 之所以按 core/v1 而非按组件命名，是为了对应 API 版本：
// 调度相关的辅助函数若依赖特定 API 版本，就放到同级版本化子包中。
package corev1
