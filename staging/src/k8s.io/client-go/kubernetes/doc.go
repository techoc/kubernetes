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

// Package kubernetes holds packages which implement a clientset for Kubernetes
// APIs.
//
// Package kubernetes 汇总为 Kubernetes 各 API 组生成的强类型 clientset：
// 顶层 Clientset 聚合了 core、apps、batch 等各组的客户端，
// 每个组客户端再按版本（如 v1、v1beta1）暴露资源操作。
// 这里的代码基本都是生成产物，类型化接口的语义与对应 API 一致。
package kubernetes
