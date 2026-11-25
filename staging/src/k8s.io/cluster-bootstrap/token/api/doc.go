/*
Copyright 2017 The Kubernetes Authors.

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

// Package api (k8s.io/cluster-bootstrap/token/api) 定义 bootstrap token 的 API 常量，
// 供 k8s.io/kubernetes/pkg/controller/bootstrap 中的 BootstrapSigner 与
// TokenCleaner 控制器使用。
//
// 本包只包含常量，不包含逻辑。这些键名、前缀和正则共同定义了 bootstrap token
// 在 Kubernetes API 中的序列化格式（Secret 类型、Data 键、token ID/Secret 的
// 字符集与长度），因此对外呈现的是稳定契约：控制器、kubeadm 和 kubelet 的引导
// 流程都依赖它们保持一致，不能随意修改。
package api
