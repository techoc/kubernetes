/*
Copyright 2018 The Kubernetes Authors.

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

// 指令 +k8s:deepcopy-gen=package 等下面几行驱动代码生成，
// 为整个包生成深拷贝方法和 OpenAPI 模型。
// +k8s:deepcopy-gen=package
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.kube-proxy.config.v1alpha1

// 生成指令说明：+groupName=kubeproxy.config.k8s.io 指定 API 组名，
// 供代码生成器（如 openapi-gen）确定组版本对应关系。
// +groupName=kubeproxy.config.k8s.io

// Package v1alpha1 定义 kube-proxy 组件配置的外部 API 版本。
//
// 这里的类型描述的是「用户如何配置 kube-proxy」，不是集群中由 apiserver 管理的资源：
// 配置通常从本地文件或 ConfigMap 读入，经过解码和默认值填充后交给内部版本使用。
// 之所以单独放在一个带版本号的包中，是为了让配置文件格式可以独立演进，
// 并沿用 Kubernetes 组件配置统一的 scheme/序列化机制；生成器据此产出
// 深拷贝和 OpenAPI 定义，无需手写这些机械代码。
package v1alpha1
