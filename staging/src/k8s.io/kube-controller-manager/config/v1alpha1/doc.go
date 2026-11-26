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

// +k8s:deepcopy-gen=package
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.kube-controller-manager.config.v1alpha1

// +groupName=kubecontrollermanager.config.k8s.io

// Package v1alpha1 定义 kube-controller-manager 对外暴露的配置文件 API。
//
// 这里放的是配置对象而不是运行时对象：它们描述“控制器管理器应该如何启动”，
// 由二进制启动时从文件反序列化，不经过 API Server，也不需要走准入或持久化流程。
// 之所以单独成组（kubecontrollermanager.config.k8s.io）并带 v1alpha1 版本，
// 是为了让配置结构能独立于控制器实现演进，同时保留后续兼容与升级的路径。
//
// 因为配置对象会被当作 runtime.Object 参与 scheme 的注册与拷贝；
// OpenAPI 模型，供文档与外部工具按固定模型包名引用；
// 生成指令说明：+groupName=... 声明 API 组名，生成的类型注册代码据此确定 group/version。
// 这些指令必须原样保留，生成物由 make update 统一刷新，不要手工改动。
package v1alpha1
