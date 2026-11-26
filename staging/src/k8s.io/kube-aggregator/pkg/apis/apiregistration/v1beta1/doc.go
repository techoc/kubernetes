/*
Copyright 2016 The Kubernetes Authors.

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

// 指令 +k8s:deepcopy-gen=package 为包内类型生成 DeepCopy 方法；
// 指令 +k8s:protobuf-gen=package 生成 protobuf 序列化代码；
// 指令 +k8s:conversion-gen=k8s.io/kube-aggregator/pkg/apis/apiregistration 生成内部版本与本版本之间的转换；
// 指令 +k8s:openapi-gen=true 生成 OpenAPI 定义；
// 指令 +k8s:defaulter-gen=TypeMeta 只为内嵌 TypeMeta 的资源类型生成默认值函数；
// 指令 +k8s:prerelease-lifecycle-gen=true 生成预发布生命周期元数据；
// 指令 +k8s:openapi-model-package=io.k8s.kube-aggregator.pkg.apis.apiregistration.v1beta1
// 指定本包生成模型的 OpenAPI 包名，避免同名类型在不同版本间撞名。
// 生成指令说明：+groupName=apiregistration.k8s.io 声明这个包对外服务的 API 组名。
// 这些指令都在包注释之前，生成器认的就是这个位置；下面另起一段写包本身是做什么的。

// +k8s:deepcopy-gen=package
// +k8s:protobuf-gen=package
// +k8s:conversion-gen=k8s.io/kube-aggregator/pkg/apis/apiregistration
// +k8s:openapi-gen=true
// +k8s:defaulter-gen=TypeMeta
// +k8s:prerelease-lifecycle-gen=true
// +k8s:openapi-model-package=io.k8s.kube-aggregator.pkg.apis.apiregistration.v1beta1

// +groupName=apiregistration.k8s.io

// Package v1beta1 定义 API Registration API，负责把某个 API `Group`/`Version`
// 注册到另一个类似 Kubernetes 的 API server 上。
// `APIService` 通过 `APIServiceSpec` 以及通用的 `TypeMeta`、`ObjectMeta`
// 保存目标 server 的信息；`APIServiceSpec` 承载聚合所需的全部配置。
// 对指定 `Group`/`Version` 的请求，先用 `CABundle` 校验目标证书
// （开发时可用 `InsecureSkipTLSVerify` 跳过校验），再转发到 `ServiceReference`
// 指向的 Service（443 端口）。`Priority` 决定该 API 组在整体 discovery 文档中的顺序。
// 返回状态是一组聚合相关的 condition；当前只有 "Available"，
// 为 true 表示请求会被重定向到指定的 API server。
package v1beta1
