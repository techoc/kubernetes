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

// +k8s:deepcopy-gen=package
// +k8s:protobuf-gen=package
// +k8s:conversion-gen=k8s.io/kube-aggregator/pkg/apis/apiregistration
// +k8s:openapi-gen=true
// +k8s:defaulter-gen=TypeMeta
// +k8s:prerelease-lifecycle-gen=true
// +k8s:openapi-model-package=io.k8s.kube-aggregator.pkg.apis.apiregistration.v1

// +groupName=apiregistration.k8s.io

// 指令 +k8s:deepcopy-gen=package、+k8s:protobuf-gen=package 等是交给代码生成器的开关，
// 不是运行期逻辑：深拷贝、线格式序列化、版本转换、OpenAPI、默认值和生命周期标记都由生成器
// 按这些开关产出。把生成结果隔离在独立文件里，手写的注册代码才能在生成文件缺失时照常编译。
// 生成指令说明：+groupName 声明本包归属 apiregistration.k8s.io，供生成器与发现文档确定 API 组名。

// Package v1 定义 API Registration API，负责把某个 API `Group`/`Version`
// 注册到另一个类似 Kubernetes 的 API server 上。
// `APIService` 通过 `APIServiceSpec` 以及通用的 `TypeMeta`、`ObjectMeta`
// 保存目标 server 的信息；`APIServiceSpec` 承载聚合所需的全部配置。
// 对指定 `Group`/`Version` 的请求，先用 `CABundle` 校验目标证书
// （开发时可用 `InsecureSkipTLSVerify` 跳过校验），再转发到 `ServiceReference`
// 指向的 Service（443 端口）。`Priority` 决定该 API 组在整体 discovery 文档中的顺序。
// 返回状态是一组聚合相关的 condition；当前只有 "Available"，
// 为 true 表示请求会被重定向到指定的 API server。
package v1
