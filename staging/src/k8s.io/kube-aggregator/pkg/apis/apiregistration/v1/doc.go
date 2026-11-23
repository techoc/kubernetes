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
// 指令 +groupName 声明本包归属 apiregistration.k8s.io，供生成器与发现文档确定 API 组名。
// 指令 +k8s:openapi-model-package 指定 OpenAPI 模型的包名，避免与 Kubernetes 核心 API 的模型重名。

// Package v1 contains the API Registration API, which is responsible for
// registering an API `Group`/`Version` with another kubernetes like API server.
// The `APIService` holds information about the other API server in
// `APIServiceSpec` type as well as general `TypeMeta` and `ObjectMeta`. The
// `APIServiceSpec` type have the main configuration needed to do the
// aggregation. Any request coming for specified `Group`/`Version` will be
// directed to the service defined by `ServiceReference` (on port 443) after
// validating the target using provided `CABundle` or skipping validation
// if development flag `InsecureSkipTLSVerify` is set. `Priority` is controlling
// the order of this API group in the overall discovery document.
// The return status is a set of conditions for this aggregation. Currently
// there is only one condition named "Available", if true, it means the
// api/server requests will be redirected to specified API server.
package v1
