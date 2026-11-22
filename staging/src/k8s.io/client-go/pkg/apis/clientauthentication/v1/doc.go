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

// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/client-go/pkg/apis/clientauthentication
// +k8s:openapi-gen=true
// +k8s:defaulter-gen=TypeMeta
// +k8s:openapi-model-package=io.k8s.client-go.pkg.apis.clientauthentication.v1

// +groupName=client.authentication.k8s.io

// Package v1 定义 exec 凭证插件协议（client.authentication.k8s.io/v1）的
// 外部版本类型。这些类型描述插件输出给 client-go 的 ExecCredential 对象，
// 与 API Server 的 API 组相互独立，仅用于本地进程间通信。
package v1
