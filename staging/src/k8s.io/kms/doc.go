/*
Copyright 2022 The Kubernetes Authors.

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

// Package kms 定义 Kubernetes KMS（Key Management Service，密钥管理服务）插件的 API。
// KMS 插件以 gRPC 服务的形式运行在本地 socket 上，为 apiserver 提供信封加密能力：
//   - apis/v1beta1：KMS v1（已弃用），仅保留安全更新；
//   - apis/v2：KMS v2，支持状态轮询、key ID 与 annotations；
//   - pkg/service：服务端 Service 抽象与 gRPC 实现；
//   - pkg/util：端点解析工具。
package kms
