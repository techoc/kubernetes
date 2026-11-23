/*
Copyright 2024 The Kubernetes Authors.

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

// Package externaljwt contains the proto definitions for the ExternalJWTSigner.
//
// Package externaljwt 定义 ExternalJWTSigner（外部 JWT 签名器）的 protobuf 接口。
// 该接口由运行在本地 Unix Domain Socket 上的外部进程实现，kube-apiserver 通过它完成：
//   - Sign：对 service account token 的 payload 签名；
//   - FetchKeys：获取用于验签的公钥集合，供 OIDC discovery 和 token 校验使用；
//   - Metadata：上报签名器的能力信息（如最大 token 有效期）。
//
// 具体协议定义位于 apis/v1 与 apis/v1alpha1 两个版本的 .proto 文件中。
package externaljwt
