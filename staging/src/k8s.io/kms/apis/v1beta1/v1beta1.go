/*
Copyright 2019 The Kubernetes Authors.

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

// Package v1beta1 contains definition of kms-plugin's gRPC service.
// Deprecated: KMSv1 is deprecated in v1.28 and will only receive security updates going forward. Use KMSv2 instead.
//
// Package v1beta1 定义 KMS v1 插件的 gRPC 服务。
// 已弃用：KMS v1 自 v1.28 起弃用，今后只接受安全更新，新接入应使用 KMS v2（apis/v2）。
package v1beta1

// IsVersionCheckMethod determines whether the supplied method is a version check against kms-plugin.
// IsVersionCheckMethod 判断给定的 gRPC 方法全名是否为 KMS 插件的版本检查调用（Version RPC）。
// 方法名格式为 "/<package>.<Service>/<Method>"，这里对应 /v1beta1.KeyManagementService/Version。
// apiserver 会用它来区分版本探测请求与普通加解密请求，以便做特殊的错误处理。
func IsVersionCheckMethod(method string) bool {
	return method == "/v1beta1.KeyManagementService/Version"
}
