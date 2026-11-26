/*
Copyright 2025 The Kubernetes Authors.

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

// 本文件处理内部凭证 API 到 v1beta1 的转换。
// beta 版本的字段集是内部版本的子集，因此转换主要是"丢弃新字段"，
// 而不是一般的双向映射。
package v1beta1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubelet/pkg/apis/credentialprovider"
)

// Convert_credentialprovider_CredentialProviderRequest_To_v1beta1_CredentialProviderRequest
// 把内部请求降级为 beta 版本。
// 这里必须手写而不是直接用生成的转换：内部版本新增了服务账号相关字段，
// beta 插件没有相应字段也无法处理，静默传递会造成语义错位，因此显式丢弃。
func Convert_credentialprovider_CredentialProviderRequest_To_v1beta1_CredentialProviderRequest(in *credentialprovider.CredentialProviderRequest, out *CredentialProviderRequest, s conversion.Scope) error {
	// 该转换有意丢弃 serviceAccountToken 与 serviceAccountAnnotations：这两个字段只在 v1 的 CredentialProviderRequest 中支持。
	// 只保留 beta 插件能理解的镜像字段，其余字段由生成函数按目标类型自然忽略。
	return autoConvert_credentialprovider_CredentialProviderRequest_To_v1beta1_CredentialProviderRequest(in, out, s)
}
