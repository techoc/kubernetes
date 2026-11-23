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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubelet/pkg/apis/credentialprovider"
)

// Convert_credentialprovider_CredentialProviderRequest_To_v1alpha1_CredentialProviderRequest
// 覆盖生成代码的默认转换：v1alpha1 结构里没有服务账号字段，只能有丢弃。
// 让旧插件只看到它能理解的字段，避免敏感 token 泄露给不认识它的协议端。
func Convert_credentialprovider_CredentialProviderRequest_To_v1alpha1_CredentialProviderRequest(in *credentialprovider.CredentialProviderRequest, out *CredentialProviderRequest, s conversion.Scope) error {
	// This conversion intentionally omits the serviceAccountToken and serviceAccountAnnotations fields which are only supported in v1 CredentialProviderRequest.
	//
	// 这是有损转换：需要服务账号信息的插件必须协商到 v1，不能指望 alpha 回包带这些字段。
	return autoConvert_credentialprovider_CredentialProviderRequest_To_v1alpha1_CredentialProviderRequest(in, out, s)
}
