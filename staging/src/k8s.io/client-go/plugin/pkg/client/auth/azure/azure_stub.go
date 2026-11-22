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

// Package azure 是已移除的 Azure auth provider 的占位实现。
// 保留同名包只为在用户仍配置 auth-provider: azure 时返回可操作的迁移提示，
// 而不是静默失败或出现“未知 provider”这种难以诊断的错误。
package azure

import (
	"errors"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

func init() {
	// 注册一个总是失败的 provider：注册本身必须成功，错误留到真正创建 provider 时再返回。
	if err := rest.RegisterAuthProviderPlugin("azure", newAzureAuthProvider); err != nil {
		klog.Fatalf("Failed to register azure auth plugin: %v", err)
	}
}

// newAzureAuthProvider 始终返回迁移错误，引导用户改用 kubelogin 凭据插件。
func newAzureAuthProvider(_ string, _ map[string]string, _ rest.AuthProviderConfigPersister) (rest.AuthProvider, error) {
	return nil, errors.New(`The azure auth plugin has been removed.
Please use the https://github.com/Azure/kubelogin kubectl/client-go credential plugin instead.
See https://kubernetes.io/docs/reference/access-authn-authz/authentication/#client-go-credential-plugins for further details`)
}
