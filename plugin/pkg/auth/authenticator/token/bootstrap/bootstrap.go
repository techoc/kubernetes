/*
Copyright 2017 The Kubernetes Authors.

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

/*
Package bootstrap 提供 TLS bootstrap token 认证器。

新节点加入集群时，使用 bootstrap token 进行初始认证。
Token 存储在 kube-system 命名空间的 Secret 中，格式为 bootstrap.kubernetes.io/token。
认证成功后，节点用户被放入 system:bootstrappers 组，用户名为 system:bootstrap:(token-id)。
然后节点可以通过 CSR（Certificate Signing Request）获取长期客户端证书。
*/
package bootstrap

import (
	"context"
	"crypto/subtle"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	corev1listers "k8s.io/client-go/listers/core/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstrapsecretutil "k8s.io/cluster-bootstrap/util/secrets"
	bootstraptokenutil "k8s.io/cluster-bootstrap/util/tokens"
)

// TODO: A few methods in this package is copied from other sources. Either
// because the existing functionality isn't exported or because it is in a
// package that shouldn't be directly imported by this packages.

// NewTokenAuthenticator 创建一个新的 bootstrap token 认证器。
// lister 应该指向 kube-system 命名空间，因为 bootstrap token secret 只存储在那里。
func NewTokenAuthenticator(lister corev1listers.SecretNamespaceLister) *TokenAuthenticator {
	return &TokenAuthenticator{lister}
}

// TokenAuthenticator 从 API Server 的 Secret 中验证 bootstrap token。
// 通过 kube-system 命名空间的 Secret lister 查找和验证 token。
type TokenAuthenticator struct {
	lister corev1listers.SecretNamespaceLister // kube-system 命名空间的 Secret lister
}

// tokenErrorf 记录一条错误消息，说明匹配到 bearer token 的 Secret 未通过某项检查。
// 用于调试：打印 Secret 名称和失败原因，帮助排查 bootstrap token 配置问题。
//
//	tokenErrorf(secret, "has invalid value for key %s", key)
func tokenErrorf(s *corev1.Secret, format string, i ...interface{}) {
	format = fmt.Sprintf("Bootstrap secret %s/%s matching bearer token ", s.Namespace, s.Name) + format
	klog.V(3).Infof(format, i...)
}

// AuthenticateToken 尝试将提供的 token 与 kube-system 命名空间中的 bootstrap token Secret 匹配。
// 如果找到匹配的 Secret，返回一个认证响应，用户属于 "system:bootstrappers" 组，
// 用户名为 "system:bootstrap:(token-id)"。
//
// 验证流程：
// 1. 解析 token 格式（token-id.token-secret）
// 2. 根据 token-id 查找名为 bootstrap-token-(token-id) 的 Secret
// 3. 检查 Secret 未被删除、类型正确、token-secret 和 token-id 匹配
// 4. 检查 token 未过期且标记为可用于认证
// 5. 返回包含用户组和用户名的认证响应
//
// Token 格式：(token-id).(token-secret)，例如 abcdef.0123456789abcdef
func (t *TokenAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	tokenID, tokenSecret, err := bootstraptokenutil.ParseToken(token)
	if err != nil {
		// Token isn't of the correct form, ignore it.
		return nil, false, nil
	}

	secretName := bootstrapapi.BootstrapTokenSecretPrefix + tokenID
	secret, err := t.lister.Get(secretName)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(3).Infof("No secret of name %s to match bootstrap bearer token", secretName)
			return nil, false, nil
		}
		return nil, false, err
	}

	if secret.DeletionTimestamp != nil {
		tokenErrorf(secret, "is deleted and awaiting removal")
		return nil, false, nil
	}

	if string(secret.Type) != string(bootstrapapi.SecretTypeBootstrapToken) || secret.Data == nil {
		tokenErrorf(secret, "has invalid type, expected %s.", bootstrapapi.SecretTypeBootstrapToken)
		return nil, false, nil
	}

	ts := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenSecretKey)
	if subtle.ConstantTimeCompare([]byte(ts), []byte(tokenSecret)) != 1 {
		tokenErrorf(secret, "has invalid value for key %s.", bootstrapapi.BootstrapTokenSecretKey)
		return nil, false, nil
	}

	id := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenIDKey)
	if id != tokenID {
		tokenErrorf(secret, "has invalid value for key %s.", bootstrapapi.BootstrapTokenIDKey)
		return nil, false, nil
	}

	if bootstrapsecretutil.HasExpired(secret, time.Now()) {
		// logging done in isSecretExpired method.
		return nil, false, nil
	}

	if bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenUsageAuthentication) != "true" {
		tokenErrorf(secret, "not marked %s=true.", bootstrapapi.BootstrapTokenUsageAuthentication)
		return nil, false, nil
	}

	groups, err := bootstrapsecretutil.GetGroups(secret)
	if err != nil {
		tokenErrorf(secret, "has invalid value for key %s: %v.", bootstrapapi.BootstrapTokenExtraGroupsKey, err)
		return nil, false, nil
	}

	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   bootstrapapi.BootstrapUserPrefix + string(id),
			Groups: groups,
		},
	}, true, nil
}
