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

package bootstrap

import (
	"context"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/api/core/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstrapsecretutil "k8s.io/cluster-bootstrap/util/secrets"
)

// validateSecretForSigning 校验一个 bootstrap token Secret 是否可用于签名。
// 依次检查 Secret 名称、token ID/Secret 数据的一致性、过期时间以及 signing 用途标记；
// 任一检查失败都返回 ok=false，调用方据此拒绝该 Secret。
func validateSecretForSigning(ctx context.Context, secret *v1.Secret) (tokenID, tokenSecret string, ok bool) {
	logger := klog.FromContext(ctx)
	nameTokenID, ok := bootstrapsecretutil.ParseName(secret.Name)
	if !ok {
		logger.V(3).Info("Invalid secret name, must be of the form "+bootstrapapi.BootstrapTokenSecretPrefix+"<secret-id>", "secretName", secret.Name)
		return "", "", false
	}

	// token ID 既来自名称也来自数据，二者必须一致，防止伪造名称引用别的 token。
	tokenID = bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenIDKey)
	if len(tokenID) == 0 {
		logger.V(3).Info("No key in Secret", "key", bootstrapapi.BootstrapTokenIDKey, "secret", klog.KObj(secret))
		return "", "", false
	}

	if nameTokenID != tokenID {
		logger.V(3).Info("Token ID doesn't match secret name", "tokenID", tokenID, "secretName", secret.Name)
		return "", "", false
	}

	tokenSecret = bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenSecretKey)
	if len(tokenSecret) == 0 {
		logger.V(3).Info("No key in secret", "key", bootstrapapi.BootstrapTokenSecretKey, "secret", klog.KObj(secret))
		return "", "", false
	}

	// 确认 Secret 尚未过期。TokenCleaner 本应清理过期 Secret，
	// 但如果它失效或尚未执行到，这里必须再次检查，避免过期 token 仍被接受。
	if bootstrapsecretutil.HasExpired(secret, time.Now()) {
		return "", "", false
	}

	// 确认该 Secret 被显式标记为可用于签名，防止其他用途的 token 被误用。
	okToSign := bootstrapsecretutil.GetData(secret, bootstrapapi.BootstrapTokenUsageSigningKey)
	if okToSign != "true" {
		return "", "", false
	}

	return tokenID, tokenSecret, true
}
