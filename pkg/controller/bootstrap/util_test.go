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
	"testing"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/klog/v2/ktesting"
)

const (
	givenTokenID     = "abc123"
	givenTokenID2    = "def456"
	givenTokenSecret = "tokenSecret"
)

// timeString 以当前时间为基准加上 delta，生成 RFC3339 格式的字符串，
// 供 token 过期时间等测试字段使用；delta 为负数即表示已过期。
func timeString(delta time.Duration) string {
	return time.Now().Add(delta).Format(time.RFC3339)
}

// TestValidateSecretForSigning 以表驱动方式验证 validateSecretForSigning 对
// 签名 token 的检查：覆盖无过期时间、已过期、格式错误以及 ID、secret、签名位缺失等情况。
func TestValidateSecretForSigning(t *testing.T) {
	// 表驱动用例：每行只改动一个字段，便于定位是哪一项校验拒绝了 token。
	cases := []struct {
		description string // 用例名称
		tokenID     string // token ID
		tokenSecret string // token secret
		okToSign    string // 是否允许签名
		expiration  string // 过期时间
		valid       bool   // 期望是否校验通过
	}{
		{
			"Signing token with no exp",
			givenTokenID, givenTokenSecret, "true", "", true,
		},
		{
			"Signing token with valid exp",
			givenTokenID, givenTokenSecret, "true", timeString(time.Hour), true,
		},
		{
			"Expired signing token",
			givenTokenID, givenTokenSecret, "true", timeString(-time.Hour), false,
		},
		{
			"Signing token with bad exp",
			givenTokenID, givenTokenSecret, "true", "garbage", false,
		},
		{
			"Signing token without signing bit",
			givenTokenID, givenTokenSecret, "", "garbage", false,
		},
		{
			"Signing token with bad signing bit",
			givenTokenID, givenTokenSecret, "", "", false,
		},
		{
			"Signing token with no ID",
			"", givenTokenSecret, "true", "", false,
		},
		{
			"Signing token with no secret",
			givenTokenID, "", "true", "", false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			// 用固定的资源版本和名称构造合法的 Secret，只让待测字段随用例变化。
			secret := &v1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:       metav1.NamespaceSystem,
					Name:            bootstrapapi.BootstrapTokenSecretPrefix + givenTokenID,
					ResourceVersion: "1",
				},
				Type: bootstrapapi.SecretTypeBootstrapToken,
				Data: map[string][]byte{
					bootstrapapi.BootstrapTokenIDKey:           []byte(tc.tokenID),
					bootstrapapi.BootstrapTokenSecretKey:       []byte(tc.tokenSecret),
					bootstrapapi.BootstrapTokenUsageSigningKey: []byte(tc.okToSign),
					bootstrapapi.BootstrapTokenExpirationKey:   []byte(tc.expiration),
				},
			}

			tokenID, tokenSecret, ok := validateSecretForSigning(ctx, secret)
			if ok != tc.valid {
				t.Errorf("Unexpected validation failure. Expected %v, got %v", tc.valid, ok)
			}
			// 只有校验通过时才检查返回值；失败用例无需关心解析出的 ID 和 secret。
			if ok {
				if tokenID != tc.tokenID {
					t.Errorf("Unexpected Token ID. Expected %q, got %q", givenTokenID, tokenID)
				}
				if tokenSecret != tc.tokenSecret {
					t.Errorf("Unexpected Token Secret. Expected %q, got %q", givenTokenSecret, tokenSecret)
				}
			}
		})
	}

}

// TestValidateSecret 验证合法且带签名许可的 Secret 能通过校验，
// 并确认返回的 token ID 与 secret 与 Secret 中存储的值一致。
func TestValidateSecret(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 构造包含完整字段的合法 Secret，作为正向校验的基准用例。
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       metav1.NamespaceSystem,
			Name:            bootstrapapi.BootstrapTokenSecretPrefix + givenTokenID,
			ResourceVersion: "1",
		},
		Type: bootstrapapi.SecretTypeBootstrapToken,
		Data: map[string][]byte{
			bootstrapapi.BootstrapTokenIDKey:           []byte(givenTokenID),
			bootstrapapi.BootstrapTokenSecretKey:       []byte(givenTokenSecret),
			bootstrapapi.BootstrapTokenUsageSigningKey: []byte("true"),
		},
	}

	tokenID, tokenSecret, ok := validateSecretForSigning(ctx, secret)
	if !ok {
		t.Errorf("Unexpected validation failure.")
	}
	if tokenID != givenTokenID {
		t.Errorf("Unexpected Token ID. Expected %q, got %q", givenTokenID, tokenID)
	}
	if tokenSecret != givenTokenSecret {
		t.Errorf("Unexpected Token Secret. Expected %q, got %q", givenTokenSecret, tokenSecret)
	}
}

// TestBadSecretName 验证 Secret 名称缺少 bootstrap token 前缀时校验失败，
// 防止任意命名的 Secret 被当作引导 token 使用。
func TestBadSecretName(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 名称直接使用 token ID，刻意省略 bootstrap token 前缀。
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       metav1.NamespaceSystem,
			Name:            givenTokenID,
			ResourceVersion: "1",
		},
		Type: bootstrapapi.SecretTypeBootstrapToken,
		Data: map[string][]byte{
			bootstrapapi.BootstrapTokenIDKey:           []byte(givenTokenID),
			bootstrapapi.BootstrapTokenSecretKey:       []byte(givenTokenSecret),
			bootstrapapi.BootstrapTokenUsageSigningKey: []byte("true"),
		},
	}

	_, _, ok := validateSecretForSigning(ctx, secret)
	if ok {
		t.Errorf("Token validation should fail with bad name")
	}
}

// TestMismatchSecretName 验证 Secret 名称中的 token ID 与数据字段里的 ID
// 不一致时校验失败，避免名称与内容被篡改后仍能通过校验。
func TestMismatchSecretName(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 名称使用 givenTokenID2，数据字段却使用 givenTokenID，制造不一致。
	secret := &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       metav1.NamespaceSystem,
			Name:            bootstrapapi.BootstrapTokenSecretPrefix + givenTokenID2,
			ResourceVersion: "1",
		},
		Type: bootstrapapi.SecretTypeBootstrapToken,
		Data: map[string][]byte{
			bootstrapapi.BootstrapTokenIDKey:           []byte(givenTokenID),
			bootstrapapi.BootstrapTokenSecretKey:       []byte(givenTokenSecret),
			bootstrapapi.BootstrapTokenUsageSigningKey: []byte("true"),
		},
	}

	_, _, ok := validateSecretForSigning(ctx, secret)
	if ok {
		t.Errorf("Token validation should fail with mismatched name")
	}
}
