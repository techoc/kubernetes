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

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	core "k8s.io/client-go/testing"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/utils/dump"
)

// newTokenSecret 构造一个符合 bootstrap token 格式的 Secret，供各控制器的单元测试复用。
// 名称前缀、类型和键名都遵循 cluster-bootstrap 约定，否则 token 校验逻辑无法识别它。
func newTokenSecret(tokenID, tokenSecret string) *v1.Secret {
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       metav1.NamespaceSystem,
			Name:            bootstrapapi.BootstrapTokenSecretPrefix + tokenID,
			ResourceVersion: "1",
			UID:             types.UID("uid" + tokenID),
		},
		Type: bootstrapapi.SecretTypeBootstrapToken,
		Data: map[string][]byte{
			bootstrapapi.BootstrapTokenIDKey:     []byte(tokenID),
			bootstrapapi.BootstrapTokenSecretKey: []byte(tokenSecret),
		},
	}
}

// addSecretExpiration 给 token Secret 写入过期时间字段。
// 过期时间是可选字段，测试需要显式添加才能覆盖清理器的过期判断分支。
func addSecretExpiration(s *v1.Secret, expiration string) {
	s.Data[bootstrapapi.BootstrapTokenExpirationKey] = []byte(expiration)
}

// addSecretSigningUsage 给 token Secret 写入 "signing" 用途标记。
// 只有带此标记的 token 才会被 CSR 签名控制器用于证书签发。
func addSecretSigningUsage(s *v1.Secret, value string) {
	s.Data[bootstrapapi.BootstrapTokenUsageSigningKey] = []byte(value)
}

// verifyActions 按序比对 fake 客户端记录的实际动作与期望动作。
// 逐个比对而非集合比对，因为控制器发出 API 请求的顺序本身也是行为契约；
// 同时报告多余的、缺失的和内容不符的动作，减少反复运行测试定位问题的次数。
func verifyActions(t *testing.T, expected, actual []core.Action) {
	for i, a := range actual {
		if len(expected) < i+1 {
			t.Errorf("%d unexpected actions: %s", len(actual)-len(expected), dump.Pretty(actual[i:]))
			break
		}

		e := expected[i]
		if !helper.Semantic.DeepEqual(e, a) {
			t.Errorf("Expected\n\t%s\ngot\n\t%s", dump.Pretty(e), dump.Pretty(a))
			continue
		}
	}

	if len(expected) > len(actual) {
		t.Errorf("%d additional expected actions", len(expected)-len(actual))
		for _, a := range expected[len(actual):] {
			t.Logf("    %s", dump.Pretty(a))
		}
	}
}
