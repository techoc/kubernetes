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
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/controller"
)

// testTokenID 是测试用的令牌 ID，同时作为 ConfigMap 中签名条目的键。
const testTokenID = "abc123"

// newSigner 构造测试用 Signer，返回 Signer、fake 客户端以及 Secret/ConfigMap informer，
// 便于在测试中直接向 informer 索引器注入对象。
func newSigner() (*Signer, *fake.Clientset, coreinformers.SecretInformer, coreinformers.ConfigMapInformer, error) {
	options := DefaultSignerOptions()
	cl := fake.NewSimpleClientset()
	informers := informers.NewSharedInformerFactory(fake.NewSimpleClientset(), controller.NoResyncPeriodFunc())
	secrets := informers.Core().V1().Secrets()
	configMaps := informers.Core().V1().ConfigMaps()
	bsc, err := NewSigner(cl, secrets, configMaps, options)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	return bsc, cl, secrets, configMaps, nil
}

// newConfigMap 构造 cluster-info ConfigMap；若 tokenID 非空，则附带对应签名条目。
func newConfigMap(tokenID, signature string) *v1.ConfigMap {
	// kube-config 载荷固定不变，仅签名条目随 tokenID 变化。
	ret := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       metav1.NamespacePublic,
			Name:            bootstrapapi.ConfigMapClusterInfo,
			ResourceVersion: "1",
		},
		Data: map[string]string{
			bootstrapapi.KubeConfigKey: "payload",
		},
	}
	// 空 tokenID 表示不附加签名，供"无签名"场景构造基线对象。
	if len(tokenID) > 0 {
		ret.Data[bootstrapapi.JWSSignatureKeyPrefix+tokenID] = signature
	}
	return ret
}

// TestNoConfigMap 验证没有 ConfigMap 时签名流程不产生任何 API 操作。
func TestNoConfigMap(t *testing.T) {
	signer, cl, _, _, err := newSigner()
	if err != nil {
		t.Fatalf("error creating Signer: %v", err)
	}
	signer.signConfigMap(context.TODO())
	verifyActions(t, []core.Action{}, cl.Actions())
}

// TestSimpleSign 验证存在可用令牌 Secret 时会为 ConfigMap 生成并写入 JWS 签名。
func TestSimpleSign(t *testing.T) {
	signer, cl, secrets, configMaps, err := newSigner()
	if err != nil {
		t.Fatalf("error creating Signer: %v", err)
	}

	cm := newConfigMap("", "")
	configMaps.Informer().GetIndexer().Add(cm)

	secret := newTokenSecret(testTokenID, "tokenSecret")
	addSecretSigningUsage(secret, "true")
	secrets.Informer().GetIndexer().Add(secret)

	signer.signConfigMap(context.TODO())

	expected := []core.Action{
		core.NewUpdateAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"},
			api.NamespacePublic,
			newConfigMap(testTokenID, "eyJhbGciOiJIUzI1NiIsImtpZCI6ImFiYzEyMyJ9..QSxpUG7Q542CirTI2ECPSZjvBOJURUW5a7XqFpNI958")),
	}

	verifyActions(t, expected, cl.Actions())
}

// TestNoSignNeeded 验证签名已正确时不会重复更新 ConfigMap。
func TestNoSignNeeded(t *testing.T) {
	signer, cl, secrets, configMaps, err := newSigner()
	if err != nil {
		t.Fatalf("error creating Signer: %v", err)
	}

	cm := newConfigMap(testTokenID, "eyJhbGciOiJIUzI1NiIsImtpZCI6ImFiYzEyMyJ9..QSxpUG7Q542CirTI2ECPSZjvBOJURUW5a7XqFpNI958")
	configMaps.Informer().GetIndexer().Add(cm)

	secret := newTokenSecret(testTokenID, "tokenSecret")
	addSecretSigningUsage(secret, "true")
	secrets.Informer().GetIndexer().Add(secret)

	signer.signConfigMap(context.TODO())

	verifyActions(t, []core.Action{}, cl.Actions())
}

// TestUpdateSignature 验证签名过期或不匹配时会用新签名覆盖旧签名。
func TestUpdateSignature(t *testing.T) {
	signer, cl, secrets, configMaps, err := newSigner()
	if err != nil {
		t.Fatalf("error creating Signer: %v", err)
	}

	cm := newConfigMap(testTokenID, "old signature")
	configMaps.Informer().GetIndexer().Add(cm)

	secret := newTokenSecret(testTokenID, "tokenSecret")
	addSecretSigningUsage(secret, "true")
	secrets.Informer().GetIndexer().Add(secret)

	signer.signConfigMap(context.TODO())

	expected := []core.Action{
		core.NewUpdateAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"},
			api.NamespacePublic,
			newConfigMap(testTokenID, "eyJhbGciOiJIUzI1NiIsImtpZCI6ImFiYzEyMyJ9..QSxpUG7Q542CirTI2ECPSZjvBOJURUW5a7XqFpNI958")),
	}

	verifyActions(t, expected, cl.Actions())
}

// TestRemoveSignature 验证令牌 Secret 消失后，ConfigMap 中的签名条目会被清除。
func TestRemoveSignature(t *testing.T) {
	signer, cl, _, configMaps, err := newSigner()
	if err != nil {
		t.Fatalf("error creating Signer: %v", err)
	}

	cm := newConfigMap(testTokenID, "old signature")
	configMaps.Informer().GetIndexer().Add(cm)

	signer.signConfigMap(context.TODO())

	expected := []core.Action{
		core.NewUpdateAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"},
			api.NamespacePublic,
			newConfigMap("", "")),
	}

	verifyActions(t, expected, cl.Actions())
}
