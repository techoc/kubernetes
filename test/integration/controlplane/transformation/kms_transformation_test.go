//go:build !windows

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

package transformation

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/crypto/cryptobyte"

	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured/unstructuredscheme"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/features"
	genericapiserver "k8s.io/apiserver/pkg/server"
	encryptionconfigcontroller "k8s.io/apiserver/pkg/server/options/encryptionconfig/controller"
	"k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	mock "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/testing/v1beta1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kmsapi "k8s.io/kms/apis/v1beta1"
	"k8s.io/kubernetes/test/integration"
	"k8s.io/kubernetes/test/integration/etcd"
	"k8s.io/kubernetes/test/integration/framework"
)

const (
	dekKeySizeLen = 2
	kmsAPIVersion = "v1beta1"
)

type envelope struct {
	providerName string
	rawEnvelope  []byte
	plainTextDEK []byte
}

func (r envelope) prefix() string {
	return fmt.Sprintf("k8s:enc:kms:v1:%s:", r.providerName)
}

func (r envelope) prefixLen() int {
	return len(r.prefix())
}

func (r envelope) dekLen() int {
	// DEK 的长度存储在紧随前缀的两个字节中。
	return int(binary.BigEndian.Uint16(r.rawEnvelope[r.prefixLen() : r.prefixLen()+dekKeySizeLen]))
}

func (r envelope) cipherTextDEK() []byte {
	return r.rawEnvelope[r.prefixLen()+dekKeySizeLen : r.prefixLen()+dekKeySizeLen+r.dekLen()]
}

func (r envelope) startOfPayload(providerName string) int {
	return r.prefixLen() + dekKeySizeLen + r.dekLen()
}

func (r envelope) cipherTextPayload() []byte {
	return r.rawEnvelope[r.startOfPayload(r.providerName):]
}

func (r envelope) plainTextPayload(secretETCDPath string) ([]byte, error) {
	block, err := aes.NewCipher(r.plainTextDEK)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize AES Cipher: %v", err)
	}
	// 密钥的 etcd path 用作认证上下文——解密时需要传入它
	ctx := context.Background()
	dataCtx := value.DefaultContext([]byte(secretETCDPath))
	aesgcmTransformer, err := aestransformer.NewGCMTransformer(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create transformer from block: %v", err)
	}
	plainSecret, _, err := aesgcmTransformer.TransformFromStorage(ctx, r.cipherTextPayload(), dataCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform from storage via AESGCM, err: %w", err)
	}

	return plainSecret, nil
}

// TestKMSProvider 是 KubeAPI、ETCD 与 KMS 插件之间的集成测试。
// 具体而言，该测试验证以下集成契约：
// 1. 经 KMS Provider 处理的 ETCD 原始记录应以 k8s:enc:kms:v1:grpc-kms-provider-name: 为前缀
// 2. 数据加密密钥（DEK）应由 envelopeTransformer 生成并传递给 KMS gRPC 插件
// 3. KMS gRPC 插件应使用密钥加密密钥（KEK）加密 DEK，并将其传回 envelopeTransformer
// 4. cipherTextPayload（如 Secret）应通过 AES CBC 变换加密
// 5. Prefix-EncryptedDEK-EncryptedPayload 结构应存入 ETCD
// 6. 对以 AES CBC 变换写入的 cipherTextPayload 直接进行 AES GCM 解密不会成功
// 7. 从 Kube API Server 直接读取时，已有的 AES CBC secret 应被解封（un-enveloped）
// 8. 对 secret 执行无操作更新应导致使用新的 AES GCM 密钥
// 9. 使用新的 AES GCM 密钥后，直接 AES GCM 解密可正常工作
func TestKMSProvider(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: kms-provider
       cachesize: 1000
       endpoint: unix:///@kms-provider.sock
`
	providerName := "kms-provider"
	pluginMock := mock.NewBase64Plugin(t, "@kms-provider.sock")
	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer test.cleanUp()

	test.secret, err = test.createSecret(testSecret, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test secret, error: %v", err)
	}

	// 由于数据加密密钥（DEK）是（每次加密操作时）随机生成的，我们需要向 KMS Mock 询问它。
	plainTextDEK := pluginMock.LastEncryptRequest()

	secretETCDPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", test.secret.Name, test.secret.Namespace)
	rawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}
	envelopeData := envelope{
		providerName: providerName,
		rawEnvelope:  rawEnvelope,
		plainTextDEK: plainTextDEK,
	}

	wantPrefix := "k8s:enc:kms:v1:kms-provider:"
	if !bytes.HasPrefix(rawEnvelope, []byte(wantPrefix)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefix, rawEnvelope)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	decryptResponse, err := pluginMock.Decrypt(ctx, &kmsapi.DecryptRequest{Version: kmsAPIVersion, Cipher: envelopeData.cipherTextDEK()})
	if err != nil {
		t.Fatalf("failed to decrypt DEK, %v", err)
	}
	dekPlainAsWouldBeSeenByETCD := decryptResponse.Plain

	if !bytes.Equal(plainTextDEK, dekPlainAsWouldBeSeenByETCD) {
		t.Fatalf("expected plainTextDEK %v to be passed to KMS Plugin, but got %s",
			plainTextDEK, dekPlainAsWouldBeSeenByETCD)
	}

	plainSecret, err := envelopeData.plainTextPayload(secretETCDPath)
	if err != nil {
		t.Fatalf("failed to transform from storage via AESCBC, err: %v", err)
	}

	if !strings.Contains(string(plainSecret), secretVal) {
		t.Fatalf("expected %q after decryption, but got %q", secretVal, string(plainSecret))
	}

	secretClient := test.restClient.CoreV1().Secrets(testNamespace)
	// 从 Kube API Server 直接读取时，Secret 应被解封（un-enveloped）。
	s, err := secretClient.Get(ctx, testSecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get Secret from %s, err: %v", testNamespace, err)
	}
	if secretVal != string(s.Data[secretKey]) {
		t.Fatalf("expected %s from KubeAPI, but got %s", secretVal, string(s.Data[secretKey]))
	}

	// 使用 AES CBC 写入数据以模拟降级
	oldSecretBytes, err := base64.StdEncoding.DecodeString(oldSecret)
	if err != nil {
		t.Fatalf("failed to base64 decode old secret, err: %v", err)
	}
	oldKeyBytes, err := base64.StdEncoding.DecodeString(oldAESCBCKey)
	if err != nil {
		t.Fatalf("failed to base64 decode old key, err: %v", err)
	}
	block, err := aes.NewCipher(oldKeyBytes)
	if err != nil {
		t.Fatalf("invalid key, err: %v", err)
	}

	oldEncryptedSecretBytes, err := aestransformer.NewCBCTransformer(block).TransformToStorage(ctx, oldSecretBytes, value.DefaultContext(secretETCDPath))
	if err != nil {
		t.Fatalf("failed to encrypt old secret, err: %v", err)
	}

	oldEncryptedSecretBuf := cryptobyte.NewBuilder(nil)
	oldEncryptedSecretBuf.AddBytes([]byte(wantPrefix))
	oldEncryptedSecretBuf.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		b.AddBytes([]byte(oldAESCBCKey))
	})
	oldEncryptedSecretBuf.AddBytes(oldEncryptedSecretBytes)

	_, err = test.writeRawRecordToETCD(secretETCDPath, oldEncryptedSecretBuf.BytesOrPanic())
	if err != nil {
		t.Fatalf("failed to write old encrypted secret, err: %v", err)
	}

	// 确认直接 AES GCM 解密不会成功
	failingRawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}
	failingOldEnvelope := envelope{
		providerName: providerName,
		rawEnvelope:  failingRawEnvelope,
		plainTextDEK: oldKeyBytes,
	}
	failingOldPlainSecret, err := failingOldEnvelope.plainTextPayload(secretETCDPath)
	if err == nil {
		t.Fatalf("AESGCM decryption failure not seen, data: %s", string(failingOldPlainSecret))
	}

	// 从 Kube API Server 直接读取时，已有的 AES CBC secret 应被解封（un-enveloped）。
	oldSecretObj, err := secretClient.Get(ctx, testSecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to read old secret via Kube API, err: %v", err)
	}
	if oldSecretVal != string(oldSecretObj.Data[secretKey]) {
		t.Fatalf("expected %s from KubeAPI, but got %s", oldSecretVal, string(oldSecretObj.Data[secretKey]))
	}

	// 无操作更新应导致使用新的 AES GCM 密钥
	oldSecretUpdated, err := secretClient.Update(ctx, oldSecretObj, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update old secret via Kube API, err: %v", err)
	}
	if oldSecretObj.ResourceVersion == oldSecretUpdated.ResourceVersion {
		t.Fatalf("old secret not updated on no-op write: %s", oldSecretObj.ResourceVersion)
	}

	// 确认直接 AES GCM 解密可正常工作
	oldRawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}
	oldEnvelope := envelope{
		providerName: providerName,
		rawEnvelope:  oldRawEnvelope,
		plainTextDEK: pluginMock.LastEncryptRequest(),
	}
	if !bytes.HasPrefix(oldRawEnvelope, []byte(wantPrefix)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefix, oldRawEnvelope)
	}
	oldPlainSecret, err := oldEnvelope.plainTextPayload(secretETCDPath)
	if err != nil {
		t.Fatalf("failed to transform from storage via AESGCM, err: %v", err)
	}
	if !strings.Contains(string(oldPlainSecret), oldSecretVal) {
		t.Fatalf("expected %q after decryption, but got %q", oldSecretVal, string(oldPlainSecret))
	}
}

// TestECHotReload 是验证 KMS 加密配置热重载可正常工作的集成测试。
// 该测试断言以下场景：
// 1. 以 'kms-provider' 启动
// 2. 创建一些 secret
// 3. 添加 'new-kms-provider' 作为写入 KMS（可以这样做，因为只有 1 个 API server）
// 4. 等待配置被感知
// 5. 对 secret 执行存储迁移
// 6. 确认 secret 具有新的前缀
// 7. 移除 'kms-provider'
// 8. 等待配置被感知
// 9. 确认读取仍然正常
// 10. 确认集群范围的 secret 读取仍然正常
// 11. 确认 API server 可使用最后应用的加密配置重启
func TestEncryptionConfigHotReload(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	// 这让测试响应更快。默认值为 1 分钟。
	encryptionconfigcontroller.EncryptionConfigFileChangePollDuration = time.Second

	storageConfig := framework.SharedEtcd()
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: kms-provider
       cachesize: 1000
       endpoint: unix:///@kms-provider.sock
`

	genericapiserver.SetHostnameFuncForTests("testAPIServerID")
	_ = mock.NewBase64Plugin(t, "@kms-provider.sock")
	var restarted bool
	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, reload: true, storageConfig: storageConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer func() {
		if !restarted {
			test.cleanUp()
		}
	}()
	ctx := testContext(t)

	// 全局 metrics registry 会跨测试运行保留——在这里重置以便进行断言
	copyConfig := rest.CopyConfig(test.kubeAPIServer.ClientConfig)
	copyConfig.GroupVersion = &schema.GroupVersion{}
	copyConfig.NegotiatedSerializer = unstructuredscheme.NewUnstructuredNegotiatedSerializer()
	rc, err := rest.RESTClientFor(copyConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Delete().AbsPath("/metrics").Do(ctx).Error(); err != nil {
		t.Fatal(err)
	}

	test.secret, err = test.createSecret(testSecret, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test secret, error: %v", err)
	}

	// 在 default 命名空间创建新 secret，用于断言热重载后集群范围的读取仍然正常。
	_, err = test.createSecret(fmt.Sprintf("%s-%s", testSecret, "1"), "default")
	if err != nil {
		t.Fatalf("Failed to create test secret in default namespace, error: %v", err)
	}

	_, err = test.createConfigMap(testConfigmap, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test configmap, error: %v", err)
	}

	// 测试热重载控制器是否健康
	mustBeHealthy(t, "/poststarthook/start-encryption-provider-config-automatic-reload", "ok", test.kubeAPIServer.ClientConfig)

	encryptionConfigWithNewProvider := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: new-kms-provider-for-secrets
       cachesize: 1000
       endpoint: unix:///@new-kms-provider.sock
    - kms:
       name: kms-provider
       cachesize: 1000
       endpoint: unix:///@kms-provider.sock
  - resources:
    - configmaps
    providers:
    - kms:
       name: new-kms-provider-for-configmaps
       cachesize: 1000
       endpoint: unix:///@new-kms-provider.sock
    - identity: {}
`
	// 启动新的 KMS 插件
	_ = mock.NewBase64Plugin(t, "@new-kms-provider.sock")
	// 更新加密配置
	updateFile(t, test.configDir, encryptionConfigFileName, []byte(encryptionConfigWithNewProvider))

	wantPrefixForSecrets := "k8s:enc:kms:v1:new-kms-provider-for-secrets:"

	// 采用这种暴力轮询方式而非精巧的 channel 通知，是为了避免在生产代码中加入测试专用代码。
	// 等待配置被感知
	verifyIfKMSTransformersSwapped(t, wantPrefixForSecrets, "", test)

	// 执行存储迁移
	// 获取 secret

	secretsList, err := test.restClient.CoreV1().Secrets("").List(
		ctx,
		metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("failed to list secrets, err: %v", err)
	}

	for _, secret := range secretsList.Items {
		// 更新 secret
		_, err = test.restClient.CoreV1().Secrets(secret.Namespace).Update(
			ctx,
			&secret,
			metav1.UpdateOptions{},
		)
		if err != nil {
			t.Fatalf("failed to update secret, err: %v", err)
		}
	}

	// 获取 configmap
	configmapsList, err := test.restClient.CoreV1().ConfigMaps("").List(
		ctx,
		metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("failed to list configmaps, err: %v", err)
	}

	for _, configmap := range configmapsList.Items {
		// 更新 configmap
		_, err = test.restClient.CoreV1().ConfigMaps(configmap.Namespace).Update(
			ctx,
			&configmap,
			metav1.UpdateOptions{},
		)
		if err != nil {
			t.Fatalf("failed to update configmap, err: %v", err)
		}
	}

	// 断言资源具有新的前缀
	secretETCDPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", test.secret.Name, test.secret.Namespace)
	rawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}

	// 断言 secret
	if !bytes.HasPrefix(rawEnvelope, []byte(wantPrefixForSecrets)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefixForSecrets, rawEnvelope)
	}

	rawConfigmapEnvelope, err := test.readRawRecordFromETCD(test.getETCDPathForResource(test.storageConfig.Prefix, "", "configmaps", testConfigmap, testNamespace))
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", test.getETCDPathForResource(test.storageConfig.Prefix, "", "configmaps", testConfigmap, testNamespace), err)
	}

	// 断言 configmap 的前缀
	wantPrefixForConfigmaps := "k8s:enc:kms:v1:new-kms-provider-for-configmaps:"
	if !bytes.HasPrefix(rawConfigmapEnvelope.Kvs[0].Value, []byte(wantPrefixForConfigmaps)) {
		t.Fatalf("expected configmap to be prefixed with %s, but got %s", wantPrefixForConfigmaps, rawConfigmapEnvelope.Kvs[0].Value)
	}

	// 移除旧的 KMS provider
	// verifyIfKMSTransformersSwapped 有时会在加密配置文件中的变更被感知之前就通过。
	// 这会导致校验两次配置变更的 metrics 测试失败。
	// 当已有的 KMS provider 正在运行时就可能发生这种情况（例如本例中的 new-kms-provider-for-secrets）。
	// 为确保变更确实被感知，我们额外添加了一个 provider（kms-provider-to-encrypt-all），
	// 并在 verifyIfKMSTransformersSwapped 中对其进行校验。
	encryptionConfigWithoutOldProvider := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: new-kms-provider-for-secrets
       cachesize: 1000
       endpoint: unix:///@new-kms-provider.sock
  - resources:
    - configmaps
    providers:
    - kms:
       name: new-kms-provider-for-configmaps
       cachesize: 1000
       endpoint: unix:///@new-kms-provider.sock
  - resources:
    - '*.*'
    providers:
    - kms:
        name: kms-provider-to-encrypt-all
        cachesize: 1000
        endpoint: unix:///@new-encrypt-all-kms-provider.sock
    - identity: {}
`
	// 启动新的 KMS 插件
	_ = mock.NewBase64Plugin(t, "@new-encrypt-all-kms-provider.sock")

	// 更新加密配置并等待热重载
	updateFile(t, test.configDir, encryptionConfigFileName, []byte(encryptionConfigWithoutOldProvider))

	wantPrefixForEncryptAll := "k8s:enc:kms:v1:kms-provider-to-encrypt-all:"

	// 等待配置被感知
	verifyIfKMSTransformersSwapped(t, wantPrefixForSecrets, wantPrefixForEncryptAll, test)

	// 确认读取 secret 仍然正常
	_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(
		ctx,
		testSecret,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("failed to read secret, err: %v", err)
	}

	// 确保集群范围的 secret 读取仍然正常
	_, err = test.restClient.CoreV1().Secrets("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list secrets, err: %v", err)
	}

	// 确保集群范围的 configmap 读取仍然正常
	_, err = test.restClient.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("failed to list configmaps, err: %v", err)
	}

	// 使用最后应用的加密配置重启 kube-apiserver，并断言服务可以启动
	previousConfigDir := test.configDir
	test.shutdownAPIServer()
	restarted = true
	test, err = newTransformTest(t, transformTestConfig{transformerConfigYAML: test.transformerConfig, reload: true, configDir: previousConfigDir, storageConfig: storageConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer test.cleanUp()

	_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(
		ctx,
		testSecret,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("failed to read secret, err: %v", err)
	}

	// 确认重启后集群范围的 secret 读取仍然正常
	if _, err = test.restClient.CoreV1().Secrets("").List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("failed to list secrets, err: %v", err)
	}

	// 确保集群范围的 configmap 读取仍然正常
	if _, err = test.restClient.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("failed to list configmaps, err: %v", err)
	}

	// 使用新的 transformTest 重建 rest client
	copyConfig = rest.CopyConfig(test.kubeAPIServer.ClientConfig)
	copyConfig.GroupVersion = &schema.GroupVersion{}
	copyConfig.NegotiatedSerializer = unstructuredscheme.NewUnstructuredNegotiatedSerializer()
	rc, err = rest.RESTClientFor(copyConfig)
	if err != nil {
		t.Fatal(err)
	}

	// 断言测试运行期间收集的 metrics 符合预期
	// NOTE: 2 次成功的自动重载来自 2 次配置文件更新
	wantMetricStrings := []string{
		`apiserver_encryption_config_controller_automatic_reload_last_timestamp_seconds{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",status="success"} FP`,
		`apiserver_encryption_config_controller_automatic_reloads_total{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",status="success"} 2`,
		fmt.Sprintf(`apiserver_encryption_config_controller_last_config_info{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",hash="%s"} 1`, getHash(encryptionConfigWithoutOldProvider)),
	}

	defer func() {
		body, err := rc.Get().AbsPath("/metrics").DoRaw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var gotMetricStrings []string
		trimFP := regexp.MustCompile(`(.*)(} \d+\.\d+.*)`)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "apiserver_encryption_config_controller_") {
				if strings.Contains(line, "_seconds") {
					line = trimFP.ReplaceAllString(line, `$1`) + "} FP" // 忽略浮点类型的 metric 值
				}
				gotMetricStrings = append(gotMetricStrings, line)
			}
		}
		if diff := cmp.Diff(wantMetricStrings, gotMetricStrings); diff != "" {
			t.Errorf("unexpected metrics diff (-want +got): %s", diff)
		}
	}()
}

func TestEncryptAll(t *testing.T) {
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - '*.*'
    providers:
    - kms:
        name: encrypt-all-kms-provider
        cachesize: 1000
        endpoint: unix:///@encrypt-all-kms-provider.sock
`

	t.Run("encrypt all resources", func(t *testing.T) {
		_ = mock.NewBase64Plugin(t, "@encrypt-all-kms-provider.sock")
		// 确保检查了所有 REST 资源
		featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
			"AllAlpha": true,
			"AllBeta":  true,
		})
		// 该 feature 已弃用，因此需要显式启用
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

		test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, runtimeConfig: []string{"api/alpha=true", "api/beta=true"}})
		if err != nil {
			t.Fatalf("failed to start KUBE API Server with encryptionConfig")
		}
		defer test.cleanUp()

		etcd.CreateTestCRDs(t, apiextensionsclientset.NewForConfigOrDie(test.kubeAPIServer.ClientConfig), false, etcd.GetCustomResourceDefinitionData()...)

		_, serverResources, err := test.restClient.Discovery().ServerGroupsAndResources()
		if err != nil {
			t.Fatal(err)
		}
		resources := etcd.GetResources(t, serverResources)
		client := dynamic.NewForConfigOrDie(test.kubeAPIServer.ClientConfig)

		etcdStorageData := etcd.GetEtcdStorageDataForNamespace(testNamespace)
		restResourceSet := sets.New[schema.GroupVersionResource]()
		stubResourceSet := sets.New[schema.GroupVersionResource]()
		for _, resource := range resources {
			gvr := resource.Mapping.Resource
			stub := etcdStorageData[gvr].Stub

			// stub 为空则跳过
			if stub == "" {
				t.Errorf("skipping resource %s because stub is empty", gvr)
				continue
			}
			restResourceSet.Insert(gvr)
			dynamicClient, obj, err := etcd.JSONToUnstructured(stub, testNamespace, &meta.RESTMapping{
				Resource:         gvr,
				GroupVersionKind: gvr.GroupVersion().WithKind(resource.Mapping.GroupVersionKind.Kind),
				Scope:            resource.Mapping.Scope,
			}, client)
			if err != nil {
				t.Fatal(err)
			}

			_, err = dynamicClient.Create(context.TODO(), obj, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
		}
		for gvr, data := range etcdStorageData {
			if data.Stub == "" {
				continue
			}
			stubResourceSet.Insert(gvr)
		}
		if !restResourceSet.Equal(stubResourceSet) {
			t.Errorf("failed to check all REST resources: %q", restResourceSet.SymmetricDifference(stubResourceSet).UnsortedList())
		}
		rawClient, etcdClient, err := integration.GetEtcdClients(test.kubeAPIServer.ServerOpts.Etcd.StorageConfig.Transport)
		if err != nil {
			t.Fatalf("failed to create etcd client: %v", err)
		}
		// kvClient 是 rawClient 的包装器；为避免 goroutine 泄漏，需要关闭客户端
		// （可通过关闭 rawClient 实现）。
		defer rawClient.Close()

		response, err := etcdClient.Get(context.TODO(), "/"+test.kubeAPIServer.ServerOpts.Etcd.StorageConfig.Prefix, clientv3.WithPrefix())
		if err != nil {
			t.Fatalf("failed to retrieve secret from etcd %v", err)
		}

		// 断言响应中的 key-value 总数大于 0
		if len(response.Kvs) == 0 {
			t.Fatalf("expected total number of keys to be greater than 0, but got %d", len(response.Kvs))
		}

		// 断言响应中的 key 总数大于或等于资源总数
		if len(response.Kvs) < len(resources) {
			t.Fatalf("expected total number of keys to be greater or equal to total resources, but got %d", len(response.Kvs))
		}

		wantPrefix := "k8s:enc:kms:v1:encrypt-all-kms-provider:"
		for _, kv := range response.Kvs {
			// 以下资源不是 REST API，因此未加密；也无法对它们执行存储迁移，
			// 所以不期望它们被加密
			if strings.Contains(kv.String(), "masterleases") ||
				strings.Contains(kv.String(), "peerserverleases") ||
				strings.Contains(kv.String(), "serviceips") ||
				strings.Contains(kv.String(), "servicenodeports") {
				// 断言这些资源未被任何 provider 加密
				if bytes.HasPrefix(kv.Value, []byte("k8s:enc:")) {
					t.Errorf("expected resource %s to not be prefixed with %s, but got %s", kv.Key, "k8s:enc:", kv.Value)
				}
				continue
			}

			// 断言其他所有资源均已加密
			if !bytes.HasPrefix(kv.Value, []byte(wantPrefix)) {
				t.Errorf("expected resource %s to be prefixed with %s, but got %s", kv.Key, wantPrefix, kv.Value)
			}
		}
	})
}

func TestEncryptAllWithWildcard(t *testing.T) {
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - configmaps
    providers:
    - identity: {}
  - resources:
    - '*.batch'
    providers:
    - kms:
        name: kms-provider
        cachesize: 1000
        endpoint: unix:///@kms-provider.sock
  - resources:
    - '*.*'
    providers:
    - kms:
        name: encrypt-all-kms-provider
        cachesize: 1000
        endpoint: unix:///@encrypt-all-kms-provider.sock
`
	_ = mock.NewBase64Plugin(t, "@kms-provider.sock")
	_ = mock.NewBase64Plugin(t, "@encrypt-all-kms-provider.sock")

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer test.cleanUp()

	wantPrefix := "k8s:enc:kms:v1:kms-provider:"
	wantPrefixForEncryptAll := "k8s:enc:kms:v1:encrypt-all-kms-provider:"

	_, err = test.createJob("test-job", "default")
	if err != nil {
		t.Fatalf("failed to create job: %v", err)
	}

	rawJobsEnvelope, err := test.readRawRecordFromETCD(test.getETCDPathForResource(test.storageConfig.Prefix, "", "jobs", "test-job", "default"))
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", test.getETCDPathForResource(test.storageConfig.Prefix, "", "jobs", "test-job", "default"), err)
	}

	// 断言 jobs 的前缀
	if !bytes.HasPrefix(rawJobsEnvelope.Kvs[0].Value, []byte(wantPrefix)) {
		t.Fatalf("expected jobs to be prefixed with %s, but got %s", wantPrefix, rawJobsEnvelope.Kvs[0].Value)
	}

	_, err = test.createDeployment("test-deployment", "default")
	if err != nil {
		t.Fatalf("failed to create deployment: %v", err)
	}

	rawDeploymentsEnvelope, err := test.readRawRecordFromETCD(test.getETCDPathForResource(test.storageConfig.Prefix, "", "deployments", "test-deployment", "default"))
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", test.getETCDPathForResource(test.storageConfig.Prefix, "", "deployments", "test-deployment", "default"), err)
	}

	// 断言 deployments 的前缀
	if !bytes.HasPrefix(rawDeploymentsEnvelope.Kvs[0].Value, []byte(wantPrefixForEncryptAll)) {
		t.Fatalf("expected deployments to be prefixed with %s, but got %s", wantPrefixForEncryptAll, rawDeploymentsEnvelope.Kvs[0].Value)
	}

	test.secret, err = test.createSecret(testSecret, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test secret, error: %v", err)
	}

	rawSecretEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read secrets from etcd: %v", err)
	}

	// 断言 secrets 的前缀
	if !bytes.HasPrefix(rawSecretEnvelope, []byte(wantPrefixForEncryptAll)) {
		t.Fatalf("expected secrets to be prefixed with %s, but got %s", wantPrefixForEncryptAll, rawSecretEnvelope)
	}

	_, err = test.createConfigMap(testConfigmap, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test configmap, error: %v", err)
	}

	rawConfigMapEnvelope, err := test.readRawRecordFromETCD(test.getETCDPathForResource(test.storageConfig.Prefix, "", "configmaps", testConfigmap, testNamespace))
	if err != nil {
		t.Fatalf("failed to read configmaps from etcd: %v", err)
	}

	// 断言 configmap 没有加密数据的前缀
	if bytes.HasPrefix(rawConfigMapEnvelope.Kvs[0].Value, []byte("k8s:enc:")) {
		t.Fatalf("expected configmaps to be not encrypted, got %s", rawConfigMapEnvelope.Kvs[0].Value)
	}
}

func TestEncryptionConfigHotReloadFilePolling(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	// 这让测试响应更快。默认值为 1 分钟。
	encryptionconfigcontroller.EncryptionConfigFileChangePollDuration = time.Second

	testCases := []struct {
		sleep      time.Duration
		name       string
		updateFile func(filePath, fileContent string) error
	}{
		{
			name: "truncate file",
			updateFile: func(filePath string, fileContent string) error {
				// os.WriteFile 在写入前会截断文件
				return os.WriteFile(filePath, []byte(fileContent), 0644)
			},
			// 远长于 KMSCloseGracePeriod
			sleep: 20 * time.Second,
		},
		{
			name: "delete and create file",
			updateFile: func(filePath, fileContent string) error {
				// os.Remove 会先删除文件，再创建新文件
				if err := os.Remove(filePath); err != nil {
					return fmt.Errorf("failed to remove encryption config, err: %w", err)
				}

				file, err := os.Create(filePath)
				if err != nil {
					return fmt.Errorf("failed to create encryption config, err: %w", err)
				}
				defer file.Close()

				if _, err := file.Write([]byte(fileContent)); err != nil {
					return fmt.Errorf("failed to write encryption config, err: %w", err)
				}

				return nil
			},
		},
		{
			name: "move file",
			updateFile: func(filePath, fileContent string) error {
				// 将新配置写入临时文件
				tmpFilePath := filePath + ".tmp"
				if err := os.WriteFile(tmpFilePath, []byte(fileContent), 0644); err != nil {
					return fmt.Errorf("failed to write config to tmp file, err: %w", err)
				}

				// 将临时文件移动到原文件位置
				if err := os.Rename(tmpFilePath, filePath); err != nil {
					return fmt.Errorf("failed to move encryption config, err: %w", err)
				}

				return nil
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: kms-provider
       cachesize: 1000
       endpoint: unix:///@kms-provider.sock
       timeout: 1s
`
			_ = mock.NewBase64Plugin(t, "@kms-provider.sock")

			test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, reload: true})
			if err != nil {
				t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
			}
			defer test.cleanUp()

			test.secret, err = test.createSecret(testSecret, testNamespace)
			if err != nil {
				t.Fatalf("Failed to create test secret, error: %v", err)
			}

			// 测试热重载控制器是否健康
			mustBeHealthy(t, "/poststarthook/start-encryption-provider-config-automatic-reload", "ok", test.kubeAPIServer.ClientConfig)

			encryptionConfigWithNewProvider := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: new-kms-provider-for-secrets
       cachesize: 1000
       endpoint: unix:///@new-kms-provider.sock
       timeout: 1s
    - kms:
       name: kms-provider
       cachesize: 1000
       endpoint: unix:///@kms-provider.sock
       timeout: 1s
  - resources:
    - configmaps
    providers:
    - kms:
       name: new-kms-provider-for-configmaps
       cachesize: 1000
       endpoint: unix:///@new-kms-provider.sock
       timeout: 1s
    - identity: {}
`
			// 启动新的 KMS 插件
			_ = mock.NewBase64Plugin(t, "@new-kms-provider.sock")
			// 更新加密配置
			if err := tc.updateFile(filepath.Join(test.configDir, encryptionConfigFileName), encryptionConfigWithNewProvider); err != nil {
				t.Fatalf("failed to update encryption config, err: %v", err)
			}

			wantPrefix := "k8s:enc:kms:v1:new-kms-provider-for-secrets:"
			verifyPrefixOfSecretResource(t, wantPrefix, test)

			// 确保在“稍后”的时间点一切仍然正常
			if tc.sleep != 0 {
				time.Sleep(tc.sleep)
			}
			_, err = test.createSecret(fmt.Sprintf("secret-%d", rand.Intn(100000)), "default")
			if err != nil {
				t.Fatalf("Failed to create test secret, error: %v", err)
			}
			_, err = test.restClient.CoreV1().Secrets("").List(
				context.TODO(),
				metav1.ListOptions{},
			)
			if err != nil {
				t.Fatalf("failed to re-list secrets, err: %v", err)
			}
		})
	}
}

func verifyPrefixOfSecretResource(t *testing.T, wantPrefix string, test *transformTest) {
	// 采用这种暴力轮询方式而非精巧的 channel 通知，是为了避免在生产代码中加入测试专用代码。
	// 等待配置被感知
	verifyIfKMSTransformersSwapped(t, wantPrefix, "", test)

	// 执行存储迁移
	secretsList, err := test.restClient.CoreV1().Secrets("").List(
		context.TODO(),
		metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("failed to list secrets, err: %v", err)
	}

	for _, secret := range secretsList.Items {
		_, err = test.restClient.CoreV1().Secrets(secret.Namespace).Update(
			context.TODO(),
			&secret,
			metav1.UpdateOptions{},
		)
		if err != nil {
			t.Fatalf("failed to update secret, err: %v", err)
		}
	}

	secretETCDPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", test.secret.Name, test.secret.Namespace)
	rawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}

	// 断言资源具有新的前缀
	if !bytes.HasPrefix(rawEnvelope, []byte(wantPrefix)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefix, rawEnvelope)
	}
}

func verifyIfKMSTransformersSwapped(t *testing.T, wantPrefix, wantPrefixForEncryptAll string, test *transformTest) {
	t.Helper()

	var swapErr error
	// 删除并重建同一个 secret 会导致偶发失败，因此使用不同索引创建新 secret，直到观察到新前缀
	// 生成一个随机整数用于 secret 名称
	idx := rand.Intn(100000)

	pollErr := wait.PollImmediate(time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		// 创建 secret
		secretName := fmt.Sprintf("secret-%d", idx)
		_, err := test.createSecret(secretName, "default")
		if err != nil {
			t.Fatalf("Failed to create test secret, error: %v", err)
		}

		rawEnvelope, err := test.readRawRecordFromETCD(test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", secretName, "default"))
		if err != nil {
			t.Fatalf("failed to read %s from etcd: %v", test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", secretName, "default"), err)
		}

		// 检查前缀
		if !bytes.HasPrefix(rawEnvelope.Kvs[0].Value, []byte(wantPrefix)) {
			idx++

			swapErr = fmt.Errorf("expected secret to be prefixed with %s, but got %s", wantPrefix, rawEnvelope.Kvs[0].Value)

			// 返回 nil error 以继续轮询直到超时
			return false, nil
		}

		if wantPrefixForEncryptAll != "" {
			deploymentName := fmt.Sprintf("deployment-%d", idx)
			_, err := test.createDeployment(deploymentName, "default")
			if err != nil {
				t.Fatalf("Failed to create test secret, error: %v", err)
			}

			rawEnvelope, err := test.readRawRecordFromETCD(test.getETCDPathForResource(test.storageConfig.Prefix, "", "deployments", deploymentName, "default"))
			if err != nil {
				t.Fatalf("failed to read %s from etcd: %v", test.getETCDPathForResource(test.storageConfig.Prefix, "", "deployments", deploymentName, "default"), err)
			}

			// 检查前缀
			if !bytes.HasPrefix(rawEnvelope.Kvs[0].Value, []byte(wantPrefixForEncryptAll)) {
				idx++

				swapErr = fmt.Errorf("expected deployment to be prefixed with %s, but got %s", wantPrefixForEncryptAll, rawEnvelope.Kvs[0].Value)

				// 返回 nil error 以继续轮询直到超时
				return false, nil
			}
		}

		return true, nil
	})
	if wait.Interrupted(pollErr) {
		t.Fatalf("failed to verify if kms transformers swapped, err: %v", swapErr)
	}
}

func updateFile(t *testing.T, configDir, filename string, newContent []byte) {
	t.Helper()

	// 创建临时文件
	tempFile, err := os.CreateTemp(configDir, "tempfile")
	if err != nil {
		t.Fatal(err)
	}
	defer tempFile.Close()

	// 将新内容写入临时文件
	_, err = tempFile.Write(newContent)
	if err != nil {
		t.Fatal(err)
	}

	// 用临时文件原子替换原文件
	err = os.Rename(tempFile.Name(), filepath.Join(configDir, filename))
	if err != nil {
		t.Fatal(err)
	}
}

func TestKMSHealthz(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: provider-1
       endpoint: unix:///@kms-provider-1.sock
    - kms:
       name: provider-2
       endpoint: unix:///@kms-provider-2.sock
`

	pluginMock1 := mock.NewBase64Plugin(t, "@kms-provider-1.sock")
	pluginMock2 := mock.NewBase64Plugin(t, "@kms-provider-2.sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start kube-apiserver, error: %v", err)
	}
	defer test.cleanUp()

	// healthz 检查的名称始终是 "kms-provider-0"，且覆盖所有 kms 插件。

	// 阶段 1——由于所有 kms 插件都保证处于运行状态，healthz 检查：
	// healthz/kms-provider-0 和 /healthz/kms-provider-1 应正常。
	mustBeHealthy(t, "/kms-provider-0", "ok", test.kubeAPIServer.ClientConfig)
	mustBeHealthy(t, "/kms-provider-1", "ok", test.kubeAPIServer.ClientConfig)
	mustNotHaveLivez(t, "/kms-provider-0", "404 page not found", test.kubeAPIServer.ClientConfig)
	mustNotHaveLivez(t, "/kms-provider-1", "404 page not found", test.kubeAPIServer.ClientConfig)

	// 阶段 2——provider-1 的 kms 插件已停止。因此预期 healthz 检查失败，
	// 并报告 provider-1 已停止
	pluginMock1.EnterFailedState()
	mustBeUnHealthy(t, "/kms-provider-0",
		"internal server error: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)

	mustNotHaveLivez(t, "/kms-provider-0", "404 page not found", test.kubeAPIServer.ClientConfig)
	mustBeHealthy(t, "/kms-provider-1", "ok", test.kubeAPIServer.ClientConfig)
	mustNotHaveLivez(t, "/kms-provider-1", "404 page not found", test.kubeAPIServer.ClientConfig)
	pluginMock1.ExitFailedState()

	// 阶段 3——provider-1 的 kms 插件现已恢复。因此预期 provider-1 的健康检查成功，
	// 但 provider-2 此时已停止。
	pluginMock2.EnterFailedState()
	mustBeHealthy(t, "/kms-provider-0", "ok", test.kubeAPIServer.ClientConfig)
	mustBeUnHealthy(t, "/kms-provider-1",
		"internal server error: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)
	pluginMock2.ExitFailedState()

	// 阶段 4——所有 kms 插件再次恢复运行，
	// healthz 检查应正常。
	mustBeHealthy(t, "/kms-provider-0", "ok", test.kubeAPIServer.ClientConfig)
	mustBeHealthy(t, "/kms-provider-1", "ok", test.kubeAPIServer.ClientConfig)
}

func TestKMSHealthzWithReload(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       name: provider-1
       endpoint: unix:///@kms-provider-1.sock
    - kms:
       name: provider-2
       endpoint: unix:///@kms-provider-2.sock
`

	pluginMock1 := mock.NewBase64Plugin(t, "@kms-provider-1.sock")
	pluginMock2 := mock.NewBase64Plugin(t, "@kms-provider-2.sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, reload: true})
	if err != nil {
		t.Fatalf("Failed to start kube-apiserver, error: %v", err)
	}
	defer test.cleanUp()

	// healthz 检查的名称始终是 "kms-provider-0"，且覆盖所有 kms 插件。

	// 阶段 1——由于所有 kms 插件都保证处于运行状态，
	// healthz 检查应正常。
	mustBeHealthy(t, "/kms-providers", "ok", test.kubeAPIServer.ClientConfig)

	// 阶段 2——provider-1 的 kms 插件已停止。因此预期 healthz 检查失败，
	// 并报告 provider-1 已停止
	pluginMock1.EnterFailedState()
	mustBeUnHealthy(t, "/kms-providers",
		"internal server error: kms-provider-0: failed to perform encrypt section of the healthz check for KMS Provider provider-1, error: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)
	pluginMock1.ExitFailedState()

	// 阶段 3——provider-1 的 kms 插件现已恢复。因此预期 provider-1 的健康检查成功，
	// 但 provider-2 此时已停止。
	pluginMock2.EnterFailedState()
	mustBeUnHealthy(t, "/kms-providers",
		"internal server error: kms-provider-1: failed to perform encrypt section of the healthz check for KMS Provider provider-2, error: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)
	pluginMock2.ExitFailedState()

	// 阶段 4——所有 kms 插件再次恢复运行，
	// healthz 检查应正常。
	mustBeHealthy(t, "/kms-providers", "ok", test.kubeAPIServer.ClientConfig)
}

func getHash(data string) string {
	if len(data) == 0 {
		return ""
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(data)))
}
