//go:build !windows

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

package transformation

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	corev1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured/unstructuredscheme"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/generic"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2"
	kmstypes "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2/v2"
	kmsv2mock "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/testing/v2"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	kmsv2api "k8s.io/kms/apis/v2"
	kmsv2svc "k8s.io/kms/pkg/service"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/kubeapiserver"
	secretstore "k8s.io/kubernetes/pkg/registry/core/secret/storage"
	"k8s.io/kubernetes/test/integration"
	"k8s.io/kubernetes/test/integration/etcd"
	"k8s.io/kubernetes/test/integration/framework"
)

type envelopekmsv2 struct {
	providerName       string
	rawEnvelope        []byte
	plainTextDEKSource []byte
	useSeed            bool
}

func (r envelopekmsv2) prefix() string {
	return fmt.Sprintf("k8s:enc:kms:v2:%s:", r.providerName)
}

func (r envelopekmsv2) prefixLen() int {
	return len(r.prefix())
}

func (r envelopekmsv2) cipherTextDEKSource() ([]byte, error) {
	o := &kmstypes.EncryptedObject{}
	if err := proto.Unmarshal(r.rawEnvelope[r.startOfPayload(r.providerName):], o); err != nil {
		return nil, err
	}

	if err := kmsv2.ValidateEncryptedObject(o); err != nil {
		return nil, err
	}

	if r.useSeed && o.EncryptedDEKSourceType != kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED {
		return nil, fmt.Errorf("wrong type used with useSeed=true")
	}

	if !r.useSeed && o.EncryptedDEKSourceType != kmstypes.EncryptedDEKSourceType_AES_GCM_KEY {
		return nil, fmt.Errorf("wrong type used with useSeed=false")
	}

	return o.EncryptedDEKSource, nil
}

func (r envelopekmsv2) startOfPayload(_ string) int {
	return r.prefixLen()
}

func (r envelopekmsv2) cipherTextPayload() ([]byte, error) {
	o := &kmstypes.EncryptedObject{}
	if err := proto.Unmarshal(r.rawEnvelope[r.startOfPayload(r.providerName):], o); err != nil {
		return nil, err
	}

	if err := kmsv2.ValidateEncryptedObject(o); err != nil {
		return nil, err
	}

	return o.EncryptedData, nil
}

func (r envelopekmsv2) plainTextPayload(secretETCDPath string) ([]byte, error) {
	var transformer value.Read
	var err error
	if r.useSeed {
		transformer, err = aestransformer.NewHKDFExtendedNonceGCMTransformer(r.plainTextDEKSource)
	} else {
		var block cipher.Block
		block, err = aes.NewCipher(r.plainTextDEKSource)
		if err != nil {
			return nil, err
		}
		transformer, err = aestransformer.NewGCMTransformer(block)
	}
	if err != nil {
		return nil, err
	}

	ctx := context.Background()
	dataCtx := value.DefaultContext(secretETCDPath)

	data, err := r.cipherTextPayload()
	if err != nil {
		return nil, fmt.Errorf("failed to get cipher text payload: %v", err)
	}
	plainSecret, _, err := transformer.TransformFromStorage(ctx, data, dataCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to transform from storage via AESGCM, err: %w", err)
	}

	return plainSecret, nil
}

// TestDefaultValues 不设置任何 feature flag，也不调用 SetKDFForTests，
// 验证默认值下写入 etcd 的数据使用 KDF。
func TestDefaultValues(t *testing.T) {
	if encryptionconfig.GetKDF("") != true {
		t.Fatalf("without updating the feature flags, default value of KMSv2KDF should be enabled.")
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.KMSv1) != false {
		t.Fatalf("without updating the feature flags, default value of KMSv1 should be disabled.")
	}

	t.Parallel() // 上面的 feature gate 检查依赖全局状态，必须检查完才能并行。

	// GetKDF() 默认为 true，因此下面验证 EncryptedDEKSourceType
	// 是否为 HKDF_SHA256_XNONCE_AES_GCM_SEED。
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - pods
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider-defaults
       endpoint: unix:///@kms-provider-defaults.sock
`
	_ = kmsv2mock.NewBase64Plugin(t, "@kms-provider-defaults.sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	t.Cleanup(test.cleanUp)

	client := kubernetes.NewForConfigOrDie(test.kubeAPIServer.ClientConfig)
	if _, err := client.CoreV1().Pods(testNamespace).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "busybox",
					Image: "busybox",
				},
			},
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	config := test.kubeAPIServer.ServerOpts.Etcd.StorageConfig
	rawClient, etcdClient, err := integration.GetEtcdClients(config.Transport)
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })

	response, err := etcdClient.Get(ctx, "/"+config.Prefix+"/pods/"+testNamespace+"/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Kvs) != 1 {
		t.Fatalf("expected 1 KVs, but got %d", len(response.Kvs))
	}
	object := kmstypes.EncryptedObject{}
	v := bytes.TrimPrefix(response.Kvs[0].Value, []byte("k8s:enc:kms:v2:kms-provider-defaults:"))
	if err := proto.Unmarshal(v, &object); err != nil {
		t.Fatal(err)
	}
	if object.EncryptedDEKSourceType != kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED {
		t.Errorf("invalid type: %d", object.EncryptedDEKSourceType)
	}
}

// TestKMSv2Provider 覆盖 KubeAPI、ETCD 与 KMSv2 插件的集成契约：
// 1. ETCD 中经 KMSv2 处理的原始记录带有 k8s:enc:kms:v2:<plugin name>: 前缀。
// 2. envelopeTransformer 生成 DEK/DEK seed 并通过 gRPC 传给插件。
// 3. 插件用 KEK 加密 DEK/seed 后回传。
// 4. 密文（如 Secret）由 AES GCM / extended nonce GCM 加密。
// 5. 加密结果按 kmstypes.EncryptedObject 结构序列化后写入 ETCD。
func TestKMSv2Provider(t *testing.T) {
	// API server ID 与指标都是全局状态，因此本测试不能并行运行。
	defaultUseSeed := encryptionconfig.GetKDF("")

	t.Run("regular gcm", func(t *testing.T) {
		kmsName := "kms-provider-v2-false"
		defer encryptionconfig.SetKDFForTests(kmsName, false)()
		testKMSv2Provider(t, kmsName, !defaultUseSeed)
	})
	t.Run("extended nonce gcm", func(t *testing.T) {
		kmsName := "kms-provider-v2-true"
		defer encryptionconfig.SetKDFForTests(kmsName, true)()
		testKMSv2Provider(t, kmsName, defaultUseSeed)
	})
}

func testKMSv2Provider(t *testing.T, kmsName string, useSeed bool) {
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: ` + kmsName + `
       endpoint: unix:///@` + kmsName + `.sock
`
	genericapiserver.SetHostnameFuncForTests("testAPIServerID")
	pluginMock := kmsv2mock.NewBase64Plugin(t, "@"+kmsName+".sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer test.cleanUp()

	ctx := testContext(t)

	// 全局指标注册表跨测试复用，这里先重置以便断言。
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

	// 核对本轮采集到的指标是否符合预期。
	wantMetricStrings := []string{
		`apiserver_envelope_encryption_dek_source_cache_size{provider_name="` + kmsName + `"} 1`,
		`apiserver_envelope_encryption_key_id_hash_last_timestamp_seconds{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",key_id_hash="sha256:6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",provider_name="` + kmsName + `",transformation_type="from_storage"} FP`,
		`apiserver_envelope_encryption_key_id_hash_last_timestamp_seconds{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",key_id_hash="sha256:6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",provider_name="` + kmsName + `",transformation_type="to_storage"} FP`,
		`apiserver_envelope_encryption_key_id_hash_total{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",key_id_hash="sha256:6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",provider_name="` + kmsName + `",transformation_type="from_storage"} 2`,
		`apiserver_envelope_encryption_key_id_hash_total{apiserver_id_hash="sha256:3c607df3b2bf22c9d9f01d5314b4bbf411c48ef43ff44ff29b1d55b41367c795",key_id_hash="sha256:6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",provider_name="` + kmsName + `",transformation_type="to_storage"} 1`,
		`apiserver_storage_transformation_operations_total{resource="secrets",status="OK",transformation_type="from_storage",transformer_prefix="k8s:enc:kms:v2:` + kmsName + `:"} 2`,
		`apiserver_storage_transformation_operations_total{resource="secrets",status="OK",transformation_type="to_storage",transformer_prefix="k8s:enc:kms:v2:` + kmsName + `:"} 1`,
	}
	defer func() {
		body, err := rc.Get().AbsPath("/metrics").DoRaw(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var gotMetricStrings []string
		trimFP := regexp.MustCompile(`(.*)(} \d+\.\d+.*)`)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "apiserver_envelope_") || strings.HasPrefix(line, "apiserver_storage_transformation_operations") {
				if strings.HasPrefix(line, "apiserver_envelope_encryption_dek_cache_fill_percent") {
					continue // KMS v1 专有，忽略即可。
				}

				if strings.Contains(line, "_seconds") {
					line = trimFP.ReplaceAllString(line, `$1`) + "} FP" // 浮点指标值不稳定，统一折算为 FP。
				}

				gotMetricStrings = append(gotMetricStrings, line)
			}
		}
		if diff := cmp.Diff(wantMetricStrings, gotMetricStrings); diff != "" {
			t.Errorf("unexpected metrics diff (-want +got): %s", diff)
		}
	}()

	test.secret, err = test.createSecret(testSecret, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test secret, error: %v", err)
	}

	plainTextDEKSource := pluginMock.LastEncryptRequest()

	secretETCDPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", test.secret.Name, test.secret.Namespace)
	rawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}

	envelopeData := envelopekmsv2{
		providerName:       kmsName,
		rawEnvelope:        rawEnvelope,
		plainTextDEKSource: plainTextDEKSource,
		useSeed:            useSeed,
	}

	wantPrefix := envelopeData.prefix()
	if !bytes.HasPrefix(rawEnvelope, []byte(wantPrefix)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefix, rawEnvelope)
	}

	ciphertext, err := envelopeData.cipherTextDEKSource()
	if err != nil {
		t.Fatalf("failed to get ciphertext DEK/seed from KMSv2 Plugin: %v", err)
	}
	decryptResponse, err := pluginMock.Decrypt(ctx, &kmsv2api.DecryptRequest{Uid: string(uuid.NewUUID()), Ciphertext: ciphertext})
	if err != nil {
		t.Fatalf("failed to decrypt DEK, %v", err)
	}
	dekSourcePlainAsWouldBeSeenByETCD := decryptResponse.Plaintext

	if !bytes.Equal(plainTextDEKSource, dekSourcePlainAsWouldBeSeenByETCD) {
		t.Fatalf("expected plainTextDEKSource %v to be passed to KMS Plugin, but got %s",
			plainTextDEKSource, dekSourcePlainAsWouldBeSeenByETCD)
	}

	plainSecret, err := envelopeData.plainTextPayload(secretETCDPath)
	if err != nil {
		t.Fatalf("failed to transform from storage via AESGCM, err: %v", err)
	}

	if !strings.Contains(string(plainSecret), secretVal) {
		t.Fatalf("expected %q after decryption, but got %q", secretVal, string(plainSecret))
	}

	secretClient := test.restClient.CoreV1().Secrets(testNamespace)
	// 直接从 kube-apiserver 读取 Secret 时应已解封装。
	s, err := secretClient.Get(ctx, testSecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get Secret from %s, err: %v", testNamespace, err)
	}
	if secretVal != string(s.Data[secretKey]) {
		t.Fatalf("expected %s from KubeAPI, but got %s", secretVal, string(s.Data[secretKey]))
	}
}

// TestKMSv2ProviderKeyIDStaleness 验证 KubeAPI 与 KMSv2 插件在空更新下的行为：
// 1. key ID 不变时，resourceVersion 不应变化。
// 2. key ID 变化时，resourceVersion 变化一次。
// 3. 之后的更新不应再改变 resourceVersion。
// 4. 插件不可用时，只要 DEK/seed 仍有效，创建 Pod 与加密应成功。
// 5. 插件不可用时，DEK/seed 有效的前提下空更新不应改变 RV。
// 6. 插件不可用时，DEK/seed 失效后创建 Pod 与加密应失败。
// 7. 插件不可用时，DEK/seed 有效的前提下空更新不应改变 RV。
func TestKMSv2ProviderKeyIDStaleness(t *testing.T) {
	t.Parallel()

	t.Run("regular gcm", func(t *testing.T) {
		t.Parallel()
		kmsName := "kms-provider-key-id-stale-false"
		testKMSv2ProviderKeyIDStaleness(t, kmsName, encryptionconfig.SetKDFForTests(kmsName, false))
	})
	t.Run("extended nonce gcm", func(t *testing.T) {
		t.Parallel()
		testKMSv2ProviderKeyIDStaleness(t, "kms-provider-key-id-stale-true", func() {})
	})
}

func testKMSv2ProviderKeyIDStaleness(t *testing.T, kmsName string, resetSetKDFForTests func()) {
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - pods
    - deployments.apps
    providers:
    - kms:
       apiVersion: v2
       name: ` + kmsName + `
       endpoint: unix:///@` + kmsName + `.sock
`
	pluginMock := kmsv2mock.NewBase64Plugin(t, "@"+kmsName+".sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer test.cleanUp()

	dynamicClient := dynamic.NewForConfigOrDie(test.kubeAPIServer.ClientConfig)

	testPod, err := test.createPod(testNamespace, dynamicClient)
	if err != nil {
		t.Fatalf("Failed to create test pod, error: %v, ns: %s", err, testNamespace)
	}
	version1 := testPod.GetResourceVersion()

	// 1. 空更新不应改变 Pod 的 RV。
	updatedPod, err := test.inplaceUpdatePod(testNamespace, testPod, dynamicClient)
	if err != nil {
		t.Fatalf("Failed to update test pod, error: %v, ns: %s", err, testNamespace)
	}
	version2 := updatedPod.GetResourceVersion()
	if version1 != version2 {
		t.Fatalf("Resource version should not have changed. old pod: %v, new pod: %v", testPod, updatedPod)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	useSeed := encryptionconfig.GetKDF(kmsName)

	var firstEncryptedDEKSource []byte
	var f checkFunc
	if useSeed {
		f = func(_ int, _ uint64, etcdKey string, obj *kmstypes.EncryptedObject) {
			firstEncryptedDEKSource = obj.EncryptedDEKSource

			if obj.KeyID != "1" {
				t.Errorf("key %s: want key ID %s, got %s", etcdKey, "1", obj.KeyID)
			}
		}
	} else {
		f = func(_ int, counter uint64, etcdKey string, obj *kmstypes.EncryptedObject) {
			firstEncryptedDEKSource = obj.EncryptedDEKSource

			if obj.KeyID != "1" {
				t.Errorf("key %s: want key ID %s, got %s", etcdKey, "1", obj.KeyID)
			}

			// 使用第一个 key 时，以下步骤会产生加密记录：
			// - 创建
			const want = 1_000_000_000 + 1 // 计数器初始值为十亿。
			if want != counter {
				t.Errorf("key %s: counter nonce is invalid: want %d, got %d", etcdKey, want, counter)
			}
		}
	}
	assertPodDEKSources(ctx, t, test.kubeAPIServer.ServerOpts.Etcd.StorageConfig,
		1, 1, kmsName, "k8s:enc:kms:v2:"+kmsName+":", f,
	)
	if len(firstEncryptedDEKSource) == 0 {
		t.Fatal("unexpected empty DEK or seed")
	}

	// 2. keyID 更新后的空更新应改变 RV。
	pluginMock.UpdateKeyID()
	if err := kmsv2mock.WaitForBase64PluginToBeUpdated(pluginMock); err != nil {
		t.Fatalf("Failed to update keyID for plugin, err: %v", err)
	}
	// 以 1 秒为轮询间隔等待 RV 变化，最多等待 1 分钟。

	version3 := ""
	err = wait.Poll(time.Second, time.Minute,
		func() (bool, error) {
			t.Log("polling for in-place update rv change")
			updatedPod, err = test.inplaceUpdatePod(testNamespace, updatedPod, dynamicClient)
			if err != nil {
				return false, err
			}
			version3 = updatedPod.GetResourceVersion()
			if version1 != version3 {
				return true, nil
			}
			return false, nil
		})
	if err != nil {
		t.Fatalf("Failed to detect one resource version update within the allotted time after keyID is updated and pod has been inplace updated, err: %v, ns: %s", err, testNamespace)
	}

	if version1 == version3 {
		t.Fatalf("Resource version should have changed after keyID update. old pod: %v, new pod: %v", testPod, updatedPod)
	}

	var wantCount uint64 = 1_000_000_000 // 计数器初始值为十亿。
	wantCount++                          // 原地更新也会改变 RV，记一次。

	// 使用第二个 key 时，以下步骤会产生加密记录：
	// - 原地更新（RV 变化）
	// - 删除（通过更新设置 deletion timestamp）
	// - 创建
	var checkDEK checkFunc
	if useSeed {
		checkDEK = func(_ int, _ uint64, etcdKey string, obj *kmstypes.EncryptedObject) {
			if len(obj.EncryptedDEKSource) == 0 {
				t.Error("unexpected empty DEK source")
			}

			if bytes.Equal(obj.EncryptedDEKSource, firstEncryptedDEKSource) {
				t.Errorf("key %s: incorrectly has the same ESEED", etcdKey)
			}

			if obj.KeyID != "2" {
				t.Errorf("key %s: want key ID %s, got %s", etcdKey, "2", obj.KeyID)
			}
		}
	} else {
		checkDEK = func(_ int, counter uint64, etcdKey string, obj *kmstypes.EncryptedObject) {
			if len(obj.EncryptedDEKSource) == 0 {
				t.Error("unexpected empty DEK source")
			}

			if bytes.Equal(obj.EncryptedDEKSource, firstEncryptedDEKSource) {
				t.Errorf("key %s: incorrectly has the same EDEK", etcdKey)
			}

			if obj.KeyID != "2" {
				t.Errorf("key %s: want key ID %s, got %s", etcdKey, "2", obj.KeyID)
			}

			if wantCount != counter {
				t.Errorf("key %s: counter nonce is invalid: want %d, got %d", etcdKey, wantCount, counter)
			}
		}
	}

	// 3. 更新后 Pod 的空更新不应改变 RV。
	updatedPod, err = test.inplaceUpdatePod(testNamespace, updatedPod, dynamicClient)
	if err != nil {
		t.Fatalf("Failed to update test pod, error: %v, ns: %s", err, testNamespace)
	}
	version4 := updatedPod.GetResourceVersion()
	if version3 != version4 {
		t.Fatalf("Resource version should not have changed again after the initial version updated as a result of the keyID update. old pod: %v, new pod: %v", testPod, updatedPod)
	}

	// 删除 Pod，以便后续用新 key 重建。
	if err := test.deletePod(testNamespace, dynamicClient); err != nil {
		t.Fatalf("failed to delete test pod: %v", err)
	}
	wantCount++ // Pod 随后被删除，计数会继续增长，因此不能断言恰好为 2。

	// 4. kms-plugin 不可用时，DEK 仍有效，新建 Pod 与加密应当成功。
	pluginMock.EnterFailedState()
	mustBeUnHealthy(t, "/kms-providers",
		"internal server error: kms-provider-0: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)

	newPod, err := test.createPod(testNamespace, dynamicClient)
	if err != nil {
		t.Fatalf("Create test pod should have succeeded due to valid DEK, ns: %s, got: %v", testNamespace, err)
	}
	wantCount++
	version5 := newPod.GetResourceVersion()

	// 5. kms-plugin 不可用但 DEK 有效时，Pod 的空更新应成功且不改变 RV。
	updatedPod, err = test.inplaceUpdatePod(testNamespace, newPod, dynamicClient)
	if err != nil {
		t.Fatalf("Failed to perform no-op update on pod when kms-plugin is down, error: %v, ns: %s", err, testNamespace)
	}
	version6 := updatedPod.GetResourceVersion()
	if version5 != version6 {
		t.Fatalf("Resource version should not have changed again after the initial version updated as a result of the keyID update. old pod: %v, new pod: %v", newPod, updatedPod)
	}

	// 将时间前移使 DEK 失效。
	origNowFunc := kmsv2.GetNowFunc(kmsName)
	t.Cleanup(kmsv2.SetNowFuncForTests(kmsName, func() time.Time {
		return origNowFunc().Add(5 * time.Minute)
	}))

	// 6. kms-plugin 不可用且 DEK 已失效时，新建 Pod 与加密应当失败。
	_, err = test.createPod(testNamespace, dynamicClient)
	if err == nil || !strings.Contains(err.Error(), `encryptedDEKSource with keyID hash "sha256:d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35" expired at 2`) {
		t.Fatalf("Create test pod should have failed due to encryption, ns: %s, got: %v", testNamespace, err)
	}

	// 7. kms-plugin 不可用且 DEK 已失效时，Pod 的空更新应成功且不改变 RV。
	updatedNewPod, err := test.inplaceUpdatePod(testNamespace, newPod, dynamicClient)
	if err != nil {
		t.Fatalf("Failed to perform no-op update on pod when kms-plugin is down, error: %v, ns: %s", err, testNamespace)
	}
	version7 := updatedNewPod.GetResourceVersion()
	if version5 != version7 {
		t.Fatalf("Resource version should not have changed again after the initial version updated as a result of the keyID update. old pod: %v, new pod: %v", newPod, updatedNewPod)
	}

	assertPodDEKSources(ctx, t, test.kubeAPIServer.ServerOpts.Etcd.StorageConfig,
		1, 1, kmsName, "k8s:enc:kms:v2:"+kmsName+":", checkDEK,
	)

	// 恢复插件，等待新的写入恢复正常。
	t.Cleanup(kmsv2.SetNowFuncForTests(kmsName, origNowFunc))
	pluginMock.ExitFailedState()
	err = wait.Poll(time.Second, 3*time.Minute,
		func() (bool, error) {
			t.Log("polling for plugin to be functional")
			_, err = test.createDeployment("panda", testNamespace)
			if err != nil {
				t.Logf("failed to create deployment, plugin is likely still unhealthy: %v", err)
			}
			return err == nil, nil
		})
	if err != nil {
		t.Fatalf("failed to restore plugin health, err: %v, ns: %s", err, testNamespace)
	}

	// 8. 确认插件恢复后空更新仍成功，且 RV 不变。
	updatedNewPod2, err := test.inplaceUpdatePod(testNamespace, updatedNewPod, dynamicClient)
	if err != nil {
		t.Fatalf("Failed to perform no-op update on pod when kms-plugin is up, error: %v, ns: %s", err, testNamespace)
	}
	version8 := updatedNewPod2.GetResourceVersion()
	if version7 != version8 {
		t.Fatalf("Resource version should not have changed after plugin health is restored. old pod: %v, new pod: %v", updatedNewPod, updatedNewPod2)
	}
	resetSetKDFForTests() // 恢复配置，避免后续检查因状态翻转而 panic。
	// 反转当前的 KDF 配置。
	defer encryptionconfig.SetKDFForTests(kmsName, !useSeed)()

	// 9. 确认 KDF 配置变化后空更新会导致 RV 变化。
	var version9 string
	err = wait.Poll(time.Second, 3*time.Minute,
		func() (bool, error) {
			t.Log("polling for in-place update rv change due to KDF config change")
			updatedNewPod2, err = test.inplaceUpdatePod(testNamespace, updatedNewPod2, dynamicClient)
			if err != nil {
				return false, err
			}
			version9 = updatedNewPod2.GetResourceVersion()
			if version8 != version9 {
				return true, nil
			}
			return false, nil
		})
	if err != nil {
		t.Fatalf("Failed to detect one resource version update within the allotted time after KDF config change and pod has been inplace updated, err: %v, ns: %s", err, testNamespace)
	}
}

func TestKMSv2ProviderDEKSourceReuse(t *testing.T) {
	t.Parallel()
	t.Run("regular gcm", func(t *testing.T) {
		t.Parallel()
		kmsName := "kms-provider-dek-reuse-false"
		defer encryptionconfig.SetKDFForTests(kmsName, false)()
		testKMSv2ProviderDEKSourceReuse(t, kmsName,
			func(i int, counter uint64, etcdKey string, obj *kmstypes.EncryptedObject) {
				if obj.KeyID != "1" {
					t.Errorf("key %s: want key ID %s, got %s", etcdKey, "1", obj.KeyID)
				}

				// counter 从十亿开始计数，因此期望值需要在从 0 开始的索引上加十亿零一。
				if uint64(i+1_000_000_000+1) != counter {
					t.Errorf("key %s: counter nonce is invalid: want %d, got %d", etcdKey, i+1, counter)
				}
			},
		)
	})
	t.Run("extended nonce gcm", func(t *testing.T) {
		t.Parallel()
		kmsName := "kms-provider-dek-reuse-true"
		defer encryptionconfig.SetKDFForTests(kmsName, true)()
		testKMSv2ProviderDEKSourceReuse(t, kmsName,
			func(_ int, _ uint64, etcdKey string, obj *kmstypes.EncryptedObject) {
				if obj.KeyID != "1" {
					t.Errorf("key %s: want key ID %s, got %s", etcdKey, "1", obj.KeyID)
				}
			},
		)
	})
}

func testKMSv2ProviderDEKSourceReuse(t *testing.T, kmsName string, f checkFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - pods
    providers:
    - kms:
       apiVersion: v2
       name: ` + kmsName + `
       endpoint: unix:///@` + kmsName + `.sock
`
	_ = kmsv2mock.NewBase64Plugin(t, "@"+kmsName+".sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	t.Cleanup(test.cleanUp)

	client := kubernetes.NewForConfigOrDie(test.kubeAPIServer.ClientConfig)

	const podCount = 1_000

	for i := range podCount {
		if _, err := client.CoreV1().Pods(testNamespace).Create(ctx, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("dek-reuse-%04d", i+1), // 按序号命名，使创建顺序与返回列表顺序及 nonce 计数一致。
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "busybox",
						Image: "busybox",
					},
				},
			},
		}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	assertPodDEKSources(ctx, t, test.kubeAPIServer.ServerOpts.Etcd.StorageConfig,
		podCount, 1, // 测试期间 key ID 不变，因此只应有一个 DEK。
		kmsName, "k8s:enc:kms:v2:"+kmsName+":", f,
	)
}

type checkFunc func(i int, counter uint64, etcdKey string, obj *kmstypes.EncryptedObject)

func assertPodDEKSources(ctx context.Context, t *testing.T, config storagebackend.Config, podCount, dekSourcesCount int, kmsName, kmsPrefix string, f checkFunc) {
	t.Helper()

	rawClient, etcdClient, err := integration.GetEtcdClients(config.Transport)
	if err != nil {
		t.Fatalf("failed to create etcd client: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })

	response, err := etcdClient.Get(ctx, "/"+config.Prefix+"/pods/"+testNamespace+"/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}

	if len(response.Kvs) != podCount {
		t.Fatalf("expected %d KVs, but got %d", podCount, len(response.Kvs))
	}

	useSeed := encryptionconfig.GetKDF(kmsName)

	out := make([]kmstypes.EncryptedObject, len(response.Kvs))
	for i, kv := range response.Kvs {
		v := bytes.TrimPrefix(kv.Value, []byte(kmsPrefix))
		if err := proto.Unmarshal(v, &out[i]); err != nil {
			t.Fatal(err)
		}

		if err := kmsv2.ValidateEncryptedObject(&out[i]); err != nil {
			t.Fatal(err)
		}

		var infoLen int
		if useSeed {
			infoLen = 32
		}

		info := out[i].EncryptedData[:infoLen]
		nonce := out[i].EncryptedData[infoLen : 12+infoLen]
		randN := nonce[:4]
		count := nonce[4:]

		if bytes.Equal(randN, make([]byte, len(randN))) {
			t.Errorf("key %s: got all zeros for first four bytes", string(kv.Key))
		}

		if useSeed {
			if bytes.Equal(info, make([]byte, infoLen)) {
				t.Errorf("key %s: got all zeros for info", string(kv.Key))
			}
		}

		counter := binary.LittleEndian.Uint64(count)
		f(i, counter, string(kv.Key), &out[i])
	}

	uniqueDEKSources := sets.NewString()
	for i := range out {
		object := &out[i]
		uniqueDEKSources.Insert(string(object.EncryptedDEKSource))
		if useSeed {
			if object.EncryptedDEKSourceType != kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED {
				t.Errorf("invalid type: %d", object.EncryptedDEKSourceType)
			}
		} else {
			if object.EncryptedDEKSourceType != kmstypes.EncryptedDEKSourceType_AES_GCM_KEY {
				t.Errorf("invalid type: %d", object.EncryptedDEKSourceType)
			}
		}
	}

	if uniqueDEKSources.Has("") {
		t.Error("unexpected empty DEK source seen")
	}

	if uniqueDEKSources.Len() != dekSourcesCount {
		t.Errorf("expected %d DEK sources, got: %d", dekSourcesCount, uniqueDEKSources.Len())
	}
}

func TestKMSv2Healthz(t *testing.T) {
	t.Parallel()

	defer encryptionconfig.SetKDFForTests("provider-1", randomBool())()
	defer encryptionconfig.SetKDFForTests("provider-2", randomBool())()

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: provider-1
       endpoint: unix:///@kms-provider-1.sock
    - kms:
       apiVersion: v2
       name: provider-2
       endpoint: unix:///@kms-provider-2.sock
`

	pluginMock1 := kmsv2mock.NewBase64Plugin(t, "@kms-provider-1.sock")
	pluginMock2 := kmsv2mock.NewBase64Plugin(t, "@kms-provider-2.sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("Failed to start kube-apiserver, error: %v", err)
	}
	defer test.cleanUp()

	// healthz 检查名固定为 "kms-provider-0"，涵盖所有 kms 插件。

	// 阶段 1：所有 kms 插件正常，healthz 检查应通过。
	mustBeHealthy(t, "/kms-providers", "ok", test.kubeAPIServer.ClientConfig)

	// 阶段 2：provider-1 的插件不可用，healthz 应失败并报告该插件。
	pluginMock1.EnterFailedState()
	mustBeUnHealthy(t, "/kms-providers",
		"internal server error: kms-provider-0: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)
	pluginMock1.ExitFailedState()

	// 阶段 3：provider-1 恢复、provider-2 不可用，healthz 应报告 provider-2。
	pluginMock2.EnterFailedState()
	mustBeUnHealthy(t, "/kms-providers",
		"internal server error: kms-provider-1: rpc error: code = FailedPrecondition desc = failed precondition - key disabled",
		test.kubeAPIServer.ClientConfig)
	pluginMock2.ExitFailedState()

	// 阶段 4：所有插件恢复，healthz 应通过。
	mustBeHealthy(t, "/kms-providers", "ok", test.kubeAPIServer.ClientConfig)

	// 阶段 5：所有插件同时不可用，healthz 应同时报告两处故障。
	pluginMock1.EnterFailedState()
	pluginMock2.EnterFailedState()
	mustBeUnHealthy(t, "/kms-providers",
		"internal server error: "+
			"[kms-provider-0: failed to perform status section of the healthz check for KMS Provider provider-1, error: rpc error: code = FailedPrecondition desc = failed precondition - key disabled,"+
			" kms-provider-1: failed to perform status section of the healthz check for KMS Provider provider-2, error: rpc error: code = FailedPrecondition desc = failed precondition - key disabled]",
		test.kubeAPIServer.ClientConfig)
}

func TestKMSv2SingleService(t *testing.T) {
	defer encryptionconfig.SetKDFForTests("kms-provider-single-service", randomBool())() // 下方改动全局状态，本测试必须串行。

	var kmsv2Calls int
	origEnvelopeKMSv2ServiceFactory := encryptionconfig.EnvelopeKMSv2ServiceFactory
	encryptionconfig.EnvelopeKMSv2ServiceFactory = func(ctx context.Context, endpoint, providerName string, callTimeout time.Duration) (kmsv2svc.Service, error) {
		kmsv2Calls++
		return origEnvelopeKMSv2ServiceFactory(ctx, endpoint, providerName, callTimeout)
	}
	t.Cleanup(func() {
		encryptionconfig.EnvelopeKMSv2ServiceFactory = origEnvelopeKMSv2ServiceFactory
	})

	// 校验串联起来的三个 server 各自提供的资源。
	// - pods and config maps from KAS
	// - CRDs and CRs from API extensions
	// - API services from aggregator
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - pods
    - configmaps
    - customresourcedefinitions.apiextensions.k8s.io
    - pandas.awesome.bears.com
    - apiservices.apiregistration.k8s.io
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider-single-service
       endpoint: unix:///@kms-provider-single-service.sock
`

	_ = kmsv2mock.NewBase64Plugin(t, "@kms-provider-single-service.sock")

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	t.Cleanup(test.cleanUp)

	// CR 的存储注册表是动态构建的，需要创建一个 CR 来验证这条链路。
	etcd.CreateTestCRDs(t, apiextensionsclientset.NewForConfigOrDie(test.kubeAPIServer.ClientConfig), false, etcd.GetCustomResourceDefinitionData()...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	gvr := schema.GroupVersionResource{Group: "awesome.bears.com", Version: "v1", Resource: "pandas"}
	stub := etcd.GetEtcdStorageData()[gvr].Stub
	dynamicClient, obj, err := etcd.JSONToUnstructured(stub, "", &meta.RESTMapping{
		Resource:         gvr,
		GroupVersionKind: gvr.GroupVersion().WithKind("Panda"),
		Scope:            meta.RESTScopeRoot,
	}, dynamic.NewForConfigOrDie(test.kubeAPIServer.ClientConfig))
	if err != nil {
		t.Fatal(err)
	}
	_, err = dynamicClient.Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	if kmsv2Calls != 1 {
		t.Fatalf("expected a single call to KMS v2 service factory: %v", kmsv2Calls)
	}
}

// TestKMSv2FeatureFlag 验证 KubeAPI 与 ETCD 的集成：
// 1. 启用 feature flag 后，带 KMSv2 的 encryptionConfig 应能加载。
// 2. 重启后仍能加载同样的配置，且能解密此前用 v2 加密的数据。
func TestKMSv2FeatureFlag(t *testing.T) {
	t.Parallel()

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider-feature-flag
       endpoint: unix:///@kms-provider-feature-flag.sock
`
	providerName := "kms-provider-feature-flag"
	pluginMock := kmsv2mock.NewBase64Plugin(t, "@kms-provider-feature-flag.sock")
	storageConfig := framework.SharedEtcd()

	// KMSv2 默认启用，带该配置的 encryptionConfig 应能正常加载。
	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, storageConfig: storageConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer func() {
		test.cleanUp()
	}()

	test.secret, err = test.createSecret(testSecret, testNamespace)
	if err != nil {
		t.Fatalf("Failed to create test secret, error: %v", err)
	}

	// DEK 是随机生成的，只能从 KMS mock 中取回实际使用的值。
	plainTextDEKSource := pluginMock.LastEncryptRequest()

	secretETCDPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", test.secret.Name, test.secret.Namespace)
	rawEnvelope, err := test.getRawSecretFromETCD()
	if err != nil {
		t.Fatalf("failed to read %s from etcd: %v", secretETCDPath, err)
	}

	envelopeData := envelopekmsv2{
		providerName:       providerName,
		rawEnvelope:        rawEnvelope,
		plainTextDEKSource: plainTextDEKSource,
		useSeed:            true, // 本用例预期默认启用 KMSv2KDF。
	}

	wantPrefix := envelopeData.prefix()
	if !bytes.HasPrefix(rawEnvelope, []byte(wantPrefix)) {
		t.Fatalf("expected secret to be prefixed with %s, but got %s", wantPrefix, rawEnvelope)
	}

	ctx := testContext(t)
	ciphertext, err := envelopeData.cipherTextDEKSource()
	if err != nil {
		t.Fatalf("failed to get ciphertext DEK from KMSv2 Plugin: %v", err)
	}
	decryptResponse, err := pluginMock.Decrypt(ctx, &kmsv2api.DecryptRequest{Uid: string(uuid.NewUUID()), Ciphertext: ciphertext})
	if err != nil {
		t.Fatalf("failed to decrypt DEK, %v", err)
	}
	dekPlainAsWouldBeSeenByETCD := decryptResponse.Plaintext

	if !bytes.Equal(plainTextDEKSource, dekPlainAsWouldBeSeenByETCD) {
		t.Fatalf("expected plainTextDEKSource %v to be passed to KMS Plugin, but got %s",
			plainTextDEKSource, dekPlainAsWouldBeSeenByETCD)
	}

	plainSecret, err := envelopeData.plainTextPayload(secretETCDPath)
	if err != nil {
		t.Fatalf("failed to transform from storage via AESGCM, err: %v", err)
	}

	if !strings.Contains(string(plainSecret), secretVal) {
		t.Fatalf("expected %q after decryption, but got %q", secretVal, string(plainSecret))
	}

	secretClient := test.restClient.CoreV1().Secrets(testNamespace)
	// 直接从 kube-apiserver 读取 Secret 时应已解封装。
	s, err := secretClient.Get(ctx, testSecret, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get Secret from %s, err: %v", testNamespace, err)
	}
	if secretVal != string(s.Data[secretKey]) {
		t.Fatalf("expected %s from KubeAPI, but got %s", secretVal, string(s.Data[secretKey]))
	}
	test.shutdownAPIServer()

	// 重启后应能加载使用同一 KMSv2 插件的加密配置，并解密此前用 v2 加密的数据。

	test, err = newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, storageConfig: storageConfig})
	if err != nil {
		t.Fatalf("Failed to restart api server, error: %v", err)
	}

	// 读取由同一插件加密的旧 Secret 不应失败。
	s, err = test.restClient.CoreV1().Secrets(testNamespace).Get(
		ctx,
		testSecret,
		metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("failed to read secret, err: %v", err)
	}
	if secretVal != string(s.Data[secretKey]) {
		t.Fatalf("expected %s from KubeAPI, but got %s", secretVal, string(s.Data[secretKey]))
	}
}

var benchSecret *api.Secret

func BenchmarkKMSv2KDF(b *testing.B) {
	b.StopTimer()

	klog.SetOutput(io.Discard)
	klog.LogToStderr(false)

	defer encryptionconfig.SetKDFForTests("kms-provider-bench", false)()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	b.Cleanup(cancel)

	ctx = request.WithNamespace(ctx, testNamespace)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider-bench
       endpoint: unix:///@kms-provider-bench.sock
`
	_ = kmsv2mock.NewBase64Plugin(b, "@kms-provider-bench.sock")

	test, err := newTransformTest(b, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		b.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	b.Cleanup(test.cleanUp)

	client := kubernetes.NewForConfigOrDie(test.kubeAPIServer.ClientConfig)

	restOptionsGetter := getRESTOptionsGetterForSecrets(b, test)

	secretStorage, err := secretstore.NewREST(restOptionsGetter)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(secretStorage.Destroy)

	const dataLen = 1_000

	secrets := make([]*api.Secret, dataLen)

	for i := range dataLen {
		secrets[i] = &api.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-secret-%d", i),
				Namespace: testNamespace,
			},
			Data: map[string][]byte{
				"lots_of_data": bytes.Repeat([]byte{1, 3, 3, 7}, i*dataLen/4),
			},
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err := secretStorage.DeleteCollection(ctx, noValidation, &metav1.DeleteOptions{}, nil)
		if err != nil {
			b.Fatal(err)
		}

		for i := range dataLen {
			out, err := secretStorage.Create(ctx, secrets[i], noValidation, &metav1.CreateOptions{})
			if err != nil {
				b.Fatal(err)
			}

			benchSecret = out.(*api.Secret)

			out, err = secretStorage.Get(ctx, benchSecret.Name, &metav1.GetOptions{})
			if err != nil {
				b.Fatal(err)
			}

			benchSecret = out.(*api.Secret)
		}
	}
	b.StopTimer()

	secretList, err := client.CoreV1().Secrets(testNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		b.Fatal(err)
	}

	if secretLen := len(secretList.Items); secretLen != dataLen {
		b.Errorf("unexpected secret len: want %d, got %d", dataLen, secretLen)
	}
}

func getRESTOptionsGetterForSecrets(t testing.TB, test *transformTest) generic.RESTOptionsGetter {
	t.Helper()

	s := test.kubeAPIServer.ServerOpts

	etcdConfigCopy := *s.Etcd
	etcdConfigCopy.SkipHealthEndpoints = true                     // 跳过健康检查协程，避免其干扰测试。
	etcdConfigCopy.EncryptionProviderConfigAutomaticReload = true // 借自动重载在 t.Cleanup 中使用 DynamicTransformers。

	// 主要复用 BuildGenericConfig 的构造流程。

	genericConfig := genericapiserver.NewConfig(legacyscheme.Codecs)

	genericConfig.MergedResourceConfig = controlplane.DefaultAPIResourceConfigSource()

	if err := s.APIEnablement.ApplyTo(genericConfig, controlplane.DefaultAPIResourceConfigSource(), legacyscheme.Scheme); err != nil {
		t.Fatal(err)
	}

	storageFactoryConfig := kubeapiserver.NewStorageFactoryConfig()
	storageFactoryConfig.APIResourceConfig = genericConfig.MergedResourceConfig
	storageFactory, err := storageFactoryConfig.Complete(&etcdConfigCopy).New()
	if err != nil {
		t.Fatal(err)
	}
	if err := etcdConfigCopy.ApplyWithStorageFactoryTo(storageFactory, genericConfig); err != nil {
		t.Fatal(err)
	}

	transformers, ok := genericConfig.ResourceTransformers.(*encryptionconfig.DynamicTransformers)
	if !ok {
		t.Fatalf("incorrect type for ResourceTransformers: %T", genericConfig.ResourceTransformers)
	}

	t.Cleanup(func() {
		// 借一次重载让现有 transformers 关闭。
		transformers.Set(nil, nil, nil, 0)
		time.Sleep(10 * time.Second) // 让清理等待超过 kmsCloseGracePeriod。
	})

	if genericConfig.RESTOptionsGetter == nil {
		t.Fatal("not REST options found")
	}

	opts, err := genericConfig.RESTOptionsGetter.GetRESTOptions(schema.GroupResource{Group: "", Resource: "secrets"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := runtime.CheckCodec(opts.StorageConfig.Codec, &api.Secret{},
		schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Secret"}); err != nil {
		t.Fatal(err)
	}

	return genericConfig.RESTOptionsGetter
}

func noValidation(_ context.Context, _ runtime.Object) error { return nil }

var benchRESTSecret *corev1.Secret

func BenchmarkKMSv2REST(b *testing.B) {
	b.StopTimer()

	klog.SetOutput(io.Discard)
	klog.LogToStderr(false)

	defer encryptionconfig.SetKDFForTests("kms-provider-bench-rest", true)()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	b.Cleanup(cancel)

	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider-bench-rest
       endpoint: unix:///@kms-provider-bench-rest.sock
`
	_ = kmsv2mock.NewBase64Plugin(b, "@kms-provider-bench-rest.sock")

	test, err := newTransformTest(b, transformTestConfig{transformerConfigYAML: encryptionConfig})
	if err != nil {
		b.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	b.Cleanup(test.cleanUp)

	client := kubernetes.NewForConfigOrDie(test.kubeAPIServer.ClientConfig)

	const dataLen = 1_000

	secretStorage := client.CoreV1().Secrets(testNamespace)

	secrets := make([]*corev1.Secret, dataLen)

	for i := range dataLen {
		secrets[i] = &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-secret-%d", i),
				Namespace: testNamespace,
			},
			Data: map[string][]byte{
				"lots_of_data": bytes.Repeat([]byte{1, 3, 3, 7}, i*dataLen/4),
			},
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err := secretStorage.DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		if err != nil {
			b.Fatal(err)
		}

		for i := range dataLen {
			out, err := secretStorage.Create(ctx, secrets[i], metav1.CreateOptions{})
			if err != nil {
				b.Fatal(err)
			}

			benchRESTSecret = out

			out, err = secretStorage.Get(ctx, benchRESTSecret.Name, metav1.GetOptions{})
			if err != nil {
				b.Fatal(err)
			}

			benchRESTSecret = out
		}
	}
	b.StopTimer()

	secretList, err := client.CoreV1().Secrets(testNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		b.Fatal(err)
	}

	if secretLen := len(secretList.Items); secretLen != dataLen {
		b.Errorf("unexpected secret len: want %d, got %d", dataLen, secretLen)
	}
}

func randomBool() bool { return utilrand.Int()%2 == 1 }

// TestKMSv2ProviderLegacyData 验证最早版本写入的遗留数据仍可被读取。
func TestKMSv2ProviderLegacyData(t *testing.T) {
	// provider 名称被写进了遗留数据，无法更改，因此本测试必须串行。
	t.Run("regular gcm", func(t *testing.T) {
		defer encryptionconfig.SetKDFForTests("kms-provider", false)()
		testKMSv2ProviderLegacyData(t)
	})
	t.Run("extended nonce gcm", func(t *testing.T) {
		defer encryptionconfig.SetKDFForTests("kms-provider", true)()
		testKMSv2ProviderLegacyData(t)
	})
}

func testKMSv2ProviderLegacyData(t *testing.T) {
	encryptionConfig := `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - kms:
       apiVersion: v2
       name: kms-provider
       endpoint: unix:///@kms-provider.sock
`

	_ = kmsv2mock.NewBase64Plugin(t, "@kms-provider.sock")

	// 读取时 value.Context.AuthenticatedData 是资源对应的 etcd 存储路径。
	// 因此手工构造存储配置，让路径保持固定。
	const legacyDataEtcdPrefix = "43da1478-5e9c-4ef3-a92a-2b19d540c8a5"

	storageConfig := storagebackend.NewDefaultConfig(path.Join(legacyDataEtcdPrefix, "registry"), nil)
	storageConfig.Transport.ServerList = []string{framework.GetEtcdURL()}

	test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: encryptionConfig, storageConfig: storageConfig})
	if err != nil {
		t.Fatalf("failed to start KUBE API Server with encryptionConfig\n %s, error: %v", encryptionConfig, err)
	}
	defer test.cleanUp()

	const legacyDataNamespace = "kms-v2-legacy-data"

	if _, err := test.createNamespace(legacyDataNamespace); err != nil {
		t.Fatal(err)
	}

	// commit: 1b4df30b3cdfeaba6024e81e559a6cd09a089d65
	// tag: v1.27.0
	const dekSourceAESGCMKeyName = "dek-source-aes-gcm-key"
	const dekSourceAESGCMKeyData = "k8s:enc:kms:v2:kms-provider:\n\x8a\x03\xb0ƙ\xdc\x01ʚ;\x00\x00\x00\x00\x8c\x80\v\xf0\x1dKo{0\v\xb5\xd5Ń\x1a\xb5\x0e\xcf\xd7Ve\xed邸\xdfE\xedMk\xcf!\x15\xc0\v\t\x82Wh \x9e\x8f\x1b\\9\b\xa4\x80\x02m\xf4P\x14z\xee\xf7\x8c\x1a\x84n5\xfdG\x83v#\x0e\xd4\f\x83YwH\xe1\x1c\xbf\x12\xc6\x1b\xba\x8br\b\x82z\xf8\xdb`\xa7]P\xb1\xe6!Lb\x8d\xb8I\x1aEL\xa0\xae+\xbe\x15R\x8e\x9b\x064\xf6P\xb6;\x9f\xa6\x8d\x96\xb2\x01\xa1\x8e\xe4a\xdf/\x90u\xde6\x9a\xc2ͻb\x88+\x16\x98=\xe9\x03\xdd\xd7HvC\n\xe5\x8cv\x05~\n\xabX_N\x9a\x84wp\xa8\x13\x0f\x82Y9x\xed\x89\x15\xb9\xe1ꦐ\xc6`R\n0\x04\xf2\xa6ѥ\x85\nk\xf4\xcf\xe4ul\x1c*[A\x12\xa0\xd9\xf2\xb5!\x82\xe4\x00\xa4L'&\xf5pln\"\xe0=\f[\xe1\xb0U97\x11|\xdaNk\xc3=\xc2\xf2\x85<7\x1e\x01\xb8\xa9\xf4\x89\xdb~\xe1\x8c\xe1\x1f\x05B@WʼS\n聛LY\x86$\xf6\x01ݝ\xcd\x1d\xe9\x02]\xf4i\xda\xfa\xc2\x0eUr\xc5Dʽdb\x13\xbe\xfe\x1c\xc5\xe1\x84\xcc\xdf&\x93j\x1eK\x04\xba\x06\x16\x85\x0e\x1f\xca\b\x90\x06\x11K\x9d[\rV\xe8E\xd5(\x91\fn\xd4\x10\x9cH\x1cܝX\x94ȁ5\x1b\x8c\xbbz\xf9Ho{\x1d\x112\x90F\xe7\xd8h\xa8\xa1\xf6\x8c\x8cvʲ1\xf9#\x82\xa3\xbe7ed\xd9\x14\xf3\x06\xff\xb7߫i\x12\x011\x1a,gafIvwT0ASoKdZ/D1L2SlH73LUMj5qa3hroljfS51wc=\"2\n\x1blocal-kek.kms.kubernetes.io\x12\x13encrypted-local-kek"
	dekSourceAESGCMKeyPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", dekSourceAESGCMKeyName, legacyDataNamespace)
	if _, err := test.writeRawRecordToETCD(dekSourceAESGCMKeyPath, []byte(dekSourceAESGCMKeyData)); err != nil {
		t.Fatal(err)
	}

	expectedDEKSourceAESGCMKeyObject := &kmstypes.EncryptedObject{
		EncryptedData:          []byte("\xb0ƙ\xdc\x01ʚ;\x00\x00\x00\x00\x8c\x80\v\xf0\x1dKo{0\v\xb5\xd5Ń\x1a\xb5\x0e\xcf\xd7Ve\xed邸\xdfE\xedMk\xcf!\x15\xc0\v\t\x82Wh \x9e\x8f\x1b\\9\b\xa4\x80\x02m\xf4P\x14z\xee\xf7\x8c\x1a\x84n5\xfdG\x83v#\x0e\xd4\f\x83YwH\xe1\x1c\xbf\x12\xc6\x1b\xba\x8br\b\x82z\xf8\xdb`\xa7]P\xb1\xe6!Lb\x8d\xb8I\x1aEL\xa0\xae+\xbe\x15R\x8e\x9b\x064\xf6P\xb6;\x9f\xa6\x8d\x96\xb2\x01\xa1\x8e\xe4a\xdf/\x90u\xde6\x9a\xc2ͻb\x88+\x16\x98=\xe9\x03\xdd\xd7HvC\n\xe5\x8cv\x05~\n\xabX_N\x9a\x84wp\xa8\x13\x0f\x82Y9x\xed\x89\x15\xb9\xe1ꦐ\xc6`R\n0\x04\xf2\xa6ѥ\x85\nk\xf4\xcf\xe4ul\x1c*[A\x12\xa0\xd9\xf2\xb5!\x82\xe4\x00\xa4L'&\xf5pln\"\xe0=\f[\xe1\xb0U97\x11|\xdaNk\xc3=\xc2\xf2\x85<7\x1e\x01\xb8\xa9\xf4\x89\xdb~\xe1\x8c\xe1\x1f\x05B@WʼS\n聛LY\x86$\xf6\x01ݝ\xcd\x1d\xe9\x02]\xf4i\xda\xfa\xc2\x0eUr\xc5Dʽdb\x13\xbe\xfe\x1c\xc5\xe1\x84\xcc\xdf&\x93j\x1eK\x04\xba\x06\x16\x85\x0e\x1f\xca\b\x90\x06\x11K\x9d[\rV\xe8E\xd5(\x91\fn\xd4\x10\x9cH\x1cܝX\x94ȁ5\x1b\x8c\xbbz\xf9Ho{\x1d\x112\x90F\xe7\xd8h\xa8\xa1\xf6\x8c\x8cvʲ1\xf9#\x82\xa3\xbe7ed\xd9\x14\xf3\x06\xff\xb7߫i"),
		KeyID:                  "1",
		EncryptedDEKSource:     []byte("gafIvwT0ASoKdZ/D1L2SlH73LUMj5qa3hroljfS51wc="),
		Annotations:            map[string][]byte{"local-kek.kms.kubernetes.io": []byte("encrypted-local-kek")},
		EncryptedDEKSourceType: kmstypes.EncryptedDEKSourceType_AES_GCM_KEY,
	}
	legacyDEKSourceAESGCMKeyObject := &kmstypes.EncryptedObject{}
	if err := proto.Unmarshal([]byte(strings.TrimPrefix(dekSourceAESGCMKeyData, "k8s:enc:kms:v2:kms-provider:")), legacyDEKSourceAESGCMKeyObject); err != nil {
		t.Fatal(err)
	}
	if err := kmsv2.ValidateEncryptedObject(legacyDEKSourceAESGCMKeyObject); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(expectedDEKSourceAESGCMKeyObject, legacyDEKSourceAESGCMKeyObject) {
		t.Errorf("kms v2 legacy encrypted object diff, want: %+v; got: %+v", expectedDEKSourceAESGCMKeyObject, legacyDEKSourceAESGCMKeyObject)
	}

	// commit: 855e7c48de7388eb330da0f8d9d2394ee818fb8d
	// tag: v1.28.0
	const dekSourceHKDFSHA256XNonceAESGCMSeedName = "dek-source-hkdf-sha256-xnonce-aes-gcm-seed"
	const dekSourceHKDFSHA256XNonceAESGCMSeedData = "k8s:enc:kms:v2:kms-provider:\n\xd1\x03\x12\x0f\x87\xae\xa2\xa2\xf5J\x11\x06о\x8a\x81\xb6\x15\xdf4H\xa3\xb7i\x93^)\xe5i\xe2\x7f\xfdo\xee\"\x170\xb7\n\xa0\v\xec\xe0\xfa\t#\tƖ\xf6ǧw\x01\xb9\xab\xb3\xf4\xdf\xec\x9eJ\x95$&Z=\x8awAc\xa5\xb2;\t\xd5\xf9\x05\xc1E)\x8b\xb8\x14\xc9\xda\x14{I\rV?\xe5:\xf0BM\x9b\xad\xaaHn>W/Q\xa3\xf5\xba\xe7˚\n\xe7\"\xa7\\p\x8c\xba2\xf2\xb0x<Ą\x88\x9a\xf1\xb5:d=z\xe3\xc3&\x03\x99m\x96\xe7\\\xe3\xa3\xd7i\xb2\f\x84g\xf94\xd2\f\xd6~\xed\xac\xf8\x1b\xc6(,[\xd1\xff\xba\x89ȇD\x02):wTM12\xb5\xfdl\xd2\xf2\x85\x120\xd3\xd9aak\xce\xddI֥\x0fk2\xf6I\xd0\xf9\xc2\xda\vŗ\xd7\u05fb\x83\xd6\xf7\x1a\xbf\x13iH\x8f\xe4\xb3#\xf3\xdf\xc8$y\rL(F\\Xt\x86\xbb\xe5K\x88=\x92\xe9\x04\xf70\x1e\t>\xac;\x9e\xe6\xf0+ۙ4\x1d\x1aV9Մ-g\xf3\xc7Z\x00\xf73\x0e\xe7\xa6\xcf\xc4\xfc\xe0\xb2\x1f\xa0\xbb\x1a\x81\xb3\xe4צ\x7f\xc6\b\x94͉\xad@\xac\x81\x015\x0f\xe8A\xe9B\xfb2\x81o\x9c?*\f\x8c\x15\xa8)\"\xe8\xff\x8d8\xd5!O\x17\xc5㍀\x83\xd3´\xca1;\xf7\xb0\xf4\x90x\xa6\x01\x95\x85\xc0\xaf\xf6\x82Qk\xab\xc1\x82<D\x93\xcf\r\xdb\xdf\x1c\x94\x17Q\xfaS\xe6\xcb\xd4Xƿ\x80\x1d\xc4\x1c\x9dP74\x82JK\vy\xe9)\xbchY\xbe\xcc|\xe4\x97\xdd<;3\x90J\a\xee#\xb2y\xe3\t`\xef\xef\x1f\"k\x8b\x96\xa0\x98\xd9\xffs\xde&\xb7\xa6\x0e\xf1\x7f2ͅb\xe3\xda5\"c\\K\xe21\xa2\xec`\x1b\xe5R\xe6j\b@\x187\xe1\xdb\x04\xf6bNO\x0e\x12\x011\x1a,/+WnKXQEM/AhXICYRNBeGk+WSuB+7OBuSYJTbP66Zyc=\"2\n\x1blocal-kek.kms.kubernetes.io\x12\x13encrypted-local-kek(\x01"
	dekSourceHKDFSHA256XNonceAESGCMSeedPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", dekSourceHKDFSHA256XNonceAESGCMSeedName, legacyDataNamespace)
	if _, err := test.writeRawRecordToETCD(dekSourceHKDFSHA256XNonceAESGCMSeedPath, []byte(dekSourceHKDFSHA256XNonceAESGCMSeedData)); err != nil {
		t.Fatal(err)
	}

	expectedDEKSourceHKDFSHA256XNonceAESGCMSeedObject := &kmstypes.EncryptedObject{
		EncryptedData:          []byte("\x12\x0f\x87\xae\xa2\xa2\xf5J\x11\x06о\x8a\x81\xb6\x15\xdf4H\xa3\xb7i\x93^)\xe5i\xe2\x7f\xfdo\xee\"\x170\xb7\n\xa0\v\xec\xe0\xfa\t#\tƖ\xf6ǧw\x01\xb9\xab\xb3\xf4\xdf\xec\x9eJ\x95$&Z=\x8awAc\xa5\xb2;\t\xd5\xf9\x05\xc1E)\x8b\xb8\x14\xc9\xda\x14{I\rV?\xe5:\xf0BM\x9b\xad\xaaHn>W/Q\xa3\xf5\xba\xe7˚\n\xe7\"\xa7\\p\x8c\xba2\xf2\xb0x<Ą\x88\x9a\xf1\xb5:d=z\xe3\xc3&\x03\x99m\x96\xe7\\\xe3\xa3\xd7i\xb2\f\x84g\xf94\xd2\f\xd6~\xed\xac\xf8\x1b\xc6(,[\xd1\xff\xba\x89ȇD\x02):wTM12\xb5\xfdl\xd2\xf2\x85\x120\xd3\xd9aak\xce\xddI֥\x0fk2\xf6I\xd0\xf9\xc2\xda\vŗ\xd7\u05fb\x83\xd6\xf7\x1a\xbf\x13iH\x8f\xe4\xb3#\xf3\xdf\xc8$y\rL(F\\Xt\x86\xbb\xe5K\x88=\x92\xe9\x04\xf70\x1e\t>\xac;\x9e\xe6\xf0+ۙ4\x1d\x1aV9Մ-g\xf3\xc7Z\x00\xf73\x0e\xe7\xa6\xcf\xc4\xfc\xe0\xb2\x1f\xa0\xbb\x1a\x81\xb3\xe4צ\x7f\xc6\b\x94͉\xad@\xac\x81\x015\x0f\xe8A\xe9B\xfb2\x81o\x9c?*\f\x8c\x15\xa8)\"\xe8\xff\x8d8\xd5!O\x17\xc5㍀\x83\xd3´\xca1;\xf7\xb0\xf4\x90x\xa6\x01\x95\x85\xc0\xaf\xf6\x82Qk\xab\xc1\x82<D\x93\xcf\r\xdb\xdf\x1c\x94\x17Q\xfaS\xe6\xcb\xd4Xƿ\x80\x1d\xc4\x1c\x9dP74\x82JK\vy\xe9)\xbchY\xbe\xcc|\xe4\x97\xdd<;3\x90J\a\xee#\xb2y\xe3\t`\xef\xef\x1f\"k\x8b\x96\xa0\x98\xd9\xffs\xde&\xb7\xa6\x0e\xf1\x7f2ͅb\xe3\xda5\"c\\K\xe21\xa2\xec`\x1b\xe5R\xe6j\b@\x187\xe1\xdb\x04\xf6bNO\x0e"),
		KeyID:                  "1",
		EncryptedDEKSource:     []byte("/+WnKXQEM/AhXICYRNBeGk+WSuB+7OBuSYJTbP66Zyc="),
		Annotations:            map[string][]byte{"local-kek.kms.kubernetes.io": []byte("encrypted-local-kek")},
		EncryptedDEKSourceType: kmstypes.EncryptedDEKSourceType_HKDF_SHA256_XNONCE_AES_GCM_SEED,
	}
	legacyDEKSourceHKDFSHA256XNonceAESGCMSeedObject := &kmstypes.EncryptedObject{}
	if err := proto.Unmarshal([]byte(strings.TrimPrefix(dekSourceHKDFSHA256XNonceAESGCMSeedData, "k8s:enc:kms:v2:kms-provider:")), legacyDEKSourceHKDFSHA256XNonceAESGCMSeedObject); err != nil {
		t.Fatal(err)
	}
	if err := kmsv2.ValidateEncryptedObject(legacyDEKSourceHKDFSHA256XNonceAESGCMSeedObject); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(expectedDEKSourceHKDFSHA256XNonceAESGCMSeedObject, legacyDEKSourceHKDFSHA256XNonceAESGCMSeedObject) {
		t.Errorf("kms v2 legacy encrypted object diff, want: %+v; got: %+v", expectedDEKSourceHKDFSHA256XNonceAESGCMSeedObject, legacyDEKSourceHKDFSHA256XNonceAESGCMSeedObject)
	}

	ctx := testContext(t)

	legacySecrets, err := test.restClient.CoreV1().Secrets(legacyDataNamespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	dekSourceAESGCMKeyTime := metav1.NewTime(time.Date(2023, 9, 1, 11, 56, 49, 0, time.FixedZone("EDT", -4*60*60)))
	dekSourceHKDFSHA256XNonceAESGCMSeedTime := metav1.NewTime(time.Date(2023, 9, 1, 10, 23, 13, 0, time.FixedZone("EDT", -4*60*60)))

	expectedLegacySecrets := &corev1.SecretList{
		Items: []corev1.Secret{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              dekSourceAESGCMKeyName,
					Namespace:         legacyDataNamespace,
					UID:               "1f4a8f7b-01b4-49d1-b898-751eb56937f1",
					CreationTimestamp: dekSourceAESGCMKeyTime,
					ManagedFields: []metav1.ManagedFieldsEntry{
						{
							Manager:    "___TestKMSv2Provider_in_k8s_io_kubernetes_test_integration_controlplane_transformation.test",
							Operation:  "Update",
							APIVersion: "v1",
							Time:       &dekSourceAESGCMKeyTime,
							FieldsType: "FieldsV1",
							FieldsV1:   metav1.NewFieldsV1(`{"f:data":{".":{},"f:api_key":{}},"f:type":{}}`),
						},
					},
				},
				Data: map[string][]byte{
					secretKey: []byte(secretVal),
				},
				Type: corev1.SecretTypeOpaque,
			},
			{
				ObjectMeta: metav1.ObjectMeta{
					Name:              dekSourceHKDFSHA256XNonceAESGCMSeedName,
					Namespace:         legacyDataNamespace,
					UID:               "87c514b4-9c26-4041-ad0d-0d07dca557ed",
					CreationTimestamp: dekSourceHKDFSHA256XNonceAESGCMSeedTime,
					ManagedFields: []metav1.ManagedFieldsEntry{
						{
							Manager:    "___TestKMSv2Provider_extended_nonce_gcm_in_k8s_io_kubernetes_test_integration_controlplane_transformation.test",
							Operation:  "Update",
							APIVersion: "v1",
							Time:       &dekSourceHKDFSHA256XNonceAESGCMSeedTime,
							FieldsType: "FieldsV1",
							FieldsV1:   metav1.NewFieldsV1(`{"f:data":{".":{},"f:api_key":{}},"f:type":{}}`),
						},
					},
				},
				Data: map[string][]byte{
					secretKey: []byte(secretVal),
				},
				Type: corev1.SecretTypeOpaque,
			},
		},
	}

	if diff := cmp.Diff(expectedLegacySecrets, legacySecrets,
		// resourceVersion 由解码时依据 etcd 状态回填，并不存在 etcd 的值里。
		cmpopts.IgnoreFields(corev1.Secret{}, "ResourceVersion"),
		cmpopts.IgnoreFields(metav1.ListMeta{}, "ResourceVersion"),
	); len(diff) > 0 {
		t.Errorf("kms v2 legacy secret data diff (-want, +got):\n%s", diff)
	}
}
