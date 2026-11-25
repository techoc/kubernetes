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
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	apiserverv1 "k8s.io/apiserver/pkg/apis/apiserver/v1"
	genericfeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage/value"
	aestransformer "k8s.io/apiserver/pkg/storage/value/encrypt/aes"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/test/integration/authutil"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/utils/ptr"
)

const (
	aesGCMPrefix = "k8s:enc:aesgcm:v1:key1:"
	aesCBCPrefix = "k8s:enc:aescbc:v1:key1:"

	aesGCMConfigYAML = `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - aesgcm:
        keys:
        - name: key1
          secret: c2VjcmV0IGlzIHNlY3VyZQ==
`

	aesCBCConfigYAML = `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - aescbc:
        keys:
        - name: key1
          secret: c2VjcmV0IGlzIHNlY3VyZQ==
`

	identityConfigYAML = `
kind: EncryptionConfiguration
apiVersion: apiserver.config.k8s.io/v1
resources:
  - resources:
    - secrets
    providers:
    - identity: {}
`
)

// TestSecretsShouldBeEnveloped 是 KubeAPI 与 etcd 之间的集成测试，用于检查在向
// KubeAPI server 传入 EncryptionConfiguration 时：
// 1. 写入时 Secret 会被加密
// 2. 读取时 Secret 会被解密
func TestSecretsShouldBeTransformed(t *testing.T) {
	testCases := []struct {
		transformerConfigContent string
		transformerPrefix        string
		unSealFunc               unSealSecret
	}{
		{aesGCMConfigYAML, aesGCMPrefix, unSealWithGCMTransformer},
		{aesCBCConfigYAML, aesCBCPrefix, unSealWithCBCTransformer},
		// TODO: 添加 secretbox
	}
	for _, tt := range testCases {
		test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: tt.transformerConfigContent})
		if err != nil {
			t.Fatalf("failed to setup test for envelop %s, error was %v", tt.transformerPrefix, err)
			continue
		}
		test.secret, err = test.createSecret(testSecret, testNamespace)
		if err != nil {
			t.Fatalf("Failed to create test secret, error: %v", err)
		}
		test.runResource(test.TContext, tt.unSealFunc, tt.transformerPrefix, "", "v1", "secrets", test.secret.Name, test.secret.Namespace)
		test.cleanUp()
	}
}

type verifier interface {
	verify(t *testing.T, err error)
}

// TestAllowUnsafeMalformedObjectDeletionFeature 是一个集成测试，用于验证：
// 1) 如果启用了特性 AllowUnsafeMalformedObjectDeletion，则可以通过启用删除选项
// 'ignoreStoreReadErrorWithClusterBreakingPotential' 来删除损坏的对象；该选项会触发
// 不安全删除流程，
// 绕过所有前置条件检查或 finalizer 约束。
// 2) 如果禁用了特性 AllowUnsafeMalformedObjectDeletion，则删除选项
// 'ignoreStoreReadErrorWithClusterBreakingPotential' 不起
// 作用（使用正常的删除流程）
func TestAllowUnsafeMalformedObjectDeletionFeature(t *testing.T) {
	tests := []struct {
		// 特性是否已启用
		featureEnabled bool
		// 变更之后加密是否已失效
		encryptionBrokenFn func(t *testing.T, got apierrors.APIStatus) bool
		// 在加密已失效但尚未删除时，
		// 对损坏对象执行 GET 的预期结果
		corruptObjGetPreDelete verifier
		// 在加密已失效、但未设置忽略存储读取错误选项时，
		// 对损坏对象执行 DELETE 的预期结果
		corrupObjDeletWithoutOption verifier
		// 在加密已失效、且启用了忽略存储读取错误选项时，
		// 对损坏对象执行 DELETE 的预期结果
		corrupObjDeleteWithOption verifier
		// 在加密已失效、启用了忽略存储读取错误选项，且
		// 用户拥有不安全删除权限时，
		// 对损坏对象执行 DELETE 的预期结果
		corrupObjDeleteWithOptionAndPrivilege verifier
		// 删除之后对损坏对象执行 GET 的预期结果
		corrupObjGetPostDelete verifier
		// 删除之后通过 informer 缓存
		// 对损坏对象执行 GET 的预期结果
		corrupObjGetFromListerCachePostDelete verifier
	}{
		{
			featureEnabled: true,
			encryptionBrokenFn: func(t *testing.T, got apierrors.APIStatus) bool {
				return got.Status().Reason == metav1.StatusReasonInternalError &&
					strings.Contains(got.Status().Message, "Internal error occurred: StorageError: corrupt object") &&
					strings.Contains(got.Status().Message, "data from the storage is not transformable revision=0: no matching prefix found")
			},
			corruptObjGetPreDelete:      wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corrupObjDeletWithoutOption: wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corrupObjDeleteWithOption: wantAPIStatusError{
				reason:          metav1.StatusReasonForbidden,
				messageContains: `not permitted to do "unsafe-delete-ignore-read-errors"`,
			},
			corrupObjDeleteWithOptionAndPrivilege: wantNoError{},
			corrupObjGetPostDelete:                wantAPIStatusError{reason: metav1.StatusReasonNotFound},
			// 不安全删除之后，预期已删除的对象会从缓存中移除
			corrupObjGetFromListerCachePostDelete: wantAPIStatusError{reason: metav1.StatusReasonNotFound},
		},
		{
			featureEnabled: false,
			encryptionBrokenFn: func(t *testing.T, got apierrors.APIStatus) bool {
				return got.Status().Reason == metav1.StatusReasonInternalError &&
					strings.Contains(got.Status().Message, "Internal error occurred: no matching prefix found")
			},
			corruptObjGetPreDelete:                wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corrupObjDeletWithoutOption:           wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corrupObjDeleteWithOption:             wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corrupObjDeleteWithOptionAndPrivilege: wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corrupObjGetPostDelete:                wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			// 尽管对象已损坏且无法从底层存储读取，
			// 缓存仍会继续提供该对象
			corrupObjGetFromListerCachePostDelete: wantNoError{},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s/%t", string(genericfeatures.AllowUnsafeMalformedObjectDeletion), tc.featureEnabled), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.AllowUnsafeMalformedObjectDeletion, tc.featureEnabled)

			test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: aesGCMConfigYAML, reload: true})
			if err != nil {
				t.Fatalf("failed to setup test for envelop %s, error was %v", aesGCMPrefix, err)
			}
			defer test.cleanUp()

			// a) 为测试用户创建一个权限最小的独立客户端，
			// 随着测试的推进逐步授予权限
			testUser := "croc"
			testUserConfig := restclient.CopyConfig(test.kubeAPIServer.ClientConfig)
			testUserConfig.Impersonate.UserName = testUser
			testUserClient := clientset.NewForConfigOrDie(testUserConfig)
			adminClient := test.restClient

			// b) 使用 admin 客户端为测试用户授予初始权限，
			// 暂时不授予 'delete-ignore-read-errors'
			permitUserToDoVerbOnSecret(t, adminClient, testUser, testNamespace, []string{"create", "get", "delete", "update"})

			// 后续测试不应再使用 admin 客户端
			test.restClient = testUserClient
			defer func() {
				// 用于任何需要 admin 权限的清理操作
				test.restClient = adminClient
			}()

			secretCorrupt := "foo-with-unsafe-delete"
			// c) 创建并删除 Secret，预期不会出现错误
			_, err = test.createSecret(secretCorrupt, testNamespace)
			if err != nil {
				t.Fatalf("'%s/%s' failed to create, got error: %v", err, testNamespace, secretCorrupt)
			}
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("'%s/%s' failed to delete, got error: %v", err, testNamespace, secretCorrupt)
			}

			// d) 重新创建 Secret
			test.secret, err = test.createSecret(secretCorrupt, testNamespace)
			if err != nil {
				t.Fatalf("Failed to create test secret, error: %v", err)
			}

			// e) 为 Secret 添加 finalizer
			withFinalizer := test.secret.DeepCopy()
			withFinalizer.Finalizers = append(withFinalizer.Finalizers, "tes.k8s.io/fake")
			test.secret, err = test.restClient.CoreV1().Secrets(testNamespace).Update(context.Background(), withFinalizer, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to add finalizer to the secret, error: %v", err)
			}

			test.runResource(test.TContext, unSealWithGCMTransformer, aesGCMPrefix, "", "v1", "secrets", test.secret.Name, test.secret.Namespace)

			factory := informers.NewSharedInformerFactoryWithOptions(adminClient, time.Minute, informers.WithNamespace(testNamespace))
			informer := factory.Core().V1().Secrets()

			// 测试会在不安全删除之后添加一个名为 "final-secret" 的 Secret，
			// 以便用它作为 informer 已追进度的标记
			finalEvent := make(chan struct{})
			finalSecretName := "final-secret"
			_, err = informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					if obj, err := meta.Accessor(obj); err == nil && obj.GetName() == finalSecretName {
						close(finalEvent)
					}
				},
			})
			if err != nil {
				t.Fatalf("unexpected error from AddEventHandler: %v", err)
			}

			lister := informer.Lister()
			informerCtx, informerCancel := context.WithCancel(test)
			defer informerCancel()
			factory.Start(informerCtx.Done())
			waitForSyncCtx, waitForSyncCancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer waitForSyncCancel()
			if !cache.WaitForCacheSync(waitForSyncCtx.Done(), informer.Informer().HasSynced) {
				t.Fatalf("caches failed to sync")
			}

			// g) Secret 应能从缓存中读取
			if _, err := lister.Secrets(testNamespace).Get(secretCorrupt); err != nil {
				t.Errorf("unexpected failure getting the secret from lister cache: %v", err)
			}

			// h) 覆盖配置并破坏对旧资源的解密，
			// 步骤 b 中创建的 Secret 将无法解密
			now := time.Now()
			encryptionConf := filepath.Join(test.configDir, encryptionConfigFileName)
			body, _ := os.ReadFile(encryptionConf)
			t.Logf("file before write: %s", body)
			// 用来自其他 provider 的新密钥替换现有密钥
			if err := os.WriteFile(encryptionConf, []byte(aesCBCConfigYAML), 0o644); err != nil {
				t.Fatalf("failed to write encryption config that's going to make decryption fail")
			}
			body, _ = os.ReadFile(encryptionConf)
			t.Logf("file after write: %s", body)

			// i) 等待破坏性变更生效
			testCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// 动态重新加载加密配置大约需要 1 分钟，因此不能使用 wait.ForeverTestTimeout
			err = wait.PollUntilContextTimeout(testCtx, 1*time.Second, 2*time.Minute, true, func(ctx context.Context) (done bool, err error) {
				_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(ctx, secretCorrupt, metav1.GetOptions{})
				var got apierrors.APIStatus
				if !errors.As(err, &got) {
					return false, nil
				}
				if done := tc.encryptionBrokenFn(t, got); done {
					return true, nil
				}
				return false, nil
			})
			if err != nil {
				t.Fatalf("encryption never broke: %v", err)
			}
			t.Logf("it took %s for the apiserver to reload the encryption config", time.Since(now))

			// j) 创建一个新 Secret，然后删除它，操作应成功
			secretNormal := "bar-with-normal-delete"
			_, err = test.createSecret(secretNormal, testNamespace)
			if err != nil {
				t.Fatalf("'%s/%s' failed to create, got error: %v", err, testNamespace, secretNormal)
			}
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretNormal, metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("'%s/%s' failed to create, got error: %v", err, testNamespace, secretNormal)
			}

			// k) 尝试获取步骤 b 中创建的已损坏 Secret，预期会
			// 失败；具体错误取决于特性是否启用。
			// 但缓存仍会提供该对象
			_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(context.Background(), secretCorrupt, metav1.GetOptions{})
			tc.corruptObjGetPreDelete.verify(t, err)
			if _, err := lister.Secrets(testNamespace).Get(secretCorrupt); err != nil {
				t.Errorf("unexpected failure when etting the secret from the cache: %v", err)
			}

			withDryRun := func(opts metav1.DeleteOptions) metav1.DeleteOptions {
				opts.DryRun = []string{metav1.DryRunAll}

				return opts
			}

			// l) 尝试带/不带 dry-run 的正常删除流程，预期都会返回错误
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, withDryRun(metav1.DeleteOptions{}))
			tc.corrupObjDeletWithoutOption.verify(t, err)
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, metav1.DeleteOptions{})
			tc.corrupObjDeletWithoutOption.verify(t, err)

			// m) 尝试通过启用该选项来删除损坏对象；
			// 另一方面，我们尚未向用户授予 'delete-ignore-read-errors'
			// 动词，因此预期 admission 会拒绝删除请求
			options := metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true),
			}
			dryRunOptions := withDryRun(options)
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, dryRunOptions)
			tc.corrupObjDeleteWithOption.verify(t, err)
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, options)
			tc.corrupObjDeleteWithOption.verify(t, err)

			// n) 授予测试用户对 Secrets 执行 'unsafe-delete-ignore-read-errors' 的权限
			permitUserToDoVerbOnSecret(t, adminClient, testUser, testNamespace, []string{"unsafe-delete-ignore-read-errors"})

			// o) 再次尝试不安全删除
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, dryRunOptions)
			tc.corrupObjDeleteWithOptionAndPrivilege.verify(t, err)
			// dry-run 不得删除对象，GET 应仍然失败，
			// 且错误与任何删除尝试之前相同
			_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(context.Background(), secretCorrupt, metav1.GetOptions{})
			tc.corruptObjGetPreDelete.verify(t, err)
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, options)
			tc.corrupObjDeleteWithOptionAndPrivilege.verify(t, err)

			// p) Secret 删除之后，最终 GET 应返回 NotFound 错误
			_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(context.Background(), secretCorrupt, metav1.GetOptions{})
			tc.corrupObjGetPostDelete.verify(t, err)

			// q) 创建最后一个 Secret，并等待 informer
			// 追上 ADDED 事件。
			_, err = test.createSecret(finalSecretName, testNamespace)
			if err != nil {
				t.Fatalf("'%s/%s' failed to create, got error: %v", err, testNamespace, finalSecretName)
			}
			select {
			case <-finalEvent:
			case <-time.After(wait.ForeverTestTimeout):
				t.Fatalf("timed out waiting for the informer to catch up")
			}

			// r) 从缓存中读取对象
			_, err = lister.Secrets(testNamespace).Get(secretCorrupt)
			tc.corrupObjGetFromListerCachePostDelete.verify(t, err)
		})
	}
}

type wantNoError struct{}

func (want wantNoError) verify(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

type wantAPIStatusError struct {
	reason          metav1.StatusReason
	messageContains string
	more            func(*testing.T, apierrors.APIStatus)
}

func (wantError wantAPIStatusError) verify(t *testing.T, err error) {
	t.Helper()

	switch {
	case err != nil:
		var statusGot apierrors.APIStatus
		if !errors.As(err, &statusGot) {
			t.Errorf("expected an API status error, but got: %v", err)
			return
		}
		if want, got := wantError.reason, statusGot.Status().Reason; want != got {
			t.Errorf("expected API status Reason: %q, but got: %q, err: %#v", want, got, statusGot)
		}
		if want, got := wantError.messageContains, statusGot.Status().Message; !strings.Contains(got, want) {
			t.Errorf("expected API status message to contain: %q, got err: %#v", want, statusGot)
		}
		if wantError.more != nil {
			wantError.more(t, statusGot)
		}
	default:
		t.Errorf("expected error: %v, but got none", err)
	}
}

// TestListCorruptObjects 是一个集成测试，用于验证：
// 1) 如果启用了特性 AllowUnsafeMalformedObjectDeletion，LIST 操作
// 在其错误响应中应包含可识别那些
// 未能从存储读取的对象的信息
// 2) 如果禁用了特性 AllowUnsafeMalformedObjectDeletion，LIST 应在
// 遇到第一个错误时立即中止，以保持向后兼容
func TestListCorruptObjects(t *testing.T) {
	// 这些是测试初始创建的 Secret，
	// 预期在加密失效后会变得不可读/损坏
	secrets := []string{"corrupt-a", "corrupt-b", "corrupt-c"}

	tests := []struct {
		featureEnabled bool
		// 在加密失效之前创建的 Secret
		secrets []string
		// 配置变更之后加密是否已失效
		encryptionBrokenFn func(t *testing.T, got apierrors.APIStatus)
		// 加密失效后对损坏对象执行 LIST 的预期结果
		listAfter verifier
	}{
		{
			secrets:        secrets,
			featureEnabled: true,
			encryptionBrokenFn: func(t *testing.T, got apierrors.APIStatus) {
				status := got.Status()
				if status.Reason != metav1.StatusReasonInternalError {
					t.Errorf("Invalid reason, got: %q, want: %q", status.Reason, metav1.StatusReasonInternalError)
				}
				corruptObjectMsg := "Internal error occurred: StorageError: corrupt object"
				if !strings.Contains(status.Message, corruptObjectMsg) {
					t.Errorf("Message should include %q, but got: %q", corruptObjectMsg, status.Message)
				}
				messageAuthenticationFailedMsg := "data from the storage is not transformable revision=0: cipher: message authentication failed"
				if !strings.Contains(status.Message, messageAuthenticationFailedMsg) {
					t.Errorf("Message should include %q, but got: %q", messageAuthenticationFailedMsg, status.Message)
				}
			},
			listAfter: wantAPIStatusError{
				reason:          metav1.StatusReasonStoreReadError,
				messageContains: "failed to read one or more secrets from the storage",
				more: func(t *testing.T, err apierrors.APIStatus) {
					t.Helper()

					details := err.Status().Details
					if details == nil {
						t.Errorf("expected Details in APIStatus, but got: %#v", err)
						return
					}
					if want, got := len(secrets), len(details.Causes); want != got {
						t.Errorf("expected to have %d in APIStatus, but got: %d", want, got)
					}
					for _, cause := range details.Causes {
						if want, got := metav1.CauseTypeUnexpectedServerResponse, cause.Type; want != got {
							t.Errorf("expected to cause type to be %s, but got: %s", want, got)
						}
					}
					for _, want := range secrets {
						var found bool
						for _, got := range details.Causes {
							if strings.HasSuffix(got.Field, want) {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("want key: %q in the Fields: %#v", want, details.Causes)
						}
					}
				},
			},
		},
		{
			secrets:        secrets,
			featureEnabled: false,
			encryptionBrokenFn: func(t *testing.T, got apierrors.APIStatus) {
				status := got.Status()
				if status.Reason != metav1.StatusReasonInternalError {
					t.Errorf("Invalid reason, got: %q, want: %q", status.Reason, metav1.StatusReasonInternalError)
				}
				noMatchingPrefixFoundMsg := "Internal error occurred: cipher: message authentication failed"
				if !strings.Contains(status.Message, noMatchingPrefixFoundMsg) {
					t.Errorf("Message should include %q, but got: %q", noMatchingPrefixFoundMsg, status.Message)
				}
			},
			listAfter: wantAPIStatusError{
				reason:          metav1.StatusReasonInternalError,
				messageContains: "unable to transform key",
			},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s/%t", string(genericfeatures.AllowUnsafeMalformedObjectDeletion), tc.featureEnabled), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.AllowUnsafeMalformedObjectDeletion, tc.featureEnabled)
			storageConfig := framework.SharedEtcd()
			test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: aesGCMConfigYAML, reload: true, storageConfig: storageConfig})
			if err != nil {
				t.Fatalf("failed to setup test for envelop %s, error was %v", aesGCMPrefix, err)
			}
			defer test.cleanUp()
			ctx := context.Background()

			// a) 在测试命名空间中创建多个 Secret
			for _, name := range tc.secrets {
				_, err = test.createSecret(name, testNamespace)
				if err != nil {
					t.Fatalf("Failed to create test secret, error: %v", err)
				}
			}

			// b) 在破坏加密之前列出所有 Secret
			result, err := test.restClient.CoreV1().Secrets(testNamespace).List(context.Background(), metav1.ListOptions{})
			if err != nil {
				t.Fatalf("listing secrets failed unexpectedly with: %v", err)
			}
			if want, got := len(tc.secrets), len(result.Items); got < 3 {
				t.Fatalf("expected at least %d secrets, but got: %d", want, got)
			}

			// c) 覆盖配置并破坏对旧资源的解密，
			// 步骤 a 中创建的 Secret 将无法解密
			client, err := clientv3.New(clientv3.Config{Endpoints: storageConfig.Transport.ServerList})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := client.Close()
				if err != nil {
					t.Fatal(err)
				}
			}()
			resp, err := client.Get(ctx, "/"+storageConfig.Prefix+"/secrets/", clientv3.WithPrefix())
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Kvs) != len(tc.secrets) {
				t.Fatalf("Expected %d number of keys, got: %d", len(tc.secrets), len(resp.Kvs))
			}
			for _, kv := range resp.Kvs {
				// 移除最后一个字节
				_, err = client.Put(ctx, string(kv.Key), string(kv.Value)[:len(kv.Value)-1])
				if err != nil {
					t.Fatal(err)
				}
			}

			_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(ctx, tc.secrets[0], metav1.GetOptions{})
			if err != nil {
				t.Logf("get returned error: %#v message: %s", err, err.Error())
			}

			var got apierrors.APIStatus
			if !errors.As(err, &got) {
				t.Fatalf("encryption never broke: %v", err)
			}
			tc.encryptionBrokenFn(t, got)

			// e) LIST 应返回预期错误
			_, err = test.restClient.CoreV1().Secrets(testNamespace).List(ctx, metav1.ListOptions{})
			tc.listAfter.verify(t, err)
		})
	}
}

func permitUserToDoVerbOnSecret(t *testing.T, client *clientset.Clientset, user, namespace string, verbs []string) {
	t.Helper()
	grantUserVerbsOnResource(t, client, user, namespace, verbs, "", "secrets")
}

// grantUserVerbsOnResource 创建一个 RBAC Role 和 RoleBinding，
// 授予指定用户在给定命名空间中对某资源执行指定动词的权限。
func grantUserVerbsOnResource(t *testing.T, client *clientset.Clientset, user, namespace string, verbs []string, apiGroup, resource string) {
	t.Helper()

	name := fmt.Sprintf("%s-can-do-%s-on-%s-in-%s", user, strings.Join(verbs, "-"), resource, namespace)
	_, err := client.RbacV1().Roles(namespace).Create(context.TODO(), &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:     verbs,
				APIGroups: []string{apiGroup},
				Resources: []string{resource},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("error while creating role: %s, err: %v", name, err)
	}

	_, err = client.RbacV1().RoleBindings(namespace).Create(context.TODO(), &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind: rbacv1.UserKind,
				Name: user,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("error while creating role binding: %s, err: %v", name, err)
	}

	authutil.WaitForNamedAuthorizationUpdate(t, context.TODO(), client.AuthorizationV1(),
		user, namespace, verbs[0], "", schema.GroupResource{Group: apiGroup, Resource: resource}, true)
}

// 基线（无封套加密）——用于与封套加密的基准测试结果对比。
func BenchmarkBase(b *testing.B) {
	runBenchmark(b, "")
}

// Identity transformer 是一个空操作（加密层面）——用于与 AESGCM 和 AESCBC 的基准测试结果对比。
func BenchmarkIdentityWrite(b *testing.B) {
	runBenchmark(b, identityConfigYAML)
}

func BenchmarkAESGCMEnvelopeWrite(b *testing.B) {
	runBenchmark(b, aesGCMConfigYAML)
}

func BenchmarkAESCBCEnvelopeWrite(b *testing.B) {
	runBenchmark(b, aesCBCConfigYAML)
}

func runBenchmark(b *testing.B, transformerConfig string) {
	b.StopTimer()
	test, err := newTransformTest(b, transformTestConfig{transformerConfigYAML: transformerConfig})
	if err != nil {
		b.Fatalf("failed to setup benchmark for config %s, error was %v", transformerConfig, err)
	}
	defer test.cleanUp()

	b.StartTimer()
	test.benchmark(b)
	b.StopTimer()
	test.printMetrics()
}

func unSealWithGCMTransformer(ctx context.Context, cipherText []byte, dataCtx value.Context,
	transformerConfig apiserverv1.ProviderConfiguration,
) ([]byte, error) {
	block, err := newAESCipher(transformerConfig.AESGCM.Keys[0].Secret)
	if err != nil {
		return nil, fmt.Errorf("failed to create block cipher: %v", err)
	}

	gcmTransformer, err := aestransformer.NewGCMTransformer(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create transformer from block: %v", err)
	}

	clearText, _, err := gcmTransformer.TransformFromStorage(ctx, cipherText, dataCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to decypt secret: %v", err)
	}

	return clearText, nil
}

func unSealWithCBCTransformer(ctx context.Context, cipherText []byte, dataCtx value.Context,
	transformerConfig apiserverv1.ProviderConfiguration,
) ([]byte, error) {
	block, err := newAESCipher(transformerConfig.AESCBC.Keys[0].Secret)
	if err != nil {
		return nil, err
	}

	cbcTransformer := aestransformer.NewCBCTransformer(block)

	clearText, _, err := cbcTransformer.TransformFromStorage(ctx, cipherText, dataCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to decypt secret: %v", err)
	}

	return clearText, nil
}

func newAESCipher(key string) (cipher.Block, error) {
	k, err := base64.StdEncoding.DecodeString(key)
	if err != nil {
		return nil, fmt.Errorf("failed to decode config secret: %v", err)
	}

	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %v", err)
	}

	return block, nil
}
