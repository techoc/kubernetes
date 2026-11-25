/*
Copyright The Kubernetes Authors.

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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
)

// TestBitFlipCorruptObjectDeletion 使用 Secrets（protobuf 编码）来验证
// KEP-3926 的解码器错误路径。与测试 transformer 错误的
// TestAllowUnsafeMalformedObjectDeletionFeature（错误的加密配置 → "untransformable"）不同，
// 本测试使用 identity（空操作）加密 provider，并直接在 etcd 中破坏存储的字节。
// 翻转 protobuf 编码 Secrets 的最后一个字节可以可靠地破坏解码，
// 从而产生 "undecodable" 错误，而不是 "untransformable" 错误。
//
// 删除后给 informer 一个更长的超时时间（2 分钟），以便从破坏窗口期间
// 累积的指数退避中恢复。reflector 的退避上限为带抖动的 [30s, 60s)
// （参见 client-go/tools/cache/reflector.go），因此 2 分钟提供了足够的余量。
func TestBitFlipCorruptObjectDeletion(t *testing.T) {
	tests := []struct {
		featureEnabled                         bool
		encryptionBrokenFn                     func(t *testing.T, got apierrors.APIStatus) bool
		corruptObjGetPreDelete                 verifier
		corruptObjDeleteWithoutOption          verifier
		corruptObjDeleteWithOption             verifier
		corruptObjDeleteWithOptionAndPrivilege verifier
		corruptObjGetPostDelete                verifier
		corruptObjGetFromListerCachePostDelete verifier
	}{
		{
			featureEnabled: true,
			encryptionBrokenFn: func(t *testing.T, got apierrors.APIStatus) bool {
				return got.Status().Reason == metav1.StatusReasonInternalError &&
					strings.Contains(got.Status().Message, "StorageError: corrupt object") &&
					strings.Contains(got.Status().Message, "object not decodable")
			},
			corruptObjGetPreDelete:        wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjDeleteWithoutOption: wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjDeleteWithOption: wantAPIStatusError{
				reason:          metav1.StatusReasonForbidden,
				messageContains: `unsafe-delete-ignore-read-errors`,
			},
			corruptObjDeleteWithOptionAndPrivilege: wantNoError{},
			corruptObjGetPostDelete:                wantAPIStatusError{reason: metav1.StatusReasonNotFound},
			corruptObjGetFromListerCachePostDelete: wantAPIStatusError{reason: metav1.StatusReasonNotFound},
		},
		{
			featureEnabled: false,
			encryptionBrokenFn: func(t *testing.T, got apierrors.APIStatus) bool {
				return got.Status().Reason == metav1.StatusReasonInternalError &&
					!strings.Contains(got.Status().Message, "corrupt object")
			},
			corruptObjGetPreDelete:                 wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjDeleteWithoutOption:          wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjDeleteWithOption:             wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjDeleteWithOptionAndPrivilege: wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjGetPostDelete:                wantAPIStatusError{reason: metav1.StatusReasonInternalError},
			corruptObjGetFromListerCachePostDelete: wantNoError{},
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("%s/%t", string(genericfeatures.AllowUnsafeMalformedObjectDeletion), tc.featureEnabled), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.AllowUnsafeMalformedObjectDeletion, tc.featureEnabled)

			if !tc.featureEnabled {
				// 当 gate=false 时，protobuf 解码错误（"unexpected EOF"）
				// 会破坏 server 端 Secrets 的 cacher，因此 GET 请求会挂起，
				// 而不是返回正常的 apierrors.APIStatus。
				// 这是一个已知问题，将单独处理。
				t.Skip("bit-flip decoder corruption with gate=false is not yet supported for Secrets")
			}

			// identity provider：空操作 transformer，无需重新加载配置。
			test, err := newTransformTest(t, transformTestConfig{transformerConfigYAML: identityConfigYAML, reload: false})
			if err != nil {
				t.Fatalf("failed to setup test, error was %v", err)
			}
			defer test.cleanUp()

			// a) 为测试用户设置一个权限最小的独立 client
			testUser := "croc"
			testUserConfig := restclient.CopyConfig(test.kubeAPIServer.ClientConfig)
			testUserConfig.Impersonate.UserName = testUser
			testUserClient := clientset.NewForConfigOrDie(testUserConfig)
			adminClient := test.restClient

			// b) 授予测试用户初始权限（尚不包括 unsafe-delete）
			permitUserToDoVerbOnSecret(t, adminClient, testUser, testNamespace, []string{"create", "get", "delete", "update"})

			// 此后测试不应再使用 admin client
			test.restClient = testUserClient
			defer func() {
				test.restClient = adminClient
			}()

			secretCorrupt := "foo-with-unsafe-delete"
			// c) 创建并删除 secret —— 预期不会出现错误
			_, err = test.createSecret(secretCorrupt, testNamespace)
			if err != nil {
				t.Fatalf("'%s/%s' failed to create, got error: %v", testNamespace, secretCorrupt, err)
			}
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("'%s/%s' failed to delete, got error: %v", testNamespace, secretCorrupt, err)
			}

			// d) 重新创建带有 finalizer 的 secret
			test.secret, err = test.createSecret(secretCorrupt, testNamespace)
			if err != nil {
				t.Fatalf("Failed to create test secret, error: %v", err)
			}
			withFinalizer := test.secret.DeepCopy()
			withFinalizer.Finalizers = append(withFinalizer.Finalizers, "test.k8s.io/fake")
			test.secret, err = test.restClient.CoreV1().Secrets(testNamespace).Update(context.Background(), withFinalizer, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("Failed to add finalizer to the secret, error: %v", err)
			}

			// e) 设置一个 informer 来跟踪缓存中的 secret
			factory := informers.NewSharedInformerFactoryWithOptions(adminClient, time.Minute, informers.WithNamespace(testNamespace))
			secretInformer := factory.Core().V1().Secrets()
			lister := secretInformer.Lister()

			// 测试会在 unsafe delete 之后添加一个名为 "final-secret" 的 secret，
			// 以便将其用作 informer 已追赶上进度的标记
			finalEvent := make(chan struct{})
			finalSecretName := "final-secret"
			_, err = secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					if obj, err := meta.Accessor(obj); err == nil && obj.GetName() == finalSecretName {
						close(finalEvent)
					}
				},
			})
			if err != nil {
				t.Fatalf("unexpected error from AddEventHandler: %v", err)
			}

			informerCtx, informerCancel := context.WithCancel(test)
			defer informerCancel()
			factory.Start(informerCtx.Done())
			waitForSyncCtx, waitForSyncCancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer waitForSyncCancel()
			if !cache.WaitForCacheSync(waitForSyncCtx.Done(), secretInformer.Informer().HasSynced) {
				t.Fatalf("informer failed to sync")
			}

			// f) 应当能从 lister 中读取该 secret
			if _, err := lister.Secrets(testNamespace).Get(secretCorrupt); err != nil {
				t.Errorf("unexpected failure getting the secret from lister cache: %v", err)
			}

			// g) 通过翻转 etcd 中的最后一个字节来破坏 secret。
			//    使用 identity provider 时，存储的值是原始 protobuf ——
			//    翻转一个字节会使其无法解码。
			etcdPath := test.getETCDPathForResource(test.storageConfig.Prefix, "", "secrets", secretCorrupt, testNamespace)
			resp, err := test.readRawRecordFromETCD(etcdPath)
			if err != nil {
				t.Fatalf("failed to read from etcd: %v", err)
			}
			if len(resp.Kvs) != 1 {
				t.Fatalf("expected 1 key in etcd, got %d", len(resp.Kvs))
			}
			value := make([]byte, len(resp.Kvs[0].Value))
			copy(value, resp.Kvs[0].Value)
			value[len(value)-1] ^= 0xFF
			if _, err := test.writeRawRecordToETCD(etcdPath, value); err != nil {
				t.Fatalf("failed to write corrupted value to etcd: %v", err)
			}

			// h) 轮询直到 GET 因预期的解码错误而失败 —— 破坏是立即生效的
			//    （无需重新加载配置），但为了稳健起见仍进行轮询。
			err = wait.PollUntilContextTimeout(t.Context(), 1*time.Second, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
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
				t.Fatalf("bit-flip corruption never took effect: %v", err)
			}

			// i) 创建一个新 secret，然后将其删除 —— 应当正常工作
			secretNormal := "bar-with-normal-delete"
			_, err = test.createSecret(secretNormal, testNamespace)
			if err != nil {
				t.Fatalf("'%s/%s' failed to create, got error: %v", testNamespace, secretNormal, err)
			}
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretNormal, metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("'%s/%s' failed to delete, got error: %v", testNamespace, secretNormal, err)
			}

			// j) GET 被破坏的 secret —— 预期失败。
			//    同时检查 lister 中是否仍有破坏前的缓存副本。
			_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(context.Background(), secretCorrupt, metav1.GetOptions{})
			tc.corruptObjGetPreDelete.verify(t, err)
			if _, err := lister.Secrets(testNamespace).Get(secretCorrupt); err != nil {
				t.Errorf("unexpected failure getting the secret from the lister cache: %v", err)
			}

			// k) 普通删除 —— 预期报错
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, metav1.DeleteOptions{})
			tc.corruptObjDeleteWithoutOption.verify(t, err)

			// l) 使用 unsafe 选项但没有权限进行删除 —— 预期 forbidden 或内部错误
			options := metav1.DeleteOptions{
				IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To(true),
			}
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, options)
			tc.corruptObjDeleteWithOption.verify(t, err)

			// m) 授予测试用户 unsafe-delete-ignore-read-errors 动词权限
			permitUserToDoVerbOnSecret(t, adminClient, testUser, testNamespace, []string{"unsafe-delete-ignore-read-errors"})

			// n) 再次尝试 unsafe delete —— 当特性启用时应成功
			err = test.restClient.CoreV1().Secrets(testNamespace).Delete(context.Background(), secretCorrupt, options)
			tc.corruptObjDeleteWithOptionAndPrivilege.verify(t, err)

			// o) 最后一次 GET
			_, err = test.restClient.CoreV1().Secrets(testNamespace).Get(context.Background(), secretCorrupt, metav1.GetOptions{})
			tc.corruptObjGetPostDelete.verify(t, err)

			// p) 通过 informer 缓存验证 —— informer 在破坏窗口期间会累积
			//    指数退避（重试间隔最高可达 [30s, 60s)），因此我们允许
			//    最多 2 分钟用于恢复。
			if tc.featureEnabled {
				// 创建 final secret，并等待 informer 追赶上来
				_, err = test.createSecret(finalSecretName, testNamespace)
				if err != nil {
					t.Fatalf("'%s/%s' failed to create, got error: %v", testNamespace, finalSecretName, err)
				}
				select {
				case <-finalEvent:
				case <-time.After(2 * time.Minute):
					t.Fatalf("timed out waiting for the informer to catch up")
				}

				// 验证被破坏的 secret 不在 lister 中
				_, err = lister.Secrets(testNamespace).Get(secretCorrupt)
				tc.corruptObjGetFromListerCachePostDelete.verify(t, err)
			} else {
				// gate=false：删除失败，因此 informer 中破坏前的旧缓存副本
				// 仍然存在。
				_, err = lister.Secrets(testNamespace).Get(secretCorrupt)
				tc.corruptObjGetFromListerCachePostDelete.verify(t, err)
			}
		})
	}
}
