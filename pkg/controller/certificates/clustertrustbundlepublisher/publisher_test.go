/*
Copyright 2024 The Kubernetes Authors.

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

package clustertrustbundlepublisher

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"testing"

	certificatesv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const testSignerName = "test.test/testSigner"

// TestCTBPublisherSync 以表驱动方式验证 ClusterTrustBundle 发布控制器的同步行为。
//
// 控制器需要保证：给定签名者恰好有一个名称确定的 CTB，内容等于当前 CA；
// 各用例分别构造“不存在 / 内容陈旧 / 存在多个 / 状态已正确”等初始状态，
// 断言同步后产生的 API 动作序列，覆盖创建、更新、删除和空操作四类结果。
func TestCTBPublisherSync(t *testing.T) {
	// checkCreatedTestSignerBundle 断言客户端记录里只有一次针对测试签名者的 CTB 创建动作。
	checkCreatedTestSignerBundle := func(t *testing.T, actions []clienttesting.Action) {
		filteredActions := filterOutListWatch(actions)
		if len(filteredActions) != 1 {
			t.Fatalf("expected 1 action, got %v", filteredActions)
		}

		createAction := expectAction[clienttesting.CreateAction](t, filteredActions[0], "create")

		ctb, ok := createAction.GetObject().(*certificatesv1.ClusterTrustBundle)
		if !ok {
			t.Fatalf("expected ClusterTrustBundle create, got %v", createAction.GetObject())
		}

		if ctb.Spec.SignerName != testSignerName {
			t.Fatalf("expected signer name %q, got %q", testSignerName, ctb.Spec.SignerName)
		}
	}

	// checkUpdatedTestSignerBundle 返回一个断言函数，检查唯一动作是把 CTB 更新为给定的 CA 内容。
	// 闭包捕获期望的 CA 字节，便于在不同用例间复用同一套检查逻辑。
	checkUpdatedTestSignerBundle := func(expectedCABytes []byte) func(t *testing.T, actions []clienttesting.Action) {
		return func(t *testing.T, actions []clienttesting.Action) {
			filteredActions := filterOutListWatch(actions)
			if len(filteredActions) != 1 {
				t.Fatalf("expected 1 action, got %v", filteredActions)
			}

			updateAction := expectAction[clienttesting.UpdateAction](t, filteredActions[0], "update")

			ctb, ok := updateAction.GetObject().(*certificatesv1.ClusterTrustBundle)
			if !ok {
				t.Fatalf("expected ClusterTrustBundle update, got %v", updateAction.GetObject())
			}

			if ctb.Spec.SignerName != testSignerName {
				t.Fatalf("expected signer name %q, got %q", testSignerName, ctb.Spec.SignerName)
			}

			if ctb.Spec.TrustBundle != string(expectedCABytes) {
				t.Fatalf("expected trust bundle payload:\n%s\n, got %q", expectedCABytes, ctb.Spec.TrustBundle)
			}
		}
	}

	// checkDeletedTestSignerBundle 返回一个断言函数，检查唯一动作是删除指定名称的 CTB。
	// 多个同名签名者 CTB 时，控制器会保留规范名称的那个并删除多余的副本。
	checkDeletedTestSignerBundle := func(name string) func(t *testing.T, actions []clienttesting.Action) {
		return func(t *testing.T, actions []clienttesting.Action) {
			filteredActions := filterOutListWatch(actions)
			if len(filteredActions) != 1 {
				t.Fatalf("expected 1 action, got %v", filteredActions)
			}

			deleteAction := expectAction[clienttesting.DeleteAction](t, filteredActions[0], "delete")

			if actionName := deleteAction.GetName(); actionName != name {
				t.Fatalf("expected deleted CTB name %q, got %q", name, actionName)
			}
		}
	}

	testCAProvider := testingCABundlleProvider(t)
	testBundleName := constructBundleName(testSignerName, testCAProvider.CurrentCABundleContent())

	for _, tt := range []struct {
		name          string
		existingCTBs  []runtime.Object
		expectActions func(t *testing.T, actions []clienttesting.Action)
		wantErr       bool
	}{
		// 集群里没有任何 CTB：应创建规范命名的那一个。
		{
			name:          "no CTBs exist",
			expectActions: checkCreatedTestSignerBundle,
		},
		// 存在其他签名者的 CTB：它们与当前签名者无关，不应干扰创建动作。
		{
			name: "no CTBs for the current signer exist",
			existingCTBs: []runtime.Object{
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nosigner",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						TrustBundle: "somedatahere",
					},
				},
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "signer:one",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  "signer",
						TrustBundle: "signerdata",
					},
				},
			},
			expectActions: checkCreatedTestSignerBundle,
		},
		// 规范命名的 CTB 内容陈旧：应更新为当前 CA。
		{
			name: "CTB for the signer exists with different content",
			existingCTBs: []runtime.Object{
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: testBundleName,
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: "olddata",
					},
				},
			},
			expectActions: checkUpdatedTestSignerBundle(testCAProvider.CurrentCABundleContent()),
		},
		// 同名签名者有多个 CTB 且内容都正确：只保留规范名称，删除多余副本。
		{
			name: "multiple CTBs for the signer",
			existingCTBs: []runtime.Object{
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: testBundleName,
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: string(testCAProvider.CurrentCABundleContent()),
					},
				},
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test.test/testSigner:name2",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: string(testCAProvider.CurrentCABundleContent()),
					},
				},
			},
			expectActions: checkDeletedTestSignerBundle("test.test/testSigner:name2"),
		},
		// 多个 CTB 中规范名称那个内容陈旧：先更新它，再删除多余副本。
		{
			name: "multiple CTBs for the signer - the one with the proper name needs changing",
			existingCTBs: []runtime.Object{
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: testBundleName,
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: "olddata",
					},
				},
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test.test/testSigner:name2",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: string(testCAProvider.CurrentCABundleContent()),
					},
				},
			},
			expectActions: func(t *testing.T, actions []clienttesting.Action) {
				filteredActions := filterOutListWatch(actions)
				if len(filteredActions) != 2 {
					t.Fatalf("expected 2 actions, got %v", filteredActions)
				}
				checkUpdatedTestSignerBundle(testCAProvider.CurrentCABundleContent())(t, filteredActions[:1])
				checkDeletedTestSignerBundle("test.test/testSigner:name2")(t, filteredActions[1:])
			},
		},
		// 当前签名者只有一个非规范名称的 CTB：应新建规范名称的，并删掉旧的那个。
		{
			name: "another CTB with a different name exists for the signer",
			existingCTBs: []runtime.Object{
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test.test/testSigner:preexisting",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: string(testCAProvider.CurrentCABundleContent()),
					},
				},
			},
			expectActions: func(t *testing.T, actions []clienttesting.Action) {
				filteredActions := filterOutListWatch(actions)
				if len(filteredActions) != 2 {
					t.Fatalf("expected 2 actions, got %v", filteredActions)
				}
				checkCreatedTestSignerBundle(t, filteredActions[:1])
				checkDeletedTestSignerBundle("test.test/testSigner:preexisting")(t, filteredActions[1:])
			},
		},
		// 状态已完全正确（含无关签名者的 CTB）：应产生零个动作，验证同步的幂等性。
		{
			name: "CTB at the correct state - noop",
			existingCTBs: []runtime.Object{
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "nosigner",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						TrustBundle: "somedatahere",
					},
				},
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: "signer:one",
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  "signer",
						TrustBundle: "signerdata",
					},
				},
				&certificatesv1.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						Name: testBundleName,
					},
					Spec: certificatesv1.ClusterTrustBundleSpec{
						SignerName:  testSignerName,
						TrustBundle: string(testCAProvider.CurrentCABundleContent()),
					},
				},
			},
			expectActions: func(t *testing.T, actions []clienttesting.Action) {
				actions = filterOutListWatch(actions)
				if len(actions) != 0 {
					t.Fatalf("expected no actions, got %v", actions)
				}
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			testCtx := ktesting.Init(t)

			fakeClient := fakeKubeClientSetWithCTBList(t, testSignerName, tt.existingCTBs...)

			p, err := NewGAClusterTrustBundlePublisher(testSignerName, testCAProvider, fakeClient)
			if err != nil {
				t.Fatalf("failed to set up a new cluster trust bundle publisher: %v", err)
			}

			controller, ok := p.(*ClusterTrustBundlePublisher[certificatesv1.ClusterTrustBundle])
			if !ok {
				t.Fatalf("failed to assert the controller for the beta API")
			}

			// 同步依赖 informer 缓存读取现有 CTB，因此先启动 informer 并等待缓存同步完成。
			go controller.ctbInformer.RunWithContext(testCtx)
			if !cache.WaitForCacheSync(testCtx.Done(), controller.ctbInformer.HasSynced) {
				t.Fatal("timed out waiting for informer to sync")
			}

			if err := controller.syncClusterTrustBundle(testCtx); (err != nil) != tt.wantErr {
				t.Errorf("syncClusterTrustBundle() error = %v, wantErr %v", err, tt.wantErr)
			}

			tt.expectActions(t, fakeClient.Actions())
		})
	}
}

func fakeKubeClientSetWithCTBList(t *testing.T, signerName string, ctbs ...runtime.Object) *fake.Clientset {
	fakeClient := fake.NewSimpleClientset(ctbs...)
	fakeClient.PrependReactor("list", "clustertrustbundles", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
		listAction, ok := action.(clienttesting.ListAction)
		if !ok {
			t.Fatalf("expected list action, got %v", action)
		}

		// fakeClient does not implement field selector, we have to do it manually
		listRestrictions := listAction.GetListRestrictions()
		if listRestrictions.Fields == nil || listRestrictions.Fields.String() != ("spec.signerName="+signerName) {
			return false, nil, nil
		}

		retList := &certificatesv1.ClusterTrustBundleList{}
		for _, ctb := range ctbs {
			ctbObj, ok := ctb.(*certificatesv1.ClusterTrustBundle)
			if !ok {
				continue
			}
			if ctbObj.Spec.SignerName == testSignerName {
				retList.Items = append(retList.Items, *ctbObj)
			}
		}

		// Ensure that Watch doesn't return any objects either by bumping up the resource version
		// beyond what the fake client uses.
		retList.ResourceVersion = "1000"

		return true, retList, nil
	})

	return fakeClient
}

func expectAction[A clienttesting.Action](t *testing.T, action clienttesting.Action, verb string) A {
	if action.GetVerb() != verb {
		t.Fatalf("expected action with verb %q, got %q", verb, action.GetVerb())
	}

	retAction, ok := action.(A)
	if !ok {
		t.Fatalf("expected %T action, got %v", *new(A), action)
	}

	return retAction
}

func filterOutListWatch(actions []clienttesting.Action) []clienttesting.Action {
	var filtered []clienttesting.Action
	for _, a := range actions {
		if a.Matches("list", "clustertrustbundles") || a.Matches("watch", "clustertrustbundles") {
			continue
		}
		filtered = append(filtered, a)
	}
	return filtered
}

func testingCABundlleProvider(t *testing.T) dynamiccertificates.CAContentProvider {
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatalf("failed to create a private key: %v", err)
	}
	caCert, err := certutil.NewSelfSignedCACert(certutil.Config{CommonName: "test-ca"}, key)
	if err != nil {
		t.Fatalf("failed to create a self-signed CA cert: %v", err)
	}

	caPEM, err := certutil.EncodeCertificates(caCert)
	if err != nil {
		t.Fatalf("failed to PEM-encode cert: %v", err)
	}

	caProvider, err := dynamiccertificates.NewStaticCAContent("testca", caPEM)
	if err != nil {
		t.Fatalf("failed to create a static CA provider: %v", err)
	}

	return caProvider
}
