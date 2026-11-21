/*
Copyright 2023 The Kubernetes Authors.

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

package clustertrustbundle

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	certificatesv1 "k8s.io/api/certificates/v1"
	certificatesv1alpha1 "k8s.io/api/certificates/v1alpha1"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestBeforeSynced 校验 informer 尚未同步时 GetTrustAnchorsByName 会返回错误。
func TestBeforeSynced(t *testing.T) {
	tCtx := ktesting.Init(t)
	kc := fake.NewSimpleClientset()

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, 0)

	ctbManager, _ := NewBetaInformerManager(tCtx, informerFactory, 256, 5*time.Minute)

	// 未启动 informer 就直接查询，用于验证“未同步即失败”的保护。
	_, err := ctbManager.GetTrustAnchorsByName(tCtx, "foo", false)
	if err == nil {
		t.Fatalf("Got nil error, wanted non-nil")
	}
}

// testClient 抽象不同 API 版本的 ClusterTrustBundle 创建/删除操作。
type testClient[T clusterTrustBundle] interface {
	Create(context.Context, *T, metav1.CreateOptions) (*T, error)
	Delete(context.Context, string, metav1.DeleteOptions) error
}

// testingFunctionBundle 是与 API 版本无关的测试函数集合，
// 用于让 v1alpha1/v1beta1/v1 复用同一套用例。
type testingFunctionBundle[T clusterTrustBundle] struct {
	ctbConstructor func(name, signerName string, labels map[string]string, bundle string) *T
	ctbToObj       func(*T) runtime.Object
	ctbTrustBundle func(*T) string

	informerManagerConstructor func(ctx context.Context, informerFactory informers.SharedInformerFactory, cacheSize int, cacheTTL time.Duration) (Manager, error)
	informerGetter             func(informers.SharedInformerFactory) cache.SharedIndexInformer
	clientGetter               func(kubernetes.Interface) testClient[T]
}

// alphaFunctionsBundle 是 v1alpha1 版本的测试函数集合。
var alphaFunctionsBundle = testingFunctionBundle[certificatesv1alpha1.ClusterTrustBundle]{
	ctbConstructor: mustMakeAlphaCTB,
	ctbToObj:       func(ctb *certificatesv1alpha1.ClusterTrustBundle) runtime.Object { return ctb },
	ctbTrustBundle: (&alphaClusterTrustBundleHandlers{}).GetTrustBundle,

	informerManagerConstructor: NewAlphaInformerManager,
	informerGetter: func(informerFactory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return informerFactory.Certificates().V1alpha1().ClusterTrustBundles().Informer()
	},
	clientGetter: func(c kubernetes.Interface) testClient[certificatesv1alpha1.ClusterTrustBundle] {
		return c.CertificatesV1alpha1().ClusterTrustBundles()
	},
}

// betaFunctionsBundle 是 v1beta1 版本的测试函数集合。
var betaFunctionsBundle = testingFunctionBundle[certificatesv1beta1.ClusterTrustBundle]{
	ctbConstructor: mustMakeBetaCTB,
	ctbToObj:       func(ctb *certificatesv1beta1.ClusterTrustBundle) runtime.Object { return ctb },
	ctbTrustBundle: (&betaClusterTrustBundleHandlers{}).GetTrustBundle,

	informerManagerConstructor: NewBetaInformerManager,
	informerGetter: func(informerFactory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return informerFactory.Certificates().V1beta1().ClusterTrustBundles().Informer()
	},
	clientGetter: func(c kubernetes.Interface) testClient[certificatesv1beta1.ClusterTrustBundle] {
		return c.CertificatesV1beta1().ClusterTrustBundles()
	},
}

// gaFunctionsBundle 是 v1 版本的测试函数集合。
var gaFunctionsBundle = testingFunctionBundle[certificatesv1.ClusterTrustBundle]{
	ctbConstructor: mustMakeGACTB,
	ctbToObj:       func(ctb *certificatesv1.ClusterTrustBundle) runtime.Object { return ctb },
	ctbTrustBundle: (&gaClusterTrustBundleHandlers{}).GetTrustBundle,

	informerManagerConstructor: NewGAInformerManager,
	informerGetter: func(informerFactory informers.SharedInformerFactory) cache.SharedIndexInformer {
		return informerFactory.Certificates().V1().ClusterTrustBundles().Informer()
	},
	clientGetter: func(c kubernetes.Interface) testClient[certificatesv1.ClusterTrustBundle] {
		return c.CertificatesV1().ClusterTrustBundles()
	},
}

// TestGetTrustAnchorsByName 在三个 API 版本上复用同一套“按名称查询”用例。
func TestGetTrustAnchorsByName(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.SyncTest("v1alpha1", func(tCtx ktesting.TContext) { testGetTrustAnchorsByName(tCtx, alphaFunctionsBundle) })
	tCtx.SyncTest("v1beta1", func(tCtx ktesting.TContext) { testGetTrustAnchorsByName(tCtx, betaFunctionsBundle) })
	tCtx.SyncTest("v1", func(tCtx ktesting.TContext) { testGetTrustAnchorsByName(tCtx, gaFunctionsBundle) })
}

// testGetTrustAnchorsByName 校验按名称获取信任锚，以及 allowMissing 为 true 时
// 对不存在的 CTB 返回空结果。
func testGetTrustAnchorsByName[T clusterTrustBundle](tCtx ktesting.TContext, b testingFunctionBundle[T]) {
	defer tCtx.Cancel("test completed")
	t := tCtx.TB()

	ctb1Bundle := mustMakeRoot(t, "root1")
	ctb1 := b.ctbConstructor("ctb1", "", nil, ctb1Bundle)
	ctb2Bundle := mustMakeRoot(t, "root2")
	ctb2 := b.ctbConstructor("ctb2", "", nil, ctb2Bundle)

	kc := fake.NewSimpleClientset(b.ctbToObj(ctb1), b.ctbToObj(ctb2))

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, 0)

	ctbManager, _ := b.informerManagerConstructor(tCtx, informerFactory, 256, 5*time.Minute)

	// 必须先等缓存同步，否则查询结果取决于 informer 的启动时序。
	informerFactory.Start(tCtx.Done())
	ctbInformer := b.informerGetter(informerFactory)
	if !cache.WaitForCacheSync(tCtx.Done(), ctbInformer.HasSynced) {
		t.Fatalf("Timed out waiting for informer to sync")
	}

	gotBundle, err := ctbManager.GetTrustAnchorsByName(tCtx, "ctb1", false)
	if err != nil {
		t.Fatalf("Error while calling GetTrustAnchorsByName: %v", err)
	}

	if diff := diffBundles(gotBundle, []byte(b.ctbTrustBundle(ctb1))); diff != "" {
		t.Fatalf("Got bad bundle; diff (-got +want)\n%s", diff)
	}

	gotBundle, err = ctbManager.GetTrustAnchorsByName(tCtx, "ctb2", false)
	if err != nil {
		t.Fatalf("Error while calling GetTrustAnchorsByName: %v", err)
	}

	if diff := diffBundles(gotBundle, []byte(b.ctbTrustBundle(ctb2))); diff != "" {
		t.Fatalf("Got bad bundle; diff (-got +want)\n%s", diff)
	}

	_, err = ctbManager.GetTrustAnchorsByName(tCtx, "not-found", false)
	if err == nil { // 错误必须非 nil
		t.Fatalf("While looking up nonexisting ClusterTrustBundle, got nil error, wanted non-nil")
	}

	_, err = ctbManager.GetTrustAnchorsByName(tCtx, "not-found", true)
	if err != nil {
		t.Fatalf("Unexpected error while calling GetTrustAnchorsByName for nonexistent CTB with allowMissing: %v", err)
	}
}

// TestGetTrustAnchorsByNameCaching 校验按名称查询的缓存命中，
// 以及 CTB 更新后缓存会被 informer 失效。
func TestGetTrustAnchorsByNameCaching(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.SyncTest("v1alpha1", func(tCtx ktesting.TContext) { testGetTrustAnchorsByNameCaching(tCtx, alphaFunctionsBundle) })
	tCtx.SyncTest("v1beta1", func(tCtx ktesting.TContext) { testGetTrustAnchorsByNameCaching(tCtx, betaFunctionsBundle) })
	tCtx.SyncTest("v1", func(tCtx ktesting.TContext) { testGetTrustAnchorsByNameCaching(tCtx, gaFunctionsBundle) })
}

// testGetTrustAnchorsByNameCaching 是缓存场景的跨版本测试主体。
func testGetTrustAnchorsByNameCaching[T clusterTrustBundle](tCtx ktesting.TContext, b testingFunctionBundle[T]) {
	defer tCtx.Cancel("test completed")
	t := tCtx.TB()

	ctb1Bundle := mustMakeRoot(t, "root1")
	ctb1 := b.ctbConstructor("foo", "", nil, ctb1Bundle)

	ctb2Bundle := mustMakeRoot(t, "root2")
	ctb2 := b.ctbConstructor("foo", "", nil, ctb2Bundle)

	kc := fake.NewSimpleClientset(b.ctbToObj(ctb1))

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, 0)

	ctbManager, _ := b.informerManagerConstructor(tCtx, informerFactory, 256, 5*time.Minute)

	// 必须先等缓存同步，否则查询结果取决于 informer 的启动时序。
	informerFactory.Start(tCtx.Done())
	ctbInformer := b.informerGetter(informerFactory)
	if !cache.WaitForCacheSync(tCtx.Done(), ctbInformer.HasSynced) {
		t.Fatalf("Timed out waiting for informer to sync")
	}

	func() {
		t.Log("foo should yield the first certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsByName(tCtx, "foo", false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb1)

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("foo should still yield the first certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsByName(tCtx, "foo", false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb1)

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	client := b.clientGetter(kc)

	if err := client.Delete(tCtx, "foo", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Error while deleting the old CTB: %v", err)
	}
	if _, err := client.Create(tCtx, ctb2, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Error while adding new CTB: %v", err)
	}

	// 等待足够长让 informer 感知新的 ClusterTrustBundle，但远小于 5 分钟的缓存 TTL；
	// 这样能证明缓存是被 informer 主动失效的，而不是自然过期。
	time.Sleep(5 * time.Second)

	func() {
		t.Log("foo should yield the new certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsByName(tCtx, "foo", false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb2)
		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()
}

// TestGetTrustAnchorsBySignerName 校验按签名者与标签选择器查询信任锚，
// 包括去重、排序与 allowMissing 行为。
func TestGetTrustAnchorsBySignerName(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.SyncTest("v1alpha1", func(tCtx ktesting.TContext) { testGetTrustAnchorsBySignerName(tCtx, alphaFunctionsBundle) })
	tCtx.SyncTest("v1beta1", func(tCtx ktesting.TContext) { testGetTrustAnchorsBySignerName(tCtx, betaFunctionsBundle) })
	tCtx.SyncTest("v1", func(tCtx ktesting.TContext) { testGetTrustAnchorsBySignerName(tCtx, gaFunctionsBundle) })
}

// testGetTrustAnchorsBySignerName 是签名者查询场景的跨版本测试主体。
func testGetTrustAnchorsBySignerName[T clusterTrustBundle](tCtx ktesting.TContext, b testingFunctionBundle[T]) {
	defer tCtx.Cancel("test completed")
	t := tCtx.TB()

	ctb1 := b.ctbConstructor("signer-a-label-a-1", "foo.bar/a", map[string]string{"label": "a"}, mustMakeRoot(t, "0"))
	ctb2 := b.ctbConstructor("signer-a-label-a-2", "foo.bar/a", map[string]string{"label": "a"}, mustMakeRoot(t, "1"))
	ctb2dup := b.ctbConstructor("signer-a-label-2-dup", "foo.bar/a", map[string]string{"label": "a"}, b.ctbTrustBundle(ctb2))
	ctb3 := b.ctbConstructor("signer-a-label-b-1", "foo.bar/a", map[string]string{"label": "b"}, mustMakeRoot(t, "2"))
	ctb4 := b.ctbConstructor("signer-b-label-a-1", "foo.bar/b", map[string]string{"label": "a"}, mustMakeRoot(t, "3"))

	kc := fake.NewSimpleClientset(b.ctbToObj(ctb1), b.ctbToObj(ctb2), b.ctbToObj(ctb2dup), b.ctbToObj(ctb3), b.ctbToObj(ctb4))

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, 0)

	ctbManager, _ := b.informerManagerConstructor(tCtx, informerFactory, 256, 5*time.Minute)

	// 必须先等缓存同步，否则查询结果取决于 informer 的启动时序。
	informerFactory.Start(tCtx.Done())
	ctbInformer := b.informerGetter(informerFactory)
	if !cache.WaitForCacheSync(tCtx.Done(), ctbInformer.HasSynced) {
		t.Fatalf("Timed out waiting for informer to sync")
	}

	func() {
		t.Log("big labelselector should cause error")
		longString := strings.Builder{}
		for i := 0; i < 63; i++ {
			longString.WriteString("v")
		}
		matchLabels := map[string]string{}
		for i := 0; i < 100*1024/63+1; i++ {
			matchLabels[fmt.Sprintf("key-%d", i)] = longString.String()
		}

		_, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", &metav1.LabelSelector{MatchLabels: matchLabels}, false)
		if err == nil || !strings.Contains(err.Error(), "label selector length") {
			t.Fatalf("Bad error, got %v, wanted it to contain \"label selector length\"", err)
		}
	}()

	func() {
		t.Log("signer-a label-a should yield two sorted certificates")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "a"}}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb1) + b.ctbTrustBundle(ctb2)

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-a with nil selector should yield zero certificates")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", nil, true)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := ""

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-b with empty selector should yield one certificates")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/b", &metav1.LabelSelector{}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		if diff := diffBundles(gotBundle, []byte(b.ctbTrustBundle(ctb4))); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-a label-b should yield one certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "b"}}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		if diff := diffBundles(gotBundle, []byte(b.ctbTrustBundle(ctb3))); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-b label-a should yield one certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/b", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "a"}}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		if diff := diffBundles(gotBundle, []byte(b.ctbTrustBundle(ctb4))); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-b label-b allowMissing=true should yield zero certificates")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/b", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "b"}}, true)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		if diff := diffBundles(gotBundle, []byte{}); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-b label-b allowMissing=false should yield zero certificates (error)")
		_, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/b", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "b"}}, false)
		if err == nil { // 错误必须非 nil
			t.Fatalf("Got nil error while calling GetTrustAnchorsBySigner, wanted non-nil")
		}
	}()
}

// TestGetTrustAnchorsBySignerNameCaching 校验签名者查询的缓存失效行为。
func TestGetTrustAnchorsBySignerNameCaching(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.SyncTest("v1alpha1", func(tCtx ktesting.TContext) { testGetTrustAnchorsBySignerNameCaching(tCtx, alphaFunctionsBundle) })
	tCtx.SyncTest("v1beta1", func(tCtx ktesting.TContext) { testGetTrustAnchorsBySignerNameCaching(tCtx, betaFunctionsBundle) })
	tCtx.SyncTest("v1", func(tCtx ktesting.TContext) { testGetTrustAnchorsBySignerNameCaching(tCtx, gaFunctionsBundle) })
}

// testGetTrustAnchorsBySignerNameCaching 是签名者缓存场景的跨版本测试主体。
func testGetTrustAnchorsBySignerNameCaching[T clusterTrustBundle](tCtx ktesting.TContext, b testingFunctionBundle[T]) {
	defer tCtx.Cancel("test completed")
	t := tCtx.TB()

	ctb1 := b.ctbConstructor("signer-a-label-a-1", "foo.bar/a", map[string]string{"label": "a"}, mustMakeRoot(t, "0"))
	ctb2 := b.ctbConstructor("signer-a-label-a-2", "foo.bar/a", map[string]string{"label": "a"}, mustMakeRoot(t, "1"))

	kc := fake.NewSimpleClientset(b.ctbToObj(ctb1))

	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, 0)

	ctbManager, _ := b.informerManagerConstructor(tCtx, informerFactory, 256, 5*time.Minute)

	// 必须先等缓存同步，否则查询结果取决于 informer 的启动时序。
	informerFactory.Start(tCtx.Done())
	ctbInformer := b.informerGetter(informerFactory)
	if !cache.WaitForCacheSync(tCtx.Done(), ctbInformer.HasSynced) {
		t.Fatalf("Timed out waiting for informer to sync")
	}

	func() {
		t.Log("signer-a label-a should yield one certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "a"}}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb1)

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	func() {
		t.Log("signer-a label-a should yield the same result when called again")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "a"}}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb1)

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()

	client := b.clientGetter(kc)
	if err := client.Delete(tCtx, "signer-a-label-a-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("Error while deleting the old CTB: %v", err)
	}
	if _, err := client.Create(tCtx, ctb2, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Error while adding new CTB: %v", err)
	}

	// 等待足够长让 informer 感知新的 ClusterTrustBundle，但远小于 5 分钟的缓存 TTL；
	// 这样能证明缓存是被 informer 主动失效的，而不是自然过期。
	time.Sleep(5 * time.Second)

	func() {
		t.Log("signer-a label-a should return the new certificate")
		gotBundle, err := ctbManager.GetTrustAnchorsBySigner(tCtx, "foo.bar/a", &metav1.LabelSelector{MatchLabels: map[string]string{"label": "a"}}, false)
		if err != nil {
			t.Fatalf("Got error while calling GetTrustAnchorsBySigner: %v", err)
		}

		wantBundle := b.ctbTrustBundle(ctb2)

		if diff := diffBundles(gotBundle, []byte(wantBundle)); diff != "" {
			t.Fatalf("Bad bundle; diff (-got +want)\n%s", diff)
		}
	}()
}

// mustMakeRoot 生成自签名的 ed25519 根证书，并返回 PEM 编码。
func mustMakeRoot(t ktesting.TB, cn string) string {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Error while generating key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(0),
		Subject: pkix.Name{
			CommonName: cn,
		},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	cert, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		t.Fatalf("Error while making certificate: %v", err)
	}

	return string(pem.EncodeToMemory(&pem.Block{
		Type:    "CERTIFICATE",
		Headers: nil,
		Bytes:   cert,
	}))
}

// mustMakeGACTB 构造 v1 ClusterTrustBundle 测试对象。
func mustMakeGACTB(name, signerName string, labels map[string]string, bundle string) *certificatesv1.ClusterTrustBundle {
	return &certificatesv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: certificatesv1.ClusterTrustBundleSpec{
			SignerName:  signerName,
			TrustBundle: bundle,
		},
	}
}

// mustMakeBetaCTB 构造 v1beta1 ClusterTrustBundle 测试对象。
func mustMakeBetaCTB(name, signerName string, labels map[string]string, bundle string) *certificatesv1beta1.ClusterTrustBundle {
	return &certificatesv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: certificatesv1beta1.ClusterTrustBundleSpec{
			SignerName:  signerName,
			TrustBundle: bundle,
		},
	}
}

// mustMakeAlphaCTB 构造 v1alpha1 ClusterTrustBundle 测试对象。
func mustMakeAlphaCTB(name, signerName string, labels map[string]string, bundle string) *certificatesv1alpha1.ClusterTrustBundle {
	return &certificatesv1alpha1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
		Spec: certificatesv1alpha1.ClusterTrustBundleSpec{
			SignerName:  signerName,
			TrustBundle: bundle,
		},
	}
}

// diffBundles 解析两组 PEM 块、排序后比较，避免块顺序不同造成假失败。
func diffBundles(a, b []byte) string {
	var block *pem.Block

	aBlocks := []*pem.Block{}
	for {
		block, a = pem.Decode(a)
		if block == nil {
			break
		}
		aBlocks = append(aBlocks, block)
	}
	sort.Slice(aBlocks, func(i, j int) bool {
		if aBlocks[i].Type < aBlocks[j].Type {
			return true
		} else if aBlocks[i].Type == aBlocks[j].Type {
			comp := bytes.Compare(aBlocks[i].Bytes, aBlocks[j].Bytes)
			return comp <= 0
		} else {
			return false
		}
	})

	bBlocks := []*pem.Block{}
	for {
		block, b = pem.Decode(b)
		if block == nil {
			break
		}
		bBlocks = append(bBlocks, block)
	}
	sort.Slice(bBlocks, func(i, j int) bool {
		if bBlocks[i].Type < bBlocks[j].Type {
			return true
		} else if bBlocks[i].Type == bBlocks[j].Type {
			comp := bytes.Compare(bBlocks[i].Bytes, bBlocks[j].Bytes)
			return comp <= 0
		} else {
			return false
		}
	})

	return cmp.Diff(aBlocks, bBlocks)
}

// TestLazyInformerManager_ensureManagerSet 校验依据 API 发现结果选择
// 最合适的 informer 管理器（v1 > v1beta1 > v1alpha1），以及 API 不可用时的回退。
func TestLazyInformerManager_ensureManagerSet(t *testing.T) {
	tests := []struct {
		name             string
		injectError      error
		ctbsAvailableGVs []string
		wantManager      string
		wantError        bool
	}{
		{
			name:        "API unavailable",
			injectError: errors.NewNotFound(schema.GroupResource{Group: "certificates.k8s.io/v1beta1"}, ""),
			wantManager: "noop",
		},
		{
			name:        "err in discovery",
			injectError: fmt.Errorf("unexpected discovery error"),
			wantError:   true,
			wantManager: "nil",
		},
		{
			name:             "API available in v1alpha1",
			ctbsAvailableGVs: []string{"v1alpha1"},
			wantManager:      "v1alpha1",
		},
		{
			name:             "API available in an unhandled version",
			ctbsAvailableGVs: []string{"v1beta2"},
			wantManager:      "noop",
		},
		{
			name:             "API available in v1beta1",
			ctbsAvailableGVs: []string{"v1beta1"},
			wantManager:      "v1beta1",
		},
		{
			name:             "API available in v1",
			ctbsAvailableGVs: []string{"v1"},
			wantManager:      "v1",
		},
		{
			name:             "err in discovery but beta API shard available",
			injectError:      fmt.Errorf("unexpected discovery error"),
			ctbsAvailableGVs: []string{"v1beta1"},
			wantError:        true,
			wantManager:      "nil",
		},
		{
			name:             "err in discovery but v1 API shard available",
			injectError:      fmt.Errorf("unexpected discovery error"),
			ctbsAvailableGVs: []string{"v1"},
			wantManager:      "v1",
		},
		{
			name:             "API available in alpha and beta - prefer beta",
			ctbsAvailableGVs: []string{"v1alpha1", "v1beta1"},
			wantManager:      "v1beta1",
		},
		{
			name:             "API available in multiple handled and unhandled versions - prefer the most-GA handled version",
			ctbsAvailableGVs: []string{"v1alpha1", "v1", "v2", "v1beta1", "v1alpha2"},
			wantManager:      "v1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, loggerCtx := ktesting.NewTestContext(t)

			fakeDisc := fakeDiscovery{
				err:         tt.injectError,
				gvResources: make(map[string]*metav1.APIResourceList),
			}

			// 只为指定 API 版本暴露 clustertrustbundles 资源。
			for _, gv := range tt.ctbsAvailableGVs {
				fakeDisc.gvResources["certificates.k8s.io/"+gv] = &metav1.APIResourceList{
					APIResources: []metav1.APIResource{
						{Name: "certificatesigningrequests"},
						{Name: "clustertrustbundles"},
					},
				}
			}

			m := &LazyInformerManager{
				managerLock:       sync.RWMutex{},
				client:            NewFakeClientset(fakeDisc),
				cacheSize:         128,
				contextWithLogger: loggerCtx,
				logger:            logger,
			}
			if err := m.ensureManagerSet(loggerCtx); tt.wantError != (err != nil) {
				t.Errorf("expected error: %t, got %v", tt.wantError, err)
			}

			// 通过具体类型断言选择结果，避免依赖内部状态字符串。
			switch manager := m.manager.(type) {
			case *InformerManager[certificatesv1alpha1.ClusterTrustBundle]:
				require.Equal(t, tt.wantManager, "v1alpha1")
			case *InformerManager[certificatesv1beta1.ClusterTrustBundle]:
				require.Equal(t, tt.wantManager, "v1beta1")
			case *InformerManager[certificatesv1.ClusterTrustBundle]:
				require.Equal(t, tt.wantManager, "v1")
			case *NoopManager:
				require.Equal(t, tt.wantManager, "noop")
			case nil:
				require.Equal(t, tt.wantManager, "nil")
			default:
				t.Fatalf("unknown manager type: %T", manager)
			}
		})
	}
}

// fakeDiscovery 通过内嵌 FakeDiscovery 实现 DiscoveryInterface，
// 并用测试数据覆盖资源发现相关方法。
type fakeDiscovery struct {
	fakediscovery.FakeDiscovery
	gvResources map[string]*metav1.APIResourceList
	err         error
}

// ServerResourcesForGroupVersion 返回预置的资源列表与错误。
func (d fakeDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return d.gvResources[groupVersion], d.err
}

// ServerResourcesForGroupVersionWithContext 返回预置的资源列表与错误。
func (d fakeDiscovery) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	return d.gvResources[groupVersion], d.err
}

// fakeDiscoveryClientSet 用自定义 Discovery 实现替换 fake Clientset 的发现能力。
type fakeDiscoveryClientSet struct {
	*fake.Clientset
	DiscoveryObj *fakeDiscovery
}

// Discovery 返回注入的 fakeDiscovery。
func (c *fakeDiscoveryClientSet) Discovery() discovery.DiscoveryInterfaces {
	return c.DiscoveryObj
}

// NewFakeClientset 创建 fake Clientset，并替换其 Discovery 实现。
func NewFakeClientset(fakeDiscovery fakeDiscovery) *fakeDiscoveryClientSet {
	cs := &fakeDiscoveryClientSet{
		Clientset:    fake.NewClientset(),
		DiscoveryObj: &fakeDiscovery,
	}
	return cs
}
