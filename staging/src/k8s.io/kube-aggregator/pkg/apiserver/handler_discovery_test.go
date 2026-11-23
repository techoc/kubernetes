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

package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/randfill"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	scheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// 用可观测的队列包装真实工作队列，让测试能判断“所有脏对象都已处理完”，
// 而不是靠 sleep 猜时间；生产代码仍然使用原始的限速队列。
func newDiscoveryManager(rm discoveryendpoint.ResourceManager) *discoveryManager {
	dm := NewDiscoveryManager(rm).(*discoveryManager)
	dm.dirtyAPIServiceQueue = newCompleterWorkqueue(dm.dirtyAPIServiceQueue)

	return dm
}

// Returns true if the queue of services to sync is complete which means
// everything has been reconciled and placed into merged document
// 复用 cache 的同步等待语义：每隔一小段时间轮询队列状态直到 complete 或 stop，
// 避免测试用固定 sleep 造成偶发的假通过。
func waitForQueueComplete(stopCh <-chan struct{}, dm *discoveryManager) bool {
	return cache.WaitForCacheSync(stopCh, func() bool {
		return dm.dirtyAPIServiceQueue.(*completerWorkqueue).isComplete()
	})
}

// Test that the discovery manager starts and aggregates from two local API services
// 用 fuzz 生成大量 group/version 后再逐个注册 APIService，验证聚合不是只适用于
// 手工拼出的一两个对象；第三组专门覆盖 ResponseKind 缺失/为空的修复逻辑。
func TestBasic(t *testing.T) {
	service1 := discoveryendpoint.NewResourceManager("apis")
	service2 := discoveryendpoint.NewResourceManager("apis")
	service3 := discoveryendpoint.NewResourceManager("apis")
	apiGroup1 := fuzzAPIGroups(2, 5, 25)
	apiGroup2 := fuzzAPIGroups(2, 5, 50)
	apiGroup3 := apidiscoveryv2.APIGroupDiscoveryList{Items: []apidiscoveryv2.APIGroupDiscovery{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "weird.example.com"},
			Versions: []apidiscoveryv2.APIVersionDiscovery{
				{
					Version:   "v1",
					Freshness: "Current",
					Resources: []apidiscoveryv2.APIResourceDiscovery{
						{
							Resource: "parent-missing-kind",
							Subresources: []apidiscoveryv2.APISubresourceDiscovery{
								{Subresource: "subresource-missing-kind"},
							},
						},
						{
							Resource:     "parent-empty-kind",
							ResponseKind: &metav1.GroupVersionKind{},
							Subresources: []apidiscoveryv2.APISubresourceDiscovery{
								{Subresource: "subresource-empty-kind", ResponseKind: &metav1.GroupVersionKind{}},
							},
						},
						{
							Resource:     "parent-with-kind",
							ResponseKind: &metav1.GroupVersionKind{Kind: "ParentWithKind"},
							Subresources: []apidiscoveryv2.APISubresourceDiscovery{
								{Subresource: "subresource-with-kind", ResponseKind: &metav1.GroupVersionKind{Kind: "SubresourceWithKind"}},
							},
						},
					},
				},
			},
		},
	}}
	apiGroup3WithFixup := apidiscoveryv2.APIGroupDiscoveryList{Items: []apidiscoveryv2.APIGroupDiscovery{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "weird.example.com"},
			Versions: []apidiscoveryv2.APIVersionDiscovery{
				{
					Version:   "v1",
					Freshness: "Current",
					Resources: []apidiscoveryv2.APIResourceDiscovery{
						{
							Resource:     "parent-missing-kind",
							ResponseKind: &metav1.GroupVersionKind{}, // defaulted by aggregator
							Subresources: []apidiscoveryv2.APISubresourceDiscovery{
								{Subresource: "subresource-missing-kind", ResponseKind: &metav1.GroupVersionKind{}}, // defaulted by aggregator
							},
						},
						{
							Resource:     "parent-empty-kind",
							ResponseKind: &metav1.GroupVersionKind{},
							Subresources: []apidiscoveryv2.APISubresourceDiscovery{
								{Subresource: "subresource-empty-kind", ResponseKind: &metav1.GroupVersionKind{}},
							},
						},
						{
							Resource:     "parent-with-kind",
							ResponseKind: &metav1.GroupVersionKind{Kind: "ParentWithKind"},
							Subresources: []apidiscoveryv2.APISubresourceDiscovery{
								{Subresource: "subresource-with-kind", ResponseKind: &metav1.GroupVersionKind{Kind: "SubresourceWithKind"}},
							},
						},
					},
				},
			},
		},
	}}
	service1.SetGroups(apiGroup1.Items)
	service2.SetGroups(apiGroup2.Items)
	service3.SetGroups(apiGroup3.Items)
	// 让每个版本拥有一个递减的 VersionPriority，测出聚合结果按版本排序
	// 而不是按注册顺序或字符串顺序返回。
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")
	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)

	// 每组服务注册自己的 GroupVersion，模拟多个聚合 APIService 指向不同后端；
	// 用服务名区分来源，聚合时必须把三份结果合并进同一个发现文档。
	for _, g := range apiGroup1.Items {
		versionPriority := int32(len(g.Versions) + 1)
		for _, v := range g.Versions {
			versionPriority--
			aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
				ObjectMeta: metav1.ObjectMeta{
					Name: v.Version + "." + g.Name,
				},
				Spec: apiregistrationv1.APIServiceSpec{
					Group:           g.Name,
					Version:         v.Version,
					VersionPriority: versionPriority,
					Service: &apiregistrationv1.ServiceReference{
						Name: "service1",
					},
				},
			}, service1)
		}
	}

	for _, g := range apiGroup2.Items {
		versionPriority := int32(len(g.Versions) + 1)
		for _, v := range g.Versions {
			versionPriority--
			aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
				ObjectMeta: metav1.ObjectMeta{
					Name: v.Version + "." + g.Name,
				},
				Spec: apiregistrationv1.APIServiceSpec{
					Group:           g.Name,
					Version:         v.Version,
					VersionPriority: versionPriority,
					Service: &apiregistrationv1.ServiceReference{
						Name: "service2",
					},
				},
			}, service2)
		}
	}

	for _, g := range apiGroup3.Items {
		versionPriority := int32(len(g.Versions) + 1)
		for _, v := range g.Versions {
			versionPriority--
			aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
				ObjectMeta: metav1.ObjectMeta{
					Name: v.Version + "." + g.Name,
				},
				Spec: apiregistrationv1.APIServiceSpec{
					Group:           g.Name,
					Version:         v.Version,
					VersionPriority: versionPriority,
					Service: &apiregistrationv1.ServiceReference{
						Name: "service3",
					},
				},
			}, service3)
		}
	}

	testCtx, testCancel := context.WithCancel(context.Background())
	defer testCancel()

	go aggregatedManager.Run(testCtx.Done(), nil)

	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	response, _, parsed := fetchPath(aggregatedResourceManager, "")
	if response.StatusCode != 200 {
		t.Fatalf("unexpected status code %d", response.StatusCode)
	}
	checkAPIGroups(t, apiGroup1, parsed)
	checkAPIGroups(t, apiGroup2, parsed)
	// 第三组的期望值填好了缺失的 ResponseKind：聚合器会补默认值，
	// 直接拿输入比会误报，所以这里单独用修正后的期望对象。
	checkAPIGroups(t, apiGroup3WithFixup, parsed)
}

// 逐组比对而不是直接比较整个列表：聚合后的顺序由优先级决定，
// 测试只关心每份输入都被完整保留，不想把排序实现细节写死在用例里。
func checkAPIGroups(t *testing.T, api apidiscoveryv2.APIGroupDiscoveryList, response *apidiscoveryv2.APIGroupDiscoveryList) {
	t.Helper()
	if len(response.Items) < len(api.Items) {
		t.Errorf("expected to check for at least %d groups, only have %d groups in response", len(api.Items), len(response.Items))
	}
	for _, knownGroup := range api.Items {
		found := false
		for _, possibleGroup := range response.Items {
			if knownGroup.Name == possibleGroup.Name {
				t.Logf("found %s", knownGroup.Name)
				found = true
				diff := cmp.Diff(knownGroup, possibleGroup)
				if len(diff) > 0 {
					t.Error(diff)
				}
			}
		}
		if found == false {
			t.Errorf("could not find %s", knownGroup.Name)
		}
	}
}

// TestInitialRunHasAllAPIServices tests that when discovery is ready, all APIService
// are present and ones that have not synced are in the list as Stale.
// 发现文档就绪时不能因为某个后端慢就把它整组藏起来：客户端需要看到完整的
// API 地图，未同步的部分用 Stale 标记，自己决定是否重试。
func TestInitialRunHasAllAPIServices(t *testing.T) {
	// 故意让后端请求永远阻塞，强制走“初始同步尚未完成”的分支；
	// 这里只有暴露非常早的发现快照才能验证 Stale 条目。
	neverReturnCh := make(chan struct{})
	defer close(neverReturnCh)
	service := discoveryendpoint.NewResourceManager("apis")
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")

	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)

	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-neverReturnCh
		service.ServeHTTP(w, r)
	}))
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initialSyncedCh := make(chan struct{})
	go aggregatedManager.Run(testCtx.Done(), initialSyncedCh)
	select {
	case <-initialSyncedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for initial sync")
	}

	response, _, parsed := fetchPath(aggregatedResourceManager, "")
	if response.StatusCode != 200 {
		t.Fatalf("unexpected status code %d", response.StatusCode)
	}

	apiGroup := apidiscoveryv2.APIGroupDiscoveryList{Items: []apidiscoveryv2.APIGroupDiscovery{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "stable.example.com"},
			Versions: []apidiscoveryv2.APIVersionDiscovery{
				{
					Version:   "v1",
					Freshness: "Stale",
				},
			},
		},
	}}

	checkAPIGroups(t, apiGroup, parsed)
}

// 同一个 APIService 换后端后，旧的缓存结果必须被清掉；否则聚合文档里
// 会同时留下两个版本的同一组，客户端不知道该信哪一个。
func TestServiceGC(t *testing.T) {
	service := discoveryendpoint.NewResourceManager("apis")

	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")
	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)

	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, service)

	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// Lookup size of cache
	getCacheLen := func() int {
		aggregatedManager.resultsLock.Lock()
		defer aggregatedManager.resultsLock.Unlock()
		return len(aggregatedManager.cachedResults)
	}

	require.Equal(t, 1, getCacheLen())

	// Change the service of the same APIService a bit to create duplicate entry
	// 服务名变了但对象名没变，缓存键只用对象名，正好检验旧结果是否被覆盖而不是追加。
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service-changed",
			},
		},
	}, service)

	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))
	require.Equal(t, 1, getCacheLen())
}

// Test that a handler associated with an APIService gets pinged after the
// APIService has been marked as dirty
// 后端发现文档是拉取而不是推送的：只有 APIService 变更时主动请求一次，
// 才能知道后端有哪些资源；这个用例确认“脏标记”确实触发了这次请求。
func TestDirty(t *testing.T) {
	var pinged atomic.Bool
	service := discoveryendpoint.NewResourceManager("apis")
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")

	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)

	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pinged.Store(true)
		service.ServeHTTP(w, r)
	}))
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// immediately check for ping, since Run() should block for local services
	if !pinged.Load() {
		t.Errorf("service handler never pinged")
	}
}

// Shows that waitForQueueComplete also waits for syncing to
// complete by artificially making the sync handler take a long time
// 队列清空不等于结果已写入：Done 在同步完成之后才调用，若 waitForQueueComplete
// 只看队列长度就会过早返回，导致后续断言读到旧文档。
func TestWaitForSync(t *testing.T) {
	pinged := atomic.Bool{}
	service := discoveryendpoint.NewResourceManager("apis")
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")

	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)

	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		pinged.Store(true)
		service.ServeHTTP(w, r)
	}))
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// immediately check for ping, since Run() should block for local services
	if !pinged.Load() {
		t.Errorf("service handler never pinged")
	}
}

// Show that an APIService can be removed and that its group no longer remains
// if there are no versions
// 删除最后一个版本时要连空组一起清掉：留一个没有任何版本的组会让客户端
// 误以为该组仍可用，却找不到任何可调用的资源。
func TestRemoveAPIService(t *testing.T) {
	aggyService := discoveryendpoint.NewResourceManager("apis")
	service := discoveryendpoint.NewResourceManager("apis")
	apiGroup := fuzzAPIGroups(2, 3, 10)
	service.SetGroups(apiGroup.Items)

	var apiServices []*apiregistrationv1.APIService
	for _, g := range apiGroup.Items {
		for _, v := range g.Versions {
			apiservice := &apiregistrationv1.APIService{
				ObjectMeta: metav1.ObjectMeta{
					Name: v.Version + "." + g.Name,
				},
				Spec: apiregistrationv1.APIServiceSpec{
					Group:   g.Name,
					Version: v.Version,
					Service: &apiregistrationv1.ServiceReference{
						Namespace: "serviceNamespace",
						Name:      "serviceName",
					},
				},
			}

			apiServices = append(apiServices, apiservice)
		}
	}

	aggregatedManager := newDiscoveryManager(aggyService)

	for _, s := range apiServices {
		aggregatedManager.AddAPIService(s, service)
	}

	testCtx, testCancel := context.WithCancel(context.Background())
	defer testCancel()

	go aggregatedManager.Run(testCtx.Done(), nil)

	// 在 Run 启动之后才删除，确保删除事件走的是运行中的协调路径，
	// 而不是被启动时的初始全量同步顺带处理掉。
	for _, s := range apiServices {
		aggregatedManager.RemoveAPIService(s.Name)
	}

	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	response, _, parsed := fetchPath(aggyService, "")
	if response.StatusCode != 200 {
		t.Fatalf("unexpected status code %d", response.StatusCode)
	}
	if len(parsed.Items) > 0 {
		t.Errorf("expected to find no groups after service deletion (got %d groups)", len(parsed.Items))
	}
}

// 老后端只支持 /apis 和 /apis/<group>/<version> 的 v1 发现文档，
// 这个用例模拟它们，验证聚合器能把旧格式转换成 v2 发现结果。
func TestLegacyFallbackNoCache(t *testing.T) {
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")
	rootAPIsHandler := discovery.NewRootAPIsHandler(discovery.DefaultAddresses{DefaultAddress: "192.168.1.1"}, scheme.Codecs)

	legacyGroupHandler := discovery.NewAPIGroupHandler(scheme.Codecs, metav1.APIGroup{
		Name: "stable.example.com",
		PreferredVersion: metav1.GroupVersionForDiscovery{
			GroupVersion: "stable.example.com/v1",
			Version:      "v1",
		},
		Versions: []metav1.GroupVersionForDiscovery{
			{
				GroupVersion: "stable.example.com/v1",
				Version:      "v1",
			},
			{
				GroupVersion: "stable.example.com/v1beta1",
				Version:      "v1beta1",
			},
			{
				GroupVersion: "stable.example.com/v1alpha1",
				Version:      "v1alpha1",
			},
			{
				GroupVersion: "stable.example.com/v2alpha1",
				Version:      "v2alpha1",
			},
		},
	})

	generateVersionResource := func(version string) metav1.APIResource {
		return metav1.APIResource{
			Name:         "foos",
			SingularName: "foo",
			Group:        "stable.example.com",
			Version:      version,
			Namespaced:   false,
			Kind:         "Foo",
			Verbs:        []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"},
			Categories:   []string{"all"},
		}
	}

	resources := map[string]metav1.APIResource{
		"v1":       generateVersionResource("v1"),
		"v1beta1":  generateVersionResource("v1beta1"),
		"v1alpha1": generateVersionResource("v1alpha1"),
	}

	legacyResourceHandlerV1 := discovery.NewAPIVersionHandler(scheme.Codecs, schema.GroupVersion{
		Group:   "stable.example.com",
		Version: "v1",
	}, discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return []metav1.APIResource{
			resources["v1"],
		}
	}))

	legacyResourceHandlerV1Beta1 := discovery.NewAPIVersionHandler(scheme.Codecs, schema.GroupVersion{
		Group:   "stable.example.com",
		Version: "v1beta1",
	}, discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return []metav1.APIResource{
			resources["v1beta1"],
		}
	}))

	legacyResourceHandlerV1Alpha1 := discovery.NewAPIVersionHandler(scheme.Codecs, schema.GroupVersion{
		Group:   "stable.example.com",
		Version: "v1alpha1",
	}, discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return []metav1.APIResource{
			resources["v1alpha1"],
		}
	}))

	handlerFunc := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apis/stable.example.com" {
			legacyGroupHandler.ServeHTTP(w, r)
		} else if r.URL.Path == "/apis/stable.example.com/v1" {
			// defer to legacy discovery
			legacyResourceHandlerV1.ServeHTTP(w, r)
		} else if r.URL.Path == "/apis/stable.example.com/v1beta1" {
			// defer to legacy discovery
			legacyResourceHandlerV1Beta1.ServeHTTP(w, r)
		} else if r.URL.Path == "/apis/stable.example.com/v1alpha1" {
			legacyResourceHandlerV1Alpha1.ServeHTTP(w, r)
		} else if r.URL.Path == "/apis/stable.example.com/v2alpha1" {
			// serve the most minimal discovery doc that could have worked prior to aggregated discovery
			// 故意只给资源名和子资源名，不给 ResponseKind：这正是老文档最常缺的字段，
			// 聚合后应该被补成空的 GroupVersionKind，而不是整条丢弃。
			json.NewEncoder(w).Encode(&metav1.APIResourceList{
				GroupVersion: "stable.example.com/v2alpha1",
				APIResources: []metav1.APIResource{
					{Name: "parent-without-kind"},
					{Name: "missing-parent/subresource-without-parent", Kind: "SubresourceWithoutParent"},
					{Name: "parent-without-kind/subresource", Kind: "Subresource"},
					{Name: "parent-without-kind/subresource-without-kind"},
				},
			})
		} else if r.URL.Path == "/apis" {
			rootAPIsHandler.ServeHTTP(w, r)
		} else {
			// Unknown url
			// 聚合器只应访问已知路径；多出来的请求说明回退逻辑找错了端点，
			// 直接 fail 比默默返回空文档更容易定位问题。
			t.Fatalf("unexpected request sent to %v", r.URL.Path)
		}
	})

	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, handlerFunc)
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1beta1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1beta1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, handlerFunc)
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1alpha1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1alpha1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, handlerFunc)
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v2alpha1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v2alpha1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, handlerFunc)

	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// At this point external services have synced. Check if discovery document
	// includes the legacy resources
	_, _, doc := fetchPath(aggregatedResourceManager, "")

	// 期望值也走一次与生产代码相同的转换函数，测试重点才是
	// “老文档有没有被读到并合并”，而不是重复实现资源字段映射规则。
	mustConvert := func(r []metav1.APIResource) []apidiscoveryv2.APIResourceDiscovery {
		converted, err := endpoints.ConvertGroupVersionIntoToDiscovery(r)
		require.NoError(t, err)
		return converted
	}
	expectAggregatedDiscovery := []apidiscoveryv2.APIGroupDiscovery{{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stable.example.com",
		},
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{
				Version:   "v1",
				Resources: mustConvert([]metav1.APIResource{resources["v1"]}),
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			},
			{
				Version:   "v1beta1",
				Resources: mustConvert([]metav1.APIResource{resources["v1beta1"]}),
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			},
			{
				Version: "v2alpha1",
				Resources: []apidiscoveryv2.APIResourceDiscovery{
					{
						Resource:     "parent-without-kind",
						ResponseKind: &metav1.GroupVersionKind{}, // defaulted
						Scope:        "Cluster",
						Subresources: []apidiscoveryv2.APISubresourceDiscovery{
							{
								Subresource:  "subresource",
								ResponseKind: &metav1.GroupVersionKind{Kind: "Subresource"},
							},
							{
								Subresource:  "subresource-without-kind",
								ResponseKind: &metav1.GroupVersionKind{}, // defaulted
							},
						},
					},
					{
						Resource:     "missing-parent",
						ResponseKind: &metav1.GroupVersionKind{}, // defaulted
						Scope:        "Cluster",
						Subresources: []apidiscoveryv2.APISubresourceDiscovery{
							{
								Subresource:  "subresource-without-parent",
								ResponseKind: &metav1.GroupVersionKind{Kind: "SubresourceWithoutParent"},
							},
						},
					},
				},
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			},
			{
				Version:   "v1alpha1",
				Resources: mustConvert([]metav1.APIResource{resources["v1alpha1"]}),
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			},
		},
	}}
	require.Equal(t, doc.Items, expectAggregatedDiscovery)
}

// 把根 /apis 的响应做成参数：同一个后端发现流程需要面对多种“不支持 v2”的回答，
// 参数化比复制多份几乎相同的测试更容易维护。
func testLegacyFallbackWithCustomRootHandler(t *testing.T, rootHandlerFn func(http.ResponseWriter, *http.Request)) {
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")

	legacyGroupHandler := discovery.NewAPIGroupHandler(scheme.Codecs, metav1.APIGroup{
		Name: "stable.example.com",
		PreferredVersion: metav1.GroupVersionForDiscovery{
			GroupVersion: "stable.example.com/v1",
			Version:      "v1",
		},
		Versions: []metav1.GroupVersionForDiscovery{
			{
				GroupVersion: "stable.example.com/v1",
				Version:      "v1",
			},
			{
				GroupVersion: "stable.example.com/v1beta1",
				Version:      "v1beta1",
			},
		},
	})

	resource := metav1.APIResource{
		Name:         "foos",
		SingularName: "foo",
		Group:        "stable.example.com",
		Version:      "v1",
		Namespaced:   false,
		Kind:         "Foo",
		Verbs:        []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"},
		Categories:   []string{"all"},
	}

	legacyResourceHandler := discovery.NewAPIVersionHandler(scheme.Codecs, schema.GroupVersion{
		Group:   "stable.example.com",
		Version: "v1",
	}, discovery.APIResourceListerFunc(func() []metav1.APIResource {
		return []metav1.APIResource{
			resource,
		}
	}))

	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/apis/stable.example.com" {
			legacyGroupHandler.ServeHTTP(w, r)
		} else if r.URL.Path == "/apis/stable.example.com/v1" {
			// defer to legacy discovery
			legacyResourceHandler.ServeHTTP(w, r)
		} else if r.URL.Path == "/apis" {
			rootHandlerFn(w, r)
		} else {
			// Unknown url
			t.Fatalf("unexpected request sent to %v", r.URL.Path)
		}
	}))
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// At this point external services have synced. Check if discovery document
	// includes the legacy resources
	_, _, doc := fetchPath(aggregatedResourceManager, "")

	converted, err := endpoints.ConvertGroupVersionIntoToDiscovery([]metav1.APIResource{resource})
	require.NoError(t, err)
	require.Equal(t, []apidiscoveryv2.APIGroupDiscovery{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: resource.Group,
			},
			Versions: []apidiscoveryv2.APIVersionDiscovery{
				{
					Version:   resource.Version,
					Resources: converted,
					Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
				},
			},
		},
	}, doc.Items)
}

// 覆盖几种典型的“后端没有聚合发现文档”响应：406、404、空响应和错误的
// Content-Type。只要不是明确声明了 v2 文档，就该静默退回老格式解析。
func TestLegacyFallback(t *testing.T) {
	rootAPIsHandler := discovery.NewRootAPIsHandler(discovery.DefaultAddresses{DefaultAddress: "192.168.1.1"}, scheme.Codecs)
	testCases := []struct {
		name        string
		rootHandler func(http.ResponseWriter, *http.Request)
	}{
		{
			name:        "Default root handler (406)",
			rootHandler: rootAPIsHandler.ServeHTTP,
		},
		{
			name: "Root handler with non 200 status code",
			rootHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(404)
			},
		},
		{
			name: "Root handler with 200 response code no content type",
			rootHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)
			},
		},
		{
			name: "Root handler with 200 response code incorrect content type",
			rootHandler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json;g=apidiscovery.k8s.io;v=v1alpha1;as=APIGroupDiscoveryList")
				w.WriteHeader(200)
			},
		},
	}
	for _, tc := range testCases {
		testLegacyFallbackWithCustomRootHandler(t, tc.rootHandler)
	}
}

// 后端返回 503 时不能让它从发现文档里消失：客户端会据此判断 API 是否存在，
// 短暂故障应该表现为 Stale，保留组信息等待下一次同步恢复。
func TestAPIServiceStale(t *testing.T) {
	aggregatedResourceManager := discoveryendpoint.NewResourceManager("apis")
	aggregatedManager := newDiscoveryManager(aggregatedResourceManager)
	aggregatedManager.AddAPIService(&apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "v1.stable.example.com",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:   "stable.example.com",
			Version: "v1",
			Service: &apiregistrationv1.ServiceReference{
				Name: "test-service",
			},
		},
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	testCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// At this point external services have synced. Check if discovery document
	// lists the APIService group version as Stale.
	_, _, doc := fetchPath(aggregatedResourceManager, "")
	require.Equal(t, []apidiscoveryv2.APIGroupDiscovery{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "stable.example.com",
			},
			Versions: []apidiscoveryv2.APIVersionDiscovery{
				{
					Version:   "v1",
					Freshness: apidiscoveryv2.DiscoveryFreshnessStale,
				},
			},
		},
	}, doc.Items)
}

// Exercises the 304 Not Modified Path of the aggregator
// This path in 1.26.0 would result in a deadlock if an aggregated APIService
// returned a 304 Not Modified response for its own aggregated discovery document.
// 测试通过分批注册 APIService 来构造“先有缓存、后端再回 304”的顺序，
// 回归会表现为等待超时，而不是断言失败，所以设了 5 秒超时兜底。
func TestNotModified(t *testing.T) {
	aggyService := discoveryendpoint.NewResourceManager("apis")
	service := discoveryendpoint.NewResourceManager("apis")
	apiGroup := fuzzAPIGroups(2, 3, 10)
	service.SetGroups(apiGroup.Items)

	var apiServices []*apiregistrationv1.APIService
	for _, g := range apiGroup.Items {
		for _, v := range g.Versions {
			apiservice := &apiregistrationv1.APIService{
				ObjectMeta: metav1.ObjectMeta{
					Name: v.Version + "." + g.Name,
				},
				Spec: apiregistrationv1.APIServiceSpec{
					Group:   g.Name,
					Version: v.Version,
					Service: &apiregistrationv1.ServiceReference{
						Namespace: "serviceNamespace",
						Name:      "serviceName",
					},
				},
			}

			apiServices = append(apiServices, apiservice)
		}
	}

	aggregatedManager := newDiscoveryManager(aggyService)
	// Add all except the last group.
	// Ensure this is done BEFORE the call to run, so they are included in initial
	// list to keep test focused
	for _, s := range apiServices[:len(apiServices)-1] {
		aggregatedManager.AddAPIService(s, service)
	}

	testCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go aggregatedManager.Run(testCtx.Done(), nil)

	// Important to wait here to ensure we prime the cache with the initial list
	// of documents in order to exercise 304 Not Modified
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))

	// Now add all groups. We excluded one group before so that waitForQueueIsComplete
	// could include it in this round. Now, if waitForQueueIsComplete ever returns
	// true, it must have synced all the pre-existing groups before, which would
	// return 304 Not Modified
	for _, s := range apiServices {
		aggregatedManager.AddAPIService(s, service)
	}

	// This would wait the full timeout on 1.26.0.
	require.True(t, waitForQueueComplete(testCtx.Done(), aggregatedManager))
}

// copied from staging/src/k8s.io/apiserver/pkg/endpoints/discovery/v2/handler_test.go
// 用固定 seed 保证失败可复现；填完随机值后要把 TypeMeta、ObjectMeta 和 Freshness
// 收敛到合法范围，否则 fuzz 出来的“不合法文档”会让测试失败在校验层而非聚合逻辑。
func fuzzAPIGroups(atLeastNumGroups, maxNumGroups int, seed int64) apidiscoveryv2.APIGroupDiscoveryList {
	fuzzer := randfill.NewWithSeed(seed)
	fuzzer.NumElements(atLeastNumGroups, maxNumGroups)
	fuzzer.NilChance(0)
	fuzzer.Funcs(func(o *apidiscoveryv2.APIGroupDiscovery, c randfill.Continue) {
		c.FillNoCustom(o)

		// The ResourceManager will just not serve the group if its versions
		// list is empty
		atLeastOne := apidiscoveryv2.APIVersionDiscovery{}
		c.Fill(&atLeastOne)
		o.Versions = append(o.Versions, atLeastOne)

		// clear invalid fuzzed values
		o.TypeMeta = metav1.TypeMeta{}
		// truncate object meta to just name
		o.ObjectMeta = metav1.ObjectMeta{Name: o.ObjectMeta.Name}
		// fix version freshness value, make versions unique and non-empty
		for i := range o.Versions {
			o.Versions[i].Freshness = "Current"
			o.Versions[i].Version = fmt.Sprintf("v%d", i+1)
		}
	})

	var apis []apidiscoveryv2.APIGroupDiscovery
	fuzzer.Fill(&apis)

	return apidiscoveryv2.APIGroupDiscoveryList{
		TypeMeta: metav1.TypeMeta{
			Kind:       "APIGroupDiscoveryList",
			APIVersion: "v1",
		},
		Items: apis,
	}

}

// copied from staging/src/k8s.io/apiserver/pkg/endpoints/discovery/v2/handler_test.go
// 直接构造带 v2 Accept 头的请求，模拟真实客户端的协商过程；只检查解码后的对象，
// 因为内容协商的处理属于另一个测试职责。
func fetchPath(handler http.Handler, etag string) (*http.Response, []byte, *apidiscoveryv2.APIGroupDiscoveryList) {
	// Expect json-formatted apis group list
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/apis", nil)

	// Ask for JSON response
	req.Header.Set("Accept", runtime.ContentTypeJSON+";g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList,"+runtime.ContentTypeJSON+";g=apidiscovery.k8s.io;v=v2beta1;as=APIGroupDiscoveryList")

	if etag != "" {
		// Quote provided etag if unquoted
		quoted := etag
		if !strings.HasPrefix(etag, "\"") {
			quoted = strconv.Quote(etag)
		}
		req.Header.Set("If-None-Match", quoted)
	}

	handler.ServeHTTP(w, req)

	bytes := w.Body.Bytes()
	var decoded *apidiscoveryv2.APIGroupDiscoveryList
	if len(bytes) > 0 {
		decoded = &apidiscoveryv2.APIGroupDiscoveryList{}
		runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), bytes, decoded)
	}

	return w.Result(), bytes, decoded
}

// completerWorkqueue is a workqueue.RateLimitingInterface that implements
// isComplete
// 生产用的限速队列只暴露 Len，无法区分“空闲”和“正在处理中”；
// 这里额外用 processing 集合跟踪已取出但未 Done 的键，让测试能精确等待协调结束。
type completerWorkqueue struct {
	lock sync.Mutex
	workqueue.TypedRateLimitingInterface[string]
	processing map[string]struct{}
}

var _ = workqueue.TypedRateLimitingInterface[string](&completerWorkqueue{})

func newCompleterWorkqueue(wq workqueue.TypedRateLimitingInterface[string]) *completerWorkqueue {
	return &completerWorkqueue{
		TypedRateLimitingInterface: wq,
		processing:                 make(map[string]struct{}),
	}
}

func (q *completerWorkqueue) Add(item string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.processing[item] = struct{}{}
	q.TypedRateLimitingInterface.Add(item)
}

func (q *completerWorkqueue) AddAfter(item string, duration time.Duration) {
	// Normally apiservices are continuously re-synced and never leave the
	// queue. Make this a no-op in the unit test so that isComplete()
	// reflects whether all explicitly dirtied items have been processed.
	// 延迟入队会让队列永远有“未来工作”，isComplete 就再也不会变真；
	// 单测只关心显式标记过的对象，因此这里刻意丢弃延迟请求。
}

func (q *completerWorkqueue) AddRateLimited(item string) {
	q.Add(item)
}

func (q *completerWorkqueue) Done(item string) {
	q.lock.Lock()
	defer q.lock.Unlock()
	delete(q.processing, item)
	q.TypedRateLimitingInterface.Done(item)
}

// 只有待处理队列为空且没有正在处理的键时才算完成。两段状态需要同一把锁，
// 否则工作者在 Done 与下层队列更新之间可能被误判为已完成。
func (q *completerWorkqueue) isComplete() bool {
	q.lock.Lock()
	defer q.lock.Unlock()
	return q.Len() == 0 && len(q.processing) == 0
}
