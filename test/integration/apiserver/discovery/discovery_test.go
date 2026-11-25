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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	kubernetes "k8s.io/client-go/kubernetes"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	aggregator "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	aggregatorclientsetscheme "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/scheme"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"

	"k8s.io/kubernetes/test/integration/framework"
)

type kubeClientSet = kubernetes.Interface

type aggegatorClientSet = aggregator.Interface

type apiextensionsClientSet = apiextensions.Interface

type dynamicClientset = dynamic.Interface
type testClientSet struct {
	kubeClientSet
	aggegatorClientSet
	apiextensionsClientSet
	dynamicClientset
}

var _ testClient = testClientSet{}

func (t testClientSet) Discovery() discovery.DiscoveryInterfaces {
	return t.kubeClientSet.Discovery()
}

var (
	scheme    = runtime.NewScheme()
	codecs    = runtimeserializer.NewCodecFactory(scheme)
	serialize runtime.NegotiatedSerializer

	basicTestGroup = apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stable.example.com",
		},
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{
				Version: "v1",
				Resources: []apidiscoveryv2.APIResourceDiscovery{
					{
						Resource:   "jobs",
						Verbs:      []string{"create", "list", "watch", "delete"},
						ShortNames: []string{"jz"},
						Categories: []string{"all"},
					},
				},
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			},
		},
	}

	basicTestGroupWithFixup = apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stable.example.com",
		},
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{
				Version: "v1",
				Resources: []apidiscoveryv2.APIResourceDiscovery{
					{
						Resource:   "jobs",
						Verbs:      []string{"create", "list", "watch", "delete"},
						ShortNames: []string{"jz"},
						Categories: []string{"all"},
						// aggregator 会为此填充一个非 nil 值
						ResponseKind: &metav1.GroupVersionKind{},
					},
				},
				Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
			},
		},
	}

	basicTestGroupStale = apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{
			Name: "stable.example.com",
		},
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{
				Version:   "v1",
				Freshness: apidiscoveryv2.DiscoveryFreshnessStale,
			},
		},
	}

	stableGroup    = "stable.example.com"
	stableV1       = metav1.GroupVersion{Group: stableGroup, Version: "v1"}
	stableV1alpha1 = metav1.GroupVersion{Group: stableGroup, Version: "v1alpha1"}
	stableV1alpha2 = metav1.GroupVersion{Group: stableGroup, Version: "v1alpha2"}
	stableV1beta1  = metav1.GroupVersion{Group: stableGroup, Version: "v1beta1"}
	stableV2       = metav1.GroupVersion{Group: stableGroup, Version: "v2"}
)

func init() {
	// 将所有内置类型添加到 scheme
	utilruntime.Must(k8sscheme.AddToScheme(scheme))
	utilruntime.Must(aggregatorclientsetscheme.AddToScheme(scheme))
	utilruntime.Must(apiextensionsv1.AddToScheme(scheme))

	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	if !ok {
		panic("failed to create serializer info")
	}

	serialize = runtime.NewSimpleNegotiatedSerializer(info)
}

// 启动一个 api server，并在测试结束时清理
// 返回一些 kubernetes clients
func setup(t *testing.T) (context.Context, testClientSet, context.CancelFunc) {
	ctx, cancelCtx := context.WithCancel(context.Background())

	server := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)

	cfg := rest.CopyConfig(server.ClientConfig)
	// Discovery 集成测试可能非常频繁地轮询，不应因客户端限流而产生不稳定。
	cfg.QPS = 50
	cfg.Burst = 100
	cfg.RateLimiter = nil

	kubeClientSet, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)

	aggegatorClientSet, err := aggregator.NewForConfig(cfg)
	require.NoError(t, err)

	apiextensionsClientSet, err := apiextensions.NewForConfig(cfg)
	require.NoError(t, err)

	dynamicClientset, err := dynamic.NewForConfig(cfg)
	require.NoError(t, err)

	client := testClientSet{
		kubeClientSet:          kubeClientSet,
		aggegatorClientSet:     aggegatorClientSet,
		apiextensionsClientSet: apiextensionsClientSet,
		dynamicClientset:       dynamicClientset,
	}
	return ctx, client, cancelCtx
}

func TestReadinessAggregatedAPIServiceDiscovery(t *testing.T) {
	// 防止本测试执行结束后仍有 goroutine 在运行
	ctx, client, cleanup := setup(t)
	defer cleanup()

	// 创建一个 resource manager 来提供我们的 GroupVersion
	resourceManager := discoveryendpoint.NewResourceManager("apis")
	resourceManager.SetGroups([]apidiscoveryv2.APIGroupDiscovery{basicTestGroup})

	apiServiceWaitCh := make(chan struct{})

	// 将我们的 ResourceManager 作为 Aggregated APIService
	// 安装到测试 server
	service := NewFakeService("test-server", client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/apis/stable.example.com") {
			// 返回无效响应，以便 APIService 能被标记为 "available"
			w.WriteHeader(http.StatusOK)
		} else if strings.HasPrefix(r.URL.Path, "/apis") {
			select {
			case <-apiServiceWaitCh:
				// 暂停响应 discovery，直到 aggregated discovery 文档将聚合组标记为 Stale。
				resourceManager.ServeHTTP(w, r)
			case <-ctx.Done():
				return
			}
		} else {
			// 拒绝 openapi/v2、openapi/v3、apis/<group>/<version>
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	go func() {
		if err := service.Run(ctx); err != nil {
			t.Errorf("unexpected error %v", err)
		}
	}()
	require.NoError(t, service.WaitForReady(ctx))

	// 为 resourcemanager 提供的每个 groupversion 创建一个
	// 连接到我们伪造 APIServer 的 APIService 对象
	for _, versionInfo := range basicTestGroup.Versions {
		groupVersion := metav1.GroupVersion{
			Group:   basicTestGroup.Name,
			Version: versionInfo.Version,
		}

		require.NoError(t, registerAPIService(ctx, client, groupVersion, service))
	}

	// 持续从 aggregator 获取文档。
	// 检查它最初是否将聚合组包含为 stale
	require.NoError(t, WaitForGroups(ctx, client, basicTestGroupStale))
	require.NoError(t, WaitForRootPaths(t, ctx, client, sets.New("/apis/"+basicTestGroup.Name), nil))

	// 允许 APIService 开始响应，并确保 APIService 可访问时 Freshness 会更新。
	close(apiServiceWaitCh)
	require.NoError(t, WaitForGroups(ctx, client, basicTestGroupWithFixup))
}

func registerAPIService(ctx context.Context, client aggregator.Interface, gv metav1.GroupVersion, service FakeService) error {
	port := service.Port()
	if port == nil {
		return errors.New("service not yet started")
	}
	// 注册 APIService
	patch := apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{
			Name: gv.Version + "." + gv.Group,
		},
		TypeMeta: metav1.TypeMeta{
			Kind:       "APIService",
			APIVersion: "apiregistration.k8s.io/v1",
		},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                 gv.Group,
			Version:               gv.Version,
			InsecureSkipTLSVerify: true,
			GroupPriorityMinimum:  1000,
			VersionPriority:       15,
			Service: &apiregistrationv1.ServiceReference{
				Namespace: "default",
				Name:      service.Name(),
				Port:      port,
			},
		},
	}

	_, err := client.
		ApiregistrationV1().
		APIServices().
		Create(context.TODO(), &patch, metav1.CreateOptions{FieldManager: "test-manager"})
	return err
}

func unregisterAPIService(ctx context.Context, client aggregator.Interface, gv metav1.GroupVersion) error {
	return client.ApiregistrationV1().APIServices().Delete(ctx, gv.Version+"."+gv.Group, metav1.DeleteOptions{})
}

func TestAggregatedAPIServiceDiscovery(t *testing.T) {
	// 防止本测试执行结束后仍有 goroutine 在运行
	ctx, client, cleanup := setup(t)
	defer cleanup()

	// 创建一个 resource manager 来提供我们的 GroupVersion
	resourceManager := discoveryendpoint.NewResourceManager("apis")
	resourceManager.SetGroups([]apidiscoveryv2.APIGroupDiscovery{basicTestGroup})

	// 将我们的 ResourceManager 作为 Aggregated APIService
	// 安装到测试 server
	service := NewFakeService("test-server", client, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/apis") {
			resourceManager.ServeHTTP(w, r)
		} else if strings.HasPrefix(r.URL.Path, "/apis/stable.example.com") {
			// 返回无效响应，以便 APIService 能被标记为 "available"
			w.WriteHeader(http.StatusOK)
		} else {
			// 拒绝 openapi/v2、openapi/v3、apis/<group>/<version>
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	go func() {
		if err := service.Run(ctx); err != nil {
			t.Errorf("unexpected error %v", err)
		}
	}()
	require.NoError(t, service.WaitForReady(ctx))

	// 为 resourcemanager 提供的每个 groupversion 创建一个
	// 连接到我们伪造 APIServer 的 APIService 对象
	var groupVersions []metav1.GroupVersion
	for _, versionInfo := range basicTestGroup.Versions {
		groupVersion := metav1.GroupVersion{
			Group:   basicTestGroup.Name,
			Version: versionInfo.Version,
		}

		require.NoError(t, registerAPIService(ctx, client, groupVersion, service))
		groupVersions = append(groupVersions, groupVersion)
	}

	// 持续从 aggregator 获取文档。
	// 检查它在合理时间内是否包含我们的 service
	require.NoError(t, WaitForGroups(ctx, client, basicTestGroupWithFixup))
	require.NoError(t, WaitForRootPaths(t, ctx, client, sets.New("/apis/"+basicTestGroup.Name), nil))

	// 取消注册，并确保该 group 从 root paths 中移除
	for _, groupVersion := range groupVersions {
		require.NoError(t, unregisterAPIService(ctx, client, groupVersion))
	}
	require.NoError(t, WaitForRootPaths(t, ctx, client, nil, sets.New("/apis/"+basicTestGroup.Name)))
}

func runTestCases(t *testing.T, cases []testCase) {
	// 防止本测试执行结束后仍有 goroutine 在运行
	ctx, client, cleanup := setup(t)
	defer cleanup()

	// 获取原始 discovery 信息，以便在各测试之间
	// 等待其重置
	originalV1, err := FetchV1DiscoveryGroups(ctx, client)
	require.NoError(t, err)

	originalV2, err := FetchV2Discovery(ctx, client)
	require.NoError(t, err)

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			func() {
				testContext, testDone := context.WithCancel(ctx)
				defer testDone()

				for i, a := range c.Actions {
					if cleaning, ok := a.(cleaningAction); ok {
						defer func() {
							require.NoError(t, cleaning.Cleanup(testContext, client), "cleanup after \"%T\" step %v", a, i)
						}()
					}
					require.NoError(t, a.Do(testContext, client), "running \"%T\" step %v", a, i)
				}
			}()

			var diff string
			err := WaitForV1GroupsWithCondition(ctx, client, func(result metav1.APIGroupList) bool {
				diff = cmp.Diff(originalV1, result)
				return reflect.DeepEqual(result, originalV1)
			})
			require.NoError(t, err, "v1 discovery must reset between tests: "+diff)

			err = WaitForResultWithCondition(ctx, client, func(result apidiscoveryv2.APIGroupDiscoveryList) bool {
				diff = cmp.Diff(originalV2, result)
				return reflect.DeepEqual(result, originalV2)
			})
			require.NoError(t, err, "v2 discovery must reset between tests: "+diff)
		})
	}
}

// 针对 CRD 集成的声明式测试
func TestCRD(t *testing.T) {
	runTestCases(t, []testCase{
		{
			// 验证添加 CRD 后，它会在合理时间内
			// 被包含在 discovery 文档中
			Name: "CRDInclusion",
			Actions: []testAction{
				applyCRD(makeCRDSpec(stableGroup, "Foo", false, []string{"v1", "v1alpha1", "v1beta1", "v2"})),
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
			},
		},
		{
			// 验证添加到 discovery 文档中的 CRD 也可以被移除
			Name: "CRDRemoval",
			Actions: []testAction{
				applyCRD(makeCRDSpec(stableGroup, "Foo", false, []string{"v1", "v1alpha1", "v1beta1", "v2"})),
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
				deleteObject{
					GroupVersionResource: metav1.GroupVersionResource(apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions")),
					Name:                 "foos.stable.example.com",
				},
				waitForAbsentGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
				waitForAbsentGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
			},
		},
		{
			// 验证如果 CRD 与 APIService 共享 groupversion，
			// 且 APIService 被删除、CRD 得到更新，
			// APIService 仍保留在 discovery 中。
			// 本测试模拟 CRD controller 的重新同步，
			// 以表明最终 APIService 会被重新创建
			Name: "CRDAPIServiceOverlap",
			Actions: []testAction{
				applyAPIService(
					apiregistrationv1.APIServiceSpec{
						Group:                 stableGroup,
						Version:               "v1",
						InsecureSkipTLSVerify: true,
						GroupPriorityMinimum:  int32(1000),
						VersionPriority:       int32(15),
						Service: &apiregistrationv1.ServiceReference{
							Name:      "unused",
							Namespace: "default",
						},
					},
				),

				// 等待 GV 出现在两个 discovery 文档中
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1}),

				applyCRD(makeCRDSpec(stableGroup, "Bar", false, []string{"v1", "v2"})),

				// 验证存在 v1 和 v2，但 v1 是 stale
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV2}),
				waitForStaleGroupVersionsV2([]metav1.GroupVersion{stableV1}),
				waitForFreshGroupVersionsV2([]metav1.GroupVersion{stableV2}),

				// 删除 aggregated apiservice 与 CRD
				// 共享的 APIService
				deleteObject{
					GroupVersionResource: metav1.GroupVersionResource(apiregistrationv1.SchemeGroupVersion.WithResource("apiservices")),
					Name:                 "v1.stable.example.com",
				},

				// 通过添加 category 和新的 groupversion 来更新 CRD，以触发重新同步
				applyCRD(makeCRDSpec(stableGroup, "Bar", false, []string{"v1", "v2", "v1alpha1"}, "all")),

				// 验证该 groupversion 被重新添加回来
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV2, stableV1alpha1}),
				waitForFreshGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV2, stableV1alpha1}),
			},
		},
		{
			// 验证如果 CRD 与 Aggregated APIService 共享 groupversion，
			// aggregated apiservice 的 discovery 信息会同时显示在
			// v1 和 v2 discovery 中
			Name: "CRDAPIServiceSameGroupDifferentVersions",
			Actions: []testAction{
				// 等待 CRD 生效
				applyCRD(makeCRDSpec(stableGroup, "Bar", false, []string{"v2", "v1alpha1"})),
				// 等待 GV 出现在两个 discovery 文档中
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV2, stableV1alpha1}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV2, stableV1alpha1}),

				applyAPIService(
					apiregistrationv1.APIServiceSpec{
						Group:                 stableGroup,
						Version:               "v1",
						InsecureSkipTLSVerify: true,
						GroupPriorityMinimum:  int32(1000),
						VersionPriority:       int32(100),
						Service: &apiregistrationv1.ServiceReference{
							Name:      "unused",
							Namespace: "default",
						},
					},
				),

				// 现在应该有 stable v1 可用
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1}),

				// aggregated apiservice 不提供的
				// CRD group-version 仍应可用
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV2, stableV1alpha1}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV2, stableV1alpha1}),

				// 移除 API service。验证已切换到 CRD
				deleteObject{
					GroupVersionResource: metav1.GroupVersionResource(apiregistrationv1.SchemeGroupVersion.WithResource("apiservices")),
					Name:                 "v1.stable.example.com",
				},

				// 验证由于 CRD 中包含 stable v1，它仍然存在
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV2, stableV1alpha1}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV2, stableV1alpha1}),

				waitForAbsentGroupVersionsV1([]metav1.GroupVersion{stableV1}),
				waitForAbsentGroupVersionsV2([]metav1.GroupVersion{stableV1}),
			},
		},
		{
			// 验证如果 CRD 与内置类型共享 group version，
			// 内置类型在两个版本的 discovery 中都优先
			Name: "CRDBuiltinOverlapPrecence",
			Actions: []testAction{
				// 创建覆盖内置类型的 CRD
				applyCRD(makeCRDSpec("apiextensions.k8s.io", "Bar", true, []string{"v1", "v2", "vfake"})),

				waitForGroupVersionsV1([]metav1.GroupVersion{{Group: "apiextensions.k8s.io", Version: "vfake"}}),
				waitForGroupVersionsV2([]metav1.GroupVersion{{Group: "apiextensions.k8s.io", Version: "vfake"}}),

				// 验证 V1 仍使用内置 group-version：
				// 显示存在 v1.CustomResourceDefinition，
				// 且不存在 v1.Bar
				waitForResourcesV1([]metav1.GroupVersionResource{
					{
						Group:    "apiextensions.k8s.io",
						Version:  "v1",
						Resource: "customresourcedefinitions",
					},
					{
						Group:    "apiextensions.k8s.io",
						Version:  "vfake",
						Resource: "bars",
					},
				}),
				waitForResourcesV2([]metav1.GroupVersionResource{
					{
						Group:    "apiextensions.k8s.io",
						Version:  "v1",
						Resource: "customresourcedefinitions",
					},
					{
						Group:    "apiextensions.k8s.io",
						Version:  "vfake",
						Resource: "bars",
					},
				}),

				waitForResourcesAbsentV1([]metav1.GroupVersionResource{
					{
						Group:    "apiextensions.k8s.io",
						Version:  "v1",
						Resource: "bars",
					},
				}),
				waitForResourcesAbsentV2([]metav1.GroupVersionResource{
					{
						Group:    "apiextensions.k8s.io",
						Version:  "v1",
						Resource: "bars",
					},
				}),
			},
		},
		{
			// 测试在该功能 alpha 阶段发现的竞态问题已修复。
			// 如果 CRD 在 aggregated APIService 的移除完成同步之前
			// 就完成同步，则很少会发生竞态。
			// 测试方法：
			//  1. 向 apiserver 添加 CRD
			// 	2. 等待它完成同步
			//  3. 添加具有相同 groupversion 的 aggregated APIService
			//  4. 移除 aggregated apiservice
			//  5. 检查 discovery 文档中存在 CRD 的 GV
			// 验证如果 CRD 与 APIService 共享 groupversion，
			// 且 APIService 被删除、CRD 得到更新，
			// 来自 CRD 的 groupversion 仍保留在 discovery 中。
			Name: "Race",
			Actions: []testAction{
				// 创建与 aggregated APIService 具有相同 GV 的 CRD
				applyCRD(makeCRDSpec(stableGroup, "Bar", false, []string{"v1", "v2"})),

				// 只有 CRD 提供 stable v2，这表明 CRD 已完成同步
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV2}),

				// 添加与 CRD 重叠的 Aggregated APIService。
				applyAPIService(
					apiregistrationv1.APIServiceSpec{
						Group:                 stableGroup,
						Version:               "v1",
						InsecureSkipTLSVerify: true,
						GroupPriorityMinimum:  int32(1000),
						VersionPriority:       int32(100),
						Service: &apiregistrationv1.ServiceReference{
							Name:      "fake",
							Namespace: "default",
						},
					},
				),

				// 删除 aggregated apiservice 与 CRD
				// 共享的 APIService
				deleteObject{
					GroupVersionResource: metav1.GroupVersionResource(apiregistrationv1.SchemeGroupVersion.WithResource("apiservices")),
					Name:                 "v1.stable.example.com",
				},

				// 验证现在对外公布的是 CRD（带有 stablev2）
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV2}),
			},
		},
	})
}

func TestFreshness(t *testing.T) {
	requireStaleGVs := func(gvs ...metav1.GroupVersion) inlineAction {
		return inlineAction(func(ctx context.Context, client testClient) error {
			document, err := FetchV2Discovery(ctx, client)
			if err != nil {
				return nil
			}

			// 将 stale 的 gv 记录在数组中，以便测试失败时输出清晰的 diff
			staleGVs := []metav1.GroupVersion{}

			// 遍历输入，因此顺序无关紧要
			for _, targetGv := range gvs {
				entry := FindGroupVersionV2(document, targetGv)
				if entry == nil {
					continue
				}

				switch entry.Freshness {
				case apidiscoveryv2.DiscoveryFreshnessCurrent:
					// 跳过
				case apidiscoveryv2.DiscoveryFreshnessStale:
					staleGVs = append(staleGVs, targetGv)
				default:
					return fmt.Errorf("unrecognized freshness '%v' on gv '%v'", entry.Freshness, targetGv)
				}
			}

			if !(len(staleGVs) == 0 && len(gvs) == 0) && !reflect.DeepEqual(staleGVs, gvs) {
				diff := cmp.Diff(staleGVs, gvs)
				return fmt.Errorf("expected sets of stale gvs to be equal:\n%v", diff)
			}

			return nil
		})
	}

	runTestCases(t, []testCase{
		{
			Name: "BuiltinsFresh",
			Actions: []testAction{
				// 等待 discovery 就绪
				waitForGroupVersionsV2{metav1.GroupVersion(apiregistrationv1.SchemeGroupVersion)},
				// 要求不存在 stale 的 groupversion，也不存在无法识别的
				// GV
				requireStaleGVs(),
			},
		},
		{
			// CRD 的 freshness 始终是 current
			Name: "CRDFresh",
			Actions: []testAction{
				// 添加 CRD 并等待它出现在 discovery 中
				applyCRD(makeCRDSpec(stableGroup, "Foo", false, []string{"v1", "v1alpha1", "v1beta1", "v2"})),
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1beta1, stableV2}),

				// 通过要求没有 stale 项来测试 CRD 是 current
				requireStaleGVs(),
			},
		},
		{
			// 创建一个不可达的 aggregated APIService，验证
			// 其 groupversion 会以 stale 状态
			// 包含在 discovery 文档中
			Name: "AggregatedUnreachable",
			Actions: []testAction{
				applyAPIService{
					Group:                stableGroup,
					Version:              "v1",
					GroupPriorityMinimum: 1000,
					VersionPriority:      15,
					Service: &apiregistrationv1.ServiceReference{
						Name:      "doesnt-exist",
						Namespace: "default",
					},
				},
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1}),
				// 要求有且仅有一个 stale GV，即 stableV1
				requireStaleGVs(stableV1),
			},
		},
	})

}

// 展示当多个 APIService 为某个 group 指定 GroupPriorityMinimum 时，
// 它在两个版本的 discovery 中排序相同
func TestGroupPriority(t *testing.T) {
	makeApiServiceSpec := func(gv metav1.GroupVersion, groupPriorityMin, versionPriority int) apiregistrationv1.APIServiceSpec {
		return apiregistrationv1.APIServiceSpec{
			Group:                 gv.Group,
			Version:               gv.Version,
			InsecureSkipTLSVerify: true,
			GroupPriorityMinimum:  int32(groupPriorityMin),
			VersionPriority:       int32(versionPriority),
			Service: &apiregistrationv1.ServiceReference{
				Name:      "unused",
				Namespace: "default",
			},
		}
	}

	checkGVOrder := inlineAction(func(ctx context.Context, client testClient) (err error) {
		// 获取 v1 和 v2 文档，确保它们的 groupversion
		// 顺序相同，且没有缺失或
		// 多余的项。
		v1GroupsAndVersions, err := FetchV1DiscoveryGroups(ctx, client)
		if err != nil {
			return err
		}
		v2GroupsAndVersions, err := FetchV2Discovery(ctx, client)
		if err != nil {
			return err
		}

		v1Gvs := []metav1.GroupVersion{}
		v2Gvs := []metav1.GroupVersion{}

		for _, group := range v1GroupsAndVersions.Groups {
			for _, version := range group.Versions {
				v1Gvs = append(v1Gvs, metav1.GroupVersion{
					Group:   group.Name,
					Version: version.Version,
				})
			}
		}

		for _, group := range v2GroupsAndVersions.Items {
			for _, version := range group.Versions {
				v2Gvs = append(v2Gvs, metav1.GroupVersion{
					Group:   group.Name,
					Version: version.Version,
				})
			}
		}

		if !reflect.DeepEqual(v1Gvs, v2Gvs) {
			return fmt.Errorf("expected equal orderings and lists of groupversions in both v1 and v2 discovery:\n%v", cmp.Diff(v1Gvs, v2Gvs))
		}

		return nil
	})

	runTestCases(t, []testCase{
		{
			// 验证 legacy 和 aggregated discovery 文档具有相同
			// 的内置 groupversion 集合
			Name: "BuiltinsAndOrdering",
			Actions: []testAction{
				waitForGroupVersionsV1{metav1.GroupVersion(apiregistrationv1.SchemeGroupVersion)},
				waitForGroupVersionsV2{metav1.GroupVersion(apiregistrationv1.SchemeGroupVersion)},
				checkGVOrder,
			},
		},
		{
			// 验证优先级非常高的 group 排在首位（位于 apiregistration v1 之后）
			// 同时验证 v1 和 v2 discovery API 的排序相同
			// 不变更 version priority
			Name: "HighGroupPriority",
			Actions: []testAction{
				// 非常高的优先级，应当优先
				// 20000 是可能的最高优先级
				applyAPIService(makeApiServiceSpec(stableV1, 20000, 15)),
				// 非常低的优先级，应当被忽略
				applyAPIService(makeApiServiceSpec(stableV1alpha1, 1, 15)),
				// 中高优先级（与 k8s 冲突），应当被忽略
				applyAPIService(makeApiServiceSpec(stableV1alpha2, 17300, 15)),
				// 等待所有添加的 group-version 出现在两个 discovery 文档中
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1alpha2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1alpha2}),
				// 检查 v1 和 v2 endpoint 的 groupversion
				// 集合完全相同
				checkGVOrder,
				// 检查第一个 group-version 是优先级
				// 最高的那个
				inlineAction(func(ctx context.Context, client testClient) error {
					v2GroupsAndVersions, err := FetchV2Discovery(ctx, client)
					if err != nil {
						return err
					}

					// 第一个 group 应始终是 apiregistration.k8s.io
					secondGV := metav1.GroupVersion{
						Group:   v2GroupsAndVersions.Items[1].Name,
						Version: v2GroupsAndVersions.Items[1].Versions[0].Version,
					}

					if !reflect.DeepEqual(&stableV1, &secondGV) {
						return fmt.Errorf("expected second group's first version to be %v, not %v", stableV1, secondGV)
					}

					return nil
				}),
			},
		},
		{
			// 验证 group 优先级非常低的排在最后
			Name: "LowGroupPriority",
			Actions: []testAction{
				// 最低优先级
				applyAPIService(makeApiServiceSpec(stableV1alpha1, 1, 15)),
				// 等待所有添加的 group-version 出现在 v2 discovery 中
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1alpha1}),
				// 检查最后一个 group-version 是优先级
				// 最低的那个
				inlineAction(func(ctx context.Context, client testClient) error {
					v2GroupsAndVersions, err := FetchV2Discovery(ctx, client)
					if err != nil {
						return err
					}
					lastGroup := v2GroupsAndVersions.Items[len(v2GroupsAndVersions.Items)-1]

					lastGV := metav1.GroupVersion{
						Group:   lastGroup.Name,
						Version: lastGroup.Versions[0].Version,
					}

					if !reflect.DeepEqual(&stableV1alpha1, &lastGV) {
						return fmt.Errorf("expected last group to be %v, not %v", stableV1alpha1, lastGV)
					}

					return nil
				}),
				// 等待所有添加的 group-version 出现在两个 discovery 文档中
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1alpha1}),
				// 检查 v1 和 v2 endpoint 的 groupversion
				// 集合完全相同
				checkGVOrder,
			},
		},
		{
			// 验证 group 内的版本按优先级排序
			Name: "VersionPriority",
			Actions: []testAction{
				applyAPIService(makeApiServiceSpec(stableV1, 1000, 2)),
				applyAPIService(makeApiServiceSpec(stableV1alpha1, 1000, 1)),
				applyAPIService(makeApiServiceSpec(stableV1alpha2, 1000, 3)),
				// 等待所有添加的 group-version 出现在两个 discovery 文档中
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1alpha2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1alpha2}),
				// 检查 v1 和 v2 endpoint 的 groupversion
				// 集合完全相同
				checkGVOrder,
				inlineAction(func(ctx context.Context, client testClient) error {
					// 查找 stable.example.com 对应的条目，
					// 验证版本顺序符合预期
					v2GroupsAndVersions, err := FetchV2Discovery(ctx, client)
					if err != nil {
						return err
					}

					// 在本测试中应排在最后
					group := v2GroupsAndVersions.Items[len(v2GroupsAndVersions.Items)-1]
					if group.Name != stableGroup {
						return fmt.Errorf("group is not where we expect: found %v, expected %v", group.Name, stableGroup)
					}

					versionOrder := []string{}
					for _, version := range group.Versions {
						versionOrder = append(versionOrder, version.Version)
					}

					expectedOrder := []string{
						stableV1alpha2.Version,
						stableV1.Version,
						stableV1alpha1.Version,
					}

					if !reflect.DeepEqual(expectedOrder, versionOrder) {
						return fmt.Errorf("version in wrong order: %v", cmp.Diff(expectedOrder, versionOrder))
					}

					return nil
				}),
			},
		},
		{
			// 验证 group 内的版本按优先级排序，
			// 且优先级相同的版本将按 Kubernetes 感知的版本
			// 比较器排序
			Name: "VersionPriorityTiebreaker",
			Actions: []testAction{
				applyAPIService(makeApiServiceSpec(stableV1, 1000, 15)),
				applyAPIService(makeApiServiceSpec(stableV1alpha1, 1000, 15)),
				applyAPIService(makeApiServiceSpec(stableV1alpha2, 1000, 15)),
				applyAPIService(makeApiServiceSpec(stableV1beta1, 1000, 15)),
				applyAPIService(makeApiServiceSpec(stableV2, 1000, 15)),
				// 等待所有添加的 group-version 出现在两个 discovery 文档中
				waitForGroupVersionsV1([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1alpha2, stableV1beta1, stableV2}),
				waitForGroupVersionsV2([]metav1.GroupVersion{stableV1, stableV1alpha1, stableV1alpha2, stableV1beta1, stableV2}),
				// 检查 v1 和 v2 endpoint 的 groupversion
				// 集合完全相同
				checkGVOrder,
				inlineAction(func(ctx context.Context, client testClient) error {
					// 查找 stable.example.com 对应的条目，
					// 验证版本顺序符合预期
					v2GroupsAndVersions, err := FetchV2Discovery(ctx, client)
					if err != nil {
						return err
					}

					// 在本测试中应排在最后
					group := v2GroupsAndVersions.Items[len(v2GroupsAndVersions.Items)-1]
					if group.Name != stableGroup {
						return fmt.Errorf("group is not where we expect: found %v, expected %v", group.Name, stableGroup)
					}

					versionOrder := []string{}
					for _, version := range group.Versions {
						versionOrder = append(versionOrder, version.Version)
					}

					expectedOrder := []string{
						stableV2.Version,
						stableV1.Version,
						stableV1beta1.Version,
						stableV1alpha2.Version,
						stableV1alpha1.Version,
					}

					if !reflect.DeepEqual(expectedOrder, versionOrder) {
						return fmt.Errorf("version in wrong order: %v", cmp.Diff(expectedOrder, versionOrder))
					}

					return nil
				}),
			},
		},
	})
}

func TestSingularNames(t *testing.T) {
	server := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--runtime-config=api/all=true"}, framework.SharedEtcd())
	t.Cleanup(server.TearDownFn)

	kubeClientSet, err := kubernetes.NewForConfig(server.ClientConfig)
	require.NoError(t, err)

	_, resources, err := kubeClientSet.Discovery().ServerGroupsAndResources()
	require.NoError(t, err)

	for _, rr := range resources {
		for _, r := range rr.APIResources {
			if strings.Contains(r.Name, "/") {
				continue
			}
			if r.SingularName == "" {
				t.Errorf("missing singularName for resource %q in %q", r.Name, rr.GroupVersion)
				continue
			}
			if r.SingularName != strings.ToLower(r.Kind) {
				t.Errorf("expected singularName for resource %q in %q to be %q, got %q", r.Name, rr.GroupVersion, strings.ToLower(r.Kind), r.SingularName)
				continue
			}
		}
	}
}

func makeCRDSpec(group string, kind string, namespaced bool, versions []string, categories ...string) apiextensionsv1.CustomResourceDefinitionSpec {
	scope := apiextensionsv1.NamespaceScoped
	if !namespaced {
		scope = apiextensionsv1.ClusterScoped
	}

	plural, singular := meta.UnsafeGuessKindToResource(schema.GroupVersionKind{Kind: kind})
	res := apiextensionsv1.CustomResourceDefinitionSpec{
		Group: group,
		Scope: scope,
		Names: apiextensionsv1.CustomResourceDefinitionNames{
			Plural:     plural.Resource,
			Singular:   singular.Resource,
			Kind:       kind,
			Categories: categories,
		},
	}

	for i, version := range versions {
		res.Versions = append(res.Versions, apiextensionsv1.CustomResourceDefinitionVersion{
			Name:    version,
			Served:  true,
			Storage: i == 0,
			Schema: &apiextensionsv1.CustomResourceValidation{
				OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"data": {
							Type: "string",
						},
					},
				},
			},
		})
	}
	return res
}
