/*
Copyright 2025 The Kubernetes Authors.

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

package storageversionmigrator

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	svmv1 "k8s.io/api/storagemigration/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	fakediscovery "k8s.io/client-go/discovery/fake"
	"k8s.io/client-go/informers"
	svminformers "k8s.io/client-go/informers/storagemigration/v1"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestIsResourceMigratable 验证 isResourceMigratable 能否按发现文档判断某 GVR 是否具备迁移所需的动词，
// 覆盖动词齐全、动词缺失、资源不存在以及多版本/多组场景下选错版本的情况。
func TestIsResourceMigratable(t *testing.T) {
	tcs := []struct {
		name      string
		resources []*metav1.APIResourceList
		resource  schema.GroupVersionResource
		want      bool
		wantErr   string
	}{
		{
			name: "migratable resource",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
						{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"get", "list", "watch", "create", "delete"}},
						{Name: "configmaps", Namespaced: true, Kind: "Event", Verbs: []string{"get", "watch", "create", "delete", "update", "patch", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"},
			want:     true,
		},
		{
			name: "non-updatable resource",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
						{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"get", "list", "watch", "create", "delete"}},
						{Name: "configmaps", Namespaced: true, Kind: "Event", Verbs: []string{"get", "watch", "create", "delete", "update", "patch", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"},
			want:     false,
		},
		{
			name: "non-patchable resource",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
						{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"get", "list", "watch", "create", "delete"}},
						{Name: "configmaps", Namespaced: true, Kind: "Configmap", Verbs: []string{"get", "watch", "create", "delete", "update", "patch", "delete"}},
						{Name: "secrets", Namespaced: true, Kind: "Secret", Verbs: []string{"get", "watch", "create", "delete", "update", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"},
			want:     false,
		},
		{
			name: "non-listable resource",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
						{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"get", "list", "watch", "create", "delete"}},
						{Name: "configmaps", Namespaced: true, Kind: "Event", Verbs: []string{"get", "watch", "create", "delete", "update", "patch", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"},
			want:     false,
		},
		{
			name: "unknown resource",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
						{Name: "events", Namespaced: true, Kind: "Event", Verbs: []string{"get", "list", "watch", "create", "delete"}},
						{Name: "configmaps", Namespaced: true, Kind: "Configmap", Verbs: []string{"get", "watch", "create", "delete", "update", "patch", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "", Version: "v1", Resource: "foo"},
			wantErr:  "resource \"/v1, Resource=foo\" not found in discovery",
		},
		{
			name: "multiple versions",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "foo", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
					},
				},
				{
					GroupVersion: "v1alpha1",
					APIResources: []metav1.APIResource{
						{Name: "foo", Namespaced: true, Kind: "Pod", Verbs: []string{"get", "watch", "create", "update", "patch", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "", Version: "v1alpha", Resource: "foo"},
			want:     false,
		},
		{
			name: "multiple versions and groups",
			resources: []*metav1.APIResourceList{
				{
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{Name: "foo", Namespaced: true, Kind: "Foo", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
					},
				},
				{
					GroupVersion: "v1alpha1",
					APIResources: []metav1.APIResource{
						{Name: "foo", Namespaced: true, Kind: "Foo", Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"}},
					},
				},
				{
					GroupVersion: "bar/v1alpha1",
					APIResources: []metav1.APIResource{
						{Name: "foo", Namespaced: true, Kind: "Foo", Group: "bar", Verbs: []string{"get", "watch", "create", "update", "patch", "delete"}},
					},
				},
			},
			resource: schema.GroupVersionResource{Group: "bar", Version: "v1alpha1", Resource: "foo"},
			want:     false,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {}))
			defer server.Close()
			discoveryClient := fakediscovery.FakeDiscovery{Fake: &kubetesting.Fake{}}
			// 用假发现客户端注入用例里的发现文档；单测没有真实 API Server 可查询。
			discoveryClient.Resources = tc.resources
			rvController := ResourceVersionController{
				discoveryClient: discovery.ToDiscoveryInterfaceWithContext(&discoveryClient),
			}

			isMigratable, err := rvController.isResourceMigratable(tCtx, tc.resource)
			if err != nil {
				// 资源不在发现文档中时返回错误而非布尔值，所以“未知资源”用例只校验错误文本。
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Unexpected error: %v, want: %v", err, tc.wantErr)
				}
				return
			}
			// 其余用例校验布尔判定，避免把缺失 list/update/patch 动词的资源误判为可迁移。
			if isMigratable != tc.want {
				t.Errorf("Expected %v, got %v", tc.want, isMigratable)
			}
		})
	}
}

// TestRVSync 验证 ResourceVersionController.sync 的各条分支：迁移未开始或已终结时直接跳过，
// 可迁移时写入最新 resourceVersion 并更新状态，资源或元数据异常时标记失败或返回错误。
func TestRVSync(t *testing.T) {
	testCases := []struct {
		name               string
		key                string
		svm                *svmv1.StorageVersionMigration
		discoveryResources *metav1.APIResourceList
		metadataList       *metav1.List
		metadataErr        bool
		expectErr          string
		expectKubeActions  []kubetesting.Action
	}{
		{
			name: "Migration not active",
			key:  "inactive-svm",
			svm:  newSVMWithConditions("inactive-svm", "", []metav1.Condition{}),
		},
		{
			name: "Successful RV acquisition",
			key:  "test-svm",
			svm: newSVMWithConditions("test-svm", "", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			}),
			discoveryResources: &metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: []string{"list", "update", "patch"}},
				},
			},
			metadataList: &metav1.List{
				ListMeta: metav1.ListMeta{
					ResourceVersion: "12345",
				},
			},
			expectKubeActions: []kubetesting.Action{
				kubetesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "12345", []metav1.Condition{
						{
							Type:   string(svmv1.MigrationRunning),
							Status: metav1.ConditionTrue,
						},
					}),
				),
			},
		},
		{
			name: "SVM not found",
			key:  "non-existent-svm",
			svm:  nil,
		},
		{
			name: "SVM has no CRD condition",
			key:  "succeeded-svm",
			svm:  newSVM("succeeded-svm", ""),
		},
		{
			name: "SVM already succeeded",
			key:  "succeeded-svm",
			svm: newSVMWithConditions("succeeded-svm", "100", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationSucceeded),
					Status: metav1.ConditionTrue,
				},
			}),
		},
		{
			name: "SVM already failed",
			key:  "failed-svm",
			svm: newSVMWithConditions("failed-svm", "100", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationFailed),
					Status: metav1.ConditionTrue,
				},
			}),
		},
		{
			name: "RV already set",
			key:  "rv-set-svm",
			svm: newSVMWithConditions("rv-set-svm", "123", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			}),
		},
		{
			name: "Resource not migratable",
			key:  "not-migratable-svm",
			svm: newSVMWithConditions("not-migratable-svm", "", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			}),
			discoveryResources: &metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: []string{"update", "patch"}}, // 缺少 "list" 动词
				},
			},
			expectKubeActions: []kubetesting.Action{
				kubetesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("not-migratable-svm", "", []metav1.Condition{
						{
							Type:   string(svmv1.MigrationRunning),
							Status: metav1.ConditionTrue,
						},
						{
							Type:   string(svmv1.MigrationFailed),
							Status: metav1.ConditionTrue,
						},
					}),
				),
			},
		},
		{
			name: "Metadata list error",
			key:  "metadata-error-svm",
			svm: newSVMWithConditions("metadata-error-svm", "", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			}),
			discoveryResources: &metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: []string{"list", "update", "patch"}},
				},
			},
			metadataList: &metav1.List{},
			metadataErr:  true,
			expectErr:    "error getting latest resourceVersion for apps/v1",
		},
		{
			name: "Invalid RV returned",
			key:  "invalid-rv-svm",
			svm: newSVMWithConditions("invalid-rv-svm", "", []metav1.Condition{
				{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			}),
			discoveryResources: &metav1.APIResourceList{
				GroupVersion: "apps/v1",
				APIResources: []metav1.APIResource{
					{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: []string{"list", "update", "patch"}},
				},
			},
			metadataList: &metav1.List{
				ListMeta: metav1.ListMeta{
					ResourceVersion: "abcde",
				},
			},
			expectKubeActions: []kubetesting.Action{
				kubetesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("invalid-rv-svm", "", []metav1.Condition{
						{
							Type:   string(svmv1.MigrationRunning),
							Status: metav1.ConditionTrue,
						},
						{
							Type:   string(svmv1.MigrationFailed),
							Status: metav1.ConditionTrue,
						},
					}),
				),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var initialSVMs []runtime.Object
			if tc.svm != nil {
				initialSVMs = append(initialSVMs, tc.svm)
			}
			kubeClient := kubefake.NewClientset(initialSVMs...)
			kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
			svmInformer := kubeInformerFactory.Storagemigration().V1().StorageVersionMigrations()

			if tc.svm != nil {
				err := svmInformer.Informer().GetStore().Add(tc.svm)
				require.NoError(t, err)
			}

			// 用假发现客户端模拟 API Server 的资源发现结果。
			discoveryClient := &fakediscovery.FakeDiscovery{Fake: &kubetesting.Fake{}}
			if tc.discoveryResources != nil {
				discoveryClient.Resources = []*metav1.APIResourceList{tc.discoveryResources}
			}

			// 用假元数据客户端模拟 list 请求，以便顺带校验请求携带的 GVR 和命名空间。
			metadatascheme := metadatafake.NewTestScheme()
			err := svmv1.AddToScheme(metadatascheme)
			require.NoError(t, err)
			err = metav1.AddMetaToScheme(metadatascheme)
			require.NoError(t, err)
			metadataClient := metadatafake.NewSimpleMetadataClient(metadatascheme)

			// 拦截所有 list 调用：既要返回预设的 resourceVersion，也要校验请求是否落在预期的命名空间范围。
			metadataClient.Fake.PrependReactor("list", "*", func(action kubetesting.Action) (handled bool, ret runtime.Object, err error) {
				listAction, ok := action.(kubetesting.ListAction)
				require.True(t, ok, "expected ListAction")

				require.Equal(t, testGVR, listAction.GetResource(), "GVR in metadata list action does not match testGVR")

				isNamespaced := false
				if tc.discoveryResources != nil && len(tc.discoveryResources.APIResources) > 0 {
					isNamespaced = tc.discoveryResources.APIResources[0].Namespaced
				}

				if isNamespaced {
					require.Equal(t, fakeSVMNamespaceName, listAction.GetNamespace(), "expected list on fake namespace")
				} else {
					require.Empty(t, listAction.GetNamespace(), "expected cluster-scoped list")
				}

				if tc.metadataErr {
					return true, nil, fmt.Errorf("failed to list")
				}
				return true, tc.metadataList, nil
			})

			controller := newTestRVController(kubeClient, discoveryClient, metadataClient, svmInformer)

			err = controller.sync(ctx, tc.key)

			if tc.expectErr != "" {
				require.ErrorContains(t, err, tc.expectErr)
			} else {
				require.NoError(t, err)
			}

			// informer 启动会产生 list/watch 噪声，过滤掉后再断言控制器真正发出的写操作。
			kubeActions := filterListWatchActions(kubeClient.Actions())
			if tc.expectKubeActions == nil {
				require.Empty(t, kubeActions, "expected zero kube client actions")
				return
			}

			require.Len(t, kubeActions, len(tc.expectKubeActions), "mismatched number of kube client actions")
			for i, expected := range tc.expectKubeActions {
				actual := kubeActions[i]
				require.Equal(t, expected.GetVerb(), actual.GetVerb(), "kube action %d: verb mismatch", i)
				require.Equal(t, expected.GetResource(), actual.GetResource(), "kube action %d: resource mismatch", i)

				actualSvm := actual.(kubetesting.UpdateAction).GetObject().(*svmv1.StorageVersionMigration)
				expectedSvm := expected.(kubetesting.UpdateAction).GetObject().(*svmv1.StorageVersionMigration)

				// 只比对关心的字段：其余元数据由 fake client 填充，全量比对会引入无关噪声。
				require.Equal(t, expectedSvm.Status.ResourceVersion, actualSvm.Status.ResourceVersion, "kube action %d: status.resourceVersion mismatch", i)

				expectedConditions := expectedSvm.Status.Conditions
				actualConditions := actualSvm.Status.Conditions
				require.Len(t, actualConditions, len(expectedConditions), "kube action %d: conditions length mismatch", i)

				for j, expectedCondition := range expectedConditions {
					actualCondition := actualConditions[j]
					require.Equal(t, expectedCondition.Type, actualCondition.Type, "kube action %d: condition %d type mismatch", i, j)
					require.Equal(t, expectedCondition.Status, actualCondition.Status, "kube action %d: condition %d status mismatch", i, j)
				}
			}
		})
	}
}

// filterListWatchActions 过滤掉 client-go 假客户端记录的 list/watch 动作，
// 只留下控制器实际触发的写操作供断言。
func filterListWatchActions(actions []kubetesting.Action) []kubetesting.Action {
	var filteredActions []kubetesting.Action
	for _, action := range actions {
		// list/watch 由 informer 产生，与被测的同步逻辑无关。
		if action.GetVerb() == "list" || action.GetVerb() == "watch" {
			continue
		}
		filteredActions = append(filteredActions, action)
	}
	return filteredActions
}

// testRESTMapper 给假 RESTMapper 加上 Reset，供发现结果定期刷新时重置缓存。
type testRESTMapper struct {
	meta.RESTMapper
}

// Reset 委托给底层 RESTMapper 的 Reset，避免测试中的 mapper 缓存过期资源。
func (m *testRESTMapper) Reset() {
	meta.MaybeResetRESTMapper(m.RESTMapper)
}

// newTestRVController 组装一个供测试使用的 ResourceVersionController，
// 注入假客户端与 informer，并预置可解析测试 GVR 的 RESTMapper。
func newTestRVController(
	kubeClient kubernetes.Interface,
	discoveryClient discovery.DiscoveryInterface,
	metadataClient metadata.Interface,
	svmInformer svminformers.StorageVersionMigrationInformer,
) *ResourceVersionController {
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{testGVK.GroupVersion()})
	mapper.Add(testGVK, meta.RESTScopeNamespace)

	rvController := &ResourceVersionController{
		kubeClient:      kubeClient,
		discoveryClient: discovery.ToDiscoveryInterfaceWithContext(discoveryClient),
		metadataClient:  metadataClient,
		svmListers:      svmInformer.Lister(),
		svmSynced:       func() bool { return true },
		mapper:          &testRESTMapper{mapper},
	}
	return rvController
}
