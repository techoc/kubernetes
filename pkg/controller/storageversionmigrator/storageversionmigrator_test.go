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
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	svmv1 "k8s.io/api/storagemigration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	svminformers "k8s.io/client-go/informers/storagemigration/v1"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller/garbagecollector"
)

var (
	testGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	testGVK = schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
)

type mockGraphBuilder struct {
	monitor  *mockMonitor
	notFound bool
	err      error
}

// GetMonitor 按测试构造返回预置的 monitor，以及资源是否存在和错误，
// 用于模拟依赖图中 GC 监听器的不同状态。
func (m *mockGraphBuilder) GetMonitor(_ context.Context, _ schema.GroupVersionResource) (*garbagecollector.Monitor, bool, error) {
	if m.monitor != nil {
		return &m.monitor.Monitor, !m.notFound, m.err
	}
	return nil, !m.notFound, m.err
}

type mockMonitor struct {
	garbagecollector.Monitor
}

type mockResourceSyncer struct {
	cache.Controller
	lastSyncRV string
}

// LastSyncResourceVersion 返回测试设定的 GC 缓存 resourceVersion，
// 用它模拟 GC informer 领先或落后于迁移对象的情况。
func (m *mockResourceSyncer) LastSyncResourceVersion() string {
	return m.lastSyncRV
}

type sortedStore struct {
	cache.Store
}

// List 按命名空间和名称排序后返回对象列表。迁移必须对对象施加确定的顺序，
// 测试才能稳定断言 patch 动作的顺序，避免 map 遍历的随机性造成偶发失败。
func (s *sortedStore) List() []interface{} {
	list := s.Store.List()
	sort.Slice(list, func(i, j int) bool {
		accI, _ := meta.Accessor(list[i])
		accJ, _ := meta.Accessor(list[j])
		if accI.GetNamespace() != accJ.GetNamespace() {
			return accI.GetNamespace() < accJ.GetNamespace()
		}
		return accI.GetName() < accJ.GetName()
	})
	return list
}

// newMockMonitor 构造带指定 resourceVersion 和对象集合的假 GC monitor，
// 使测试能精确控制“GC 缓存是否已追上迁移版本”这一关键前置条件。
func newMockMonitor(lastSyncRV string, items []runtime.Object) *mockMonitor {
	store := cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
	for _, item := range items {
		_ = store.Add(item)
	}

	return &mockMonitor{
		Monitor: garbagecollector.Monitor{
			Store: &sortedStore{Store: store},
			Controller: &mockResourceSyncer{
				lastSyncRV: lastSyncRV,
			},
		},
	}
}

// newTestSVMController 组装一个使用 fake 客户端、动态客户端和 RESTMapper 的控制器，
// RESTMapper 只认识 apps/deployments，便于构造资源存在或不存在的测试场景。
func newTestSVMController(
	kubeClient kubernetes.Interface,
	svmInformer svminformers.StorageVersionMigrationInformer,
	graphBuilder *mockGraphBuilder,
) *SVMController {
	dynamicClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{testGVK.GroupVersion()})
	mapper.Add(testGVK, meta.RESTScopeNamespace)

	return &SVMController{
		controllerName:         "test-svm-controller",
		kubeClient:             kubeClient,
		dynamicClient:          dynamicClient,
		svmListers:             svmInformer.Lister(),
		svmSynced:              func() bool { return true },
		restMapper:             mapper,
		dependencyGraphBuilder: graphBuilder,
	}
}

// newSVM 构造一个指向测试用 GroupResource、处于指定 resourceVersion 和条件的迁移对象。
func newSVM(name, resourceVersion string, conditions ...metav1.Condition) *svmv1.StorageVersionMigration {
	return &svmv1.StorageVersionMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.Now(),
		},
		Spec: svmv1.StorageVersionMigrationSpec{
			Resource: metav1.GroupResource{
				Group:    testGVR.Group,
				Resource: testGVR.Resource,
			},
		},
		Status: svmv1.StorageVersionMigrationStatus{
			ResourceVersion: resourceVersion,
			Conditions:      conditions,
		},
	}
}

// newSVMWithConditions 基于 newSVM 覆盖状态条件，方便断言迁移结束后的状态更新。
func newSVMWithConditions(name, resourceVersion string, conditions []metav1.Condition) *svmv1.StorageVersionMigration {
	svm := newSVM(name, resourceVersion)
	svm.Status.Conditions = conditions
	return svm
}

// TestSync 以表驱动方式覆盖 sync 的主要路径：迁移成功、对象不存在、已完成、
// GC 缓存落后、资源不在 GC 中、API 与动态客户端错误等，并校验对 SVM 状态的
// 更新动作和对各对象的 patch 动作。RV 大于迁移对象 RV 的资源会被跳过，
// 因为它们是在迁移开始后新建的，不属于本次重写范围。
func TestSync(t *testing.T) {
	newResource := func(name, namespace, rv, uid string) *unstructured.Unstructured {
		return &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":            name,
					"namespace":       namespace,
					"resourceVersion": rv,
					"uid":             uid,
				},
			},
		}
	}

	// TODO: 添加模拟的 discovery 客户端。
	testCases := []struct {
		name                 string
		key                  string
		svm                  *svmv1.StorageVersionMigration
		graphBuilder         *mockGraphBuilder
		expectErr            bool
		expectKubeActions    []k8stesting.Action
		expectDynamicActions []k8stesting.Action
		dynamicClientErrors  map[string]error
	}{
		{
			name: "Successful migration",
			key:  "test-svm",
			svm: newSVM("test-svm", "100", metav1.Condition{
				Type:   string(svmv1.MigrationRunning),
				Status: metav1.ConditionTrue,
				Reason: migrationRunningReason,
			}),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("100", []runtime.Object{
					newResource("res1", "ns1", "90", "uid1"),
					newResource("res2", "ns1", "100", "uid2"),
					newResource("res3", "ns2", "101", "uid3"), // RV 超过迁移版本，应跳过
				}),
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "100", []metav1.Condition{
						{
							Type:   string(svmv1.MigrationRunning),
							Status: metav1.ConditionTrue,
						},
						{
							Type:   string(svmv1.MigrationSucceeded),
							Status: metav1.ConditionTrue,
						},
					}),
				),
			},
			expectDynamicActions: []k8stesting.Action{
				k8stesting.NewPatchAction(testGVR, "ns1", "res1", types.MergePatchType, mustMarshal(t, typeMetaUIDRV{
					TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
					objectRV: objectRV{ResourceVersion: "90"},
				})),
				k8stesting.NewPatchAction(testGVR, "ns1", "res2", types.MergePatchType, mustMarshal(t, typeMetaUIDRV{
					TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
					objectRV: objectRV{ResourceVersion: "100"},
				})),
			},
		},
		{
			name:      "SVM not found",
			key:       "non-existent-svm",
			svm:       nil,
			expectErr: false,
		},
		{
			name: "SVM already succeeded",
			key:  "succeeded-svm",
			svm: newSVM("succeeded-svm", "100", metav1.Condition{
				Type:   string(svmv1.MigrationSucceeded),
				Status: metav1.ConditionTrue,
			}),
			expectErr: false,
		},
		{
			name: "GC cache is not up to date",
			key:  "stale-gc-svm",
			svm:  newSVM("stale-gc-svm", "100"),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("99", []runtime.Object{}), // GC 版本落后，sync 必须报错重试
			},
			expectErr: true,
		},
		{
			name: "Resource not in GC",
			key:  "no-resource",
			svm: func() *svmv1.StorageVersionMigration {
				s := newSVM("no-resource", "100")
				s.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Minute))
				return s
			}(),
			graphBuilder: &mockGraphBuilder{
				monitor:  newMockMonitor("99", []runtime.Object{}),
				notFound: true,
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "100", []metav1.Condition{{
						Type:   string(svmv1.MigrationFailed),
						Status: metav1.ConditionTrue,
					}}),
				),
			},
		},
		{
			name: "Fatal patch error fails migration",
			key:  "fatal-error-svm",
			svm:  newSVM("fatal-error-svm", "100"),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("100", []runtime.Object{
					newResource("res1", "ns1", "90", "uid1"),
				}),
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "100", []metav1.Condition{
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
			dynamicClientErrors: map[string]error{
				"ns1/res1": fmt.Errorf("fatal error"),
			},
		},
		{
			name: "Conflict on patch is ignored",
			key:  "conflict-svm",
			svm:  newSVM("conflict-svm", "100"),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("100", []runtime.Object{
					newResource("res1", "ns1", "90", "uid1"),
					newResource("res2", "ns2", "95", "uid2"),
				}),
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "100", []metav1.Condition{
						{
							Type:   string(svmv1.MigrationRunning),
							Status: metav1.ConditionTrue,
						},
						{
							Type:   string(svmv1.MigrationSucceeded),
							Status: metav1.ConditionTrue,
						},
					}),
				),
			},
			expectDynamicActions: []k8stesting.Action{
				k8stesting.NewPatchAction(testGVR, "ns1", "res1", types.MergePatchType, mustMarshal(t, typeMetaUIDRV{
					TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
					objectRV: objectRV{ResourceVersion: "90"},
				})),
				k8stesting.NewPatchAction(testGVR, "ns2", "res2", types.MergePatchType, mustMarshal(t, typeMetaUIDRV{
					TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
					objectRV: objectRV{ResourceVersion: "95"},
				})),
			},
			dynamicClientErrors: map[string]error{
				"ns1/res1": apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, "res1", nil),
			},
		},
		{
			name: "Retriable patch error is returned directly",
			key:  "retriable-error-svm",
			svm:  newSVM("retriable-error-svm", "100"),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("100", []runtime.Object{
					newResource("res1", "ns1", "90", "uid1"),
				}),
			},
			dynamicClientErrors: map[string]error{
				"ns1/res1": apierrors.NewTooManyRequests("simulating throttling", 1),
			},
			expectErr: true,
			expectDynamicActions: []k8stesting.Action{
				k8stesting.NewPatchAction(testGVR, "ns1", "res1", types.MergePatchType, mustMarshal(t, typeMetaUIDRV{
					TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
					objectRV: objectRV{ResourceVersion: "90"},
				})),
			},
		},
		{
			name: "Incomparable resource version for gc fails migration",
			key:  "incomparable-resource",
			svm: func() *svmv1.StorageVersionMigration {
				s := newSVM("incomparable-resource", "100")
				s.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Minute))
				return s
			}(),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("abc", []runtime.Object{
					newResource("res1", "ns1", "90", "uid1"),
				}),
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "100", []metav1.Condition{{
						Type:   string(svmv1.MigrationFailed),
						Status: metav1.ConditionTrue,
					}}),
				),
			},
		},
		{
			name: "Incomparable resource version for object fails migration",
			key:  "incomparable-resource-obj",
			svm: func() *svmv1.StorageVersionMigration {
				s := newSVM("incomparable-resource-obj", "100")
				s.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Minute))
				return s
			}(),
			graphBuilder: &mockGraphBuilder{
				monitor: newMockMonitor("100", []runtime.Object{
					newResource("res1", "ns1", "abc", "uid1"),
				}),
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVMWithConditions("test-svm", "100", []metav1.Condition{
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

			controller := newTestSVMController(kubeClient, svmInformer, tc.graphBuilder)

			dynamicClient := controller.dynamicClient.(*dynamicfake.FakeDynamicClient)
			dynamicClient.PrependReactor("patch", "*", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				patchAction := action.(k8stesting.PatchAction)
				key := fmt.Sprintf("%s/%s", patchAction.GetNamespace(), patchAction.GetName())
				if err, found := tc.dynamicClientErrors[key]; found {
					return true, nil, err
				}
				return true, nil, nil
			})

			err := controller.sync(ctx, tc.key)

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectKubeActions != nil {
				kubeActions := filterActions(kubeClient.Actions())
				require.Len(t, kubeActions, len(tc.expectKubeActions), "mismatched number of kube client actions, expected %d, got %d", len(tc.expectKubeActions), len(kubeActions))

				for i, expected := range tc.expectKubeActions {
					actual := kubeActions[i]
					require.Equal(t, expected.GetVerb(), actual.GetVerb(), "kube action %d: verb mismatch", i)
					require.Equal(t, expected.GetResource(), actual.GetResource(), "kube action %d: resource mismatch", i)

					actualSvm := actual.(k8stesting.UpdateAction).GetObject().(*svmv1.StorageVersionMigration)
					expectedSvm := expected.(k8stesting.UpdateAction).GetObject().(*svmv1.StorageVersionMigration)
					expectedConditions := expectedSvm.Status.Conditions
					actualConditions := actualSvm.Status.Conditions
					require.Len(t, expectedConditions, len(actualConditions), "kube action %d: conditions mismatch", i)
					for j, expectedCondition := range expectedConditions {
						actualCondition := actualConditions[j]
						require.Equal(t, expectedCondition.Type, actualCondition.Type, "kube action %d: condition type mismatch", i)
						require.Equal(t, expectedCondition.Status, actualCondition.Status, "kube action %d: condition status mismatch", i)
					}
				}
			}

			if tc.expectDynamicActions != nil {
				dynamicActions := filterActions(dynamicClient.Actions())
				require.Len(t, dynamicActions, len(tc.expectDynamicActions), "mismatched number of dynamic client actions")
				sortPatchActions(dynamicActions)
				sortPatchActions(tc.expectDynamicActions)

				for i, expected := range tc.expectDynamicActions {
					actual := dynamicActions[i]
					require.Equal(t, expected.GetVerb(), actual.GetVerb(), "dynamic action %d: verb mismatch", i)
					require.Equal(t, expected.GetResource(), actual.GetResource(), "dynamic action %d: resource mismatch", i)

					if expectedPatch, ok := expected.(k8stesting.PatchAction); ok {
						actualPatch := actual.(k8stesting.PatchAction)
						require.Equal(t, expectedPatch.GetPatchType(), actualPatch.GetPatchType(), "dynamic action %d: patch type mismatch", i)
						require.Equal(t, string(expectedPatch.GetPatch()), string(actualPatch.GetPatch()), "dynamic action %d: patch payload mismatch", i)
					}
				}
			}
		})
	}
}

// mustMarshal 把期望的 patch 负载序列化为 JSON；序列化失败直接让测试终止，
// 因为负载构造错误会让后续比较失去意义。
func mustMarshal(t *testing.T, obj interface{}) []byte {
	data, err := json.Marshal(obj)
	require.NoError(t, err)
	return data
}

// filterActions 只保留 update 和 patch 动作，过滤掉 list/get 等无关调用，
// 让断言集中在控制器真正产生的状态变更上。
func filterActions(actions []k8stesting.Action) []k8stesting.Action {
	var relevantActions []k8stesting.Action
	for _, action := range actions {
		if action.GetVerb() == "update" || action.GetVerb() == "patch" {
			relevantActions = append(relevantActions, action)
		}
	}
	return relevantActions
}

// sortPatchActions 按命名空间和名称给 patch 动作排序，
// 消除批次处理中并发或 map 迭代顺序带来的不确定性。
func sortPatchActions(actions []k8stesting.Action) {
	sort.Slice(actions, func(i, j int) bool {
		actionI := actions[i].(k8stesting.PatchAction)
		actionJ := actions[j].(k8stesting.PatchAction)
		if actionI.GetNamespace() != actionJ.GetNamespace() {
			return actionI.GetNamespace() < actionJ.GetNamespace()
		}
		return actionI.GetName() < actionJ.GetName()
	})
}

// TestSVMConditions 验证不同失败类型下迁移条件（Condition）的取值：
// 全部成功置 MigrationSucceeded；可重试的限流错误保持 MigrationRunning 并报告剩余数量；
// 不可恢复错误置 MigrationFailed。这样控制器能区分暂时性失败与终态失败。
func TestSVMConditions(t *testing.T) {
	newResource := func(name, namespace, rv, uid string) *unstructured.Unstructured {
		return &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":            name,
					"namespace":       namespace,
					"resourceVersion": rv,
					"uid":             uid,
				},
			},
		}
	}

	testCases := []struct {
		name                   string
		items                  []runtime.Object
		dynamicClientErrors    map[string]error
		expectErr              bool
		expectConditionType    svmv1.MigrationConditionType
		expectConditionMessage string
	}{
		{
			name: "Successful migration",
			items: []runtime.Object{
				newResource("res1", "ns1", "90", "uid1"),
				newResource("res2", "ns1", "100", "uid2"),
				newResource("res3", "ns2", "101", "uid3"), // RV 超过迁移版本，应跳过
			},
			expectErr:              false,
			expectConditionType:    svmv1.MigrationSucceeded,
			expectConditionMessage: "Migration completed",
		},
		{
			name: "Transient failure on second object",
			items: []runtime.Object{
				newResource("res1", "ns1", "90", "uid1"),
				newResource("res2", "ns1", "100", "uid2"),
			},
			dynamicClientErrors: map[string]error{
				"ns1/res2": apierrors.NewTooManyRequests("simulating throttling", 1),
			},
			expectErr:              true,
			expectConditionType:    svmv1.MigrationRunning,
			expectConditionMessage: "The migration is running, 1/2 objects not yet migrated; transient error: simulating throttling",
		},
		{
			name: "Fatal failure on second object",
			items: []runtime.Object{
				newResource("res1", "ns1", "90", "uid1"),
				newResource("res2", "ns1", "100", "uid2"),
			},
			dynamicClientErrors: map[string]error{
				"ns1/res2": fmt.Errorf("fatal error"),
			},
			expectErr:              false, // sync returns nil when runMigration returns true for failed
			expectConditionType:    svmv1.MigrationFailed,
			expectConditionMessage: "1/2 not yet migrated. migration encountered unhandled error: fatal error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			svm := newSVM("test-svm", "100")
			kubeClient := kubefake.NewClientset(svm)
			kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
			svmInformer := kubeInformerFactory.Storagemigration().V1().StorageVersionMigrations()

			err := svmInformer.Informer().GetStore().Add(svm)
			require.NoError(t, err)

			graphBuilder := &mockGraphBuilder{
				monitor: newMockMonitor("100", tc.items),
			}

			controller := newTestSVMController(kubeClient, svmInformer, graphBuilder)

			dynamicClient := controller.dynamicClient.(*dynamicfake.FakeDynamicClient)
			dynamicClient.PrependReactor("patch", "*", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				patchAction := action.(k8stesting.PatchAction)
				key := fmt.Sprintf("%s/%s", patchAction.GetNamespace(), patchAction.GetName())
				if err, found := tc.dynamicClientErrors[key]; found {
					return true, nil, err
				}
				return true, nil, nil
			})

			err = controller.sync(ctx, "test-svm")

			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			updatedSVM, err := kubeClient.StoragemigrationV1().StorageVersionMigrations().Get(ctx, "test-svm", metav1.GetOptions{})
			require.NoError(t, err)

			cond := meta.FindStatusCondition(updatedSVM.Status.Conditions, string(tc.expectConditionType))
			require.NotNil(t, cond, "Condition %s not found", tc.expectConditionType)
			require.Equal(t, metav1.ConditionTrue, cond.Status)
			require.Contains(t, cond.Message, tc.expectConditionMessage)
		})
	}
}

// TestSyncDoesNotPanicWhenResourceNotInMapper 是一个回归测试：
// 当迁移目标资源不在 RESTMapper 中时 sync 不得 panic。
// 修复前，resourceFor 对未知资源返回 (nil, false, nil)，sync 又对 nil GVR
// 调用 String()，导致 kube-controller-manager 崩溃。
func TestSyncDoesNotPanicWhenResourceNotInMapper(t *testing.T) {
	ctx := context.Background()

	// 构造一个已设置 resourceVersion、尚未进入终态的 SVM，
	// 其目标资源不在只认识 apps/deployments 的测试 RESTMapper 中，
	// 从而进入修复前会触发 nil 解引用的代码路径。
	svm := newSVM("svm-nomap", "100")
	svm.Spec.Resource = metav1.GroupResource{
		Group:    "nonexistent.example.com",
		Resource: "widgets",
	}

	kubeClient := kubefake.NewClientset(svm)
	factory := informers.NewSharedInformerFactory(kubeClient, 0)
	svmInformer := factory.Storagemigration().V1().StorageVersionMigrations()
	require.NoError(t, svmInformer.Informer().GetStore().Add(svm))
	ctrl := newTestSVMController(kubeClient, svmInformer, &mockGraphBuilder{})

	// 捕获 panic 并转为测试失败：修复前这里会因 gvr.String() 的 nil 解引用
	// 直接崩溃整个 kube-controller-manager。
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("sync panicked for a resource absent from the RESTMapper: %v", r)
		}
	}()

	// 创建时间很近，因此 sync 应返回错误以触发重新入队，而不是 panic。
	err := ctrl.sync(ctx, "svm-nomap")
	require.Error(t, err)
	require.Contains(t, err.Error(), "resource does not exist in rest mapper")
}
