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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	svmv1 "k8s.io/api/storagemigration/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apiextensionsscheme "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	svminformers "k8s.io/client-go/informers"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// init 将 apiextensions v1 注册到 scheme，确保伪造的 CRD 客户端能够正确编解码 CRD 对象。
func init() {
	_ = apiextensionsv1.AddToScheme(apiextensionsscheme.Scheme)
}

// TestCustomResourceController_Sync 以表驱动方式验证自定义资源控制器的 sync 流程：
// 涵盖初始同步、迁移在运行中/成功/失败时的状态更新，以及 older SVM 优先提升等场景。
func TestCustomResourceController_Sync(t *testing.T) {
	// newSVM 构造一个指定资源与状态的 StorageVersionMigration 测试对象。
	newSVM := func(name, group, resource string, conditions ...metav1.Condition) *svmv1.StorageVersionMigration {
		return &svmv1.StorageVersionMigration{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: svmv1.StorageVersionMigrationSpec{
				Resource: metav1.GroupResource{Group: group, Resource: resource},
			},
			Status: svmv1.StorageVersionMigrationStatus{
				Conditions: conditions,
			},
		}
	}

	// newCRD 构造带有 v1/v2 两个版本、并指定存储版本与 generation 的 CRD 测试对象。
	newCRD := func(name string, generation int64, storageVersion string) *apiextensionsv1.CustomResourceDefinition {
		return &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name:       name,
				Generation: generation,
			},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{Name: "v1", Storage: storageVersion == "v1"},
					{Name: "v2", Storage: storageVersion == "v2"},
				},
			},
		}
	}

	// newCRDPatchAction 构造针对 CRD status 的合并补丁动作，供客户端动作断言使用。
	newCRDPatchAction := func(name string, conditions []apiextensionsv1.CustomResourceDefinitionCondition, storedVersions []string) k8stesting.PatchAction {
		statusMap := map[string]interface{}{
			"conditions": conditions,
		}
		// storedVersions 为 nil 时表示本次补丁不涉及存储版本列表。
		if storedVersions != nil {
			statusMap["storedVersions"] = storedVersions
		}
		patchObj := map[string]interface{}{
			"status": statusMap,
		}
		patch, err := json.Marshal(patchObj)
		if err != nil {
			panic(err)
		}
		return k8stesting.NewPatchAction(
			apiextensionsv1.SchemeGroupVersion.WithResource("customresourcedefinitions"),
			"",
			name,
			types.MergePatchType,
			patch,
		)
	}

	testCases := []struct {
		// name 是用例名称；svms 是初始的迁移对象；crd 是初始 CRD（nil 表示不存在）。
		// expectErr 表示 sync 是否应报错；expectKubeActions/expectCRDActions 是期望的客户端动作。
		name              string
		svms              []*svmv1.StorageVersionMigration
		crd               *apiextensionsv1.CustomResourceDefinition
		expectErr         bool
		expectKubeActions []k8stesting.Action
		expectCRDActions  []k8stesting.Action
	}{
		{
			// CRD 不存在时只把 SVM 置为 Running，不应产生 CRD 补丁。
			name:      "Initial sync: CRD missing",
			svms:      []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets")},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVM("test-svm", "example.com", "widgets", metav1.Condition{
						Type:    string(svmv1.MigrationRunning),
						Status:  metav1.ConditionTrue,
						Reason:  "MigrationRunning",
						Message: "The migration is running",
					}),
				),
			},
		},
		{
			// CRD 存在时除置 SVM 为 Running 外，还要在 CRD 上标记迁移进行中。
			name:      "Initial sync: CRD exists",
			svms:      []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets")},
			crd:       newCRD("widgets.example.com", 1, "v2"),
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVM("test-svm", "example.com", "widgets", metav1.Condition{
						Type:    string(svmv1.MigrationRunning),
						Status:  metav1.ConditionTrue,
						Reason:  "MigrationRunning",
						Message: "The migration is running",
					}),
				),
			},
			expectCRDActions: []k8stesting.Action{
				newCRDPatchAction(
					"widgets.example.com",
					[]apiextensionsv1.CustomResourceDefinitionCondition{
						{
							Type:    apiextensionsv1.StorageMigrating,
							Status:  apiextensionsv1.ConditionTrue,
							Reason:  "MigrationRunning",
							Message: "Migration test-svm is running",
						},
					},
					nil,
				),
			},
		},
		{
			// 已在运行中则不再产生任何写动作，避免重复同步。
			name: "Migration running (No-op)",
			svms: []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets", metav1.Condition{
				Type:   string(svmv1.MigrationRunning),
				Status: metav1.ConditionTrue,
			}, metav1.Condition{
				Type:   string(svmv1.MigrationRunning),
				Status: metav1.ConditionTrue,
			})},
			crd:       newCRD("widgets.example.com", 1, "v2"),
			expectErr: false,
		},
		{
			// 迁移成功且 generation 匹配时，清除 CRD 上的迁移标记并更新 storedVersions。
			name: "Migration succeeded: Update CRD stored versions",
			svms: []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets",
				metav1.Condition{
					Type:   string(svmv1.MigrationSucceeded),
					Status: metav1.ConditionTrue,
				},
				metav1.Condition{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			)},
			crd: func() *apiextensionsv1.CustomResourceDefinition {
				c := newCRD("widgets.example.com", 1, "v2")
				c.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{
					{
						Type:               apiextensionsv1.StorageMigrating,
						Status:             apiextensionsv1.ConditionTrue,
						ObservedGeneration: 1,
						Reason:             "MigrationRunning",
					},
				}
				return c
			}(),
			expectErr: false,
			expectCRDActions: []k8stesting.Action{
				newCRDPatchAction(
					"widgets.example.com",
					[]apiextensionsv1.CustomResourceDefinitionCondition{
						{
							Type:    apiextensionsv1.StorageMigrating,
							Status:  apiextensionsv1.ConditionFalse,
							Reason:  "MigrationSucceeded",
							Message: "The migration has succeeded and the stored versions have been updated",
						},
					},
					[]string{"v2"},
				),
			},
		},
		{
			// generation 变化说明 CRD 在迁移期间被修改，不能覆盖其 storedVersions。
			name: "Migration succeeded: CRD generation mismatch (No update)",
			svms: []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets",
				metav1.Condition{
					Type:   string(svmv1.MigrationSucceeded),
					Status: metav1.ConditionTrue,
				},
				metav1.Condition{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			)},
			crd:       newCRD("widgets.example.com", 2, "v2"),
			expectErr: false,
			expectCRDActions: []k8stesting.Action{
				newCRDPatchAction(
					"widgets.example.com",
					[]apiextensionsv1.CustomResourceDefinitionCondition{
						{
							Type:    apiextensionsv1.StorageMigrating,
							Status:  apiextensionsv1.ConditionFalse,
							Reason:  "MigrationSucceeded",
							Message: "The migration has succeeded but the stored versions have not been updated due to a generation mismatch",
						},
					},
					nil,
				),
			},
		},
		{
			// 迁移失败时清除 CRD 迁移标记，并保留 storedVersions 不变。
			name: "Migration failed: CRD condition updated",
			svms: []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets",
				metav1.Condition{
					Type:   string(svmv1.MigrationFailed),
					Status: metav1.ConditionTrue,
				},
				metav1.Condition{
					Type:   string(svmv1.MigrationRunning),
					Status: metav1.ConditionTrue,
				},
			)},
			crd: func() *apiextensionsv1.CustomResourceDefinition {
				c := newCRD("widgets.example.com", 1, "v2")
				c.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{
					{
						Type:               apiextensionsv1.StorageMigrating,
						Status:             apiextensionsv1.ConditionTrue,
						ObservedGeneration: 1,
						Reason:             "MigrationRunning",
					},
				}
				return c
			}(),
			expectErr: false,
			expectCRDActions: []k8stesting.Action{
				newCRDPatchAction(
					"widgets.example.com",
					[]apiextensionsv1.CustomResourceDefinitionCondition{
						{
							Type:    apiextensionsv1.StorageMigrating,
							Status:  apiextensionsv1.ConditionFalse,
							Reason:  "MigrationFailed",
							Message: "The migration has failed",
						},
					},
					[]string{"v2"},
				),
			},
		},
		{
			// 迁移完成后清理 admission 阶段设置的 Running 条件，改为 Completed。
			name: "Cleanup admission updates SVM condition",
			svms: []*svmv1.StorageVersionMigration{newSVM("test-svm", "example.com", "widgets",
				metav1.Condition{
					Type:   string(svmv1.MigrationSucceeded),
					Status: metav1.ConditionTrue,
				},
				metav1.Condition{
					Type:               string(svmv1.MigrationRunning),
					Status:             metav1.ConditionTrue,
					Reason:             "MigrationRunning",
					ObservedGeneration: 1,
				},
			)},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateSubresourceAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"status",
					"",
					newSVM("test-svm", "example.com", "widgets",
						metav1.Condition{
							Type:   string(svmv1.MigrationSucceeded),
							Status: metav1.ConditionTrue,
						},
						metav1.Condition{
							Type:   string(svmv1.MigrationRunning),
							Status: metav1.ConditionFalse,
							Reason: "MigrationCompleted",
						},
					),
				),
			},
		},
		{
			// 多个待处理迁移并存时按创建时间排序，优先提升更早创建的 SVM。
			name: "Promote older SVM first",
			svms: []*svmv1.StorageVersionMigration{
				func() *svmv1.StorageVersionMigration {
					svm := newSVM("test-svm-newer", "example.com", "widgets")
					svm.CreationTimestamp = metav1.Time{Time: time.Now().Add(1 * time.Hour)}
					return svm
				}(),
				func() *svmv1.StorageVersionMigration {
					svm := newSVM("test-svm-older", "example.com", "widgets")
					svm.CreationTimestamp = metav1.Time{Time: time.Now().Add(-1 * time.Hour)}
					return svm
				}(),
			},
			expectErr: false,
			expectKubeActions: []k8stesting.Action{
				k8stesting.NewUpdateAction(
					svmv1.SchemeGroupVersion.WithResource("storageversionmigrations"),
					"",
					newSVM("test-svm-older", "example.com", "widgets", metav1.Condition{
						Type:    string(svmv1.MigrationRunning),
						Status:  metav1.ConditionTrue,
						Reason:  "MigrationRunning",
						Message: "The migration is running",
					}),
				),
			},
		},
	}

	// filterActions 过滤掉 list/watch/get 等只读动作，只保留写动作用于断言。
	filterActions := func(actions []k8stesting.Action) []k8stesting.Action {
		var ret []k8stesting.Action
		for _, action := range actions {
			// informer 启动会产生只读动作，与被测同步逻辑无关，需剔除。
			if action.GetVerb() == "list" || action.GetVerb() == "watch" || action.GetVerb() == "get" {
				continue
			}
			ret = append(ret, action)
		}
		return ret
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			// 用初始 SVM 列表构造伪造的 kube 客户端与 informer 存储。
			var initialSVMs []runtime.Object
			for _, svm := range tc.svms {
				initialSVMs = append(initialSVMs, svm)
			}
			kubeClient := kubefake.NewClientset(initialSVMs...)

			kubeInformerFactory := svminformers.NewSharedInformerFactory(kubeClient, 0)
			svmInformer := kubeInformerFactory.Storagemigration().V1().StorageVersionMigrations()
			// 控制器从 informer 存储中读取对象，需预先填充用例数据。
			for _, svm := range tc.svms {
				err := svmInformer.Informer().GetStore().Add(svm)
				require.NoError(t, err)
			}

			var initialCRDs []runtime.Object
			if tc.crd != nil {
				initialCRDs = append(initialCRDs, tc.crd)
			}
			crdClientSet := apiextensionsfake.NewClientset(initialCRDs...)
			// 用对象跟踪器（object tracker）响应补丁请求，从而记录 CRD 的写动作。

			crdScheme := runtime.NewScheme()
			_ = apiextensionsv1.AddToScheme(crdScheme)
			crdTracker := k8stesting.NewObjectTracker(crdScheme, serializer.NewCodecFactory(crdScheme).UniversalDecoder())

			for _, obj := range initialCRDs {
				_ = crdTracker.Add(obj)
			}

			crdClientSet.PrependReactor("*", "*", k8stesting.ObjectReaction(crdTracker))

			controller := NewCustomResourceController(
				ctx,
				kubeClient,
				svmInformer,
				crdClientSet.ApiextensionsV1().CustomResourceDefinitions(),
			)
			defer controller.queue.ShutDown()
			// 无 SVM 时使用默认资源；有 SVM 时以第一个 SVM 的资源作为同步目标。

			resource := metav1.GroupResource{Group: "example.com", Resource: "widgets"}
			if len(tc.svms) > 0 {
				resource = tc.svms[0].Spec.Resource
			}

			err := controller.sync(ctx, resource)

			// 错误预期按用例区分，避免遗漏期望的失败分支。
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectKubeActions != nil {
				kubeActions := filterActions(kubeClient.Actions())
				require.Len(t, kubeActions, len(tc.expectKubeActions), "mismatched number of kube client actions")

				for i, expected := range tc.expectKubeActions {
					actual := kubeActions[i]
					require.Equal(t, expected.GetVerb(), actual.GetVerb(), "kube action %d: verb mismatch", i)
					require.Equal(t, expected.GetResource(), actual.GetResource(), "kube action %d: resource mismatch", i)

					// Update 动作需逐字段比对 SVM 名称与条件，Reason 为空时不参与比较。
					if updateAction, ok := actual.(k8stesting.UpdateAction); ok {
						actualObj := updateAction.GetObject().(*svmv1.StorageVersionMigration)
						expectedObj := expected.(k8stesting.UpdateAction).GetObject().(*svmv1.StorageVersionMigration)

						require.Equal(t, expectedObj.Name, actualObj.Name, "object name mismatch")
						require.Len(t, actualObj.Status.Conditions, len(expectedObj.Status.Conditions), "condition count mismatch")
						for j, expCond := range expectedObj.Status.Conditions {
							actCond := actualObj.Status.Conditions[j]
							require.Equal(t, expCond.Type, actCond.Type, "condition type mismatch")
							require.Equal(t, expCond.Status, actCond.Status, "condition status mismatch")
							// 有些条件未设置 Reason，仅校验明确指定的字段。
							if expCond.Reason != "" {
								require.Equal(t, expCond.Reason, actCond.Reason, "condition reason mismatch")
							}
						}
					}
				}
			}

			// 校验 CRD 状态的更新动作。
			if tc.expectCRDActions != nil {
				crdActions := filterActions(crdClientSet.Actions())
				require.Len(t, crdActions, len(tc.expectCRDActions), "mismatched number of crd client actions")

				for i, expected := range tc.expectCRDActions {
					actual := crdActions[i]
					require.Equal(t, expected.GetVerb(), actual.GetVerb(), "crd action %d: verb mismatch", i)

					if patchAction, ok := actual.(k8stesting.PatchAction); ok {
						actualPatch := patchAction.GetPatch()
						expectedPatch := expected.(k8stesting.PatchAction).GetPatch()

						var actualMap, expectedMap map[string]interface{}
						require.NoError(t, json.Unmarshal(actualPatch, &actualMap))
						require.NoError(t, json.Unmarshal(expectedPatch, &expectedMap))

						// LastTransitionTime 由时钟决定，逐字段比对条件时将其忽略。
						actualStatus := actualMap["status"].(map[string]interface{})
						expectedStatus := expectedMap["status"].(map[string]interface{})

						if expectedConds, ok := expectedStatus["conditions"].([]interface{}); ok {
							actualConds := actualStatus["conditions"].([]interface{})
							require.Len(t, actualConds, len(expectedConds))
							for k, expC := range expectedConds {
								expCondMap := expC.(map[string]interface{})
								actCondMap := actualConds[k].(map[string]interface{})
								require.Equal(t, expCondMap["type"], actCondMap["type"])
								require.Equal(t, expCondMap["status"], actCondMap["status"])
								require.Equal(t, expCondMap["reason"], actCondMap["reason"])
								require.Equal(t, expCondMap["message"], actCondMap["message"])
							}
							// 条件已单独校验，删除后即可对补丁其余部分做整体比较。
							delete(actualStatus, "conditions")
							delete(expectedStatus, "conditions")
						}

						require.Equal(t, expectedStatus, actualStatus)
					}
				}
			}
		})
	}
}
