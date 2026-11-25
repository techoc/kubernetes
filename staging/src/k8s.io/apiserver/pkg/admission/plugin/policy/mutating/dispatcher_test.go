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

package mutating

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/openapi/openapitest"
	"k8s.io/utils/ptr"
)

// TestDispatcher 是 MutatingAdmissionPolicy 分发器的端到端用例。
//
// 每个用例给出对象、匹配规则与 mutation 表达式，断言补丁应用后的最终对象。
// 覆盖的要点包括：ApplyConfiguration 补丁、多策略叠加、匹配/不匹配、reinvocation
// 以及默认值在补丁后生效（策略写入的字段会被 defaulter 补齐）。
// 类型转换器基于内置的 OpenAPI 客户端预热，因此测试不依赖网络。
func TestDispatcher(t *testing.T) {
	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	testCases := []struct {
		name              string
		object, oldObject runtime.Object
		gvk               schema.GroupVersionKind
		gvr               schema.GroupVersionResource
		params            []runtime.Object // 本测试中所有参数都应为 ConfigMap。
		policyHooks       []PolicyHook
		expect            runtime.Object
	}{
		{
			name: "simple patch",
			gvk:  deploymentGVK,
			gvr:  deploymentGVR,
			object: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
				}},
			policyHooks: []PolicyHook{
				{
					Policy: mutations(matchConstraints(policy("policy1"), &admissionregistrationv1.MatchResources{
						MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
						ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
							{
								RuleWithOperations: admissionregistrationv1.RuleWithOperations{
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{"apps"},
										APIVersions: []string{"v1"},
										Resources:   []string{"deployments"},
									},
									Operations: []admissionregistrationv1.OperationType{"*"},
								},
							},
						},
					}), admissionregistrationv1.Mutation{
						PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
						ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
							Expression: `Object{
									spec: Object.spec{
										replicas: object.spec.replicas + 100
									}
								}`,
						}}),
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy1",
						},
					}},
				},
			},
			expect: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](101),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
		},
		{
			name: "with param",
			gvk:  deploymentGVK,
			gvr:  deploymentGVR,
			object: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
			params: []runtime.Object{
				&corev1.ConfigMap{
					TypeMeta: metav1.TypeMeta{
						APIVersion: "v1",
						Kind:       "ConfigMap",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:      "cm1",
						Namespace: "default",
					},
					Data: map[string]string{
						"key": "10",
					},
				},
			},
			policyHooks: []PolicyHook{
				{
					Policy: paramKind(mutations(matchConstraints(policy("policy1"), &admissionregistrationv1.MatchResources{
						MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
						ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
							{
								RuleWithOperations: admissionregistrationv1.RuleWithOperations{
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{"apps"},
										APIVersions: []string{"v1"},
										Resources:   []string{"deployments"},
									},
									Operations: []admissionregistrationv1.OperationType{"*"},
								},
							},
						}}),
						admissionregistrationv1.Mutation{
							PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
							ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
								Expression: `Object{
									spec: Object.spec{
										replicas: object.spec.replicas + int(params.data['key'])
									}
								}`,
							}}),
						&admissionregistrationv1.ParamKind{
							APIVersion: "v1",
							Kind:       "ConfigMap",
						}),
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy1",
							ParamRef:   &admissionregistrationv1.ParamRef{Name: "cm1", Namespace: "default"},
						},
					}},
				},
			},
			expect: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](11),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
		},
		{
			name: "both policies reinvoked",
			gvk:  deploymentGVK,
			gvr:  deploymentGVR,
			object: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
			policyHooks: []PolicyHook{
				{
					Policy: mutations(matchConstraints(policy("policy1"), &admissionregistrationv1.MatchResources{
						MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
						ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
							{
								RuleWithOperations: admissionregistrationv1.RuleWithOperations{
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{"apps"},
										APIVersions: []string{"v1"},
										Resources:   []string{"deployments"},
									},
									Operations: []admissionregistrationv1.OperationType{"*"},
								},
							},
						},
					}), admissionregistrationv1.Mutation{
						PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
						ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
							Expression: `Object{
									metadata: Object.metadata{
										labels: {"policy1": string(int(object.?metadata.labels["count"].orValue("1")) + 1)}
									}
								}`,
						}}),
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy1",
						},
					}},
				},
				{
					Policy: mutations(matchConstraints(policy("policy2"), &admissionregistrationv1.MatchResources{
						MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
						NamespaceSelector: &metav1.LabelSelector{},
						ObjectSelector:    &metav1.LabelSelector{},
						ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
							{
								RuleWithOperations: admissionregistrationv1.RuleWithOperations{
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{"apps"},
										APIVersions: []string{"v1"},
										Resources:   []string{"deployments"},
									},
									Operations: []admissionregistrationv1.OperationType{"*"},
								},
							},
						},
					}), admissionregistrationv1.Mutation{
						PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
						ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
							Expression: `Object{
									metadata: Object.metadata{
										labels: {"policy2": string(int(object.?metadata.labels["count"].orValue("1")) + 1)}
									}
								}`,
						}}),
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy2",
						},
					}},
				},
			},
			expect: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
					Labels: map[string]string{
						"policy1": "2",
						"policy2": "2",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
		},
		{
			name: "1st policy sets match condition that 2nd policy matches",
			gvk:  deploymentGVK,
			gvr:  deploymentGVR,
			object: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
			policyHooks: []PolicyHook{
				{
					Policy: &admissionregistrationv1.MutatingAdmissionPolicy{
						ObjectMeta: metav1.ObjectMeta{
							Name: "policy1",
						},
						Spec: admissionregistrationv1.MutatingAdmissionPolicySpec{
							MatchConstraints: &admissionregistrationv1.MatchResources{
								MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
								NamespaceSelector: &metav1.LabelSelector{},
								ObjectSelector:    &metav1.LabelSelector{},
								ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
									{
										RuleWithOperations: admissionregistrationv1.RuleWithOperations{
											Rule: admissionregistrationv1.Rule{
												APIGroups:   []string{"apps"},
												APIVersions: []string{"v1"},
												Resources:   []string{"deployments"},
											},
											Operations: []admissionregistrationv1.OperationType{"*"},
										},
									},
								},
							},
							Mutations: []admissionregistrationv1.Mutation{{
								PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
								ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
									Expression: `Object{
									metadata: Object.metadata{
										labels: {"environment": "production"}
									}
								}`}},
							},
						},
					},
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy1",
						},
					}},
				},
				{
					Policy: &admissionregistrationv1.MutatingAdmissionPolicy{
						ObjectMeta: metav1.ObjectMeta{
							Name: "policy2",
						},
						Spec: admissionregistrationv1.MutatingAdmissionPolicySpec{
							MatchConstraints: &admissionregistrationv1.MatchResources{
								MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
								NamespaceSelector: &metav1.LabelSelector{},
								ObjectSelector:    &metav1.LabelSelector{},
								ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
									{
										RuleWithOperations: admissionregistrationv1.RuleWithOperations{
											Rule: admissionregistrationv1.Rule{
												APIGroups:   []string{"apps"},
												APIVersions: []string{"v1"},
												Resources:   []string{"deployments"},
											},
											Operations: []admissionregistrationv1.OperationType{"*"},
										},
									},
								},
							},
							MatchConditions: []admissionregistrationv1.MatchCondition{
								{
									Name:       "prodonly",
									Expression: `object.?metadata.labels["environment"].orValue("") == "production"`,
								},
							},
							Mutations: []admissionregistrationv1.Mutation{{
								PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
								ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
									Expression: `Object{
									metadata: Object.metadata{
										labels: {"policy1invoked": "true"}
									}
								}`}},
							},
						},
					},
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy2",
						},
					}},
				},
			},
			expect: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
					Labels: map[string]string{
						"environment":    "production",
						"policy1invoked": "true",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
		},
		{
			// TODO: 该行为沿袭自 webhook 的 matchConditions，但值得重新审视
			name: "1st policy still does not match after 2nd policy sets match condition",
			gvk:  deploymentGVK,
			gvr:  deploymentGVR,
			object: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
			policyHooks: []PolicyHook{
				{
					Policy: &admissionregistrationv1.MutatingAdmissionPolicy{
						ObjectMeta: metav1.ObjectMeta{
							Name: "policy1",
						},
						Spec: admissionregistrationv1.MutatingAdmissionPolicySpec{
							MatchConstraints: &admissionregistrationv1.MatchResources{
								MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
								NamespaceSelector: &metav1.LabelSelector{},
								ObjectSelector:    &metav1.LabelSelector{},
								ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
									{
										RuleWithOperations: admissionregistrationv1.RuleWithOperations{
											Rule: admissionregistrationv1.Rule{
												APIGroups:   []string{"apps"},
												APIVersions: []string{"v1"},
												Resources:   []string{"deployments"},
											},
											Operations: []admissionregistrationv1.OperationType{"*"},
										},
									},
								},
							},
							MatchConditions: []admissionregistrationv1.MatchCondition{
								{
									Name:       "prodonly",
									Expression: `object.?metadata.labels["environment"].orValue("") == "production"`,
								},
							},
							Mutations: []admissionregistrationv1.Mutation{{
								PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
								ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
									Expression: `Object{
									metadata: Object.metadata{
										labels: {"policy1invoked": "true"}
									}
								}`}},
							},
						},
					},
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy1",
						},
					}},
				},
				{
					Policy: &admissionregistrationv1.MutatingAdmissionPolicy{
						ObjectMeta: metav1.ObjectMeta{
							Name: "policy2",
						},
						Spec: admissionregistrationv1.MutatingAdmissionPolicySpec{
							MatchConstraints: &admissionregistrationv1.MatchResources{
								MatchPolicy:       ptr.To(admissionregistrationv1.Equivalent),
								NamespaceSelector: &metav1.LabelSelector{},
								ObjectSelector:    &metav1.LabelSelector{},
								ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{
									{
										RuleWithOperations: admissionregistrationv1.RuleWithOperations{
											Rule: admissionregistrationv1.Rule{
												APIGroups:   []string{"apps"},
												APIVersions: []string{"v1"},
												Resources:   []string{"deployments"},
											},
											Operations: []admissionregistrationv1.OperationType{"*"},
										},
									},
								},
							},
							Mutations: []admissionregistrationv1.Mutation{{
								PatchType: admissionregistrationv1.PatchTypeApplyConfiguration,
								ApplyConfiguration: &admissionregistrationv1.ApplyConfiguration{
									Expression: `Object{
									metadata: Object.metadata{
										labels: {"environment": "production"}
									}
								}`}},
							},
						},
					},
					Bindings: []*admissionregistrationv1.MutatingAdmissionPolicyBinding{{
						ObjectMeta: metav1.ObjectMeta{Name: "binding"},
						Spec: admissionregistrationv1.MutatingAdmissionPolicyBindingSpec{
							PolicyName: "policy2",
						},
					}},
				},
			},
			expect: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "d1",
					Namespace: "default",
					Labels: map[string]string{
						"environment": "production",
					},
				},
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Volumes: []corev1.Volume{{Name: "x"}},
						},
					},
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
					},
				}},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcManager := patch.NewTypeConverterManager(nil, openapitest.NewEmbeddedFileClient())
	go tcManager.Run(ctx)

	// 预热类型转换器：轮询直到 Deployment 的 schema 可用，否则首个用例会拿到 503。
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Second, true, func(context.Context) (done bool, err error) {
		converter := tcManager.GetTypeConverter(deploymentGVK)
		return converter != nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	scheme := runtime.NewScheme()
	err = appsv1.AddToScheme(scheme)
	if err != nil {
		t.Fatal(err)
	}
	err = corev1.AddToScheme(scheme)
	if err != nil {
		t.Fatal(err)
	}

	// 注册伪造的 defaulter：完整 defaulter 会引入噪音并造成依赖环。
	scheme.AddTypeDefaultingFunc(&appsv1.Deployment{},
		func(obj interface{}) { fakeSetDefaultForDeployment(obj.(*appsv1.Deployment)) })

	objectInterfaces := admission.NewObjectInterfacesFromScheme(scheme)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := fake.NewClientset(tc.params...)

			// 始终包含 default 命名空间
			err := client.Tracker().Add(&corev1.Namespace{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Namespace",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "default",
				},
				Spec: corev1.NamespaceSpec{},
			})
			if err != nil {
				t.Fatal(err)
			}

			informerFactory := informers.NewSharedInformerFactory(client, 0)
			matcher := matching.NewMatcher(informerFactory.Core().V1().Namespaces().Lister(), client)
			paramInformer, err := informerFactory.ForResource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"})
			if err != nil {
				t.Fatal(err)
			}
			informerFactory.Start(ctx.Done())
			informerFactory.WaitForCacheSync(ctx.Done())
			for i, h := range tc.policyHooks {
				tc.policyHooks[i].ParamInformer = paramInformer
				tc.policyHooks[i].ParamScope = testParamScope{}
				tc.policyHooks[i].Evaluator = compilePolicy(h.Policy)
			}

			dispatcher := NewDispatcher(fakeAuthorizer{}, matcher, tcManager)
			err = dispatcher.Start(ctx)
			if err != nil {
				t.Fatalf("error starting dispatcher: %v", err)
			}

			metaAccessor, err := meta.Accessor(tc.object)
			if err != nil {
				t.Fatal(err)
			}

			attrs := admission.NewAttributesRecord(tc.object, tc.oldObject, tc.gvk,
				metaAccessor.GetNamespace(), metaAccessor.GetName(), tc.gvr,
				"", admission.Create, &metav1.CreateOptions{}, false, nil)
			vAttrs := &admission.VersionedAttributes{
				Attributes:         attrs,
				VersionedKind:      tc.gvk,
				VersionedObject:    admission.NewLazyObject(tc.object),
				VersionedOldObject: admission.NewLazyObject(tc.oldObject),
			}

			err = dispatcher.Dispatch(ctx, vAttrs, objectInterfaces, tc.policyHooks)
			if err != nil {
				t.Fatalf("error dispatching policy hooks: %v", err)
			}

			obj := vAttrs.VersionedObject.Object()
			if !equality.Semantic.DeepEqual(obj, tc.expect) {
				t.Errorf("unexpected result, got diff:\n%s\n", cmp.Diff(tc.expect, obj))
			}
		})
	}
}

// testParamScope 把测试参数当作命名空间级对象处理。
type testParamScope struct{}

// Name 返回命名空间作用域。
func (t testParamScope) Name() meta.RESTScopeName {
	return meta.RESTScopeNameNamespace
}

var _ meta.RESTScope = testParamScope{}

// fakeSetDefaultForDeployment 只补默认的更新策略类型，避免引入完整 defaulter 的
// 依赖与噪音，同时仍能验证「补丁后执行 defaulting」这一行为。
func fakeSetDefaultForDeployment(obj *appsv1.Deployment) {
	// 只默认化 strategy 类型，让测试有一个可观察的默认化字段
	strategy := &obj.Spec.Strategy
	if strategy.Type == "" {
		strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	}
}
