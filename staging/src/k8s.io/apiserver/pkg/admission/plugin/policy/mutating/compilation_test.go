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
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/openapi/openapitest"
	"k8s.io/utils/ptr"
)

// TestCompilation 是对 mutatingEvaluator.compile 的白盒测试：
// 编译结果是若干 CEL program，需要手工调用它们来断言求值结果。
//
// 白盒测试：绕过准入流程，直接对编译产物（Mutators）调用 Patch，
// 从而精确断言 CEL 求值、patch 应用顺序、变量与参数绑定等行为。
func TestCompilation(t *testing.T) {
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	testCases := []struct {
		name           string
		policy         *v1.MutatingAdmissionPolicy
		gvr            schema.GroupVersionResource
		object         runtime.Object
		oldObject      runtime.Object
		params         runtime.Object
		namespace      *corev1.Namespace
		expectedErr    string
		expectedResult runtime.Object
	}{
		{
			// 多个 mutation 按声明顺序依次应用：先 +100 再 replace 为当前值 +10，结果 111。
			name: "applyConfiguration then jsonPatch",
			policy: mutations(policy("d1"), v1.Mutation{
				PatchType: v1.PatchTypeApplyConfiguration,
				ApplyConfiguration: &v1.ApplyConfiguration{
					Expression: `Object{
									spec: Object.spec{
										replicas: object.spec.replicas + 100
									}
								}`,
				},
			},
				v1.Mutation{
					PatchType: v1.PatchTypeJSONPatch,
					JSONPatch: &v1.JSONPatch{
						Expression: `[
							JSONPatch{op: "replace", path: "/spec/replicas", value: object.spec.replicas + 10}
						]`,
					},
				}),
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](111)}},
		},
		{
			// 顺序颠倒后结果仍为 111：JSONPatch 先把 1 改为 11，applyConfiguration 再 +100。
			name: "jsonPatch then applyConfiguration",
			policy: mutations(policy("d1"),
				v1.Mutation{
					PatchType: v1.PatchTypeJSONPatch,
					JSONPatch: &v1.JSONPatch{
						Expression: `[
							JSONPatch{op: "replace", path: "/spec/replicas", value: object.spec.replicas + 10}
						]`,
					},
				},
				v1.Mutation{
					PatchType: v1.PatchTypeApplyConfiguration,
					ApplyConfiguration: &v1.ApplyConfiguration{
						Expression: `Object{
									spec: Object.spec{
										replicas: object.spec.replicas + 100
									}
								}`,
					},
				}),
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](111)}},
		},
		{
			// 变量在 mutation 之前求值，可在表达式中通过 variables 引用。
			name: "jsonPatch with variable",
			policy: jsonPatches(variables(policy("d1"), v1.Variable{Name: "desired", Expression: "10"}), v1.JSONPatch{
				Expression: `[
					JSONPatch{op: "replace", path: "/spec/replicas", value: variables.desired + 1}, 
				]`,
			}),
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](11)}},
		},
		{
			// applyConfiguration 表达式同样可以引用变量。
			name: "apply configuration with variable",
			policy: applyConfigurations(variables(policy("d1"), v1.Variable{Name: "desired", Expression: "10"}),
				`Object{
					spec: Object.spec{
						replicas: variables.desired + 1
					}
				}`),
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](11)}},
		},
		{
			// params 按 ParamKind 解析为对象，通过 params 变量访问；ConfigMap 数据是字符串，需显式转换。
			name: "apply configuration with params",
			policy: paramKind(applyConfigurations(policy("d1"),
				`Object{
					spec: Object.spec{
						replicas: int(params.data['k1'])
					}
				}`), &v1.ParamKind{Kind: "ConfigMap", APIVersion: "v1"}),
			params:         &corev1.ConfigMap{Data: map[string]string{"k1": "100"}},
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](100)}},
		},
		{
			// 嵌套 all() 使求值代价超过预算，必须在求值期被中断，
			// 防止恶意或低效表达式耗尽 apiserver CPU。
			name: "jsonPatch with excessive cost",
			policy: jsonPatches(variables(policy("d1"), v1.Variable{Name: "list", Expression: "[0,1,2,3,4,5,6,7,8,9]"}), v1.JSONPatch{
				Expression: `[
					JSONPatch{op: "replace", path: "/spec/replicas", 
						value: variables.list.all(x1, variables.list.all(x2, variables.list.all(x3, variables.list.all(x4, variables.list.all(x5, variables.list.all(x5, "0123456789" == "0123456789"))))))? 1 : 0
					}
				]`,
			}),
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "operation cancelled: actual cost limit exceeded",
		},
		{
			// applyConfiguration 路径同样受 CEL 代价预算约束。
			name: "applyConfiguration with excessive cost",
			policy: variables(applyConfigurations(policy("d1"),
				`Object{
					spec: Object.spec{
						replicas: variables.list.all(x1, variables.list.all(x2, variables.list.all(x3, variables.list.all(x4, variables.list.all(x5, variables.list.all(x5, "0123456789" == "0123456789"))))))? 1 : 0
					}
				}`), v1.Variable{Name: "list", Expression: "[0,1,2,3,4,5,6,7,8,9]"}),
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "operation cancelled: actual cost limit exceeded",
		},
		{
			// request 变量暴露 GVK 等请求属性，表达式可据此按资源类型分支。
			name: "request variable",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{op: "replace", path: "/spec/replicas", 
						value: request.kind.group == 'apps' && request.kind.version == 'v1' && request.kind.kind == 'Deployment' ? 10 : 0
					}
				]`}),
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](10)}},
		},
		{
			// namespaceObject 只在提供了 namespace 时可用。
			name: "namespace request variable",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{op: "replace", path: "/spec/replicas", 
						value: namespaceObject.metadata.name == 'ns1' ? 10 : 0
					}
				]`}),
			namespace:      &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ns1"}},
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](10)}},
		},
		{
			// authorizer 变量允许策略在 CEL 中做二次授权检查；fakeAuthorizer 恒允许。
			name: "authorizer check",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{op: "replace", path: "/spec/replicas", 
						value: authorizer.group('').resource('endpoints').check('create').allowed() ? 10 : 0
					}
				]`,
			}),
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](10)}},
		},
		{
			// Object 类型构造后可直接读取字段。
			name: "object type has field access",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{
						op: "add", path: "/metadata/labels",
						value: {
							"value": Object{field: "fieldValue"}.field,
						}
					}
				]`,
			}),
			gvr:    deploymentGVR,
			object: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}},
			expectedResult: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"value": "fieldValue",
			}}},
		},
		{
			// has() 对未设置字段返回 false，对已设置字段返回 true。
			name: "object type has field testing",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{
						op: "add", path: "/metadata/labels",
						value: {
							"field": string(has(Object{field: "fieldValue"}.field)),
							"field-unset": string(has(Object{}.field)),
						}
					}
				]`,
			}),
			gvr:    deploymentGVR,
			object: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}},
			expectedResult: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"field":       "true",
				"field-unset": "false",
			}}},
		},
		{
			// Object 相等性按字段值比较，空对象相等。
			name: "object type equality",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{
						op: "add", path: "/metadata/labels",
						value: {
							"empty": string(Object{} == Object{}),
							"same": string(Object{field: "x"} == Object{field: "x"}),
							"different": string(Object{field: "x"} == Object{field: "y"}),
						}
					}
				]`,
			}),
			gvr:    deploymentGVR,
			object: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}},
			expectedResult: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"empty":     "true",
				"same":      "true",
				"different": "false",
			}}},
		},
		{
			// TODO: 该用例固化了既有行为，在 MutatingAdmissionPolicy 进入 beta
			// 之前应当修复。由于我们尚未执行类型完全绑定的整体编译，
			// 因此可能初始化出非法的 Object 类型（如 Object.invalid）。进入 beta 前，
			// 应在求值之前用完全绑定的类型重新编译所有表达式，
			// 并在初始化了此类非法 Object 类型时报告错误。
			name: "object types are not fully type checked",
			policy: jsonPatches(policy("d1"), v1.JSONPatch{
				Expression: `[
					JSONPatch{
						op: "add", path: "/spec",
						value: Object.invalid{replicas: 1}
					}
				]`,
			}),
			gvr:    deploymentGVR,
			object: &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}},
			expectedResult: &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
				},
			},
		},
	}

	scheme := runtime.NewScheme()
	err := appsv1.AddToScheme(scheme)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// TypeConverterManager 依赖 OpenAPI 文档，需要异步加载；用嵌入式客户端避免网络依赖。
	tcManager := patch.NewTypeConverterManager(nil, openapitest.NewEmbeddedFileClient())
	go tcManager.Run(ctx)

	// 等待 Deployment 的转换器就绪，否则 applyConfiguration 无法完成结构化合并。
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Second, true, func(context.Context) (done bool, err error) {
		converter := tcManager.GetTypeConverter(deploymentGVK)
		return converter != nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var gvk schema.GroupVersionKind
			gvks, _, err := scheme.ObjectKinds(tc.object)
			if err != nil {
				t.Fatal(err)
			}
			// 测试对象只注册到一个 GVK，多值说明 scheme 配置有问题。
			if len(gvks) == 1 {
				gvk = gvks[0]
			} else {
				t.Fatalf("Failed to find gvk for type: %T", tc.object)
			}

			policyEvaluator := compilePolicy(tc.policy)
			if policyEvaluator.CompositedCompiler != nil {
				// 复合编译器需要上下文承载类型信息与错误收集。
				ctx = policyEvaluator.CompositedCompiler.CreateContext(ctx)
			}
			obj := tc.object

			// 手工补全 TypeMeta：对象从 Go 结构体构造时缺少 kind/apiVersion。
			typeAccessor, err := meta.TypeAccessor(obj)
			if err != nil {
				t.Fatal(err)
			}
			typeAccessor.SetKind(gvk.Kind)
			typeAccessor.SetAPIVersion(gvk.GroupVersion().String())
			typeConverter := tcManager.GetTypeConverter(gvk)

			metaAccessor, err := meta.Accessor(obj)
			if err != nil {
				t.Fatal(err)
			}

			// 依次应用编译出的每个 mutation，后一个看到的是前一个的结果。
			for _, patcher := range policyEvaluator.Mutators {
				attrs := admission.NewAttributesRecord(obj, tc.oldObject, gvk,
					metaAccessor.GetNamespace(), metaAccessor.GetName(), tc.gvr,
					"", admission.Create, &metav1.CreateOptions{}, false, nil)
				vAttrs := &admission.VersionedAttributes{
					Attributes:         attrs,
					VersionedKind:      gvk,
					VersionedObject:    admission.NewLazyObject(obj),
					VersionedOldObject: admission.NewLazyObject(tc.oldObject),
				}
				r := patch.Request{
					MatchedResource:     tc.gvr,
					VersionedAttributes: vAttrs,
					ObjectInterfaces:    admission.NewObjectInterfacesFromScheme(scheme),
					OptionalVariables:   cel.OptionalVariableBindings{VersionedParams: tc.params, Authorizer: fakeAuthorizer{}},
					Namespace:           tc.namespace,
					TypeConverter:       typeConverter,
				}
				obj, err = patcher.Patch(ctx, r, celconfig.RuntimeCELCostBudget)
				if len(tc.expectedErr) > 0 {
					if err == nil {
						t.Fatalf("expected error: %s", tc.expectedErr)
					} else {
						if !strings.Contains(err.Error(), tc.expectedErr) {
							t.Fatalf("expected error: %s, got: %s", tc.expectedErr, err.Error())
						}
						return
					}
				}
				if err != nil && len(tc.expectedErr) == 0 {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			got, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				t.Fatal(err)
			}

			// 期望值也补齐 TypeMeta，否则比较会因缺少 apiVersion/kind 而失败。
			wantTypeAccessor, err := meta.TypeAccessor(tc.expectedResult)
			if err != nil {
				t.Fatal(err)
			}
			wantTypeAccessor.SetKind(gvk.Kind)
			wantTypeAccessor.SetAPIVersion(gvk.GroupVersion().String())

			want, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.expectedResult)
			if err != nil {
				t.Fatal(err)
			}
			if !equality.Semantic.DeepEqual(want, got) {
				t.Errorf("unexpected result, got diff:\n%s\n", cmp.Diff(want, got))
			}
		})
	}
}

// policy 构造一个指定名称的空策略，后续 helper 在此基础上追加字段。
func policy(name string) *v1.MutatingAdmissionPolicy {
	return &v1.MutatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.MutatingAdmissionPolicySpec{},
	}
}

// variables 追加 CEL 变量定义。
func variables(policy *v1.MutatingAdmissionPolicy, variables ...v1.Variable) *v1.MutatingAdmissionPolicy {
	policy.Spec.Variables = append(policy.Spec.Variables, variables...)
	return policy
}

// jsonPatches 追加 JSONPatch 类型的 mutation。
func jsonPatches(policy *v1.MutatingAdmissionPolicy, jsonPatches ...v1.JSONPatch) *v1.MutatingAdmissionPolicy {
	for _, jsonPatch := range jsonPatches {
		policy.Spec.Mutations = append(policy.Spec.Mutations, v1.Mutation{
			JSONPatch: &jsonPatch,
			PatchType: v1.PatchTypeJSONPatch,
		})
	}

	return policy
}

// applyConfigurations 追加 ApplyConfiguration 类型的 mutation。
func applyConfigurations(policy *v1.MutatingAdmissionPolicy, expressions ...string) *v1.MutatingAdmissionPolicy {
	for _, expression := range expressions {
		policy.Spec.Mutations = append(policy.Spec.Mutations, v1.Mutation{
			ApplyConfiguration: &v1.ApplyConfiguration{Expression: expression},
			PatchType:          v1.PatchTypeApplyConfiguration,
		})
	}
	return policy
}

// paramKind 设置策略的参数类型。
func paramKind(policy *v1.MutatingAdmissionPolicy, paramKind *v1.ParamKind) *v1.MutatingAdmissionPolicy {
	policy.Spec.ParamKind = paramKind
	return policy
}

// mutations 直接追加完整的 mutation 定义，用于需要显式控制顺序的用例。
func mutations(policy *v1.MutatingAdmissionPolicy, mutations ...v1.Mutation) *v1.MutatingAdmissionPolicy {
	policy.Spec.Mutations = append(policy.Spec.Mutations, mutations...)
	return policy
}

// matchConstraints 设置策略的匹配约束。
func matchConstraints(policy *v1.MutatingAdmissionPolicy, matchConstraints *v1.MatchResources) *v1.MutatingAdmissionPolicy {
	policy.Spec.MatchConstraints = matchConstraints
	return policy
}

// fakeAuthorizer 恒返回 Allow，隔离 authorizer 变量对测试结果的影响。
type fakeAuthorizer struct{}

func (f fakeAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	return authorizer.DecisionAllow, "", nil
}
