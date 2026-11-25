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

package patch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/client-go/openapi/openapitest"
	"k8s.io/utils/ptr"
)

// TestApplyConfiguration 验证 ApplyConfiguration 风格（对象字面量）的 mutating 表达式
// 如何通过结构化合并差异（SMD）作用到已有对象上。
// 覆盖点：listType=map 的增改语义、条件表达式、oldObject 可见性、
// 原子字段/非法类型/非法字段等错误路径，以及与 SSA 一致的重复键合并行为。
func TestApplyConfiguration(t *testing.T) {
	deploymentGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	tests := []struct {
		name              string
		expression        string
		gvr               schema.GroupVersionResource
		object, oldObject runtime.Object
		expectedResult    runtime.Object
		expectedErr       string
	}{
		{
			// volumes 在 schema 中是 listType=map/listMapKey=name，
			// 因此同名条目应合并而不是整体替换。
			name: "apply configuration add to listType=map",
			expression: `Object{
					spec: Object.spec{
						template: Object.spec.template{
							spec: Object.spec.template.spec{
								volumes: [Object.spec.template.spec.volumes{
									name: "y"
								}]
							}
						}
					}
				}`,
			gvr: deploymentGVR,
			object: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{Name: "x"}},
					},
				},
			}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{Name: "x"}, {Name: "y"}},
					},
				},
			}},
		},
		{
			// 只写 name 键即可命中已有条目，其余字段按合并语义保留。
			name: "apply configuration update listType=map entry",
			expression: `Object{
					spec: Object.spec{
						template: Object.spec.template{
							spec: Object.spec.template.spec{
								volumes: [Object.spec.template.spec.volumes{
									name: "y",
									hostPath: Object.spec.template.spec.volumes.hostPath{
										path: "a"
									}
								}]
							}
						}
					}
				}`,
			gvr: deploymentGVR,
			object: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{Name: "x"}, {Name: "y"}},
					},
				},
			}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Volumes: []corev1.Volume{{Name: "x"}, {Name: "y", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "a"}}}},
					},
				},
			}},
		},
		{
			// 表达式内可使用三元运算符等纯函数逻辑，结果直接决定写入值。
			name: "apply configuration with conditionals",
			expression: `Object{
					spec: Object.spec{
						replicas: object.spec.replicas % 2 == 0?object.spec.replicas + 1:object.spec.replicas
					}
				}`,
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](2)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)}},
		},
		{
			// 更新场景下 oldObject 必须可见，否则无法实现“基于旧值计算新值”的策略。
			name: "apply configuration with old object",
			expression: `Object{
					spec: Object.spec{
						replicas: oldObject.spec.replicas % 2 == 0?oldObject.spec.replicas + 1:oldObject.spec.replicas
					}
				}`,
			gvr:            deploymentGVR,
			object:         &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			oldObject:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](2)}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](3)}},
		},
		{
			// 从空对象出发构造一个完整 Pod 模板，验证嵌套结构、map 字面量
			// 以及 resource.Quantity 等特殊类型都能正确转换。
			name: "complex apply configuration initialization",
			expression: `Object{
					spec: Object.spec{
						replicas: 1,
						template: Object.spec.template{
							metadata: Object.spec.template.metadata{
								labels: {"app": "nginx"}
							},
							spec: Object.spec.template.spec{
								containers: [Object.spec.template.spec.containers{
									name: "nginx",
									image: "nginx:1.14.2",
									ports: [Object.spec.template.spec.containers.ports{
										containerPort: 80
									}],
									resources: Object.spec.template.spec.containers.resources{
										limits: {"cpu": "128M"},
									}
								}]
							}
						}
					}
				}`,

			gvr:    deploymentGVR,
			object: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{}},
			expectedResult: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To[int32](1),
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"app": "nginx"},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{
							Name:  "nginx",
							Image: "nginx:1.14.2",
							Ports: []corev1.ContainerPort{
								{ContainerPort: 80},
							},
							Resources: corev1.ResourceRequirements{
								Limits: corev1.ResourceList{corev1.ResourceName("cpu"): resource.MustParse("128M")},
							},
						}},
					},
				},
			}},
		},
		{
			// selector 在 schema 中被标记为原子字段，不允许合并修改；
			// 拒绝而非静默覆盖，避免策略意外破坏不可变的选择器语义。
			name: "apply configuration with change to atomic",
			expression: `Object{
					spec: Object.spec{
						selector: Object.spec.selector{
							matchLabels: {"l": "v"}
						}
					}
				}`,
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "error applying patch: invalid ApplyConfiguration: may not mutate atomic arrays, maps or structs: .spec.selector",
		},
		{
			// 类型名必须能由字段路径推导出来，防止表达式引用任意 OpenAPI 类型。
			name: "apply configuration with invalid type name",
			expression: `Object{
					spec: Object.specx{
						replicas: 1
					}
				}`,
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "type mismatch: unexpected type name \"Object.specx\", expected \"Object.spec\", which matches field name path from root Object type",
		},
		{
			// 字段不存在应在转换为类型化对象时被发现并报错，而不是被忽略。
			name: "apply configuration with invalid field name",
			expression: `Object{
					spec: Object.spec{
						replicasx: 1
					}
				}`,
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "error applying patch: failed to convert patch object to typed object: .spec.replicasx: field not declared in schema",
		},
		{
			// 根表达式必须返回 Object 类型；返回标量属于策略编写错误。
			name:        "apply configuration with invalid return type",
			expression:  `"I'm a teapot!"`,
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "must evaluate to Object but got string",
		},
		{
			// 根类型必须正是根对象类型，不能是其子结构（如 Object.spec.metadata）。
			name:        "apply configuration with invalid initializer return type",
			expression:  `Object.spec.metadata{}`,
			gvr:         deploymentGVR,
			object:      &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: ptr.To[int32](1)}},
			expectedErr: "must evaluate to Object but got Object.spec.metadata",
		},
		{
			// 原对象中存在重复键的 listType=map 列表（env 两条 name=test），
			// 但补丁未触碰该列表时应原样保留，说明合并不会无条件去重。
			name: "apply configuration with duplicate env vars in original object",
			expression: `Object{
					spec: Object.spec{
						replicas: 3
					}
				}`,
			gvr: deploymentGVR,
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: "nginx",
								Env: []corev1.EnvVar{
									{Name: "test", Value: "a"},
									{Name: "test", Value: "b"},
								},
							}},
						},
					},
				},
			},
			expectedResult: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](3),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: "nginx",
								Env: []corev1.EnvVar{
									{Name: "test", Value: "a"},
									{Name: "test", Value: "b"},
								},
							}},
						},
					},
				},
			},
		},
		// 该用例验证：即使原对象含有重复项，修改已存在的环境变量也能正确生效。
		// 由于 schema 中 env 被声明为 `listType=map` 且 `listMapKey=name`，
		// 结构化合并差异（SMD）以 'name' 作为唯一键。
		// 当补丁更新 name="test" 的条目时，SMD 会合并这些变更。
		// 作为对 map 型列表执行合并的副作用，原对象中同一键的重复条目
		// 会在结果中被归并为单一条目。
		// 这与 Server-Side Apply（SSA）和 kubectl apply 的行为一致。
		{
			name: "apply configuration modify existing env variable",
			expression: `Object{
					spec: Object.spec{
						template: Object.spec.template{
							spec: Object.spec.template.spec{
								containers: [Object.spec.template.spec.containers{
									name: "nginx",
									env: [Object.spec.template.spec.containers.env{
										name: "test",
										value: "c"
									}]
								}]
							}
						}
					}
				}`,
			gvr: deploymentGVR,
			object: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: "nginx",
								Env: []corev1.EnvVar{
									{Name: "test", Value: "a"},
									{Name: "test", Value: "b"},
									{Name: "foo", Value: "bar"},
								},
							}},
						},
					},
				},
			},
			expectedResult: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Replicas: ptr.To[int32](1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name: "nginx",
								Env: []corev1.EnvVar{
									{Name: "test", Value: "c"},
									{Name: "foo", Value: "bar"},
								},
							}},
						},
					},
				},
			},
		},
	}

	// 编译器与类型转换器在整个测试中复用：编译一次表达式即可，避免重复开销。
	compiler, err := cel.NewCompositedCompiler(environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()))
	if err != nil {
		t.Fatal(err)
	}

	// TypeConverterManager 异步加载 OpenAPI schema；下面轮询等待 Deployment 的转换器就绪，
	// 否则 ApplyConfiguration 无法解析类型。超时即失败，避免测试挂起。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tcManager := NewTypeConverterManager(nil, openapitest.NewEmbeddedFileClient())
	go tcManager.Run(ctx)

	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Second, true, func(context.Context) (done bool, err error) {
		converter := tcManager.GetTypeConverter(deploymentGVK)
		return converter != nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// HasPatchTypes 打开 patch 类型（Object/spec 等）的变量声明，
			// 这是 ApplyConfiguration 表达式能使用类型化字面量的前提。
			accessor := &ApplyConfigurationCondition{Expression: tc.expression}
			compileResult := compiler.CompileMutatingEvaluator(accessor, cel.OptionalVariableDeclarations{HasPatchTypes: true}, environment.StoredExpressions)

			patcher := applyConfigPatcher{expressionEvaluator: compileResult}

			scheme := runtime.NewScheme()
			err := appsv1.AddToScheme(scheme)
			if err != nil {
				t.Fatal(err)
			}

			// 从对象反查 GVK，并写回 TypeMeta，使后续转换与 schema 查找使用一致的类型标识。
			var gvk schema.GroupVersionKind
			gvks, _, err := scheme.ObjectKinds(tc.object)
			if err != nil {
				t.Fatal(err)
			}
			if len(gvks) == 1 {
				gvk = gvks[0]
			} else {
				t.Fatalf("Failed to find gvk for type: %T", tc.object)
			}

			metaAccessor, err := meta.Accessor(tc.object)
			if err != nil {
				t.Fatal(err)
			}

			typeAccessor, err := meta.TypeAccessor(tc.object)
			if err != nil {
				t.Fatal(err)
			}
			typeAccessor.SetKind(gvk.Kind)
			typeAccessor.SetAPIVersion(gvk.GroupVersion().String())

			attrs := admission.NewAttributesRecord(tc.object, tc.oldObject, gvk,
				metaAccessor.GetNamespace(), metaAccessor.GetName(), tc.gvr,
				"", admission.Create, &metav1.CreateOptions{}, false, nil)
			vAttrs := &admission.VersionedAttributes{
				Attributes:         attrs,
				VersionedKind:      gvk,
				VersionedObject:    admission.NewLazyObject(tc.object),
				VersionedOldObject: admission.NewLazyObject(tc.oldObject),
			}

			r := Request{
				MatchedResource:     tc.gvr,
				VersionedAttributes: vAttrs,
				ObjectInterfaces:    admission.NewObjectInterfacesFromScheme(scheme),
				OptionalVariables:   cel.OptionalVariableBindings{},
				TypeConverter:       tcManager.GetTypeConverter(gvk),
			}

			patched, err := patcher.Patch(ctx, r, celconfig.RuntimeCELCostBudget)
			// 错误用例只做子串匹配，因为底层错误消息可能带有路径前缀等额外上下文。
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

			// 统一转成 unstructured 比较：避免 typed 对象中零值与 nil 的差异干扰断言，
			// 也更接近策略实际作用在序列化对象上的语义。
			got, err := runtime.DefaultUnstructuredConverter.ToUnstructured(patched)
			if err != nil {
				t.Fatal(err)
			}

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
