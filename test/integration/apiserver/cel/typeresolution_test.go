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

package cel

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/interpreter"

	"k8s.io/apiserver/pkg/cel/environment"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	nodev1 "k8s.io/api/node/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsscheme "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	commoncel "k8s.io/apiserver/pkg/cel"
	celopenapi "k8s.io/apiserver/pkg/cel/openapi"
	"k8s.io/apiserver/pkg/cel/openapi/resolver"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/utils/ptr"

	apiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	corev1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/generated/openapi"
	"k8s.io/kubernetes/test/integration/framework"
)

func TestTypeResolver(t *testing.T) {
	server, err := apiservertesting.StartTestServer(t, nil, nil, framework.SharedEtcd())
	if err != nil {
		t.Fatal(err)
	}
	defer server.TearDownFn()

	config := server.ClientConfig

	client, err := extclientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	crd, err := installCRD(client)
	if err != nil {
		t.Fatal(err)
	}
	defer func(crd *apiextensionsv1.CustomResourceDefinition) {
		err := client.ApiextensionsV1().CustomResourceDefinitions().Delete(context.Background(), crd.Name, metav1.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}(crd)
	discoveryResolver := &resolver.ClientDiscoveryResolver{Discovery: client.Discovery()}
	definitionsResolver := resolver.NewDefinitionsSchemaResolver(openapi.GetOpenAPIDefinitions, k8sscheme.Scheme, apiextensionsscheme.Scheme)
	// 等待 CRD schema 在 OpenAPI v3 endpoint 发布
	err = wait.PollImmediate(time.Second, time.Minute, func() (done bool, err error) {
		p, err := client.OpenAPIV3().Paths()
		if err != nil {
			return
		}
		if _, ok := p["apis/apis.example.com/v1beta1"]; ok {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timeout wait for CRD schema publication: %v", err)
	}

	for _, tc := range []struct {
		name                string
		obj                 runtime.Object
		expression          string
		expectResolutionErr bool
		expectCompileErr    bool
		expectEvalErr       bool
		expectedResult      any
		resolvers           []resolver.SchemaResolver
	}{
		{
			name: "unknown type",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"kind":       "Bad",
				"apiVersion": "bad.example.com/v1",
			}},
			expectResolutionErr: true,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},
		},
		{
			name:                "deployment",
			obj:                 sampleReplicatedDeployment(),
			expression:          "self.spec.replicas > 1",
			expectResolutionErr: false,
			expectCompileErr:    false,
			expectEvalErr:       false,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},

			// 期望一个布尔值，即 `true`。
			expectedResult: true,
		},
		{
			name:                "missing field",
			obj:                 sampleReplicatedDeployment(),
			expression:          "self.spec.missing > 1",
			expectResolutionErr: false,
			expectCompileErr:    true,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},
		},
		{
			name:                "mistyped expression",
			obj:                 sampleReplicatedDeployment(),
			expression:          "self.spec.replicas == '1'",
			expectResolutionErr: false,
			expectCompileErr:    true,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},
		},
		{
			name: "crd valid",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"kind":       "CronTab",
				"apiVersion": "apis.example.com/v1beta1",
				"spec": map[string]any{
					"cronSpec": "* * * * *",
					"image":    "foo-image",
					"replicas": 2,
				},
			}},
			expression:          "self.spec.replicas > 1",
			expectResolutionErr: false,
			expectCompileErr:    false,
			expectEvalErr:       false,
			resolvers:           []resolver.SchemaResolver{discoveryResolver},

			// 期望一个布尔值，即 `true`。
			expectedResult: true,
		},
		{
			name: "crd missing field",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"kind":       "CronTab",
				"apiVersion": "apis.example.com/v1beta1",
				"spec": map[string]any{
					"cronSpec": "* * * * *",
					"image":    "foo-image",
					"replicas": 2,
				},
			}},
			expression:          "self.spec.missing > 1",
			expectResolutionErr: false,
			expectCompileErr:    true,
			resolvers:           []resolver.SchemaResolver{discoveryResolver},
		},
		{
			name: "crd mistyped",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"kind":       "CronTab",
				"apiVersion": "apis.example.com/v1beta1",
				"spec": map[string]any{
					"cronSpec": "* * * * *",
					"image":    "foo-image",
					"replicas": 2,
				},
			}},
			expression:          "self.spec.replica == '1'",
			expectResolutionErr: false,
			expectCompileErr:    true,
			resolvers:           []resolver.SchemaResolver{discoveryResolver},
		},
		{
			name: "items population",
			obj:  sampleReplicatedDeployment(),
			// `containers` 是一个数组，其元素类型为 `Container`
			// `ports` 是一个 `ContainerPort` 数组
			expression: "size(self.spec.template.spec.containers) > 0 &&" +
				"self.spec.template.spec.containers.all(c, c.ports.all(p, p.containerPort < 1024))",
			expectResolutionErr: false,
			expectCompileErr:    false,
			expectEvalErr:       false,
			expectedResult:      true,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},
		},
		{
			name: "int-or-string int",
			obj: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				Spec: appsv1.DeploymentSpec{
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxSurge: &intstr.IntOrString{Type: intstr.Int, IntVal: 5},
						},
					},
				},
			},
			expression: "has(self.spec.strategy.rollingUpdate) &&" +
				"type(self.spec.strategy.rollingUpdate.maxSurge) == int &&" +
				"self.spec.strategy.rollingUpdate.maxSurge > 1",
			expectResolutionErr: false,
			expectCompileErr:    false,
			expectEvalErr:       false,
			expectedResult:      true,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},
		},
		{
			name: "int-or-string string",
			obj: &appsv1.Deployment{
				TypeMeta: metav1.TypeMeta{
					Kind:       "Deployment",
					APIVersion: "apps/v1",
				},
				Spec: appsv1.DeploymentSpec{
					Strategy: appsv1.DeploymentStrategy{
						Type: appsv1.RollingUpdateDeploymentStrategyType,
						RollingUpdate: &appsv1.RollingUpdateDeployment{
							MaxSurge: &intstr.IntOrString{Type: intstr.String, StrVal: "10%"},
						},
					},
				},
			},
			expression: "has(self.spec.strategy.rollingUpdate) &&" +
				"type(self.spec.strategy.rollingUpdate.maxSurge) == string &&" +
				"self.spec.strategy.rollingUpdate.maxSurge == '10%'",
			expectResolutionErr: false,
			expectCompileErr:    false,
			expectEvalErr:       false,
			expectedResult:      true,
			resolvers:           []resolver.SchemaResolver{definitionsResolver, discoveryResolver},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gvk := tc.obj.GetObjectKind().GroupVersionKind()
			var s *spec.Schema
			for _, r := range tc.resolvers {
				var err error
				s, err = r.ResolveSchema(gvk)
				if err != nil {
					if tc.expectResolutionErr {
						return
					}
					t.Fatalf("cannot resolve type: %v", err)
				}
				if tc.expectResolutionErr {
					t.Fatalf("expected resulution error but got none")
				}
			}
			program, err := simpleCompileCEL(s, tc.expression)
			if err != nil {
				if tc.expectCompileErr {
					return
				}
				t.Fatalf("cannot eval: %v", err)
			}
			if tc.expectCompileErr {
				t.Fatalf("expected compilation error but got none")
			}
			unstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.obj)
			if err != nil {
				t.Fatal(err)
			}
			ret, _, err := program.Eval(&simpleActivation{self: celopenapi.UnstructuredToVal(unstructured, s)})
			if err != nil {
				if tc.expectEvalErr {
					return
				}
				t.Fatalf("cannot eval: %v", err)
			}
			if tc.expectEvalErr {
				t.Fatalf("expected eval error but got none")
			}
			if !reflect.DeepEqual(ret.Value(), tc.expectedResult) {
				t.Errorf("wrong result, expected %q but got %q", tc.expectedResult, ret)
			}
		})
	}

}

// TestBuiltinResolution 断言所有 resolver 实现都应该
// 无错误地解析 Kubernetes 内置类型。
func TestBuiltinResolution(t *testing.T) {
	// 首先，设置 server 和 client
	server, err := apiservertesting.StartTestServer(t, nil, nil, framework.SharedEtcd())
	if err != nil {
		t.Fatal(err)
	}
	defer server.TearDownFn()

	config := server.ClientConfig

	client, err := extclientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		resolver resolver.SchemaResolver
		scheme   *runtime.Scheme
	}{
		{
			name:     "definitions",
			resolver: resolver.NewDefinitionsSchemaResolver(openapi.GetOpenAPIDefinitions, k8sscheme.Scheme, apiextensionsscheme.Scheme),
			scheme:   buildTestScheme(),
		},
		{
			name:     "discovery",
			resolver: &resolver.ClientDiscoveryResolver{Discovery: client.Discovery()},
			scheme:   buildTestScheme(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for gvk := range tc.scheme.AllKnownTypes() {
				// 跳过 metav1 的别名
				if gvk.Kind == "APIGroup" || gvk.Kind == "APIGroupList" || gvk.Kind == "APIVersions" ||
					strings.HasSuffix(gvk.Kind, "Options") || strings.HasSuffix(gvk.Kind, "Event") {
					continue
				}
				// 跳过不会实际出现的私有类型、引用类型和别名类型
				if gvk.Kind == "SerializedReference" || gvk.Kind == "List" || gvk.Kind == "RangeAllocation" {
					continue
				}
				// 跳过内部类型
				if gvk.Version == "__internal" {
					continue
				}
				// apiextensions.k8s.io/v1beta1 未发布
				if tc.name == "discovery" && gvk.Group == "apiextensions.k8s.io" && gvk.Version == "v1beta1" {
					continue
				}
				// apiextensions.k8s.io 的 ConversionReview 未发布
				if tc.name == "discovery" && gvk.Group == "apiextensions.k8s.io" && gvk.Kind == "ConversionReview" {
					continue
				}
				_, err = tc.resolver.ResolveSchema(gvk)
				if err != nil {
					t.Errorf("resolver %q cannot resolve %v", tc.name, gvk)
				}
			}
		})
	}
}

// simpleCompileCEL 根据 schema 编译 CEL 表达式，
// 使用实际的默认值。
// `self` 定义为被求值的对象。
func simpleCompileCEL(schema *spec.Schema, expression string) (cel.Program, error) {
	env, err := environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()).Env(environment.NewExpressions)
	if err != nil {
		return nil, err
	}
	declType := celopenapi.SchemaDeclType(schema, true).MaybeAssignTypeName("selfType")
	rt := commoncel.NewDeclTypeProvider(declType)
	opts, err := rt.EnvOptions(env.CELTypeProvider())
	if err != nil {
		return nil, err
	}
	rootType, _ := rt.FindDeclType("selfType")
	opts = append(opts, cel.Variable("self", rootType.CelType()))
	env, err = env.Extend(opts...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expression)
	if issues != nil {
		return nil, issues.Err()
	}
	return env.Program(ast)
}

// sampleReplicatedDeployment 返回一个包含 2 个副本的示例 Deployment。
// 该对象没有内联，因为 Deployment 的 schema 是众所周知的，
// 因此在阅读测试用例时无需参考它。
func sampleReplicatedDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Deployment",
			APIVersion: "apps/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo-deployment",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](2),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app": "demo",
				},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app": "demo",
					},
				},
				Spec: apiv1.PodSpec{
					Containers: []apiv1.Container{
						{
							Name:  "web",
							Image: "nginx",
							Ports: []apiv1.ContainerPort{
								{
									Name:          "http",
									Protocol:      apiv1.ProtocolTCP,
									ContainerPort: 80,
								},
							},
						},
					},
				},
			},
		},
	}
}

func installCRD(apiExtensionClient extclientset.Interface) (*apiextensionsv1.CustomResourceDefinition, error) {
	// CRD 借鉴自 https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definitions/
	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: "crontabs.apis.example.com",
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "apis.example.com",
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "crontabs",
				Singular: "crontab",
				Kind:     "CronTab",
				ListKind: "CronTabList",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1beta1",
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							XPreserveUnknownFields: ptr.To(true),
							Type:                   "object",
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"spec": {
									Type: "object",
									Properties: map[string]apiextensionsv1.JSONSchemaProps{
										"cronSpec": {Type: "string"},
										"image":    {Type: "string"},
										"replicas": {Type: "integer"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	return apiExtensionClient.ApiextensionsV1().
		CustomResourceDefinitions().Create(context.Background(), crd, metav1.CreateOptions{})
}

type simpleActivation struct {
	self any
}

func (a *simpleActivation) ResolveName(name string) (interface{}, bool) {
	switch name {
	case "self":
		return a.self, true
	default:
		return nil, false
	}
}

func (a *simpleActivation) Parent() interpreter.Activation {
	return nil
}

func buildTestScheme() *runtime.Scheme {
	// 精心挑选的测试 API server 提供的 scheme
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = admissionregistrationv1.AddToScheme(scheme)
	_ = networkingv1.AddToScheme(scheme)
	_ = nodev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = apiextensionsscheme.AddToScheme(scheme)
	return scheme
}
