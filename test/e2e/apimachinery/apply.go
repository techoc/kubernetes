/*
Copyright 2021 The Kubernetes Authors.

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

package apimachinery

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	appsv1 "k8s.io/api/apps/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2edeployment "k8s.io/kubernetes/test/e2e/framework/deployment"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

var _ = SIGDescribe("ServerSideApply", func() {
	f := framework.NewDefaultFramework("apply")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	var client clientset.Interface
	var ns string

	ginkgo.BeforeEach(func() {
		client = f.ClientSet
		ns = f.Namespace.Name
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		_ = client.AppsV1().Deployments(ns).Delete(ctx, "deployment", metav1.DeleteOptions{})
		_ = client.AppsV1().Deployments(ns).Delete(ctx, "deployment-shared-unset", metav1.DeleteOptions{})
		_ = client.AppsV1().Deployments(ns).Delete(ctx, "deployment-shared-map-item-removal", metav1.DeleteOptions{})
		_ = client.CoreV1().Pods(ns).Delete(ctx, "test-pod", metav1.DeleteOptions{})
	})

	/*
		Release : v1.21
		Testname: Server Side Apply, Create
		Description: apply 一个对象。对不存在的对象执行 apply，MUST 创建该对象。
	*/
	ginkgo.It("should create an applied object if it does not already exist", func(ctx context.Context) {
		testCases := []struct {
			resource      string
			name          string
			body          string
			managedFields string
		}{
			{
				resource: "pods",
				name:     "test-pod",
				body: `{
				"apiVersion": "v1",
				"kind": "Pod",
				"metadata": {
					"name": "test-pod"
				},
				"spec": {
					"containers": [{
						"name":  "test-container",
						"image": "test-image"
					}]
				}
			}`,
				managedFields: `{"f:spec":{"f:containers":{"k:{\"name\":\"test-container\"}":{".":{},"f:image":{},"f:name":{}}}}}`,
			}, {
				resource: "services",
				name:     "test-svc",
				body: `{
				"apiVersion": "v1",
				"kind": "Service",
				"metadata": {
					"name": "test-svc"
				},
				"spec": {
					"ports": [{
						"port": 8080,
						"protocol": "UDP"
					}]
				}
			}`,
				managedFields: `{"f:spec":{"f:ports":{"k:{\"port\":8080,\"protocol\":\"UDP\"}":{".":{},"f:port":{},"f:protocol":{}}}}}`,
			},
		}

		for _, tc := range testCases {
			_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				Namespace(ns).
				Resource(tc.resource).
				Name(tc.name).
				Param("fieldManager", "apply_test").
				Body([]byte(tc.body)).
				Do(ctx).
				Get()
			if err != nil {
				framework.Failf("Failed to create object using Apply patch: %v", err)
			}

			_, err = client.CoreV1().RESTClient().Get().Namespace(ns).Resource(tc.resource).Name(tc.name).Do(ctx).Get()
			if err != nil {
				framework.Failf("Failed to retrieve object: %v", err)
			}

			// 验证使用不同的 field manager 重新 apply 不会产生冲突
			obj, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				Namespace(ns).
				Resource(tc.resource).
				Name(tc.name).
				Param("fieldManager", "apply_test_2").
				Body([]byte(tc.body)).
				Do(ctx).
				Get()
			if err != nil {
				framework.Failf("Failed to re-apply object using Apply patch: %v", err)
			}

			// 确认两个 applier 各自拥有相应的字段
			accessor, err := meta.Accessor(obj)
			framework.ExpectNoError(err, "getting ObjectMeta")
			managedFields := accessor.GetManagedFields()
			for _, entry := range managedFields {
				if entry.Manager == "apply_test_2" || entry.Manager == "apply_test" {
					if entry.FieldsV1.String() != tc.managedFields {
						framework.Failf("Expected managed fields %s, got %s", tc.managedFields, entry.FieldsV1.String())
					}
				}
			}
		}
	})

	/*
		Release : v1.21
		Testname: Server Side Apply, Subresource
		Description: apply 一个资源，随后对其 subresource 执行 apply。该 subresource MUST 被更新为 apply 对象中的内容。
	*/
	ginkgo.It("should work for subresources", func(ctx context.Context) {
		{
			testCases := []struct {
				resource    string
				name        string
				body        string
				statusPatch string
			}{
				{
					resource: "pods",
					name:     "test-pod",
					body: `{
				"apiVersion": "v1",
				"kind": "Pod",
				"metadata": {
					"name": "test-pod"
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}`,
					statusPatch: `{
				"apiVersion": "v1",
				"kind": "Pod",
				"metadata": {
					"name": "test-pod"
				},
				"status": {"conditions": [{"type": "MyStatus", "status":"True"}]}}`,
				},
			}

			for _, tc := range testCases {
				_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
					Namespace(ns).
					Resource(tc.resource).
					Name(tc.name).
					Param("fieldManager", "apply_test").
					Body([]byte(tc.body)).
					Do(ctx).
					Get()
				if err != nil {
					framework.Failf("Failed to create object using Apply patch: %v", err)
				}

				_, err = client.CoreV1().RESTClient().Get().Namespace(ns).Resource(tc.resource).Name(tc.name).Do(ctx).Get()
				if err != nil {
					framework.Failf("Failed to retrieve object: %v", err)
				}

				// 验证除非明确指向 subresource endpoint，否则 apply 不会更新 subresource
				_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
					Namespace(ns).
					Resource(tc.resource).
					Name(tc.name).
					Param("fieldManager", "apply_test2").
					Body([]byte(tc.statusPatch)).
					Do(ctx).
					Get()
				if err != nil {
					framework.Failf("Failed to Apply Status using Apply patch: %v", err)
				}
				pod, err := client.CoreV1().Pods(ns).Get(ctx, "test-pod", metav1.GetOptions{})
				framework.ExpectNoError(err, "retrieving test pod")
				for _, c := range pod.Status.Conditions {
					if c.Type == "MyStatus" {
						framework.Failf("Apply should not update subresources unless the endpoint is specifically specified")
					}
				}

				// 验证对 subresource 的 apply 会更新该 subresource
				_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
					Namespace(ns).
					Resource(tc.resource).
					SubResource("status").
					Name(tc.name).
					Param("fieldManager", "apply_test2").
					Body([]byte(tc.statusPatch)).
					Do(ctx).
					Get()
				if err != nil {
					framework.Failf("Failed to Apply Status using Apply patch: %v", err)
				}

				pod, err = client.CoreV1().Pods(ns).Get(ctx, "test-pod", metav1.GetOptions{})
				framework.ExpectNoError(err, "retrieving test pod")

				myStatusFound := false
				for _, c := range pod.Status.Conditions {
					if c.Type == "MyStatus" {
						myStatusFound = true
						break
					}
				}
				if myStatusFound == false {
					framework.Failf("Expected pod to have applied status")
				}
			}
		}
	})

	/*
		Release : v1.21
		Testname: Server Side Apply, unset field
		Description: apply 一个对象，随后再次 apply 以移除某个字段。该字段 MUST 被删除。
	*/
	ginkgo.It("should remove a field if it is owned but removed in the apply request", func(ctx context.Context) {
		obj := []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"replicas": 3,
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
						"ports": [{
							"containerPort": 80,
							"protocol": "TCP"
						}]
					}]
				}
			}
		}
	}`)

		_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment").
			Param("fieldManager", "apply_test").
			Body(obj).Do(ctx).Get()
		if err != nil {
			framework.Failf("Failed to create object using Apply patch: %v", err)
		}

		obj = []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"replicas": 3,
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}
		}
	}`)

		_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment").
			Param("fieldManager", "apply_test").
			Body(obj).Do(ctx).Get()
		if err != nil {
			framework.Failf("Failed to remove container port using Apply patch: %v", err)
		}

		deployment, err := client.AppsV1().Deployments(ns).Get(ctx, "deployment", metav1.GetOptions{})
		if err != nil {
			framework.Failf("Failed to retrieve object: %v", err)
		}

		if len(deployment.Spec.Template.Spec.Containers[0].Ports) > 0 {
			framework.Failf("Expected no container ports but got: %v, object: \n%#v", deployment.Spec.Template.Spec.Containers[0].Ports, deployment)
		}

	})

	/*
		Release : v1.21
		Testname: Server Side Apply, unset field shared
		Description: apply 一个对象。取消某个同时被其他 manager 拥有的字段的所有权，然后再次 apply。该字段 MUST NOT 从对象中被移除。
	*/
	ginkgo.It("should not remove a field if an owner unsets the field but other managers still have ownership of the field", func(ctx context.Context) {
		// spec.replicas 是可选的、会被默认值填充的字段
		// spec.template.spec.hostname 是可选的、不会被默认值填充的字段
		apply := []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment-shared-unset",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"replicas": 3,
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"hostname": "test-hostname",
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}
		}
	}`)

		for _, fieldManager := range []string{"shared_owner_1", "shared_owner_2"} {
			_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
				AbsPath("/apis/apps/v1").
				Namespace(ns).
				Resource("deployments").
				Name("deployment-shared-unset").
				Param("fieldManager", fieldManager).
				Body(apply).
				Do(ctx).
				Get()
			if err != nil {
				framework.Failf("Failed to create object using Apply patch: %v", err)
			}
		}

		// 移除 spec.replicas 和 spec.template.spec.hostname
		apply = []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment-shared-unset",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}
		}
	}`)

		patched, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment-shared-unset").
			Param("fieldManager", "shared_owner_1").
			Body(apply).
			Do(ctx).
			Get()
		if err != nil {
			framework.Failf("Failed to create object using Apply patch: %v", err)
		}

		deployment, ok := patched.(*appsv1.Deployment)
		if !ok {
			framework.Failf("Failed to convert response object to Deployment")
		}
		if *deployment.Spec.Replicas != 3 {
			framework.Failf("Expected deployment.spec.replicas to be 3, but got %d", deployment.Spec.Replicas)
		}
		if deployment.Spec.Template.Spec.Hostname != "test-hostname" {
			framework.Failf("Expected deployment.spec.template.spec.hostname to be \"test-hostname\", but got %s", deployment.Spec.Template.Spec.Hostname)
		}
	})

	/*
		Release : v1.21
		Testname: Server Side Apply, Force Apply
		Description: apply 一个对象。强制 apply 该对象的修改版本，使其与 managedFields 产生冲突。强制 apply MUST 成功更新该对象。
	*/
	ginkgo.It("should ignore conflict errors if force apply is used", func(ctx context.Context) {
		obj := []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"replicas": 3,
			"selector": {
				"matchLabels": {
					 "app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}
		}
	}`)
		_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment").
			Param("fieldManager", "apply_test").
			Body(obj).Do(ctx).Get()
		if err != nil {
			framework.Failf("Failed to create object using Apply patch: %v", err)
		}

		_, err = client.CoreV1().RESTClient().Patch(types.MergePatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment").
			Body([]byte(`{"spec":{"replicas": 5}}`)).Do(ctx).Get()
		if err != nil {
			framework.Failf("Failed to patch object: %v", err)
		}

		_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment").
			Param("fieldManager", "apply_test").
			Body(obj).Do(ctx).Get()
		if err == nil {
			framework.Failf("Expecting to get conflicts when applying object")
		}
		status, ok := err.(*apierrors.StatusError)
		if !(ok && apierrors.IsConflict(status)) {
			framework.Failf("Expecting to get conflicts as API error")
		}
		if len(status.Status().Details.Causes) < 1 {
			framework.Failf("Expecting to get at least one conflict when applying object, got: %v", status.Status().Details.Causes)
		}

		_, err = client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment").
			Param("force", "true").
			Param("fieldManager", "apply_test").
			Body(obj).Do(ctx).Get()
		if err != nil {
			framework.Failf("Failed to apply object with force: %v", err)
		}
	})

	/*
		Release : v1.21
		Testname: Server Side Apply, CRD
		Description: 创建 CRD 并 apply 一个自定义资源。后续不与之前请求冲突的 apply 应更新对象；引起冲突的 apply 应失败。
	*/
	ginkgo.It("should work for CRDs", func(ctx context.Context) {
		config, err := framework.LoadConfig()
		if err != nil {
			framework.Failf("%s", err)
		}
		apiExtensionClient, err := apiextensionclientset.NewForConfig(config)
		if err != nil {
			framework.Failf("%s", err)
		}
		dynamicClient, err := dynamic.NewForConfig(config)
		if err != nil {
			framework.Failf("%s", err)
		}

		noxuDefinition := fixtures.NewRandomNameMultipleVersionCustomResourceDefinition(apiextensionsv1.ClusterScoped)

		var c apiextensionsv1.CustomResourceValidation
		err = json.Unmarshal([]byte(`{
		"openAPIV3Schema": {
			"type": "object",
			"properties": {
				"spec": {
					"type": "object",
					"x-kubernetes-preserve-unknown-fields": true,
					"properties": {
						"cronSpec": {
							"type": "string",
							"pattern": "^(\\d+|\\*)(/\\d+)?(\\s+(\\d+|\\*)(/\\d+)?){4}$"
						},
						"ports": {
							"type": "array",
							"x-kubernetes-list-map-keys": [
								"containerPort",
								"protocol"
							],
							"x-kubernetes-list-type": "map",
							"items": {
								"properties": {
									"containerPort": {
										"format": "int32",
										"type": "integer"
									},
									"hostIP": {
										"type": "string"
									},
									"hostPort": {
										"format": "int32",
										"type": "integer"
									},
									"name": {
										"type": "string"
									},
									"protocol": {
										"type": "string"
									}
								},
								"required": [
									"containerPort",
									"protocol"
								],
								"type": "object"
							}
						}
					}
				}
			}
		}
	}`), &c)
		if err != nil {
			framework.Failf("%s", err)
		}
		for i := range noxuDefinition.Spec.Versions {
			noxuDefinition.Spec.Versions[i].Schema = &c
		}

		noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
		if err != nil {
			framework.Failf("cannot create crd %s", err)
		}

		defer func() {
			err = fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient)
			framework.ExpectNoError(err, "deleting CustomResourceDefinition")
		}()

		kind := noxuDefinition.Spec.Names.Kind
		apiVersion := noxuDefinition.Spec.Group + "/" + noxuDefinition.Spec.Versions[0].Name
		name := "mytest"

		rest := apiExtensionClient.Discovery().RESTClient()
		yamlBody := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: %s
  finalizers:
  - test-finalizer
spec:
  cronSpec: "* * * * */5"
  replicas: 1
  ports:
  - name: x
    containerPort: 80
    protocol: TCP`, apiVersion, kind, name))
		result, err := rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test").
			Body(yamlBody).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to create custom resource with apply: %v:\n%v", err, string(result))
		}
		verifyNumFinalizers(result, 1)
		verifyFinalizersIncludes(result, "test-finalizer")
		verifyReplicas(result, 1)
		verifyNumPorts(result, 1)

		// 验证 apply 在存在多个 resourceVersion 时仍能工作
		apiVersionBeta := noxuDefinition.Spec.Group + "/" + noxuDefinition.Spec.Versions[1].Name
		yamlBodyBeta := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: %s
spec:
  cronSpec: "* * * * */5"
  replicas: 1
  ports:
  - name: x
    containerPort: 80
    protocol: TCP`, apiVersionBeta, kind, name))
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[1].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test").
			Body(yamlBodyBeta).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to create custom resource with apply: %v:\n%v", err, string(result))
		}
		verifyReplicas(result, 1)
		verifyNumPorts(result, 1)

		// 测试结束后重置 finalizer，确保对象可被删除
		defer func() {
			result, err = rest.Patch(types.MergePatchType).
				AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
				Name(name).
				Body([]byte(`{"metadata":{"finalizers":[]}}`)).
				DoRaw(ctx)
			if err != nil {
				framework.Failf("failed to reset finalizers: %v:\n%v", err, string(result))
			}
		}()

		// patch 对象，向 finalizer 列表添加另一个 finalizer
		result, err = rest.Patch(types.MergePatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Body([]byte(`{"metadata":{"finalizers":["test-finalizer","another-one"]}}`)).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to add finalizer with merge patch: %v:\n%v", err, string(result))
		}
		verifyNumFinalizers(result, 2)
		verifyFinalizersIncludes(result, "test-finalizer")
		verifyFinalizersIncludes(result, "another-one")

		// 重新 apply 相同配置应正常成功，因为 finalizer 带有 list 类型扩展 'set'
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test").
			SetHeader("Accept", "application/json").
			Body(yamlBody).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to apply same config after adding a finalizer: %v:\n%v", err, string(result))
		}
		verifyNumFinalizers(result, 2)
		verifyFinalizersIncludes(result, "test-finalizer")
		verifyFinalizersIncludes(result, "another-one")

		// patch 对象以修改副本数
		result, err = rest.Patch(types.MergePatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Body([]byte(`{"spec":{"replicas": 5}}`)).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to update number of replicas with merge patch: %v:\n%v", err, string(result))
		}
		verifyReplicas(result, 5)

		// 再次 apply 应产生冲突，因为副本数已被修改
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test").
			Body(yamlBody).
			DoRaw(ctx)
		if err == nil {
			framework.Failf("Expecting to get conflicts when applying object after updating replicas, got no error: %s", result)
		}
		status, ok := err.(*apierrors.StatusError)
		if !ok {
			framework.Failf("Expecting to get conflicts as API error")
		}
		if len(status.Status().Details.Causes) != 1 {
			framework.Failf("Expecting to get one conflict when applying object after updating replicas, got: %v", status.Status().Details.Causes)
		}

		// 强制重新 apply 应正常成功
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("force", "true").
			Param("fieldManager", "apply_test").
			Body(yamlBody).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to apply object with force after updating replicas: %v:\n%v", err, string(result))
		}
		verifyReplicas(result, 1)

		// 新的 applier 尝试修改已有列表项，应产生冲突
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test_2").
			Body([]byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: %s
spec:
  ports:
  - name: "y"
    containerPort: 80
    protocol: TCP`, apiVersion, kind, name))).
			DoRaw(ctx)
		if err == nil {
			framework.Failf("Expecting to get conflicts when a different applier updates existing list item, got no error: %s", result)
		}
		status, ok = err.(*apierrors.StatusError)
		if !ok {
			framework.Failf("Expecting to get conflicts as API error")
		}
		if len(status.Status().Details.Causes) != 1 {
			framework.Failf("Expecting to get one conflict when a different applier updates existing list item, got: %v", status.Status().Details.Causes)
		}

		// 新的 applier 尝试添加新列表项，应正常成功
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test_2").
			Body([]byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: %s
spec:
  ports:
  - name: "y"
    containerPort: 8080
    protocol: TCP`, apiVersion, kind, name))).
			SetHeader("Accept", "application/json").
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to add a new list item to the object as a different applier: %v:\n%v", err, string(result))
		}
		verifyNumPorts(result, 2)

		// UpdateOnCreate 场景
		notExistingYAMLBody := []byte(fmt.Sprintf(`
	{
		"apiVersion": "%s",
		"kind": "%s",
		"metadata": {
		  "name": "%s",
		  "finalizers": [
			"test-finalizer"
		  ]
		},
		"spec": {
		  "cronSpec": "* * * * */5",
		  "replicas": 1,
		  "ports": [
			{
			  "name": "x",
			  "containerPort": 80
			}
		  ]
		},
		"protocol": "TCP"
	}`, apiVersion, kind, "should-not-exist"))
		_, err = rest.Put().
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Name("should-not-exist").
			Param("fieldManager", "apply_test").
			Body(notExistingYAMLBody).
			DoRaw(ctx)
		if !apierrors.IsNotFound(err) {
			framework.Failf("create on update should fail with notFound, got %v", err)
		}

		// 创建 CRD 以测试 atomic list
		crd := fixtures.NewRandomNameV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
		err = json.Unmarshal([]byte(`{
		"openAPIV3Schema": {
			"type": "object",
			"properties": {
				"spec": {
					"type": "object",
					"x-kubernetes-preserve-unknown-fields": true,
					"properties": {
						"atomicList": {
							"type": "array",
							"x-kubernetes-list-type": "atomic",
							"items": {
								"type": "string"
							}
						}
					}
				}
			}
		}
	}`), &c)
		if err != nil {
			framework.Failf("%s", err)
		}
		for i := range crd.Spec.Versions {
			crd.Spec.Versions[i].Schema = &c
		}

		crd, err = fixtures.CreateNewV1CustomResourceDefinition(crd, apiExtensionClient, dynamicClient)
		if err != nil {
			framework.Failf("cannot create crd %s", err)
		}

		defer func() {
			err = fixtures.DeleteV1CustomResourceDefinition(crd, apiExtensionClient)
			framework.ExpectNoError(err, "deleting CustomResourceDefinition")
		}()

		crdKind := crd.Spec.Names.Kind
		crdApiVersion := crd.Spec.Group + "/" + crd.Spec.Versions[0].Name

		crdYamlBody := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: %s
spec:
  atomicList:
  - "item1"`, crdApiVersion, crdKind, name))
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", crd.Spec.Group, crd.Spec.Versions[0].Name, crd.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test").
			Body(crdYamlBody).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to create custom resource with apply: %v:\n%v", err, string(result))
		}

		verifyList(result, []interface{}{"item1"})

		crdYamlBody = []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: %s
spec:
  atomicList:
  - "item2"`, crdApiVersion, crdKind, name))
		result, err = rest.Patch(types.ApplyPatchType).
			AbsPath("/apis", crd.Spec.Group, crd.Spec.Versions[0].Name, crd.Spec.Names.Plural).
			Name(name).
			Param("fieldManager", "apply_test_2").
			Param("force", "true").
			Body(crdYamlBody).
			DoRaw(ctx)
		if err != nil {
			framework.Failf("failed to create custom resource with apply: %v:\n%v", err, string(result))
		}

		// 由于是 atomic list，其内容必须被最近一次 apply 整体替换
		verifyList(result, []interface{}{"item2"})
	})

	/*
		Release : v1.21
		Testname: Server Side Apply, Update take ownership
		Description: apply 一个对象，然后发送 Update 请求获取某字段的所有权。该字段应归新 manager 所有，原 manager 再次 apply MUST NOT 修改其已不拥有的字段。
	*/
	ginkgo.It("should give up ownership of a field if forced applied by a controller", func(ctx context.Context) {
		// applier 创建副本数为 3 的 deployment
		apply := []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment-shared-map-item-removal",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"replicas": 3,
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}
		}
	}`)

		_, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment-shared-map-item-removal").
			Param("fieldManager", "test_applier").
			Body(apply).
			Do(ctx).
			Get()
		if err != nil {
			framework.Failf("Failed to create object using Apply patch: %v", err)
		}

		replicas := int32(4)
		_, err = e2edeployment.UpdateDeploymentWithRetries(client, ns, "deployment-shared-map-item-removal", func(update *appsv1.Deployment) {
			update.Spec.Replicas = &replicas
		})
		framework.ExpectNoError(err)

		// applier 省略 replicas 字段
		apply = []byte(`{
		"apiVersion": "apps/v1",
		"kind": "Deployment",
		"metadata": {
			"name": "deployment-shared-map-item-removal",
			"labels": {"app": "nginx"}
		},
		"spec": {
			"selector": {
				"matchLabels": {
					"app": "nginx"
				}
			},
			"template": {
				"metadata": {
					"labels": {
						"app": "nginx"
					}
				},
				"spec": {
					"containers": [{
						"name":  "nginx",
						"image": "` + imageutils.GetE2EImage(imageutils.NginxNew) + `",
					}]
				}
			}
		}
	}`)

		patched, err := client.CoreV1().RESTClient().Patch(types.ApplyPatchType).
			AbsPath("/apis/apps/v1").
			Namespace(ns).
			Resource("deployments").
			Name("deployment-shared-map-item-removal").
			Param("fieldManager", "test_applier").
			Body(apply).
			Do(ctx).
			Get()
		if err != nil {
			framework.Failf("Failed to create object using Apply patch: %v", err)
		}

		// 即使 controller 修改了容器的某个字段，也要确保容器被删除
		deployment, ok := patched.(*appsv1.Deployment)
		if !ok {
			framework.Failf("Failed to convert response object to Deployment")
		}
		if *deployment.Spec.Replicas != 4 {
			framework.Failf("Expected deployment.spec.replicas to be 4, but got %d", deployment.Spec.Replicas)
		}
	})
})

// verifyNumFinalizers 检查 len(.metadata.finalizers) == n
func verifyNumFinalizers(b []byte, n int) {
	obj := unstructured.Unstructured{}
	err := obj.UnmarshalJSON(b)
	if err != nil {
		framework.Failf("failed to unmarshal response: %v", err)
	}
	if actual, expected := len(obj.GetFinalizers()), n; actual != expected {
		framework.Failf("expected %v finalizers but got %v:\n%v", expected, actual, string(b))
	}
}

// verifyFinalizersIncludes 检查 .metadata.finalizers 包含 e
func verifyFinalizersIncludes(b []byte, e string) {
	obj := unstructured.Unstructured{}
	err := obj.UnmarshalJSON(b)
	if err != nil {
		framework.Failf("failed to unmarshal response: %v", err)
	}
	for _, a := range obj.GetFinalizers() {
		if a == e {
			return
		}
	}
	framework.Failf("expected finalizers to include %q but got: %v", e, obj.GetFinalizers())
}

// verifyReplicas 检查 .spec.replicas == r
func verifyReplicas(b []byte, r int) {
	obj := unstructured.Unstructured{}
	err := obj.UnmarshalJSON(b)
	if err != nil {
		framework.Failf("failed to find replicas number in response: %v:\n%v", err, string(b))
	}
	spec, ok := obj.Object["spec"]
	if !ok {
		framework.Failf("failed to find replicas number in response:\n%v", string(b))
	}
	specMap, ok := spec.(map[string]interface{})
	if !ok {
		framework.Failf("failed to find replicas number in response:\n%v", string(b))
	}
	replicas, ok := specMap["replicas"]
	if !ok {
		framework.Failf("failed to find replicas number in response:\n%v", string(b))
	}
	replicasNumber, ok := replicas.(int64)
	if !ok {
		framework.Failf("failed to find replicas number in response: expected int64 but got: %v", reflect.TypeOf(replicas))
	}
	if actual, expected := replicasNumber, int64(r); actual != expected {
		framework.Failf("expected %v ports but got %v:\n%v", expected, actual, string(b))
	}
}

// verifyNumPorts 检查 len(.spec.ports) == n
func verifyNumPorts(b []byte, n int) {
	obj := unstructured.Unstructured{}
	err := obj.UnmarshalJSON(b)
	if err != nil {
		framework.Failf("failed to find ports list in response: %v:\n%v", err, string(b))
	}
	spec, ok := obj.Object["spec"]
	if !ok {
		framework.Failf("failed to find ports list in response:\n%v", string(b))
	}
	specMap, ok := spec.(map[string]interface{})
	if !ok {
		framework.Failf("failed to find ports list in response:\n%v", string(b))
	}
	ports, ok := specMap["ports"]
	if !ok {
		framework.Failf("failed to find ports list in response:\n%v", string(b))
	}
	portsList, ok := ports.([]interface{})
	if !ok {
		framework.Failf("failed to find ports list in response: expected array but got: %v", reflect.TypeOf(ports))
	}
	if actual, expected := len(portsList), n; actual != expected {
		framework.Failf("expected %v ports but got %v:\n%v", expected, actual, string(b))
	}
}

// verifyList 检查 .spec.atomicList 与传入的 expectedList 完全一致
func verifyList(b []byte, expectedList []interface{}) {
	obj := unstructured.Unstructured{}
	err := obj.UnmarshalJSON(b)
	if err != nil {
		framework.Failf("failed to find atomicList in response: %v:\n%v", err, string(b))
	}
	spec, ok := obj.Object["spec"]
	if !ok {
		framework.Failf("failed to find atomicList in response:\n%v", string(b))
	}
	specMap, ok := spec.(map[string]interface{})
	if !ok {
		framework.Failf("failed to find atomicList in response:\n%v", string(b))
	}
	list, ok := specMap["atomicList"]
	if !ok {
		framework.Failf("failed to find atomicList in response:\n%v", string(b))
	}
	listString, ok := list.([]interface{})
	if !ok {
		framework.Failf("failed to find atomicList in response:\n%v", string(b))
	}
	if !reflect.DeepEqual(listString, expectedList) {
		framework.Failf("Expected list %s, got %s", expectedList, listString)
	}
}
