/*
Copyright 2018 The Kubernetes Authors.

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

package integration

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	autoscaling "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/utils/ptr"
)

var labelSelectorPath = ".status.labelSelector"
var anotherLabelSelectorPath = ".status.anotherLabelSelector"

// NewNoxuSubresourcesCRDs 返回带 status/scale 子资源的测试 CRD 集合。
func NewNoxuSubresourcesCRDs(scope apiextensionsv1.ResourceScope) []*apiextensionsv1.CustomResourceDefinition {
	return []*apiextensionsv1.CustomResourceDefinition{
		// 使用按版本配置子资源的 CRD
		{
			ObjectMeta: metav1.ObjectMeta{Name: "noxus.mygroup.example.com"},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: "mygroup.example.com",
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural:     "noxus",
					Singular:   "nonenglishnoxu",
					Kind:       "WishIHadChosenNoxu",
					ShortNames: []string{"foo", "bar", "abc", "def"},
					ListKind:   "NoxuItemList",
				},
				Scope: scope,
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    "v1beta1",
						Served:  true,
						Storage: true,
						Subresources: &apiextensionsv1.CustomResourceSubresources{
							Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
							Scale: &apiextensionsv1.CustomResourceSubresourceScale{
								SpecReplicasPath:   ".spec.replicas",
								StatusReplicasPath: ".status.replicas",
								LabelSelectorPath:  &labelSelectorPath,
							},
						},
						Schema: fixtures.AllowAllSchema(),
					},
					{
						Name:    "v1",
						Served:  true,
						Storage: false,
						Subresources: &apiextensionsv1.CustomResourceSubresources{
							Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
							Scale: &apiextensionsv1.CustomResourceSubresourceScale{
								SpecReplicasPath:   ".spec.replicas",
								StatusReplicasPath: ".status.replicas",
								LabelSelectorPath:  &anotherLabelSelectorPath,
							},
						},
						Schema: fixtures.AllowAllSchema(),
					},
				},
			},
		},
	}
}

// NewNoxuSubresourceInstance 构造一个用于子资源测试的 noxu 实例。
func NewNoxuSubresourceInstance(namespace, name, version string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": fmt.Sprintf("mygroup.example.com/%s", version),
			"kind":       "WishIHadChosenNoxu",
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"spec": map[string]interface{}{
				"num":      int64(10),
				"replicas": int64(3),
			},
			"status": map[string]interface{}{
				"replicas": int64(7),
			},
		},
	}
}

// NewNoxuSubresourceInstanceWithReplicas 构造副本数字段名可定制的 noxu 实例。
func NewNoxuSubresourceInstanceWithReplicas(namespace, name, version, replicasField string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": fmt.Sprintf("mygroup.example.com/%s", version),
			"kind":       "WishIHadChosenNoxu",
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
			"spec": map[string]interface{}{
				"num":         int64(10),
				replicasField: int64(3),
			},
			"status": map[string]interface{}{
				"replicas": int64(7),
			},
		},
	}
}

// TestStatusSubresource 校验 status 子资源：UpdateStatus 不修改 spec，普通 Update 不修改 status。
func TestStatusSubresource(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	noxuDefinitions := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)
	for _, noxuDefinition := range noxuDefinitions {
		noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
		if err != nil {
			t.Fatal(err)
		}

		ns := "not-the-default"
		for _, v := range noxuDefinition.Spec.Versions {
			noxuResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, v.Name)
			_, err = instantiateVersionedCustomResource(t, NewNoxuSubresourceInstance(ns, "foo", v.Name), noxuResourceClient, noxuDefinition, v.Name)
			if err != nil {
				t.Fatalf("unable to create noxu instance: %v", err)
			}
			gottenNoxuInstance, err := noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			// 创建时不应设置 status
			if val, ok := gottenNoxuInstance.Object["status"]; ok {
				t.Fatalf("status should not be set after creation, got %v", val)
			}

			// 设置 .status.num = 20
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(20), "status", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 设置 .spec.num = 20
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(20), "spec", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// UpdateStatus 不应更新 spec。
			// 确认 .spec.num = 10 且 .status.num = 20
			updatedStatusInstance, err := noxuResourceClient.UpdateStatus(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("unable to update status: %v", err)
			}

			specNum, found, err := unstructured.NestedInt64(updatedStatusInstance.Object, "spec", "num")
			if !found || err != nil {
				t.Fatalf("unable to get .spec.num")
			}
			if specNum != int64(10) {
				t.Fatalf(".spec.num: expected: %v, got: %v", int64(10), specNum)
			}

			statusNum, found, err := unstructured.NestedInt64(updatedStatusInstance.Object, "status", "num")
			if !found || err != nil {
				t.Fatalf("unable to get .status.num")
			}
			if statusNum != int64(20) {
				t.Fatalf(".status.num: expected: %v, got: %v", int64(20), statusNum)
			}

			gottenNoxuInstance, err = noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			// 设置 .status.num = 40
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(40), "status", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 设置 .spec.num = 40
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(40), "spec", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Update 不应更新 status。
			// 确认 .spec.num = 40 且 .status.num = 20
			updatedInstance, err := noxuResourceClient.Update(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("unable to update instance: %v", err)
			}

			specNum, found, err = unstructured.NestedInt64(updatedInstance.Object, "spec", "num")
			if !found || err != nil {
				t.Fatalf("unable to get .spec.num")
			}
			if specNum != int64(40) {
				t.Fatalf(".spec.num: expected: %v, got: %v", int64(40), specNum)
			}

			statusNum, found, err = unstructured.NestedInt64(updatedInstance.Object, "status", "num")
			if !found || err != nil {
				t.Fatalf("unable to get .status.num")
			}
			if statusNum != int64(20) {
				t.Fatalf(".status.num: expected: %v, got: %v", int64(20), statusNum)
			}
			noxuResourceClient.Delete(context.TODO(), "foo", metav1.DeleteOptions{})
		}
		if err := fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient); err != nil {
			t.Fatal(err)
		}
	}
}

// TestScaleSubresource 校验 scale 子资源的读写语义及 specReplicasPath 的校验。
func TestScaleSubresource(t *testing.T) {
	groupResource := schema.GroupResource{
		Group:    "mygroup.example.com",
		Resource: "noxus",
	}

	tearDown, config, _, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	apiExtensionClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	noxuDefinitions := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)
	for _, noxuDefinition := range noxuDefinitions {
		for _, v := range noxuDefinition.Spec.Versions {
			// 从新的 CRD 开始，使对象不带 resourceVersion
			noxuDefinition := noxuDefinition.DeepCopy()

			subresources, err := getSubresourcesForVersion(noxuDefinition, v.Name)
			if err != nil {
				t.Fatal(err)
			}
			// 为 specReplicasPath 设置非法的 json path
			subresources.Scale.SpecReplicasPath = "foo,bar"
			_, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
			if err == nil {
				t.Fatalf("unexpected non-error: specReplicasPath should be a valid json path under .spec")
			}

			subresources.Scale.SpecReplicasPath = ".spec.replicas"
			noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
			if err != nil {
				t.Fatal(err)
			}

			ns := "not-the-default"
			noxuResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, v.Name)
			_, err = instantiateVersionedCustomResource(t, NewNoxuSubresourceInstance(ns, "foo", v.Name), noxuResourceClient, noxuDefinition, v.Name)
			if err != nil {
				t.Fatalf("unable to create noxu instance: %v", err)
			}

			scaleClient, err := fixtures.CreateNewVersionedScaleClient(noxuDefinition, config, v.Name)
			if err != nil {
				t.Fatal(err)
			}

			// 设置 .status.labelSelector = bar
			gottenNoxuInstance, err := noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, "bar", strings.Split((*subresources.Scale.LabelSelectorPath)[1:], ".")...)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			_, err = noxuResourceClient.UpdateStatus(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("unable to update status: %v", err)
			}

			// 获取 scale 对象
			gottenScale, err := scaleClient.Scales("not-the-default").Get(context.TODO(), groupResource, "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if gottenScale.Spec.Replicas != 3 {
				t.Fatalf("Scale.Spec.Replicas: expected: %v, got: %v", 3, gottenScale.Spec.Replicas)
			}
			if gottenScale.Status.Selector != "bar" {
				t.Fatalf("Scale.Status.Selector: expected: %v, got: %v", "bar", gottenScale.Status.Selector)
			}

			// 更新 scale 对象
			// 确认 spec 被更新，而 status 未被更新
			gottenScale.Spec.Replicas = 5
			gottenScale.Status.Selector = "baz"
			updatedScale, err := scaleClient.Scales("not-the-default").Update(context.TODO(), groupResource, gottenScale, metav1.UpdateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if updatedScale.Spec.Replicas != 5 {
				t.Fatalf("replicas: expected: %v, got: %v", 5, updatedScale.Spec.Replicas)
			}
			if updatedScale.Status.Selector != "bar" {
				t.Fatalf("scale should not update status: expected %v, got: %v", "bar", updatedScale.Status.Selector)
			}

			// 确认 .spec.replicas = 5，而 status 未被更新
			updatedNoxuInstance, err := noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			specReplicas, found, err := unstructured.NestedInt64(updatedNoxuInstance.Object, "spec", "replicas")
			if !found || err != nil {
				t.Fatalf("unable to get .spec.replicas")
			}
			if specReplicas != 5 {
				t.Fatalf("replicas: expected: %v, got: %v", 5, specReplicas)
			}
			statusLabelSelector, found, err := unstructured.NestedString(updatedNoxuInstance.Object, strings.Split((*subresources.Scale.LabelSelectorPath)[1:], ".")...)
			if !found || err != nil {
				t.Fatalf("unable to get %s", *subresources.Scale.LabelSelectorPath)
			}
			if statusLabelSelector != "bar" {
				t.Fatalf("scale should not update status: expected %v, got: %v", "bar", statusLabelSelector)
			}

			// 校验上限值
			// 设置 .spec.replicas = math.MaxInt64
			gottenNoxuInstance, err = noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(math.MaxInt64), "spec", "replicas")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			_, err = noxuResourceClient.Update(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
			if err == nil {
				t.Fatalf("unexpected non-error: .spec.replicas should be less than 2147483647")
			}
			noxuResourceClient.Delete(context.TODO(), "foo", metav1.DeleteOptions{})
			if err := fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestApplyScaleSubresource 校验对 scale 子资源做 apply 时 managedFields 的归属。
func TestApplyScaleSubresource(t *testing.T) {
	tearDown, config, _, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	apiExtensionClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	noxuDefinition := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)[0]
	subresources, err := getSubresourcesForVersion(noxuDefinition, "v1beta1")
	if err != nil {
		t.Fatal(err)
	}
	subresources.Scale.SpecReplicasPath = ".spec.replicas[0]"
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	// 为其创建客户端。
	ns := "not-the-default"
	noxuResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, "v1beta1")

	obj := NewNoxuSubresourceInstanceWithReplicas(ns, "foo", "v1beta1", "replicas[0]")
	obj, err = noxuResourceClient.Create(context.TODO(), obj, metav1.CreateOptions{})
	if err != nil {
		t.Logf("%#v", obj)
		t.Fatalf("Failed to create CustomResource: %v", err)
	}

	noxuResourceClient = newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, "v1")
	patch := `{"metadata": {"name": "foo"}, "kind": "WishIHadChosenNoxu", "apiVersion": "mygroup.example.com/v1", "spec": {"replicas": 3}}`
	obj, err = noxuResourceClient.Patch(context.TODO(), "foo", types.ApplyPatchType, []byte(patch), metav1.PatchOptions{FieldManager: "applier"})
	if err != nil {
		t.Logf("%#v", obj)
		t.Fatalf("Failed to Apply CustomResource: %v", err)
	}

	if got := len(obj.GetManagedFields()); got != 2 {
		t.Fatalf("Expected 2 managed fields, got %v: %v", got, obj.GetManagedFields())
	}

	_, err = noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, []byte(`{"spec": {"replicas": 5}}`), metav1.PatchOptions{FieldManager: "scaler"}, "scale")
	if err != nil {
		t.Fatal(err)
	}

	obj, err = noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to Get CustomResource: %v", err)
	}

	// managed fields 应有 3 条：分别属于 scale、spec 和其余字段
	managedFields := obj.GetManagedFields()
	if len(managedFields) != 3 {
		t.Fatalf("Expected 3 managed fields, got %v: %v", len(managedFields), obj.GetManagedFields())
	}
	specEntry := managedFields[0]
	if specEntry.Manager != "applier" || specEntry.APIVersion != "mygroup.example.com/v1" || specEntry.Operation != "Apply" || specEntry.FieldsV1.GetRawString() != `{"f:spec":{}}` || specEntry.Subresource != "" {
		t.Fatalf("Unexpected entry: %v", specEntry)
	}
	scaleEntry := managedFields[1]
	if scaleEntry.Manager != "scaler" || scaleEntry.APIVersion != "mygroup.example.com/v1" || scaleEntry.Operation != "Update" || scaleEntry.FieldsV1.GetRawString() != `{"f:spec":{"f:replicas":{}}}` || scaleEntry.Subresource != "scale" {
		t.Fatalf("Unexpected entry: %v", scaleEntry)
	}
	restEntry := managedFields[2]
	if restEntry.Manager != "integration.test" || restEntry.APIVersion != "mygroup.example.com/v1beta1" {
		t.Fatalf("Unexpected entry: %v", restEntry)
	}
}

// TestValidationSchemaWithStatus 校验带 status 的校验 schema 不会把子 schema 的字段限制在 properties 内。
func TestValidationSchemaWithStatus(t *testing.T) {
	tearDown, config, _, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	apiExtensionClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	noxuDefinition := newNoxuValidationCRDs()[0]

	// 确保即便在子 schema 中也不会把字段限制在 properties 内
	noxuDefinition.Spec.Versions[0].Schema.OpenAPIV3Schema = &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type:        "object",
				Description: "Validation for spec",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"replicas": {
						Type: "integer",
					},
				},
			},
		},
		Required:    []string{"spec"},
		Description: "This is a description at the root of the schema",
	}
	noxuDefinition.Spec.Versions[1].Schema.OpenAPIV3Schema = noxuDefinition.Spec.Versions[0].Schema.OpenAPIV3Schema

	_, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatalf("unable to created crd %v: %v", noxuDefinition.Name, err)
	}
}

// TestValidateOnlyStatus 校验 UpdateStatus 只校验 status，不校验非法的 spec。
func TestValidateOnlyStatus(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	// UpdateStatus 应只校验 status
	// 1. 创建 CRD，.spec.num 与 .status.num 的最大值均为 10
	// 2. 创建 CR，.spec.num = 10 且 .status.num = 10（合法）
	// 3. 把 CR 的 spec 更新为 .spec.num = 15（spec 非法），预期无错误
	// 4. 把 CR 的 spec 更新为 .spec.num = 15（spec 非法），预期报错

	// spec.num 与 status.num 的最大值为 10
	schema := &apiextensionsv1.JSONSchemaProps{
		Type: "object",
		Properties: map[string]apiextensionsv1.JSONSchemaProps{
			"spec": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"num": {
						Type:    "integer",
						Maximum: ptr.To[float64](10),
					},
				},
			},
			"status": {
				Type: "object",
				Properties: map[string]apiextensionsv1.JSONSchemaProps{
					"num": {
						Type:    "integer",
						Maximum: ptr.To[float64](10),
					},
				},
			},
		},
	}

	noxuDefinitions := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)
	for _, noxuDefinition := range noxuDefinitions {
		noxuDefinition.Spec.Versions[0].Schema = &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: schema.DeepCopy(),
		}
		noxuDefinition.Spec.Versions[1].Schema = &apiextensionsv1.CustomResourceValidation{
			OpenAPIV3Schema: schema.DeepCopy(),
		}

		noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
		if err != nil {
			t.Fatal(err)
		}
		ns := "not-the-default"
		for _, v := range noxuDefinition.Spec.Versions {
			noxuResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, v.Name)

			// 设置 .spec.num = 10 且 .status.num = 10
			noxuInstance := NewNoxuSubresourceInstance(ns, "foo", v.Name)
			err = unstructured.SetNestedField(noxuInstance.Object, int64(10), "status", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			createdNoxuInstance, err := instantiateVersionedCustomResource(t, noxuInstance, noxuResourceClient, noxuDefinition, v.Name)
			if err != nil {
				t.Fatalf("unable to create noxu instance: %v", err)
			}

			// 把 spec 更新为 .spec.num = 15，预期无错误
			err = unstructured.SetNestedField(createdNoxuInstance.Object, int64(15), "spec", "num")
			if err != nil {
				t.Fatalf("unexpected error setting .spec.num: %v", err)
			}
			createdNoxuInstance, err = noxuResourceClient.UpdateStatus(context.TODO(), createdNoxuInstance, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 把 .status.num 更新为 15，预期报错
			err = unstructured.SetNestedField(createdNoxuInstance.Object, int64(15), "status", "num")
			if err != nil {
				t.Fatalf("unexpected error setting .status.num: %v", err)
			}
			_, err = noxuResourceClient.UpdateStatus(context.TODO(), createdNoxuInstance, metav1.UpdateOptions{})
			if err == nil {
				t.Fatal("expected error, but got none")
			}
			statusError, isStatus := err.(*apierrors.StatusError)
			if !isStatus || statusError == nil {
				t.Fatalf("expected status error, got %T: %v", err, err)
			}
			expectedErr := "WishIHadChosenNoxu.mygroup.example.com \"foo\" is invalid: status.num: Invalid value: 15: num in body should be less than or equal to 10"
			if !strings.Contains(statusError.Error(), expectedErr) {
				t.Fatalf("expected %q in error, got: %v", expectedErr, err)
			}
			noxuResourceClient.Delete(context.TODO(), "foo", metav1.DeleteOptions{})
		}
		if err := fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSubresourcesDiscovery 校验 discovery 中暴露的 status 和 scale 子资源信息。
func TestSubresourcesDiscovery(t *testing.T) {
	tearDown, config, _, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	apiExtensionClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	noxuDefinitions := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)
	for _, noxuDefinition := range noxuDefinitions {
		noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
		if err != nil {
			t.Fatal(err)
		}

		for _, v := range noxuDefinition.Spec.Versions {
			group := "mygroup.example.com"
			version := v.Name

			resources, err := apiExtensionClient.Discovery().ServerResourcesForGroupVersion(group + "/" + version)
			if err != nil {
				t.Fatal(err)
			}

			if len(resources.APIResources) != 3 {
				t.Fatalf("Expected exactly the resources \"noxus\", \"noxus/status\" and \"noxus/scale\" in group version %v/%v via discovery, got: %v", group, version, resources.APIResources)
			}

			// 检查 status 的 discovery 信息
			status := resources.APIResources[1]

			if status.Name != "noxus/status" {
				t.Fatalf("incorrect status via discovery: expected name: %v, got: %v", "noxus/status", status.Name)
			}

			if status.Namespaced != true {
				t.Fatalf("incorrect status via discovery: expected namespace: %v, got: %v", true, status.Namespaced)
			}

			if status.Kind != "WishIHadChosenNoxu" {
				t.Fatalf("incorrect status via discovery: expected kind: %v, got: %v", "WishIHadChosenNoxu", status.Kind)
			}

			expectedVerbs := []string{"get", "patch", "update"}
			sort.Strings(status.Verbs)
			if !reflect.DeepEqual([]string(status.Verbs), expectedVerbs) {
				t.Fatalf("incorrect status via discovery: expected: %v, got: %v", expectedVerbs, status.Verbs)
			}

			// 检查 scale 的 discovery 信息
			scale := resources.APIResources[2]

			if scale.Group != autoscaling.GroupName {
				t.Fatalf("incorrect scale via discovery: expected group: %v, got: %v", autoscaling.GroupName, scale.Group)
			}

			if scale.Version != "v1" {
				t.Fatalf("incorrect scale via discovery: expected version: %v, got %v", "v1", scale.Version)
			}

			if scale.Name != "noxus/scale" {
				t.Fatalf("incorrect scale via discovery: expected name: %v, got: %v", "noxus/scale", scale.Name)
			}

			if scale.Namespaced != true {
				t.Fatalf("incorrect scale via discovery: expected namespace: %v, got: %v", true, scale.Namespaced)
			}

			if scale.Kind != "Scale" {
				t.Fatalf("incorrect scale via discovery: expected kind: %v, got: %v", "Scale", scale.Kind)
			}

			sort.Strings(scale.Verbs)
			if !reflect.DeepEqual([]string(scale.Verbs), expectedVerbs) {
				t.Fatalf("incorrect scale via discovery: expected: %v, got: %v", expectedVerbs, scale.Verbs)
			}
		}
		if err := fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient); err != nil {
			t.Fatal(err)
		}
	}
}

// TestGeneration 校验 metadata.generation 仅在 spec 变化时递增，更新 status 不递增。
func TestGeneration(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	noxuDefinitions := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)
	for _, noxuDefinition := range noxuDefinitions {
		noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
		if err != nil {
			t.Fatal(err)
		}

		ns := "not-the-default"
		for _, v := range noxuDefinition.Spec.Versions {
			noxuResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, v.Name)
			_, err = instantiateVersionedCustomResource(t, NewNoxuSubresourceInstance(ns, "foo", v.Name), noxuResourceClient, noxuDefinition, v.Name)
			if err != nil {
				t.Fatalf("unable to create noxu instance: %v", err)
			}

			// 设置 .metadata.generation = 1
			gottenNoxuInstance, err := noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if gottenNoxuInstance.GetGeneration() != 1 {
				t.Fatalf(".metadata.generation should be 1 after creation")
			}

			// 设置 .status.num = 20
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(20), "status", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// UpdateStatus 不递增 generation
			updatedStatusInstance, err := noxuResourceClient.UpdateStatus(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("unable to update status: %v", err)
			}
			if updatedStatusInstance.GetGeneration() != 1 {
				t.Fatalf("updating status should not increment .metadata.generation: expected: %v, got: %v", 1, updatedStatusInstance.GetGeneration())
			}

			gottenNoxuInstance, err = noxuResourceClient.Get(context.TODO(), "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}

			// 设置 .spec.num = 20
			err = unstructured.SetNestedField(gottenNoxuInstance.Object, int64(20), "spec", "num")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Update 递增 generation
			updatedInstance, err := noxuResourceClient.Update(context.TODO(), gottenNoxuInstance, metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("unable to update instance: %v", err)
			}
			if updatedInstance.GetGeneration() != 2 {
				t.Fatalf("updating spec should increment .metadata.generation: expected: %v, got: %v", 2, updatedStatusInstance.GetGeneration())
			}
			noxuResourceClient.Delete(context.TODO(), "foo", metav1.DeleteOptions{})
		}
		if err := fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSubresourcePatch 校验对 status/scale 子资源的 patch 只作用于对应子资源，且 no-op patch 不递增 resourceVersion。
func TestSubresourcePatch(t *testing.T) {
	groupResource := schema.GroupResource{
		Group:    "mygroup.example.com",
		Resource: "noxus",
	}

	tearDown, config, _, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	apiExtensionClient, err := clientset.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	noxuDefinitions := NewNoxuSubresourcesCRDs(apiextensionsv1.NamespaceScoped)
	for _, noxuDefinition := range noxuDefinitions {
		noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
		if err != nil {
			t.Fatal(err)
		}

		ns := "not-the-default"
		for _, v := range noxuDefinition.Spec.Versions {
			noxuResourceClient := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, v.Name)

			t.Logf("Creating foo")
			_, err = instantiateVersionedCustomResource(t, NewNoxuSubresourceInstance(ns, "foo", v.Name), noxuResourceClient, noxuDefinition, v.Name)
			if err != nil {
				t.Fatalf("unable to create noxu instance: %v", err)
			}

			scaleClient, err := fixtures.CreateNewVersionedScaleClient(noxuDefinition, config, v.Name)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("Patching .status.num to 999")
			patch := []byte(`{"spec": {"num":999}, "status": {"num":999}}`)
			patchedNoxuInstance, err := noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, patch, metav1.PatchOptions{}, "status")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 999, "status", "num") // .status.num 应为 999
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 10, "spec", "num")    // .spec.num 应保持 10

			// 若资源在上次写入后一秒内未变，server-side-apply 会递增 resourceVersion；
			// 等待一秒可确保当 no-op patch 会递增 resourceVersion 时能被观测到
			time.Sleep(time.Second)
			// 空操作 patch
			rv := patchedNoxuInstance.GetResourceVersion()
			found := false
			t.Logf("Patching .status.num again to 999")
			patchedNoxuInstance, err = noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, patch, metav1.PatchOptions{}, "status")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 确认 no-op patch 不递增 resourceVersion
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 999, "status", "num")
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 10, "spec", "num")
			expectString(t, patchedNoxuInstance.UnstructuredContent(), rv, "metadata", "resourceVersion")

			// 空 patch
			t.Logf("Applying empty patch")
			patchedNoxuInstance, err = noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, []byte(`{}`), metav1.PatchOptions{}, "status")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// 空 patch 属于 no-op patch。确认它不递增 resourceVersion
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 999, "status", "num")
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 10, "spec", "num")
			expectString(t, patchedNoxuInstance.UnstructuredContent(), rv, "metadata", "resourceVersion")

			t.Logf("Patching .spec.replicas to 7")
			patch = []byte(`{"spec": {"replicas":7}, "status": {"replicas":7}}`)
			patchedNoxuInstance, err = noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, patch, metav1.PatchOptions{}, "scale")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 7, "spec", "replicas")
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 0, "status", "replicas") // .status.replicas 应保持 0
			rv, found, err = unstructured.NestedString(patchedNoxuInstance.UnstructuredContent(), "metadata", "resourceVersion")
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatalf("metadata.resourceVersion not found")
			}

			// Scale.Spec.Replicas = 7，但 Scale.Status.Replicas 应保持 0
			gottenScale, err := scaleClient.Scales("not-the-default").Get(context.TODO(), groupResource, "foo", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if gottenScale.Spec.Replicas != 7 {
				t.Fatalf("Scale.Spec.Replicas: expected: %v, got: %v", 7, gottenScale.Spec.Replicas)
			}
			if gottenScale.Status.Replicas != 0 {
				t.Fatalf("Scale.Status.Replicas: expected: %v, got: %v", 0, gottenScale.Spec.Replicas)
			}

			// 空操作 patch
			t.Logf("Patching .spec.replicas again to 7")
			patchedNoxuInstance, err = noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, patch, metav1.PatchOptions{}, "scale")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 确认 no-op patch 不递增 resourceVersion
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 7, "spec", "replicas")
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 0, "status", "replicas")
			expectString(t, patchedNoxuInstance.UnstructuredContent(), rv, "metadata", "resourceVersion")

			// 空 patch
			t.Logf("Applying empty patch")
			patchedNoxuInstance, err = noxuResourceClient.Patch(context.TODO(), "foo", types.MergePatchType, []byte(`{}`), metav1.PatchOptions{}, "scale")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// 空 patch 属于 no-op patch。确认它不递增 resourceVersion
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 7, "spec", "replicas")
			expectInt64(t, patchedNoxuInstance.UnstructuredContent(), 0, "status", "replicas")
			expectString(t, patchedNoxuInstance.UnstructuredContent(), rv, "metadata", "resourceVersion")

			// 确认 status 和 scale 都不支持 strategic merge patch
			_, err = noxuResourceClient.Patch(context.TODO(), "foo", types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "status")
			if err == nil {
				t.Fatalf("unexpected non-error: strategic merge patch is not supported for custom resources")
			}

			_, err = noxuResourceClient.Patch(context.TODO(), "foo", types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "scale")
			if err == nil {
				t.Fatalf("unexpected non-error: strategic merge patch is not supported for custom resources")
			}
			noxuResourceClient.Delete(context.TODO(), "foo", metav1.DeleteOptions{})
		}
		if err := fixtures.DeleteV1CustomResourceDefinition(noxuDefinition, apiExtensionClient); err != nil {
			t.Fatal(err)
		}
	}
}
