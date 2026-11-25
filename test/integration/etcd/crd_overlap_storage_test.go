/*
Copyright 2019 The Kubernetes Authors.

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

package etcd

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/controller/finalizer"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
)

// TestOverlappingBuiltInResources 确保 CR finalizer 应跳过的 group-resource 列表保持最新。
func TestOverlappingBuiltInResources(t *testing.T) {
	// 验证存储路径与 CRD 重叠的内建资源都列在 OverlappingBuiltInResources() 中。
	detectedOverlappingResources := map[schema.GroupResource]bool{}
	for gvr, gvrData := range GetEtcdStorageData() {
		if !strings.HasSuffix(gvr.Group, ".k8s.io") {
			// 只有完全限定的 group 名才可能存在对应 CRD。
			continue
		}
		if !strings.Contains(gvrData.ExpectedEtcdPath, "/"+gvr.Group+"/"+gvr.Resource+"/") {
			// CRD 数据存放在 .../<group>/<resource>/... 路径下。
			continue
		}
		detectedOverlappingResources[gvr.GroupResource()] = true
	}

	for detected := range detectedOverlappingResources {
		if !finalizer.OverlappingBuiltInResources()[detected] {
			t.Errorf("built-in resource %#v would overlap with custom resource storage if a CRD was created for the same group/resource", detected)
			t.Errorf("add %#v to the OverlappingBuiltInResources() list to prevent deletion by the CRD finalizer", detected)
		}
	}
	for skip := range finalizer.OverlappingBuiltInResources() {
		if !detectedOverlappingResources[skip] {
			t.Errorf("resource %#v does not overlap with any built-in resources in storage, but is skipped for CRD finalization by OverlappingBuiltInResources()", skip)
			t.Errorf("remove %#v from OverlappingBuiltInResources() to ensure CRD finalization cleans up stored custom resources", skip)
		}
	}
}

// TestOverlappingCustomResourceAPIService 确保创建并删除与 APIService 存储路径重叠的自定义资源不会破坏 APIService 数据。
func TestOverlappingCustomResourceAPIService(t *testing.T) {
	apiServer := StartRealAPIServerOrDie(t)
	defer apiServer.Cleanup()

	apiServiceClient, err := apiregistrationclient.NewForConfig(apiServer.Config)
	if err != nil {
		t.Fatal(err)
	}
	crdClient, err := crdclient.NewForConfig(apiServer.Config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(apiServer.Config)
	if err != nil {
		t.Fatal(err)
	}

	// 先确认 APIService 列表可正常获取。
	apiServices, err := apiServiceClient.APIServices().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	apiServiceNames := sets.NewString()
	for _, s := range apiServices.Items {
		apiServiceNames.Insert(s.Name)
	}
	if len(apiServices.Items) == 0 {
		t.Fatal("expected APIService objects, got none")
	}

	// 创建一个 CRD 覆盖 apiregistration.k8s.io/apiservices 路径，并使用不兼容的 schema。
	crdCRD, err := crdClient.CustomResourceDefinitions().Create(context.TODO(), &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "apiservices.apiregistration.k8s.io",
			Annotations: map[string]string{"api-approved.kubernetes.io": "unapproved, testing only"},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "apiregistration.k8s.io",
			Scope: apiextensionsv1.ClusterScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "apiservices", Singular: "customapiservice", Kind: "CustomAPIService", ListKind: "CustomAPIServiceList"},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1",
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:     "object",
							Required: []string{"foo"},
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"foo": {Type: "string"},
								"bar": {Type: "string", Default: &apiextensionsv1.JSON{Raw: []byte(`"default"`)}},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 等待 CRD 完成 established。
	if err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		crd, err := crdClient.CustomResourceDefinitions().Get(context.TODO(), crdCRD.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range crd.Status.Conditions {
			if condition.Status == apiextensionsv1.ConditionTrue && condition.Type == apiextensionsv1.Established {
				return true, nil
			}
		}
		conditionJSON, _ := json.Marshal(crd.Status.Conditions)
		t.Logf("waiting for establishment (conditions: %s)", string(conditionJSON))
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	// 确认 API 请求仍由内建 handler 处理，返回内建 Kind。

	// v1 列表请求应成功。
	v1DynamicList, err := dynamicClient.Resource(schema.GroupVersionResource{Group: "apiregistration.k8s.io", Version: "v1", Resource: "apiservices"}).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 结果应来自内建 handler，而非 CR handler。
	if _, hasDefaultedCRField := v1DynamicList.Items[0].Object["spec"].(map[string]interface{})["bar"]; hasDefaultedCRField {
		t.Fatalf("expected no CR defaulting, got %#v", v1DynamicList.Items[0].Object)
	}

	// v1 创建请求应成功，并使用内建校验而非 CR 校验。
	testAPIService, err := apiServiceClient.APIServices().Create(context.TODO(), &apiregistrationv1.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1.example.com"},
		Spec: apiregistrationv1.APIServiceSpec{
			Group:                "example.com",
			Version:              "v1",
			VersionPriority:      100,
			GroupPriorityMinimum: 100,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = apiServiceClient.APIServices().Delete(context.TODO(), testAPIService.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// discovery 也应交给内建 handler 处理。
	v1Resources, err := apiServer.Client.Discovery().ServerResourcesForGroupVersion("apiregistration.k8s.io/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range v1Resources.APIResources {
		if r.Name == "apiservices" {
			if r.Kind != "APIService" {
				t.Errorf("expected kind=APIService in discovery, got %s", r.Kind)
			}
		}
	}
	v2Resources, err := apiServer.Client.Discovery().ServerResourcesForGroupVersion("apiregistration.k8s.io/v2")
	if err == nil {
		t.Fatalf("expected error looking up apiregistration.k8s.io/v2 discovery, got %#v", v2Resources)
	}

	// 删除这个存储路径重叠的 CRD。
	err = crdClient.CustomResourceDefinitions().Delete(context.TODO(), crdCRD.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 等待 CRD 彻底删除。
	if err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		crd, err := crdClient.CustomResourceDefinitions().Get(context.TODO(), crdCRD.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		conditionJSON, _ := json.Marshal(crd.Status.Conditions)
		t.Logf("waiting for deletion (conditions: %s)", string(conditionJSON))
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	// CRD 删除后原有 APIService 对象不应被清理。
	time.Sleep(5 * time.Second)
	finalAPIServices, err := apiServiceClient.APIServices().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(finalAPIServices.Items) != len(apiServices.Items) {
		t.Fatalf("expected %d APIService objects, got %d", len(apiServices.Items), len(finalAPIServices.Items))
	}
}

// TestOverlappingCustomResourceCustomResourceDefinition 确保创建并删除与 CustomResourceDefinition 存储路径重叠的自定义资源不会破坏 CRD 数据。
func TestOverlappingCustomResourceCustomResourceDefinition(t *testing.T) {
	apiServer := StartRealAPIServerOrDie(t)
	defer apiServer.Cleanup()

	crdClient, err := crdclient.NewForConfig(apiServer.Config)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(apiServer.Config)
	if err != nil {
		t.Fatal(err)
	}

	// 先确认 CustomResourceDefinition 列表可正常获取。
	crds, err := crdClient.CustomResourceDefinitions().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	crdNames := sets.NewString()
	for _, s := range crds.Items {
		crdNames.Insert(s.Name)
	}
	if len(crds.Items) == 0 {
		t.Fatal("expected CustomResourceDefinition objects, got none")
	}

	// 创建一个 CRD 覆盖 apiregistration.k8s.io/apiservices 路径，并使用不兼容的 schema。
	crdCRD, err := crdClient.CustomResourceDefinitions().Create(context.TODO(), &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "customresourcedefinitions.apiextensions.k8s.io",
			Annotations: map[string]string{"api-approved.kubernetes.io": "unapproved, testing only"},
		},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "apiextensions.k8s.io",
			Scope: apiextensionsv1.ClusterScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   "customresourcedefinitions",
				Singular: "customcustomresourcedefinition",
				Kind:     "CustomCustomResourceDefinition",
				ListKind: "CustomAPIServiceList",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
				{
					Name:    "v1",
					Served:  true,
					Storage: true,
					Schema: &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
							Type:     "object",
							Required: []string{"foo"},
							Properties: map[string]apiextensionsv1.JSONSchemaProps{
								"foo": {Type: "string"},
								"bar": {Type: "string", Default: &apiextensionsv1.JSON{Raw: []byte(`"default"`)}},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 等待 CRD 完成 established。
	if err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		crd, err := crdClient.CustomResourceDefinitions().Get(context.TODO(), crdCRD.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, condition := range crd.Status.Conditions {
			if condition.Status == apiextensionsv1.ConditionTrue && condition.Type == apiextensionsv1.Established {
				return true, nil
			}
		}
		conditionJSON, _ := json.Marshal(crd.Status.Conditions)
		t.Logf("waiting for establishment (conditions: %s)", string(conditionJSON))
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	// 确认 API 请求仍由内建 handler 处理，返回内建 Kind。

	// v1 列表请求应成功。
	v1DynamicList, err := dynamicClient.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 结果应来自内建 handler，而非 CR handler。
	if _, hasDefaultedCRField := v1DynamicList.Items[0].Object["spec"].(map[string]interface{})["bar"]; hasDefaultedCRField {
		t.Fatalf("expected no CR defaulting, got %#v", v1DynamicList.Items[0].Object)
	}

	// v1 更新请求应成功，并使用内建校验而非 CR 校验。
	_, err = crdClient.CustomResourceDefinitions().Patch(context.TODO(), crdCRD.Name, types.MergePatchType, []byte(`{"metadata":{"annotations":{"test":"updated"}}}`), metav1.PatchOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// discovery 也应交给内建 handler 处理。
	v1Resources, err := apiServer.Client.Discovery().ServerResourcesForGroupVersion("apiextensions.k8s.io/v1")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range v1Resources.APIResources {
		if r.Name == "customresourcedefinitions" {
			if r.Kind != "CustomResourceDefinition" {
				t.Errorf("expected kind=CustomResourceDefinition in discovery, got %s", r.Kind)
			}
		}
	}
	v2Resources, err := apiServer.Client.Discovery().ServerResourcesForGroupVersion("apiextensions.k8s.io/v2")
	if err == nil {
		t.Fatalf("expected error looking up apiregistration.k8s.io/v2 discovery, got %#v", v2Resources)
	}

	// 删除这个存储路径重叠的 CRD。
	err = crdClient.CustomResourceDefinitions().Delete(context.TODO(), crdCRD.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 等待 CRD 彻底删除。
	if err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		crd, err := crdClient.CustomResourceDefinitions().Get(context.TODO(), crdCRD.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		conditionJSON, _ := json.Marshal(crd.Status.Conditions)
		t.Logf("waiting for deletion (conditions: %s)", string(conditionJSON))
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}

	// CRD 删除后其他 CustomResourceDefinition 对象不应被清理。
	time.Sleep(5 * time.Second)
	finalCRDs, err := crdClient.CustomResourceDefinitions().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(finalCRDs.Items) != len(crds.Items) {
		t.Fatalf("expected %d APIService objects, got %d", len(crds.Items), len(finalCRDs.Items))
	}
}
