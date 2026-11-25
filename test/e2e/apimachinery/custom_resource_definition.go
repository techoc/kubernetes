/*
Copyright 2016 The Kubernetes Authors.

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
	"fmt"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"
	apimachineryutils "k8s.io/kubernetes/test/e2e/common/apimachinery"
	"k8s.io/kubernetes/test/e2e/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = SIGDescribe("CustomResourceDefinition resources [Privileged:ClusterAdmin]", func() {

	f := framework.NewDefaultFramework("custom-resource-definition")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("Simple CustomResourceDefinition", func() {
		/*
			Release: v1.9
			Testname: Custom Resource Definition, create
			Description: 创建一个 API extension client 并定义一个随机的 CustomResourceDefinition。
			创建该 CustomResourceDefinition，然后删除它。创建和删除 MUST 成功。
		*/
		framework.ConformanceIt("creating/deleting custom resource definition objects works", func(ctx context.Context) {

			config, err := framework.LoadConfig()
			framework.ExpectNoError(err, "loading config")
			apiExtensionClient, err := clientset.NewForConfig(config)
			framework.ExpectNoError(err, "initializing apiExtensionClient")

			randomDefinition := fixtures.NewRandomNameV1CustomResourceDefinition(v1.ClusterScoped)

			// 创建 CRD，并等待该资源被识别且可用。
			randomDefinition, err = fixtures.CreateNewV1CustomResourceDefinitionWatchUnsafe(randomDefinition, apiExtensionClient)
			framework.ExpectNoError(err, "creating CustomResourceDefinition")

			defer func() {
				err = fixtures.DeleteV1CustomResourceDefinition(randomDefinition, apiExtensionClient)
				framework.ExpectNoError(err, "deleting CustomResourceDefinition")
			}()
		})

		/*
			Release: v1.16
			Testname: Custom Resource Definition, list
			Description: 创建一个 API extension client，定义 10 个带标签的 CustomResourceDefinition，并使用 label selector
			列出它们；list 结果 MUST 只包含带标签的 CustomResourceDefinition。通过 delete collection 删除带标签的
			CustomResourceDefinition；删除 MUST 成功，并且 MUST 只删除带标签的 CustomResourceDefinition。
		*/
		framework.ConformanceIt("listing custom resource definition objects works", func(ctx context.Context) {
			testListSize := 10
			config, err := framework.LoadConfig()
			framework.ExpectNoError(err, "loading config")
			apiExtensionClient, err := clientset.NewForConfig(config)
			framework.ExpectNoError(err, "initializing apiExtensionClient")

			// 给创建的 CRD 打上标签，这样即使它们是 cluster scoped，也只列出这些 CRD
			testUUID := string(uuid.NewUUID())

			// 创建 CRD，并等待该资源被识别且可用。
			crds := make([]*v1.CustomResourceDefinition, testListSize)
			for i := range testListSize {
				crd := fixtures.NewRandomNameV1CustomResourceDefinition(v1.ClusterScoped)
				crd.Labels = map[string]string{"e2e-list-test-uuid": testUUID}
				crd, err = fixtures.CreateNewV1CustomResourceDefinitionWatchUnsafe(crd, apiExtensionClient)
				framework.ExpectNoError(err, "creating CustomResourceDefinition")
				crds[i] = crd
			}

			// 创建一个不带标签的 CRD，以验证 label selector 的匹配是否正确
			crd := fixtures.NewRandomNameV1CustomResourceDefinition(v1.ClusterScoped)
			crd, err = fixtures.CreateNewV1CustomResourceDefinitionWatchUnsafe(crd, apiExtensionClient)
			framework.ExpectNoError(err, "creating CustomResourceDefinition")
			defer func() {
				err = fixtures.DeleteV1CustomResourceDefinition(crd, apiExtensionClient)
				framework.ExpectNoError(err, "deleting CustomResourceDefinition")
			}()

			selectorListOpts := metav1.ListOptions{LabelSelector: "e2e-list-test-uuid=" + testUUID}
			list, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().List(ctx, selectorListOpts)
			framework.ExpectNoError(err, "listing CustomResourceDefinitions")
			gomega.Expect(list.Items).To(gomega.HaveLen(testListSize))
			for _, actual := range list.Items {
				var expected *v1.CustomResourceDefinition
				for _, e := range crds {
					if e.Name == actual.Name && e.Namespace == actual.Namespace {
						expected = e
					}
				}
				gomega.Expect(expected).ToNot(gomega.BeNil())
				if !equality.Semantic.DeepEqual(actual.Spec, expected.Spec) {
					framework.Failf("Expected CustomResourceDefinition in list with name %s to match crd created with same name, but got different specs:\n%s",
						actual.Name, cmp.Diff(expected.Spec, actual.Spec))
				}
			}

			// 用 delete collection 删除这些 CRD
			err = fixtures.DeleteV1CustomResourceDefinitions(selectorListOpts, apiExtensionClient)
			framework.ExpectNoError(err, "deleting CustomResourceDefinitions")
			_, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "getting remaining CustomResourceDefinition")
		})

		/*
			Release: v1.16
			Testname: Custom Resource Definition, status sub-resource
			Description: 创建一个 CustomResourceDefinition。尝试读取、更新和 patch 它的 status sub-resource；
			所有修改 sub-resource 的操作 MUST 能在后续读取中看到。
		*/
		framework.ConformanceIt("getting/updating/patching custom resource definition status sub-resource works", func(ctx context.Context) {
			config, err := framework.LoadConfig()
			framework.ExpectNoError(err, "loading config")
			apiExtensionClient, err := clientset.NewForConfig(config)
			framework.ExpectNoError(err, "initializing apiExtensionClient")
			dynamicClient, err := dynamic.NewForConfig(config)
			framework.ExpectNoError(err, "initializing dynamic client")
			gvr := v1.SchemeGroupVersion.WithResource("customresourcedefinitions")
			resourceClient := dynamicClient.Resource(gvr)

			// 创建 CRD，并等待该资源被识别且可用。
			crd := fixtures.NewRandomNameV1CustomResourceDefinition(v1.ClusterScoped)
			crd, err = fixtures.CreateNewV1CustomResourceDefinitionWatchUnsafe(crd, apiExtensionClient)
			framework.ExpectNoError(err, "creating CustomResourceDefinition")
			defer func() {
				err = fixtures.DeleteV1CustomResourceDefinition(crd, apiExtensionClient)
				framework.ExpectNoError(err, "deleting CustomResourceDefinition")
			}()

			var updated *v1.CustomResourceDefinition
			updateCondition := v1.CustomResourceDefinitionCondition{Message: "updated"}
			err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
				// 用 dynamic client 读取 status sub-resource，因为 typed client 没有暴露它。
				u, err := resourceClient.Get(ctx, crd.GetName(), metav1.GetOptions{}, "status")
				framework.ExpectNoError(err, "getting CustomResourceDefinition status")
				status := unstructuredToCRD(u)
				if !equality.Semantic.DeepEqual(status.Spec, crd.Spec) {
					framework.Failf("Expected CustomResourceDefinition Spec to match status sub-resource Spec, but got:\n%s", cmp.Diff(status.Spec, crd.Spec))
				}
				status.Status.Conditions = append(status.Status.Conditions, updateCondition)
				updated, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().UpdateStatus(ctx, status, metav1.UpdateOptions{})
				return err
			})
			framework.ExpectNoError(err, "updating CustomResourceDefinition status")
			expectCondition(updated.Status.Conditions, updateCondition)

			patchCondition := v1.CustomResourceDefinitionCondition{Message: "patched"}
			patched, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Patch(ctx, crd.GetName(),
				types.JSONPatchType,
				[]byte(`[{"op": "add", "path": "/status/conditions", "value": [{"message": "patched"}]}]`), metav1.PatchOptions{},
				"status")
			framework.ExpectNoError(err, "patching CustomResourceDefinition status")
			expectCondition(updated.Status.Conditions, updateCondition)
			expectCondition(patched.Status.Conditions, patchCondition)
		})
	})

	/*
		Release: v1.16
		Testname: Custom Resource Definition, discovery
		Description: 获取 /apis、/apis/apiextensions.k8s.io 和 /apis/apiextensions.k8s.io/v1 discovery 文档，
		并确认它们表明 CustomResourceDefinition apiextensions.k8s.io/v1 资源可用。
	*/
	framework.ConformanceIt("should include custom resource definition resources in discovery documents", func(ctx context.Context) {
		{
			ginkgo.By("fetching the /apis discovery document")
			apiGroupList := &metav1.APIGroupList{}
			err := f.ClientSet.Discovery().RESTClient().Get().AbsPath("/apis").Do(ctx).Into(apiGroupList)
			framework.ExpectNoError(err, "fetching /apis")

			ginkgo.By("finding the apiextensions.k8s.io API group in the /apis discovery document")
			var group *metav1.APIGroup
			for _, g := range apiGroupList.Groups {
				if g.Name == v1.GroupName {
					group = &g
					break
				}
			}
			gomega.Expect(group).ToNot(gomega.BeNil(), "apiextensions.k8s.io API group not found in /apis discovery document")

			ginkgo.By("finding the apiextensions.k8s.io/v1 API group/version in the /apis discovery document")
			var version *metav1.GroupVersionForDiscovery
			for _, v := range group.Versions {
				if v.Version == v1.SchemeGroupVersion.Version {
					version = &v
					break
				}
			}
			gomega.Expect(version).ToNot(gomega.BeNil(), "apiextensions.k8s.io/v1 API group version not found in /apis discovery document")
		}

		{
			ginkgo.By("fetching the /apis/apiextensions.k8s.io discovery document")
			group := &metav1.APIGroup{}
			err := f.ClientSet.Discovery().RESTClient().Get().AbsPath("/apis/apiextensions.k8s.io").Do(ctx).Into(group)
			framework.ExpectNoError(err, "fetching /apis/apiextensions.k8s.io")
			gomega.Expect(group.Name).To(gomega.Equal(v1.GroupName), "verifying API group name in /apis/apiextensions.k8s.io discovery document")

			ginkgo.By("finding the apiextensions.k8s.io/v1 API group/version in the /apis/apiextensions.k8s.io discovery document")
			var version *metav1.GroupVersionForDiscovery
			for _, v := range group.Versions {
				if v.Version == v1.SchemeGroupVersion.Version {
					version = &v
					break
				}
			}
			gomega.Expect(version).ToNot(gomega.BeNil(), "apiextensions.k8s.io/v1 API group version not found in /apis/apiextensions.k8s.io discovery document")
		}

		{
			ginkgo.By("fetching the /apis/apiextensions.k8s.io/v1 discovery document")
			apiResourceList := &metav1.APIResourceList{}
			err := f.ClientSet.Discovery().RESTClient().Get().AbsPath("/apis/apiextensions.k8s.io/v1").Do(ctx).Into(apiResourceList)
			framework.ExpectNoError(err, "fetching /apis/apiextensions.k8s.io/v1")
			gomega.Expect(apiResourceList.GroupVersion).To(gomega.Equal(v1.SchemeGroupVersion.String()), "verifying API group/version in /apis/apiextensions.k8s.io/v1 discovery document")

			ginkgo.By("finding customresourcedefinitions resources in the /apis/apiextensions.k8s.io/v1 discovery document")
			var crdResource *metav1.APIResource
			for i := range apiResourceList.APIResources {
				if apiResourceList.APIResources[i].Name == "customresourcedefinitions" {
					crdResource = &apiResourceList.APIResources[i]
				}
			}
			gomega.Expect(crdResource).ToNot(gomega.BeNil(), "customresourcedefinitions resource not found in /apis/apiextensions.k8s.io/v1 discovery document")
		}
	})

	/*
		Release: v1.17
		Testname: Custom Resource Definition, defaulting
		Description: 创建一个没有默认值的 CustomResourceDefinition，然后创建一个 CR。添加默认值并读取 CR，直到默认值生效。
		再创建另一个 CR。移除原有默认值，为另一个字段添加默认值并读取 CR，直到新字段被默认值填充，而旧字段的默认值仍然保留。
	*/
	framework.ConformanceIt("custom resource defaulting for requests and from storage works", func(ctx context.Context) {
		config, err := framework.LoadConfig()
		framework.ExpectNoError(err, "loading config")
		apiExtensionClient, err := clientset.NewForConfig(config)
		framework.ExpectNoError(err, "initializing apiExtensionClient")
		dynamicClient, err := dynamic.NewForConfig(config)
		framework.ExpectNoError(err, "initializing dynamic client")

		// 创建没有默认值的 CRD，并等待该资源被识别且可用。
		crd := fixtures.NewRandomNameV1CustomResourceDefinition(v1.ClusterScoped)
		if crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties == nil {
			crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties = map[string]v1.JSONSchemaProps{}
		}
		crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["a"] = v1.JSONSchemaProps{Type: "string"}
		crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["b"] = v1.JSONSchemaProps{Type: "string"}
		crd, err = fixtures.CreateNewV1CustomResourceDefinitionWatchUnsafe(crd, apiExtensionClient)
		framework.ExpectNoError(err, "creating CustomResourceDefinition")
		defer func() {
			err = fixtures.DeleteV1CustomResourceDefinition(crd, apiExtensionClient)
			framework.ExpectNoError(err, "deleting CustomResourceDefinition")
		}()

		// 在 storage 中没有默认值时创建 CR
		name1 := names.SimpleNameGenerator.GenerateName("cr-1")
		gvr := schema.GroupVersionResource{
			Group:    crd.Spec.Group,
			Version:  crd.Spec.Versions[0].Name,
			Resource: crd.Spec.Names.Plural,
		}
		crClient := dynamicClient.Resource(gvr)
		u1, err := crClient.Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       crd.Spec.Names.Kind,
			"metadata": map[string]interface{}{
				"name": name1,
			},
		}}, metav1.CreateOptions{})
		framework.ExpectNoError(err, "creating CR")
		gomega.Expect(u1).To(apimachineryutils.HaveValidResourceVersion())
		// 将 a 的默认值设为 "A"，等待读取 CR 时默认值被填充
		crd, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Patch(ctx, crd.Name, types.JSONPatchType, []byte(`[
			{"op":"add","path":"/spec/versions/0/schema/openAPIV3Schema/properties/a/default", "value": "A"}
		]`), metav1.PatchOptions{})
		framework.ExpectNoError(err, "setting default for a to \"A\" in schema")

		err = wait.PollImmediate(time.Millisecond*100, wait.ForeverTestTimeout, func() (bool, error) {
			u1, err = crClient.Get(ctx, name1, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			a, found, err := unstructured.NestedFieldNoCopy(u1.Object, "a")
			if err != nil {
				return false, err
			}
			if !found {
				return false, nil
			}
			if a != "A" {
				return false, fmt.Errorf("expected a:\"A\", but got a:%q", a)
			}
			return true, nil
		})
		framework.ExpectNoError(err, "waiting for CR to be defaulted on read")

		// 在 storage 中有默认值时创建 CR
		name2 := names.SimpleNameGenerator.GenerateName("cr-2")
		u2, err := crClient.Create(ctx, &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": gvr.Group + "/" + gvr.Version,
			"kind":       crd.Spec.Names.Kind,
			"metadata": map[string]interface{}{
				"name": name2,
			},
		}}, metav1.CreateOptions{})
		framework.ExpectNoError(err, "creating CR")
		v, found, err := unstructured.NestedFieldNoCopy(u2.Object, "a")
		if !found {
			framework.Failf("field `a` should have been defaulted in %+v", u2.Object)
		}
		gomega.Expect(v).To(gomega.Equal("A"), "\"a\" is defaulted to \"A\"")

		// 删除 a 的默认值，为 b 添加默认值 "B"，等待读取 CR 时 b 的默认值被填充
		crd, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Patch(ctx, crd.Name, types.JSONPatchType, []byte(`[
			{"op":"remove","path":"/spec/versions/0/schema/openAPIV3Schema/properties/a/default"},
			{"op":"add","path":"/spec/versions/0/schema/openAPIV3Schema/properties/b/default", "value": "B"}
		]`), metav1.PatchOptions{})
		framework.ExpectNoError(err, "setting default for b to \"B\" and remove default for a")

		err = wait.PollImmediate(time.Millisecond*100, wait.ForeverTestTimeout, func() (bool, error) {
			u2, err := crClient.Get(ctx, name2, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			b, found, err := unstructured.NestedFieldNoCopy(u2.Object, "b")
			if err != nil {
				return false, err
			}
			if !found {
				return false, nil
			}
			if b != "B" {
				return false, fmt.Errorf("expected b:\"B\", but got b:%q", b)
			}
			a, found, err := unstructured.NestedFieldNoCopy(u2.Object, "a")
			if err != nil {
				return false, err
			}
			if !found {
				return false, fmt.Errorf("expected a:\"A\" to be unchanged, but it was removed")
			}
			if a != "A" {
				return false, fmt.Errorf("expected a:\"A\" to be unchanged, but it changed to %q", a)
			}
			return true, nil
		})
		framework.ExpectNoError(err, "waiting for CR to be defaulted on read for b and a staying the same")
		gomega.Expect(resourceversion.CompareResourceVersion(u1.GetResourceVersion(), u2.GetResourceVersion())).To(gomega.BeNumerically("==", -1), "second created object should have a larger resource version")
	})

})

func unstructuredToCRD(obj *unstructured.Unstructured) *v1.CustomResourceDefinition {
	crd := new(v1.CustomResourceDefinition)
	err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, crd)
	framework.ExpectNoError(err, "converting unstructured to CustomResourceDefinition")
	return crd
}

func expectCondition(conditions []v1.CustomResourceDefinitionCondition, expected v1.CustomResourceDefinitionCondition) {
	for _, c := range conditions {
		if equality.Semantic.DeepEqual(c, expected) {
			return
		}
	}
	framework.Failf("Condition %#v not found in conditions %#v", expected, conditions)
}
