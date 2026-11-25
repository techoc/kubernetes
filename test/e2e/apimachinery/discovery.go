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

package apimachinery

import (
	"context"
	"fmt"
	"path"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	clientdiscovery "k8s.io/client-go/discovery"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	"k8s.io/kubernetes/test/utils/crd"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var storageVersionServerVersion = utilversion.MustParseSemantic("v1.13.99")
var _ = SIGDescribe("Discovery", func() {
	f := framework.NewDefaultFramework("discovery")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	var namespaceName string

	ginkgo.BeforeEach(func() {
		namespaceName = f.Namespace.Name

		e2eskipper.SkipUnlessServerVersionGTE(storageVersionServerVersion, f.ClientSet.Discovery())

		ginkgo.By("Setting up server cert")
		setupServerCert(namespaceName, serviceName)
	})

	ginkgo.It("should accurately determine present and missing resources", func(ctx context.Context) {
		// 检查 legacy api group 的资源是否工作
		ok, err := clientdiscovery.IsResourceEnabled(f.ClientSet.Discovery(), schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"})
		framework.ExpectNoError(err)
		if !ok {
			framework.Failf("namespace.v1 should always be present")
		}
		// 检查非 legacy api group 的资源是否工作
		ok, err = clientdiscovery.IsResourceEnabled(f.ClientSet.Discovery(), schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"})
		framework.ExpectNoError(err)
		if !ok {
			framework.Failf("deployments.v1.apps should always be present")
		}
		// 检查已有 api group 中无意义的资源名是否按预期处理
		ok, err = clientdiscovery.IsResourceEnabled(f.ClientSet.Discovery(), schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "please-dont-ever-create-this"})
		framework.ExpectNoError(err)
		if ok {
			framework.Failf("please-dont-ever-create-this.v1.apps should never be present")
		}
		// 检查无意义 api group 中的资源是否按预期处理
		ok, err = clientdiscovery.IsResourceEnabled(f.ClientSet.Discovery(), schema.GroupVersionResource{Group: "not-these-apps", Version: "v1", Resource: "deployments"})
		framework.ExpectNoError(err)
		if ok {
			framework.Failf("deployments.v1.not-these-apps should never be present")
		}
	})

	ginkgo.It("Custom resource should have storage version hash", func(ctx context.Context) {
		testcrd, err := crd.CreateTestCRD(f)
		if err != nil {
			return
		}
		ginkgo.DeferCleanup(testcrd.CleanUp)
		spec := testcrd.Crd.Spec
		resources, err := testcrd.APIExtensionClient.Discovery().ServerResourcesForGroupVersion(spec.Group + "/" + spec.Versions[0].Name)
		if err != nil {
			framework.Failf("failed to find the discovery doc for %v: %v", resources, err)
		}
		found := false
		var storageVersion string
		for _, v := range spec.Versions {
			if v.Storage {
				storageVersion = v.Name
			}
		}
		// 免责声明：storageVersionHash 的推导算法属于实现细节，
		// 客户端不应依赖它。以下计算仅用于测试。
		expected := discovery.StorageVersionHash(spec.Group, storageVersion, spec.Names.Kind)

		for _, r := range resources.APIResources {
			if r.Name == spec.Names.Plural {
				found = true
				if r.StorageVersionHash != expected {
					framework.Failf("expected storageVersionHash of %s/%s/%s to be %s, got %s", r.Group, r.Version, r.Name, expected, r.StorageVersionHash)
				}
			}
		}
		if !found {
			framework.Failf("didn't find resource %s in the discovery doc", spec.Names.Plural)
		}
	})

	/*
	   Release : v1.19
	   Testname: Discovery, confirm the PreferredVersion for each api group
	   Description: 确保能取到 api 列表。
	   除后缀为 example.com 的 group 外，每个 api group MUST 返回有效的 PreferredVersion。
	*/
	framework.ConformanceIt("should validate PreferredVersion for each APIGroup", func(ctx context.Context) {

		// 获取 APIGroup endpoint 列表
		list := &metav1.APIGroupList{}
		err := f.ClientSet.Discovery().RESTClient().Get().AbsPath("/apis/").Do(ctx).Into(list)
		framework.ExpectNoError(err, "Failed to find /apis/")
		gomega.Expect(list.Groups).ToNot(gomega.BeEmpty(), "Missing APIGroups")

		for _, group := range list.Groups {
			if strings.HasSuffix(group.Name, ".example.com") {
				// 忽略 e2e 测试运行期间动态增删的已知示例 API group
				continue
			}
			framework.Logf("Checking APIGroup: %v", group.Name)

			// 定位 APIGroup endpoint
			checkGroup := &metav1.APIGroup{}
			apiPath := "/apis/" + group.Name + "/"
			err = f.ClientSet.Discovery().RESTClient().Get().AbsPath(apiPath).Do(ctx).Into(checkGroup)
			framework.ExpectNoError(err, "Fail to access: %s", apiPath)
			gomega.Expect(checkGroup.Versions).ToNot(gomega.BeEmpty(), "No version found for %v", group.Name)
			framework.Logf("PreferredVersion.GroupVersion: %s", checkGroup.PreferredVersion.GroupVersion)
			framework.Logf("Versions found %v", checkGroup.Versions)

			// 确认 PreferredVersion 是列出的有效版本之一
			match := false
			for _, version := range checkGroup.Versions {
				if version.GroupVersion == checkGroup.PreferredVersion.GroupVersion {
					framework.Logf("%s matches %s", version.GroupVersion, checkGroup.PreferredVersion.GroupVersion)
					match = true
					break
				}
			}
			if !match {
				framework.Failf("Failed to find a valid version for PreferredVersion %s in versions:\n%s", checkGroup.PreferredVersion.GroupVersion, format.Object(checkGroup.Versions, 1))
			}
		}
	})

	/*
		Release: v1.28
		Testname: Discovery, confirm the groupVerion and a resourcefrom each apiGroup
		Description: 取到的每个 apiGroup MUST 都能找到对应的 resourceList。
		每个 apiGroup 的 groupVersion MUST 与 schema 报告的一致。
		每个 resourceList 中 MUST 能找到至少一个有效资源。
	*/
	framework.ConformanceIt("should locate the groupVersion and a resource within each APIGroup", func(ctx context.Context) {

		tests := []struct {
			apiBasePath   string
			apiGroup      string
			apiVersion    string
			validResource string
		}{
			{
				apiBasePath:   "/api",
				apiGroup:      "",
				apiVersion:    "v1",
				validResource: "namespaces",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "admissionregistration.k8s.io",
				apiVersion:    "v1",
				validResource: "validatingwebhookconfigurations",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "apiextensions.k8s.io",
				apiVersion:    "v1",
				validResource: "customresourcedefinitions",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "apiregistration.k8s.io",
				apiVersion:    "v1",
				validResource: "apiservices",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "apps",
				apiVersion:    "v1",
				validResource: "deployments",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "authentication.k8s.io",
				apiVersion:    "v1",
				validResource: "tokenreviews",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "authorization.k8s.io",
				apiVersion:    "v1",
				validResource: "selfsubjectaccessreviews",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "autoscaling",
				apiVersion:    "v1",
				validResource: "horizontalpodautoscalers",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "autoscaling",
				apiVersion:    "v2",
				validResource: "horizontalpodautoscalers",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "batch",
				apiVersion:    "v1",
				validResource: "jobs",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "certificates.k8s.io",
				apiVersion:    "v1",
				validResource: "certificatesigningrequests",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "coordination.k8s.io",
				apiVersion:    "v1",
				validResource: "leases",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "discovery.k8s.io",
				apiVersion:    "v1",
				validResource: "endpointslices",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "events.k8s.io",
				apiVersion:    "v1",
				validResource: "events",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "networking.k8s.io",
				apiVersion:    "v1",
				validResource: "ingresses",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "node.k8s.io",
				apiVersion:    "v1",
				validResource: "runtimeclasses",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "policy",
				apiVersion:    "v1",
				validResource: "poddisruptionbudgets",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "scheduling.k8s.io",
				apiVersion:    "v1",
				validResource: "priorityclasses",
			},
			{
				apiBasePath:   "/apis",
				apiGroup:      "storage.k8s.io",
				apiVersion:    "v1",
				validResource: "csinodes",
			},
		}

		for _, t := range tests {
			resourceList := &metav1.APIResourceList{}
			apiPath := path.Join(t.apiBasePath, t.apiGroup, t.apiVersion)
			ginkgo.By(fmt.Sprintf("Requesting APIResourceList from %q", apiPath))
			err := f.ClientSet.Discovery().RESTClient().Get().AbsPath(apiPath).Do(ctx).Into(resourceList)
			framework.ExpectNoError(err, "Fail to access: %s", apiPath)
			gomega.Expect(resourceList.GroupVersion).To(gomega.Equal((schema.GroupVersion{Group: t.apiGroup, Version: t.apiVersion}).String()))

			foundResource := false
			for _, r := range resourceList.APIResources {
				if t.validResource == r.Name {
					foundResource = true
					break
				}
			}
			gomega.Expect(foundResource).To(gomega.BeTrueBecause("Resource %q was not found inside of resourceList\n%#v", t.validResource, resourceList.APIResources))
		}
	})
})
