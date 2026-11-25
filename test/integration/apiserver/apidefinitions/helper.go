/*
Copyright The Kubernetes Authors.

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

package apidefinitions

import (
	"context"
	"slices"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	apiextensionsclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	apiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/etcd"
	"k8s.io/kubernetes/test/integration/framework"
)

type DefinitionTestFunc func(t *testing.T, setup Definition)

type Definition struct {
	Config        *rest.Config
	Client        kubernetes.Interface
	DynamicClient dynamic.Interface
	Mapping       *meta.RESTMapping
	Resource      metav1.APIResource
	Namespace     string
	StorageData   etcd.StorageData
	Subresources  []string
}

func (d *Definition) HasVerb(verb string) bool {
	return slices.Contains(d.Resource.Verbs, verb)
}

func (d *Definition) HasSubresource(subresource string) bool {
	return slices.Contains(d.Subresources, subresource)
}

func (d *Definition) HasStatus() bool {
	return d.HasSubresource("status")
}

// ResourceClient 返回一个限定在适当命名空间内的 dynamic resource client。
func (d *Definition) ResourceClient() dynamic.ResourceInterface {
	namespace := d.Namespace
	if d.Mapping.Scope == meta.RESTScopeRoot {
		namespace = ""
	}
	return d.DynamicClient.Resource(d.Mapping.Resource).Namespace(namespace)
}

// TestAllDefinitions 启动一个 apiserver，并对每个
// 可发现的资源运行 testFunc。它会注册一组固定的 CRD，
// 以便让一部分自定义资源可被发现和测试。
func TestAllDefinitions(t *testing.T, testNamespace string, testFunc DefinitionTestFunc) {
	server, err := apiservertesting.StartTestServer(t, apiservertesting.NewDefaultTestServerOptions(), []string{
		"--disable-admission-plugins", "ServiceAccount,TaintNodesByCondition",
		// 启用所有 API 和特性
		"--runtime-config=api/all=true",
		"--feature-gates=AllAlpha=true,AllBeta=true",
	}, framework.SharedEtcd())
	if err != nil {
		t.Fatal(err)
	}
	defer server.TearDownFn()

	client, err := kubernetes.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}
	dynamicClient, err := dynamic.NewForConfig(server.ClientConfig)
	if err != nil {
		t.Fatal(err)
	}

	etcd.CreateTestCRDs(t, apiextensionsclientset.NewForConfigOrDie(server.ClientConfig), false, etcd.GetCustomResourceDefinitionData()...)

	if _, err := client.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNamespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	storageData := etcd.GetEtcdStorageDataForNamespace(testNamespace)

	_, resourceLists, err := client.Discovery().ServerGroupsAndResources()
	if err != nil {
		t.Fatalf("Failed to get ServerGroupsAndResources: %v", err)
	}

	for _, resourceList := range resourceLists {
		for _, resource := range resourceList.APIResources {
			// 只遍历根资源
			if strings.Contains(resource.Name, "/") {
				continue
			}

			mapping, err := CreateMapping(resourceList.GroupVersion, resource)
			if err != nil {
				t.Fatal(err)
			}

			t.Run(mapping.Resource.String(), func(t *testing.T) {

				storageData, ok := storageData[mapping.Resource]
				if !ok {
					t.Skipf("no test data for %s in etcd.GetEtcdStorageData, skipping", mapping.Resource)
				}

				var subresources []string
				for _, r := range resourceList.APIResources {
					if suffix, ok := strings.CutPrefix(r.Name, resource.Name+"/"); ok {
						subresources = append(subresources, suffix)
					}
				}

				setup := Definition{
					Config:        server.ClientConfig,
					Client:        client,
					DynamicClient: dynamicClient,
					Mapping:       mapping,
					Resource:      resource,
					Namespace:     testNamespace,
					StorageData:   storageData,
					Subresources:  subresources,
				}

				testFunc(t, setup)
			})
		}
	}
}

func CreateMapping(groupVersion string, resource metav1.APIResource) (*meta.RESTMapping, error) {
	gv, err := schema.ParseGroupVersion(groupVersion)
	if err != nil {
		return nil, err
	}
	if len(resource.Group) > 0 || len(resource.Version) > 0 {
		gv = schema.GroupVersion{
			Group:   resource.Group,
			Version: resource.Version,
		}
	}
	gvk := gv.WithKind(resource.Kind)
	gvr := gv.WithResource(resource.Name)

	scope := meta.RESTScopeRoot
	if resource.Namespaced {
		scope = meta.RESTScopeNamespace
	}
	return &meta.RESTMapping{
		Resource:         gvr,
		GroupVersionKind: gvk,
		Scope:            scope,
	}, nil
}

// ResourceString 返回 GroupVersionResource 的 kubectl 风格表示形式
// "resource.version.group"（例如 "deployments.v1.apps"、"pods.v1"）。
func ResourceString(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return gvr.Resource + "." + gvr.Version
	}
	return gvr.Resource + "." + gvr.Version + "." + gvr.Group
}

// matchesException 在 gvr 与 exceptions 中的某项匹配时返回 true。
// 例外项采用 "resource.version.group" 形式。例如，
// "pods.v1" 或 "deployments.v1.apps"。
func matchesException(gvr schema.GroupVersionResource, exceptions sets.Set[string]) bool {
	return exceptions.Has(ResourceString(gvr))
}

// TestObj 是一个通用测试辅助函数，它根据创建 stub 创建 Unstructured 对象，
// 并从单独的 JSON 载荷中显式设置 status。
func TestObj(t *testing.T, stub, status string, gvk schema.GroupVersionKind) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(stub), &obj.Object); err != nil {
		t.Fatal(err)
	}
	var statusObj map[string]interface{}
	if err := json.Unmarshal([]byte(status), &statusObj); err != nil {
		t.Fatal(err)
	}
	if s, ok := statusObj["status"]; ok {
		obj.Object["status"] = s
	}
	obj.SetAPIVersion(gvk.GroupVersion().String())
	obj.SetKind(gvk.Kind)
	return obj
}
