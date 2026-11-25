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

package crd

import (
	"context"
	"fmt"
	"time"

	"k8s.io/utils/ptr"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	crdclientset "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/kubernetes/test/e2e/framework"
)

// CleanCrdFn 声明移除 CRD 所需的清理函数
type CleanCrdFn func(ctx context.Context) error

// TestCrd 包含使用 CRD 进行测试所需的全部组件
type TestCrd struct {
	APIExtensionClient *crdclientset.Clientset
	Crd                *apiextensionsv1.CustomResourceDefinition
	DynamicClients     map[string]dynamic.ResourceInterface
	CleanUp            CleanCrdFn
}

// Option 是 CRD 对象的修改器，用于定制 CreateMultiVersionTestCRD 和 CreateTestCRD。
type Option func(crd *apiextensionsv1.CustomResourceDefinition)

// CreateMultiVersionTestCRD 专门为调用它的测试创建一个新的 CRD。
func CreateMultiVersionTestCRD(f *framework.Framework, group string, opts ...Option) (*TestCrd, error) {
	testcrd := &TestCrd{}

	// 创建一个供各种测试使用的自定义资源定义。
	config, err := framework.LoadConfig()
	if err != nil {
		framework.Failf("failed to load config: %v", err)
		return nil, err
	}
	apiExtensionClient, err := crdclientset.NewForConfig(config)
	if err != nil {
		framework.Failf("failed to initialize apiExtensionClient: %v", err)
		return nil, err
	}
	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		framework.Failf("failed to initialize dynamic client: %v", err)
		return nil, err
	}

	crd := genRandomCRD(f, group, opts...)
	// 稳健地执行 CRD 创建调用。
	var got *apiextensionsv1.CustomResourceDefinition
	if err := wait.PollUntilContextTimeout(context.TODO(), f.Timeouts.Poll, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		// 创建 CRD 并等待资源被识别且可用。
		got, err = fixtures.CreateNewV1CustomResourceDefinitionWatchUnsafe(crd, apiExtensionClient)
		if err != nil {
			if apierrors.IsAlreadyExists(err) {
				// 发生冲突时重新生成
				framework.Logf("CustomResourceDefinition name %q was already taken, generate a new name and retry", crd.Name)
				crd = genRandomCRD(f, group, opts...)
			} else {
				framework.Logf("Unexpected error while creating CustomResourceDefinition: %v", err)
			}
			return false, nil
		}
		return true, nil
	}); err != nil {
		return nil, err
	}

	resourceClients := map[string]dynamic.ResourceInterface{}
	for _, v := range got.Spec.Versions {
		if v.Served {
			gvr := schema.GroupVersionResource{Group: got.Spec.Group, Version: v.Name, Resource: got.Spec.Names.Plural}
			resourceClients[v.Name] = dynamicClient.Resource(gvr).Namespace(f.Namespace.Name)
		}
	}

	testcrd.APIExtensionClient = apiExtensionClient
	testcrd.Crd = got
	testcrd.DynamicClients = resourceClients
	testcrd.CleanUp = func(ctx context.Context) error {
		err := fixtures.DeleteV1CustomResourceDefinition(got, apiExtensionClient)
		if err != nil {
			framework.Failf("failed to delete CustomResourceDefinition(%s): %v", got.Name, err)
		}
		return err
	}
	return testcrd, nil
}

// genRandomCRD 根据 framework 的基础名称和随机后缀生成随机的 CRD 名称和 kind。
// 它还会将 group 设为提供的值，并将 scope 设为 NamespaceScoped。如果 opts 中没有提供版本，
// 则会创建一个默认版本 "v1"，其 schema 允许所有字段。
func genRandomCRD(f *framework.Framework, group string, opts ...Option) *apiextensionsv1.CustomResourceDefinition {
	suffix := framework.RandomSuffix()
	name := fmt.Sprintf("e2e-test-%s-%s-crd", f.UniqueName, suffix)
	kind := fmt.Sprintf("e2e-test-%s-%s-crd", f.UniqueName, suffix)

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: name + "s." + group},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural:   name + "s",
				Singular: name,
				Kind:     kind,
				ListKind: kind + "List",
			},
			Scope: apiextensionsv1.NamespaceScoped,
		},
	}
	for _, opt := range opts {
		opt(crd)
	}
	if len(crd.Spec.Versions) == 0 {
		crd.Spec.Versions = []apiextensionsv1.CustomResourceDefinitionVersion{{
			Served:  true,
			Storage: true,
			Name:    "v1",
			Schema:  fixtures.AllowAllSchema(),
		}}
	}
	return crd
}

// CreateTestCRD 专门为调用它的测试创建一个新的 CRD。
func CreateTestCRD(f *framework.Framework, opts ...Option) (*TestCrd, error) {
	group := fmt.Sprintf("%s.example.com", f.BaseName)
	return CreateMultiVersionTestCRD(f, group, append([]Option{func(crd *apiextensionsv1.CustomResourceDefinition) {
		crd.Spec.Versions = []apiextensionsv1.CustomResourceDefinitionVersion{
			{
				Name:    "v1",
				Served:  true,
				Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						XPreserveUnknownFields: ptr.To(true),
						Type:                   "object",
					},
				},
			},
		}
	}}, opts...)...)
}
