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
	stderrors "errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
)

// TestInternalVersionIsHandlerVersion 验证校验时使用请求经过的 handler 版本
// （而非存储版本），因此非存储版本可通过校验，存储版本反而会因 apiVersion 模式不匹配而失败。
func TestInternalVersionIsHandlerVersion(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	noxuDefinition := fixtures.NewMultipleVersionNoxuCRD(apiextensionsv1.NamespaceScoped)

	assert.Equal(t, "v1beta1", noxuDefinition.Spec.Versions[0].Name)
	assert.Equal(t, "v1beta2", noxuDefinition.Spec.Versions[1].Name)
	assert.True(t, noxuDefinition.Spec.Versions[1].Storage)

	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	ns := "not-the-default"

	noxuNamespacedResourceClientV1beta1 := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, "v1beta1") // 使用非存储版本 v1beta1

	t.Logf("Creating foo")
	noxuInstanceToCreate := fixtures.NewNoxuInstance(ns, "foo")
	_, err = noxuNamespacedResourceClientV1beta1.Create(context.TODO(), noxuInstanceToCreate, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}

	// 通过 update 来更新校验规则，否则 CreateCRDUsingRemovedAPI 中的缓存预热会失败
	t.Logf("Updating CRD to validate apiVersion")
	noxuDefinition, err = UpdateCustomResourceDefinitionWithRetry(apiExtensionClient, noxuDefinition.Name, func(crd *apiextensionsv1.CustomResourceDefinition) {
		for i := range crd.Spec.Versions {
			crd.Spec.Versions[i].Schema = &apiextensionsv1.CustomResourceValidation{
				OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object",
					Properties: map[string]apiextensionsv1.JSONSchemaProps{
						"apiVersion": {
							Pattern: "^mygroup.example.com/v1beta1$", // 这意味着只能通过 v1beta1 handler 版本进行 patch
							Type:    "string",
						},
					},
					Required: []string{"apiVersion"},
				},
			}
		}
	})
	assert.NoError(t, err)

	time.Sleep(time.Second)

	// 通过 handler 版本 v1beta1 进行 patch 应当成功（校验允许该 API 版本）
	{
		t.Logf("patch of handler version v1beta1 (non-storage version) should succeed")
		i := 0
		err = wait.PollUntilContextTimeout(context.Background(), time.Millisecond*100, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
			patch := []byte(fmt.Sprintf(`{"i": %d}`, i))
			i++

			_, patchErr := noxuNamespacedResourceClientV1beta1.Patch(ctx, "foo", types.MergePatchType, patch, metav1.PatchOptions{})
			if patchErr != nil {
				// 规避 "grpc: the client connection is closing" 错误
				// TODO: 修复该 grpc 错误
				var statusErr *errors.StatusError
				if stderrors.As(patchErr, &statusErr) && statusErr.Status().Code == http.StatusInternalServerError {
					return false, nil
				}
				return false, patchErr
			}
			return true, nil
		})
		assert.NoError(t, err)
	}

	// 通过 handler 版本匹配存储版本进行 patch 应当失败（校验不允许该 API 版本）
	{
		t.Logf("patch of handler version v1beta2 (storage version) should fail")
		i := 0
		noxuNamespacedResourceClientV1beta2 := newNamespacedCustomResourceVersionedClient(ns, dynamicClient, noxuDefinition, "v1beta2") // 使用存储版本 v1beta2
		err = wait.PollUntilContextTimeout(context.Background(), time.Millisecond*100, wait.ForeverTestTimeout, true, func(ctx context.Context) (done bool, err error) {
			patch := []byte(fmt.Sprintf(`{"i": %d}`, i))
			i++

			_, patchErr := noxuNamespacedResourceClientV1beta2.Patch(ctx, "foo", types.MergePatchType, patch, metav1.PatchOptions{})
			require.Error(t, patchErr)

			// 规避 "grpc: the client connection is closing" 错误
			// TODO: 修复该 grpc 错误
			var statusErr *errors.StatusError
			if stderrors.As(patchErr, &statusErr) && statusErr.Status().Code == http.StatusInternalServerError {
				return false, nil
			}

			assert.ErrorContains(t, patchErr, "apiVersion")
			return true, nil
		})
		assert.NoError(t, err)
	}
}

// TestVersionedNamespacedScopedCRD 验证命名空间作用域的多版本 CRD 上的基础 CRUD。
func TestVersionedNamespacedScopedCRD(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	noxuDefinition := fixtures.NewMultipleVersionNoxuCRD(apiextensionsv1.NamespaceScoped)
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	ns := "not-the-default"
	testSimpleCRUD(t, ns, noxuDefinition, dynamicClient)
}

// TestVersionedClusterScopedCRD 验证集群作用域的多版本 CRD 上的基础 CRUD。
func TestVersionedClusterScopedCRD(t *testing.T) {
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	noxuDefinition := fixtures.NewMultipleVersionNoxuCRD(apiextensionsv1.ClusterScoped)
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	ns := ""
	testSimpleCRUD(t, ns, noxuDefinition, dynamicClient)
}

// TestStoragedVersionInNamespacedCRDStatus 验证命名空间作用域 CRD 的 status.storedVersions。
func TestStoragedVersionInNamespacedCRDStatus(t *testing.T) {
	noxuDefinition := fixtures.NewMultipleVersionNoxuCRD(apiextensionsv1.NamespaceScoped)
	ns := "not-the-default"
	testStoragedVersionInCRDStatus(t, ns, noxuDefinition)
}

// TestStoragedVersionInClusterScopedCRDStatus 验证集群作用域 CRD 的 status.storedVersions。
func TestStoragedVersionInClusterScopedCRDStatus(t *testing.T) {
	noxuDefinition := fixtures.NewMultipleVersionNoxuCRD(apiextensionsv1.ClusterScoped)
	ns := ""
	testStoragedVersionInCRDStatus(t, ns, noxuDefinition)
}

// testStoragedVersionInCRDStatus 是作用域无关的共享实现：切换存储版本后，
// storedVersions 应包含历史存储版本，且变更立即生效。
func testStoragedVersionInCRDStatus(t *testing.T, ns string, noxuDefinition *apiextensionsv1.CustomResourceDefinition) {
	versionsV1Beta1Storage := []apiextensionsv1.CustomResourceDefinitionVersion{
		{
			Name:    "v1beta1",
			Served:  true,
			Storage: true,
			Schema:  fixtures.AllowAllSchema(),
		},
		{
			Name:    "v1beta2",
			Served:  true,
			Storage: false,
			Schema:  fixtures.AllowAllSchema(),
		},
	}
	versionsV1Beta2Storage := []apiextensionsv1.CustomResourceDefinitionVersion{
		{
			Name:    "v1beta1",
			Served:  true,
			Storage: false,
			Schema:  fixtures.AllowAllSchema(),
		},
		{
			Name:    "v1beta2",
			Served:  true,
			Storage: true,
			Schema:  fixtures.AllowAllSchema(),
		},
	}
	tearDown, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	noxuDefinition.Spec.Versions = versionsV1Beta1Storage
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	// 存储版本列表应初始化为当前存储版本
	crd, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), noxuDefinition.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if e, a := []string{"v1beta1"}, crd.Status.StoredVersions; !reflect.DeepEqual(e, a) {
		t.Errorf("expected %v, got %v", e, a)
	}

	// 切换 CRD 的存储版本后应立即反映到 status
	crd.Spec.Versions = versionsV1Beta2Storage
	_, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	crd, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), noxuDefinition.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if e, a := []string{"v1beta1", "v1beta2"}, crd.Status.StoredVersions; !reflect.DeepEqual(e, a) {
		t.Errorf("expected %v, got %v", e, a)
	}

	err = fixtures.DeleteV1CustomResourceDefinition(crd, apiExtensionClient)
	if err != nil {
		t.Fatal(err)
	}
}
