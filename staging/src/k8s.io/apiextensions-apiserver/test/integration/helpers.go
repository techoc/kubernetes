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

// Package integration 包含 apiextensions-apiserver 的集成测试，
// 覆盖 CRD 与自定义资源的 CRUD、子资源、校验与剪枝等行为。
package integration

import (
	"context"
	"fmt"
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	_ "k8s.io/component-base/logs/testinit" // 注册日志相关命令行参数
)

var swaggerMetadataDescriptions = metav1.ObjectMeta{}.SwaggerDoc()

// instantiateCustomResource 按 CRD 的第一个版本创建 CR，并校验返回对象的基本元数据。
func instantiateCustomResource(t *testing.T, instanceToCreate *unstructured.Unstructured, client dynamic.ResourceInterface, definition *apiextensionsv1.CustomResourceDefinition) (*unstructured.Unstructured, error) {
	return instantiateVersionedCustomResource(t, instanceToCreate, client, definition, definition.Spec.Versions[0].Name)
}

// instantiateVersionedCustomResource 用指定版本创建 CR，并断言 server 返回了
// UID、apiVersion 与 kind——这些由存储在服务端补齐，便于调用方直接依赖。
func instantiateVersionedCustomResource(t *testing.T, instanceToCreate *unstructured.Unstructured, client dynamic.ResourceInterface, definition *apiextensionsv1.CustomResourceDefinition, version string) (*unstructured.Unstructured, error) {
	createdInstance, err := client.Create(context.TODO(), instanceToCreate, metav1.CreateOptions{})
	if err != nil {
		t.Logf("%#v", createdInstance)
		return nil, err
	}
	createdObjectMeta, err := meta.Accessor(createdInstance)
	if err != nil {
		t.Fatal(err)
	}
	// 每个对象都应有 UID
	if len(createdObjectMeta.GetUID()) == 0 {
		t.Errorf("missing uuid: %#v", createdInstance)
	}
	createdTypeMeta, err := meta.TypeAccessor(createdInstance)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := definition.Spec.Group+"/"+version, createdTypeMeta.GetAPIVersion(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	if e, a := definition.Spec.Names.Kind, createdTypeMeta.GetKind(); e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
	return createdInstance, nil
}

// newNamespacedCustomResourceVersionedClient 为 CRD 的指定版本构造命名空间限定的动态客户端。
// 集群级资源忽略 ns，返回集群级客户端。
func newNamespacedCustomResourceVersionedClient(ns string, client dynamic.Interface, crd *apiextensionsv1.CustomResourceDefinition, version string) dynamic.ResourceInterface {
	gvr := schema.GroupVersionResource{Group: crd.Spec.Group, Version: version, Resource: crd.Spec.Names.Plural}

	if crd.Spec.Scope != apiextensionsv1.ClusterScoped {
		return client.Resource(gvr).Namespace(ns)
	}
	return client.Resource(gvr)
}

// newNamespacedCustomResourceClient 用 CRD 的第一个版本构造命名空间限定的动态客户端。
func newNamespacedCustomResourceClient(ns string, client dynamic.Interface, crd *apiextensionsv1.CustomResourceDefinition) dynamic.ResourceInterface {
	return newNamespacedCustomResourceVersionedClient(ns, client, crd, crd.Spec.Versions[0].Name)
}

// UpdateCustomResourceDefinitionWithRetry 更新 CRD，遇到版本冲突时最多重试 5 次。
// 冲突说明对象已被其他 writer 修改，重新读取后再应用变更。
func UpdateCustomResourceDefinitionWithRetry(client clientset.Interface, name string, update func(*apiextensionsv1.CustomResourceDefinition)) (*apiextensionsv1.CustomResourceDefinition, error) {
	for i := 0; i < 5; i++ {
		crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get CustomResourceDefinition %q: %v", name, err)
		}
		update(crd)
		crd, err = client.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
		if err == nil {
			return crd, nil
		}
		if !errors.IsConflict(err) {
			return nil, fmt.Errorf("failed to update CustomResourceDefinition %q: %v", name, err)
		}
	}
	return nil, fmt.Errorf("too many retries after conflicts updating CustomResourceDefinition %q", name)
}

// UpdateV1CustomResourceDefinitionWithRetry 更新 CRD，遇到版本冲突时最多重试 5 次。
func UpdateV1CustomResourceDefinitionWithRetry(client clientset.Interface, name string, update func(*apiextensionsv1.CustomResourceDefinition)) (*apiextensionsv1.CustomResourceDefinition, error) {
	for i := 0; i < 5; i++ {
		crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get CustomResourceDefinition %q: %v", name, err)
		}
		update(crd)
		crd, err = client.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), crd, metav1.UpdateOptions{})
		if err == nil {
			return crd, nil
		}
		if !errors.IsConflict(err) {
			return nil, fmt.Errorf("failed to update CustomResourceDefinition %q: %v", name, err)
		}
	}
	return nil, fmt.Errorf("too many retries after conflicts updating CustomResourceDefinition %q", name)
}

// getSchemaForVersion 返回 CRD 中指定版本的校验 schema。
func getSchemaForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) (*apiextensionsv1.CustomResourceValidation, error) {
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return v.Schema, nil
		}
	}
	return nil, fmt.Errorf("version %s not found in CustomResourceDefinition: %v", version, crd.Name)
}

// getSubresourcesForVersion 返回 CRD 中指定版本的子资源配置。
func getSubresourcesForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) (*apiextensionsv1.CustomResourceSubresources, error) {
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return v.Subresources, nil
		}
	}
	return nil, fmt.Errorf("version %s not found in CustomResourceDefinition: %v", version, crd.Name)
}

// getColumnsForVersion 返回 CRD 中指定版本的打印列；未配置时返回逻辑默认列。
// 注意：逻辑默认列不指向原 CRD 对象，修改它不会写回 CRD；
// 需要变更时请遍历原 CRD 对象。
func getColumnsForVersion(crd *apiextensionsv1.CustomResourceDefinition, version string) ([]apiextensionsv1.CustomResourceColumnDefinition, error) {
	for _, v := range crd.Spec.Versions {
		if version == v.Name {
			return serveDefaultColumnsIfEmpty(v.AdditionalPrinterColumns), nil
		}
	}
	return nil, fmt.Errorf("version %s not found in CustomResourceDefinition: %v", version, crd.Name)
}

// serveDefaultColumnsIfEmpty 在未配置列时应用逻辑默认值（Age 列）。
// 注意：返回的逻辑默认列不指向原 CRD 对象，修改它不会写回 CRD；
// 需要变更时请遍历原 CRD 对象。
func serveDefaultColumnsIfEmpty(columns []apiextensionsv1.CustomResourceColumnDefinition) []apiextensionsv1.CustomResourceColumnDefinition {
	if len(columns) > 0 {
		return columns
	}
	return []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Age", Type: "date", Description: swaggerMetadataDescriptions["creationTimestamp"], JSONPath: ".metadata.creationTimestamp"},
	}
}
