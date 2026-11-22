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
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
)

// TestYAML 验证 discovery、创建、获取、列表、更新、删除等接口接受并返回 YAML，
// 以及 watch 和 patch 拒绝 YAML 的行为。
func TestYAML(t *testing.T) {
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

	noxuDefinition := fixtures.NewNoxuV1CustomResourceDefinition(apiextensionsv1.ClusterScoped)
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	kind := noxuDefinition.Spec.Names.Kind
	listKind := noxuDefinition.Spec.Names.ListKind
	apiVersion := noxuDefinition.Spec.Group + "/" + noxuDefinition.Spec.Versions[0].Name

	rest := apiExtensionClient.Discovery().RESTClient()

	// 发现接口
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name).
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetAPIVersion() != "v1" || obj.GetKind() != "APIResourceList" {
			t.Fatalf("unexpected discovery kind: %s", string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "groupVersion"); v != apiVersion || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	// 错误响应
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "missingname").
			DoRaw(context.TODO())
		if !errors.IsNotFound(err) {
			t.Fatalf("expected not found, got %v", err)
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetAPIVersion() != "v1" || obj.GetKind() != "Status" {
			t.Fatalf("unexpected discovery kind: %s", string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "reason"); v != "NotFound" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	uid := types.UID("")
	resourceVersion := ""

	// 创建
	{
		yamlBody := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: mytest
values:
  numVal: 1
  boolVal: true
  stringVal: "1"`, apiVersion, kind))

		result, err := rest.Post().
			SetHeader("Accept", "application/yaml").
			SetHeader("Content-Type", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Body(yamlBody).
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "values", "numVal"); v != 1 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedBool(obj.Object, "values", "boolVal"); v != true || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "values", "stringVal"); v != "1" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		uid = obj.GetUID()
		resourceVersion = obj.GetResourceVersion()
	}

	// 获取
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest").
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err, string(result))
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "values", "numVal"); v != 1 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedBool(obj.Object, "values", "boolVal"); v != true || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "values", "stringVal"); v != "1" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	// 列表
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		listObj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if listObj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, listObj.GetAPIVersion())
		}
		if listObj.GetKind() != listKind {
			t.Fatalf("expected %s, got %s", kind, listObj.GetKind())
		}
		items, ok, err := unstructured.NestedSlice(listObj.Object, "items")
		if !ok || err != nil || len(items) != 1 {
			t.Fatalf("expected one item, got %v %v %v", items, ok, err)
		}
		obj := unstructured.Unstructured{Object: items[0].(map[string]interface{})}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "values", "numVal"); v != 1 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedBool(obj.Object, "values", "boolVal"); v != true || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "values", "stringVal"); v != "1" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	// watch 拒绝 YAML（不支持流式输出）
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Param("watch", "true").
			DoRaw(context.TODO())
		if !errors.IsNotAcceptable(err) {
			t.Fatalf("expected not acceptable error, got %v (%s)", err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetAPIVersion() != "v1" || obj.GetKind() != "Status" {
			t.Fatalf("unexpected result: %s", string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "reason"); v != "NotAcceptable" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "code"); v != http.StatusNotAcceptable || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	// 更新
	{
		yamlBody := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: mytest
  uid: %s
  resourceVersion: "%s"
values:
  numVal: 2
  boolVal: false
  stringVal: "2"`, apiVersion, kind, uid, resourceVersion))
		result, err := rest.Put().
			SetHeader("Accept", "application/yaml").
			SetHeader("Content-Type", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest").
			Body(yamlBody).
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "values", "numVal"); v != 2 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedBool(obj.Object, "values", "boolVal"); v != false || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "values", "stringVal"); v != "2" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if obj.GetUID() != uid {
			t.Fatalf("uid changed: %v vs %v", uid, obj.GetUID())
		}
	}

	// patch 拒绝 YAML 请求（只允许 JSON mime 类型）
	{
		yamlBody := []byte(fmt.Sprintf(`
values:
  numVal: 3`))
		result, err := rest.Patch(types.MergePatchType).
			SetHeader("Accept", "application/yaml").
			SetHeader("Content-Type", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest").
			Body(yamlBody).
			DoRaw(context.TODO())
		if !errors.IsUnsupportedMediaType(err) {
			t.Fatalf("Expected bad request, got %v\n%s", err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetAPIVersion() != "v1" || obj.GetKind() != "Status" {
			t.Fatalf("expected %s %s, got %s %s", "v1", "Status", obj.GetAPIVersion(), obj.GetKind())
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "reason"); v != "UnsupportedMediaType" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	// 删除
	{
		result, err := rest.Delete().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest").
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetAPIVersion() != "v1" || obj.GetKind() != "Status" {
			t.Fatalf("unexpected response: %s", string(result))
		}
		if v, ok, err := unstructured.NestedString(obj.Object, "status"); v != "Success" || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}
}

// TestYAMLSubresource 验证 status 和 scale 子资源接口对 YAML 的支持。
func TestYAMLSubresource(t *testing.T) {
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

	noxuDefinition := NewNoxuSubresourcesCRDs(apiextensionsv1.ClusterScoped)[0]
	noxuDefinition, err = fixtures.CreateNewV1CustomResourceDefinition(noxuDefinition, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	kind := noxuDefinition.Spec.Names.Kind
	apiVersion := noxuDefinition.Spec.Group + "/" + noxuDefinition.Spec.Versions[0].Name

	rest := apiExtensionClient.Discovery().RESTClient()

	uid := types.UID("")
	resourceVersion := ""

	// 创建
	{
		yamlBody := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: mytest
spec:
  replicas: 3`, apiVersion, kind))

		result, err := rest.Post().
			SetHeader("Accept", "application/yaml").
			SetHeader("Content-Type", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural).
			Body(yamlBody).
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "spec", "replicas"); v != 3 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		uid = obj.GetUID()
		resourceVersion = obj.GetResourceVersion()
	}

	// 获取 /status
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest", "status").
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err, string(result))
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "spec", "replicas"); v != 3 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}

	// 更新 /status
	{
		yamlBody := []byte(fmt.Sprintf(`
apiVersion: %s
kind: %s
metadata:
  name: mytest
  uid: %s
  resourceVersion: "%s"
spec:
  replicas: 5
status:
  replicas: 3`, apiVersion, kind, uid, resourceVersion))
		result, err := rest.Put().
			SetHeader("Accept", "application/yaml").
			SetHeader("Content-Type", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest", "status").
			Body(yamlBody).
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err, string(result))
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err)
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != apiVersion {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != kind {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "spec", "replicas"); v != 3 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "status", "replicas"); v != 3 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if obj.GetUID() != uid {
			t.Fatalf("uid changed: %v vs %v", uid, obj.GetUID())
		}
	}

	// 获取 /scale
	{
		result, err := rest.Get().
			SetHeader("Accept", "application/yaml").
			AbsPath("/apis", noxuDefinition.Spec.Group, noxuDefinition.Spec.Versions[0].Name, noxuDefinition.Spec.Names.Plural, "mytest", "scale").
			DoRaw(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		obj, err := decodeYAML(result)
		if err != nil {
			t.Fatal(err, string(result))
		}
		if obj.GetName() != "mytest" {
			t.Fatalf("expected mytest, got %s", obj.GetName())
		}
		if obj.GetAPIVersion() != "autoscaling/v1" {
			t.Fatalf("expected %s, got %s", apiVersion, obj.GetAPIVersion())
		}
		if obj.GetKind() != "Scale" {
			t.Fatalf("expected %s, got %s", kind, obj.GetKind())
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "spec", "replicas"); v != 3 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
		if v, ok, err := unstructured.NestedFloat64(obj.Object, "status", "replicas"); v != 3 || !ok || err != nil {
			t.Fatal(v, ok, err, string(result))
		}
	}
}

// decodeYAML 解析响应体，并断言它不是 JSON 而是合法 YAML。
func decodeYAML(data []byte) (*unstructured.Unstructured, error) {
	retval := &unstructured.Unstructured{Object: map[string]interface{}{}}
	// 确认这不是 JSON
	if json.Unmarshal(data, &retval.Object) == nil {
		return nil, fmt.Errorf("data is JSON, not YAML: %s", string(data))
	}
	// 确认它是 YAML
	retval.Object = map[string]interface{}{}
	if err := yaml.Unmarshal(data, &retval.Object); err != nil {
		return nil, fmt.Errorf("error decoding YAML: %v\noriginal YAML: %s", err, string(data))
	}
	return retval, nil
}
