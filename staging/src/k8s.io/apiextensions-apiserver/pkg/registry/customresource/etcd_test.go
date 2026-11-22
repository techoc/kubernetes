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

// Package customresource_test 通过 REST 存储层验证自定义资源的增删改查、子资源与表格输出行为。
package customresource_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metainternal "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/generic"
	registrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"

	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver"
	"k8s.io/apiextensions-apiserver/pkg/crdserverscheme"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresource"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresource/tableconvertor"
)

// newStorage 启动进程内的 etcd 测试服务，并组装出带 scale/status 子资源与表格列的
// CustomResourceStorage。调用方负责终止返回的 server。
func newStorage(t *testing.T) (customresource.CustomResourceStorage, *etcd3testing.EtcdTestServer) {
	server, etcdStorage := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	// 自定义资源以非结构化方式保存，因此编解码器使用 UnstructuredJSONScheme。
	etcdStorage.Codec = unstructured.UnstructuredJSONScheme
	groupResource := schema.GroupResource{Group: "mygroup.example.com", Resource: "noxus"}
	restOptions := generic.RESTOptions{StorageConfig: etcdStorage.ForResource(groupResource), Decorator: generic.UndecoratedStorage, DeleteCollectionWorkers: 1, ResourcePrefix: "noxus"}

	parameterScheme := runtime.NewScheme()
	// List/Get/Delete 的元数据选项必须注册为非版本化类型，否则请求参数无法解码。
	parameterScheme.AddUnversionedTypes(schema.GroupVersion{Group: "mygroup.example.com", Version: "v1beta1"},
		&metav1.ListOptions{},
		&metav1.GetOptions{},
		&metav1.DeleteOptions{},
	)

	// 元数据选项委托给 parameterScheme，其余对象交给非结构化 typer 推断类型。
	typer := apiserver.UnstructuredObjectTyper{
		Delegate:          parameterScheme,
		UnstructuredTyper: crdserverscheme.NewUnstructuredObjectTyper(),
	}

	kind := schema.GroupVersionKind{Group: "mygroup.example.com", Version: "v1beta1", Kind: "Noxu"}

	labelSelectorPath := ".status.labelSelector"
	scale := &apiextensionsinternal.CustomResourceSubresourceScale{
		SpecReplicasPath:   ".spec.replicas",
		StatusReplicasPath: ".status.replicas",
		LabelSelectorPath:  &labelSelectorPath,
	}

	status := &apiextensionsinternal.CustomResourceSubresourceStatus{}

	// 这些列刻意混入缺失路径、类型不匹配以及需要字符串化的数值/布尔值，
	// 用来覆盖 tableconvertor 的边界处理。
	headers := []apiextensionsv1.CustomResourceColumnDefinition{
		{Name: "Age", Type: "date", JSONPath: ".metadata.creationTimestamp"},
		{Name: "Replicas", Type: "integer", JSONPath: ".spec.replicas"},
		{Name: "Missing", Type: "string", JSONPath: ".spec.missing"},
		{Name: "Invalid", Type: "integer", JSONPath: ".spec.string"},
		{Name: "String", Type: "string", JSONPath: ".spec.string"},
		{Name: "StringFloat64", Type: "string", JSONPath: ".spec.float64"},
		{Name: "StringInt64", Type: "string", JSONPath: ".spec.replicas"},
		{Name: "StringBool", Type: "string", JSONPath: ".spec.bool"},
		{Name: "Float64", Type: "number", JSONPath: ".spec.float64"},
		{Name: "Bool", Type: "boolean", JSONPath: ".spec.bool"},
	}
	// 表头固定有效，构建失败在测试里没有单独处理的必要。
	table, _ := tableconvertor.New(headers)

	storage, err := customresource.NewStorage(
		groupResource,
		groupResource,
		kind,
		schema.GroupVersionKind{Group: "mygroup.example.com", Version: "v1beta1", Kind: "NoxuItemList"},
		customresource.NewStrategy(
			typer,
			true,
			kind,
			nil,
			nil,
			nil,
			status,
			scale,
			nil,
		),
		restOptions,
		[]string{"all"},
		table,
		managedfields.ResourcePathMappings{},
	)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	return storage, server
}

// createCustomResource 是辅助函数，创建 CustomResource 并返回带最新 resourceVersion 的对象。
func createCustomResource(storage *customresource.REST, cr unstructured.Unstructured, t *testing.T) (unstructured.Unstructured, error) {
	// 命名空间放进请求上下文，存储层据此拼出 etcd 键。
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), cr.GetNamespace())
	obj, err := storage.Create(ctx, &cr, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create CustomResource, %v", err)
	}
	newCR := obj.(*unstructured.Unstructured)
	return *newCR, nil
}

// validNewCustomResource 返回一个字段类型齐全、可供各测试复用的最小自定义资源。
func validNewCustomResource() *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "mygroup.example.com/v1beta1",
			"kind":       "Noxu",
			"metadata": map[string]interface{}{
				"namespace": "default",
				"name":      "foo",
				// 固定为 12h30m 之前，TestColumns 才能稳定断言 Age 列输出 "12h"。
				"creationTimestamp": time.Now().Add(-time.Hour*12 - 30*time.Minute).UTC().Format(time.RFC3339),
			},
			"spec": map[string]interface{}{
				"replicas":         int64(7),
				"string":           "string",
				"float64":          float64(3.1415926),
				"bool":             true,
				"stringList":       []interface{}{"foo", "bar"},
				"mixedList":        []interface{}{"foo", int64(42)},
				"nonPrimitiveList": []interface{}{"foo", []interface{}{int64(1), int64(2)}},
			},
		},
	}
}

// validCustomResource 是共享的只读基准对象；需要修改的测试必须先 DeepCopy。
var validCustomResource = *validNewCustomResource()

// TestCreate 验证通用存储的创建路径。
func TestCreate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()
	test := registrytest.New(t, storage.CustomResource.Store)
	cr := validNewCustomResource()
	// 通用注册表测试按无命名空间对象执行。
	cr.SetNamespace("")
	test.TestCreate(
		cr,
	)
}

// TestGet 验证按名称读取自定义资源。
func TestGet(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()
	test := registrytest.New(t, storage.CustomResource.Store)
	test.TestGet(validNewCustomResource())
}

// TestList 验证列表读取。
func TestList(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()
	test := registrytest.New(t, storage.CustomResource.Store)
	test.TestList(validNewCustomResource())
}

// TestDelete 验证删除语义。
func TestDelete(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()
	test := registrytest.New(t, storage.CustomResource.Store)
	test.TestDelete(validNewCustomResource())
}

// TestGenerationNumber 验证 generation 初始化为 1，且只有 spec 变更才会递增。
func TestGenerationNumber(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()
	modifiedRno := *validNewCustomResource()
	modifiedRno.SetGeneration(10)
	ctx := genericapirequest.NewDefaultContext()
	cr, err := createCustomResource(storage.CustomResource, modifiedRno, t)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	etcdCR, err := storage.CustomResource.Get(ctx, cr.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	storedCR, _ := etcdCR.(*unstructured.Unstructured)

	// 创建时服务端会忽略客户端上报的 generation，并初始化为 1。
	if storedCR.GetGeneration() != 1 {
		t.Fatalf("Unexpected generation number %v", storedCR.GetGeneration())
	}

	// spec 变更应递增 generation。
	setSpecReplicas(storedCR, getSpecReplicas(storedCR)+1)
	if _, _, err := storage.CustomResource.Update(ctx, storedCR.GetName(), rest.DefaultUpdatedObjectInfo(storedCR), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	etcdCR, err = storage.CustomResource.Get(ctx, cr.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	storedCR, _ = etcdCR.(*unstructured.Unstructured)
	if storedCR.GetGeneration() != 2 {
		t.Fatalf("Unexpected generation, spec: %v", storedCR.GetGeneration())
	}

	// status 变更不应递增 generation。
	setStatusReplicas(storedCR, getStatusReplicas(storedCR)+1)
	if _, _, err := storage.CustomResource.Update(ctx, storedCR.GetName(), rest.DefaultUpdatedObjectInfo(storedCR), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	etcdCR, err = storage.CustomResource.Get(ctx, cr.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	storedCR, _ = etcdCR.(*unstructured.Unstructured)
	if storedCR.GetGeneration() != 2 {
		t.Fatalf("Unexpected generation, spec: %v", storedCR.GetGeneration())
	}

}

// TestCategories 验证存储对外暴露的类别为 "all"。
func TestCategories(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	expected := []string{"all"}
	actual := storage.CustomResource.Categories()
	ok := reflect.DeepEqual(actual, expected)
	if !ok {
		t.Errorf("categories are not equal. expected = %v actual = %v", expected, actual)
	}
}

// TestColumns 验证表格转换后的列定义与单元值，包括缺失、类型不匹配和字符串化等边界。
func TestColumns(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/foo"
	validCustomResource := validNewCustomResource()
	// 直接写入底层存储，绕过 REST 层以精确控制对象内容。
	if err := storage.CustomResource.Storage.Create(ctx, key, validCustomResource, nil, 0, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gottenList, err := storage.CustomResource.List(ctx, &metainternal.ListOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tbl, err := storage.CustomResource.ConvertToTable(ctx, gottenList, &metav1.TableOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Name 列由列表机制自动补上，其余列来自 CRD 的 additionalPrinterColumns。
	expectedColumns := []struct {
		Name, Type string
	}{
		{"Name", "string"},
		{"Age", "date"},
		{"Replicas", "integer"},
		{"Missing", "string"},
		{"Invalid", "integer"},
		{"String", "string"},
		{"StringFloat64", "string"},
		{"StringInt64", "string"},
		{"StringBool", "string"},
		{"Float64", "number"},
		{"Bool", "boolean"},
	}
	if len(tbl.ColumnDefinitions) != len(expectedColumns) {
		t.Fatalf("got %d columns, expected %d. Got: %+v", len(tbl.ColumnDefinitions), len(expectedColumns), tbl.ColumnDefinitions)
	}
	for i, d := range tbl.ColumnDefinitions {
		if d.Name != expectedColumns[i].Name {
			t.Errorf("got column %d name %q, expected %q", i, d.Name, expectedColumns[i].Name)
		}
		if d.Type != expectedColumns[i].Type {
			t.Errorf("got column %d type %q, expected %q", i, d.Type, expectedColumns[i].Type)
		}
	}

	// 缺失路径得到 nil；string 类型列会把数值/布尔值格式化为字符串。
	expectedRows := [][]interface{}{
		{
			"foo",
			"12h",
			int64(7),
			nil,
			nil,
			"string",
			"3.1415926",
			"7",
			"true",
			float64(3.1415926),
			true,
		},
	}
	for i, r := range tbl.Rows {
		if !reflect.DeepEqual(r.Cells, expectedRows[i]) {
			t.Errorf("got row %d with cells %#v, expected %#v", i, r.Cells, expectedRows[i])
		}
	}
}

// TestStatusUpdate 验证 status 子资源只修改 status，不影响 spec。
func TestStatusUpdate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/foo"
	validCustomResource := validNewCustomResource()
	if err := storage.CustomResource.Storage.Create(ctx, key, validCustomResource, nil, 0, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gottenObj, err := storage.CustomResource.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	update := gottenObj.(*unstructured.Unstructured)
	updateContent := update.Object
	updateContent["status"] = map[string]interface{}{
		"replicas": int64(7),
	}

	if _, _, err := storage.Status.Update(ctx, update.GetName(), rest.DefaultUpdatedObjectInfo(update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	obj, err := storage.CustomResource.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cr, ok := obj.(*unstructured.Unstructured)
	if !ok {
		t.Fatal("unexpected error: custom resource should be of type Unstructured")
	}
	content := cr.UnstructuredContent()

	spec := content["spec"].(map[string]interface{})
	status := content["status"].(map[string]interface{})

	if spec["replicas"].(int64) != 7 {
		t.Errorf("we expected .spec.replicas to not be updated but it was updated to %v", spec["replicas"].(int64))
	}
	if status["replicas"].(int64) != 7 {
		t.Errorf("we expected .status.replicas to be updated to %d but it was %v", 7, status["replicas"].(int64))
	}
}

// TestScaleGet 验证 scale 子资源把 .spec.replicas 映射为 Scale.Spec.Replicas，并保留对象元数据。
func TestScaleGet(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	// Create 会把存储层生成的对象（含 UID/resourceVersion）回填到 cr。
	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	if err := storage.CustomResource.Storage.Create(ctx, key, &validCustomResource, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, validCustomResource, err)
	}

	want := &autoscalingv1.Scale{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Scale",
			APIVersion: "autoscaling/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              cr.GetName(),
			Namespace:         metav1.NamespaceDefault,
			UID:               cr.GetUID(),
			ResourceVersion:   cr.GetResourceVersion(),
			CreationTimestamp: cr.GetCreationTimestamp(),
		},
		Spec: autoscalingv1.ScaleSpec{
			Replicas: int32(7),
		},
	}

	obj, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching scale for %s: %v", name, err)
	}

	got := obj.(*autoscalingv1.Scale)
	if !apiequality.Semantic.DeepEqual(got, want) {
		t.Errorf("unexpected scale: %s", cmp.Diff(got, want))
	}
}

// TestScaleGetWithoutSpecReplicas 验证缺少 .spec.replicas 时 scale 读取会返回明确错误。
func TestScaleGetWithoutSpecReplicas(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	withoutSpecReplicas := validCustomResource.DeepCopy()
	// 移除字段后，scale 无法推导出副本数。
	unstructured.RemoveNestedField(withoutSpecReplicas.Object, "spec", "replicas")
	if err := storage.CustomResource.Storage.Create(ctx, key, withoutSpecReplicas, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, withoutSpecReplicas, err)
	}

	_, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err == nil {
		t.Fatalf("error expected for %s", name)
	}
	if expected := `the spec replicas field ".spec.replicas" does not exist`; !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected error string %q, got: %v", expected, err)
	}
}

// TestScaleUpdate 验证通过 scale 子资源更新 .spec.replicas，以及并发更新时的冲突处理。
func TestScaleUpdate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	if err := storage.CustomResource.Storage.Create(ctx, key, &validCustomResource, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, validCustomResource, err)
	}

	obj, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching scale for %s: %v", name, err)
	}
	scale, ok := obj.(*autoscalingv1.Scale)
	if !ok {
		t.Fatalf("%v is not of the type autoscalingv1.Scale", scale)
	}

	replicas := 12
	update := autoscalingv1.Scale{
		ObjectMeta: scale.ObjectMeta,
		Spec: autoscalingv1.ScaleSpec{
			Replicas: int32(replicas),
		},
	}

	if _, _, err := storage.Scale.Update(ctx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("error updating scale %v: %v", update, err)
	}

	obj, err = storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching scale for %s: %v", name, err)
	}
	scale = obj.(*autoscalingv1.Scale)
	if scale.Spec.Replicas != int32(replicas) {
		t.Errorf("wrong replicas count: expected: %d got: %d", replicas, scale.Spec.Replicas)
	}

	update.ResourceVersion = scale.ResourceVersion
	update.Spec.Replicas = 15

	// 再次更新可能因 resourceVersion 竞争而冲突；冲突属于可接受结果。
	if _, _, err = storage.Scale.Update(ctx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil && !errors.IsConflict(err) {
		t.Fatalf("unexpected error, expecting an update conflict but got %v", err)
	}
}

// TestScaleUpdateWithoutSpecReplicas 验证 spec 中原本没有 replicas 时，scale 更新会创建该字段。
func TestScaleUpdateWithoutSpecReplicas(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	withoutSpecReplicas := validCustomResource.DeepCopy()
	unstructured.RemoveNestedField(withoutSpecReplicas.Object, "spec", "replicas")
	if err := storage.CustomResource.Storage.Create(ctx, key, withoutSpecReplicas, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, withoutSpecReplicas, err)
	}

	replicas := 12
	update := autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			ResourceVersion: cr.GetResourceVersion(),
		},
		Spec: autoscalingv1.ScaleSpec{
			Replicas: int32(replicas),
		},
	}

	if _, _, err := storage.Scale.Update(ctx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("error updating scale %v: %v", update, err)
	}

	obj, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching scale for %s: %v", name, err)
	}
	scale := obj.(*autoscalingv1.Scale)
	if scale.Spec.Replicas != int32(replicas) {
		t.Errorf("wrong replicas count: expected: %d got: %d", replicas, scale.Spec.Replicas)
	}
}

// TestScaleUpdateWithoutResourceVersion 验证省略 resourceVersion 的 scale 更新会被接受。
func TestScaleUpdateWithoutResourceVersion(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	if err := storage.CustomResource.Storage.Create(ctx, key, &validCustomResource, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, validCustomResource, err)
	}

	replicas := int32(8)
	// 不设置 resourceVersion，表示跳过乐观并发校验。
	update := autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: autoscalingv1.ScaleSpec{
			Replicas: replicas,
		},
	}

	if _, _, err := storage.Scale.Update(ctx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("error updating scale %v: %v", update, err)
	}

	obj, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching scale for %s: %v", name, err)
	}
	scale := obj.(*autoscalingv1.Scale)
	if scale.Spec.Replicas != replicas {
		t.Errorf("wrong replicas count: expected: %d got: %d", replicas, scale.Spec.Replicas)
	}
}

// TestScaleUpdateWithoutResourceVersionWithConflicts 验证不带 resourceVersion 的并发标签更新与
// scale 更新都不会互相覆盖。
func TestScaleUpdateWithoutResourceVersionWithConflicts(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	if err := storage.CustomResource.Storage.Create(ctx, key, &validCustomResource, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, validCustomResource, err)
	}

	// fetchObject 读取最新的自定义资源，供并发标签更新使用。
	fetchObject := func(name string) (*unstructured.Unstructured, error) {
		gotObj, err := storage.CustomResource.Get(ctx, name, &metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("error fetching custom resource %s: %v", name, err)
		}
		return gotObj.(*unstructured.Unstructured), nil
	}

	// applyPatch 基于存储中的当前对象打标签，避免用读取时的旧快照覆盖并发写入。
	applyPatch := func(labelName, labelValue string) rest.TransformFunc {
		return func(_ context.Context, _, currentObject runtime.Object) (objToUpdate runtime.Object, patchErr error) {
			o := currentObject.(metav1.Object)
			o.SetLabels(map[string]string{
				labelName: labelValue,
			})
			return currentObject, nil
		}
	}

	errs := make(chan error, 1)
	rounds := 100
	go func() {
		// 持续提交一个更新标签的 patch，并校验标签更新确实生效。
		labelName := "timestamp"
		for i := 0; i < rounds; i++ {
			expectedLabelValue := fmt.Sprint(i)
			update, err := fetchObject(name)
			if err != nil {
				errs <- err
				return
			}
			setNestedField(update, expectedLabelValue, "metadata", "labels", labelName)
			if _, _, err := storage.CustomResource.Update(ctx, name, rest.DefaultUpdatedObjectInfo(nil, applyPatch(labelName, fmt.Sprint(i))), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {

				errs <- fmt.Errorf("error updating custom resource label: %v", err)
				return
			}

			gotObj, err := fetchObject(name)
			if err != nil {
				errs <- err
				return
			}
			gotLabelValue, _, err := unstructured.NestedString(gotObj.Object, "metadata", "labels", labelName)
			if err != nil {
				errs <- fmt.Errorf("error getting label %s of custom resource %s: %v", labelName, name, err)
				return
			}
			if gotLabelValue != expectedLabelValue {
				errs <- fmt.Errorf("wrong label value: expected: %s, got: %s", expectedLabelValue, gotLabelValue)
				return
			}
		}
	}()

	replicas := int32(0)
	update := autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
	// 持续提交不带 resourceVersion 的 scale 更新，副本数单调递增，并校验更新生效。
	for i := 0; i < rounds; i++ {
		select {
		case err := <-errs:
			t.Fatal(err)
		default:
			replicas++
			update.Spec.Replicas = replicas
			if _, _, err := storage.Scale.Update(ctx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
				t.Fatalf("error updating scale %v: %v", update, err)
			}

			obj, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
			if err != nil {
				t.Fatalf("error fetching scale for %s: %v", name, err)
			}
			scale := obj.(*autoscalingv1.Scale)
			if scale.Spec.Replicas != replicas {
				t.Errorf("wrong replicas count: expected: %d got: %d", replicas, scale.Spec.Replicas)
			}
		}
	}
}

// TestScaleUpdateWithResourceVersionWithConflicts 验证携带过期 resourceVersion 的 scale 更新会返回冲突。
func TestScaleUpdateWithResourceVersionWithConflicts(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.CustomResource.Store.DestroyFunc()

	name := "foo"

	var cr unstructured.Unstructured
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), metav1.NamespaceDefault)
	key := "/noxus/" + metav1.NamespaceDefault + "/" + name
	if err := storage.CustomResource.Storage.Create(ctx, key, &validCustomResource, &cr, 0, false); err != nil {
		t.Fatalf("error setting new custom resource (key: %s) %v: %v", key, validCustomResource, err)
	}

	obj, err := storage.Scale.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error fetching scale for %s: %v", name, err)
	}
	scale, ok := obj.(*autoscalingv1.Scale)
	if !ok {
		t.Fatalf("%v is not of the type autoscalingv1.Scale", scale)
	}

	replicas := int32(12)
	update := autoscalingv1.Scale{
		ObjectMeta: scale.ObjectMeta,
		Spec: autoscalingv1.ScaleSpec{
			Replicas: replicas,
		},
	}
	// 伪造一个与存储不一致的 resourceVersion，强制触发冲突。
	update.ResourceVersion = "1"

	_, _, err = storage.Scale.Update(ctx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Fatal("expecting an update conflict error")
	}
	if !errors.IsConflict(err) {
		t.Fatalf("unexpected error, expecting an update conflict but got %v", err)
	}
}

// setSpecReplicas 设置 .spec.replicas。
func setSpecReplicas(u *unstructured.Unstructured, replicas int64) {
	setNestedField(u, replicas, "spec", "replicas")
}

// getSpecReplicas 读取 .spec.replicas；字段缺失或类型错误时按 0 处理。
func getSpecReplicas(u *unstructured.Unstructured) int64 {
	val, found, err := unstructured.NestedInt64(u.Object, "spec", "replicas")
	if !found || err != nil {
		return 0
	}
	return val
}

// setStatusReplicas 设置 .status.replicas。
func setStatusReplicas(u *unstructured.Unstructured, replicas int64) {
	setNestedField(u, replicas, "status", "replicas")
}

// getStatusReplicas 读取 .status.replicas；字段缺失或类型错误时按 0 处理。
func getStatusReplicas(u *unstructured.Unstructured) int64 {
	val, found, err := unstructured.NestedInt64(u.Object, "status", "replicas")
	if !found || err != nil {
		return 0
	}
	return val
}

// setNestedField 确保 Object 非 nil 后写入嵌套字段。
func setNestedField(u *unstructured.Unstructured, value interface{}, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	unstructured.SetNestedField(u.Object, value, fields...)
}
