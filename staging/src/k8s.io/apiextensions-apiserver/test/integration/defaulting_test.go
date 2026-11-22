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

package integration

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	serveroptions "k8s.io/apiextensions-apiserver/pkg/cmd/server/options"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
	"k8s.io/apiextensions-apiserver/test/integration/storage"
)

// defaultingFixture 是 defaulting 测试共用的双版本 CRD 定义。
var defaultingFixture = &apiextensionsv1.CustomResourceDefinition{
	ObjectMeta: metav1.ObjectMeta{Name: "foos.tests.example.com"},
	Spec: apiextensionsv1.CustomResourceDefinitionSpec{
		Group: "tests.example.com",
		Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
			{
				Name:    "v1beta1",
				Storage: false,
				Served:  true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.replicas",
						StatusReplicasPath: ".status.replicas",
					},
				},
			},
			{
				Name:    "v1beta2",
				Storage: true,
				Served:  false,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
					Scale: &apiextensionsv1.CustomResourceSubresourceScale{
						SpecReplicasPath:   ".spec.replicas",
						StatusReplicasPath: ".status.replicas",
					},
				},
			},
		},
		Names: apiextensionsv1.CustomResourceDefinitionNames{
			Plural:   "foos",
			Singular: "foo",
			Kind:     "Foo",
			ListKind: "FooList",
		},
		Scope:                 apiextensionsv1.ClusterScoped,
		PreserveUnknownFields: false,
	},
}

const defaultingFooV1beta1Schema = `
type: object
properties:
  spec:
    type: object
    properties:
      a:
        type: string
        default: "A"
      b:
        type: string
        default: "B"
      c:
        type: string
      v1beta1:
        type: string
        default: "v1beta1"
      v1beta2:
        type: string
      replicas:
        default: 1
        format: int32
        minimum: 0
        type: integer
  status:
    type: object
    properties:
      a:
        type: string
        default: "A"
      b:
        type: string
        default: "B"
      c:
        type: string
      v1beta1:
        type: string
        default: "v1beta1"
      v1beta2:
        type: string
      replicas:
        default: 0
        format: int32
        minimum: 0
        type: integer
`

const defaultingFooV1beta2Schema = `
type: object
properties:
  spec:
    type: object
    properties:
      a:
        type: string
        default: "A"
      b:
        type: string
        default: "B"
      c:
        type: string
      v1beta1:
        type: string
      v1beta2:
        type: string
        default: "v1beta2"
      replicas:
        default: 1
        format: int32
        minimum: 0
        type: integer
  status:
    type: object
    properties:
      a:
        type: string
        default: "A"
      b:
        type: string
        default: "B"
      c:
        type: string
      v1beta1:
        type: string
      v1beta2:
        type: string
        default: "v1beta2"
      replicas:
        default: 0
        format: int32
        minimum: 0
        type: integer
`

const defaultingFooInstance = `
kind: Foo
apiVersion: tests.example.com/v1beta1
metadata:
  name: foo
`

// TestCustomResourceDefaultingWithWatchCache 在启用 watch cache 时验证默认值填充。
func TestCustomResourceDefaultingWithWatchCache(t *testing.T) {
	testDefaulting(t, true)
}

// TestCustomResourceDefaultingWithoutWatchCache 在关闭 watch cache 时验证默认值填充。
func TestCustomResourceDefaultingWithoutWatchCache(t *testing.T) {
	testDefaulting(t, false)
}

// testDefaulting 验证不同版本 schema 的默认值在创建、读取、缓存读取与 watch 中的表现。
func testDefaulting(t *testing.T, watchCache bool) {
	tearDownFn, apiExtensionClient, dynamicClient, err := fixtures.StartDefaultServerWithClients(t, fmt.Sprintf("--watch-cache=%v", watchCache))
	if err != nil {
		t.Fatal(err)
	}
	defer tearDownFn()

	crd := defaultingFixture.DeepCopy()
	crd.Spec.Versions[0].Schema = &apiextensionsv1.CustomResourceValidation{}
	if err := yaml.Unmarshal([]byte(defaultingFooV1beta1Schema), &crd.Spec.Versions[0].Schema.OpenAPIV3Schema); err != nil {
		t.Fatal(err)
	}
	crd.Spec.Versions[1].Schema = &apiextensionsv1.CustomResourceValidation{}
	if err := yaml.Unmarshal([]byte(defaultingFooV1beta2Schema), &crd.Spec.Versions[1].Schema.OpenAPIV3Schema); err != nil {
		t.Fatal(err)
	}

	crd, err = fixtures.CreateNewV1CustomResourceDefinition(crd, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	// mustExist 断言给定的嵌套路径都存在。
	mustExist := func(obj map[string]interface{}, pths [][]string) {
		t.Helper()
		for _, pth := range pths {
			if _, found, _ := unstructured.NestedFieldNoCopy(obj, pth...); !found {
				t.Errorf("Expected '%s' field was missing", strings.Join(pth, "."))
			}
		}
	}
	// mustNotExist 断言给定的嵌套路径都不存在。
	mustNotExist := func(obj map[string]interface{}, pths [][]string) {
		t.Helper()
		for _, pth := range pths {
			if fld, found, _ := unstructured.NestedFieldNoCopy(obj, pth...); found {
				t.Errorf("Expected '%s' field to not exist, but it does: %v", strings.Join(pth, "."), fld)
			}
		}
	}
	// updateCRD 以乐观并发方式更新 CRD；冲突时重新读取并重试。
	updateCRD := func(update func(*apiextensionsv1.CustomResourceDefinition)) {
		t.Helper()
		var err error
		for retry := 0; retry < 10; retry++ {
			var obj *apiextensionsv1.CustomResourceDefinition
			obj, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(context.TODO(), crd.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			update(obj)
			obj, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Update(context.TODO(), obj, metav1.UpdateOptions{})
			if err != nil && apierrors.IsConflict(err) {
				continue
			} else if err != nil {
				t.Fatal(err)
			}
			crd = obj
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// addDefault 同时为指定版本的 spec 与 status 根下的字段添加默认值。
	addDefault := func(version string, key string, value interface{}) {
		t.Helper()
		updateCRD(func(obj *apiextensionsv1.CustomResourceDefinition) {
			for _, root := range []string{"spec", "status"} {
				for i := range obj.Spec.Versions {
					if obj.Spec.Versions[i].Name != version {
						continue
					}
					obj.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties[root].Properties[key] = apiextensionsv1.JSONSchemaProps{
						Type:    "string",
						Default: jsonPtr(value),
					}
				}
			}
		})
	}
	// removeDefault 同时移除指定版本 spec 与 status 根下字段的默认值。
	removeDefault := func(version string, key string) {
		t.Helper()
		updateCRD(func(obj *apiextensionsv1.CustomResourceDefinition) {
			for _, root := range []string{"spec", "status"} {
				for i := range obj.Spec.Versions {
					if obj.Spec.Versions[i].Name != version {
						continue
					}
					props := obj.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties[root].Properties[key]
					props.Default = nil
					obj.Spec.Versions[i].Schema.OpenAPIV3Schema.Properties[root].Properties[key] = props
				}
			}
		})
	}

	t.Logf("Creating CR and expecting defaulted fields in spec, but status does not exist at all")
	fooClient := dynamicClient.Resource(schema.GroupVersionResource{Group: crd.Spec.Group, Version: crd.Spec.Versions[0].Name, Resource: crd.Spec.Names.Plural})
	foo := &unstructured.Unstructured{}
	if err := yaml.Unmarshal([]byte(defaultingFooInstance), &foo.Object); err != nil {
		t.Fatal(err)
	}
	unstructured.SetNestedField(foo.Object, "a", "spec", "a")
	unstructured.SetNestedField(foo.Object, "b", "status", "b")
	foo, err = fooClient.Create(context.TODO(), foo, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unable to create CR: %v", err)
	}
	initialResourceVersion := foo.GetResourceVersion()
	t.Logf("CR created: %#v", foo.UnstructuredContent())
	// spec.a 与 spec.b 在两个版本中都会填默认值；
	// spec.v1beta1 在读取入站请求时填充；
	// spec.v1beta2 在读取存储返回时填充。
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"spec", "v1beta1"}, {"spec", "v1beta2"}, {"spec", "replicas"}})
	mustNotExist(foo.Object, [][]string{{"status"}})

	t.Logf("Updating status and expecting 'a' and 'b' to show up.")
	unstructured.SetNestedField(foo.Object, map[string]interface{}{}, "status")
	if foo, err = fooClient.UpdateStatus(context.TODO(), foo, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"status", "a"}, {"status", "b"}, {"status", "replicas"}})

	t.Logf("Add 'c' default to the storage version and wait until GET sees it in both status and spec")
	addDefault("v1beta2", "c", "C")

	t.Logf("wait until GET sees 'c' in both status and spec")
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		obj, err := fooClient.Get(ctx, foo.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		if _, found, _ := unstructured.NestedString(obj.Object, "spec", "c"); !found {
			t.Log("will retry, did not find spec.c in the object")
			return false, nil
		}
		foo = obj
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"spec", "c"}, {"status", "a"}, {"status", "b"}, {"status", "c"}})

	t.Logf("wait until GET sees 'c' in both status and spec of cached get")
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		obj, err := fooClient.Get(context.TODO(), foo.GetName(), metav1.GetOptions{ResourceVersion: "0"})
		if err != nil {
			return false, err
		}
		if _, found, _ := unstructured.NestedString(obj.Object, "spec", "c"); !found {
			t.Logf("will retry, did not find spec.c in the cached object")
			return false, nil
		}
		foo = obj
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"spec", "c"}, {"status", "a"}, {"status", "b"}, {"status", "c"}})

	t.Logf("verify LIST sees 'c' in both status and spec")
	foos, err := fooClient.List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, foo := range foos.Items {
		mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"spec", "c"}, {"status", "a"}, {"status", "b"}, {"status", "c"}})
	}

	t.Logf("verify LIST from cache sees 'c' in both status and spec")
	foos, err = fooClient.List(context.TODO(), metav1.ListOptions{ResourceVersion: "0"})
	if err != nil {
		t.Fatal(err)
	}
	for _, foo := range foos.Items {
		mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"spec", "c"}, {"status", "a"}, {"status", "b"}, {"status", "c"}})
	}

	// 使用 watch cache 时跳过该测试：修改 CRD 会重置 watch cache 的最小可用 resourceVersion。
	// watch cache 由 list 与 watch 填充，二者本测试都已覆盖；
	// 其内容可通过 rv=0 的 list 读取，本测试也已覆盖。
	if !watchCache {
		t.Logf("verify WATCH sees 'c' in both status and spec")
		w, err := fooClient.Watch(context.TODO(), metav1.ListOptions{ResourceVersion: initialResourceVersion})
		if err != nil {
			t.Fatal(err)
		}
		defer w.Stop()
		select {
		case event := <-w.ResultChan():
			// 测试执行期间起始 RV 可能已被压缩，因此容忍过期错误。
			// 参见：https://github.com/kubernetes/kubernetes/issues/125760
			if event.Type == watch.Error {
				if !apierrors.IsResourceExpired(apierrors.FromObject(event.Object)) {
					t.Fatalf("unexpected watch event: %v, %#v", event.Type, event.Object)
				}
				t.Logf("skipping the WATCH at RV = %s, because the revision has been compacetd, err: %#v", initialResourceVersion, event.Object)
				break
			}
			if event.Type != watch.Modified {
				t.Fatalf("unexpected watch event: %v, %#v", event.Type, event.Object)
			}
			if e, a := "v1beta1", event.Object.GetObjectKind().GroupVersionKind().Version; e != a {
				t.Errorf("watch event for v1beta1 API returned %v", a)
			}
			mustExist(
				event.Object.(*unstructured.Unstructured).Object,
				[][]string{{"spec", "a"}, {"spec", "b"}, {"spec", "c"}, {"status", "a"}, {"status", "b"}, {"status", "c"}},
			)
		case <-time.After(wait.ForeverTestTimeout):
			t.Fatal("timed out without getting watch event")
		}
	}

	t.Logf("Add 'c' default to the REST version, remove it from the storage version, and wait until GET no longer sees it in both status and spec")
	addDefault("v1beta1", "c", "C")
	removeDefault("v1beta2", "c")
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		obj, err := fooClient.Get(ctx, foo.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		_, found, _ := unstructured.NestedString(obj.Object, "spec", "c")
		foo = obj
		return !found, nil
	}); err != nil {
		t.Fatal(err)
	}
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"status", "a"}, {"status", "b"}})
	mustNotExist(foo.Object, [][]string{{"spec", "c"}, {"status", "c"}})

	t.Logf("Updating status, expecting 'c' to be set in status only")
	if foo, err = fooClient.UpdateStatus(context.TODO(), foo, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"status", "a"}, {"status", "b"}, {"status", "c"}})
	mustNotExist(foo.Object, [][]string{{"spec", "c"}})

	t.Logf("Removing 'a', 'b' and `c` properties from the REST version. Expecting that 'c' goes away in spec, but not in status. 'a' and 'b' were presisted.")
	removeDefault("v1beta1", "a")
	removeDefault("v1beta1", "b")
	removeDefault("v1beta1", "c")
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		obj, err := fooClient.Get(ctx, foo.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		_, found, _ := unstructured.NestedString(obj.Object, "spec", "c")
		foo = obj
		return !found, nil
	}); err != nil {
		t.Fatal(err)
	}
	mustExist(foo.Object, [][]string{{"spec", "a"}, {"spec", "b"}, {"status", "a"}, {"status", "b"}, {"status", "c"}})
	mustNotExist(foo.Object, [][]string{{"spec", "c"}})
}

// metaDefaultingFixture 是验证内嵌资源 metadata 默认值行为的 CRD 定义。
var metaDefaultingFixture = &apiextensionsv1.CustomResourceDefinition{
	ObjectMeta: metav1.ObjectMeta{Name: "foos.tests.example.com"},
	Spec: apiextensionsv1.CustomResourceDefinitionSpec{
		Group: "tests.example.com",
		Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
			{
				Name:    "v1beta1",
				Storage: true,
				Served:  true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{
					Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
				},
			},
		},
		Names: apiextensionsv1.CustomResourceDefinitionNames{
			Plural:   "foos",
			Singular: "foo",
			Kind:     "Foo",
			ListKind: "FooList",
		},
		Scope:                 apiextensionsv1.ClusterScoped,
		PreserveUnknownFields: false,
	},
}

const metaDefaultingFooV1beta1Schema = `
type: object
properties:
  fields:
    type: object
    x-kubernetes-embedded-resource: true
    properties:
      apiVersion:
        type: string
        default: foos/v1
      kind:
        type: string
        default: Foo
      metadata:
        type: object
        properties:
          name:
            type: string
            default: Bar
          unknown:
            type: string
            default: unknown
  fullMetadata:
    type: object
    x-kubernetes-embedded-resource: true
    properties:
      apiVersion:
        type: string
        default: foos/v1
      kind:
        type: string
        default: Foo
      metadata:
        type: object
        default:
          name: Bar
          unknown: unknown
  fullObject:
    type: object
    x-kubernetes-embedded-resource: true
    properties:
      foo:
        type: string
    default:
      apiVersion: foos/v1
      kind: Foo
      metadata:
        name: Bar
        unknown: unknown
  spanning:
    type: object
    properties:
      embedded:
        type: object
        properties:
          foo:
            type: string
        x-kubernetes-embedded-resource: true
    default:
      embedded:
        apiVersion: foos/v1
        kind: Foo
        metadata:
          name: Bar
          unknown: unknown
  preserve-fields:
    type: object
    x-kubernetes-embedded-resource: true
    x-kubernetes-preserve-unknown-fields: true
    properties:
      apiVersion:
        type: string
        default: foos/v1
      kind:
        type: string
        default: Foo
      metadata:
        type: object
        properties:
          name:
            type: string
            default: Bar
          unknown:
            type: string
            default: unknown
  preserve-fullMetadata:
    type: object
    x-kubernetes-embedded-resource: true
    x-kubernetes-preserve-unknown-fields: true
    properties:
      apiVersion:
        type: string
        default: foos/v1
      kind:
        type: string
        default: Foo
      metadata:
        type: object
        default:
          name: Bar
          unknown: unknown
  preserve-fullObject:
    type: object
    x-kubernetes-embedded-resource: true
    x-kubernetes-preserve-unknown-fields: true
    default:
      apiVersion: foos/v1
      kind: Foo
      metadata:
        name: Bar
        unknown: unknown
  preserve-spanning:
    type: object
    properties:
      embedded:
        type: object
        x-kubernetes-embedded-resource: true
        x-kubernetes-preserve-unknown-fields: true
    default:
      embedded:
        apiVersion: foos/v1
        kind: Foo
        metadata:
          name: Bar
          unknown: unknown
`

const metaDefaultingFooInstance = `
kind: Foo
apiVersion: tests.example.com/v1beta1
metadata:
  name: foo
`

// TestCustomResourceDefaultingOfMetaFields 验证内嵌资源的 apiVersion/kind/metadata
// 默认值填充，以及未知 metadata 字段的剪除。
func TestCustomResourceDefaultingOfMetaFields(t *testing.T) {
	tearDown, config, options, err := fixtures.StartDefaultServer(t)
	if err != nil {
		t.Fatal(err)
	}
	apiExtensionClient, err := clientset.NewForConfig(config)
	if err != nil {
		tearDown()
		t.Fatal(err)
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		tearDown()
		t.Fatal(err)
	}
	defer tearDown()

	crd := metaDefaultingFixture.DeepCopy()
	crd.Spec.Versions[0].Schema = &apiextensionsv1.CustomResourceValidation{}
	if err := yaml.Unmarshal([]byte(metaDefaultingFooV1beta1Schema), &crd.Spec.Versions[0].Schema.OpenAPIV3Schema); err != nil {
		t.Fatal(err)
	}
	crd, err = fixtures.CreateNewV1CustomResourceDefinition(crd, apiExtensionClient, dynamicClient)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Creating CR and expecting defaulted, embedded objects, with the unknown ObjectMeta fields pruned")
	fooClient := dynamicClient.Resource(schema.GroupVersionResource{Group: crd.Spec.Group, Version: crd.Spec.Versions[0].Name, Resource: crd.Spec.Names.Plural})

	tests := []struct {
		path  []string
		value interface{}
	}{
		{[]string{"fields"}, map[string]interface{}{"metadata": map[string]interface{}{}}},
		{[]string{"fullMetadata"}, map[string]interface{}{}},
		{[]string{"fullObject"}, nil},
		{[]string{"spanning", "embedded"}, nil},
		{[]string{"preserve-fields"}, map[string]interface{}{"metadata": map[string]interface{}{}}},
		{[]string{"preserve-fullMetadata"}, map[string]interface{}{}},
		{[]string{"preserve-fullObject"}, nil},
		{[]string{"preserve-spanning", "embedded"}, nil},
	}

	returnedFoo := &unstructured.Unstructured{}
	if err := yaml.Unmarshal([]byte(metaDefaultingFooInstance), &returnedFoo.Object); err != nil {
		t.Fatal(err)
	}
	for _, tst := range tests {
		if tst.value != nil {
			if err := unstructured.SetNestedField(returnedFoo.Object, tst.value, tst.path...); err != nil {
				t.Fatal(err)
			}
		}
	}
	returnedFoo, err = fooClient.Create(context.TODO(), returnedFoo, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unable to create CR: %v", err)
	}
	t.Logf("CR created: %#v", returnedFoo.UnstructuredContent())

	// 读取持久化后的对象，确认落盘内容同样经过默认值填充与剪枝
	RESTOptionsGetter := serveroptions.NewCRDRESTOptionsGetter(*options.RecommendedOptions.Etcd, nil, nil)
	restOptions, err := RESTOptionsGetter.GetRESTOptions(schema.GroupResource{Group: crd.Spec.Group, Resource: crd.Spec.Names.Plural}, nil)
	if err != nil {
		t.Fatal(err)
	}
	etcdClient, _, err := storage.GetEtcdClients(restOptions.StorageConfig.Transport)
	if err != nil {
		t.Fatal(err)
	}
	defer etcdClient.Close()
	etcdObjectReader := storage.NewEtcdObjectReader(etcdClient, &restOptions, crd)

	persistedFoo, err := etcdObjectReader.GetStoredCustomResource("", returnedFoo.GetName())
	if err != nil {
		t.Fatalf("Unable read CR from stored: %v", err)
	}

	// 返回对象与持久化对象都应已剪枝
	for _, tst := range tests {
		for _, foo := range []*unstructured.Unstructured{returnedFoo, persistedFoo} {
			source := "request"
			if foo == persistedFoo {
				source = "persisted"
			}
			t.Run(fmt.Sprintf("%s of %s object", strings.Join(tst.path, "."), source), func(t *testing.T) {
				obj, found, err := unstructured.NestedMap(foo.Object, tst.path...)
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					t.Errorf("expected defaulted objected, didn't find any")
				} else if expected := map[string]interface{}{
					"apiVersion": "foos/v1",
					"kind":       "Foo",
					"metadata": map[string]interface{}{
						"name": "Bar",
					},
				}; !reflect.DeepEqual(obj, expected) {
					t.Errorf("unexpected defaulted object\n  expected: %v\n  got: %v", expected, obj)
				}
			})
		}
	}
}

// jsonPtr 将任意值序列化为 JSON 并包装为 apiextensionsv1.JSON 指针。
func jsonPtr(x interface{}) *apiextensionsv1.JSON {
	bs, err := json.Marshal(x)
	if err != nil {
		panic(err)
	}
	ret := apiextensionsv1.JSON{Raw: bs}
	return &ret
}
