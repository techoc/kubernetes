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

package fake

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/watchlist"
)

const (
	testGroup      = "testgroup"
	testVersion    = "testversion"
	testResource   = "testkinds"
	testNamespace  = "testns"
	testName       = "testname"
	testKind       = "TestKind"
	testAPIVersion = "testgroup/testversion"
)

// newUnstructured 构造最小 unstructured 对象，仅含 apiVersion/kind/metadata。
func newUnstructured(apiVersion, kind, namespace, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": apiVersion,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"namespace": namespace,
				"name":      name,
			},
		},
	}
}

// newUnstructuredWithSpec 在标准对象上附加 spec 字段，供 Patch 测试使用。
func newUnstructuredWithSpec(spec map[string]interface{}) *unstructured.Unstructured {
	u := newUnstructured(testAPIVersion, testKind, testNamespace, testName)
	u.Object["spec"] = spec
	return u
}

// TestDoesClientSupportWatchListSemantics 断言 fake client 明确声明不支持 WatchList，
// 从而让 informer 走传统的 List+Watch 路径，避免 fake 在未实现该协议时产生误判。
func TestDoesClientSupportWatchListSemantics(t *testing.T) {
	target := &FakeDynamicClient{}
	if !watchlist.DoesClientNotSupportWatchListSemantics(target) {
		t.Fatalf("FakeDynamicClient should NOT support WatchList semantics")
	}
}

// TestGet 验证 Get 返回的对象保留 apiVersion/kind，并能按 GVR 与命名空间精确检索。
func TestGet(t *testing.T) {
	scheme := runtime.NewScheme()

	client := NewSimpleDynamicClient(scheme, newUnstructured("group/version", "TheKind", "ns-foo", "name-foo"))
	get, err := client.Resource(schema.GroupVersionResource{Group: "group", Version: "version", Resource: "thekinds"}).Namespace("ns-foo").Get(context.TODO(), "name-foo", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}

	expected := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "group/version",
			"kind":       "TheKind",
			"metadata": map[string]interface{}{
				"name":      "name-foo",
				"namespace": "ns-foo",
			},
		},
	}
	if !equality.Semantic.DeepEqual(get, expected) {
		t.Fatal(cmp.Diff(expected, get))
	}
}

// TestListDecoding 复刻真实 List API 的解码路径，证明 dynamic client 返回的 list
// 仍带有正确的 GVK（kind 为 TheKindList）。
func TestListDecoding(t *testing.T) {
	// this the duplication of logic from the real List API.  This will prove that our dynamic client actually returns the gvk
	uncastObj, err := runtime.Decode(unstructured.UnstructuredJSONScheme, []byte(`{"apiVersion": "group/version", "kind": "TheKindList", "items":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	list := uncastObj.(*unstructured.UnstructuredList)
	expectedList := &unstructured.UnstructuredList{
		Object: map[string]interface{}{
			"apiVersion": "group/version",
			"kind":       "TheKindList",
		},
		Items: []unstructured.Unstructured{},
	}
	if !equality.Semantic.DeepEqual(list, expectedList) {
		t.Fatal(cmp.Diff(expectedList, list))
	}
}

// TestGetDecoding 复刻真实 Get API 的解码路径，验证单对象解码后 GVK 完整。
func TestGetDecoding(t *testing.T) {
	// this the duplication of logic from the real Get API.  This will prove that our dynamic client actually returns the gvk
	uncastObj, err := runtime.Decode(unstructured.UnstructuredJSONScheme, []byte(`{"apiVersion": "group/version", "kind": "TheKind"}`))
	if err != nil {
		t.Fatal(err)
	}
	get := uncastObj.(*unstructured.Unstructured)
	expectedObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "group/version",
			"kind":       "TheKind",
		},
	}
	if !equality.Semantic.DeepEqual(get, expectedObj) {
		t.Fatal(cmp.Diff(expectedObj, get))
	}
}

// TestList 验证 List 只返回指定 GVR 的对象，并保持确定性的排序。
// fixture 中混入 group2/version 的对象，期望结果中被排除；同 GVR 的对象按
// 名称升序返回（bar、baz、foo），避免 map 遍历顺序带来的不稳定断言。
func TestList(t *testing.T) {
	scheme := runtime.NewScheme()

	client := NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			{Group: "group", Version: "version", Resource: "thekinds"}: "TheKindList",
		},
		newUnstructured("group/version", "TheKind", "ns-foo", "name-foo"),
		newUnstructured("group2/version", "TheKind", "ns-foo", "name2-foo"),
		newUnstructured("group/version", "TheKind", "ns-foo", "name-bar"),
		newUnstructured("group/version", "TheKind", "ns-foo", "name-baz"),
		newUnstructured("group2/version", "TheKind", "ns-foo", "name2-baz"),
	)
	listFirst, err := client.Resource(schema.GroupVersionResource{Group: "group", Version: "version", Resource: "thekinds"}).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	expected := []unstructured.Unstructured{
		*newUnstructured("group/version", "TheKind", "ns-foo", "name-bar"),
		*newUnstructured("group/version", "TheKind", "ns-foo", "name-baz"),
		*newUnstructured("group/version", "TheKind", "ns-foo", "name-foo"),
	}
	if !equality.Semantic.DeepEqual(listFirst.Items, expected) {
		t.Fatal(cmp.Diff(expected, listFirst.Items))
	}
}

// Test_ListKind 验证使用 UnstructuredList 作为种子时，List 返回正确的 list 元数据。
// fake tracker 的 resourceVersion 从 1 开始，每创建一个对象递增；种子里
// 已有三个对象，因此 rv 为 4，continue 为空串。
func Test_ListKind(t *testing.T) {
	scheme := runtime.NewScheme()

	client := NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{
			{Group: "group", Version: "version", Resource: "thekinds"}: "TheKindList",
		},
		&unstructured.UnstructuredList{
			Object: map[string]interface{}{
				"apiVersion": "group/version",
				"kind":       "TheKindList",
			},
			Items: []unstructured.Unstructured{
				*newUnstructured("group/version", "TheKind", "ns-foo", "name-foo"),
				*newUnstructured("group/version", "TheKind", "ns-foo", "name-bar"),
				*newUnstructured("group/version", "TheKind", "ns-foo", "name-baz"),
			},
		},
	)
	listFirst, err := client.Resource(schema.GroupVersionResource{Group: "group", Version: "version", Resource: "thekinds"}).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	expectedList := &unstructured.UnstructuredList{
		Object: map[string]interface{}{
			"apiVersion": "group/version",
			"kind":       "TheKindList",
			"metadata": map[string]interface{}{
				"continue":        "",
				"resourceVersion": "4", // Three objects created so far, starting value is 1.
			},
		},
		Items: []unstructured.Unstructured{
			*newUnstructured("group/version", "TheKind", "ns-foo", "name-bar"),
			*newUnstructured("group/version", "TheKind", "ns-foo", "name-baz"),
			*newUnstructured("group/version", "TheKind", "ns-foo", "name-foo"),
		},
	}
	if !equality.Semantic.DeepEqual(listFirst, expectedList) {
		t.Fatal(cmp.Diff(expectedList, listFirst))
	}
}

// patchTestCase 描述一个 Patch 用例：输入对象、patch 类型与内容、期望错误或结果。
type patchTestCase struct {
	name                  string
	object                runtime.Object
	patchType             types.PatchType
	patchBytes            []byte
	wantErrMsg            string
	expectedPatchedObject runtime.Object
}

// runner 用新构造的 fake client 执行 Patch，并校验错误与结果。
func (tc *patchTestCase) runner(t *testing.T) {
	client := NewSimpleDynamicClient(runtime.NewScheme(), tc.object)
	resourceInterface := client.Resource(schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}).Namespace(testNamespace)

	got, recErr := resourceInterface.Patch(context.TODO(), testName, tc.patchType, tc.patchBytes, metav1.PatchOptions{})

	if err := tc.verifyErr(recErr); err != nil {
		t.Error(err)
	}

	if err := tc.verifyResult(got); err != nil {
		t.Error(err)
	}

}

// verifyErr verifies that the given error returned from Patch is the error
// expected by the test case.
//
// 中文：期望错误时要求错误文本完全一致；不期望错误时必须为 nil。
func (tc *patchTestCase) verifyErr(err error) error {
	if tc.wantErrMsg != "" && err == nil {
		return fmt.Errorf("want error, got nil")
	}

	if tc.wantErrMsg == "" && err != nil {
		return fmt.Errorf("want no error, got %v", err)
	}

	if err != nil {
		if want, got := tc.wantErrMsg, err.Error(); want != got {
			return fmt.Errorf("incorrect error: want: %q got: %q", want, got)
		}
	}
	return nil
}

// verifyResult 对比 Patch 返回对象与期望对象；两者同为 nil 视为成功。
func (tc *patchTestCase) verifyResult(result *unstructured.Unstructured) error {
	if tc.expectedPatchedObject == nil && result == nil {
		return nil
	}
	if !equality.Semantic.DeepEqual(result, tc.expectedPatchedObject) {
		return fmt.Errorf("unexpected diff in received object: %s", cmp.Diff(tc.expectedPatchedObject, result))
	}
	return nil
}

// TestPatch 覆盖 JSON Patch、Merge Patch 对 unstructured 的效果与边界。
// unstructured 不支持 StrategicMergePatch（没有 Go 类型信息可供合并策略），
// 因此该类型会返回解析错误；JSON Patch 的 add/replace/remove 与 Merge Patch 的
// 字段替换均应生效。
func TestPatch(t *testing.T) {
	testCases := []patchTestCase{
		{
			// 中文：strategic merge 不能处理 JSON patch 数组，报 invalid JSON document。
			name:       "jsonpatch fails with merge type",
			object:     newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
			patchType:  types.StrategicMergePatchType,
			patchBytes: []byte(`[]`),
			wantErrMsg: "invalid JSON document",
		}, {
			name:      "jsonpatch works with empty patch",
			object:    newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
			patchType: types.JSONPatchType,
			// No-op
			patchBytes:            []byte(`[]`),
			expectedPatchedObject: newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
		}, {
			name:      "jsonpatch works with simple change patch",
			object:    newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
			patchType: types.JSONPatchType,
			// change spec.foo from bar to foobar
			patchBytes:            []byte(`[{"op": "replace", "path": "/spec/foo", "value": "foobar"}]`),
			expectedPatchedObject: newUnstructuredWithSpec(map[string]interface{}{"foo": "foobar"}),
		}, {
			name:      "jsonpatch works with simple addition",
			object:    newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
			patchType: types.JSONPatchType,
			// add spec.newvalue = dummy
			patchBytes:            []byte(`[{"op": "add", "path": "/spec/newvalue", "value": "dummy"}]`),
			expectedPatchedObject: newUnstructuredWithSpec(map[string]interface{}{"foo": "bar", "newvalue": "dummy"}),
		}, {
			name:      "jsonpatch works with simple deletion",
			object:    newUnstructuredWithSpec(map[string]interface{}{"foo": "bar", "toremove": "shouldnotbehere"}),
			patchType: types.JSONPatchType,
			// remove spec.newvalue = dummy
			patchBytes:            []byte(`[{"op": "remove", "path": "/spec/toremove"}]`),
			expectedPatchedObject: newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
		}, {
			// 中文：把 JSON Patch 内容误标为 strategic merge，同样解析失败。
			name:      "strategic merge patch fails with JSONPatch",
			object:    newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
			patchType: types.StrategicMergePatchType,
			// add spec.newvalue = dummy
			patchBytes: []byte(`[{"op": "add", "path": "/spec/newvalue", "value": "dummy"}]`),
			wantErrMsg: "invalid JSON document",
		}, {
			name:                  "merge patch works with simple replacement",
			object:                newUnstructuredWithSpec(map[string]interface{}{"foo": "bar"}),
			patchType:             types.MergePatchType,
			patchBytes:            []byte(`{ "spec": { "foo": "baz" } }`),
			expectedPatchedObject: newUnstructuredWithSpec(map[string]interface{}{"foo": "baz"}),
		},
		// TODO: Add tests for strategic merge using v1.Pod for example to ensure the test cases
		// demonstrate expected use cases.
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.runner)
	}
}

// TestListWithUnstructuredObjectsAndTypedScheme 确保 fake dynamic client 使用注册了
// 类型化对象的 scheme 与 unstructured 种子时 List 仍正常工作：scheme 里注册了类型化对象（mockResource），但种子是 unstructured；List
// 仍应返回 unstructured list，rv 为 2（一个对象 + 初始 1）。
func TestListWithUnstructuredObjectsAndTypedScheme(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName("name")
	u.SetNamespace("namespace")

	typedScheme := runtime.NewScheme()
	typedScheme.AddKnownTypeWithName(gvk, &mockResource{})
	typedScheme.AddKnownTypeWithName(listGVK, &mockResourceList{})

	client := NewSimpleDynamicClient(typedScheme, &u)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})

	if err != nil {
		t.Error("error listing", err)
	}

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("2") // One object created so far, initial value is 1.
	expectedList.SetContinue("")
	expectedList.Items = append(expectedList.Items, u)

	if diff := cmp.Diff(expectedList, list); diff != "" {
		t.Fatal("unexpected diff (-want, +got): ", diff)
	}
}

// TestListWithNoFixturesAndTypedScheme 验证没有种子对象时，List 返回空列表且
// resourceVersion 为初始值 1。
func TestListWithNoFixturesAndTypedScheme(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	typedScheme := runtime.NewScheme()
	typedScheme.AddKnownTypeWithName(gvk, &mockResource{})
	typedScheme.AddKnownTypeWithName(listGVK, &mockResourceList{})

	client := NewSimpleDynamicClient(typedScheme)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})

	if err != nil {
		t.Error("error listing", err)
	}

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("1") // No objects created so far.
	expectedList.SetContinue("")

	if diff := cmp.Diff(expectedList, list); diff != "" {
		t.Fatal("unexpected diff (-want, +got): ", diff)
	}
}

// This test ensures list works when the dynamic client is seeded with an empty scheme and
// unstructured typed fixtures
//
// 中文：空 scheme + unstructured 种子，List 仍能工作并返回带 RV 的 unstructured list。
func TestListWithNoScheme(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetName("name")
	u.SetNamespace("namespace")

	emptyScheme := runtime.NewScheme()

	client := NewSimpleDynamicClient(emptyScheme, &u)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})

	if err != nil {
		t.Error("error listing", err)
	}

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("2") // One object created so far, initial value is 1.
	expectedList.SetContinue("")
	expectedList.Items = append(expectedList.Items, u)

	if diff := cmp.Diff(expectedList, list); diff != "" {
		t.Fatal("unexpected diff (-want, +got): ", diff)
	}
}

// This test ensures list works when the dynamic client is seeded with an empty scheme and
// unstructured typed fixtures
//
// 中文：种子是类型化对象，List 返回时被转换为等价的 unstructured 表示。
func TestListWithTypedFixtures(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}
	gvk := gvr.GroupVersion().WithKind(testKind)

	listGVK := gvk
	listGVK.Kind += "List"

	r := mockResource{}
	r.SetGroupVersionKind(gvk)
	r.SetName("name")
	r.SetNamespace("namespace")

	u := unstructured.Unstructured{}
	u.SetGroupVersionKind(r.GetObjectKind().GroupVersionKind())
	u.SetName(r.GetName())
	u.SetNamespace(r.GetNamespace())

	typedScheme := runtime.NewScheme()
	typedScheme.AddKnownTypeWithName(gvk, &mockResource{})
	typedScheme.AddKnownTypeWithName(listGVK, &mockResourceList{})

	client := NewSimpleDynamicClient(typedScheme, &r)
	list, err := client.Resource(gvr).Namespace("namespace").List(context.Background(), metav1.ListOptions{})

	if err != nil {
		t.Error("error listing", err)
	}

	expectedList := &unstructured.UnstructuredList{}
	expectedList.SetGroupVersionKind(listGVK)
	expectedList.SetResourceVersion("2") // One object created so far, initial value is 1.
	expectedList.SetContinue("")
	expectedList.Items = []unstructured.Unstructured{u}

	if diff := cmp.Diff(expectedList, list); diff != "" {
		t.Fatal("unexpected diff (-want, +got): ", diff)
	}
}

// mockResource / mockResourceList 是测试用的极简类型化对象，只承载 TypeMeta 与元数据，
// 用于验证 typed scheme 与 unstructured 之间的相互转换。
type (
	mockResource struct {
		metav1.TypeMeta   `json:""`
		metav1.ObjectMeta `json:"metadata"`
	}
	mockResourceList struct {
		metav1.TypeMeta `json:""`
		metav1.ListMeta `json:"metadata"`

		Items []mockResource
	}
)

// DeepCopyObject 实现 runtime.Object；测试对象无指针字段，浅拷贝即可。
func (l *mockResourceList) DeepCopyObject() runtime.Object {
	o := *l
	return &o
}

// DeepCopyObject 实现 runtime.Object；测试对象无指针字段，浅拷贝即可。
func (r *mockResource) DeepCopyObject() runtime.Object {
	o := *r
	return &o
}

var _ runtime.Object = (*mockResource)(nil)
var _ runtime.Object = (*mockResourceList)(nil)
