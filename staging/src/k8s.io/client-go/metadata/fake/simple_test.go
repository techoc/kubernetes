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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/watchlist"
)

// 测试夹具：metadata 客户端只处理 PartialObjectMetadata，
// 这里的组/版本/资源名仅用于构造 GVR，不对应真实 API。
const (
	testGroup      = "testgroup"
	testVersion    = "testversion"
	testResource   = "testkinds"
	testNamespace  = "testns"
	testName       = "testname"
	testKind       = "TestKind"
	testAPIVersion = "testgroup/testversion"
)

// newPartialObjectMetadata 构造带 TypeMeta 与命名空间/名称的最小元数据对象。
func newPartialObjectMetadata(apiVersion, kind, namespace, name string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: apiVersion,
			Kind:       kind,
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
	}
}

// newPartialObjectMetadataWithAnnotations 在基础对象上附加注解，
// 供 patch 用例验证字段增删改。
func newPartialObjectMetadataWithAnnotations(annotations map[string]string) *metav1.PartialObjectMetadata {
	u := newPartialObjectMetadata(testAPIVersion, testKind, testNamespace, testName)
	u.Annotations = annotations
	return u
}

// TestDoesClientSupportWatchListSemantics 确认 metadata fake 客户端不支持
// WatchList 语义，与真实客户端的降级路径保持一致。
func TestDoesClientSupportWatchListSemantics(t *testing.T) {
	target := &FakeMetadataClient{}
	if !watchlist.DoesClientNotSupportWatchListSemantics(target) {
		t.Fatalf("FakeMetadataClient should NOT support WatchList semantics")
	}
}

// TestList 验证按 GVR 过滤：只有 group/version 下的对象被返回，
// 期望顺序与 fake tracker 的输出一致。
func TestList(t *testing.T) {
	scheme := NewTestScheme()
	metav1.AddMetaToScheme(scheme)
	client := NewSimpleMetadataClient(scheme,
		newPartialObjectMetadata("group/version", "TheKind", "ns-foo", "name-foo"),
		newPartialObjectMetadata("group2/version", "TheKind", "ns-foo", "name2-foo"),
		newPartialObjectMetadata("group/version", "TheKind", "ns-foo", "name-bar"),
		newPartialObjectMetadata("group/version", "TheKind", "ns-foo", "name-baz"),
		newPartialObjectMetadata("group2/version", "TheKind", "ns-foo", "name2-baz"),
	)
	listFirst, err := client.Resource(schema.GroupVersionResource{Group: "group", Version: "version", Resource: "thekinds"}).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}

	expected := []metav1.PartialObjectMetadata{
		*newPartialObjectMetadata("group/version", "TheKind", "ns-foo", "name-bar"),
		*newPartialObjectMetadata("group/version", "TheKind", "ns-foo", "name-baz"),
		*newPartialObjectMetadata("group/version", "TheKind", "ns-foo", "name-foo"),
	}
	if !equality.Semantic.DeepEqual(listFirst.Items, expected) {
		t.Fatal(cmp.Diff(expected, listFirst.Items))
	}
}

// patchTestCase 描述一次 Patch 调用：输入对象/补丁类型/补丁字节，
// 以及期望的错误消息或结果对象。
type patchTestCase struct {
	name                  string
	object                runtime.Object
	patchType             types.PatchType
	patchBytes            []byte
	wantErrMsg            string
	expectedPatchedObject runtime.Object
}

// runner 为每个用例创建独立的 fake client，执行 Patch 并校验错误与结果。
// 每个用例使用新 client，避免上一个用例的修改泄漏到下一个用例。
func (tc *patchTestCase) runner(t *testing.T) {
	scheme := NewTestScheme()
	metav1.AddMetaToScheme(scheme)
	client := NewSimpleMetadataClient(scheme, tc.object)
	resourceInterface := client.Resource(schema.GroupVersionResource{Group: testGroup, Version: testVersion, Resource: testResource}).Namespace(testNamespace)

	got, recErr := resourceInterface.Patch(context.TODO(), testName, tc.patchType, tc.patchBytes, metav1.PatchOptions{})

	if err := tc.verifyErr(recErr); err != nil {
		t.Error(err)
	}

	if err := tc.verifyResult(got); err != nil {
		t.Error(err)
	}

}

// verifyErr 校验 Patch 返回的错误是否与用例期望一致。
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

// verifyResult 校验 Patch 返回的对象；预期为 nil 时只要求结果也为 nil。
func (tc *patchTestCase) verifyResult(result *metav1.PartialObjectMetadata) error {
	if tc.expectedPatchedObject == nil && result == nil {
		return nil
	}
	if !equality.Semantic.DeepEqual(result, tc.expectedPatchedObject) {
		return fmt.Errorf("unexpected diff in received object: %s", cmp.Diff(tc.expectedPatchedObject, result))
	}
	return nil
}

// TestPatch 覆盖 JSON Patch、Merge Patch 与 Strategic Merge Patch 在
// PartialObjectMetadata 上的行为。注意测试资源名是 "thekinds"，
// 与常量 testResource（"testkinds"）不同：此处按资源名精确匹配。
func TestPatch(t *testing.T) {
	testCases := []patchTestCase{
		{
			// PartialObjectMetadata 没有注册 strategic merge 元数据，合并补丁会按 JSON 解析而失败。
			name:       "jsonpatch fails with merge type",
			object:     newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
			patchType:  types.StrategicMergePatchType,
			patchBytes: []byte(`[]`),
			wantErrMsg: "invalid JSON document",
		}, {
			// 空 JSON Patch 是合法的 no-op，对象应保持不变。
			name:      "jsonpatch works with empty patch",
			object:    newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
			patchType: types.JSONPatchType,
			// 空补丁是合法的 no-op。
			patchBytes:            []byte(`[]`),
			expectedPatchedObject: newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
		}, {
			name:      "jsonpatch works with simple change patch",
			object:    newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
			patchType: types.JSONPatchType,
			// 把 spec.foo 从 bar 改为 foobar。
			patchBytes:            []byte(`[{"op": "replace", "path": "/metadata/annotations/foo", "value": "foobar"}]`),
			expectedPatchedObject: newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "foobar"}),
		}, {
			name:      "jsonpatch works with simple addition",
			object:    newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
			patchType: types.JSONPatchType,
			// 添加 spec.newvalue = dummy。
			patchBytes:            []byte(`[{"op": "add", "path": "/metadata/annotations/newvalue", "value": "dummy"}]`),
			expectedPatchedObject: newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar", "newvalue": "dummy"}),
		}, {
			name:      "jsonpatch works with simple deletion",
			object:    newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar", "toremove": "shouldnotbehere"}),
			patchType: types.JSONPatchType,
			// 删除 spec.newvalue = dummy。
			patchBytes:            []byte(`[{"op": "remove", "path": "/metadata/annotations/toremove"}]`),
			expectedPatchedObject: newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
		}, {
			name:      "strategic merge patch fails with JSONPatch",
			object:    newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
			patchType: types.StrategicMergePatchType,
			// 添加 spec.newvalue = dummy。
			patchBytes: []byte(`[{"op": "add", "path": "/metadata/annotations/newvalue", "value": "dummy"}]`),
			wantErrMsg: "invalid JSON document",
		}, {
			// merge patch 是递归合并，因此只覆盖给定的 annotations 键。
			name:                  "merge patch works with simple replacement",
			object:                newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "bar"}),
			patchType:             types.MergePatchType,
			patchBytes:            []byte(`{ "metadata": {"annotations": { "foo": "baz" } } }`),
			expectedPatchedObject: newPartialObjectMetadataWithAnnotations(map[string]string{"foo": "baz"}),
		},
		// TODO: 补充以 v1.Pod 为例的 strategic merge 测试，确保用例覆盖预期行为。
	}

	for _, tc := range testCases {
		t.Run(tc.name, tc.runner)
	}
}
