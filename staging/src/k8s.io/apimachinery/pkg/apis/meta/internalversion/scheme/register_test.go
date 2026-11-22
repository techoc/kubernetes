/*
Copyright 2017 The Kubernetes Authors.

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

package scheme

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestListOptions 验证 meta/internalversion scheme 中 ListOptions 的三条契约：
// metav1 与 internalversion 之间可无损往返转换、非法选择器会在转换时被拒绝，
// 以及 ParameterCodec 能把 ListOptions 编解码为 URL 查询参数。
func TestListOptions(t *testing.T) {
	// 验证往返转换
	ten := int64(10)
	in := &metav1.ListOptions{
		LabelSelector:   "a=1",
		FieldSelector:   "b=1",
		ResourceVersion: "10",
		TimeoutSeconds:  &ten,
		Watch:           true,
	}
	out := &metainternalversion.ListOptions{}
	if err := Scheme.Convert(in, out, nil); err != nil {
		t.Fatal(err)
	}
	actual := &metav1.ListOptions{}
	if err := Scheme.Convert(out, actual, nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, actual) {
		t.Errorf("unexpected: %s", cmp.Diff(in, actual))
	}

	// 验证非法转换会被拒绝：internalversion 转换需要解析选择器，语法错误的表达式必须报错，
	// 否则非法查询会一路透传到存储层。
	for i, failingObject := range []*metav1.ListOptions{
		{LabelSelector: "a!!!"},
		{FieldSelector: "a!!!"},
	} {
		out = &metainternalversion.ListOptions{}
		if err := Scheme.Convert(failingObject, out, nil); err == nil {
			t.Errorf("%d: unexpected conversion: %#v", i, out)
		}
	}

	// 验证 kind 注册：两个版本的 ListOptions 都必须以自身 GroupVersion 注册，且不是 unversioned。
	if gvks, unversioned, err := Scheme.ObjectKinds(in); err != nil || unversioned || gvks[0] != metav1.SchemeGroupVersion.WithKind("ListOptions") {
		t.Errorf("unexpected: %v %v %v", gvks[0], unversioned, err)
	}
	if gvks, unversioned, err := Scheme.ObjectKinds(out); err != nil || unversioned || gvks[0] != metainternalversion.SchemeGroupVersion.WithKind("ListOptions") {
		t.Errorf("unexpected: %v %v %v", gvks[0], unversioned, err)
	}

	// watch=1 必须被解析为 true；这是 internalversion 参数解码的典型用例。
	actual = &metav1.ListOptions{}
	if err := ParameterCodec.DecodeParameters(url.Values{"watch": []string{"1"}}, metav1.SchemeGroupVersion, actual); err != nil {
		t.Fatal(err)
	}
	if !actual.Watch {
		t.Errorf("unexpected watch decode: %#v", actual)
	}

	// 检查 ParameterCodec 编码后再解码能还原全部字段。
	query, err := ParameterCodec.EncodeParameters(in, metav1.SchemeGroupVersion)
	if err != nil {
		t.Fatal(err)
	}
	actual = &metav1.ListOptions{}
	if err := ParameterCodec.DecodeParameters(query, metav1.SchemeGroupVersion, actual); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, actual) {
		t.Errorf("unexpected: %s", cmp.Diff(in, actual))
	}
}
