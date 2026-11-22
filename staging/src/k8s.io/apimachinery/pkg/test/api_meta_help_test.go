/*
Copyright 2014 The Kubernetes Authors.

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

package test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/api/meta"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/testapigroup"
	v1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// TestIsList 确认 meta.IsListType 能区分列表类型与普通对象。
func TestIsList(t *testing.T) {
	tests := []struct {
		obj    runtime.Object
		isList bool
	}{
		{&testapigroup.CarpList{}, true},
		{&testapigroup.Carp{}, false},
	}
	for _, item := range tests {
		if e, a := item.isList, meta.IsListType(item.obj); e != a {
			t.Errorf("%v: Expected %v, got %v", reflect.TypeOf(item.obj), e, a)
		}
	}
}

// TestExtractList 覆盖 meta.ExtractList 对四类列表的展开：
// []runtime.Object、[]RawExtension（无 Object 时包成 runtime.Unknown）、
// []*T（返回原指针）和 []T（返回值元素的地址）。
func TestExtractList(t *testing.T) {
	// list1 直接存 runtime.Object，展开时应原样返回同一批指针。
	list1 := []runtime.Object{
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
	}
	list2 := &ListV1{
		Items: []runtime.RawExtension{
			{Raw: []byte("foo")},
			{Raw: []byte("bar")},
			{Object: &v1.Carp{ObjectMeta: metav1.ObjectMeta{Name: "other"}}},
		},
	}
	list3 := &fakePtrValueList{
		Items: []*testapigroup.Carp{
			{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
		},
	}
	list4 := &testapigroup.CarpList{
		Items: []testapigroup.Carp{
			{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "3"}},
		},
	}

	testCases := []struct {
		in    runtime.Object
		out   []interface{}
		equal bool
	}{
		{
			in:  &List{},
			out: []interface{}{},
		},
		{
			in:  &ListV1{},
			out: []interface{}{},
		},
		{
			in:  &List{Items: list1},
			out: []interface{}{list1[0], list1[1]},
		},
		{
			// Raw 项被包装为 runtime.Unknown，因此只能做 DeepEqual 而非指针比较。
			in:    list2,
			out:   []interface{}{&runtime.Unknown{Raw: list2.Items[0].Raw}, &runtime.Unknown{Raw: list2.Items[1].Raw}, list2.Items[2].Object},
			equal: true,
		},
		{
			in:  list3,
			out: []interface{}{list3.Items[0], list3.Items[1]},
		},
		{
			in:  list4,
			out: []interface{}{&list4.Items[0], &list4.Items[1], &list4.Items[2]},
		},
	}
	for i, test := range testCases {
		list, err := meta.ExtractList(test.in)
		if err != nil {
			t.Fatalf("%d: extract: Unexpected error %v", i, err)
		}
		if e, a := len(test.out), len(list); e != a {
			t.Fatalf("%d: extract: Expected %v, got %v", i, e, a)
		}
		for j, e := range test.out {
			if e != list[j] {
				if !test.equal {
					t.Fatalf("%d: extract: Expected list[%d] to be %#v, but found %#v", i, j, e, list[j])
				}
				if !reflect.DeepEqual(e, list[j]) {
					t.Fatalf("%d: extract: Expected list[%d] to be %#v, but found %#v", i, j, e, list[j])
				}
			}
		}
	}
}

// TestEachListItem 与 TestExtractList 数据相同，但走回调接口。
// 注意 list2 的 Raw 项在 EachListItem 中回调为 nil，因为该 API 不会构造 Unknown。
func TestEachListItem(t *testing.T) {
	list1 := []runtime.Object{
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
	}
	list2 := &ListV1{
		Items: []runtime.RawExtension{
			{Raw: []byte("foo")},
			{Raw: []byte("bar")},
			{Object: &v1.Carp{ObjectMeta: metav1.ObjectMeta{Name: "other"}}},
		},
	}
	list3 := &fakePtrValueList{
		Items: []*testapigroup.Carp{
			{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
		},
	}
	list4 := &testapigroup.CarpList{
		Items: []testapigroup.Carp{
			{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
			{ObjectMeta: metav1.ObjectMeta{Name: "3"}},
		},
	}

	testCases := []struct {
		in  runtime.Object
		out []interface{}
	}{
		{
			in:  &List{},
			out: []interface{}{},
		},
		{
			in:  &ListV1{},
			out: []interface{}{},
		},
		{
			in:  &List{Items: list1},
			out: []interface{}{list1[0], list1[1]},
		},
		{
			// 只有 Raw 的项回调 nil，与 ExtractList 的 Unknown 包装不同。
			in:  list2,
			out: []interface{}{nil, nil, list2.Items[2].Object},
		},
		{
			in:  list3,
			out: []interface{}{list3.Items[0], list3.Items[1]},
		},
		{
			in:  list4,
			out: []interface{}{&list4.Items[0], &list4.Items[1], &list4.Items[2]},
		},
	}
	for i, test := range testCases {
		list := []runtime.Object{}
		err := meta.EachListItem(test.in, func(obj runtime.Object) error {
			list = append(list, obj)
			return nil
		})
		if err != nil {
			t.Fatalf("%d: each: Unexpected error %v", i, err)
		}
		if e, a := len(test.out), len(list); e != a {
			t.Fatalf("%d: each: Expected %v, got %v", i, e, a)
		}
		for j, e := range test.out {
			if e != list[j] {
				t.Fatalf("%d: each: Expected list[%d] to be %#v, but found %#v", i, j, e, list[j])
			}
		}
	}
}

// fakePtrInterfaceList 的 Items 是 *[]runtime.Object，用于覆盖
// "列表字段本身是指针"这种非常规形态。
type fakePtrInterfaceList struct {
	Items *[]runtime.Object
}

func (obj fakePtrInterfaceList) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}
func (obj fakePtrInterfaceList) DeepCopyObject() runtime.Object {
	panic("fakePtrInterfaceList does not support DeepCopy")
}

// TestExtractListOfInterfacePtrs 确保指针指向的空切片被正确解引用为空列表，
// 而不是因字段是指针而报错。
func TestExtractListOfInterfacePtrs(t *testing.T) {
	pl := &fakePtrInterfaceList{
		Items: &[]runtime.Object{},
	}
	list, err := meta.ExtractList(pl)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}
	if len(list) > 0 {
		t.Fatalf("Expected empty list, got %#v", list)
	}
}

// fakePtrValueList 的 Items 是 []*T，用来验证 ExtractList 返回元素原有指针
// 而不是复制出的新对象。
type fakePtrValueList struct {
	Items []*testapigroup.Carp
}

func (obj fakePtrValueList) GetObjectKind() schema.ObjectKind {
	return schema.EmptyObjectKind
}
func (obj *fakePtrValueList) DeepCopyObject() runtime.Object {
	if obj == nil {
		return nil
	}
	clone := fakePtrValueList{
		Items: make([]*testapigroup.Carp, len(obj.Items)),
	}
	for i, carp := range obj.Items {
		clone.Items[i] = carp.DeepCopy()
	}
	return &clone
}

// TestSetList 验证 SetList 把 []runtime.Object 写回具体类型的列表（[]T）。
func TestSetList(t *testing.T) {
	pl := &testapigroup.CarpList{}
	list := []runtime.Object{
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "3"}},
	}
	err := meta.SetList(pl, list)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}
	if e, a := len(list), len(pl.Items); e != a {
		t.Fatalf("Expected %v, got %v", e, a)
	}
	for i := range list {
		if e, a := list[i].(*testapigroup.Carp).Name, pl.Items[i].Name; e != a {
			t.Fatalf("Expected %v, got %v", e, a)
		}
	}
}

// TestSetListToRuntimeObjectArray 验证目标是 []runtime.Object 时直接赋值，
// 不做任何类型转换。
func TestSetListToRuntimeObjectArray(t *testing.T) {
	pl := &List{}
	list := []runtime.Object{
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "1"}},
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "2"}},
		&testapigroup.Carp{ObjectMeta: metav1.ObjectMeta{Name: "3"}},
	}
	err := meta.SetList(pl, list)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}
	if e, a := len(list), len(pl.Items); e != a {
		t.Fatalf("Expected %v, got %v", e, a)
	}
	for i := range list {
		if e, a := list[i], pl.Items[i]; e != a {
			t.Fatalf("%d: unmatched: %s", i, cmp.Diff(e, a))
		}
	}
}

// TestSetListToMatchingType 验证目标列表的 Item 类型与输入一致（Unstructured）时，
// 元素被直接放入，而不是经过 interface 中转丢失结构。
func TestSetListToMatchingType(t *testing.T) {
	pl := &unstructured.UnstructuredList{}
	list := []runtime.Object{
		&unstructured.Unstructured{Object: map[string]interface{}{"foo": 1}},
		&unstructured.Unstructured{Object: map[string]interface{}{"foo": 2}},
		&unstructured.Unstructured{Object: map[string]interface{}{"foo": 3}},
	}
	err := meta.SetList(pl, list)
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}
	if e, a := len(list), len(pl.Items); e != a {
		t.Fatalf("Expected %v, got %v", e, a)
	}
	for i := range list {
		if e, a := list[i], &pl.Items[i]; !reflect.DeepEqual(e, a) {
			t.Fatalf("%d: unmatched: %s", i, cmp.Diff(e, a))
		}
	}
}

// TestSetExtractListRoundTrip 用 fuzz 生成随机列表，验证 ExtractList 与 SetList
// 互为逆操作，包括 nil/空切片等边界。
func TestSetExtractListRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	// NilChance(0) 避免生成大量 nil 元素干扰对比；NumElements 限定列表规模。
	fuzzer := randfill.New().NilChance(0).NumElements(1, 5).Funcs(metafuzzer.Funcs(codecs)...).MaxDepth(10)
	for i := 0; i < 5; i++ {
		start := &testapigroup.CarpList{}
		fuzzer.Fill(&start.Items)

		list, err := meta.ExtractList(start)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
			continue
		}
		got := &testapigroup.CarpList{}
		err = meta.SetList(got, list)
		if err != nil {
			t.Errorf("Unexpected error %v", err)
			continue
		}
		if e, a := start, got; !reflect.DeepEqual(e, a) {
			t.Fatalf("Expected %#v, got %#v", e, a)
		}
	}
}
