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

package objectmeta

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/equality"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

// TestRoundtripObjectMeta 校验 ObjectMeta 经 SetObjectMeta/GetObjectMeta 往返后语义不变。
func TestRoundtripObjectMeta(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	codec := json.NewSerializerWithOptions(json.DefaultMetaFactory, scheme, scheme, json.SerializerOptions{})
	seed := rand.Int63()
	fuzzer := fuzzer.FuzzerFor(metafuzzer.Funcs, rand.NewSource(seed), codecs)

	N := 1000
	for i := 0; i < N; i++ {
		u := &unstructured.Unstructured{Object: map[string]interface{}{}}
		original := &metav1.ObjectMeta{}
		fuzzer.Fill(original)
		if err := SetObjectMeta(u.Object, original); err != nil {
			t.Fatalf("unexpected error setting ObjectMeta: %v", err)
		}
		o, _, err := GetObjectMeta(u.Object, false)
		if err != nil {
			t.Fatalf("unexpected error getting the Objectmeta: %v", err)
		}

		if !equality.Semantic.DeepEqual(original, o) {
			t.Errorf("diff: %v\nCodec: %#v", cmp.Diff(original, o), codec)
		}
	}
}

// TestMalformedObjectMetaFields 为所有 metadata 字段设置各种随机值和类型。
// 如果 json.Unmarshal 能接受它们，就比较 GetObjectMeta 的结果是否一致；否则应丢弃畸形字段。
func TestMalformedObjectMetaFields(t *testing.T) {
	fuzzer := fuzzer.FuzzerFor(metafuzzer.Funcs, rand.NewSource(rand.Int63()), serializer.NewCodecFactory(runtime.NewScheme()))
	spuriousValues := func() []interface{} {
		return []interface{}{
			// 基本类型
			nil,
			int64(1),
			float64(1.5),
			true,
			"a",
			// 格式正确的复杂值
			[]interface{}{"a", "b"},
			map[string]interface{}{"a": "1", "b": "2"},
			[]interface{}{int64(1), int64(2)},
			[]interface{}{float64(1.5), float64(2.5)},
			// json 解码可以容忍的已知值
			map[string]interface{}{"a": "1", "b": nil},
			// 畸形值
			map[string]interface{}{"a": "1", "b": []interface{}{"nested"}},
			[]interface{}{"a", int64(1), float64(1.5), true, []interface{}{"nested"}},
		}
	}
	N := 100
	for i := 0; i < N; i++ {
		fuzzedObjectMeta := &metav1.ObjectMeta{}
		fuzzer.Fill(fuzzedObjectMeta)
		goodMetaMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fuzzedObjectMeta.DeepCopy())
		if err != nil {
			t.Fatal(err)
		}
		for _, pth := range jsonPaths(nil, goodMetaMap) {
			for _, v := range spuriousValues() {
				// 跳过同类型的值，因为它们只会在更深层引发解码错误
				orig, err := JSONPathValue(goodMetaMap, pth, 0)
				if err != nil {
					t.Fatalf("unexpected to not find something at %v: %v", pth, err)
				}
				if reflect.TypeOf(v) == reflect.TypeOf(orig) {
					continue
				}

				// 构造替换后的 map
				spuriousMetaMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fuzzedObjectMeta.DeepCopy())
				if err != nil {
					t.Fatal(err)
				}
				if err := SetJSONPath(spuriousMetaMap, pth, 0, v); err != nil {
					t.Fatal(err)
				}

				// 看看能否反序列化为 ObjectMeta
				spuriousJSON, err := utiljson.Marshal(spuriousMetaMap)
				if err != nil {
					t.Fatalf("error on %v=%#v: %v", pth, v, err)
				}
				expectedObjectMeta := &metav1.ObjectMeta{}
				if err := utiljson.Unmarshal(spuriousJSON, expectedObjectMeta); err != nil {
					// 如果标准 json 反序列化无法解码该字段，则整个丢弃该字段
					truncatedMetaMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(fuzzedObjectMeta.DeepCopy())
					if err != nil {
						t.Fatal(err)
					}

					// 不同字段的处理逻辑如下：
					switch {
					default:
						// 默认删除整个顶层字段
						DeleteJSONPath(truncatedMetaMap, pth[:1], 0)
					}

					truncatedJSON, err := utiljson.Marshal(truncatedMetaMap)
					if err != nil {
						t.Fatalf("error on %v=%#v: %v", pth, v, err)
					}
					expectedObjectMeta = &metav1.ObjectMeta{}
					if err := utiljson.Unmarshal(truncatedJSON, expectedObjectMeta); err != nil {
						t.Fatalf("error on %v=%#v: %v", pth, v, err)
					}
				}

				// 确认 dropInvalidTypedFields+getObjectMeta 的结果与预期一致
				u := &unstructured.Unstructured{Object: map[string]interface{}{"metadata": spuriousMetaMap}}
				actualObjectMeta, _, err := GetObjectMeta(u.Object, true)
				if err != nil {
					t.Errorf("got unexpected error after dropping invalid typed fields on %v=%#v: %v", pth, v, err)
					continue
				}

				if !equality.Semantic.DeepEqual(expectedObjectMeta, actualObjectMeta) {
					t.Errorf("%v=%#v, diff: %v\n", pth, v, cmp.Diff(expectedObjectMeta, actualObjectMeta))
					t.Errorf("expectedObjectMeta %#v", expectedObjectMeta)
				}
			}
		}
	}
}

// TestGetObjectMetaWithOptions 覆盖丢弃畸形字段和返回未知字段路径两类选项的组合。
func TestGetObjectMetaWithOptions(t *testing.T) {
	unknownAndMalformed := map[string]interface{}{
		"kind":       "Pod",
		"apiVersion": "v1",
		"metadata": map[string]interface{}{
			"name":         "my-meta",
			"unknownField": "foo",
			"generateName": nil,
			"generation":   nil,
			"labels": map[string]string{
				"foo": "bar",
			},
			"annotations": 11,
		},
	}

	unknownOnly := map[string]interface{}{
		"kind":       "Pod",
		"apiVersion": "v1",
		"metadata": map[string]interface{}{
			"name":         "my-meta",
			"unknownField": "foo",
			"generateName": nil,
			"generation":   nil,
			"labels": map[string]string{
				"foo": "bar",
			},
		},
	}

	malformedOnly := map[string]interface{}{
		"kind":       "Pod",
		"apiVersion": "v1",
		"metadata": map[string]interface{}{
			"name":         "my-meta",
			"generateName": nil,
			"generation":   nil,
			"labels": map[string]string{
				"foo": "bar",
			},
			"annotations": 11,
		},
	}

	var testcases = []struct {
		obj                     map[string]interface{}
		dropMalformedFields     bool
		returnUnknownFieldPaths bool
		expectedObject          *metav1.ObjectMeta
		expectedUnknownPaths    []string
		expectedErr             string
	}{
		{
			obj:                     unknownAndMalformed,
			dropMalformedFields:     false,
			returnUnknownFieldPaths: false,
			expectedErr:             "json: cannot unmarshal number into Go struct field ObjectMeta.annotations of type map[string]string",
		},
		{
			obj:                     unknownAndMalformed,
			dropMalformedFields:     true,
			returnUnknownFieldPaths: false,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		},
		{
			obj:                     unknownAndMalformed,
			dropMalformedFields:     false,
			returnUnknownFieldPaths: true,
			expectedErr:             "json: cannot unmarshal number into Go struct field ObjectMeta.annotations of type map[string]string",
		},
		{
			obj:                     unknownAndMalformed,
			dropMalformedFields:     true,
			returnUnknownFieldPaths: true,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
			expectedUnknownPaths: []string{"metadata.unknownField"},
		},

		{
			obj:                     unknownOnly,
			dropMalformedFields:     false,
			returnUnknownFieldPaths: false,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		},
		{
			obj:                     unknownOnly,
			dropMalformedFields:     true,
			returnUnknownFieldPaths: false,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		},
		{
			obj:                     unknownOnly,
			dropMalformedFields:     false,
			returnUnknownFieldPaths: true,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
			expectedUnknownPaths: []string{"metadata.unknownField"},
		},
		{
			obj:                     unknownOnly,
			dropMalformedFields:     true,
			returnUnknownFieldPaths: true,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
			expectedUnknownPaths: []string{"metadata.unknownField"},
		},

		{
			obj:                     malformedOnly,
			dropMalformedFields:     false,
			returnUnknownFieldPaths: false,
			expectedErr:             "json: cannot unmarshal number into Go struct field ObjectMeta.annotations of type map[string]string",
		},
		{
			obj:                     malformedOnly,
			dropMalformedFields:     true,
			returnUnknownFieldPaths: false,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		},
		{
			obj:                     malformedOnly,
			dropMalformedFields:     false,
			returnUnknownFieldPaths: true,
			expectedErr:             "json: cannot unmarshal number into Go struct field ObjectMeta.annotations of type map[string]string",
		},
		{
			obj:                     malformedOnly,
			dropMalformedFields:     true,
			returnUnknownFieldPaths: true,
			expectedObject: &metav1.ObjectMeta{
				Name: "my-meta",
				Labels: map[string]string{
					"foo": "bar",
				},
			},
		},
	}
	for _, tc := range testcases {
		opts := ObjectMetaOptions{
			ReturnUnknownFieldPaths: tc.returnUnknownFieldPaths,
			DropMalformedFields:     tc.dropMalformedFields,
		}
		obj, _, unknownPaths, err := GetObjectMetaWithOptions(tc.obj, opts)
		if !reflect.DeepEqual(tc.expectedObject, obj) {
			t.Errorf("expected: %v, got: %v", tc.expectedObject, obj)
		}
		if (err == nil && tc.expectedErr != "") || err != nil && (err.Error() != tc.expectedErr) {
			t.Errorf("expected: %v, got: %v", tc.expectedErr, err)
		}
		if !reflect.DeepEqual(tc.expectedUnknownPaths, unknownPaths) {
			t.Errorf("expected: %v, got: %v", tc.expectedUnknownPaths, unknownPaths)
		}
	}
}

// TestGetObjectMetaNils 校验 null 的 metadata 字段被读成零值，且与 kube JSON 解码结果一致。
func TestGetObjectMetaNils(t *testing.T) {
	u := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"kind":       "Pod",
			"apiVersion": "v1",
			"metadata": map[string]interface{}{
				"generateName": nil,
				"generation":   nil,
				"labels": map[string]interface{}{
					"foo": nil,
				},
			},
		},
	}

	o, _, err := GetObjectMeta(u.Object, true)
	if err != nil {
		t.Fatal(err)
	}
	if o.GenerateName != "" {
		t.Errorf("expected null json generateName value to be read as \"\" string, but got: %q", o.GenerateName)
	}
	if o.Generation != 0 {
		t.Errorf("expected null json generation value to be read as zero, but got: %d", o.Generation)
	}
	if got, expected := o.Labels, map[string]string{"foo": ""}; !reflect.DeepEqual(got, expected) {
		t.Errorf("unexpected labels, expected=%#v, got=%#v", expected, got)
	}

	// 顺便确认 kube JSON 解码的行为确实如此
	bs, _ := utiljson.Marshal(u.UnstructuredContent())
	kubeObj, _, err := clientgoscheme.Codecs.UniversalDecoder(corev1.SchemeGroupVersion).Decode(bs, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	pod, ok := kubeObj.(*corev1.Pod)
	if !ok {
		t.Fatalf("expected v1 Pod, got: %T", pod)
	}
	if got, expected := o.GenerateName, pod.ObjectMeta.GenerateName; got != expected {
		t.Errorf("expected generatedName to be %q, got %q", expected, got)
	}
	if got, expected := o.Generation, pod.ObjectMeta.Generation; got != expected {
		t.Errorf("expected generation to be %d, got %d", expected, got)
	}
	if got, expected := o.Labels, pod.ObjectMeta.Labels; !reflect.DeepEqual(got, expected) {
		t.Errorf("expected labels to be %v, got %v", expected, got)
	}
}

// TestGetObjectMeta 校验大小写不同的字段名中只有正确拼写被采纳。
func TestGetObjectMeta(t *testing.T) {
	for i := 0; i < 100; i++ {
		u := &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{
				"name": "good",
				"Name": "bad1",
				"nAme": "bad2",
				"naMe": "bad3",
				"namE": "bad4",

				"namespace": "good",
				"Namespace": "bad1",
				"nAmespace": "bad2",
				"naMespace": "bad3",
				"namEspace": "bad4",

				"creationTimestamp": "a",
			},
		}}

		meta, _, err := GetObjectMeta(u.Object, true)
		if err != nil {
			t.Fatal(err)
		}
		if meta.Name != "good" || meta.Namespace != "good" {
			t.Fatalf("got %#v", meta)
		}
	}
}
