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

package testing

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metaunstruct "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// doRoundTrip 对单个 GVK 验证两条通道：JSON -> map -> 对象，以及 DefaultUnstructuredConverter 往返。
func doRoundTrip(t *testing.T, internalVersion schema.GroupVersion, externalVersion schema.GroupVersion, kind string) {
	// 先对 internal 版本做 fuzz 再转 external：自定义 fuzz 函数只支持 internal 对象。
	internalObj, err := legacyscheme.Scheme.New(internalVersion.WithKind(kind))
	if err != nil {
		t.Fatalf("Couldn't create internal object %v: %v", kind, err)
	}
	seed := rand.Int63()
	fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(seed), legacyscheme.Codecs).
		// 覆写默认 fuzz 函数，不生成 InitContainers 及其状态：
		// 本测试只做 JSON 操作，这些字段会在转换中消失，留着会造成假阳性。
		Funcs(
			func(s *api.PodSpec, c randfill.Continue) {
				c.FillNoCustom(s)
				s.InitContainers = nil
			},
			func(s *api.PodStatus, c randfill.Continue) {
				c.FillNoCustom(s)
				s.InitContainerStatuses = nil
			},
		).Fill(internalObj)

	item, err := legacyscheme.Scheme.New(externalVersion.WithKind(kind))
	if err != nil {
		t.Fatalf("Couldn't create external object %v: %v", kind, err)
	}
	if err := legacyscheme.Scheme.Convert(internalObj, item, nil); err != nil {
		t.Fatalf("Conversion for %v failed: %v", kind, err)
	}

	// 第一条通道：对象 -> JSON -> 无类型 map -> JSON -> 对象，
	// 模拟 unstructured 客户端中转数据的过程。
	data, err := json.Marshal(item)
	if err != nil {
		t.Errorf("Error when marshaling object: %v", err)
		return
	}
	unstr := make(map[string]interface{})
	err = json.Unmarshal(data, &unstr)
	if err != nil {
		t.Errorf("Error when unmarshaling to unstructured: %v", err)
		return
	}

	data, err = json.Marshal(unstr)
	if err != nil {
		t.Errorf("Error when marshaling unstructured: %v", err)
		return
	}
	// 反射创建同类型空对象，作为 JSON 回转的目标。
	unmarshalledObj := reflect.New(reflect.TypeOf(item).Elem()).Interface()
	err = json.Unmarshal(data, &unmarshalledObj)
	if err != nil {
		t.Errorf("Error when unmarshaling to object: %v", err)
		return
	}
	if !apiequality.Semantic.DeepEqual(item, unmarshalledObj) {
		t.Errorf("Object changed during JSON operations, diff: %v", cmp.Diff(item, unmarshalledObj))
		return
	}

	// 第二条通道：走 DefaultUnstructuredConverter 的专用转换路径。
	newUnstr, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
	if err != nil {
		t.Errorf("ToUnstructured failed: %v", err)
		return
	}

	newObj := reflect.New(reflect.TypeOf(item).Elem()).Interface().(runtime.Object)
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(newUnstr, newObj)
	if err != nil {
		t.Errorf("FromUnstructured failed: %v", err)
		return
	}

	// 语义等价比较，容忍 map 顺序等无关差异。
	if !apiequality.Semantic.DeepEqual(item, newObj) {
		t.Errorf("Object changed, diff: %v", cmp.Diff(item, newObj))
	}
}

// TestRoundTrip 对所有可 round-trip 的外部类型重复做随机 unstructured 往返测试。
func TestRoundTrip(t *testing.T) {
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		// 不可 round-trip 的类型和内部版本无法参与 unstructured 往返。
		if nonRoundTrippableTypes.Has(gvk.Kind) {
			continue
		}
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		t.Logf("Testing: %v in %v", gvk.Kind, gvk.GroupVersion().String())
		// 每个类型跑 50 轮，失败即停，避免同一类型刷出大量重复错误。
		for i := 0; i < 50; i++ {
			doRoundTrip(t, schema.GroupVersion{Group: gvk.Group, Version: runtime.APIVersionInternal}, gvk.GroupVersion(), gvk.Kind)
			if t.Failed() {
				break
			}
		}
	}
}

// TestRoundtripToUnstructured 复用 apitesting 的通用实现，覆盖 scheme 中更多类型的 unstructured 往返。
func TestRoundtripToUnstructured(t *testing.T) {
	skipped := sets.New[schema.GroupVersionKind]()
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		// 收集不可 round-trip 的类型交给通用测试跳过。
		if nonRoundTrippableTypes.Has(gvk.Kind) {
			skipped.Insert(gvk)
		}
	}

	roundtrip.RoundtripToUnstructured(t, legacyscheme.Scheme, FuzzerFuncs, skipped, nil)
}

// TestRoundTripWithEmptyCreationTimestamp 验证空的 creationTimestamp 不会导致 unstructured 回转失败。
func TestRoundTripWithEmptyCreationTimestamp(t *testing.T) {
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		if nonRoundTrippableTypes.Has(gvk.Kind) {
			continue
		}
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}

		item, err := legacyscheme.Scheme.New(gvk)
		if err != nil {
			t.Fatalf("Couldn't create external object %v: %v", gvk, err)
		}
		t.Logf("Testing: %v in %v", gvk.Kind, gvk.GroupVersion().String())

		unstrBody, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
		if err != nil {
			t.Fatalf("ToUnstructured failed: %v", err)
		}

		unstructObj := &metaunstruct.Unstructured{}
		unstructObj.Object = unstrBody

		// 通过 ObjectMeta accessor 把时间戳置空，模拟客户端省略该字段的情形。
		if meta, err := meta.Accessor(unstructObj); err == nil {
			meta.SetCreationTimestamp(metav1.Time{})
		} else {
			t.Fatalf("Unable to set creation timestamp: %v", err)
		}

		// 空的 creationTimestamp 等元数据字段不应导致回转失败。
		newObj := reflect.New(reflect.TypeOf(item).Elem()).Interface().(runtime.Object)
		err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructObj.Object, newObj)
		if err != nil {
			t.Fatalf("FromUnstructured failed: %v", err)
		}
	}
}

// BenchmarkToUnstructured 测量对象转 unstructured map 的开销。
func BenchmarkToUnstructured(b *testing.B) {
	items := benchmarkItems(b)
	size := len(items)
	convertor := runtime.DefaultUnstructuredConverter
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unstr, err := convertor.ToUnstructured(&items[i%size])
		if err != nil || unstr == nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkFromUnstructured 测量 unstructured map 回转成对象的开销。
// map 预先转换好，计时只覆盖 FromUnstructured 调用。
func BenchmarkFromUnstructured(b *testing.B) {
	items := benchmarkItems(b)
	convertor := runtime.DefaultUnstructuredConverter
	var unstr []map[string]interface{}
	for i := range items {
		item, err := convertor.ToUnstructured(&items[i])
		if err != nil || item == nil {
			b.Fatalf("unexpected error: %v", err)
		}
		unstr = append(unstr, item)
	}
	size := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj := v1.Pod{}
		if err := convertor.FromUnstructured(unstr[i%size], &obj); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkToUnstructuredViaJSON 测量通过 JSON 往返构造 map 的开销，作为转换器性能的对照。
func BenchmarkToUnstructuredViaJSON(b *testing.B) {
	items := benchmarkItems(b)
	var data [][]byte
	for i := range items {
		item, err := json.Marshal(&items[i])
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		data = append(data, item)
	}
	size := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unstr := map[string]interface{}{}
		if err := json.Unmarshal(data[i%size], &unstr); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkFromUnstructuredViaJSON 测量通过 JSON 往返从 map 还原对象的开销，作为转换器性能的对照。
func BenchmarkFromUnstructuredViaJSON(b *testing.B) {
	items := benchmarkItems(b)
	var unstr []map[string]interface{}
	for i := range items {
		data, err := json.Marshal(&items[i])
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		item := map[string]interface{}{}
		if err := json.Unmarshal(data, &item); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		unstr = append(unstr, item)
	}
	size := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		item, err := json.Marshal(unstr[i%size])
		if err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		obj := v1.Pod{}
		if err := json.Unmarshal(item, &obj); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}
