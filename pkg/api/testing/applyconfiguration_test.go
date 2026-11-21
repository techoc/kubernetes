/*
Copyright 2021 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/applyconfigurations"
	v1mf "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/randfill"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// TestUnstructuredRoundTripApplyConfigurations 将每个已知对象类型先经由 unstructured 转换为对应的 apply
// configuration，再转换回原对象类型，并验证对象没有发生变化。
// 覆盖 unstructured 通道是为了保证 SSA 客户端依赖的 apply configuration 转换可逆。
func TestUnstructuredRoundTripApplyConfigurations(t *testing.T) {
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		// 不可 round-trip 的类型及内部版本没有对应的外部表示，无法参与 apply configuration 转换。
		if nonRoundTrippableTypes.Has(gvk.Kind) {
			continue
		}
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		// 只为注册了 apply configuration builder 的类型做验证。
		if builder := applyconfigurations.ForKind(gvk); builder == nil {
			continue
		}

		t.Run(gvk.String(), func(t *testing.T) {
			// 多次使用不同随机种子，覆盖更多字段组合；一旦失败立即停止，避免刷屏。
			for i := 0; i < 3; i++ {
				item := fuzzObject(t, gvk)
				builder := applyconfigurations.ForKind(gvk)
				unstructuredRoundTripApplyConfiguration(t, item, builder)
				if t.Failed() {
					break
				}
			}
		})
	}
}

// TestJsonRoundTripApplyConfigurations 将每个已知对象类型先经由 JSON 转换为对应的 apply
// configuration，再转换回原对象类型，并验证对象没有发生变化。
// JSON 通道与 unstructured 通道相互独立，两者都覆盖才能确认转换的可逆性。
func TestJsonRoundTripApplyConfigurations(t *testing.T) {
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		// 不可 round-trip 的类型及内部版本没有对应的外部表示，无法参与 apply configuration 转换。
		if nonRoundTrippableTypes.Has(gvk.Kind) {
			continue
		}
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		// 只为注册了 apply configuration builder 的类型做验证。
		if builder := applyconfigurations.ForKind(gvk); builder == nil {
			continue
		}

		t.Run(gvk.String(), func(t *testing.T) {
			// 多次使用不同随机种子，覆盖更多字段组合；一旦失败立即停止，避免刷屏。
			for i := 0; i < 3; i++ {
				item := fuzzObject(t, gvk)
				builder := applyconfigurations.ForKind(gvk)
				jsonRoundTripApplyConfiguration(t, item, builder)
				if t.Failed() {
					break
				}

			}
		})
	}
}

// unstructuredRoundTripApplyConfiguration 验证对象经 unstructured 转成 apply configuration 再转回来后保持不变。
// 任何一步转换失败都直接返回，因为后续比较已无意义。
func unstructuredRoundTripApplyConfiguration(t *testing.T, item runtime.Object, applyConfig interface{}) {
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(item)
	if err != nil {
		t.Errorf("ToUnstructured failed: %v", err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(u, applyConfig)
	if err != nil {
		t.Errorf("FromUnstructured failed: %v", err)
		return
	}
	// 用反射创建与原对象相同类型的新实例，作为回转的目标。
	rtObj := reflect.New(reflect.TypeOf(item).Elem()).Interface().(runtime.Object)
	u, err = runtime.DefaultUnstructuredConverter.ToUnstructured(applyConfig)
	if err != nil {
		t.Errorf("ToUnstructured failed: %v", err)
		return
	}
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(u, rtObj)
	if err != nil {
		t.Errorf("FromUnstructured failed: %v", err)
		return
	}
	// 语义等价比较（而非逐字节比较），避免因 map 顺序等无关差异误报。
	if !apiequality.Semantic.DeepEqual(item, rtObj) {
		t.Errorf("Object changed, diff: %v", cmp.Diff(item, rtObj))
	}
}

// jsonRoundTripApplyConfiguration 验证对象经 JSON 转成 apply configuration 再转回来后保持不变。
// 走 JSON 而非 unstructured，是为了覆盖 apply configuration 自带的 JSON 序列化实现。
func jsonRoundTripApplyConfiguration(t *testing.T, item runtime.Object, applyConfig interface{}) {

	objData, err := json.Marshal(item)
	if err != nil {
		t.Errorf("json.Marshal failed: %v", err)
		return
	}
	err = json.Unmarshal(objData, applyConfig)
	if err != nil {
		t.Errorf("applyConfig.UnmarshalJSON failed: %v", err)
		return
	}
	// 用反射创建与原对象相同类型的新实例，作为回转的目标。
	rtObj := reflect.New(reflect.TypeOf(item).Elem()).Interface().(runtime.Object)
	applyData, err := json.Marshal(applyConfig)
	if err != nil {
		t.Errorf("applyConfig.MarshalJSON failed: %v", err)
		return
	}
	err = json.Unmarshal(applyData, rtObj)
	if err != nil {
		t.Errorf("json.Unmarshal failed: %v", err)
		return
	}
	// 语义等价比较，容忍 map 顺序等无关差异。
	if !apiequality.Semantic.DeepEqual(item, rtObj) {
		t.Errorf("Object changed, diff: %v", cmp.Diff(item, rtObj))
	}
}

// fuzzObject 为指定的外部 GVK 生成一个随机的外部版本对象。
func fuzzObject(t *testing.T, gvk schema.GroupVersionKind) runtime.Object {
	internalVersion := schema.GroupVersion{Group: gvk.Group, Version: runtime.APIVersionInternal}
	externalVersion := gvk.GroupVersion()
	kind := gvk.Kind

	// 先对 internal 版本做 fuzz，再转成 external 版本。
	// 原因是自定义 fuzz 函数只支持 internal 对象。
	internalObj, err := legacyscheme.Scheme.New(internalVersion.WithKind(kind))
	if err != nil {
		t.Fatalf("Couldn't create internal object %v: %v", kind, err)
	}
	seed := rand.Int63()
	fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(seed), legacyscheme.Codecs).
		Funcs(
			// 不生成 InitContainers 及其状态：本测试只做 JSON 操作，这些字段会在转换中丢失。
			func(s *api.PodSpec, c randfill.Continue) {
				c.FillNoCustom(s)
				s.InitContainers = nil
			},
			func(s *api.PodStatus, c randfill.Continue) {
				c.FillNoCustom(s)
				s.InitContainerStatuses = nil
			},
			// apply configuration 类型没有 managed fields，因此从 fuzz 用例中排除。
			func(s *v1.ObjectMeta, c randfill.Continue) {
				c.FillNoCustom(s)
				s.ManagedFields = nil
				s.SelfLink = ""
			},
		).Fill(internalObj)

	item, err := legacyscheme.Scheme.New(externalVersion.WithKind(kind))
	if err != nil {
		t.Fatalf("Couldn't create external object %v: %v", kind, err)
	}
	if err := legacyscheme.Scheme.Convert(internalObj, item, nil); err != nil {
		t.Fatalf("Conversion for %v failed: %v", kind, err)
	}
	return item
}

// BenchmarkApplyConfigurationsFromUnstructured 测量从 unstructured map 构造 apply configuration 的开销。
// 预先把样例转成 map 再做基准，确保计时只覆盖目标转换。
func BenchmarkApplyConfigurationsFromUnstructured(b *testing.B) {
	items := benchmarkItems(b)
	convertor := runtime.DefaultUnstructuredConverter
	unstr := make([]map[string]interface{}, len(items))
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
		builder := &v1mf.PodApplyConfiguration{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstr[i%size], builder); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}

// BenchmarkApplyConfigurationsToUnstructured 测量把 apply configuration 转回 unstructured map 的开销。
// 预先构造好 builder，保证计时循环里只包含目标转换。
func BenchmarkApplyConfigurationsToUnstructured(b *testing.B) {
	items := benchmarkItems(b)
	convertor := runtime.DefaultUnstructuredConverter
	builders := make([]*v1mf.PodApplyConfiguration, len(items))
	for i := range items {
		item, err := convertor.ToUnstructured(&items[i])
		if err != nil || item == nil {
			b.Fatalf("unexpected error: %v", err)
		}
		builder := &v1mf.PodApplyConfiguration{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item, builder); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
		builders[i] = builder
	}
	b.ResetTimer()
	size := len(items)
	for i := 0; i < b.N; i++ {
		builder := builders[i%size]
		if _, err := runtime.DefaultUnstructuredConverter.ToUnstructured(builder); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
	b.StopTimer()
}
