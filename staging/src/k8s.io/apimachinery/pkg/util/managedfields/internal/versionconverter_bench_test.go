/*
Copyright The Kubernetes Authors.

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

package internal

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/merge"
)

// benchDeployment 是结构化（reflect 支持）对象，其 JSON 形状是 apps Deployment schema 的子集。
// 内置类型以 reflect 支持的 typed value 到达版本转换器，因此在快速路径之前，
// Convert 必须通过 TypedToObject 深拷贝整个对象才能读取 apiVersion；
// 此类型用于复现该开销。
type benchDeployment struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// DeepCopyObject 实现 runtime.Object；基准里不调用它，但类型必须满足接口。
func (d *benchDeployment) DeepCopyObject() runtime.Object {
	out := &benchDeployment{TypeMeta: d.TypeMeta}
	d.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	return out
}

// structuredDeployment 返回带 n 个 label 和 n 个 annotation 的 reflect 支持 Deployment，
// 用于覆盖内置类型代码路径。
func structuredDeployment(apiVersion string, n int) runtime.Object {
	labels, annotations := metaMaps(n)
	return &benchDeployment{
		TypeMeta: metav1.TypeMeta{APIVersion: apiVersion, Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{
			Name:        "nginx-deployment",
			Namespace:   "default",
			Labels:      labels,
			Annotations: annotations,
		},
	}
}

// unstructuredDeployment 返回 value 支持的 Deployment，覆盖 CRD 代码路径；
// typed value 已持有映射，因此 TypedToObject 开销很低。
func unstructuredDeployment(apiVersion string, n int) runtime.Object {
	labels, annotations := metaMaps(n)
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":        "nginx-deployment",
			"namespace":   "default",
			"labels":      toInterfaceMap(labels),
			"annotations": toInterfaceMap(annotations),
		},
	}}
}

// metaMaps 生成 n 个 label 与 n 个 annotation，用于控制对象规模。
func metaMaps(n int) (labels, annotations map[string]string) {
	labels = make(map[string]string, n)
	annotations = make(map[string]string, n)
	for i := range n {
		labels[fmt.Sprintf("k8s.io/label-%d", i)] = fmt.Sprintf("value-%d", i)
		annotations[fmt.Sprintf("k8s.io/annotation-%d", i)] = fmt.Sprintf("value-%d", i)
	}
	return labels, annotations
}

// toInterfaceMap 把 map[string]string 转成 unstructured 需要的 map[string]interface{}。
func toInterfaceMap(m map[string]string) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// benchVersionConverter 构造一个只认识 apps/v1beta1 与 apps/v1 的转换器。
func benchVersionConverter() merge.Converter {
	oc := fakeObjectConvertorForTestSchema{
		gvkForVersion("v1beta1"): objForGroupVersion("apps/v1beta1"),
		gvkForVersion("v1"):      objForGroupVersion("apps/v1"),
	}
	return newVersionConverter(testTypeConverter, oc, schema.GroupVersion{Group: "apps", Version: runtime.APIVersionInternal})
}

// benchBackings 覆盖内置类型（反射）与 CRD（unstructured）两种底层表示。
var benchBackings = []struct {
	name  string
	build func(apiVersion string, n int) runtime.Object
}{
	{"structured", structuredDeployment},
	{"unstructured", unstructuredDeployment},
}

// benchConversions 区分同版本（快速路径）与跨版本（回退路径）。
var benchConversions = []struct {
	name    string
	version fieldpath.APIVersion
}{
	{"same-version", "apps/v1beta1"},
	{"cross-version", "apps/v1"},
}

// benchSizes 控制对象字段数量，观察转换成本随规模的变化。
var benchSizes = []int{0, 10, 100, 1000}

// BenchmarkVersionConverter 在多种对象大小下测量 versionConverter.Convert，
// 覆盖 reflect 支持（内置）和 value 支持（CRD）输入，
// 以及同版本（快速路径）和跨版本（回退路径）场景。
func BenchmarkVersionConverter(b *testing.B) {
	vc := benchVersionConverter()
	for _, backing := range benchBackings {
		for _, conv := range benchConversions {
			for _, n := range benchSizes {
				// 输入在计时外构造一次，避免把 ObjectToTyped 的成本算进 Convert。
				input, err := testTypeConverter.ObjectToTyped(backing.build("apps/v1beta1", n))
				if err != nil {
					b.Fatalf("ObjectToTyped(%s, fields=%d): %v", backing.name, n, err)
				}
				b.Run(fmt.Sprintf("%s/%s/fields=%d", backing.name, conv.name, n), func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := vc.Convert(input, conv.version); err != nil {
							b.Fatalf("Convert: %v", err)
						}
					}
				})
			}
		}
	}
}

// TestBenchmarkFastPathEngages 防止基准测试偏离目标：确认同版本场景在两类输入上均走快速路径，
// 不调用 TypedToObject 或 ConvertToVersion；
// 跨版本场景则不走快速路径，确保前后比较针对预期代码路径。
func TestBenchmarkFastPathEngages(t *testing.T) {
	for _, backing := range benchBackings {
		t.Run(backing.name, func(t *testing.T) {
			input, err := testTypeConverter.ObjectToTyped(backing.build("apps/v1beta1", 10))
			if err != nil {
				t.Fatalf("ObjectToTyped: %v", err)
			}

			//同版本：走快速路径，不物化对象或转换版本。
			tc := &countingTypeConverter{TypeConverter: testTypeConverter}
			oc := &countingObjectConvertor{ObjectConvertor: benchObjectConvertor()}
			vc := newVersionConverter(tc, oc, schema.GroupVersion{Group: "apps", Version: runtime.APIVersionInternal})
			out, err := vc.Convert(input, fieldpath.APIVersion("apps/v1beta1"))
			if err != nil {
				t.Fatalf("Convert same-version: %v", err)
			}
			if out != input {
				t.Errorf("same-version Convert should return input unchanged")
			}
			if tc.typedToObjectCalls != 0 {
				t.Errorf("same-version Convert called TypedToObject %d times, want 0", tc.typedToObjectCalls)
			}
			if oc.convertToVersionCalls != 0 {
				t.Errorf("same-version Convert called ConvertToVersion %d times, want 0", oc.convertToVersionCalls)
			}

			//跨版本：走回退路径并物化对象。
			tc = &countingTypeConverter{TypeConverter: testTypeConverter}
			oc = &countingObjectConvertor{ObjectConvertor: benchObjectConvertor()}
			vc = newVersionConverter(tc, oc, schema.GroupVersion{Group: "apps", Version: runtime.APIVersionInternal})
			if _, err := vc.Convert(input, fieldpath.APIVersion("apps/v1")); err != nil {
				t.Fatalf("Convert cross-version: %v", err)
			}
			if tc.typedToObjectCalls == 0 {
				t.Errorf("cross-version Convert should call TypedToObject")
			}
		})
	}
}

// benchObjectConvertor 返回与 benchVersionConverter 相同配置的转换器。
func benchObjectConvertor() fakeObjectConvertorForTestSchema {
	return fakeObjectConvertorForTestSchema{
		gvkForVersion("v1beta1"): objForGroupVersion("apps/v1beta1"),
		gvkForVersion("v1"):      objForGroupVersion("apps/v1"),
	}
}
