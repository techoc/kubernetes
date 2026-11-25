/*
Copyright 2023 The Kubernetes Authors.

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

package meta

import (
	"reflect"
	"strconv"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// fakeObjectItemsNum 是测试列表的规模。
	fakeObjectItemsNum = 1000
	// exemptObjectIndex 预留为“豁免位置”的样例下标（四分之一处）；当前文件未使用。
	exemptObjectIndex = fakeObjectItemsNum / 4
)

// SampleSpec 与 FooSpec 内容相同，但分属不同结构体，用于覆盖两种接收者形态。
type SampleSpec struct {
	Flied int
}

type FooSpec struct {
	Flied int
}

// FooList 的值接收者列表，Item 为 Foo（值类型实现 runtime.Object）。
type FooList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Foo
}

func (s *FooList) DeepCopyObject() runtime.Object { panic("unimplemented") }

// SampleList 的 Item 为 Sample，由指针实现 runtime.Object，模拟 corev1.Pod 等真实类型。
type SampleList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []Sample
}

func (s *SampleList) DeepCopyObject() runtime.Object { panic("unimplemented") }

// RawExtensionList 的 Item 是 runtime.RawExtension，验证 meta 工具对包装类型的处理。
type RawExtensionList struct {
	metav1.TypeMeta
	metav1.ListMeta

	Items []runtime.RawExtension
}

func (l RawExtensionList) DeepCopyObject() runtime.Object { panic("unimplemented") }

// Foo 以值类型实现 runtime.Object，与下面 Sample 的指针实现形成对照。
type Foo struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Spec FooSpec
}

func (f Foo) GetObjectKind() schema.ObjectKind {
	tm := f.TypeMeta
	return &tm
}

func (f Foo) DeepCopyObject() runtime.Object { panic("unimplemented") }

// Sample 由指针实现 runtime.Object，与真实 API 类型一致。
type Sample struct {
	metav1.TypeMeta
	metav1.ObjectMeta
	Spec SampleSpec
}

func (s *Sample) GetObjectKind() schema.ObjectKind {
	tm := s.TypeMeta
	return &tm
}

func (s *Sample) DeepCopyObject() runtime.Object { panic("unimplemented") }

// fakeSampleList 构造 numItems 个带 TypeMeta/ObjectMeta/Spec 的 Sample，
// 名称即索引，便于在测试中核对每个元素都被访问到。
func fakeSampleList(numItems int) *SampleList {
	out := &SampleList{
		Items: make([]Sample, numItems),
	}

	for i := range out.Items {
		out.Items[i] = Sample{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "sample.org/v1",
				Kind:       "Sample",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      strconv.Itoa(i),
				Namespace: "default",
				Labels: map[string]string{
					"label-key-1": "label-value-1",
				},
				Annotations: map[string]string{
					"annotations-key-1": "annotations-value-1",
				},
			},
			Spec: SampleSpec{
				Flied: i,
			},
		}
	}
	return out
}

// fakeExtensionList 每个元素是持有 Foo 对象的 RawExtension，用于验证
// ExtractList/EachListItem 对 RawExtension 的解包。
func fakeExtensionList(numItems int) *RawExtensionList {
	out := &RawExtensionList{
		Items: make([]runtime.RawExtension, numItems),
	}

	for i := range out.Items {
		out.Items[i] = runtime.RawExtension{
			Object: &Foo{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "sample.org/v2",
					Kind:       "Sample",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      strconv.Itoa(i),
					Namespace: "default",
					Labels: map[string]string{
						"label-key-1": "label-value-1",
					},
					Annotations: map[string]string{
						"annotations-key-1": "annotations-value-1",
					},
				},
				Spec: FooSpec{
					Flied: i,
				},
			},
		}
	}
	return out
}

// fakeUnstructuredList 返回 map 形态的 UnstructuredList，覆盖非结构化路径。
func fakeUnstructuredList(numItems int) runtime.Unstructured {
	out := &unstructured.UnstructuredList{
		Items: make([]unstructured.Unstructured, numItems),
	}

	for i := range out.Items {
		out.Items[i] = unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"creationTimestamp": nil,
					"name":              strconv.Itoa(i),
				},
				"spec": map[string]interface{}{
					"hostname": "example.com",
				},
				"status": map[string]interface{}{},
			},
		}
	}
	return out
}

// fakeFooList 构造 Item 为值类型 Foo 的列表，与 fakeSampleList 覆盖不同接收者形态。
func fakeFooList(numItems int) *FooList {
	out := &FooList{
		Items: make([]Foo, numItems),
	}

	for i := range out.Items {
		out.Items[i] = Foo{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "foo.org/v1",
				Kind:       "Foo",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      strconv.Itoa(i),
				Namespace: "default",
				Labels: map[string]string{
					"label-key-1": "label-value-1",
				},
				Annotations: map[string]string{
					"annotations-key-1": "annotations-value-1",
				},
			},
			Spec: FooSpec{
				Flied: i,
			},
		}
	}
	return out
}

// TestEachList 验证 EachListItem 与 EachListItemWithAlloc 对四种列表实现
// （值接收者、指针接收者、RawExtension、Unstructured）都能遍历到全部元素。
func TestEachList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func(num int) (list runtime.Object)
		expectObjectNum int
	}{
		{
			name: "StructReceiverList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeFooList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "PointerReceiverList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeSampleList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "RawExtensionList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeExtensionList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "UnstructuredList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeUnstructuredList(fakeObjectItemsNum)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("EachListItem", func(t *testing.T) {
				// 用“名字集合”记账：每访问一个元素就删除其名字，最后必须清空。
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				err := EachListItem(list, func(object runtime.Object) error {
					o, err := Accessor(object)
					if err != nil {
						return err
					}
					delete(expectObjectNames, o.GetName())
					return nil
				})
				if err != nil {
					t.Errorf("each list item %#v: %v", list, err)
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
			t.Run("EachListItemWithAlloc", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				err := EachListItemWithAlloc(list, func(object runtime.Object) error {
					o, err := Accessor(object)
					if err != nil {
						return err
					}
					delete(expectObjectNames, o.GetName())
					return nil
				})
				if err != nil {
					t.Errorf("each list %#v with alloc: %v", list, err)
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
		})
	}
}

// TestExtractList 验证 ExtractList 与 ExtractListWithAlloc 返回的元素数量与内容正确；
// 值类型元素需要先取地址才能拿到 Accessor。
func TestExtractList(t *testing.T) {
	tests := []struct {
		name            string
		generateFunc    func(num int) (list runtime.Object)
		expectObjectNum int
	}{
		{
			name: "StructReceiverList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeFooList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "PointerReceiverList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeSampleList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "RawExtensionList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeExtensionList(num)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
		{
			name: "UnstructuredList",
			generateFunc: func(num int) (list runtime.Object) {
				return fakeUnstructuredList(fakeObjectItemsNum)
			},
			expectObjectNum: fakeObjectItemsNum,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("ExtractList", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				objs, err := ExtractList(list)
				if err != nil {
					t.Fatalf("extract list %#v: %v", list, err)
				}
				for i := range objs {
					var (
						o   metav1.Object
						err error
						obj = objs[i]
					)

					if reflect.TypeOf(obj).Kind() == reflect.Struct {
						// 值类型元素不可寻址，复制一份后再取 Accessor。
						copy := reflect.New(reflect.TypeOf(obj))
						copy.Elem().Set(reflect.ValueOf(obj))
						o, err = Accessor(copy.Interface())
					} else {
						o, err = Accessor(obj)
					}
					if err != nil {
						t.Fatalf("Accessor object %#v: %v", obj, err)
					}
					delete(expectObjectNames, o.GetName())
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
			t.Run("ExtractListWithAlloc", func(t *testing.T) {
				expectObjectNames := map[string]struct{}{}
				for i := 0; i < tc.expectObjectNum; i++ {
					expectObjectNames[strconv.Itoa(i)] = struct{}{}
				}
				list := tc.generateFunc(tc.expectObjectNum)
				objs, err := ExtractListWithAlloc(list)
				if err != nil {
					t.Fatalf("extract list with alloc: %v", err)
				}
				for i := range objs {
					var (
						o   metav1.Object
						err error
						obj = objs[i]
					)
					if reflect.TypeOf(obj).Kind() == reflect.Struct {
						copy := reflect.New(reflect.TypeOf(obj))
						copy.Elem().Set(reflect.ValueOf(obj))
						o, err = Accessor(copy.Interface())
					} else {
						o, err = Accessor(obj)
					}
					if err != nil {
						t.Fatalf("Accessor object %#v: %v", obj, err)
					}
					delete(expectObjectNames, o.GetName())
				}
				if len(expectObjectNames) != 0 {
					t.Fatal("expectObjectNames should be empty")
				}
			})
		})
	}
}

// TestLenList 覆盖 nil 列表、空列表与各具体列表类型的长度计算。
func TestLenList(t *testing.T) {
	tests := []struct {
		name string
		list runtime.Object
		want int
	}{
		{
			name: "nil",
			list: nil,
			want: 0,
		},
		{
			name: "empty FooList",
			list: &FooList{},
			want: 0,
		},
		{
			name: "FooList",
			list: fakeFooList(fakeObjectItemsNum),
			want: fakeObjectItemsNum,
		},
		{
			name: "SampleList",
			list: fakeSampleList(fakeObjectItemsNum),
			want: fakeObjectItemsNum,
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
			want: fakeObjectItemsNum,
		},
		{
			name: "UnstructuredList",
			list: fakeUnstructuredList(fakeObjectItemsNum).(*unstructured.UnstructuredList),
			want: fakeObjectItemsNum,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LenList(tc.list); got != tc.want {
				t.Errorf("LenList() = %d, want %d", got, tc.want)
			}
		})
	}
}

// BenchmarkExtractListItem 测量 ExtractList 在不同列表实现下的开销。
func BenchmarkExtractListItem(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "StructReceiverList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "PointerReceiverList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ExtractList(tc.list)
				if err != nil {
					b.Fatalf("ExtractList: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkEachListItem 测量 EachListItem 的遍历开销（含 Unstructured 路径）。
func BenchmarkEachListItem(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "StructReceiverList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "PointerReceiverList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
		{
			name: "UnstructuredList",
			list: fakeUnstructuredList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := EachListItem(tc.list, func(object runtime.Object) error {
					return nil
				})
				if err != nil {
					b.Fatalf("EachListItem: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkExtractListItemWithAlloc 测量带分配版本 ExtractListWithAlloc 的开销。
func BenchmarkExtractListItemWithAlloc(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "StructReceiverList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "PointerReceiverList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := ExtractListWithAlloc(tc.list)
				if err != nil {
					b.Fatalf("ExtractListWithAlloc: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}

// BenchmarkEachListItemWithAlloc 测量带分配版本 EachListItemWithAlloc 的开销。
func BenchmarkEachListItemWithAlloc(b *testing.B) {
	tests := []struct {
		name string
		list runtime.Object
	}{
		{
			name: "StructReceiverList",
			list: fakeFooList(fakeObjectItemsNum),
		},
		{
			name: "PointerReceiverList",
			list: fakeSampleList(fakeObjectItemsNum),
		},
		{
			name: "RawExtensionList",
			list: fakeExtensionList(fakeObjectItemsNum),
		},
		{
			name: "UnstructuredList",
			list: fakeUnstructuredList(fakeObjectItemsNum),
		},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := EachListItemWithAlloc(tc.list, func(object runtime.Object) error {
					return nil
				})
				if err != nil {
					b.Fatalf("EachListItemWithAlloc: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}
