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

package cbor

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/randfill"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor/internal/modes"
)

// TestStreamingCollectionsEncoding 验证流式编码与普通非流式编码的输出逐字节一致，
// 且流式编码器确实会多次调用 Write（而不是先把所有内容缓冲起来）。
// 该测试覆盖流式路径需要特别处理的各类边界：数值类型区分、重复 JSON 标签、
// 非法 UTF-8、nil/空/缺失的切片与 map、自定义 Marshaler、原始字节以及带额外字段的 List。
func TestStreamingCollectionsEncoding(t *testing.T) {
	var buf writeCountingBuffer
	var remainingItems int64 = 1
	for _, tc := range []struct {
		name         string
		in           runtime.Object
		cannotStream bool
	}{
		// 保留整数与浮点数之间的区分：CBOR 中二者是不同的主类型，不能混为一谈。
		{
			name: "Struct with floats",
			in: &StructWithFloatsList{
				Items: []StructWithFloats{
					{
						Int:     1,
						Float32: float32(1),
						Float64: 1.1,
					},
				},
			},
		},
		{
			name: "Unstructured object float",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"int":     1,
					"float32": float32(1),
					"float64": 1.1,
				},
			},
		},
		{
			name: "Unstructured items float",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"int":     1,
							"float32": float32(1),
							"float64": 1.1,
						},
					},
				},
			},
		},
		// 处理具有重复 JSON 字段名的结构体：编码输出中不能出现重复的 map 键。
		{
			name: "StructWithDuplicatedTags",
			in: &StructWithDuplicatedTagsList{
				Items: []StructWithDuplicatedTags{
					{
						Key1: "key1",
						Key2: "key2",
					},
				},
			},
		},
		// Go 字符串可能包含非法 UTF-8 序列；编码时不得因此报错。
		{
			name: "UnstructuredList object invalid UTF-8",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"key": "\x80", // first byte is a continuation byte
				},
			},
		},
		{
			name: "UnstructuredList items invalid UTF-8",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"key": "\x80",
						},
					},
				},
			},
		},
		// 保留切片/map 的三种状态：缺失、存在但为 null、存在且为空。
		{
			name: "CarpList items nil",
			in: &testapigroupv1.CarpList{
				Items: nil,
			},
		},
		{
			name: "CarpList slice nil",
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Status: testapigroupv1.CarpStatus{
							Conditions: nil,
						},
					},
				},
			},
		},
		{
			name: "CarpList map nil",
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Spec: testapigroupv1.CarpSpec{
							NodeSelector: nil,
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList items nil",
			in: &unstructured.UnstructuredList{
				Items: nil,
			},
		},
		{
			name: "UnstructuredList items slice nil",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"slice": ([]string)(nil),
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList items map nil",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"map": (map[string]string)(nil),
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList object nil",
			in: &unstructured.UnstructuredList{
				Object: nil,
			},
		},
		{
			name: "UnstructuredList object slice nil",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"slice": ([]string)(nil),
				},
			},
		},
		{
			name: "UnstructuredList object map nil",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"map": (map[string]string)(nil),
				},
			},
		},
		{
			name: "CarpList items empty",
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{},
			},
		},
		{
			name: "CarpList slice empty",
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Status: testapigroupv1.CarpStatus{
							Conditions: []testapigroupv1.CarpCondition{},
						},
					},
				},
			},
		},
		{
			name: "CarpList map empty",
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Spec: testapigroupv1.CarpSpec{
							NodeSelector: map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList items empty",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{},
			},
		},
		{
			name: "UnstructuredList items slice empty",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"slice": []string{},
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList items map empty",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"map": map[string]string{},
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList object empty",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{},
			},
		},
		{
			name: "UnstructuredList object slice empty",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"slice": []string{},
				},
			},
		},
		{
			name: "UnstructuredList object map empty",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"map": map[string]string{},
				},
			},
		},
		// 处理实现了 json.Marshaler 的结构体：List 顶层若自定义 Marshaler 则无法流式编码，
		// 但元素级自定义 Marshaler 不影响流式输出。
		{
			name:         "List with json.Marshaler cannot be streamed",
			in:           &ListWithMarshalJSONList{},
			cannotStream: true,
		},
		{
			name: "Struct with json.Marshaler",
			in: &StructWithMarshalJSONList{
				Items: []StructWithMarshalJSON{
					{},
				},
			},
		},
		// 处理实现 cbor.Marshaler 但未实现 json.Marshaler 的结构体。
		{
			name:         "List with cbor.Marshaler cannot be streamed",
			in:           &ListWithMarshalCBORList{},
			cannotStream: true,
		},
		{
			name: "Struct with cbor.Marshaler",
			in: &StructWithMarshalCBORList{
				Items: []StructWithMarshalCBOR{
					{},
				},
			},
		},
		// 处理同时实现 json.Marshaler 与 cbor.Marshaler 的结构体。
		{
			name:         "List with json.Marshaler and cbor.Marshaler cannot be streamed",
			in:           &ListWithBothMarshalersList{},
			cannotStream: true,
		},
		{
			name: "Struct with json.Marshaler and cbor.Marshaler",
			in: &StructWithBothMarshalersList{
				Items: []StructWithBothMarshalers{
					{},
				},
			},
		},
		// 处理原始字节：[]byte 与 [N]byte 在 CBOR 中都应编码为 byte string。
		{
			name: "Struct with raw bytes",
			in: &StructWithRawBytesList{
				Items: []StructWithRawBytes{
					{
						Slice: []byte{0x01, 0x02, 0x03},
						Array: [3]byte{0x01, 0x02, 0x03},
					},
				},
			},
		},
		{
			name: "UnstructuredList object raw bytes",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"slice": []byte{0x01, 0x02, 0x03},
					"array": [3]byte{0x01, 0x02, 0x03},
				},
			},
		},
		{
			name: "UnstructuredList items raw bytes",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"slice": []byte{0x01, 0x02, 0x03},
							"array": [3]byte{0x01, 0x02, 0x03},
						},
					},
				},
			},
		},
		// 其他场景：仅 TypeMeta、空列表、带 continue/remainingItemCount 的列表、多元素列表。
		{
			name: "List just kind",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind: "List",
				},
			},
		},
		{
			name: "List just apiVersion",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
				},
			},
		},
		{
			name: "List no elements",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "2345",
				},
				Items: []testapigroupv1.Carp{},
			},
		},
		{
			name: "List one element with continue",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion:    "2345",
					Continue:           "abc",
					RemainingItemCount: &remainingItems,
				},
				Items: []testapigroupv1.Carp{
					{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Carp"}, ObjectMeta: metav1.ObjectMeta{
							Name:      "pod",
							Namespace: "default",
						},
					},
				},
			},
		},
		{
			name: "List two elements",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "2345",
				},
				Items: []testapigroupv1.Carp{
					{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Carp"}, ObjectMeta: metav1.ObjectMeta{
							Name:      "pod",
							Namespace: "default",
						},
					},
					{
						TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Carp"}, ObjectMeta: metav1.ObjectMeta{
							Name:      "pod2",
							Namespace: "default2",
						},
					},
				},
			},
		},
		{
			// 顶层存在额外字段时，流式编码无法保证输出与非流式一致，必须回退。
			name: "List with extra field cannot be streamed",
			in: &ListWithAdditionalFields{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "2345",
				},
				Items: []testapigroupv1.Carp{},
			},
			cannotStream: true,
		},
		{
			// CBOR 编码器中 cbor struct tag 优先于 json，因此遵循 json tag 的
			// 流式编码器不能接管该类型；它退回通用编码器，后者会把 items 键
			// 重命名为 "elements"。
			name: "List with cbor tag cannot be streamed",
			in: &ListWithCBORTagList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
				Items: []testapigroupv1.Carp{
					{ObjectMeta: metav1.ObjectMeta{Name: "pod"}},
				},
			},
			cannotStream: true,
		},
		{
			// 非集合类型（单个对象）自然不适用流式集合编码。
			name: "Not a collection cannot be streamed",
			in: &testapigroupv1.Carp{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
			},
			cannotStream: true,
		},
		{
			name: "UnstructuredList empty",
			in:   &unstructured.UnstructuredList{},
		},
		{
			name: "UnstructuredList just kind",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"kind": "List"},
			},
		},
		{
			name: "UnstructuredList just apiVersion",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"apiVersion": "v1"},
			},
		},
		{
			name: "UnstructuredList no elements",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind": "List", "apiVersion": "v1", "metadata": map[string]interface{}{"resourceVersion": "2345"},
				},
				Items: []unstructured.Unstructured{},
			},
		},
		{
			name: "UnstructuredList one element with continue",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind": "List", "apiVersion": "v1", "metadata": map[string]interface{}{
						"resourceVersion":    "2345",
						"continue":           "abc",
						"remainingItemCount": "1",
					},
				},
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Carp",
							"metadata": map[string]interface{}{
								"name":      "pod",
								"namespace": "default",
							},
						},
					},
				},
			},
		},
		{
			name: "UnstructuredList two elements",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind": "List", "apiVersion": "v1", "metadata": map[string]interface{}{
						"resourceVersion": "2345",
					},
				},
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Carp",
							"metadata": map[string]interface{}{
								"name":      "pod",
								"namespace": "default",
							},
						},
					},
					{
						Object: map[string]interface{}{
							"apiVersion": "v1",
							"kind":       "Carp",
							"metadata": map[string]interface{}{
								"name":      "pod2",
								"namespace": "default",
							},
						},
					},
				},
			},
		},
		{
			// UnstructuredList 的 Object 中已存在 items，与 Items 字段冲突；
			// 必须与非流式编码保持一致，不能因流式路径而重复输出 items。
			name: "UnstructuredList conflict on items",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"items": []unstructured.Unstructured{
						{
							Object: map[string]interface{}{
								"name": "pod",
							},
						},
					},
				},
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"name": "pod2",
						},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 用同一份输入分别做流式与非流式编码，然后逐字节比较。
			buf.Reset()
			s := NewSerializer(nil, nil, StreamingCollectionsEncoding(true))
			if err := s.Encode(tc.in, &buf); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			var normalBuf bytes.Buffer
			normalS := NewSerializer(nil, nil, StreamingCollectionsEncoding(false))
			if err := normalS.Encode(tc.in, &normalBuf); err != nil {
				t.Fatalf("normal encode error: %v", err)
			}

			if diff := cmp.Diff(buf.Bytes(), normalBuf.Bytes()); diff != "" {
				t.Errorf("streaming and normal encoding differ:\n%s", diff)
			}

			// 通过 Write 调用次数判断是否真的走了流式路径：
			// 非流式路径通常只写一次，流式路径至少写 map 头、items 键与各元素。
			expectStreaming := !tc.cannotStream
			if expectStreaming && buf.writeCount <= 2 {
				t.Errorf("expected streaming but Write was called only: %d", buf.writeCount)
			}
			if !expectStreaming && buf.writeCount > 2 {
				t.Errorf("expected non-streaming but Write was called more than once: %d", buf.writeCount)
			}
		})
	}
}

// 以下类型是流式编码测试用的最小化 fixture。它们只实现 DeepCopyObject
// 以满足 runtime.Object，不参与真实业务逻辑。

type StructWithFloatsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithFloats `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *StructWithFloatsList) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithFloats struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Int     int
	Float32 float32
	Float64 float64
}

func (s *StructWithFloats) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithDuplicatedTagsList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithDuplicatedTags `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *StructWithDuplicatedTagsList) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithDuplicatedTags struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Key1 string `json:"key"`
	Key2 string `json:"key"` //nolint:govet
}

func (s *StructWithDuplicatedTags) DeepCopyObject() runtime.Object {
	return nil
}

type ListWithMarshalJSONList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []string `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *ListWithMarshalJSONList) DeepCopyObject() runtime.Object {
	return nil
}

func (l *ListWithMarshalJSONList) MarshalJSON() ([]byte, error) {
	return []byte(`"marshallJSON"`), nil
}

type StructWithMarshalJSONList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithMarshalJSON `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (s *StructWithMarshalJSONList) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithMarshalJSON struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

func (l *StructWithMarshalJSON) DeepCopyObject() runtime.Object {
	return nil
}

func (l *StructWithMarshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(`"marshallJSON"`), nil
}

type ListWithMarshalCBORList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []string `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *ListWithMarshalCBORList) DeepCopyObject() runtime.Object {
	return nil
}

func (l *ListWithMarshalCBORList) MarshalCBOR() ([]byte, error) {
	return []byte("\x6bmarshalCBOR"), nil
}

type StructWithMarshalCBORList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithMarshalCBOR `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (s *StructWithMarshalCBORList) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithMarshalCBOR struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

func (l *StructWithMarshalCBOR) DeepCopyObject() runtime.Object {
	return nil
}

func (l *StructWithMarshalCBOR) MarshalCBOR() ([]byte, error) {
	return []byte("\x6bmarshalCBOR"), nil
}

type ListWithBothMarshalersList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []string `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *ListWithBothMarshalersList) DeepCopyObject() runtime.Object {
	return nil
}

func (l *ListWithBothMarshalersList) MarshalJSON() ([]byte, error) {
	return []byte(`"marshalJSON"`), nil
}

func (l *ListWithBothMarshalersList) MarshalCBOR() ([]byte, error) {
	return []byte("\x6bmarshalCBOR"), nil
}

type StructWithBothMarshalersList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithBothMarshalers `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (s *StructWithBothMarshalersList) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithBothMarshalers struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

func (l *StructWithBothMarshalers) DeepCopyObject() runtime.Object {
	return nil
}

func (l *StructWithBothMarshalers) MarshalJSON() ([]byte, error) {
	return []byte(`"marshalJSON"`), nil
}

func (l *StructWithBothMarshalers) MarshalCBOR() ([]byte, error) {
	return []byte("\x6bmarshalCBOR"), nil
}

type StructWithRawBytesList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithRawBytes `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (s *StructWithRawBytesList) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithRawBytes struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Slice             []byte
	Array             [3]byte
}

func (s *StructWithRawBytes) DeepCopyObject() runtime.Object {
	return nil
}

type ListWithAdditionalFields struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []testapigroupv1.Carp `json:"items" protobuf:"bytes,2,rep,name=items"`
	AdditionalField int
}

func (s *ListWithAdditionalFields) DeepCopyObject() runtime.Object {
	return nil
}

// ListWithCBORTagList 的 json tag 结构合法，但 Items 上的 cbor struct tag
// 重命名了键。由于 CBOR 编码器优先采用 cbor tag，getListMeta 必须拒绝对该类型
// 进行流式编码。
type ListWithCBORTagList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []testapigroupv1.Carp `json:"items" cbor:"elements" protobuf:"bytes,2,rep,name=items"`
}

func (s *ListWithCBORTagList) DeepCopyObject() runtime.Object {
	return nil
}

// writeCountingBuffer 在 bytes.Buffer 基础上统计 Write 调用次数，
// 用于区分流式与非流式编码路径（流式会分多次写出）。
type writeCountingBuffer struct {
	writeCount int
	bytes.Buffer
}

// Write 记录调用次数后写入底层缓冲区。
func (b *writeCountingBuffer) Write(data []byte) (int, error) {
	b.writeCount++
	return b.Buffer.Write(data)
}

// Reset 同时清空计数与缓冲内容，便于在测试用例之间复用。
func (b *writeCountingBuffer) Reset() {
	b.writeCount = 0
	b.Buffer.Reset()
}

// TestFuzzCollectionsEncoding 用随机填充的对象反复验证三条不变式：
// 流式确定性编码与非流式编码逐字节相同；流式路径确实多次写出；
// 非确定性编码与确定性编码在语义（解码后的值与类型化对象）上等价。
func TestFuzzCollectionsEncoding(t *testing.T) {
	// FieldsV1 的内部表示不适合随机填充（可能生成非法 JSON），直接保持零值。
	disableFuzzFieldsV1 := func(field *metav1.FieldsV1, c randfill.Continue) {}
	// 为 UnstructuredList 生成结构合理的 Object：metadata 中的 resourceVersion 等
	// 必须是字符串形式，否则反序列化到类型化对象时会失败。
	fuzzUnstructuredList := func(list *unstructured.UnstructuredList, c randfill.Continue) {
		list.Object = map[string]interface{}{
			"kind":       "List",
			"apiVersion": "v1",
			c.String(0):  c.String(0),
			c.String(0):  int64(c.Intn(1000000)), // Limit to int64 range
			c.String(0):  c.Bool(),
			"metadata": map[string]interface{}{
				"resourceVersion":    fmt.Sprintf("%d", c.Intn(1000000)), // String format
				"continue":           c.String(0),
				"remainingItemCount": fmt.Sprintf("%d", c.Intn(1000000)), // String format
				c.String(0):          c.String(0),
			},
		}
		c.Fill(&list.Items)
	}
	fuzzMap := func(kvs map[string]interface{}, c randfill.Continue) {
		kvs[c.String(0)] = c.Bool()
		kvs[c.String(0)] = int64(c.Intn(1000000)) // Limit to int64 range
		kvs[c.String(0)] = c.String(0)
	}
	// 注册自定义填充函数，覆盖默认的随机生成逻辑。
	f := randfill.New().Funcs(disableFuzzFieldsV1, fuzzUnstructuredList, fuzzMap)
	streamingSerializer := NewSerializer(nil, nil)
	normalSerializer := NewSerializer(nil, nil, StreamingCollectionsEncoding(false))

	for _, tc := range []struct {
		name   string
		newObj func() runtime.Object
	}{
		{name: "CarpList", newObj: func() runtime.Object { return &testapigroupv1.CarpList{} }},
		{name: "UnstructuredList", newObj: func() runtime.Object { return &unstructured.UnstructuredList{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var streamingBuf writeCountingBuffer
			var normalBuf, ndetBuf bytes.Buffer
			// 多次试验以覆盖随机组合；失败信息中带上 trial 编号便于复现。
			for i := range 1000 {
				obj := tc.newObj()
				f.Fill(obj)

				// 以非流式确定性编码作为所有比较的基准。
				normalBuf.Reset()
				if err := normalSerializer.Encode(obj, &normalBuf); err != nil {
					t.Fatalf("trial %d: normal encode error: %v", i, err)
				}

				// 流式确定性编码必须与基准逐字节一致。
				streamingBuf.Reset()
				if err := streamingSerializer.Encode(obj, &streamingBuf); err != nil {
					t.Fatalf("trial %d: streaming encode error: %v", i, err)
				}
				if diff := cmp.Diff(normalBuf.Bytes(), streamingBuf.Bytes()); diff != "" {
					t.Logf("normal:    %x", normalBuf.Bytes())
					t.Logf("streaming: %x", streamingBuf.Bytes())
					t.Fatalf("trial %d: streaming and non-streaming differ:\n%s", i, diff)
				}
				if streamingBuf.writeCount <= 2 {
					t.Errorf("trial %d: expected streaming encoding to use more than 2 writes, got %d", i, streamingBuf.writeCount)
				}

				// 流式非确定性编码只要求语义等价：字节可以不同，但解码结果必须相同。
				ndetBuf.Reset()
				if err := streamingSerializer.EncodeNondeterministic(obj, &ndetBuf); err != nil {
					t.Fatalf("trial %d: nondeterministic encode error: %v", i, err)
				}

				var detObj, ndetObj interface{}
				if err := modes.Decode.Unmarshal(normalBuf.Bytes(), &detObj); err != nil {
					t.Fatalf("trial %d: decode deterministic: %v", i, err)
				}
				if err := modes.Decode.Unmarshal(ndetBuf.Bytes(), &ndetObj); err != nil {
					t.Fatalf("trial %d: decode nondeterministic: %v", i, err)
				}
				if diff := cmp.Diff(detObj, ndetObj); diff != "" {
					t.Errorf("trial %d: semantic mismatch between deterministic and nondeterministic:\n%s", i, diff)
				}

				detTyped := reflect.New(reflect.TypeOf(obj).Elem()).Interface()
				ndetTyped := reflect.New(reflect.TypeOf(obj).Elem()).Interface()
				if err := modes.Decode.Unmarshal(normalBuf.Bytes(), detTyped); err != nil {
					t.Fatalf("trial %d: decode deterministic into %T: %v", i, obj, err)
				}
				if err := modes.Decode.Unmarshal(ndetBuf.Bytes(), ndetTyped); err != nil {
					t.Fatalf("trial %d: decode nondeterministic into %T: %v", i, obj, err)
				}
				if !apiequality.Semantic.DeepEqual(detTyped, ndetTyped) {
					t.Errorf("trial %d: typed %T mismatch between deterministic and nondeterministic encodings", i, obj)
				}
			}
		})
	}
}

// mustCBORSelfDescribed 把 v 编码为 CBOR，并在前面加上自描述 CBOR 标签（0xd9d9f7），
// 以匹配 runtime.RawExtension 存储 CBOR 内容字节时的格式。
func mustCBORSelfDescribed(t *testing.T, v interface{}) []byte {
	t.Helper()
	data, err := modes.Encode.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal %#v to CBOR: %v", v, err)
	}
	return append([]byte{0xd9, 0xd9, 0xf7}, data...)
}

// TestStreamEncodeListRawExtension 是针对 Items 为 runtime.RawExtension 的 metav1.List
// 流式编码的回归测试。流式编码必须与非流式编码逐字节一致，并且能够往返。
//
// 此前流式路径通过 meta.ExtractList 处理 items，会把持有原始字节的 RawExtension
// 展平为 runtime.Unknown（丢失其 content type）。runtime.Unknown 实现了 MarshalJSON
// 但没有实现 MarshalCBOR，导致 CBOR 内容要么被错误地按结构体编码，要么在编码器
// 尝试从 JSON 转码时直接失败。现在的流式路径直接对每个 RawExtension 做 marshal，
// 从而尊重 runtime.RawExtension.MarshalCBOR。
func TestStreamEncodeListRawExtension(t *testing.T) {
	streamingSerializer := NewSerializer(nil, nil)
	normalSerializer := NewSerializer(nil, nil, StreamingCollectionsEncoding(false))

	for _, tc := range []struct {
		name string
		// nonEmpty 表示 Items 会编码为非空数组（应走流式路径）。
		nonEmpty bool
		// roundTrips 表示解码输出能精确还原输入。
		// 仅对 CBOR 内容的 RawExtension 成立：解码会把 JSON 字节和已填充的 Object
		// 归一化为 CBOR 的 Raw 字节。
		roundTrips bool
		items      []runtime.RawExtension
	}{
		{
			name:       "RawExtension with CBOR bytes",
			nonEmpty:   true,
			roundTrips: true,
			items: []runtime.RawExtension{
				{Raw: mustCBORSelfDescribed(t, map[string]interface{}{"foo": "bar"})},
				{Raw: mustCBORSelfDescribed(t, map[string]interface{}{"baz": int64(1)})},
			},
		},
		{
			name:     "RawExtension with JSON bytes",
			nonEmpty: true,
			items: []runtime.RawExtension{
				{Raw: []byte(`{"foo":"bar"}`)},
			},
		},
		{
			name:     "RawExtension with Object",
			nonEmpty: true,
			items: []runtime.RawExtension{
				{Object: &testapigroupv1.Carp{ObjectMeta: metav1.ObjectMeta{Name: "carp"}}},
			},
		},
		{
			name:  "RawExtension items empty",
			items: []runtime.RawExtension{},
		},
		{
			name:  "RawExtension items nil",
			items: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := &metav1.List{
				TypeMeta: metav1.TypeMeta{Kind: "List", APIVersion: "v1"},
				ListMeta: metav1.ListMeta{ResourceVersion: "42"},
				Items:    tc.items,
			}

			var streamingBuf writeCountingBuffer
			if err := streamingSerializer.Encode(in, &streamingBuf); err != nil {
				t.Fatalf("streaming encode error: %v", err)
			}

			var normalBuf bytes.Buffer
			if err := normalSerializer.Encode(in, &normalBuf); err != nil {
				t.Fatalf("normal encode error: %v", err)
			}

			if diff := cmp.Diff(normalBuf.Bytes(), streamingBuf.Bytes()); diff != "" {
				t.Logf("normal:    %x", normalBuf.Bytes())
				t.Logf("streaming: %x", streamingBuf.Bytes())
				t.Errorf("streaming output differs from normal encoding:\n%s", diff)
			}

			// 非空列表应真正走流式路径（写入次数多于 map 头 + items 键）。
			if tc.nonEmpty && streamingBuf.writeCount <= 2 {
				t.Errorf("expected streaming encoding to use more than 2 writes, got %d", streamingBuf.writeCount)
			}

			// 往返校验：对 CBOR 内容项，解码流式输出必须还原输入（见上方 roundTrips 说明）。
			if tc.roundTrips {
				out := &metav1.List{}
				if err := modes.Decode.Unmarshal(streamingBuf.Bytes(), out); err != nil {
					t.Fatalf("decode error: %v", err)
				}
				if !apiequality.Semantic.DeepEqual(in, out) {
					t.Errorf("round-trip mismatch:\n%s", cmp.Diff(in, out))
				}
			}
		})
	}
}

// TestStreamEncodeCollectionsDeterministic 验证流式序列化器的输出与普通非流式序列化器完全一致。
// 它使用带完整顶层字段的列表，覆盖 map 头、字段顺序与 items 流式写出的组合。
func TestStreamEncodeCollectionsDeterministic(t *testing.T) {
	streamingSerializer := NewSerializer(nil, nil)
	normalSerializer := NewSerializer(nil, nil, StreamingCollectionsEncoding(false))

	for _, tc := range []struct {
		name string
		in   runtime.Object
	}{
		{
			name: "CarpList with all top-level fields",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "CarpList",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "42",
				},
				Items: []testapigroupv1.Carp{
					{ObjectMeta: metav1.ObjectMeta{Name: "a"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "b"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "c"}},
				},
			},
		},
		{
			name: "UnstructuredList with all top-level fields",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind":       "List",
					"apiVersion": "v1",
					"metadata":   map[string]interface{}{"resourceVersion": "42"},
				},
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"name": "a"}},
					{Object: map[string]interface{}{"name": "b"}},
					{Object: map[string]interface{}{"name": "b"}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 开启流式编码。
			var streamingBuf writeCountingBuffer
			if err := streamingSerializer.Encode(tc.in, &streamingBuf); err != nil {
				t.Fatalf("streaming encode error: %v", err)
			}

			// 使用普通非流式编码器作为对照。
			var normalBuf bytes.Buffer
			if err := normalSerializer.Encode(tc.in, &normalBuf); err != nil {
				t.Fatalf("normal encode error: %v", err)
			}

			// 两者输出必须完全相同。
			if diff := cmp.Diff(normalBuf.Bytes(), streamingBuf.Bytes()); diff != "" {
				t.Logf("normal: %x", normalBuf.Bytes())
				t.Logf("streaming: %x", streamingBuf.Bytes())
				t.Errorf("streaming output differs from normal encoding:\n%s", diff)
			}

			if streamingBuf.writeCount <= 2 {
				t.Errorf("expected streaming encoding to use more than 2 writes, got %d", streamingBuf.writeCount)
			}
		})
	}
}

// TestStreamEncodeCollectionsNondeterministic 验证流式序列化器的 EncodeNondeterministic 方法：
//  1. 语义正确：输出解码后与确定性编码得到相同对象；
//  2. 非幂等：多次试验中可观察到 key 顺序变化
//     （这是概率性断言；使用多 key 对象使误报概率低到可忽略）。
func TestStreamEncodeCollectionsNondeterministic(t *testing.T) {
	streamingSerializer := NewSerializer(nil, nil)
	normalSerializer := NewSerializer(nil, nil, StreamingCollectionsEncoding(false))

	// 列表包含 kind+apiVersion+metadata+items 共 4 个 key。
	// 使用 SortFastShuffle 时可能的排列数为 4! = 24。
	// 200 次试验中只出现 1 种排列的概率为 (1/24)^199 ≈ 0，因此该断言不会随机失败。
	const nTrials = 200

	for _, tc := range []struct {
		name string
		in   runtime.Object
	}{
		{
			name: "CarpList",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind:       "CarpList",
					APIVersion: "v1",
				},
				ListMeta: metav1.ListMeta{
					ResourceVersion: "42",
				},
				Items: []testapigroupv1.Carp{
					{ObjectMeta: metav1.ObjectMeta{Name: "a"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "b"}},
					{ObjectMeta: metav1.ObjectMeta{Name: "c"}},
				},
			},
		},
		{
			name: "UnstructuredList",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"kind":       "List",
					"apiVersion": "v1",
					"metadata":   map[string]interface{}{"resourceVersion": "42"},
				},
				Items: []unstructured.Unstructured{
					{Object: map[string]interface{}{"name": "a"}},
					{Object: map[string]interface{}{"name": "b"}},
					{Object: map[string]interface{}{"name": "c"}},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// 先做一次非流式确定性编码，作为语义等价比较的基准。
			var detBuf bytes.Buffer
			if err := normalSerializer.Encode(tc.in, &detBuf); err != nil {
				t.Fatalf("deterministic encode error: %v", err)
			}
			var detObj map[string]interface{}
			if err := modes.Decode.Unmarshal(detBuf.Bytes(), &detObj); err != nil {
				t.Fatalf("decode deterministic output: %v", err)
			}
			detTyped := reflect.New(reflect.TypeOf(tc.in).Elem()).Interface()
			if err := modes.Decode.Unmarshal(detBuf.Bytes(), detTyped); err != nil {
				t.Fatalf("decode deterministic output into %T: %v", tc.in, err)
			}

			// 进行 nTrials 次非确定性编码；一旦观察到第二种不同的字节序列即可提前结束。
			var firstEncoding string
			for i := range nTrials {
				var buf writeCountingBuffer
				if err := streamingSerializer.EncodeNondeterministic(tc.in, &buf); err != nil {
					t.Fatalf("trial %d: nondeterministic encode error: %v", i, err)
				}
				payload := buf.Bytes()

				// 语义正确性：解码结果必须与确定性基准相同。
				var ndetObj map[string]interface{}
				if err := modes.Decode.Unmarshal(payload, &ndetObj); err != nil {
					t.Fatalf("trial %d: decode nondeterministic output: %v", i, err)
				}
				if diff := cmp.Diff(detObj, ndetObj); diff != "" {
					t.Errorf("trial %d: semantic mismatch between deterministic and nondeterministic:\n%s", i, diff)
				}

				ndetTyped := reflect.New(reflect.TypeOf(tc.in).Elem()).Interface()
				if err := modes.Decode.Unmarshal(payload, ndetTyped); err != nil {
					t.Fatalf("trial %d: decode nondeterministic output into %T: %v", i, tc.in, err)
				}
				if !apiequality.Semantic.DeepEqual(detTyped, ndetTyped) {
					t.Errorf("trial %d: typed %T mismatch between deterministic and nondeterministic encodings", i, tc.in)
				}

				if buf.writeCount <= 2 {
					t.Errorf("trial %d: expected streaming encoding to use more than 2 writes, got %d", i, buf.writeCount)
				}

				enc := string(payload)
				if i == 0 {
					firstEncoding = enc
				} else if enc != firstEncoding {
					return
				}
			}

			t.Errorf("nondeterministic encoding produced only 1 unique byte sequence over %d trials; expected varied output", nTrials)
		})
	}
}

// TestCollectionHeadMatchesLibrary 验证 writeArrayHead 与 writeMapHead 的输出，
// 在所有可通过实际分配集合达到的长度编码宽度边界（及其相邻值）上，
// 与 fxamacker/cbor 库自身的 head 编码逐字节一致。
// 该库对 n 元素数组/map 会先写一个确定长度的 head，因此我们流式写出的 head 必须等于该前缀。
//
// 可达到的尺寸覆盖 1、2、3、5 字节参数形式（最后一种在 n=65536 时出现）。
// 8 字节形式（n > 2^32）无法通过分配产生，改由 TestWriteCollectionHeadBoundaries
// 对照 RFC 8949 的显式字节进行覆盖。
func TestCollectionHeadMatchesLibrary(t *testing.T) {
	// 宽度类别边界及其相邻值：
	//   n <= 23             -> 1 字节 head
	//   24    <= n <= 255   -> 2 字节 head
	//   256   <= n <= 65535 -> 3 字节 head
	//   65536 <= n          -> 5 字节 head
	sizes := []int{0, 1, 23, 24, 25, 255, 256, 257, 65535, 65536, 65537}

	// 只比较 head 前缀：库输出还包含集合内容，而我们只关心长度头。
	assertPrefix := func(t *testing.T, kind string, n int, head, lib []byte) {
		t.Helper()
		end := min(len(head), len(lib))
		if !bytes.Equal(head, lib[:end]) {
			t.Errorf("%s head mismatch for n=%d:\n  ours:    % x\n  library: % x", kind, n, head, lib[:min(len(lib), len(head)+4)])
		}
	}

	for _, n := range sizes {
		t.Run(fmt.Sprintf("array/%d", n), func(t *testing.T) {
			var head bytes.Buffer
			if err := writeArrayHead(&head, n); err != nil {
				t.Fatalf("writeArrayHead(%d): %v", n, err)
			}
			lib, err := modes.Encode.Marshal(make([]bool, n))
			if err != nil {
				t.Fatalf("library marshal []bool len %d: %v", n, err)
			}
			assertPrefix(t, "array", n, head.Bytes(), lib)
		})
		t.Run(fmt.Sprintf("map/%d", n), func(t *testing.T) {
			var head bytes.Buffer
			if err := writeMapHead(&head, n); err != nil {
				t.Fatalf("writeMapHead(%d): %v", n, err)
			}
			m := make(map[int64]bool, n)
			for i := range n {
				m[int64(i)] = false
			}
			lib, err := modes.Encode.Marshal(m)
			if err != nil {
				t.Fatalf("library marshal map len %d: %v", n, err)
			}
			assertPrefix(t, "map", n, head.Bytes(), lib)
		})
	}
}

// TestWriteCollectionHeadBoundaries 对照 RFC 8949 第 3 节的显式期望字节，
// 检查 writeCollectionHead 在每种 additional-information 宽度边界及其相邻值上的输出，
// 数组（主类型 0x80）与 map（主类型 0xa0）都要覆盖。
// 与 TestCollectionHeadMatchesLibrary 不同，这里直接调用 writeCollectionHead，
// 因此可以覆盖没有任何可分配集合能达到的 8 字节参数形式（n > 2^32）。
func TestWriteCollectionHeadBoundaries(t *testing.T) {
	for _, tc := range []struct {
		n     int64
		array []byte
		mp    []byte
	}{
		{n: 0, array: []byte{0x80}, mp: []byte{0xa0}},
		{n: 1, array: []byte{0x81}, mp: []byte{0xa1}},
		{n: 23, array: []byte{0x97}, mp: []byte{0xb7}},                                                                                                            // max 1-byte
		{n: 24, array: []byte{0x98, 0x18}, mp: []byte{0xb8, 0x18}},                                                                                                // min 2-byte
		{n: 255, array: []byte{0x98, 0xff}, mp: []byte{0xb8, 0xff}},                                                                                               // max 2-byte
		{n: 256, array: []byte{0x99, 0x01, 0x00}, mp: []byte{0xb9, 0x01, 0x00}},                                                                                   // min 3-byte
		{n: 65535, array: []byte{0x99, 0xff, 0xff}, mp: []byte{0xb9, 0xff, 0xff}},                                                                                 // max 3-byte
		{n: 65536, array: []byte{0x9a, 0x00, 0x01, 0x00, 0x00}, mp: []byte{0xba, 0x00, 0x01, 0x00, 0x00}},                                                         // min 5-byte
		{n: 4294967295, array: []byte{0x9a, 0xff, 0xff, 0xff, 0xff}, mp: []byte{0xba, 0xff, 0xff, 0xff, 0xff}},                                                    // max 5-byte (2^32-1)
		{n: 4294967296, array: []byte{0x9b, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}, mp: []byte{0xbb, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}},    // min 9-byte (2^32)
		{n: 1099511627775, array: []byte{0x9b, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff}, mp: []byte{0xbb, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff}}, // 0xFFFFFFFFFF
	} {
		t.Run(fmt.Sprintf("array/%d", tc.n), func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeCollectionHead(&buf, cborTypeArray, tc.n); err != nil {
				t.Fatalf("writeCollectionHead: %v", err)
			}
			if !bytes.Equal(buf.Bytes(), tc.array) {
				t.Errorf("n=%d: got % x, want % x", tc.n, buf.Bytes(), tc.array)
			}
		})
		t.Run(fmt.Sprintf("map/%d", tc.n), func(t *testing.T) {
			var buf bytes.Buffer
			if err := writeCollectionHead(&buf, cborTypeMap, tc.n); err != nil {
				t.Fatalf("writeCollectionHead: %v", err)
			}
			if !bytes.Equal(buf.Bytes(), tc.mp) {
				t.Errorf("n=%d: got % x, want % x", tc.n, buf.Bytes(), tc.mp)
			}
		})
	}
}
