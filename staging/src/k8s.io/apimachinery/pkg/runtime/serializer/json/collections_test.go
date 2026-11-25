/*
Copyright 2025 The Kubernetes Authors.

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

package json

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/randfill"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TestCollectionsEncoding 对同一组用例分别跑普通编码与流式编码，确认两种
// 实现产出完全相同的字节（流式只是分批写出，不改变语义）。
func TestCollectionsEncoding(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		testCollectionsEncoding(t, NewSerializerWithOptions(DefaultMetaFactory, nil, nil, SerializerOptions{}), false)
	})
	t.Run("Streaming", func(t *testing.T) {
		testCollectionsEncoding(t, NewSerializerWithOptions(DefaultMetaFactory, nil, nil, SerializerOptions{StreamingCollectionsEncoding: true}), true)
	})
}

// testCollectionsEncoding 全面验证 encoder 流式实现的行为。
//
// 除输出字节外，还用 writeCountingBuffer 断言 Write 调用次数：流式模式
// 必须写出多次，非流式模式只能写一次（含 cannotStream 的降级情形）。
func testCollectionsEncoding(t *testing.T, s *Serializer, streamingEnabled bool) {
	var buf writeCountingBuffer
	var remainingItems int64 = 1
	// As defined in KEP-5116 we it should include the following scenarios:
	// Context: https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/5116-streaming-response-encoding#unit-tests
	for _, tc := range []struct {
		name         string
		in           runtime.Object
		cannotStream bool
		expect       string
		// allow 提供可接受的等价输出。json v1->v2 过渡期用于容忍畸形输入
		// 处理方式的差异。
		allow []string
	}{
		// 整数 1 与浮点 1.0 必须输出不同文本，避免下游把类型搞混。
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{},\"Int\":1,\"Float32\":1,\"Float64\":1.1}]}\n",
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
			expect: "{\"float32\":1,\"float64\":1.1,\"int\":1,\"items\":[]}\n",
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
			expect: "{\"items\":[{\"float32\":1,\"float64\":1.1,\"int\":1}]}\n",
		},
		// 两个字段映射到同一个 JSON key 时，期望行为是丢弃而不是输出重复键。
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{}}]}\n",
		},
		// 非法 UTF-8 被替换为 U+FFFD，而不是报错或原样透传。
		{
			name: "UnstructuredList object invalid UTF-8 ",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"key": "\x80", // first byte is a continuation byte
				},
			},
			expect: "{\"items\":[],\"key\":\"\\ufffd\"}\n",
			// Go json/v2 emits U+FFFD as raw UTF-8 bytes rather than the \ufffd escape.
			allow: []string{"{\"items\":[],\"key\":\"\ufffd\"}\n"},
		},
		{
			name: "UnstructuredList items invalid UTF-8 ",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{
					{
						Object: map[string]interface{}{
							"key": "\x80", // first byte is a continuation byte
						},
					},
				},
			},
			expect: "{\"items\":[{\"key\":\"\\ufffd\"}]}\n",
			// Go json/v2 emits U+FFFD as raw UTF-8 bytes rather than the \ufffd escape.
			allow: []string{"{\"items\":[{\"key\":\"\ufffd\"}]}\n"},
		},
		// nil slice/map 输出 null，空 slice/map 输出 []/{ }；对 Unstructured
		// 而言 nil Items 会被规范化为 []，而 typed list 保留 null。
		{
			name: "CarpList items nil",
			in: &testapigroupv1.CarpList{
				Items: nil,
			},
			expect: "{\"metadata\":{},\"items\":null}\n",
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{},\"spec\":{},\"status\":{}}]}\n",
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{},\"spec\":{},\"status\":{}}]}\n",
		},
		{
			name: "UnstructuredList items nil",
			in: &unstructured.UnstructuredList{
				Items: nil,
			},
			expect: "{\"items\":[]}\n",
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
			expect: "{\"items\":[{\"slice\":null}]}\n",
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
			expect: "{\"items\":[{\"map\":null}]}\n",
		},
		{
			name: "UnstructuredList object nil",
			in: &unstructured.UnstructuredList{
				Object: nil,
			},
			expect: "{\"items\":[]}\n",
		},
		{
			name: "UnstructuredList object slice nil",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"slice": ([]string)(nil),
				},
			},
			expect: "{\"items\":[],\"slice\":null}\n",
		},
		{
			name: "UnstructuredList object map nil",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"map": (map[string]string)(nil),
				},
			},
			expect: "{\"items\":[],\"map\":null}\n",
		},
		{
			name: "CarpList items empty",
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{},
			},
			expect: "{\"metadata\":{},\"items\":[]}\n",
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{},\"spec\":{},\"status\":{}}]}\n",
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{},\"spec\":{},\"status\":{}}]}\n",
		},
		{
			name: "UnstructuredList items empty",
			in: &unstructured.UnstructuredList{
				Items: []unstructured.Unstructured{},
			},
			expect: "{\"items\":[]}\n",
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
			expect: "{\"items\":[{\"slice\":[]}]}\n",
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
			expect: "{\"items\":[{\"map\":{}}]}\n",
		},
		{
			name: "UnstructuredList object empty",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{},
			},
			expect: "{\"items\":[]}\n",
		},
		{
			name: "UnstructuredList object slice empty",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"slice": []string{},
				},
			},
			expect: "{\"items\":[],\"slice\":[]}\n",
		},
		{
			name: "UnstructuredList object map empty",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"map": map[string]string{},
				},
			},
			expect: "{\"items\":[],\"map\":{}}\n",
		},
		// 顶层列表若实现 MarshalJSON，整体输出由它决定，无法逐项流式写出。
		{
			name:         "List with MarshallJSON cannot be streamed",
			in:           &ListWithMarshalJSONList{},
			expect:       "\"marshallJSON\"\n",
			cannotStream: true,
		},
		{
			name: "Struct with MarshallJSON",
			in: &StructWithMarshalJSONList{
				Items: []StructWithMarshalJSON{
					{},
				},
			},
			expect: "{\"metadata\":{},\"items\":[\"marshallJSON\"]}\n",
		},
		// []byte 编码为 base64 字符串，[N]byte 编码为数字数组（与 encoding/json
		// 一致）。
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
			expect: "{\"metadata\":{},\"items\":[{\"metadata\":{},\"Slice\":\"AQID\",\"Array\":[1,2,3]}]}\n",
		},
		{
			name: "UnstructuredList object raw bytes",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"slice": []byte{0x01, 0x02, 0x03},
					"array": [3]byte{0x01, 0x02, 0x03},
				},
			},
			expect: "{\"array\":[1,2,3],\"items\":[],\"slice\":\"AQID\"}\n",
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
			expect: "{\"items\":[{\"array\":[1,2,3],\"slice\":\"AQID\"}]}\n",
		},
		// Other scenarios:
		{
			name: "List just kind",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind: "List",
				},
			},
			expect: "{\"kind\":\"List\",\"metadata\":{},\"items\":null}\n",
		},
		{
			name: "List just apiVersion",
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
				},
			},
			expect: "{\"apiVersion\":\"v1\",\"metadata\":{},\"items\":null}\n",
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
			expect: "{\"kind\":\"List\",\"apiVersion\":\"v1\",\"metadata\":{\"resourceVersion\":\"2345\"},\"items\":[]}\n",
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
					{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Carp"}, ObjectMeta: metav1.ObjectMeta{
						Name:      "pod",
						Namespace: "default",
					}},
				},
			},
			expect: "{\"kind\":\"List\",\"apiVersion\":\"v1\",\"metadata\":{\"resourceVersion\":\"2345\",\"continue\":\"abc\",\"remainingItemCount\":1},\"items\":[{\"kind\":\"Carp\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"pod\",\"namespace\":\"default\"},\"spec\":{},\"status\":{}}]}\n",
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
					{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Carp"}, ObjectMeta: metav1.ObjectMeta{
						Name:      "pod",
						Namespace: "default",
					}},
					{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Carp"}, ObjectMeta: metav1.ObjectMeta{
						Name:      "pod2",
						Namespace: "default2",
					}},
				},
			},
			expect: `{"kind":"List","apiVersion":"v1","metadata":{"resourceVersion":"2345"},"items":[{"kind":"Carp","apiVersion":"v1","metadata":{"name":"pod","namespace":"default"},"spec":{},"status":{}},{"kind":"Carp","apiVersion":"v1","metadata":{"name":"pod2","namespace":"default2"},"spec":{},"status":{}}]}
`,
		},
		{
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
			expect:       "{\"kind\":\"List\",\"apiVersion\":\"v1\",\"metadata\":{\"resourceVersion\":\"2345\"},\"items\":[],\"AdditionalField\":0}\n",
		},
		{
			name: "Not a collection cannot be streamed",
			in: &testapigroupv1.Carp{
				TypeMeta: metav1.TypeMeta{
					Kind:       "List",
					APIVersion: "v1",
				},
			},
			cannotStream: true,
			expect:       "{\"kind\":\"List\",\"apiVersion\":\"v1\",\"metadata\":{},\"spec\":{},\"status\":{}}\n",
		},
		{
			name:   "UnstructuredList empty",
			in:     &unstructured.UnstructuredList{},
			expect: "{\"items\":[]}\n",
		},
		{
			name: "UnstructuredList just kind",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"kind": "List"},
			},
			expect: "{\"items\":[],\"kind\":\"List\"}\n",
		},
		{
			name: "UnstructuredList just apiVersion",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"apiVersion": "v1"},
			},
			expect: "{\"apiVersion\":\"v1\",\"items\":[]}\n",
		},
		{
			name: "UnstructuredList no elements",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"kind": "List", "apiVersion": "v1", "metadata": map[string]interface{}{"resourceVersion": "2345"}},
				Items:  []unstructured.Unstructured{},
			},
			expect: "{\"apiVersion\":\"v1\",\"items\":[],\"kind\":\"List\",\"metadata\":{\"resourceVersion\":\"2345\"}}\n",
		},
		{
			name: "UnstructuredList one element with continue",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"kind": "List", "apiVersion": "v1", "metadata": map[string]interface{}{
					"resourceVersion":    "2345",
					"continue":           "abc",
					"remainingItemCount": "1",
				}},
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
			expect: "{\"apiVersion\":\"v1\",\"items\":[{\"apiVersion\":\"v1\",\"kind\":\"Carp\",\"metadata\":{\"name\":\"pod\",\"namespace\":\"default\"}}],\"kind\":\"List\",\"metadata\":{\"continue\":\"abc\",\"remainingItemCount\":\"1\",\"resourceVersion\":\"2345\"}}\n",
		},
		{
			name: "UnstructuredList two elements",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"kind": "List", "apiVersion": "v1", "metadata": map[string]interface{}{
					"resourceVersion": "2345",
				}},
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
			expect: "{\"apiVersion\":\"v1\",\"items\":[{\"apiVersion\":\"v1\",\"kind\":\"Carp\",\"metadata\":{\"name\":\"pod\",\"namespace\":\"default\"}},{\"apiVersion\":\"v1\",\"kind\":\"Carp\",\"metadata\":{\"name\":\"pod2\",\"namespace\":\"default\"}}],\"kind\":\"List\",\"metadata\":{\"resourceVersion\":\"2345\"}}\n",
		},
		{
			name: "UnstructuredList conflict on items",
			in: &unstructured.UnstructuredList{
				Object: map[string]interface{}{"items": []unstructured.Unstructured{
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
			expect: "{\"items\":[{\"name\":\"pod2\"}]}\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf.Reset()
			if err := s.Encode(tc.in, &buf); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			t.Logf("encoded: %s", buf.String())
			// allow 中的等价形式只容忍格式差异，不放过语义差异。
			if got := buf.String(); got != tc.expect && !slices.Contains(tc.allow, got) {
				t.Errorf("not matching:\n%s", cmp.Diff(got, tc.expect))
			}
			// 用 Write 次数验证是否真的走了流式路径。
			expectStreaming := !tc.cannotStream && streamingEnabled
			if expectStreaming && buf.writeCount <= 1 {
				t.Errorf("expected streaming but Write was called only: %d", buf.writeCount)
			}
			if !expectStreaming && buf.writeCount > 1 {
				t.Errorf("expected non-streaming but Write was called more than once: %d", buf.writeCount)
			}
		})
	}
}

// StructWithFloatsList 是测试用列表类型；DeepCopyObject 返回 nil 即可，
// 编码路径不会调用它。
type StructWithFloatsList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithFloats `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *StructWithFloatsList) DeepCopyObject() runtime.Object {
	return nil
}

// StructWithFloats 无 json tag，用于验证默认字段名与数值类型的保留。
type StructWithFloats struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Int     int
	Float32 float32
	Float64 float64
}

func (s *StructWithFloats) DeepCopyObject() runtime.Object {
	return nil
}

type StructWithDuplicatedTagsList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithDuplicatedTags `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (l *StructWithDuplicatedTagsList) DeepCopyObject() runtime.Object {
	return nil
}

// StructWithDuplicatedTags 故意让 Key1/Key2 共用 json:"key"，触发重复键
// 处理逻辑；nolint 是因为 govet 会报告重复 tag。
type StructWithDuplicatedTags struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	Key1 string `json:"key"`
	Key2 string `json:"key"` //nolint:govet
}

func (s *StructWithDuplicatedTags) DeepCopyObject() runtime.Object {
	return nil
}

// ListWithMarshalJSONList 在列表级别实现 MarshalJSON，用于验证该情形
// 会整体降级为非流式编码。
type ListWithMarshalJSONList struct {
	metav1.TypeMeta `json:""`
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
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithMarshalJSON `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (s *StructWithMarshalJSONList) DeepCopyObject() runtime.Object {
	return nil
}

// StructWithMarshalJSON 在元素级别实现 MarshalJSON；列表仍可流式编码，
// 每个元素单独调用自定义序列化。
type StructWithMarshalJSON struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

func (l *StructWithMarshalJSON) DeepCopyObject() runtime.Object {
	return nil
}

func (l *StructWithMarshalJSON) MarshalJSON() ([]byte, error) {
	return []byte(`"marshallJSON"`), nil
}

type StructWithRawBytesList struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []StructWithRawBytes `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (s *StructWithRawBytesList) DeepCopyObject() runtime.Object {
	return nil
}

// StructWithRawBytes 验证 []byte 与 [N]byte 的不同 JSON 表示。
type StructWithRawBytes struct {
	metav1.TypeMeta   `json:""`
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Slice             []byte
	Array             [3]byte
}

func (s *StructWithRawBytes) DeepCopyObject() runtime.Object {
	return nil
}

// ListWithAdditionalFields 带有 ListMeta/Items 之外的字段，因此无法只
// 流式输出 items，必须整体编码。
type ListWithAdditionalFields struct {
	metav1.TypeMeta `json:""`
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items           []testapigroupv1.Carp `json:"items" protobuf:"bytes,2,rep,name=items"`
	AdditionalField int
}

func (s *ListWithAdditionalFields) DeepCopyObject() runtime.Object {
	return nil
}

// writeCountingBuffer 统计 Write 调用次数，用来区分流式与非流式实现；
// 它嵌入 bytes.Buffer 以保留全部写入内容。
type writeCountingBuffer struct {
	writeCount int
	bytes.Buffer
}

func (b *writeCountingBuffer) Write(data []byte) (int, error) {
	b.writeCount++
	return b.Buffer.Write(data)
}

func (b *writeCountingBuffer) Reset() {
	b.writeCount = 0
	b.Buffer.Reset()
}

// TestFuzzCollectionsEncoding 用随机填充的列表做差分测试：流式编码器的
// 输出必须与普通编码器逐字节一致。随机源不固定，以扩大覆盖面。
func TestFuzzCollectionsEncoding(t *testing.T) {
	// FieldsV1 的内部结构对随机填充不友好，直接跳过。
	disableFuzzFieldsV1 := func(field *metav1.FieldsV1, c randfill.Continue) {}
	fuzzUnstructuredList := func(list *unstructured.UnstructuredList, c randfill.Continue) {
		list.Object = map[string]interface{}{
			"kind":       "List",
			"apiVersion": "v1",
			c.String(0):  c.String(0),
			c.String(0):  c.Uint64(),
			c.String(0):  c.Bool(),
			"metadata": map[string]interface{}{
				"resourceVersion":    fmt.Sprintf("%d", c.Uint64()),
				"continue":           c.String(0),
				"remainingItemCount": fmt.Sprintf("%d", c.Uint64()),
				c.String(0):          c.String(0),
			}}
		c.Fill(&list.Items)
	}
	fuzzMap := func(kvs map[string]interface{}, c randfill.Continue) {
		kvs[c.String(0)] = c.Bool()
		kvs[c.String(0)] = c.Uint64()
		kvs[c.String(0)] = c.String(0)
	}
	f := randfill.New().Funcs(disableFuzzFieldsV1, fuzzUnstructuredList, fuzzMap)
	streamingBuffer := &bytes.Buffer{}
	normalSerializer := NewSerializerWithOptions(DefaultMetaFactory, nil, nil, SerializerOptions{StreamingCollectionsEncoding: false})
	normalBuffer := &bytes.Buffer{}
	t.Run("CarpList", func(t *testing.T) {
		// 每轮都重置两个 buffer，确保比较的是单次编码结果。
		for range 1000 {
			list := &testapigroupv1.CarpList{}
			f.Fill(list)
			streamingBuffer.Reset()
			normalBuffer.Reset()
			ok, err := streamEncodeCollections(list, streamingBuffer)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Fatalf("expected streaming encoder to encode %T", list)
			}
			if err := normalSerializer.Encode(list, normalBuffer); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(normalBuffer.String(), streamingBuffer.String()); diff != "" {
				t.Logf("normal: %s", normalBuffer.String())
				t.Logf("streaming: %s", streamingBuffer.String())
				t.Errorf("not matching:\n%s", diff)
			}
		}
	})
	t.Run("UnstructuredList", func(t *testing.T) {
		for range 1000 {
			list := &unstructured.UnstructuredList{}
			f.Fill(list)
			streamingBuffer.Reset()
			normalBuffer.Reset()
			ok, err := streamEncodeCollections(list, streamingBuffer)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Fatalf("expected streaming encoder to encode %T", list)
			}
			if err := normalSerializer.Encode(list, normalBuffer); err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(normalBuffer.String(), streamingBuffer.String()); diff != "" {
				t.Logf("normal: %s", normalBuffer.String())
				t.Logf("streaming: %s", streamingBuffer.String())
				t.Errorf("not matching:\n%s", diff)
			}
		}
	})
}

// BenchmarkStreamEncodeCollections 对两种列表类型分别测量流式编码开销。
// 随机源固定为 12345，保证两次运行的输入一致、结果可比。
func BenchmarkStreamEncodeCollections(b *testing.B) {
	disableFuzzFieldsV1 := func(field *metav1.FieldsV1, c randfill.Continue) {}
	fuzzMap := func(kvs map[string]interface{}, c randfill.Continue) {
		kvs[c.String(0)] = c.Bool()
		kvs[c.String(0)] = c.Uint64()
		kvs[c.String(0)] = c.String(0)
	}
	f := randfill.New().RandSource(rand.NewSource(12345)).Funcs(disableFuzzFieldsV1, fuzzMap)
	carpList := &testapigroupv1.CarpList{}
	carpList.Items = make([]testapigroupv1.Carp, 1000)
	for i := range 1000 {
		f.Fill(&carpList.Items[i])
	}
	carpList.Kind = "CarpList"
	carpList.APIVersion = "testapigroup.k8s.io/v1"
	carpList.ResourceVersion = "12345"
	unstructuredList := &unstructured.UnstructuredList{}
	unstructuredList.Object = map[string]interface{}{
		"kind":       "List",
		"apiVersion": "v1",
		"metadata": map[string]interface{}{
			"resourceVersion": "12345",
		},
	}
	unstructuredList.Items = make([]unstructured.Unstructured, 1000)
	for i := range 1000 {
		unstrMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&carpList.Items[i])
		if err != nil {
			b.Fatalf("failed to convert carp to unstructured: %v", err)
		}
		unstructuredList.Items[i] = unstructured.Unstructured{Object: unstrMap}
	}
	b.Run("CarpList", func(b *testing.B) {
		var buf bytes.Buffer
		b.ReportAllocs()
		// 重置计时器以排除上面的数据准备时间。
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			ok, err := streamEncodeCollections(carpList, &buf)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				b.Fatal("not ok")
			}
		}
	})
	b.Run("UnstructuredList", func(b *testing.B) {
		var buf bytes.Buffer
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			ok, err := streamEncodeCollections(unstructuredList, &buf)
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				b.Fatal("not ok")
			}
		}
	})
}
