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

package protobuf

import (
	"bytes"
	"encoding/base64"
	"io"
	"math/rand"
	"os/exec"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestCollectionsEncoding 分别在普通模式与流式集合编码模式下运行同一组用例，
// 确保两种编码路径产生完全相同的字节。
func TestCollectionsEncoding(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		testCollectionsEncoding(t, NewSerializer(nil, nil), false)
	})
	t.Run("Streaming", func(t *testing.T) {
		testCollectionsEncoding(t, NewSerializerWithOptions(nil, nil, SerializerOptions{StreamingCollectionsEncoding: true}), true)
	})
}

// testCollectionsEncoding 是集合编码用例的公共实现。
// expect 为期望的 base64 protobuf 字节；streamingEnabled 决定是否额外校验 Write 调用次数，
// 以区分流式编码（多次 Write）与一次性缓冲编码（单次 Write）。
func testCollectionsEncoding(t *testing.T, s *Serializer, streamingEnabled bool) {
	var remainingItems int64 = 1
	testCases := []struct {
		name string
		in   runtime.Object
		// expect 是 base64 编码的 protobuf 字节。
		expect string
	}{
		// nil 与空切片/空 map 在 protobuf 中都应被省略，产生相同输出。
		{
			name: "CarpList items nil",
			in: &testapigroupv1.CarpList{
				Items: nil,
			},
			expect: "azhzAAoECgASABIICgYKABIAGgAaACIA",
		},
		{
			name: "CarpList slice nil",
			// 元素内的切片为 nil 时，protobuf 不应输出该字段。
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Status: testapigroupv1.CarpStatus{
							Conditions: nil,
						},
					},
				},
			},
			expect: "azhzAAoECgASABJBCgYKABIAGgASNwoQCgASABoAIgAqADIAOABCABIXGgBCAEoAUgBYAGAAaACCAQCKAQCaAQAaCgoAGgAiACoAMgAaACIA",
		},
		{
			name: "CarpList map nil",
			// 元素内的 map 为 nil 时同样不应输出。
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Spec: testapigroupv1.CarpSpec{
							NodeSelector: nil,
						},
					},
				},
			},
			expect: "azhzAAoECgASABJBCgYKABIAGgASNwoQCgASABoAIgAqADIAOABCABIXGgBCAEoAUgBYAGAAaACCAQCKAQCaAQAaCgoAGgAiACoAMgAaACIA",
		},
		{
			name: "CarpList items empty",
			// 空（非 nil）切片与 nil 一样不产生字段。
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{},
			},
			expect: "azhzAAoECgASABIICgYKABIAGgAaACIA",
		},
		{
			name: "CarpList slice empty",
			// 空切片与 nil 序列化结果相同。
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Status: testapigroupv1.CarpStatus{
							Conditions: []testapigroupv1.CarpCondition{},
						},
					},
				},
			},
			expect: "azhzAAoECgASABJBCgYKABIAGgASNwoQCgASABoAIgAqADIAOABCABIXGgBCAEoAUgBYAGAAaACCAQCKAQCaAQAaCgoAGgAiACoAMgAaACIA",
		},
		{
			name: "CarpList map empty",
			// 空 map 与 nil 序列化结果相同。
			in: &testapigroupv1.CarpList{
				Items: []testapigroupv1.Carp{
					{
						Spec: testapigroupv1.CarpSpec{
							NodeSelector: map[string]string{},
						},
					},
				},
			},
			expect: "azhzAAoECgASABJBCgYKABIAGgASNwoQCgASABoAIgAqADIAOABCABIXGgBCAEoAUgBYAGAAaACCAQCKAQCaAQAaCgoAGgAiACoAMgAaACIA",
		},
		{
			name: "List just kind",
			// TypeMeta 只有 Kind 时，只输出 kind 字段。
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					Kind: "List",
				},
			},
			expect: "azhzAAoICgASBExpc3QSCAoGCgASABoAGgAiAA==",
		},
		{
			name: "List just apiVersion",
			// TypeMeta 只有 APIVersion 时，只输出 apiVersion 字段。
			in: &testapigroupv1.CarpList{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
				},
			},
			expect: "azhzAAoGCgJ2MRIAEggKBgoAEgAaABoAIgA=",
		},
		{
			name: "List no elements",
			// 空列表仍应携带 ListMeta 中的 resourceVersion。
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
			expect: "azhzAAoKCgJ2MRIETGlzdBIMCgoKABIEMjM0NRoAGgAiAA==",
		},
		{
			name: "List one element with continue",
			// continue 与 remainingItemCount 是分页续读的关键元数据，必须完整编码。
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
			expect: "azhzAAoKCgJ2MRIETGlzdBJUCg8KABIEMjM0NRoDYWJjIAESQQoaCgNwb2QSABoHZGVmYXVsdCIAKgAyADgAQgASFxoAQgBKAFIAWABgAGgAggEAigEAmgEAGgoKABoAIgAqADIAGgAiAA==",
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
			expect: "azhzAAoKCgJ2MRIETGlzdBKUAQoKCgASBDIzNDUaABJBChoKA3BvZBIAGgdkZWZhdWx0IgAqADIAOABCABIXGgBCAEoAUgBYAGAAaACCAQCKAQCaAQAaCgoAGgAiACoAMgASQwocCgRwb2QyEgAaCGRlZmF1bHQyIgAqADIAOABCABIXGgBCAEoAUgBYAGAAaACCAQCKAQCaAQAaCgoAGgAiACoAMgAaACIA",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf writeCountingBuffer
			if err := s.Encode(tc.in, &buf); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			actualBytes := buf.Bytes()
			expectBytes, err := io.ReadAll(base64.NewDecoder(base64.StdEncoding, bytes.NewBufferString(tc.expect)))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(expectBytes, actualBytes) {
				// 失败时尽量用 protoc 解码出可读的 proto 结构，方便定位差异。
				expectedBytes, err := base64.StdEncoding.DecodeString(tc.expect)
				if err == nil {
					t.Errorf("expected:\n%v\ngot:\n%v", expectedBytes, actualBytes)
				} else {
					t.Errorf("expected:\n%v\ngot:\n%v", tc.expect, base64.StdEncoding.EncodeToString(actualBytes))
				}
				actualProto := dumpProto(t, actualBytes[4:])
				expectedProto := dumpProto(t, expectBytes[4:])
				if actualProto != "" && expectedProto != "" {
					t.Log(cmp.Diff(actualProto, expectedProto))
				} else {
					t.Log(cmp.Diff(actualBytes, expectBytes))
				}
			}
			// 流式编码应分多次写出；非流式则必须一次性写出。
			if streamingEnabled && buf.writeCount <= 1 {
				t.Errorf("expected streaming but Write was called only: %d", buf.writeCount)
			}
			if !streamingEnabled && buf.writeCount > 1 {
				t.Errorf("expected non-streaming but Write was called more than once: %d", buf.writeCount)
			}
		})
	}
}

// dumpProto 在 PATH 中找到 protoc 时尽力 dump 给定的 proto 字节。
// 仅在测试失败后调用，用来提供更清晰的差异信息。
// dumpProto 仅在用例失败时用于增强可读性；找不到 protoc 时静默返回空串。
func dumpProto(t *testing.T, data []byte) string {
	t.Helper()
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Logf("cannot find protoc in path to dump proto contents: %v", err)
		return ""
	}
	cmd := exec.Command(protoc, "--decode_raw")
	cmd.Stdin = bytes.NewBuffer(data)
	d, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("protoc invocation failed: %v", err)
		return ""
	}
	return string(d)
}

// writeCountingBuffer 记录 Write 被调用的次数，用于判断编码器是否走流式路径。
type writeCountingBuffer struct {
	writeCount int
	bytes.Buffer
}

// Write 累加计数后委托给内嵌的 bytes.Buffer。
func (b *writeCountingBuffer) Write(data []byte) (int, error) {
	b.writeCount++
	return b.Buffer.Write(data)
}

// Reset 同时清空计数与缓冲内容。
func (b *writeCountingBuffer) Reset() {
	b.writeCount = 0
	b.Buffer.Reset()
}

// TestFuzzCollection 用随机生成的列表分别做流式与非流式编码，要求两者输出逐字节一致，
// 这是启用流式集合编码的前提条件。
func TestFuzzCollection(t *testing.T) {
	f := randfill.New()
	streamingEncoder := NewSerializerWithOptions(nil, nil, SerializerOptions{StreamingCollectionsEncoding: true})
	streamingBuffer := &bytes.Buffer{}
	normalEncoder := NewSerializerWithOptions(nil, nil, SerializerOptions{StreamingCollectionsEncoding: false})
	normalBuffer := &bytes.Buffer{}
	for i := 0; i < 1000; i++ {
		list := &testapigroupv1.CarpList{}
		f.FillNoCustom(list)
		streamingBuffer.Reset()
		normalBuffer.Reset()
		if err := streamingEncoder.Encode(list, streamingBuffer); err != nil {
			t.Fatal(err)
		}
		if err := normalEncoder.Encode(list, normalBuffer); err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(streamingBuffer.String(), normalBuffer.String()); diff != "" {
			t.Logf("normal: %s", normalBuffer.String())
			t.Logf("streaming: %s", streamingBuffer.String())
			t.Fatalf("unexpected output:\n%s", diff)
		}
	}
}

// TestCallsToSize 验证流式列表编码对每个元素的 Size() 只调用一次，
// 因为 Size 可能很昂贵，重复调用会放大开销。
func TestCallsToSize(t *testing.T) {
	counter := &countingSizer{data: []byte("abba")}
	listMeta := metav1.ListMeta{}
	listData := streamingListData{
		totalSize:    14,
		listMeta:     listMeta,
		listMetaSize: listMeta.Size(),
		itemsSizes:   []int{counter.Size()},
		items:        []runtime.Object{counter},
	}
	err := streamingEncodeUnknownList(io.Discard, runtime.Unknown{}, listData, &runtime.Allocator{})
	if err != nil {
		t.Fatal(err)
	}
	if counter.count != 1 {
		t.Errorf("Expected only 1 call to sizer, got %d", counter.count)
	}
}

// countingSizer 是带 Size 调用计数的假 ProtobufMarshaller，用于验证 Size 的调用次数。
type countingSizer struct {
	data  []byte
	count int
}

// 编译期断言：countingSizer 必须实现 ProtobufMarshaller。
var _ runtime.ProtobufMarshaller = (*countingSizer)(nil)

// MarshalTo 把固定数据复制到目标缓冲。
func (s *countingSizer) MarshalTo(data []byte) (int, error) {
	return copy(data, s.data), nil
}

// Size 返回数据长度，同时累加调用次数。
func (s *countingSizer) Size() int {
	s.count++
	return len(s.data)
}

// DeepCopyObject 仅为满足 runtime.Object，测试中不使用。
func (s *countingSizer) DeepCopyObject() runtime.Object {
	return nil
}

// GetObjectKind 仅为满足 runtime.Object，测试中不使用。
func (s *countingSizer) GetObjectKind() schema.ObjectKind {
	return nil
}

// BenchmarkStreamEncodeProtobufCollections 基准测试流式编码一个含 1000 个元素的列表。
func BenchmarkStreamEncodeProtobufCollections(b *testing.B) {
	// FieldsV1 与 map 字段的随机填充对编码开销没有代表性，这里禁用/简化，
	// 让基准聚焦于集合本身的编码成本。
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

	encoder := NewSerializerWithOptions(nil, nil, SerializerOptions{StreamingCollectionsEncoding: true})
	b.Run("CarpList", func(b *testing.B) {
		var buf bytes.Buffer
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()
			if err := encoder.Encode(carpList, &buf); err != nil {
				b.Fatal(err)
			}
		}
	})
}
