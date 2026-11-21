/*
Copyright 2015 The Kubernetes Authors.

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
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	_ "k8s.io/kubernetes/pkg/apis/extensions"
	_ "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
)

// TestUniversalDeserializer 验证通用反序列化器能正确处理所有支持的数据格式。
// 对每种 media type 先编码一个 Pod 再解码，确认跨格式结果一致。
func TestUniversalDeserializer(t *testing.T) {
	expected := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}, TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}}
	d := legacyscheme.Codecs.UniversalDeserializer()
	// 覆盖 JSON/YAML/Protobuf 三种对外数据格式，避免某种格式的解码路径回归。
	for _, mediaType := range []string{"application/json", "application/yaml", "application/vnd.kubernetes.protobuf"} {
		info, ok := runtime.SerializerInfoForMediaType(legacyscheme.Codecs.SupportedMediaTypes(), mediaType)
		if !ok {
			t.Fatal(mediaType)
		}
		buf := &bytes.Buffer{}
		if err := info.Serializer.Encode(expected, buf); err != nil {
			t.Fatalf("%s: %v", mediaType, err)
		}
		obj, _, err := d.Decode(buf.Bytes(), &schema.GroupVersionKind{Kind: "Pod", Version: "v1"}, nil)
		if err != nil {
			t.Fatalf("%s: %v", mediaType, err)
		}
		if !apiequality.Semantic.DeepEqual(expected, obj) {
			t.Fatalf("%s: %#v", mediaType, obj)
		}
	}
}

// TestAllFieldsHaveTags 检查所有会做 protobuf 序列化的外部类型是否带有 protobuf tag。
// 漏加 tag 的字段不会进入 protobuf 字节流，会造成跨版本数据丢失。
func TestAllFieldsHaveTags(t *testing.T) {
	for gvk, obj := range legacyscheme.Scheme.AllKnownTypes() {
		if gvk.Version == runtime.APIVersionInternal {
			// internal 版本不序列化为 protobuf
			continue
		}
		if gvk.Group == "componentconfig" {
			// component config 不序列化为 protobuf
			continue
		}
		if err := fieldsHaveProtobufTags(obj); err != nil {
			t.Errorf("type %s as gvk %v is missing tags: %v", obj, gvk, err)
		}
	}
}

// fieldsHaveProtobufTags 递归检查类型（含切片、map、指针的元素类型）的字段 tag。
func fieldsHaveProtobufTags(obj reflect.Type) error {
	switch obj.Kind() {
	case reflect.Slice, reflect.Map, reflect.Pointer, reflect.Array:
		// 容器类型本身没有字段，真正需要检查的是其元素类型。
		return fieldsHaveProtobufTags(obj.Elem())
	case reflect.Struct:
		for i := 0; i < obj.NumField(); i++ {
			f := obj.Field(i)
			if f.Name == "TypeMeta" && f.Type.Name() == "TypeMeta" {
				// 外部 protobuf 使用带 TypeMeta 的 envelope 类型，因此字段本身无需 tag。
				continue
			}
			_, jsonTagExists := f.Tag.Lookup("json")
			// 有 json tag 却没有 protobuf tag，说明该字段会在 protobuf 编码中静默丢失。
			if jsonTagExists && len(f.Tag.Get("protobuf")) == 0 {
				return fmt.Errorf("field %s in %s has a 'json' tag but no protobuf tag", f.Name, obj)
			}
		}
	}
	return nil
}

// TestProtobufRoundTrip 验证随机的 v1.Pod 经生成的 Marshal/Unmarshal 后保持不变。
func TestProtobufRoundTrip(t *testing.T) {
	obj := &v1.Pod{}
	fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(benchmarkSeed), legacyscheme.Codecs).Fill(obj)
	// InitContainers 在转换中会变成注解，这里置空以避免往返比较出现假阳性。
	obj.Spec.InitContainers = nil
	obj.Status.InitContainerStatuses = nil
	data, err := obj.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	out := &v1.Pod{}
	if err := out.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	// 不相等时打印十六进制转储，方便定位是哪个字段的编码出了问题。
	if !apiequality.Semantic.Equalities.DeepEqual(out, obj) {
		t.Logf("marshal\n%s", hex.Dump(data))
		t.Fatalf("Unmarshal is unequal\n%s", cmp.Diff(out, obj))
	}
}

// BenchmarkEncodeCodecProtobuf 测量 protobuf codec 编码开销，包含清空 APIVersion/Kind 的反射操作。
func BenchmarkEncodeCodecProtobuf(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	s := protobuf.NewSerializer(nil, nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.Encode(s, &items[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeCodecFromInternalProtobuf 测量从 internal 对象到 protobuf 字节流的“完整”编码开销，
// 包含版本转换与类型设置。
func BenchmarkEncodeCodecFromInternalProtobuf(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	encodable := make([]api.Pod, width)
	for i := range items {
		if err := legacyscheme.Scheme.Convert(&items[i], &encodable[i], nil); err != nil {
			b.Fatal(err)
		}
	}
	s := protobuf.NewSerializer(nil, nil)
	codec := legacyscheme.Codecs.EncoderForVersion(s, v1.SchemeGroupVersion)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.Encode(codec, &encodable[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeProtobufGeneratedMarshal 测量生成的 Marshal 方法本身的性能，不含 codec 的额外开销。
func BenchmarkEncodeProtobufGeneratedMarshal(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := items[i%width].Marshal(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeProtobufGeneratedMarshalList10 测量含 10 个元素的 PodList 的生成序列化开销。
func BenchmarkEncodeProtobufGeneratedMarshalList10(b *testing.B) {
	item := benchmarkItemsList(b, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := item.Marshal(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeProtobufGeneratedMarshalList100 测量含 100 个元素的 PodList 的生成序列化开销。
func BenchmarkEncodeProtobufGeneratedMarshalList100(b *testing.B) {
	item := benchmarkItemsList(b, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := item.Marshal(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeProtobufGeneratedMarshalList1000 测量含 1000 个元素的 PodList 的生成序列化开销。
func BenchmarkEncodeProtobufGeneratedMarshalList1000(b *testing.B) {
	item := benchmarkItemsList(b, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := item.Marshal(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeCodecToInternalProtobuf 测量从 protobuf 字节流解码到 internal 对象的“完整”解码开销，
// 包含转换与类型设置；编码数据预先生成，不计入耗时。
func BenchmarkDecodeCodecToInternalProtobuf(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	s := protobuf.NewSerializer(legacyscheme.Scheme, legacyscheme.Scheme)
	encoder := legacyscheme.Codecs.EncoderForVersion(s, v1.SchemeGroupVersion)
	var encoded [][]byte
	for i := range items {
		data, err := runtime.Encode(encoder, &items[i])
		if err != nil {
			b.Fatal(err)
		}
		encoded = append(encoded, data)
	}

	decoder := legacyscheme.Codecs.DecoderToVersion(s, api.SchemeGroupVersion)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.Decode(decoder, encoded[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeIntoProtobuf 测量生成的 Unmarshal 方法本身的性能，作为普通 protobuf 解码的基线。
func BenchmarkDecodeIntoProtobuf(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	encoded := make([][]byte, width)
	for i := range items {
		data, err := (&items[i]).Marshal()
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = data
		validate := &v1.Pod{}
		validate.Reset() // decode normally resets internally
		if err := validate.Unmarshal(data); err != nil {
			b.Fatalf("Failed to unmarshal %d: %v\n%#v", i, err, items[i])
		}
	}

	for i := 0; i < b.N; i++ {
		obj := &v1.Pod{}
		obj.Reset() // decode normally resets internally
		if err := obj.Unmarshal(encoded[i%width]); err != nil {
			b.Fatal(err)
		}
	}
}
