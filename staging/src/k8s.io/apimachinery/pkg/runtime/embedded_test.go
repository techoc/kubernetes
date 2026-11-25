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

package runtime_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	runtimetesting "k8s.io/apimachinery/pkg/runtime/testing"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// TestDecodeEmptyRawExtensionAsObject 验证嵌入的 RawExtension 在解码时保留原始
// 字节：空对象解码为 "{}"，带 kind/apiVersion 的对象也原样保留，而不是被
// 提前解析或丢弃。
func TestDecodeEmptyRawExtensionAsObject(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "v1test"}
	externalGVK := externalGV.WithKind("ObjectTest")

	s := runtime.NewScheme()
	s.AddKnownTypes(internalGV, &runtimetesting.ObjectTest{})
	s.AddKnownTypeWithName(externalGVK, &runtimetesting.ObjectTestExternal{})
	utilruntime.Must(runtimetesting.RegisterConversions(s))

	codec := serializer.NewCodecFactory(s).LegacyCodec(externalGV)

	obj, gvk, err := codec.Decode([]byte(`{"kind":"`+externalGVK.Kind+`","apiVersion":"`+externalGV.String()+`","items":[{}]}`), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	test := obj.(*runtimetesting.ObjectTest)
	if unk, ok := test.Items[0].(*runtime.Unknown); !ok || unk.Kind != "" || unk.APIVersion != "" || string(unk.Raw) != "{}" || unk.ContentType != runtime.ContentTypeJSON {
		t.Fatalf("unexpected object: %#v", test.Items[0])
	}
	if *gvk != externalGVK {
		t.Fatalf("unexpected kind: %#v", gvk)
	}

	obj, gvk, err = codec.Decode([]byte(`{"kind":"`+externalGVK.Kind+`","apiVersion":"`+externalGV.String()+`","items":[{"kind":"Other","apiVersion":"v1"}]}`), nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	test = obj.(*runtimetesting.ObjectTest)
	if unk, ok := test.Items[0].(*runtime.Unknown); !ok || unk.Kind != "" || unk.APIVersion != "" || string(unk.Raw) != `{"kind":"Other","apiVersion":"v1"}` || unk.ContentType != runtime.ContentTypeJSON {
		t.Fatalf("unexpected object: %#v", test.Items[0])
	}
	if *gvk != externalGVK {
		t.Fatalf("unexpected kind: %#v", gvk)
	}
}

// TestArrayOfRuntimeObject 验证 []runtime.Object 经 EncodableList 编码后，
// 再通过 ExtractList/DecodeList 解码可完整还原，包括 Unknown 原始字节与嵌套列表。
func TestArrayOfRuntimeObject(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "v1test"}

	s := runtime.NewScheme()
	s.AddKnownTypes(internalGV, &runtimetesting.EmbeddedTest{})
	s.AddKnownTypeWithName(externalGV.WithKind("EmbeddedTest"), &runtimetesting.EmbeddedTestExternal{})
	s.AddKnownTypes(internalGV, &runtimetesting.ObjectTest{})
	s.AddKnownTypeWithName(externalGV.WithKind("ObjectTest"), &runtimetesting.ObjectTestExternal{})
	utilruntime.Must(runtimetesting.RegisterConversions(s))

	codec := serializer.NewCodecFactory(s).LegacyCodec(externalGV)

	innerItems := []runtime.Object{
		&runtimetesting.EmbeddedTest{ID: "baz"},
	}
	items := []runtime.Object{
		&runtimetesting.EmbeddedTest{ID: "foo"},
		&runtimetesting.EmbeddedTest{ID: "bar"},
		// TODO: 在 YAML 移除之前，这段 JSON 的键必须按升序排列，才能保证序列化往返一致。
		&runtime.Unknown{
			Raw:         []byte(`{"apiVersion":"unknown.group/unknown","foo":"bar","kind":"OtherTest"}`),
			ContentType: runtime.ContentTypeJSON,
		},
		&runtimetesting.ObjectTest{
			Items: runtime.NewEncodableList(codec, innerItems),
		},
	}
	internal := &runtimetesting.ObjectTest{
		Items: runtime.NewEncodableList(codec, items),
	}
	wire, err := runtime.Encode(codec, internal)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("Wire format is:\n%s\n", string(wire))

	obj := &runtimetesting.ObjectTestExternal{}
	if err := json.Unmarshal(wire, obj); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	t.Logf("exact wire is: %s", string(obj.Items[0].Raw))

	items[3] = &runtimetesting.ObjectTest{Items: innerItems}
	internal.Items = items

	decoded, err := runtime.Decode(codec, wire)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	list, err := meta.ExtractList(decoded)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if errs := runtime.DecodeList(list, codec); len(errs) > 0 {
		t.Fatalf("unexpected error: %v", errs)
	}

	list2, err := meta.ExtractList(list[3])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if errs := runtime.DecodeList(list2, codec); len(errs) > 0 {
		t.Fatalf("unexpected error: %v", errs)
	}
	if err := meta.SetList(list[3], list2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// DecodeList 应尽可能为 runtime.Unknown 对象补全 TypeMeta。
	internal.Items[2].(*runtime.Unknown).TypeMeta = runtime.TypeMeta{Kind: "OtherTest", APIVersion: "unknown.group/unknown"}
	if e, a := internal.Items, list; !reflect.DeepEqual(e, a) {
		t.Errorf("mismatched decoded: %s", diff.ObjectGoPrintSideBySide(e, a))
	}
}

// TestNestedObject 验证嵌套 runtime.Object 的编码/解码行为：内部对象先以
// Unknown 形式保留原始字节，需显式再解码；同时检查外部类型的 JSON 反序列化
// 保留原始字节，而内部类型无法直接通过 JSON 还原嵌套对象。
func TestNestedObject(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "v1test"}
	embeddedTestExternalGVK := externalGV.WithKind("EmbeddedTest")

	s := runtime.NewScheme()
	s.AddKnownTypes(internalGV, &runtimetesting.EmbeddedTest{})
	s.AddKnownTypeWithName(embeddedTestExternalGVK, &runtimetesting.EmbeddedTestExternal{})
	utilruntime.Must(runtimetesting.RegisterConversions(s))

	codec := serializer.NewCodecFactory(s).LegacyCodec(externalGV)

	inner := &runtimetesting.EmbeddedTest{
		ID: "inner",
	}
	outer := &runtimetesting.EmbeddedTest{
		ID:     "outer",
		Object: runtime.NewEncodable(codec, inner),
	}

	wire, err := runtime.Encode(codec, outer)
	if err != nil {
		t.Fatalf("Unexpected encode error '%v'", err)
	}

	t.Logf("Wire format is:\n%v\n", string(wire))

	decoded, err := runtime.Decode(codec, wire)
	if err != nil {
		t.Fatalf("Unexpected decode error %v", err)
	}

	// 为后续比较做准备：把编码前的对象也指向内部对象。
	outer.Object = inner

	if e, a := outer, decoded; reflect.DeepEqual(e, a) {
		t.Errorf("Expected unequal %#v %#v", e, a)
	}

	obj, err := runtime.Decode(codec, decoded.(*runtimetesting.EmbeddedTest).Object.(*runtime.Unknown).Raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded.(*runtimetesting.EmbeddedTest).Object = obj
	if e, a := outer, decoded; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected equal %#v %#v", e, a)
	}

	// 测试外部类型的 JSON 解码：应保留原始字节。
	var externalViaJSON runtimetesting.EmbeddedTestExternal
	err = json.Unmarshal(wire, &externalViaJSON)
	if err != nil {
		t.Fatalf("Unexpected decode error %v", err)
	}
	if externalViaJSON.Kind == "" || externalViaJSON.APIVersion == "" || externalViaJSON.ID != "outer" {
		t.Errorf("Expected objects to have type info set, got %#v", externalViaJSON)
	}
	if len(externalViaJSON.EmptyObject.Raw) > 0 {
		t.Errorf("Expected deserialization of empty nested objects into empty bytes, got %#v", externalViaJSON)
	}

	// 同时测试 JSON 解码，因为 Decode 使用 yaml 反序列化。
	// JSON 的通用反序列化无法加载嵌套对象，因为没有设置默认 schema。
	// 需要直接 JSON 解码的使用方必须使用外部表示。
	var decodedViaJSON runtimetesting.EmbeddedTest
	err = json.Unmarshal(wire, &decodedViaJSON)
	if err == nil {
		t.Fatal("Expeceted decode error")
	}
	if _, ok := err.(*json.UnmarshalTypeError); !ok {
		t.Fatalf("Unexpected decode error: %v", err)
	}
	if a := decodedViaJSON; a.Object != nil || a.EmptyObject != nil {
		t.Errorf("Expected embedded objects to be nil: %#v", a)
	}
}

// TestDeepCopyOfRuntimeObject 检查 runtime.Object 经过 DeepCopy 后仍能保真：
// 深拷贝与原对象深度相等，且编码结果一致。
func TestDeepCopyOfRuntimeObject(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "v1test"}
	embeddedTestExternalGVK := externalGV.WithKind("EmbeddedTest")

	s := runtime.NewScheme()
	s.AddKnownTypes(internalGV, &runtimetesting.EmbeddedTest{})
	s.AddKnownTypeWithName(embeddedTestExternalGVK, &runtimetesting.EmbeddedTestExternal{})
	utilruntime.Must(runtimetesting.RegisterConversions(s))

	original := &runtimetesting.EmbeddedTest{
		ID: "outer",
		Object: &runtimetesting.EmbeddedTest{
			ID: "inner",
		},
	}

	codec := serializer.NewCodecFactory(s).LegacyCodec(externalGV)

	originalData, err := runtime.Encode(codec, original)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	t.Logf("originalRole = %v\n", string(originalData))

	copyOfOriginal := original.DeepCopy()
	copiedData, err := runtime.Encode(codec, copyOfOriginal)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	t.Logf("copyOfRole   = %v\n", string(copiedData))

	if !reflect.DeepEqual(original, copyOfOriginal) {
		t.Errorf("expected \n%v\n, got \n%v", string(originalData), string(copiedData))
	}
}
