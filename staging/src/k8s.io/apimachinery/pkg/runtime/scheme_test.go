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
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	runtimetesting "k8s.io/apimachinery/pkg/runtime/testing"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// testConversions 统计转换调用次数，以便测试断言某条 codec 路径确实执行了转换，
// 而不是直接返回输入对象。
type testConversions struct {
	internalToExternalCalls int
	externalToInternalCalls int
}

func (c *testConversions) internalToExternalSimple(in *runtimetesting.InternalSimple, out *runtimetesting.ExternalSimple, scope conversion.Scope) error {
	out.TypeMeta = in.TypeMeta
	out.TestString = in.TestString
	c.internalToExternalCalls++
	return nil
}

func (c *testConversions) externalToInternalSimple(in *runtimetesting.ExternalSimple, out *runtimetesting.InternalSimple, scope conversion.Scope) error {
	out.TypeMeta = in.TypeMeta
	out.TestString = in.TestString
	c.externalToInternalCalls++
	return nil
}

// registerConversions 注册简单类型双向的转换函数，
// 以便将 scheme 作为 codec 进行测试。
func (c *testConversions) registerConversions(s *runtime.Scheme) error {
	if err := s.AddConversionFunc((*runtimetesting.InternalSimple)(nil), (*runtimetesting.ExternalSimple)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return c.internalToExternalSimple(a.(*runtimetesting.InternalSimple), b.(*runtimetesting.ExternalSimple), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*runtimetesting.ExternalSimple)(nil), (*runtimetesting.InternalSimple)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return c.externalToInternalSimple(a.(*runtimetesting.ExternalSimple), b.(*runtimetesting.InternalSimple), scope)
	}); err != nil {
		return err
	}
	return nil
}

// TestScheme 覆盖 scheme/codec 的核心往返流程：编码后再解码回内部类型、直接解码为外部类型、
// 与 unstructured 之间的相互转换，以及 unstructured 对象必须同时携带 kind 和 version 的要求。
func TestScheme(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	internalGVK := internalGV.WithKind("Simple")
	externalGV := schema.GroupVersion{Group: "test.group", Version: "testExternal"}
	externalGVK := externalGV.WithKind("Simple")

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(internalGVK, &runtimetesting.InternalSimple{})
	scheme.AddKnownTypeWithName(externalGVK, &runtimetesting.ExternalSimple{})
	utilruntime.Must(runtimetesting.RegisterConversions(scheme))

	// 若启用，则在转换期间清除 TypeMeta。
	//scheme.AddIgnoredConversionType(&TypeMeta{}, &TypeMeta{})

	// 验证 scheme 实现了 ObjectTyper 接口
	var _ runtime.ObjectTyper = scheme

	conversions := &testConversions{
		internalToExternalCalls: 0,
		externalToInternalCalls: 0,
	}

	// 注册函数，验证 scope.Meta() 是否被正确设置。
	utilruntime.Must(conversions.registerConversions(scheme))

	t.Run("Encode, Decode, DecodeInto, and DecodeToVersion", func(t *testing.T) {
		simple := &runtimetesting.InternalSimple{
			TestString: "foo",
		}

		codecs := serializer.NewCodecFactory(scheme)
		codec := codecs.LegacyCodec(externalGV)
		info, _ := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
		jsonserializer := info.Serializer

		obj := runtime.Object(simple)
		data, err := runtime.Encode(codec, obj)
		if err != nil {
			t.Fatal(err)
		}

		obj2, err := runtime.Decode(codec, data)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := obj2.(*runtimetesting.InternalSimple); !ok {
			t.Fatalf("Got wrong type")
		}
		if e, a := simple, obj2; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected:\n %#v,\n Got:\n %#v", e, a)
		}

		obj3 := &runtimetesting.InternalSimple{}
		if err := runtime.DecodeInto(codec, data, obj3); err != nil {
			t.Fatal(err)
		}
		// 清除 TypeMeta 是 scheme 的职责，此处不测试该行为（ConvertToVersion
		// 不再自动清除 TypeMeta）。
		simple.TypeMeta = runtime.TypeMeta{Kind: "Simple", APIVersion: externalGV.String()}
		if e, a := simple, obj3; !reflect.DeepEqual(e, a) {
			t.Errorf("Expected:\n %#v,\n Got:\n %#v", e, a)
		}

		obj4, err := runtime.Decode(jsonserializer, data)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := obj4.(*runtimetesting.ExternalSimple); !ok {
			t.Fatalf("Got wrong type")
		}
	})
	t.Run("Convert", func(t *testing.T) {
		simple := &runtimetesting.InternalSimple{
			TestString: "foo",
		}

		external := &runtimetesting.ExternalSimple{}
		if err := scheme.Convert(simple, external, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if e, a := simple.TestString, external.TestString; e != a {
			t.Errorf("Expected %q, got %q", e, a)
		}
	})
	t.Run("Convert internal to unstructured", func(t *testing.T) {
		simple := &runtimetesting.InternalSimple{
			TestString: "foo",
		}

		unstructuredObj := &runtimetesting.Unstructured{}
		err := scheme.Convert(simple, unstructuredObj, nil)
		if err == nil || !strings.Contains(err.Error(), "to Unstructured without providing a preferred version to convert to") {
			t.Fatalf("Unexpected non-error: %v", err)
		}
		if err := scheme.Convert(simple, unstructuredObj, externalGV); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if e, a := simple.TestString, unstructuredObj.Object["testString"].(string); e != a {
			t.Errorf("Expected %q, got %q", e, a)
		}
		if e := unstructuredObj.GetObjectKind().GroupVersionKind(); e != externalGVK {
			t.Errorf("Unexpected object kind: %#v", e)
		}
		if gvks, unversioned, err := scheme.ObjectKinds(unstructuredObj); err != nil || gvks[0] != externalGVK || unversioned {
			t.Errorf("Scheme did not recognize unversioned: %v, %#v %t", err, gvks, unversioned)
		}
	})
	t.Run("Convert external to unstructured", func(t *testing.T) {
		unstructuredObj := &runtimetesting.Unstructured{}
		external := &runtimetesting.ExternalSimple{
			TestString: "foo",
		}

		if err := scheme.Convert(external, unstructuredObj, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if e, a := external.TestString, unstructuredObj.Object["testString"].(string); e != a {
			t.Errorf("Expected %q, got %q", e, a)
		}
		if e := unstructuredObj.GetObjectKind().GroupVersionKind(); e != externalGVK {
			t.Errorf("Unexpected object kind: %#v", e)
		}
	})
	t.Run("Convert unstructured to unstructured", func(t *testing.T) {
		uIn := &runtimetesting.Unstructured{Object: map[string]interface{}{
			"test": []interface{}{"other", "test"},
		}}
		uOut := &runtimetesting.Unstructured{}
		if err := scheme.Convert(uIn, uOut, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !reflect.DeepEqual(uIn.Object, uOut.Object) {
			t.Errorf("Unexpected object contents: %#v", uOut.Object)
		}
	})
	t.Run("Convert unstructured to structured", func(t *testing.T) {
		unstructuredObj := &runtimetesting.Unstructured{
			Object: map[string]interface{}{
				"testString": "bla",
			},
		}
		unstructuredObj.SetGroupVersionKind(externalGV.WithKind("Simple"))
		externalOut := &runtimetesting.ExternalSimple{}
		if err := scheme.Convert(unstructuredObj, externalOut, nil); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if externalOut.TestString != "bla" {
			t.Errorf("Unexpected object contents: %#v", externalOut)
		}
	})
	t.Run("Encode and Convert should each have caused an increment", func(t *testing.T) {
		if e, a := 3, conversions.internalToExternalCalls; e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
	})
	t.Run("DecodeInto and Decode should each have caused an increment because of a conversion", func(t *testing.T) {
		if e, a := 2, conversions.externalToInternalCalls; e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
	})
	t.Run("Verify that unstructured types must have V and K set", func(t *testing.T) {
		emptyObj := &runtimetesting.Unstructured{Object: make(map[string]interface{})}
		if _, _, err := scheme.ObjectKinds(emptyObj); !runtime.IsMissingKind(err) {
			t.Errorf("unexpected error: %v", err)
		}
		emptyObj.SetGroupVersionKind(schema.GroupVersionKind{Kind: "Test"})
		if _, _, err := scheme.ObjectKinds(emptyObj); !runtime.IsMissingVersion(err) {
			t.Errorf("unexpected error: %v", err)
		}
		emptyObj.SetGroupVersionKind(schema.GroupVersionKind{Kind: "Test", Version: "v1"})
		if _, _, err := scheme.ObjectKinds(emptyObj); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// TestBadJSONRejection 验证 JSON 序列化器会拒绝缺少 kind 或指定未注册 kind 的文档，
// 而不会随意解码成某个类型。
func TestBadJSONRejection(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	info, _ := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	jsonserializer := info.Serializer

	badJSONMissingKind := []byte(`{ }`)
	if _, err := runtime.Decode(jsonserializer, badJSONMissingKind); err == nil {
		t.Errorf("Did not reject despite lack of kind field: %s", badJSONMissingKind)
	}
	badJSONUnknownType := []byte(`{"kind": "bar"}`)
	if _, err1 := runtime.Decode(jsonserializer, badJSONUnknownType); err1 == nil {
		t.Errorf("Did not reject despite use of unknown type: %s", badJSONUnknownType)
	}
}

// TestExternalToInternalMapping 验证序列化后的外部类型会被解码为对应的已注册内部类型，
// 且两者比较结果相等。
func TestExternalToInternalMapping(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "testExternal"}

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(internalGV.WithKind("OptionalExtensionType"), &runtimetesting.InternalOptionalExtensionType{})
	scheme.AddKnownTypeWithName(externalGV.WithKind("OptionalExtensionType"), &runtimetesting.ExternalOptionalExtensionType{})
	utilruntime.Must(runtimetesting.RegisterConversions(scheme))

	codec := serializer.NewCodecFactory(scheme).LegacyCodec(externalGV)

	table := []struct {
		obj     runtime.Object
		encoded string
	}{
		{
			&runtimetesting.InternalOptionalExtensionType{Extension: nil},
			`{"kind":"OptionalExtensionType","apiVersion":"` + externalGV.String() + `"}`,
		},
	}

	for i, item := range table {
		gotDecoded, err := runtime.Decode(codec, []byte(item.encoded))
		if err != nil {
			t.Errorf("unexpected error '%v' (%v)", err, item.encoded)
		} else if e, a := item.obj, gotDecoded; !reflect.DeepEqual(e, a) {
			t.Errorf("%d: unexpected objects:\n%s", i, diff.ObjectGoPrintSideBySide(e, a))
		}
	}
}

// TestExtensionMapping 验证嵌入的扩展对象会携带自身的 apiVersion/kind 进行编码，
// 并在解码后还原为 runtime.Unknown。
func TestExtensionMapping(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "testExternal"}

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(internalGV.WithKind("ExtensionType"), &runtimetesting.InternalExtensionType{})
	scheme.AddKnownTypeWithName(internalGV.WithKind("OptionalExtensionType"), &runtimetesting.InternalOptionalExtensionType{})
	scheme.AddKnownTypeWithName(externalGV.WithKind("ExtensionType"), &runtimetesting.ExternalExtensionType{})
	scheme.AddKnownTypeWithName(externalGV.WithKind("OptionalExtensionType"), &runtimetesting.ExternalOptionalExtensionType{})

	// 当对象在两个 scheme 中相同时，先注册外部版本，这样 ObjectVersionAndKind 会返回
	// 外部版本。
	scheme.AddKnownTypeWithName(externalGV.WithKind("A"), &runtimetesting.ExtensionA{})
	scheme.AddKnownTypeWithName(externalGV.WithKind("B"), &runtimetesting.ExtensionB{})
	scheme.AddKnownTypeWithName(internalGV.WithKind("A"), &runtimetesting.ExtensionA{})
	scheme.AddKnownTypeWithName(internalGV.WithKind("B"), &runtimetesting.ExtensionB{})
	utilruntime.Must(runtimetesting.RegisterConversions(scheme))

	codec := serializer.NewCodecFactory(scheme).LegacyCodec(externalGV)

	table := []struct {
		obj      runtime.Object
		expected runtime.Object
		encoded  string
	}{
		{
			&runtimetesting.InternalExtensionType{
				Extension: runtime.NewEncodable(codec, &runtimetesting.ExtensionA{TestString: "foo"}),
			},
			&runtimetesting.InternalExtensionType{
				Extension: &runtime.Unknown{
					Raw:         []byte(`{"apiVersion":"test.group/testExternal","kind":"A","testString":"foo"}`),
					ContentType: runtime.ContentTypeJSON,
				},
			},
			// 在序列化对象中设置 apiVersion，便于客户端使用
			`{"apiVersion":"` + externalGV.String() + `","kind":"ExtensionType","extension":{"apiVersion":"test.group/testExternal","kind":"A","testString":"foo"}}
`,
		}, {
			&runtimetesting.InternalExtensionType{Extension: runtime.NewEncodable(codec, &runtimetesting.ExtensionB{TestString: "bar"})},
			&runtimetesting.InternalExtensionType{
				Extension: &runtime.Unknown{
					Raw:         []byte(`{"apiVersion":"test.group/testExternal","kind":"B","testString":"bar"}`),
					ContentType: runtime.ContentTypeJSON,
				},
			},
			// 在序列化对象中设置 apiVersion，便于客户端使用
			`{"apiVersion":"` + externalGV.String() + `","kind":"ExtensionType","extension":{"apiVersion":"test.group/testExternal","kind":"B","testString":"bar"}}
`,
		}, {
			&runtimetesting.InternalExtensionType{Extension: nil},
			&runtimetesting.InternalExtensionType{
				Extension: nil,
			},
			`{"apiVersion":"` + externalGV.String() + `","kind":"ExtensionType","extension":null}
`,
		},
	}

	for i, item := range table {
		gotEncoded, err := runtime.Encode(codec, item.obj)
		if err != nil {
			t.Errorf("unexpected error '%v' (%#v)", err, item.obj)
		} else if e, a := item.encoded, string(gotEncoded); e != a {
			t.Errorf("expected\n%#v\ngot\n%#v\n", e, a)
		}

		gotDecoded, err := runtime.Decode(codec, []byte(item.encoded))
		if err != nil {
			t.Errorf("unexpected error '%v' (%v)", err, item.encoded)
		} else if e, a := item.expected, gotDecoded; !reflect.DeepEqual(e, a) {
			t.Errorf("%d: unexpected objects:\n%s", i, diff.ObjectGoPrintSideBySide(e, a))
		}
	}
}

// TestEncode 验证通过带版本的 codec 编码内部对象后，所得数据可解码回同一个内部对象，
// 且返回的 GVK 是目标外部版本。
func TestEncode(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	internalGVK := internalGV.WithKind("Simple")
	externalGV := schema.GroupVersion{Group: "test.group", Version: "testExternal"}
	externalGVK := externalGV.WithKind("Simple")

	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(internalGVK, &runtimetesting.InternalSimple{})
	scheme.AddKnownTypeWithName(externalGVK, &runtimetesting.ExternalSimple{})
	utilruntime.Must(runtimetesting.RegisterConversions(scheme))

	codec := serializer.NewCodecFactory(scheme).LegacyCodec(externalGV)

	test := &runtimetesting.InternalSimple{
		TestString: "I'm the same",
	}
	obj := runtime.Object(test)
	data, err := runtime.Encode(codec, obj)
	obj2, gvk, err2 := codec.Decode(data, nil, nil)
	if err != nil || err2 != nil {
		t.Fatalf("Failure: '%v' '%v'", err, err2)
	}
	if _, ok := obj2.(*runtimetesting.InternalSimple); !ok {
		t.Fatalf("Got wrong type")
	}
	if !reflect.DeepEqual(obj2, test) {
		t.Errorf("Expected:\n %#v,\n Got:\n %#v", test, obj2)
	}
	if *gvk != externalGVK {
		t.Errorf("unexpected gvk returned by decode: %#v", *gvk)
	}
}

// TestUnversionedTypes 验证注册为无版本的类型即使 codec 指向另一个 group，
// 也会保留其声明时的 group/version；无版本对象永远不会被执行转换。
func TestUnversionedTypes(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	internalGVK := internalGV.WithKind("Simple")
	externalGV := schema.GroupVersion{Group: "test.group", Version: "testExternal"}
	externalGVK := externalGV.WithKind("Simple")
	otherGV := schema.GroupVersion{Group: "group", Version: "other"}

	scheme := runtime.NewScheme()
	scheme.AddUnversionedTypes(externalGV, &runtimetesting.InternalSimple{})
	scheme.AddKnownTypeWithName(internalGVK, &runtimetesting.InternalSimple{})
	scheme.AddKnownTypeWithName(externalGVK, &runtimetesting.ExternalSimple{})
	scheme.AddKnownTypeWithName(otherGV.WithKind("Simple"), &runtimetesting.ExternalSimple{})
	utilruntime.Must(runtimetesting.RegisterConversions(scheme))

	codec := serializer.NewCodecFactory(scheme).LegacyCodec(externalGV)

	if unv, ok := scheme.IsUnversioned(&runtimetesting.InternalSimple{}); !unv || !ok {
		t.Fatalf("type not unversioned and in scheme: %t %t", unv, ok)
	}

	kinds, _, err := scheme.ObjectKinds(&runtimetesting.InternalSimple{})
	if err != nil {
		t.Fatal(err)
	}
	kind := kinds[0]
	if kind != externalGV.WithKind("InternalSimple") {
		t.Fatalf("unexpected: %#v", kind)
	}

	test := &runtimetesting.InternalSimple{
		TestString: "I'm the same",
	}
	obj := runtime.Object(test)
	data, err := runtime.Encode(codec, obj)
	if err != nil {
		t.Fatal(err)
	}
	obj2, gvk, err := codec.Decode(data, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := obj2.(*runtimetesting.InternalSimple); !ok {
		t.Fatalf("Got wrong type")
	}
	if !reflect.DeepEqual(obj2, test) {
		t.Errorf("Expected:\n %#v,\n Got:\n %#v", test, obj2)
	}
	// 对象被序列化为无版本对象（使用其定义时所属的 group 和 version）
	if *gvk != externalGV.WithKind("InternalSimple") {
		t.Errorf("unexpected gvk returned by decode: %#v", *gvk)
	}

	// 序列化到不同的 group 时，对象仍保留其首选名称
	codec = serializer.NewCodecFactory(scheme).LegacyCodec(otherGV)
	data, err = runtime.Encode(codec, obj)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"apiVersion":"test.group/testExternal","kind":"InternalSimple","testString":"I'm the same"}`+"\n" {
		t.Errorf("unexpected data: %s", data)
	}
}

// 返回一个配置好测试对象的新 Scheme。
// 这些注册刻意让同一个 Go 类型对应多个 group/version/kind 组合，
// 以检验歧义处理逻辑。
func GetTestScheme() *runtime.Scheme {
	internalGV := schema.GroupVersion{Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Version: "v1"}
	alternateExternalGV := schema.GroupVersion{Group: "custom", Version: "v1"}
	alternateInternalGV := schema.GroupVersion{Group: "custom", Version: runtime.APIVersionInternal}
	differentExternalGV := schema.GroupVersion{Group: "other", Version: "v2"}

	s := runtime.NewScheme()
	// 通常情况下不会添加 TestType2，但这是测试代码，且两个类型来自同一个包，
	// 因此需要将其注册进系统，以便转换器能与 ExternalType2 匹配。
	s.AddKnownTypes(internalGV, &runtimetesting.TestType1{}, &runtimetesting.TestType2{}, &runtimetesting.ExternalInternalSame{})
	s.AddKnownTypes(externalGV, &runtimetesting.ExternalInternalSame{})
	s.AddKnownTypeWithName(externalGV.WithKind("TestType1"), &runtimetesting.ExternalTestType1{})
	s.AddKnownTypeWithName(externalGV.WithKind("TestType2"), &runtimetesting.ExternalTestType2{})
	s.AddKnownTypeWithName(internalGV.WithKind("TestType3"), &runtimetesting.TestType1{})
	s.AddKnownTypeWithName(externalGV.WithKind("TestType3"), &runtimetesting.ExternalTestType1{})
	s.AddKnownTypeWithName(externalGV.WithKind("TestType4"), &runtimetesting.ExternalTestType1{})
	s.AddKnownTypeWithName(alternateInternalGV.WithKind("TestType3"), &runtimetesting.TestType1{})
	s.AddKnownTypeWithName(alternateExternalGV.WithKind("TestType3"), &runtimetesting.ExternalTestType1{})
	s.AddKnownTypeWithName(alternateExternalGV.WithKind("TestType5"), &runtimetesting.ExternalTestType1{})
	s.AddKnownTypeWithName(differentExternalGV.WithKind("TestType1"), &runtimetesting.ExternalTestType1{})
	s.AddUnversionedTypes(externalGV, &runtimetesting.UnversionedType{})
	utilruntime.Must(runtimetesting.RegisterConversions(s))

	return s
}

// TestKnownTypes 验证 KnownTypes 只返回某个 group/version 下已注册的类型，
// 对未注册的 group/version 不返回任何类型。
func TestKnownTypes(t *testing.T) {
	s := GetTestScheme()
	if len(s.KnownTypes(schema.GroupVersion{Group: "group", Version: "v2"})) != 0 {
		t.Errorf("should have no known types for v2")
	}

	types := s.KnownTypes(schema.GroupVersion{Version: "v1"})
	for _, s := range []string{"TestType1", "TestType2", "TestType3", "ExternalInternalSame"} {
		if _, ok := types[s]; !ok {
			t.Errorf("missing type %q", s)
		}
	}
}

// TestAddKnownTypesIdemPotent 验证通过任意注册入口重复注册同一类型时，
// 只会保留一条记录，不会出现重复或 panic。这一点很重要，因为初始化路径可能会执行两次。
func TestAddKnownTypesIdemPotent(t *testing.T) {
	s := runtime.NewScheme()

	gv := schema.GroupVersion{Group: "foo", Version: "v1"}
	s.AddKnownTypes(gv, &runtimetesting.InternalSimple{})
	s.AddKnownTypes(gv, &runtimetesting.InternalSimple{})
	if len(s.KnownTypes(gv)) != 1 {
		t.Errorf("expected only one %v type after double registration", gv)
	}
	if len(s.AllKnownTypes()) != 1 {
		t.Errorf("expected only one type after double registration")
	}

	s.AddKnownTypeWithName(gv.WithKind("InternalSimple"), &runtimetesting.InternalSimple{})
	s.AddKnownTypeWithName(gv.WithKind("InternalSimple"), &runtimetesting.InternalSimple{})
	if len(s.KnownTypes(gv)) != 1 {
		t.Errorf("expected only one %v type after double registration with custom name", gv)
	}
	if len(s.AllKnownTypes()) != 1 {
		t.Errorf("expected only one type after double registration with custom name")
	}

	s.AddUnversionedTypes(gv, &runtimetesting.InternalSimple{})
	s.AddUnversionedTypes(gv, &runtimetesting.InternalSimple{})
	if len(s.KnownTypes(gv)) != 1 {
		t.Errorf("expected only one %v type after double registration with custom name", gv)
	}
	if len(s.AllKnownTypes()) != 1 {
		t.Errorf("expected only one type after double registration with custom name")
	}

	kinds, _, err := s.ObjectKinds(&runtimetesting.InternalSimple{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(kinds) != 1 {
		t.Errorf("expected only one kind for InternalSimple after double registration")
	}
}

// 重新定义一个同名的 InternalSimple，但显然与 runtimetesting 中的类型是不同类型
type InternalSimple struct {
	runtime.TypeMeta `json:""`
	TestString       string `json:"testString"`
}

func (s *InternalSimple) DeepCopyObject() runtime.Object { return nil }

// TestConflictingAddKnownTypes 验证将两种不同的 Go 类型注册到同一个 kind 下会触发 panic：
// 静默接受会导致解码结果不确定。
func TestConflictingAddKnownTypes(t *testing.T) {
	s := runtime.NewScheme()
	gv := schema.GroupVersion{Group: "foo", Version: "v1"}

	panicked := make(chan bool)
	go func() {
		defer func() {
			if recover() != nil {
				panicked <- true
			}
		}()
		s.AddKnownTypeWithName(gv.WithKind("InternalSimple"), &runtimetesting.InternalSimple{})
		s.AddKnownTypeWithName(gv.WithKind("InternalSimple"), &runtimetesting.ExternalSimple{})
		panicked <- false
	}()
	if !<-panicked {
		t.Errorf("Expected AddKnownTypesWithName to panic with conflicting type registrations")
	}

	go func() {
		defer func() {
			if recover() != nil {
				panicked <- true
			}
		}()

		s.AddUnversionedTypes(gv, &runtimetesting.InternalSimple{})
		s.AddUnversionedTypes(gv, &InternalSimple{})
		panicked <- false
	}()
	if !<-panicked {
		t.Errorf("Expected AddUnversionedTypes to panic with conflicting type registrations")
	}
}

// TestConvertToVersionBasic 检查通过 scheme 进行的最简单的内部到外部类型转换。
func TestConvertToVersionBasic(t *testing.T) {
	s := GetTestScheme()
	tt := &runtimetesting.TestType1{A: "I'm not a pointer object"}
	other, err := s.ConvertToVersion(tt, schema.GroupVersion{Version: "v1"})
	if err != nil {
		t.Fatalf("Failure: %v", err)
	}
	converted, ok := other.(*runtimetesting.ExternalTestType1)
	if !ok {
		t.Fatalf("Got wrong type: %T", other)
	}
	if tt.A != converted.A {
		t.Fatalf("Failed to convert object correctly: %#v", converted)
	}
}

// testGroupVersioner 是一个带有固定目标和成功标志的 GroupVersioner，
// 用于以可控的方式驱动转换决策。
type testGroupVersioner struct {
	target schema.GroupVersionKind
	ok     bool
}

func (m testGroupVersioner) KindForGroupVersionKinds(kinds []schema.GroupVersionKind) (schema.GroupVersionKind, bool) {
	return m.target, m.ok
}

func (m testGroupVersioner) Identifier() string {
	return "testGroupVersioner"
}

// TestConvertToVersion 覆盖版本定向转换：转换到内部和外部版本、未注册/unstructured 输入、
// 多 group 目标，以及 ConvertToVersion 绝不返回输入对象也绝不修改它的不变式。
// same: true 的用例还会额外断言：当无需拷贝时，UnsafeConvertToVersion 会返回原始对象。
func TestConvertToVersion(t *testing.T) {
	testCases := []struct {
		scheme *runtime.Scheme
		in     runtime.Object
		gv     runtime.GroupVersioner
		same   bool
		out    runtime.Object
		errFn  func(error) bool
	}{
		// 类型未在 scheme 中注册时报错
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.UnknownType{},
			errFn:  func(err error) bool { return err != nil && runtime.IsNotRegisteredError(err) },
		},
		// group versioner 未返回目标时报错
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     testGroupVersioner{},
			errFn: func(err error) bool {
				return err != nil && strings.Contains(err.Error(), "is not suitable for converting")
			},
		},
		// 转换到内部版本
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     schema.GroupVersion{Version: runtime.APIVersionInternal},
			out:    &runtimetesting.TestType1{A: "test"},
		},
		// 从 unstructured 转换到内部版本
		{
			scheme: GetTestScheme(),
			in: &runtimetesting.Unstructured{Object: map[string]interface{}{
				"apiVersion": "custom/v1",
				"kind":       "TestType3",
				"A":          "test",
			}},
			gv:  schema.GroupVersion{Version: runtime.APIVersionInternal},
			out: &runtimetesting.TestType1{A: "test"},
		},
		// 从 unstructured 转换到外部版本
		{
			scheme: GetTestScheme(),
			in: &runtimetesting.Unstructured{Object: map[string]interface{}{
				"apiVersion": "custom/v1",
				"kind":       "TestType3",
				"A":          "test",
			}},
			gv:  schema.GroupVersion{Group: "custom", Version: "v1"},
			out: &runtimetesting.ExternalTestType1{MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "custom/v1", ObjectKind: "TestType3"}, A: "test"},
		},
		// 优先选择最佳匹配
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     schema.GroupVersions{{Version: runtime.APIVersionInternal}, {Version: "v1"}},
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// 无版本类型原样返回
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.UnversionedType{A: "test"},
			gv:     schema.GroupVersions{{Version: "v1"}},
			same:   true,
			out: &runtimetesting.UnversionedType{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "UnversionedType"},
				A:                                     "test",
			},
		},
		// 目标类型列表中不包含时，仍返回无版本类型
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.UnversionedType{A: "test"},
			gv:     schema.GroupVersions{{Group: "other", Version: "v2"}},
			same:   true,
			out: &runtimetesting.UnversionedType{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "UnversionedType"},
				A:                                     "test",
			},
		},
		// 检测到已经处于目标版本
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     schema.GroupVersions{{Version: "v1"}},
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// 检测到已经处于第一个目标版本
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     schema.GroupVersions{{Version: "v1"}, {Version: runtime.APIVersionInternal}},
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// 检测到已经处于第一个目标版本
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     schema.GroupVersions{{Version: "v1"}, {Version: runtime.APIVersionInternal}},
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// 外部类型注册在多个 group、version 和 kind 下，且可定向到其中任意一个（1/3）：kind 不同
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     testGroupVersioner{ok: true, target: schema.GroupVersionKind{Kind: "TestType3", Version: "v1"}},
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType3"},
				A:                                     "test",
			},
		},
		// 外部类型注册在多个 group、version 和 kind 下，且可定向到其中任意一个（2/3）：gv 不同
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     testGroupVersioner{ok: true, target: schema.GroupVersionKind{Kind: "TestType3", Group: "custom", Version: "v1"}},
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "custom/v1", ObjectKind: "TestType3"},
				A:                                     "test",
			},
		},
		// 外部类型注册在多个 group、version 和 kind 下，且可定向到其中任意一个（3/3）：gvk 不同
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     testGroupVersioner{ok: true, target: schema.GroupVersionKind{Group: "custom", Version: "v1", Kind: "TestType5"}},
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "custom/v1", ObjectKind: "TestType5"},
				A:                                     "test",
			},
		},
		// multi group versioner 可识别多个 group 并强制输出到特定版本；因版本不同而执行拷贝
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     runtime.NewMultiGroupVersioner(schema.GroupVersion{Group: "other", Version: "v2"}, schema.GroupKind{Group: "custom", Kind: "TestType3"}, schema.GroupKind{Kind: "TestType1"}),
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "other/v2", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// multi group versioner 可识别多个 group 并强制输出到特定版本；因版本不同而执行拷贝
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     runtime.NewMultiGroupVersioner(schema.GroupVersion{Group: "other", Version: "v2"}, schema.GroupKind{Kind: "TestType1"}, schema.GroupKind{Group: "custom", Kind: "TestType3"}),
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "other/v2", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// 当 kind 和 group 都不匹配时，multi group versioner 无法找到匹配项（group "other" 中没有 TestType1 kind，默认 group 中也没有 "TestType5" kind）
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.TestType1{A: "test"},
			gv:     runtime.NewMultiGroupVersioner(schema.GroupVersion{Group: "custom", Version: "v1"}, schema.GroupKind{Group: "other"}, schema.GroupKind{Kind: "TestType5"}),
			errFn: func(err error) bool {
				return err != nil && strings.Contains(err.Error(), "is not suitable for converting")
			},
		},
		// multi group versioner 可识别多个 group 并强制输出到特定版本；无需拷贝
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     runtime.NewMultiGroupVersioner(schema.GroupVersion{Group: "", Version: "v1"}, schema.GroupKind{Group: "custom", Kind: "TestType3"}, schema.GroupKind{Kind: "TestType1"}),
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// multi group versioner 可识别多个 group 并强制输出到特定版本；无需拷贝
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.ExternalTestType1{A: "test"},
			gv:     runtime.NewMultiGroupVersioner(schema.GroupVersion{Group: "", Version: "v1"}, schema.GroupKind{Kind: "TestType1"}, schema.GroupKind{Group: "custom", Kind: "TestType3"}),
			same:   true,
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType1"},
				A:                                     "test",
			},
		},
		// 当各 group/version 下 kind 相同时，group versioner 可为给定输入选择特定的目标 kind
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.TestType1{A: "test"},
			gv:     testGroupVersioner{ok: true, target: schema.GroupVersionKind{Version: "v1", Kind: "TestType3"}},
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "v1", ObjectKind: "TestType3"},
				A:                                     "test",
			},
		},
		// group versioner 可以选择不同的 kind
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.TestType1{A: "test"},
			gv:     testGroupVersioner{ok: true, target: schema.GroupVersionKind{Kind: "TestType5", Group: "custom", Version: "v1"}},
			out: &runtimetesting.ExternalTestType1{
				MyWeirdCustomEmbeddedVersionKindField: runtimetesting.MyWeirdCustomEmbeddedVersionKindField{APIVersion: "custom/v1", ObjectKind: "TestType5"},
				A:                                     "test",
			},
		},
	}
	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			original := test.in.DeepCopyObject()
			out, err := test.scheme.ConvertToVersion(test.in, test.gv)
			switch {
			case test.errFn != nil:
				if !test.errFn(err) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
			}
			if out == test.in {
				t.Fatalf("ConvertToVersion should always copy out: %#v", out)
			}

			if test.same {
				if !reflect.DeepEqual(original, test.in) {
					t.Fatalf("unexpected mutation of input: %s", cmp.Diff(original, test.in))
				}
				if !reflect.DeepEqual(out, test.out) {
					t.Fatalf("unexpected out: %s", cmp.Diff(out, test.out))
				}
				unsafe, err := test.scheme.UnsafeConvertToVersion(test.in, test.gv)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !reflect.DeepEqual(unsafe, test.out) {
					t.Fatalf("unexpected unsafe: %s", cmp.Diff(unsafe, test.out))
				}
				if unsafe != test.in {
					t.Fatalf("UnsafeConvertToVersion should return same object: %#v", unsafe)
				}
				return
			}
			if !reflect.DeepEqual(out, test.out) {
				t.Fatalf("unexpected out: %s", cmp.Diff(out, test.out))
			}
		})
	}
}

// TestConvert 覆盖显式的转换到目标对象（into）的方式，包括在给定目标版本的情况下
// 将内部对象转换为 unstructured。
func TestConvert(t *testing.T) {
	testCases := []struct {
		scheme *runtime.Scheme
		in     runtime.Object
		into   runtime.Object
		gv     runtime.GroupVersioner
		out    runtime.Object
		errFn  func(error) bool
	}{
		// 给定目标版本，从内部类型转换到 unstructured
		{
			scheme: GetTestScheme(),
			in:     &runtimetesting.TestType1{A: "test"},
			into:   &runtimetesting.Unstructured{},
			out: &runtimetesting.Unstructured{Object: map[string]interface{}{
				"myVersionKey": "custom/v1",
				"myKindKey":    "TestType3",
				"A":            "test",
			}},
			gv: schema.GroupVersion{Group: "custom", Version: "v1"},
		},
	}
	for i, test := range testCases {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			err := test.scheme.Convert(test.in, test.into, test.gv)
			switch {
			case test.errFn != nil:
				if !test.errFn(err) {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			case err != nil:
				t.Fatalf("unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(test.into, test.out) {
				t.Fatalf("unexpected out: %s", cmp.Diff(test.into, test.out))
			}
		})
	}
}

// TestMetaValues 验证转换回调收到正确的 scope，
// 且 internal->external->internal 的往返过程会保留该值。
func TestMetaValues(t *testing.T) {
	internalGV := schema.GroupVersion{Group: "test.group", Version: runtime.APIVersionInternal}
	externalGV := schema.GroupVersion{Group: "test.group", Version: "externalVersion"}

	s := runtime.NewScheme()
	s.AddKnownTypeWithName(internalGV.WithKind("Simple"), &runtimetesting.InternalSimple{})
	s.AddKnownTypeWithName(externalGV.WithKind("Simple"), &runtimetesting.ExternalSimple{})
	utilruntime.Must(runtimetesting.RegisterConversions(s))

	conversions := &testConversions{
		internalToExternalCalls: 0,
		externalToInternalCalls: 0,
	}

	// 注册函数，验证 scope.Meta() 是否被正确设置。
	utilruntime.Must(conversions.registerConversions(s))

	simple := &runtimetesting.InternalSimple{
		TestString: "foo",
	}

	out, err := s.ConvertToVersion(simple, externalGV)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	internal, err := s.ConvertToVersion(out, internalGV)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if e, a := simple, internal; !reflect.DeepEqual(e, a) {
		t.Errorf("Expected:\n %#v,\n Got:\n %#v", e, a)
	}

	if e, a := 1, conversions.internalToExternalCalls; e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
	if e, a := 1, conversions.externalToInternalCalls; e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
}

// TestMetaValuesUnregisteredConvert 验证为未注册到 scheme 中的类型注册的转换函数，
// 在显式调用时仍会执行。
func TestMetaValuesUnregisteredConvert(t *testing.T) {
	type InternalSimple struct {
		Version    string `json:"apiVersion,omitempty"`
		Kind       string `json:"kind,omitempty"`
		TestString string `json:"testString"`
	}
	type ExternalSimple struct {
		Version    string `json:"apiVersion,omitempty"`
		Kind       string `json:"kind,omitempty"`
		TestString string `json:"testString"`
	}
	s := runtime.NewScheme()
	// 此处刻意不注册这些类型。

	internalToExternalCalls := 0

	// 注册函数，验证 scope.Meta() 是否被正确设置。
	convertSimple := func(in *InternalSimple, out *ExternalSimple, scope conversion.Scope) error {
		out.TestString = in.TestString
		internalToExternalCalls++
		return nil
	}
	if err := s.AddConversionFunc((*InternalSimple)(nil), (*ExternalSimple)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return convertSimple(a.(*InternalSimple), b.(*ExternalSimple), scope)
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	simple := &InternalSimple{TestString: "foo"}
	external := &ExternalSimple{}
	if err := s.Convert(simple, external, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if e, a := simple.TestString, external.TestString; e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}

	// 验证转换处理函数确实被调用了。
	if e, a := 1, internalToExternalCalls; e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
}

// TestRegisterValidate 验证 scheme 在创建和更新时都会分发到按类型注册的校验函数，
// 并且子资源和 options 会被传递给这些函数。
func TestRegisterValidate(t *testing.T) {
	invalidValue := field.Invalid(field.NewPath("testString"), "", "Invalid value").WithOrigin("invalid-value")
	invalidLength := field.Invalid(field.NewPath("testString"), "", "Invalid length").WithOrigin("invalid-length")
	invalidStatusErr := field.Invalid(field.NewPath("testString"), "", "Invalid condition").WithOrigin("invalid-condition")
	invalidIfOptionErr := field.Invalid(field.NewPath("testString"), "", "Invalid when option is set").WithOrigin("invalid-when-option-set")

	testCases := []struct {
		name        string
		object      runtime.Object
		oldObject   runtime.Object
		subresource []string
		options     map[string]bool
		expected    field.ErrorList
	}{
		{
			name:     "single error",
			object:   &TestType1{},
			options:  map[string]bool{"option1": false},
			expected: field.ErrorList{invalidValue},
		},
		{
			name:     "multiple errors",
			object:   &TestType2{},
			expected: field.ErrorList{invalidValue, invalidLength},
		},
		{
			name:      "update error",
			object:    &TestType2{},
			oldObject: &TestType2{},
			expected:  field.ErrorList{invalidLength},
		},
		{
			name:     "options error",
			object:   &TestType1{},
			options:  map[string]bool{"option1": true},
			expected: field.ErrorList{invalidIfOptionErr},
		},
		{
			name:        "subresource error",
			object:      &TestType1{},
			subresource: []string{"status"},
			options:     map[string]bool{"option1": false},
			expected:    field.ErrorList{invalidStatusErr},
		},
	}

	s := runtime.NewScheme()
	ctx := context.Background()

	// 注册多个类型进行测试，确保注册行为符合预期
	s.AddValidationFunc(&TestType1{}, func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList {
		if enabled, exists := op.HasOption("option1"); enabled || !exists {
			return field.ErrorList{invalidIfOptionErr}
		}
		if slices.Equal(op.Request.Subresources, []string{"status"}) {
			return field.ErrorList{invalidStatusErr}
		}
		return field.ErrorList{invalidValue}
	})

	s.AddValidationFunc(&TestType2{}, func(ctx context.Context, op operation.Operation, object, oldObject interface{}) field.ErrorList {
		if oldObject != nil {
			return field.ErrorList{invalidLength}
		}
		return field.ErrorList{invalidValue, invalidLength}
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var results field.ErrorList
			if tc.oldObject == nil {
				results = s.Validate(ctx, tc.options, tc.object, tc.subresource...)
			} else {
				results = s.ValidateUpdate(ctx, tc.options, tc.object, tc.oldObject, tc.subresource...)
			}
			matcher := field.ErrorMatcher{}.ByType().ByField().ByOrigin()
			matcher.Test(t, tc.expected, results)
		})
	}
}

// TestType1 是仅由 TestRegisterValidate 使用的最小 runtime.Object。
type TestType1 struct {
	Version    string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	TestString string `json:"testString"`
}

func (TestType1) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (TestType1) DeepCopyObject() runtime.Object { return nil }

// TestType2 是另一个仅由 TestRegisterValidate 使用的最小 runtime.Object，
// 用于检验多个校验器的注册。
type TestType2 struct {
	Version    string `json:"apiVersion,omitempty"`
	Kind       string `json:"kind,omitempty"`
	TestString string `json:"testString"`
}

func (TestType2) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

func (TestType2) DeepCopyObject() runtime.Object { return nil }

// TestToOpenAPIDefinitionName 验证为 unstructured GVK 合成的名称是确定性的，
// 以及 GVK 缺少某部分时返回的错误。
// 该名称用于作为 OpenAPI schema 的键，因此必须保持稳定。
func TestToOpenAPIDefinitionName(t *testing.T) {
	testCases := []struct {
		name        string
		registerGvk *schema.GroupVersionKind // 未设置时默认为 gvk
		registerObj runtime.Object
		gvk         schema.GroupVersionKind
		out         string
		wantErr     error
	}{
		{
			name:        "unstructured type",
			registerObj: &unstructured.Unstructured{},
			gvk:         schema.GroupVersionKind{Group: "stable.example.com", Version: "v1", Kind: "CronTab"},
			out:         "com.example.stable.v1.CronTab",
		},
		{
			name:        "unregistered type: empty group",
			registerObj: &unstructured.Unstructured{},
			gvk:         schema.GroupVersionKind{Version: "v1", Kind: "CronTab"},
			wantErr:     fmt.Errorf("unable to convert GroupVersionKind with empty fields to unstructured type to an OpenAPI definition name: %v", schema.GroupVersionKind{Version: "v1", Kind: "CronTab"}),
		},
		{
			name:        "unregistered type: empty version",
			registerObj: &unstructured.Unstructured{},
			registerGvk: &schema.GroupVersionKind{Group: "stable.example.com", Version: "v1", Kind: "CronTab"},
			gvk:         schema.GroupVersionKind{Group: "stable.example.com", Kind: "CronTab"},
			wantErr:     fmt.Errorf("version is required on all types: %v", schema.GroupVersionKind{Group: "stable.example.com", Kind: "CronTab"}),
		},
		{
			name:        "unregistered type: empty kind",
			registerObj: &unstructured.Unstructured{},
			gvk:         schema.GroupVersionKind{Group: "stable.example.com", Version: "v1"},
			wantErr:     fmt.Errorf("unable to convert GroupVersionKind with empty fields to unstructured type to an OpenAPI definition name: %v", schema.GroupVersionKind{Group: "stable.example.com", Version: "v1"}),
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			if test.registerGvk == nil {
				test.registerGvk = &test.gvk
			}

			scheme := runtime.NewScheme()
			scheme.AddKnownTypeWithName(*test.registerGvk, test.registerObj)
			utilruntime.Must(runtimetesting.RegisterConversions(scheme))

			out, err := scheme.ToOpenAPIDefinitionName(test.gvk)
			if test.wantErr != nil {
				if err == nil || err.Error() != test.wantErr.Error() {
					t.Errorf("expected error: %v but got %v", test.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if out != test.out {
				t.Errorf("expected %s, got %s", test.out, out)
			}
		})
	}
}
