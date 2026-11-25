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

// Package roundtrip 提供通用的 API 类型往返（round-trip）测试：随机填充对象后经
// 序列化/反序列化，断言对象语义保持不变。各 API 组只需在自己的 install 包中调用这里的
// 入口函数，即可覆盖 internal → external → wire → external → internal 的完整链路。
package roundtrip

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	flag "github.com/spf13/pflag"
	"sigs.k8s.io/randfill"

	apitesting "k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/dump"
)

// InstallFunc 把某个 API 组的类型注册进 scheme，等价于该组 install 包的 Install。
type InstallFunc func(scheme *runtime.Scheme)

// RoundTripTestForAPIGroup 方便在 install 包中调用，用来确认仅安装本组类型（不含服务端聚合的
// 其它组）时，scheme 已包含往返测试所需的全部信息。
func RoundTripTestForAPIGroup(t *testing.T, installFn InstallFunc, fuzzingFuncs fuzzer.FuzzerFuncs) {
	scheme := runtime.NewScheme()
	installFn(scheme)

	RoundTripTestForScheme(t, scheme, fuzzingFuncs)
}

// RoundTripTestForScheme 适用于调用方已经持有 scheme 的场景，用来确认其中注册的编解码信息自洽。
func RoundTripTestForScheme(t *testing.T, scheme *runtime.Scheme, fuzzingFuncs fuzzer.FuzzerFuncs) {
	codecFactory := runtimeserializer.NewCodecFactory(scheme)
	// 合并 meta 组的 fuzzer 与调用方自定义的 fuzzer：前者负责 ObjectMeta 等公共字段，
	// 后者补齐本组特有的不可往返字段或约束（如枚举、必填项）。
	f := fuzzer.FuzzerFor(
		fuzzer.MergeFuzzerFuncs(metafuzzer.Funcs, fuzzingFuncs),
		rand.NewSource(rand.Int63()),
		codecFactory,
	)
	RoundTripTypesWithoutProtobuf(t, scheme, codecFactory, f, nil)
}

// RoundTripProtobufTestForAPIGroup 与 RoundTripTestForAPIGroup 相同，但额外覆盖 protobuf 序列化。
func RoundTripProtobufTestForAPIGroup(t *testing.T, installFn InstallFunc, fuzzingFuncs fuzzer.FuzzerFuncs) {
	scheme := runtime.NewScheme()
	installFn(scheme)

	RoundTripProtobufTestForScheme(t, scheme, fuzzingFuncs)
}

// RoundTripProtobufTestForScheme 与 RoundTripTestForScheme 相同，但额外覆盖 protobuf 序列化。
func RoundTripProtobufTestForScheme(t *testing.T, scheme *runtime.Scheme, fuzzingFuncs fuzzer.FuzzerFuncs) {
	codecFactory := runtimeserializer.NewCodecFactory(scheme)
	fuzzer := fuzzer.FuzzerFor(
		fuzzer.MergeFuzzerFuncs(metafuzzer.Funcs, fuzzingFuncs),
		rand.NewSource(rand.Int63()),
		codecFactory,
	)
	RoundTripTypes(t, scheme, codecFactory, fuzzer, nil)
}

// FuzzIters 是每个 Kind 的模糊测试迭代次数。随机输入可能偶发触发不可重现的失败，
// 因此做成可配置 flag，便于复跑或增大采样以复现问题。
var FuzzIters = flag.Int("fuzz-iters", defaultFuzzIters, "How many fuzzing iterations to do.")

// globalNonRoundTrippableTypes 是在所有 GroupVersion 中都实际保留、因而无法往返的 Kind 集合。
// 它们不参与往返测试。
var globalNonRoundTrippableTypes = sets.NewString(
	"ExportOptions",
	"GetOptions",
	// WatchEvent 不含 kind 和 version，只能由调用方在已知目标类型时隐式反序列化。
	// watch 请求通过 content type 而非每个对象内嵌的 kind/version 来定义 schema。
	"WatchEvent",
	// ListOptions 现已属于 meta 组。
	"ListOptions",
	// DeleteOptions 只在 metav1 中被读取。
	"DeleteOptions",
)

// GlobalNonRoundTrippableTypes 返回在所有 GroupVersion 中实际保留、无法往返的 Kind 集合。
// 它们无法往返，因此可以被任意自定义/下游的往返测试排除。
//
//	kinds := scheme.AllKnownTypes()
//	for gvk := range kinds {
//	    if roundtrip.GlobalNonRoundTrippableTypes().Has(gvk.Kind) {
//	        continue
//	    }
//	    t.Run(gvk.Group+"."+gvk.Version+"."+gvk.Kind, func(t *testing.T) {
//	        // 往返测试。
//	    })
//	}
func GlobalNonRoundTrippableTypes() sets.String {
	return sets.NewString(globalNonRoundTrippableTypes.List()...)
}

// RoundTripTypesWithoutProtobuf 对 scheme 中所有可往返的 Kind 执行往返测试，
// 跳过 skip 列表中的 GroupVersionKind，且不覆盖 protobuf 序列化。
func RoundTripTypesWithoutProtobuf(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool) {
	roundTripTypes(t, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, true)
}

// RoundTripTypes 对 scheme 中所有可往返的 Kind 执行往返测试，包含 protobuf 序列化。
func RoundTripTypes(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool) {
	roundTripTypes(t, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, false)
}

func roundTripTypes(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool, skipProtobuf bool) {
	// 以组为单位遍历，仅测试 internal 版本；external 版本会作为转换目标在内部测试中一并覆盖。
	for _, group := range groupsFromScheme(scheme) {
		t.Logf("starting group %q", group)
		internalVersion := schema.GroupVersion{Group: group, Version: runtime.APIVersionInternal}
		internalKindToGoType := scheme.KnownTypes(internalVersion)

		for kind := range internalKindToGoType {
			// 全局不可往返的 Kind 在任何组里都跳过。
			if globalNonRoundTrippableTypes.Has(kind) {
				continue
			}

			internalGVK := internalVersion.WithKind(kind)
			roundTripSpecificKind(t, internalGVK, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, skipProtobuf)
		}

		t.Logf("finished group %q", group)
	}
}

// RoundTripExternalTypes 对 scheme 中所有外部（非 internal）可往返的 Kind 执行往返测试，
// 跳过 nonRoundTripExternalTypes 列表中的 GroupVersionKind。
func RoundTripExternalTypes(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool) {
	kinds := scheme.AllKnownTypes()
	for gvk := range kinds {
		// internal 版本由 RoundTripTypes 系列覆盖，这里只看外部版本。
		if gvk.Version == runtime.APIVersionInternal || globalNonRoundTrippableTypes.Has(gvk.Kind) {
			continue
		}
		t.Run(gvk.Group+"."+gvk.Version+"."+gvk.Kind, func(t *testing.T) {
			roundTripSpecificKind(t, gvk, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, false)
		})
	}
}

// RoundTripExternalTypesWithoutProtobuf 与 RoundTripExternalTypes 相同，但不覆盖 protobuf 序列化。
func RoundTripExternalTypesWithoutProtobuf(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool) {
	kinds := scheme.AllKnownTypes()
	for gvk := range kinds {
		if gvk.Version == runtime.APIVersionInternal || globalNonRoundTrippableTypes.Has(gvk.Kind) {
			continue
		}
		t.Run(gvk.Group+"."+gvk.Version+"."+gvk.Kind, func(t *testing.T) {
			roundTripSpecificKind(t, gvk, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, true)
		})
	}
}

// RoundTripSpecificKindWithoutProtobuf 只对指定的单个 Kind 执行往返测试，不覆盖 protobuf。
func RoundTripSpecificKindWithoutProtobuf(t *testing.T, gvk schema.GroupVersionKind, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool) {
	roundTripSpecificKind(t, gvk, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, true)
}

// RoundTripSpecificKind 只对指定的单个 Kind 执行往返测试，包含 protobuf。
func RoundTripSpecificKind(t *testing.T, gvk schema.GroupVersionKind, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool) {
	roundTripSpecificKind(t, gvk, scheme, codecFactory, fuzzer, nonRoundTrippableTypes, false)
}

func roundTripSpecificKind(t *testing.T, gvk schema.GroupVersionKind, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, nonRoundTrippableTypes map[schema.GroupVersionKind]bool, skipProtobuf bool) {
	if nonRoundTrippableTypes[gvk] {
		t.Logf("skipping %v", gvk)
		return
	}

	// 反复执行若干次，因为 runTest 使用的随机值每次不同，单次通过不代表字段覆盖充分。
	for i := 0; i < *FuzzIters; i++ {
		if gvk.Version == runtime.APIVersionInternal {
			// internal 类型需要先转换到每个已注册的外部版本再序列化。
			roundTripToAllExternalVersions(t, scheme, codecFactory, fuzzer, gvk, nonRoundTrippableTypes, skipProtobuf)
		} else {
			roundTripOfExternalType(t, scheme, codecFactory, fuzzer, gvk, skipProtobuf)
		}
		// 一旦失败即停止，避免同一失败重复刷屏，也节省快速失败的时间。
		if t.Failed() {
			break
		}
	}
}

// fuzzInternalObject 使用 apitesting 包中注册的 fuzzer 随机填充任意 runtime 对象。
// 随后清空 TypeMeta，确保测试依赖 scheme 中的类型信息而非对象自带的 kind/version。
func fuzzInternalObject(t *testing.T, fuzzer *randfill.Filler, object runtime.Object) runtime.Object {
	fuzzer.Fill(object)

	j, err := apimeta.TypeAccessor(object)
	if err != nil {
		t.Fatalf("Unexpected error %v for %#v", err, object)
	}
	// 清除 TypeMeta：往返链路应完全由 scheme 推导 kind/version，避免对象自带信息掩盖注册缺失。
	j.SetKind("")
	j.SetAPIVersion("")

	return object
}

// groupsFromScheme 返回 scheme 中注册过的全部 API 组（去重后按字典序排列，保证测试可重复）。
func groupsFromScheme(scheme *runtime.Scheme) []string {
	ret := sets.String{}
	for gvk := range scheme.AllKnownTypes() {
		ret.Insert(gvk.Group)
	}
	return ret.List()
}

// roundTripToAllExternalVersions 取一个 internal 对象，对同 GroupKind 下的每个外部版本分别
// 转换并序列化，验证所有版本间的转换与编解码都无损。
func roundTripToAllExternalVersions(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, internalGVK schema.GroupVersionKind, nonRoundTrippableTypes map[schema.GroupVersionKind]bool, skipProtobuf bool) {
	object, err := scheme.New(internalGVK)
	if err != nil {
		t.Fatalf("Couldn't make a %v? %v", internalGVK, err)
	}
	// 只有带 TypeMeta 的类型才能参与编解码往返；否则报错并提示加入跳过列表。
	if _, err := apimeta.TypeAccessor(object); err != nil {
		t.Fatalf("%q is not a TypeMeta and cannot be tested - add it to nonRoundTrippableInternalTypes: %v", internalGVK, err)
	}

	fuzzInternalObject(t, fuzzer, object)

	// 找出 scheme 中所有可能的序列化版本。
	// TODO: 支持以不同名称交叉注册的 Kind。
	for externalGVK, externalGoType := range scheme.AllKnownTypes() {
		if externalGVK.Version == runtime.APIVersionInternal {
			continue
		}
		// 只处理同 GroupKind 的外部版本，保证存在合法的转换路径。
		if externalGVK.GroupKind() != internalGVK.GroupKind() {
			continue
		}
		if nonRoundTrippableTypes[externalGVK] {
			t.Logf("\tskipping  %v %v", externalGVK, externalGoType)
			continue
		}
		t.Logf("\tround tripping to %v %v", externalGVK, externalGoType)

		roundTrip(t, scheme, apitesting.TestCodec(codecFactory, externalGVK.GroupVersion()), object)

		// TODO: 过渡期结束后移除此特例。
		// kubeadm 的类型暂不支持 protobuf，作为过渡期特例排除。
		if !skipProtobuf && externalGVK.Group != "kubeadm.k8s.io" {
			s := protobuf.NewSerializer(scheme, scheme)
			protobufCodec := codecFactory.CodecForVersions(s, s, externalGVK.GroupVersion(), nil)
			roundTrip(t, scheme, protobufCodec, object)
		}
	}
}

// roundTripOfExternalType 对单个外部类型直接做 json（以及可选 protobuf）编解码往返，
// 不涉及 internal 转换，用于校验外部类型自身的序列化契约。
func roundTripOfExternalType(t *testing.T, scheme *runtime.Scheme, codecFactory runtimeserializer.CodecFactory, fuzzer *randfill.Filler, externalGVK schema.GroupVersionKind, skipProtobuf bool) {
	object, err := scheme.New(externalGVK)
	if err != nil {
		t.Fatalf("Couldn't make a %v? %v", externalGVK, err)
	}
	typeAcc, err := apimeta.TypeAccessor(object)
	if err != nil {
		t.Fatalf("%q is not a TypeMeta and cannot be tested - add it to nonRoundTrippableInternalTypes: %v", externalGVK, err)
	}

	fuzzInternalObject(t, fuzzer, object)

	// 外部类型的往返依赖对象内嵌的 kind/version，这里显式写回，模拟从 wire 读到的形态。
	typeAcc.SetKind(externalGVK.Kind)
	typeAcc.SetAPIVersion(externalGVK.GroupVersion().String())

	roundTrip(t, scheme, json.NewSerializerWithOptions(json.DefaultMetaFactory, scheme, scheme, json.SerializerOptions{}), object)

	// TODO: 过渡期结束后移除此特例。
	if !skipProtobuf {
		roundTrip(t, scheme, protobuf.NewSerializer(scheme, scheme), object)
	}
}

// roundTrip 使用给定 codec 对 runtime 对象执行一次往返测试，确保对象可以被深拷贝、转换、
// 序列化并反序列化而不丢失数据。
//
// 对于 internal 类型，链路为
//
//	internal -> external -> json/protobuf -> external -> internal。
//
// 对于 external 类型，链路为
//
//	external -> json/protobuf -> external。
func roundTrip(t *testing.T, scheme *runtime.Scheme, codec runtime.Codec, object runtime.Object) {
	original := object

	// 深拷贝原始对象
	object = object.DeepCopyObject()
	name := reflect.TypeOf(object).Elem().Name()
	if !apiequality.Semantic.DeepEqual(original, object) {
		t.Errorf("%v: DeepCopy altered the object, diff: %v", name, cmp.Diff(original, object))
		t.Errorf("%s", dump.Pretty(original))
		t.Errorf("%s", dump.Pretty(object))
		return
	}

	// 用给定的 codec 编码（序列化）深拷贝
	data, err := runtime.Encode(codec, object)
	if err != nil {
		if runtime.IsNotRegisteredError(err) {
			t.Logf("%v: not registered: %v (%s)", name, err, dump.Pretty(object))
		} else {
			t.Errorf("%v: %v (%s)", name, err, dump.Pretty(object))
		}
		return
	}

	// 确认深拷贝仍与原对象相等：编码（以及其中隐含的转换）都不应改写对象。
	// TODO: 消除此全局变量。
	if !apiequality.Semantic.DeepEqual(original, object) {
		t.Errorf("%v: encode altered the object, diff: %v", name, cmp.Diff(original, object))
		return
	}

	// 再编码一次，确认输出稳定。
	secondData, err := runtime.Encode(codec, object)
	if err != nil {
		if runtime.IsNotRegisteredError(err) {
			t.Logf("%v: not registered: %v (%s)", name, err, dump.Pretty(object))
		} else {
			t.Errorf("%v: %v (%s)", name, err, dump.Pretty(object))
		}
		return
	}

	// 序列化到 wire 的结果必须稳定，否则对象未变时也会重复写 DB（见 etcd 的 resourceVersion 语义）。
	if !bytes.Equal(data, secondData) {
		t.Errorf("%v: serialization is not stable: %s", name, dump.Pretty(object))
	}

	// 将编码后的数据解码（反序列化）回对象
	obj2, err := runtime.Decode(codec, data)
	if err != nil {
		t.Errorf("%v: %v\nCodec: %#v\nData: %s\nSource: %s", name, err, codec, dataAsString(data), dump.Pretty(object))
		panic("failed")
	}

	// 确认解码得到的对象与原对象相等
	if !apiequality.Semantic.DeepEqual(original, obj2) {
		t.Errorf("%v: diff: %v\nCodec: %#v\nSource:\n\n%s\n\nEncoded:\n\n%s\n\nFinal:\n\n%s", name, cmp.Diff(original, obj2), codec, dump.Pretty(original), dataAsString(data), dump.Pretty(obj2))
		return
	}

	// 把编码数据解码进一个新建对象（而不是让 codec 自己创建对象），
	// 验证调用方预分配对象的解码路径同样正确。
	obj3 := reflect.New(reflect.TypeOf(object).Elem()).Interface().(runtime.Object)
	if err := runtime.DecodeInto(codec, data, obj3); err != nil {
		t.Errorf("%v: %v", name, err)
		return
	}

	// 特例：有些 Kind 同时是 internal 和 external（meta.k8s.io 中很常见）。对这些类型，
	// 上面的 runtime.DecodeInto 会返回 external 变体并设置 APIVersion 和 kind，
	// 而输入对象可能是 internal。此时清空 obj3 的 kind/version 以便正确比较。
	intAndExt, err := internalAndExternalKind(scheme, object)
	if err != nil {
		t.Errorf("%v: %v", name, err)
		return
	}
	if intAndExt {
		typeAcc, err := apimeta.TypeAccessor(object)
		if err != nil {
			t.Fatalf("%v: error accessing TypeMeta: %v", name, err)
		}
		// 仅当输入对象本身没有 APIVersion 时才清空，避免误伤 external 输入携带的元信息。
		if len(typeAcc.GetAPIVersion()) == 0 {
			typeAcc, err := apimeta.TypeAccessor(obj3)
			if err != nil {
				t.Fatalf("%v: error accessing TypeMeta: %v", name, err)
			}
			typeAcc.SetAPIVersion("")
			typeAcc.SetKind("")
		}
	}

	// 确认 DecodeInto 得到的新对象在比较前仍与原对象相等
	if !apiequality.Semantic.DeepEqual(object, obj3) {
		t.Errorf("%v: diff: %v\nCodec: %#v", name, cmp.Diff(object, obj3), codec)
		return
	}

	// 对深拷贝对象做保结构（structure-preserving）的 fuzzing。如果它与原对象共享底层数据，
	// 说明所谓“深拷贝”其实是浅拷贝，fuzzing 后 original 与 obj3 就会出现差异。
	// 注意：这里用“编码+解码”作为另一条可保证深拷贝的路径来对照。
	fuzzer.ValueFuzz(object)
	if !apiequality.Semantic.DeepEqual(original, obj3) {
		t.Errorf("%v: fuzzing a copy altered the original, diff: %v", name, cmp.Diff(original, obj3))
		return
	}
}

// internalAndExternalKind 判断对象在 scheme 中是否同时注册为 internal 和外部版本。
// 只有这种“双身份”类型才需要 roundTrip 中的 TypeMeta 清理特例。
func internalAndExternalKind(scheme *runtime.Scheme, object runtime.Object) (bool, error) {
	kinds, _, err := scheme.ObjectKinds(object)
	if err != nil {
		return false, err
	}
	internal, external := false, false
	for _, k := range kinds {
		if k.Version == runtime.APIVersionInternal {
			internal = true
		} else {
			external = true
		}
	}
	return internal && external, nil
}

// dataAsString 把字节数组转成可读字符串；检测到非 JSON 数据时按十六进制转储，
// 便于排查 protobuf 等二进制格式的差异。
func dataAsString(data []byte) string {
	dataString := string(data)
	if !strings.HasPrefix(dataString, "{") {
		dataString = "\n" + hex.Dump(data)
	}
	return dataString
}
