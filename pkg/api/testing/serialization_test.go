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

package testing

import (
	"bytes"
	"encoding/hex"
	gojson "encoding/json"
	"io"
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	k8s_apps_v1 "k8s.io/kubernetes/pkg/apis/apps/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"sigs.k8s.io/yaml"
)

// fuzzInternalObject 使用 apitesting 包注册的 fuzzer 对任意 runtime 对象做随机填充。
func fuzzInternalObject(t *testing.T, forVersion schema.GroupVersion, item runtime.Object, seed int64) runtime.Object {
	fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(seed), legacyscheme.Codecs).Fill(item)

	j, err := meta.TypeAccessor(item)
	if err != nil {
		t.Fatalf("Unexpected error %v for %#v", err, item)
	}
	// 清空类型元信息：codec 会在编码时自行填充，保留随机值会干扰比较。
	j.SetKind("")
	j.SetAPIVersion("")

	return item
}

// ConvertV1ReplicaSetToAPIReplicationController 经由 internal 类型把 apps/v1 ReplicaSet 转成 core.ReplicationController。
// 分两步走是为了复用已有的 apps -> core 转换逻辑，而不是另写一份直接转换。
func ConvertV1ReplicaSetToAPIReplicationController(in *appsv1.ReplicaSet, out *api.ReplicationController, s conversion.Scope) error {
	intermediate1 := &apps.ReplicaSet{}
	if err := k8s_apps_v1.Convert_v1_ReplicaSet_To_apps_ReplicaSet(in, intermediate1, s); err != nil {
		return err
	}

	intermediate2 := &v1.ReplicationController{}
	if err := k8s_api_v1.Convert_apps_ReplicaSet_To_v1_ReplicationController(intermediate1, intermediate2, s); err != nil {
		return err
	}

	return k8s_api_v1.Convert_v1_ReplicationController_To_core_ReplicationController(intermediate2, out, s)
}

// TestSetControllerConversion 验证 ReplicaSet 与 ReplicationController 之间的多步转换链路。
// 重点是 selector 从 label selector 表示转换为旧式 selector 表示后仍能正确编码解码。
func TestSetControllerConversion(t *testing.T) {
	s := legacyscheme.Scheme
	// 注册自定义转换，使解码器能在两种资源的 v1 表示之间搭建桥梁。
	if err := s.AddConversionFunc((*appsv1.ReplicaSet)(nil), (*api.ReplicationController)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return ConvertV1ReplicaSetToAPIReplicationController(a.(*appsv1.ReplicaSet), b.(*api.ReplicationController), scope)
	}); err != nil {
		t.Fatal(err)
	}

	rs := &apps.ReplicaSet{}
	rc := &api.ReplicationController{}
	extGroup := schema.GroupVersion{Group: "apps", Version: "v1"}
	extCodec := legacyscheme.Codecs.LegacyCodec(extGroup)

	defaultGroup := schema.GroupVersion{Group: "", Version: "v1"}
	defaultCodec := legacyscheme.Codecs.LegacyCodec(defaultGroup)

	fuzzInternalObject(t, schema.GroupVersion{Group: "apps", Version: runtime.APIVersionInternal}, rs, rand.Int63())

	// 显式设置可转换为旧式格式的 selector：fuzz 生成的选择器通常无法参与此类转换。
	rs.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"foo": "bar",
			"baz": "quux",
		},
	}

	// 按 internal.apps -> v1.apps -> internal.core -> v1.core -> internal.apps 走完整条转换链，
	// 每一步失败都能从 t.Logf 中定位。
	t.Logf("rs._internal.apps -> rs.v1.apps")
	data, err := runtime.Encode(extCodec, rs)
	if err != nil {
		t.Fatalf("unexpected encoding error: %v", err)
	}

	// MultiGroupVersioner 允许同一份数据按 apps 或 core 组解码，模拟跨组资源的转换场景。
	decoder := legacyscheme.Codecs.DecoderToVersion(
		legacyscheme.Codecs.UniversalDeserializer(),
		runtime.NewMultiGroupVersioner(
			defaultGroup,
			schema.GroupKind{Group: defaultGroup.Group},
			schema.GroupKind{Group: extGroup.Group},
		),
	)

	t.Logf("rs.v1.apps -> rc._internal")
	if err := runtime.DecodeInto(decoder, data, rc); err != nil {
		t.Fatalf("unexpected decoding error: %v", err)
	}

	t.Logf("rc._internal -> rc.v1")
	data, err = runtime.Encode(defaultCodec, rc)
	if err != nil {
		t.Fatalf("unexpected encoding error: %v", err)
	}

	t.Logf("rc.v1 -> rs._internal.apps")
	if err := runtime.DecodeInto(decoder, data, rs); err != nil {
		t.Fatalf("unexpected decoding error: %v", err)
	}
}

// TestSpecificKind 对单个指定 GVK 做 round-trip，便于在新增 API 类型时定位问题。
func TestSpecificKind(t *testing.T) {
	// 取消下面的注释可打印出所有转换函数的注册情况。
	// legacyscheme.Scheme.Log(t)
	internalGVK := schema.GroupVersionKind{Group: "apps", Version: runtime.APIVersionInternal, Kind: "DaemonSet"}

	seed := rand.Int63()
	fuzzer := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(seed), legacyscheme.Codecs)

	roundtrip.RoundTripSpecificKind(t, internalGVK, legacyscheme.Scheme, legacyscheme.Codecs, fuzzer, nil)
}

// nonRoundTrippableTypes 是不参与 round-trip 测试的类型集合。
var nonRoundTrippableTypes = sets.New[string](
	"ExportOptions",
	"GetOptions",
	// WatchEvent 不包含 kind 和 version，只能由调用方按预期类型隐式反序列化。
	// watch 调用通过 content type 而非每个对象里的 kind/version 来定义 schema。
	"WatchEvent",
	// ListOptions 现在属于 meta group
	"ListOptions",
	// DeleteOptions、CreateOptions、UpdateOptions 只由 metav1 读取
	"DeleteOptions",
	"CreateOptions",
	"UpdateOptions",
	"PatchOptions",
)

var commonKinds = []string{"Status", "ListOptions", "DeleteOptions", "GetOptions", "CreateOptions", "UpdateOptions", "PatchOptions"}

// TestCommonKindsRegistered 验证 legacyscheme 中注册的每个 group/version 都包含通用 kind。
// 通过解码含 kind 的最小 JSON 再编码，确认类型能正确注册且 kind/apiVersion 被保留。
func TestCommonKindsRegistered(t *testing.T) {
	gvs := map[schema.GroupVersion]bool{}
	for gvk := range legacyscheme.Scheme.AllKnownTypes() {
		// 内部版本不对外暴露，不需要通用 kind。
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		gvs[gvk.GroupVersion()] = true
	}

	for _, kind := range commonKinds {
		for gv := range gvs {
			gvk := gv.WithKind(kind)
			obj, err := legacyscheme.Scheme.New(gvk)
			if err != nil {
				t.Error(err)
			}
			defaults := gv.WithKind("")
			var got *schema.GroupVersionKind
			// 只提供 kind 的最小 JSON，解码后应补全为期望的 gvk。
			if obj, got, err = legacyscheme.Codecs.LegacyCodec().Decode([]byte(`{"kind":"`+kind+`"}`), &defaults, obj); err != nil || gvk != *got {
				t.Errorf("expected %v: %v %v", gvk, got, err)
			}
			data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(gv), obj)
			if err != nil {
				t.Errorf("expected %v: %v\n%s", gvk, err, string(data))
				continue
			}
			// 编码结果必须包含正确的 kind 与 apiVersion，否则客户端无法识别对象类型。
			if !bytes.Contains(data, []byte(`"kind":"`+kind+`","apiVersion":"`+gv.String()+`"`)) {
				if kind != "Status" {
					t.Errorf("expected %v: %v\n%s", gvk, err, string(data))
					continue
				}
				// TODO: this is wrong, but legacy clients expect it
				// Status 是历史遗留例外：旧客户端期望 apiVersion 固定为 v1。
				if !bytes.Contains(data, []byte(`"kind":"`+kind+`","apiVersion":"v1"`)) {
					t.Errorf("expected %v: %v\n%s", gvk, err, string(data))
					continue
				}
			}
		}
	}
}

// TestRoundTripTypes 对 legacyscheme 中所有可 round-trip 的 Kind 执行 round-trip 测试。
func TestRoundTripTypes(t *testing.T) {
	seed := rand.Int63()
	fuzzer := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(seed), legacyscheme.Codecs)
	nonRoundTrippableTypes := map[schema.GroupVersionKind]bool{}

	roundtrip.RoundTripTypes(t, legacyscheme.Scheme, legacyscheme.Codecs, fuzzer, nonRoundTrippableTypes)
}

// TestEncodePtr 验证 Go 指针类型字段（如 *int64、*bool）经编解码后不会丢失信息或被修改。
func TestEncodePtr(t *testing.T) {
	grace := int64(30)
	enableServiceLinks := v1.DefaultEnableServiceLinks
	preemptNever := api.PreemptNever
	pod := &api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"name": "foo"},
		},
		Spec: api.PodSpec{
			RestartPolicy: api.RestartPolicyAlways,
			DNSPolicy:     api.DNSClusterFirst,

			TerminationGracePeriodSeconds: &grace,

			SecurityContext:    &api.PodSecurityContext{},
			SchedulerName:      v1.DefaultSchedulerName,
			EnableServiceLinks: &enableServiceLinks,
			PreemptionPolicy:   &preemptNever,
		},
	}
	obj := runtime.Object(pod)
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), obj)
	obj2, err2 := runtime.Decode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), data)
	// 编解码任一步失败都直接终止，因为后续比较已无意义。
	if err != nil || err2 != nil {
		t.Fatalf("Failure: '%v' '%v'", err, err2)
	}
	// 解码结果必须仍是 internal 的 core.Pod 类型。
	if _, ok := obj2.(*api.Pod); !ok {
		t.Fatalf("Got wrong type")
	}
	// 语义比较确认指针字段的值被忠实保留。
	if !apiequality.Semantic.DeepEqual(obj2, pod) {
		t.Errorf("\nExpected:\n\n %#v,\n\nGot:\n\n %#vDiff: %v\n\n", pod, obj2, cmp.Diff(obj2, pod))
	}
}

// TestDecodeTimeStampWithoutQuotes 验证 YAML 中无引号的时间戳能被正确解析为时间值。
func TestDecodeTimeStampWithoutQuotes(t *testing.T) {
	testYAML := []byte(`
apiVersion: v1
kind: Pod
metadata:
  creationTimestamp: 2018-08-30T14:10:58Z
  name: test
spec:
  containers: null
status: {}`)
	if obj, err := runtime.Decode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), testYAML); err != nil {
		t.Fatalf("unable to decode yaml: %v", err)
	} else {
		if obj2, ok := obj.(*api.Pod); !ok {
			t.Fatalf("Got wrong type")
		} else {
			if obj2.ObjectMeta.CreationTimestamp.UnixNano() != parseTimeOrDie("2018-08-30T14:10:58Z").UnixNano() {
				t.Fatalf("Time stamps do not match")
			}
		}
	}
}

// TestBadJSONRejection 验证缺少 kind 或使用未知 kind 的 JSON 会被解码器拒绝。
// 否则这类对象会被静默解码成错误类型，给上层带来难以排查的问题。
func TestBadJSONRejection(t *testing.T) {
	badJSONMissingKind := []byte(`{ }`)
	if _, err := runtime.Decode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), badJSONMissingKind); err == nil {
		t.Errorf("Did not reject despite lack of kind field: %s", badJSONMissingKind)
	}
	badJSONUnknownType := []byte(`{"kind": "bar"}`)
	if _, err1 := runtime.Decode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), badJSONUnknownType); err1 == nil {
		t.Errorf("Did not reject despite use of unknown type: %s", badJSONUnknownType)
	}
}

// TestUnversionedTypes 验证默认 codec 能正确编解码无版本（meta 类型）对象。
func TestUnversionedTypes(t *testing.T) {
	testcases := []runtime.Object{
		&metav1.Status{Status: "Failure", Message: "something went wrong"},
		&metav1.APIVersions{Versions: []string{"A", "B", "C"}},
		&metav1.APIGroupList{Groups: []metav1.APIGroup{{Name: "mygroup"}}},
		&metav1.APIGroup{Name: "mygroup"},
		&metav1.APIResourceList{GroupVersion: "mygroup/myversion"},
	}

	for _, obj := range testcases {
		// 先确认 codec 能编码这些无版本对象。
		unversionedJSON, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), obj)
		if err != nil {
			t.Errorf("%v: unexpected error: %v", obj, err)
			continue
		}

		// 再确认被测的带版本 codec 能解码回来。
		versionDecodedObject, err := runtime.Decode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), unversionedJSON)
		if err != nil {
			t.Errorf("%v: unexpected error: %v", obj, err)
			continue
		}
		// 最后比较解码结果，确保往返过程没有信息丢失。
		if !reflect.DeepEqual(obj, versionDecodedObject) {
			t.Errorf("%v: expected %#v, got %#v", obj, obj, versionDecodedObject)
			continue
		}
	}
}

// TestObjectWatchFraming 验证 watch 事件在所有支持的 RFC2046 media type 下都能正确编解码。
// 其中二进制与 Unicode 数据特意加入，用来检验帧边界与内容编码是否可靠。
func TestObjectWatchFraming(t *testing.T) {
	f := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(benchmarkSeed), legacyscheme.Codecs)
	secret := &api.Secret{}
	f.Fill(secret)
	// 显式加入二进制、Unicode 与长数据，专门考验帧读写对边界情况的处理。
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data["binary"] = []byte{0x00, 0x10, 0x30, 0x55, 0xff, 0x00}
	secret.Data["utf8"] = []byte("a string with \u0345 characters")
	secret.Data["long"] = bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x00}, 1000)
	// watch 走的是 v1 序列化路径，先把 internal Secret 转成 v1。
	converted, _ := legacyscheme.Scheme.ConvertToVersion(secret, v1.SchemeGroupVersion)
	v1secret := converted.(*v1.Secret)
	for _, info := range legacyscheme.Codecs.SupportedMediaTypes() {
		// 只有带流式序列化器的 media type 才支持 watch 分帧。
		if info.StreamSerializer == nil {
			continue
		}
		s := info.StreamSerializer
		framer := s.Framer
		embedded := info.Serializer
		if embedded == nil {
			t.Errorf("no embedded serializer for %s", info.MediaType)
			continue
		}
		// 事件内嵌对象按 internal 版本解码，便于与原始 Secret 比较。
		innerDecode := legacyscheme.Codecs.DecoderToVersion(embedded, api.SchemeGroupVersion)

		// 第一组检查：单个对象经过分帧写入再读回是否一致。
		obj := &bytes.Buffer{}
		if err := s.Encode(v1secret, obj); err != nil {
			t.Fatal(err)
		}
		out := &bytes.Buffer{}
		w := framer.NewFrameWriter(out)
		if n, err := w.Write(obj.Bytes()); err != nil || n != len(obj.Bytes()) {
			t.Fatal(err)
		}
		sr := streaming.NewDecoder(framer.NewFrameReader(io.NopCloser(out)), s)
		resultSecret := &v1.Secret{}
		res, _, err := sr.Decode(nil, resultSecret)
		if err != nil {
			t.Fatalf("%v:\n%s", err, hex.Dump(obj.Bytes()))
		}
		// 分帧读取不会带回类型元信息，手动补回以便做等价比较。
		resultSecret.Kind = "Secret"
		resultSecret.APIVersion = "v1"
		if !apiequality.Semantic.DeepEqual(v1secret, res) {
			t.Fatalf("objects did not match: %s", cmp.Diff(v1secret, res))
		}

		// 第二组检查：完整的 watch 事件（含类型与内嵌对象）经帧读写后能否还原。
		obj = &bytes.Buffer{}
		if err := embedded.Encode(v1secret, obj); err != nil {
			t.Fatal(err)
		}
		event := &metav1.WatchEvent{Type: string(watch.Added)}
		event.Object.Raw = obj.Bytes()
		obj = &bytes.Buffer{}
		if err := s.Encode(event, obj); err != nil {
			t.Fatal(err)
		}
		out = &bytes.Buffer{}
		w = framer.NewFrameWriter(out)
		if n, err := w.Write(obj.Bytes()); err != nil || n != len(obj.Bytes()) {
			t.Fatal(err)
		}
		sr = streaming.NewDecoder(framer.NewFrameReader(io.NopCloser(out)), s)
		outEvent := &metav1.WatchEvent{}
		_, _, err = sr.Decode(nil, outEvent)
		if err != nil || outEvent.Type != string(watch.Added) {
			t.Fatalf("%v: %#v", err, outEvent)
		}
		if outEvent.Object.Object == nil && outEvent.Object.Raw != nil {
			// 某些 media type 以 Raw 字节返回内嵌对象，需显式解码为 runtime.Object。
			outEvent.Object.Object, err = runtime.Decode(innerDecode, outEvent.Object.Raw)
			if err != nil {
				t.Fatalf("%v:\n%s", err, hex.Dump(outEvent.Object.Raw))
			}
		}

		// 最终与原始 internal Secret 比较，确认整个 watch 事件链路无损。
		if !apiequality.Semantic.DeepEqual(secret, outEvent.Object.Object) {
			t.Fatalf("%s: did not match after frame decoding: %s", info.MediaType, cmp.Diff(secret, outEvent.Object.Object))
		}
	}
}

// benchmarkSeed 固定随机种子，保证各基准使用相同且可复现的数据集。
const benchmarkSeed = 100

// benchmarkItems 生成 10 个随机的 v1.Pod 作为基准测试数据。
func benchmarkItems(b *testing.B) []v1.Pod {
	apiObjectFuzzer := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(benchmarkSeed), legacyscheme.Codecs)
	items := make([]v1.Pod, 10)
	for i := range items {
		var pod api.Pod
		apiObjectFuzzer.Fill(&pod)
		// InitContainers 在转换中会变成注解，置空以免干扰编解码基准。
		pod.Spec.InitContainers, pod.Status.InitContainerStatuses = nil, nil
		out, err := legacyscheme.Scheme.ConvertToVersion(&pod, v1.SchemeGroupVersion)
		if err != nil {
			panic(err)
		}
		items[i] = *out.(*v1.Pod)
	}
	return items
}

// benchmarkItemsList 生成包含指定数量 Pod 的 v1.PodList，用于列表序列化基准。
func benchmarkItemsList(b *testing.B, numItems int) v1.PodList {
	apiObjectFuzzer := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(benchmarkSeed), legacyscheme.Codecs)
	items := make([]v1.Pod, numItems)
	for i := range items {
		var pod api.Pod
		apiObjectFuzzer.Fill(&pod)
		pod.Spec.InitContainers, pod.Status.InitContainerStatuses = nil, nil
		out, err := legacyscheme.Scheme.ConvertToVersion(&pod, v1.SchemeGroupVersion)
		if err != nil {
			panic(err)
		}
		items[i] = *out.(*v1.Pod)
	}

	return v1.PodList{
		Items: items,
	}
}

// BenchmarkEncodeCodec 测量 codec 编码开销，其中包含清空 APIVersion 与 Kind 的反射操作。
func BenchmarkEncodeCodec(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &items[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeCodecFromInternal 测量从 internal 对象做 codec 编码的开销，包含版本转换。
func BenchmarkEncodeCodecFromInternal(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	encodable := make([]api.Pod, width)
	for i := range items {
		if err := legacyscheme.Scheme.Convert(&items[i], &encodable[i], nil); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &encodable[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeJSONMarshal 测量标准库的 JSON 编码性能，作为 codec 编码的基线。
func BenchmarkEncodeJSONMarshal(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gojson.Marshal(&items[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeCodec 测量 codec 解码开销；编码数据预先生成，不计入计时。
func BenchmarkDecodeCodec(b *testing.B) {
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	items := benchmarkItems(b)
	width := len(items)
	encoded := make([][]byte, width)
	for i := range items {
		data, err := runtime.Encode(codec, &items[i])
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = data
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := runtime.Decode(codec, encoded[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeIntoExternalCodec 测量 codec 解码到外部 v1.Pod 的开销。
func BenchmarkDecodeIntoExternalCodec(b *testing.B) {
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	items := benchmarkItems(b)
	width := len(items)
	encoded := make([][]byte, width)
	for i := range items {
		data, err := runtime.Encode(codec, &items[i])
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = data
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj := v1.Pod{}
		if err := runtime.DecodeInto(codec, encoded[i%width], &obj); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeIntoInternalCodec 测量 codec 解码到 internal core.Pod 的开销，包含版本转换。
func BenchmarkDecodeIntoInternalCodec(b *testing.B) {
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	items := benchmarkItems(b)
	width := len(items)
	encoded := make([][]byte, width)
	for i := range items {
		data, err := runtime.Encode(codec, &items[i])
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = data
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj := api.Pod{}
		if err := runtime.DecodeInto(codec, encoded[i%width], &obj); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeIntoJSON 测量标准库的 JSON 解码性能，作为 codec 解码的基线。
func BenchmarkDecodeIntoJSON(b *testing.B) {
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	items := benchmarkItems(b)
	width := len(items)
	encoded := make([][]byte, width)
	for i := range items {
		data, err := runtime.Encode(codec, &items[i])
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = data
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj := v1.Pod{}
		if err := gojson.Unmarshal(encoded[i%width], &obj); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkEncodeYAMLMarshal 测量 YAML 编码性能，作为对照基线。
func BenchmarkEncodeYAMLMarshal(b *testing.B) {
	items := benchmarkItems(b)
	width := len(items)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := yaml.Marshal(&items[i%width]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// BenchmarkDecodeIntoYAML 测量 YAML 解码性能，作为对照基线。
func BenchmarkDecodeIntoYAML(b *testing.B) {
	codec := legacyscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)
	items := benchmarkItems(b)
	width := len(items)
	encoded := make([][]byte, width)
	for i := range items {
		data, err := runtime.Encode(codec, &items[i])
		if err != nil {
			b.Fatal(err)
		}
		encoded[i] = data
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj := v1.Pod{}
		if err := yaml.Unmarshal(encoded[i%width], &obj); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
