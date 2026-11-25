/*
Copyright 2024 The Kubernetes Authors.

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

package roundtrip

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	cborserializer "k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/google/go-cmp/cmp"
)

// 测试不变量：
//   - JSON 与 CBOR 两条路径产出的 unstructured 必须相同；
//   - CBOR 确定性/非确定性编码必须产出相同的 unstructured；
//   - unstructured 自身再次往返不得变化；
//   - 经 unstructured 转回原生对象后必须与原始对象语义相等。
//
// 这些约束共同保证任意编码器都不会丢失或改写 API 数据。
func RoundtripToUnstructured(t *testing.T, scheme *runtime.Scheme, funcs fuzzer.FuzzerFuncs, skipped sets.Set[schema.GroupVersionKind], nointernal sets.Set[schema.GroupVersionKind]) {
	codecs := serializer.NewCodecFactory(scheme)

	// 默认用纳秒时间做种子；失败时可通过 TEST_RAND_SEED 复现具体输入。
	seed := int64(time.Now().Nanosecond())
	if override := os.Getenv("TEST_RAND_SEED"); len(override) > 0 {
		overrideSeed, err := strconv.ParseInt(override, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seed = overrideSeed
		t.Logf("using overridden seed: %d", seed)
	} else {
		t.Logf("seed (override with TEST_RAND_SEED if desired): %d", seed)
	}

	var buf bytes.Buffer
	for gvk := range scheme.AllKnownTypes() {
		if globalNonRoundTrippableTypes.Has(gvk.Kind) {
			// 已知无法无损往返的类型（例如含 channel/函数字段）直接排除。
			continue
		}

		if gvk.Version == runtime.APIVersionInternal {
			// 只测外部版本；内部版本不作为 wire 格式。
			continue
		}

		// 子测试名带上 version/group/kind，便于从失败输出直接定位类型。
		subtestName := fmt.Sprintf("%s.%s/%s", gvk.Version, gvk.Group, gvk.Kind)
		if gvk.Group == "" {
			subtestName = fmt.Sprintf("%s/%s", gvk.Version, gvk.Kind)
		}

		t.Run(subtestName, func(t *testing.T) {
			if skipped.Has(gvk) {
				t.SkipNow()
			}

			// 每个子测试使用相同种子的 fuzzer，保证失败可复现。
			fuzzer := fuzzer.FuzzerFor(fuzzer.MergeFuzzerFuncs(metafuzzer.Funcs, funcs), rand.NewSource(seed), codecs)

			for i := 0; i < *FuzzIters; i++ {
				item, err := scheme.New(gvk)
				if err != nil {
					t.Fatalf("couldn't create external object %v: %v", gvk.Kind, err)
				}

				if nointernal.Has(gvk) {
					// 没有内部对应类型的 GVK（如部分 test-only 类型）直接 fuzz。
					fuzzer.Fill(item)
				} else {
					// 常规路径：先 fuzz 内部类型再转换，以覆盖真实的转换逻辑。
					internalObj, err := scheme.New(gvk.GroupKind().WithVersion(runtime.APIVersionInternal))
					if err != nil {
						t.Fatalf("couldn't create internal object %v: %v", gvk.Kind, err)
					}
					fuzzer.Fill(internalObj)

					if err := scheme.Convert(internalObj, item, nil); err != nil {
						t.Fatalf("conversion for %v failed: %v", gvk.Kind, err)
					}
				}

				// unstructured 解码要求 apiVersion 与 kind 已序列化，因此补上 TypeMeta。
				item.GetObjectKind().SetGroupVersionKind(gvk)

				jsonSerializer := jsonserializer.NewSerializerWithOptions(jsonserializer.DefaultMetaFactory, scheme, scheme, jsonserializer.SerializerOptions{})
				cborSerializer := cborserializer.NewSerializer(scheme, scheme)

				// 原始对象 -> JSON -> unstructured。
				buf.Reset()
				if err := jsonSerializer.Encode(item, &buf); err != nil {
					t.Fatalf("error encoding native to json: %v", err)
				}
				var uJSON runtime.Object = &unstructured.Unstructured{}
				uJSON, _, err = jsonSerializer.Decode(buf.Bytes(), &gvk, uJSON)
				if err != nil {
					t.Fatalf("error decoding json to unstructured: %v", err)
				}

				// 原始对象 -> CBOR -> unstructured。
				buf.Reset()
				if err := cborSerializer.Encode(item, &buf); err != nil {
					t.Fatalf("error encoding native to cbor: %v", err)
				}
				var uCBOR runtime.Object = &unstructured.Unstructured{}
				uCBOR, _, err = cborSerializer.Decode(buf.Bytes(), &gvk, uCBOR)
				if err != nil {
					// 解码失败时附带诊断文本，方便直接看出 CBOR 结构问题。
					diag, _ := cbor.Diagnose(buf.Bytes())
					t.Fatalf("error decoding cbor to unstructured: %v, diag: %s", err, diag)
				}

				// 原始对象 -> JSON -> unstructured 与原始对象 -> CBOR -> unstructured 的结果必须相同，
				// 否则下游行为会因编码不同而分叉。
				if !apiequality.Semantic.DeepEqual(uJSON, uCBOR) {
					t.Fatalf("unstructured via json differed from unstructured via cbor: %v", cmp.Diff(uJSON, uCBOR))
				}

				// original->CBOR(nondeterministic)->Unstructured
				buf.Reset()
				if err := cborSerializer.EncodeNondeterministic(item, &buf); err != nil {
					t.Fatalf("error encoding native to cbor: %v", err)
				}
				var uCBORNondeterministic runtime.Object = &unstructured.Unstructured{}
				uCBORNondeterministic, _, err = cborSerializer.Decode(buf.Bytes(), &gvk, uCBORNondeterministic)
				if err != nil {
					diag, _ := cbor.Diagnose(buf.Bytes())
					t.Fatalf("error decoding cbor to unstructured: %v, diag: %s", err, diag)
				}

				// 原始对象 -> CBOR -> unstructured 与原始对象 -> CBOR（非确定性）-> unstructured
				// 的结果必须相同；编码顺序（map key 排序与否）不得影响解码后的语义。
				if !apiequality.Semantic.DeepEqual(uCBOR, uCBORNondeterministic) {
					t.Fatalf("unstructured via nondeterministic cbor differed from unstructured via cbor: %v", cmp.Diff(uCBOR, uCBORNondeterministic))
				}

				// unstructured 自身必须是不动点，否则一次额外的序列化就会改变对象。
				buf.Reset()
				if err := jsonSerializer.Encode(uJSON, &buf); err != nil {
					t.Fatalf("error encoding unstructured to json: %v", err)
				}
				var uJSON2 runtime.Object = &unstructured.Unstructured{}
				uJSON2, _, err = jsonSerializer.Decode(buf.Bytes(), &gvk, uJSON2)
				if err != nil {
					t.Fatalf("error decoding json to unstructured: %v", err)
				}
				if !apiequality.Semantic.DeepEqual(uJSON, uJSON2) {
					t.Errorf("object changed during native-json-unstructured-json-unstructured roundtrip, diff: %s", cmp.Diff(uJSON, uJSON2))
				}

				// original->JSON/CBOR->Unstructured == original->JSON/CBOR->Unstructured->CBOR->Unstructured
				buf.Reset()
				if err := cborSerializer.Encode(uCBOR, &buf); err != nil {
					t.Fatalf("error encoding unstructured to cbor: %v", err)
				}
				var uCBOR2 runtime.Object = &unstructured.Unstructured{}
				uCBOR2, _, err = cborSerializer.Decode(buf.Bytes(), &gvk, uCBOR2)
				if err != nil {
					diag, _ := cbor.Diagnose(buf.Bytes())
					t.Fatalf("error decoding cbor to unstructured: %v, diag: %s", err, diag)
				}
				if !apiequality.Semantic.DeepEqual(uCBOR, uCBOR2) {
					t.Errorf("object changed during native-cbor-unstructured-cbor-unstructured roundtrip, diff: %s", cmp.Diff(uCBOR, uCBOR2))
				}

				// original->JSON/CBOR->Unstructured->CBOR->Unstructured == original->JSON/CBOR->Unstructured->CBOR(nondeterministic)->Unstructured
				buf.Reset()
				if err := cborSerializer.EncodeNondeterministic(uCBOR, &buf); err != nil {
					t.Fatalf("error encoding unstructured to cbor: %v", err)
				}
				var uCBOR2Nondeterministic runtime.Object = &unstructured.Unstructured{}
				uCBOR2Nondeterministic, _, err = cborSerializer.Decode(buf.Bytes(), &gvk, uCBOR2Nondeterministic)
				if err != nil {
					diag, _ := cbor.Diagnose(buf.Bytes())
					t.Fatalf("error decoding cbor to unstructured: %v, diag: %s", err, diag)
				}
				if !apiequality.Semantic.DeepEqual(uCBOR, uCBOR2Nondeterministic) {
					t.Errorf("object changed during native-cbor-unstructured-cbor(nondeterministic)-unstructured roundtrip, diff: %s", cmp.Diff(uCBOR, uCBOR2Nondeterministic))
				}

				// 最终回到原生类型，验证整条链路无损。
				buf.Reset()
				if err := jsonSerializer.Encode(uJSON, &buf); err != nil {
					t.Fatalf("error encoding unstructured to json: %v", err)
				}
				finalJSON, _, err := jsonSerializer.Decode(buf.Bytes(), &gvk, nil)
				if err != nil {
					t.Fatalf("error decoding json to native: %v", err)
				}
				if !apiequality.Semantic.DeepEqual(item, finalJSON) {
					t.Errorf("object changed during native-json-unstructured-json-native roundtrip, diff: %s", cmp.Diff(item, finalJSON))
				}

				// original->JSON/CBOR->Unstructured->CBOR->final == original
				buf.Reset()
				if err := cborSerializer.Encode(uCBOR, &buf); err != nil {
					t.Fatalf("error encoding unstructured to cbor: %v", err)
				}
				finalCBOR, _, err := cborSerializer.Decode(buf.Bytes(), &gvk, nil)
				if err != nil {
					diag, _ := cbor.Diagnose(buf.Bytes())
					t.Fatalf("error decoding cbor to native: %v, diag: %s", err, diag)
				}
				if !apiequality.Semantic.DeepEqual(item, finalCBOR) {
					t.Errorf("object changed during native-cbor-unstructured-cbor-native roundtrip, diff: %s", cmp.Diff(item, finalCBOR))
				}

				// original->JSON/CBOR->Unstructured->CBOR(nondeterministic)->final == original
				buf.Reset()
				if err := cborSerializer.EncodeNondeterministic(uCBOR, &buf); err != nil {
					t.Fatalf("error encoding unstructured to cbor: %v", err)
				}
				finalCBORNondeterministic, _, err := cborSerializer.Decode(buf.Bytes(), &gvk, nil)
				if err != nil {
					diag, _ := cbor.Diagnose(buf.Bytes())
					t.Fatalf("error decoding cbor to native: %v, diag: %s", err, diag)
				}
				if !apiequality.Semantic.DeepEqual(item, finalCBORNondeterministic) {
					t.Errorf("object changed during native-cbor-unstructured-cbor-native roundtrip, diff: %s", cmp.Diff(item, finalCBORNondeterministic))
				}
			}
		})
	}
}
