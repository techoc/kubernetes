/*
Copyright 2018 The Kubernetes Authors.

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

package unstructured_test

import (
	"bytes"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/equality"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	cborserializer "k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
)

// TestNilUnstructuredContent 验证零值 Unstructured 的 UnstructuredContent()
// 返回非 nil 的空 map，且不会因此修改对象本身（DeepCopy 后仍相等）。
func TestNilUnstructuredContent(t *testing.T) {
	var u unstructured.Unstructured
	uCopy := u.DeepCopy()
	content := u.UnstructuredContent()
	expContent := make(map[string]interface{})
	assert.EqualValues(t, expContent, content)
	assert.Equal(t, uCopy, &u)
}

// 先用 converter 把 ObjectMeta 整体写入 metadata，再用 getter/setter
// 逐字段复制到另一个对象；两者应完全一致。这验证 accessor 与
// ToUnstructured 的语义（尤其是 omitempty）没有偏差。
func TestUnstructuredMetadataRoundTrip(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	seed := rand.Int63()
	fuzzer := fuzzer.FuzzerFor(metafuzzer.Funcs, rand.NewSource(seed), codecs)

	N := 1000
	for i := 0; i < N; i++ {
		u := &unstructured.Unstructured{Object: map[string]interface{}{}}
		uCopy := u.DeepCopy()
		metadata := &metav1.ObjectMeta{}
		fuzzer.Fill(metadata)

		if err := setObjectMeta(u, metadata); err != nil {
			t.Fatalf("unexpected error setting fuzzed ObjectMeta: %v", err)
		}
		setObjectMetaUsingAccessors(u, uCopy)

		if !equality.Semantic.DeepEqual(u, uCopy) {
			t.Errorf("diff: %v", cmp.Diff(u, uCopy))
		}
	}
}

// 把所有字段显式设为零值，metadata map 必须完全为空，证明
// setter 遵守 omitempty 而不是留下 "name": "" 之类的条目。
func TestUnstructuredMetadataOmitempty(t *testing.T) {
	scheme := runtime.NewScheme()
	codecs := serializer.NewCodecFactory(scheme)
	seed := rand.Int63()
	fuzzer := fuzzer.FuzzerFor(metafuzzer.Funcs, rand.NewSource(seed), codecs)

	// 模糊测试，确保不会遗漏下面的任何函数调用。
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	metadata := &metav1.ObjectMeta{}
	fuzzer.Fill(metadata)
	if err := setObjectMeta(u, metadata); err != nil {
		t.Fatalf("unexpected error setting fuzzed ObjectMeta: %v", err)
	}

	// 显式设置元数据所有字段的零值，
	// 检查带 omitempty 的零值字段不会被写入。
	u.SetName("")
	u.SetGenerateName("")
	u.SetNamespace("")
	u.SetSelfLink("")
	u.SetUID("")
	u.SetResourceVersion("")
	u.SetGeneration(0)
	u.SetCreationTimestamp(metav1.Time{})
	u.SetDeletionTimestamp(nil)
	u.SetDeletionGracePeriodSeconds(nil)
	u.SetLabels(nil)
	u.SetAnnotations(nil)
	u.SetOwnerReferences(nil)
	u.SetFinalizers(nil)
	u.SetManagedFields(nil)

	gotMetadata, _, err := unstructured.NestedFieldNoCopy(u.UnstructuredContent(), "metadata")
	if err != nil {
		t.Error(err)
	}
	emptyMetadata := make(map[string]interface{})

	if !reflect.DeepEqual(gotMetadata, emptyMetadata) {
		t.Errorf("expected %v, got %v", emptyMetadata, gotMetadata)
	}
}

// TestRoundTripJSONCBORUnstructured 对以下路径进行模糊往返测试：
// JSON 与 CBOR 之间的非结构化对象。
func TestRoundTripJSONCBORUnstructured(t *testing.T) {
	roundtripType[*unstructured.Unstructured](t)
}

// TestRoundTripJSONCBORUnstructuredList 对以下路径进行模糊往返测试：
// JSON 与 CBOR 之间的非结构化列表对象。
func TestRoundTripJSONCBORUnstructuredList(t *testing.T) {
	roundtripType[*unstructured.UnstructuredList](t)
}

// setObjectMeta 用通用的 unstructured 转换把 ObjectMeta 写入 metadata 字段。
// objectMeta 为 nil 时删除整个 metadata；这为“无元数据”提供了统一表示。
func setObjectMeta(u *unstructured.Unstructured, objectMeta *metav1.ObjectMeta) error {
	if objectMeta == nil {
		unstructured.RemoveNestedField(u.UnstructuredContent(), "metadata")
		return nil
	}
	metadata, err := runtime.DefaultUnstructuredConverter.ToUnstructured(objectMeta)
	if err != nil {
		return err
	}
	u.UnstructuredContent()["metadata"] = metadata
	return nil
}

// setObjectMetaUsingAccessors 通过 Unstructured 的 getter/setter 逐字段复制
// 元数据。字段清单必须与 setObjectMeta 的覆盖范围保持一致，否则测试会
// 误报或漏报。
func setObjectMetaUsingAccessors(u, uCopy *unstructured.Unstructured) {
	uCopy.SetName(u.GetName())
	uCopy.SetGenerateName(u.GetGenerateName())
	uCopy.SetNamespace(u.GetNamespace())
	uCopy.SetSelfLink(u.GetSelfLink())
	uCopy.SetUID(u.GetUID())
	uCopy.SetResourceVersion(u.GetResourceVersion())
	uCopy.SetGeneration(u.GetGeneration())
	uCopy.SetCreationTimestamp(u.GetCreationTimestamp())
	uCopy.SetDeletionTimestamp(u.GetDeletionTimestamp())
	uCopy.SetDeletionGracePeriodSeconds(u.GetDeletionGracePeriodSeconds())
	uCopy.SetLabels(u.GetLabels())
	uCopy.SetAnnotations(u.GetAnnotations())
	uCopy.SetOwnerReferences(u.GetOwnerReferences())
	uCopy.SetFinalizers(u.GetFinalizers())
	uCopy.SetManagedFields(u.GetManagedFields())
}

// 两种顺序都要跑（JSON→CBOR 与 CBOR→JSON），因为不同编码对数字、
// null、空容器等的保留行为可能不对称。
func roundtripType[U runtime.Unstructured](t *testing.T) {
	scheme := runtime.NewScheme()
	fuzzer := fuzzer.FuzzerFor(fuzzer.MergeFuzzerFuncs(metafuzzer.Funcs, unstructuredFuzzerFuncs), rand.NewSource(getSeed(t)), serializer.NewCodecFactory(scheme))

	jS := jsonserializer.NewSerializerWithOptions(jsonserializer.DefaultMetaFactory, scheme, scheme, jsonserializer.SerializerOptions{})
	cS := cborserializer.NewSerializer(scheme, scheme)

	for i := 0; i < 50; i++ {
		original := reflect.New(reflect.TypeFor[U]().Elem()).Interface().(runtime.Unstructured)
		fuzzer.Fill(original)
		// unstructured -> JSON > unstructured > CBOR -> unstructured -> JSON -> unstructured（往返路径）
		roundtrip(t, original, jS, cS)
		// unstructured -> CBOR > unstructured > JSON -> unstructured -> CBOR -> unstructured（往返路径）
		roundtrip(t, original, cS, jS)
	}
}

// roundtrip 测试 Unstructured 对象沿以下序列往返后保持一致：
// Unstructured -> A -> Unstructured -> B -> Unstructured -> A -> Unstructured（往返序列）。
// 给定两种编码 A 和 B 的序列化器，最终对象及两个中间对象都必须与原对象相等。
func roundtrip(t *testing.T, original runtime.Unstructured, a, b runtime.Serializer) {
	var buf bytes.Buffer

	buf.Reset()
	//（原始）Unstructured -> A。
	if err := a.Encode(original, &buf); err != nil {
		t.Fatalf("error encoding original unstructured to A: %v", err)
	}
	// A -> 中间 unstructured 对象。
	uA := reflect.New(reflect.TypeOf(original).Elem()).Interface().(runtime.Object)
	uA, _, err := a.Decode(buf.Bytes(), nil, uA)
	if err != nil {
		t.Fatalf("error decoding A to unstructured: %v", err)
	}

	// 比较原始 unstructured 对象与中间对象。
	tmp, ok := uA.(runtime.Unstructured)
	if !ok {
		t.Fatalf("unexpected type %T for unstructured", tmp)
	}
	if !unstructuredEqual(t, original, uA.(runtime.Unstructured)) {
		t.Fatalf("original unstructured differed from unstructured via A: %v", cmp.Diff(original, uA))
	}

	buf.Reset()
	// 中间 unstructured 对象 -> B。
	if err := b.Encode(uA, &buf); err != nil {
		t.Fatalf("error encoding unstructured to B: %v", err)
	}
	// B -> 中间 unstructured 对象。
	uB := reflect.New(reflect.TypeOf(original).Elem()).Interface().(runtime.Object)
	uB, _, err = b.Decode(buf.Bytes(), nil, uB)
	if err != nil {
		t.Fatalf("error decoding B to unstructured: %v", err)
	}

	// 比较原始对象与中间 unstructured 对象。
	tmp, ok = uB.(runtime.Unstructured)
	if !ok {
		t.Fatalf("unexpected type %T for unstructured", tmp)
	}
	if !unstructuredEqual(t, original, uB.(runtime.Unstructured)) {
		t.Fatalf("unstructured via A differed from unstructured via B: %v", cmp.Diff(original, uB))
	}

	// 中间 unstructured 对象 -> A。
	buf.Reset()
	if err := a.Encode(uB, &buf); err != nil {
		t.Fatalf("error encoding unstructured to A: %v", err)
	}
	// A -> 最终 unstructured 对象。
	final := reflect.New(reflect.TypeOf(original).Elem()).Interface().(runtime.Object)
	final, _, err = a.Decode(buf.Bytes(), nil, final)
	if err != nil {
		t.Fatalf("error decoding A to unstructured: %v", err)
	}

	// 比较原始与最终 unstructured 对象。
	tmp, ok = final.(runtime.Unstructured)
	if !ok {
		t.Fatalf("unexpected type %T for unstructured", tmp)
	}
	if !unstructuredEqual(t, original, final.(runtime.Unstructured)) {
		t.Errorf("object changed during unstructured->A->unstructured->B->unstructured roundtrip, diff: %s", cmp.Diff(original, final))
	}
}

// getSeed 返回 fuzz 随机种子。默认基于当前纳秒时间，可通过
// TEST_RAND_SEED 环境变量固定以便复现失败用例；种子会打印到日志中。
func getSeed(t *testing.T) int64 {
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
	return seed
}

// 限制生成对象的深度与每层条目数，避免良构但极其庞大的数据拖慢 fuzz。
const (
	maxUnstructuredDepth  = 64
	maxUnstructuredFanOut = 5
)

// unstructuredFuzzerFuncs 注册自定义 fuzz 函数：随机填充 Unstructured 的
// Object map，以及 UnstructuredList 的 Object 与 Items。apiVersion/kind 使用
// 合法格式，避免生成无法被 serializer 接受的对象。
func unstructuredFuzzerFuncs(codecs serializer.CodecFactory) []interface{} {
	return []interface{}{
		func(u *unstructured.Unstructured, c randfill.Continue) {
			obj := make(map[string]interface{})
			obj["apiVersion"] = generateValidAPIVersionString(c)
			obj["kind"] = generateNonEmptyString(c)
			for j := c.Intn(maxUnstructuredFanOut); j >= 0; j-- {
				obj[c.String(0)] = generateRandomTypeValue(maxUnstructuredDepth, c)
			}
			u.Object = obj
		},
		func(ul *unstructured.UnstructuredList, c randfill.Continue) {
			obj := make(map[string]interface{})
			obj["apiVersion"] = generateValidAPIVersionString(c)
			obj["kind"] = generateNonEmptyString(c)
			for j := c.Intn(maxUnstructuredFanOut); j >= 0; j-- {
				obj[c.String(0)] = generateRandomTypeValue(maxUnstructuredDepth, c)
			}
			for j := c.Intn(maxUnstructuredFanOut); j >= 0; j-- {
				var item = unstructured.Unstructured{}
				c.Fill(&item)
				ul.Items = append(ul.Items, item)
			}
			ul.Object = obj
		},
	}
}

// generateNonEmptyString 反复生成随机字符串直到非空；fuzz 库可能生成空串，
// 而 apiVersion/kind 等字段不能为空。
func generateNonEmptyString(c randfill.Continue) string {
	temp := c.String(0)
	for len(temp) == 0 {
		temp = c.String(0)
	}
	return temp
}

// apiVersion 的 group 与 version 部分都不能包含 "/"。
func generateNonEmptyNoSlashString(c randfill.Continue) string {
	temp := strings.ReplaceAll(generateNonEmptyString(c), "/", "")
	for len(temp) == 0 {
		temp = strings.ReplaceAll(generateNonEmptyString(c), "/", "")
	}
	return temp
}

// 前者表示 <group>/<version>，后者表示核心组的 <version>。
func generateValidAPIVersionString(c randfill.Continue) string {
	if c.Bool() {
		return generateNonEmptyNoSlashString(c) + "/" + generateNonEmptyNoSlashString(c)
	} else {
		return generateNonEmptyNoSlashString(c)
	}
}

// 该函数只生成 unstructured 解码后可能出现的类型集合。数组与
// 对象会递归调用，深度到达 0 时只生成非递归类型，防止无限展开。
func generateRandomTypeValue(depth int, c randfill.Continue) interface{} {
	t := c.Rand.Intn(120)
	// 达到非结构化对象的最大深度后，
	// 只添加 20 以上的非递归类型。
	if depth == 0 {
		t = 20 + c.Rand.Intn(120-20)
	}

	switch {
	case t < 10:
		item := make([]interface{}, c.Intn(maxUnstructuredFanOut))
		for k := range item {
			item[k] = generateRandomTypeValue(depth-1, c)
		}
		return item
	case t < 20:
		item := map[string]interface{}{}
		for j := c.Intn(maxUnstructuredFanOut); j >= 0; j-- {
			item[c.String(0)] = generateRandomTypeValue(depth-1, c)
		}
		return item
	case t < 40:
		// 仅有效的 UTF-8 编码。
		var item string
		c.Fill(&item)
		return item
	case t < 60:
		var item int64
		c.Fill(&item)
		return item
	case t < 80:
		var item bool
		c.Fill(&item)
		return item
	case t < 100:
		return c.Rand.NormFloat64()
	case t < 120:
		return nil
	default:
		panic("invalid case")
	}
}

func unstructuredEqual(t *testing.T, a, b runtime.Unstructured) bool {
	return anyEqual(t, a.UnstructuredContent(), b.UnstructuredContent())
}

// 如果 int64 无法被 float64 精确表示，则两个数不可能相等，
// 直接返回 false，避免引入舍入误差。
func numberEqual(a int64, b float64) bool {
	// 确保 int64 到 float64 的往返转换无损。
	f, accuracy := big.NewInt(a).Float64()
	if accuracy == big.Exact {
		// JSON 往返转换不能保留所有数字的 int64 与 float64 类型差异。
		return f == b
	}
	return false
}

// anyEqual 递归比较任意 unstructured 值。b 必须是解码后允许的类型集合，
// 否则视为测试数据异常并直接 Fatal；a 可以是任意合法类型，遇到未知类型
// 同样 Fatal。数值比较走 numberEqual 以容忍 int64/float64 的规范化。
func anyEqual(t *testing.T, a, b interface{}) bool {
	switch b.(type) {
	case nil, bool, string, int64, float64, []interface{}, map[string]interface{}:
	default:
		t.Fatalf("unexpected value %v of type %T", b, b)
	}

	switch ac := a.(type) {
	case nil, bool, string:
		return ac == b
	case int64:
		if bc, ok := b.(float64); ok {
			return numberEqual(ac, bc)
		}
		return ac == b
	case float64:
		if bc, ok := b.(int64); ok {
			return numberEqual(bc, ac)
		}
		return ac == b
	case []interface{}:
		bc, ok := b.([]interface{})
		if !ok {
			return false
		}
		if len(ac) != len(bc) {
			return false
		}
		for i, aa := range ac {
			if !anyEqual(t, aa, bc[i]) {
				return false
			}
		}
		return true
	case map[string]interface{}:
		bc, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		if len(ac) != len(bc) {
			return false
		}
		for k, aa := range ac {
			bb, ok := bc[k]
			if !ok {
				return false
			}
			if !anyEqual(t, aa, bb) {
				return false
			}
		}
		return true
	default:
		t.Fatalf("unexpected value %v of type %T", a, a)
	}
	return true
}
