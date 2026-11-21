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
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// TestDeepCopyApiObjects 对所有注册类型的 internal 与 v1 版本做随机 DeepCopy 测试。
// 多轮随机 fuzz 是为了覆盖尽可能多的字段组合，尤其是嵌套指针和切片。
func TestDeepCopyApiObjects(t *testing.T) {
	for i := 0; i < *roundtrip.FuzzIters; i++ {
		for _, version := range []schema.GroupVersion{{Group: "", Version: runtime.APIVersionInternal}, {Group: "", Version: "v1"}} {
			// 每轮创建新 fuzzer，让不同版本和轮次使用不同的随机种子。
			f := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(rand.Int63()), legacyscheme.Codecs)
			for kind := range legacyscheme.Scheme.KnownTypes(version) {
				doDeepCopyTest(t, version.WithKind(kind), f)
			}
		}
	}
}

// doDeepCopyTest 验证单个 GVK 的 DeepCopy 既是值相等的，也是内存独立的。
func doDeepCopyTest(t *testing.T, kind schema.GroupVersionKind, f *randfill.Filler) {
	item, err := legacyscheme.Scheme.New(kind)
	if err != nil {
		t.Fatalf("Could not create a %v: %s", kind, err)
	}
	f.Fill(item)
	itemCopy := item.DeepCopyObject()
	// 先验证拷贝内容一致；更深层的内存独立性由后续 re-fuzz 检查保证。
	if !reflect.DeepEqual(item, itemCopy) {
		t.Errorf("\nexpected: %#v\n\ngot:      %#v\n\ndiff:      %v", item, itemCopy, cmp.Diff(item, itemCopy))
	}

	// 序列化原始对象作为基线，便于用字节比较捕捉后续任何意外改动。
	prefuzzData := &bytes.Buffer{}
	if err := legacyscheme.Codecs.LegacyCodec(kind.GroupVersion()).Encode(item, prefuzzData); err != nil {
		t.Errorf("Could not encode a %v: %s", kind, err)
		return
	}

	// 再次 fuzz 拷贝：若 DeepCopy 真正独立，原对象不应被改动。
	f.Fill(itemCopy)

	postfuzzData := &bytes.Buffer{}
	if err := legacyscheme.Codecs.LegacyCodec(kind.GroupVersion()).Encode(item, postfuzzData); err != nil {
		t.Errorf("Could not encode a %v: %s", kind, err)
		return
	}

	// 前后编码结果必须一致，否则说明拷贝与原始对象共享了内存。
	if !bytes.Equal(prefuzzData.Bytes(), postfuzzData.Bytes()) {
		t.Log(cmp.Diff(prefuzzData.String(), postfuzzData.String()))
		t.Errorf("Fuzzing copy modified original of %#v", kind)
		return
	}
}

// TestDeepCopySingleType 只针对 Pod 反复 fuzz，作为深入调试某类对象拷贝问题的快捷入口。
func TestDeepCopySingleType(t *testing.T) {
	for i := 0; i < *roundtrip.FuzzIters; i++ {
		for _, version := range []schema.GroupVersion{{Group: "", Version: runtime.APIVersionInternal}, {Group: "", Version: "v1"}} {
			f := fuzzer.FuzzerFor(FuzzerFuncs, rand.NewSource(rand.Int63()), legacyscheme.Codecs)
			doDeepCopyTest(t, version.WithKind("Pod"), f)
		}
	}
}
