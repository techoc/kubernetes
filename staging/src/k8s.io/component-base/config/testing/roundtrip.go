/*
Copyright 2019 The Kubernetes Authors.

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

// Package testing 提供配置类型在不同 API 版本间往返的通用测试辅助。
// 它基于 scheme 中已注册的全部版本，自动为 testdata 下的用例生成交叉编解码测试。
package testing

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// RoundTripTest 为给定 scheme 运行往返测试，测试数据来自 testdata。
func RoundTripTest(t *testing.T, scheme *runtime.Scheme, codecs serializer.CodecFactory) {
	tc := GetRoundtripTestCases(t, scheme, codecs)
	RunTestsOnYAMLData(t, tc)
}

// GetRoundtripTestCases 为给定 scheme 生成往返测试用例。
// 对每个 GroupKind 的任意输入/输出版本组合，从 testdata/<Kind>/roundtrip/<case>
// 读取对应的 YAML 作为输入与期望输出。
func GetRoundtripTestCases(t *testing.T, scheme *runtime.Scheme, codecs serializer.CodecFactory) []TestCase {
	cases := []TestCase{}
	// 收集每个 GroupKind 已注册的外部版本（内部版本不参与编解码）。
	versionsForKind := map[schema.GroupKind][]string{}
	for gvk := range scheme.AllKnownTypes() {
		if gvk.Version != runtime.APIVersionInternal {
			versionsForKind[gvk.GroupKind()] = append(versionsForKind[gvk.GroupKind()], gvk.Version)
		}
	}

	for gk, versions := range versionsForKind {
		testdir := filepath.Join("testdata", gk.Kind, "roundtrip")
		dirs, err := os.ReadDir(testdir)
		if err != nil {
			t.Fatalf("failed to read testdir %s: %v", testdir, err)
		}

		for _, dir := range dirs {
			for _, vin := range versions {
				for _, vout := range versions {
					marshalGVK := gk.WithVersion(vout)
					codec, err := getCodecForGV(codecs, marshalGVK.GroupVersion())
					if err != nil {
						t.Fatalf("failed to get codec for %v: %v", marshalGVK.GroupVersion().String(), err)
					}

					testname := dir.Name()
					cases = append(cases, TestCase{
						name:  fmt.Sprintf("%s_%sTo%s_%s", gk.Kind, vin, vout, testname),
						in:    filepath.Join(testdir, testname, vin+".yaml"),
						out:   filepath.Join(testdir, testname, vout+".yaml"),
						codec: codec,
					})
				}
			}
		}
	}
	return cases
}

// roundTrip 执行单个往返用例：解码输入、编码、再解码，并逐项比对。
func roundTrip(t *testing.T, tc TestCase) {
	object := decodeYAML(t, tc.in, tc.codec)

	// original 保存内部类型对象的原始形态，用于检测编码是否意外修改了对象。
	original := object

	// 用给定 codec 序列化对象。
	data, err := runtime.Encode(tc.codec, object)
	if err != nil {
		t.Fatalf("failed to encode object: %v", err)
	}

	// 编码不应修改原对象（例如填充默认值或重排字段）。
	if !apiequality.Semantic.DeepEqual(original, object) {
		t.Fatalf("encode altered the object, diff (- want, + got): \n%v", cmp.Diff(original, object))
	}

	// 把编码结果解码回对象。
	obj2, err := runtime.Decode(tc.codec, data)
	if err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	// 解码得到的对象必须与原始对象语义相等。
	if !apiequality.Semantic.DeepEqual(original, obj2) {
		t.Fatalf("object was not the same after roundtrip, diff (- want, + got):\n%v", cmp.Diff(object, obj2))
	}

	// 与输入文件对比，确认往返后的输出与期望一致。
	matchOutputFile(t, data, tc.out)
}
