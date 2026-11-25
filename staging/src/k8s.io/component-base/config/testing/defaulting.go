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

package testing

import (
	"fmt"
	"path/filepath"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

// DefaultingTest 对给定 scheme 运行默认化测试。
// 测试数据位于各包 testdata/<Kind>/before|after/<version>.yaml，
// 通过 YAML 解码比较默认化前后结果，覆盖编解码与默认化两条路径。
func DefaultingTest(t *testing.T, scheme *runtime.Scheme, codecs serializer.CodecFactory) {
	cases := GetDefaultingTestCases(t, scheme, codecs)
	RunTestsOnYAMLData(t, cases)
}

// GetDefaultingTestCases 返回给定 scheme 的默认化测试用例。
// 内部版本跳过：它们不是用户可见的序列化格式，默认化通过外部版本完成。
func GetDefaultingTestCases(t *testing.T, scheme *runtime.Scheme, codecs serializer.CodecFactory) []TestCase {
	cases := []TestCase{}
	for gvk := range scheme.AllKnownTypes() {
		if gvk.Version == runtime.APIVersionInternal {
			continue
		}
		// 每个 Kind 的外部版本共用同一组 before/after 目录，文件名按版本区分。
		beforeDir := fmt.Sprintf("testdata/%s/before", gvk.Kind)
		afterDir := fmt.Sprintf("testdata/%s/after", gvk.Kind)
		filename := fmt.Sprintf("%s.yaml", gvk.Version)

		codec, err := getCodecForGV(codecs, gvk.GroupVersion())
		if err != nil {
			t.Fatal(err)
		}

		cases = append(cases, TestCase{
			name:  fmt.Sprintf("%s default_%s", gvk.Kind, gvk.Version),
			in:    filepath.Join(beforeDir, filename),
			out:   filepath.Join(afterDir, filename),
			codec: codec,
		})
	}
	return cases
}
