/*
Copyright 2022 The Kubernetes Authors.

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

package format_test

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/onsi/gomega/format"
)

func TestGomegaFormatObject(t *testing.T) {
	for name, test := range map[string]struct {
		value       interface{}
		expected    string
		indentation uint
	}{
		"int":            {value: 1, expected: `<int>: 1`},
		"string":         {value: "hello world", expected: `<string>: hello world`},
		"struct":         {value: myStruct{a: 1, b: 2}, expected: `<format_test.myStruct>: {a: 1, b: 2}`},
		"gomegastringer": {value: typeWithGomegaStringer(2), expected: `<format_test.typeWithGomegaStringer>: my stringer 2`},

		// v1.Pod 在 test/utils/format 中测试。

		// 对于 unstructured.Unstructured，对应的测试使用的是真实类型。
		// 预期结果稍有不同，因为“我们的” myUnstructured 没有特殊的
		// JSON 编码方法，但对于测试是否使用了 YAML 而言，这已经足够。
		"unstructured-pointer": {
			value: &myUnstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"name": "some-name",
					},
				},
			},
			expected: `<*format_test.myUnstructured | <hex>>: 
    Object:
      metadata:
        name: some-name`,
		},
		"unstructured-value": {
			value: myUnstructured{
				Object: map[string]any{
					"metadata": map[string]any{
						"name": "some-name",
					},
				},
			},
			expected: `<format_test.myUnstructured>: 
    Object:
      metadata:
        name: some-name`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			actual := format.Object(test.value, test.indentation)
			actual = regexp.MustCompile(`\| 0x[a-z0-9]+`).ReplaceAllString(actual, `| <hex>`)
			if test.expected != actual {
				t.Errorf("Expected:\n%s\nActual:\n%s\n", test.expected, actual)
			}
		})
	}

}

type typeWithGomegaStringer int

func (v typeWithGomegaStringer) GomegaString() string {
	return fmt.Sprintf("my stringer %d", v)
}

type myStruct struct {
	a, b int
}

type myUnstructured struct {
	Object map[string]any
}
