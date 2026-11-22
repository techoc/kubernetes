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

package internal

import "testing"

// TestPathElementRoundTrip 验证路径元素字符串的解析与再序列化是幂等的，
// 覆盖字段、索引、key（含 JSON 对象）与 value（含 null/字符串/数字/对象）四类。
func TestPathElementRoundTrip(t *testing.T) {
	tests := []string{
		`i:0`,
		`i:1234`,
		`f:`,
		`f:spec`,
		`f:more-complicated-string`,
		`k:{"name":"my-container"}`,
		`k:{"port":"8080","protocol":"TCP"}`,
		`k:{"optionalField":null}`,
		`k:{"jsonField":{"A":1,"B":null,"C":"D","E":{"F":"G"}}}`,
		`k:{"listField":["1","2","3"]}`,
		`v:null`,
		`v:"some-string"`,
		`v:1234`,
		`v:{"some":"json"}`,
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			pe, err := NewPathElement(test)
			if err != nil {
				t.Fatalf("Failed to create path element: %v", err)
			}
			output, err := PathElementString(pe)
			if err != nil {
				t.Fatalf("Failed to create string from path element: %v", err)
			}
			if test != output {
				t.Fatalf("Expected round-trip:\ninput: %v\noutput: %v", test, output)
			}
		})
	}
}

// TestPathElementIgnoreUnknown 验证未知限定符（如 r:）被忽略而不是报错，
// 以便向前兼容未来新增的路径元素类型。
func TestPathElementIgnoreUnknown(t *testing.T) {
	_, err := NewPathElement("r:Hello")
	if err != nil {
		t.Fatalf("Unknown qualifiers should be ignored")
	}
}

// TestNewPathElementError 覆盖各类非法输入：缺冒号、索引非整数、
// 空索引、非法 JSON 等都必须返回错误。
func TestNewPathElementError(t *testing.T) {
	tests := []string{
		``,
		`no-colon`,
		`i:index is not a number`,
		`i:1.23`,
		`i:`,
		`v:invalid json`,
		`v:`,
		`k:invalid json`,
		`k:{"name":invalid}`,
	}

	for _, test := range tests {
		t.Run(test, func(t *testing.T) {
			_, err := NewPathElement(test)
			if err == nil {
				t.Fatalf("Expected error, no error found")
			}
		})
	}
}
