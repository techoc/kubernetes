/*
Copyright The Kubernetes Authors.

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

package v1

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestKeyValueCompat 验证 KeyValue 的自定义 JSON 编解码与 1.34 之前的行为保持兼容。
//
// 测试用例覆盖以下几类输入：
//   - nil 切片：应编码为 JSON null；
//   - 空切片：应编码为 []；
//   - 零值 KeyValue：应编码为 [{}]；
//   - ASCII 字符串：作为普通字符串编码，而不是 base64；
//   - 合法 UTF-8 字符串：原样保留；
//   - 非法 UTF-8 字节：编码时被替换为 U+FFFD，并且能正确往返。
//
// 每个用例除了校验编码结果（expectedJSON），还会校验反序列化后的值（expectedRoundTripped）。
// 对于非法 UTF-8 用例，variantJSON 提供另一种包含原始非法字节的 JSON 输入，验证解码路径同样做替换。
func TestKeyValueCompat(t *testing.T) {
	// 定义表驱动测试的用例结构：
	// name 为用例名；envs 为待编码的输入；variantJSON 为可选的替代 JSON 输入；
	// expectedJSON 为期望的编码结果；expectedRoundTripped 为期望的解码结果。
	testcases := []struct {
		name                 string
		envs                 []*KeyValue
		variantJSON          string
		expectedJSON         string
		expectedRoundTripped []*KeyValue
	}{
		{
			// nil 切片编码为 JSON null，解码回来也应为 nil。
			name:                 "null",
			envs:                 nil,
			expectedJSON:         `null`,
			expectedRoundTripped: nil,
		},
		{
			// 非 nil 的空切片编码为空数组 []，解码回来仍为非 nil 空切片。
			name:                 "zero-length list",
			envs:                 []*KeyValue{},
			expectedJSON:         `[]`,
			expectedRoundTripped: []*KeyValue{},
		},
		{
			// 零值 KeyValue 的字段都带 omitempty，因此编码为空对象 {}。
			name:                 "zero-value env",
			envs:                 []*KeyValue{{}},
			expectedJSON:         `[{}]`,
			expectedRoundTripped: []*KeyValue{{}},
		},
		{
			// ASCII 字符串按普通字符串编码，验证不是 base64。
			name:                 "ascii env",
			envs:                 []*KeyValue{{Key: "key", Value: []byte("value")}},
			expectedJSON:         `[{"key":"key","value":"value"}]`,
			expectedRoundTripped: []*KeyValue{{Key: "key", Value: []byte("value")}},
		},
		{
			// 合法的多字节 UTF-8（含 emoji）应原样编解码，不被替换。
			name:                 "utf8 env",
			envs:                 []*KeyValue{{Key: "key", Value: []byte("Iñtërnâtiônàlizætiøn🐹")}},
			expectedJSON:         `[{"key":"key","value":"Iñtërnâtiônàlizætiøn🐹"}]`,
			expectedRoundTripped: []*KeyValue{{Key: "key", Value: []byte("Iñtërnâtiônàlizætiøn🐹")}},
		},
		{
			// 非法 UTF-8 字节（0x80 是非法的续字节）用例。
			name:                 "non-utf8 env",
			envs:                 []*KeyValue{{Key: "key", Value: []byte{'A', 0x80, 'Z'}}},              // invalid utf8 continuation byte (0x80)
			variantJSON:          `[{"key":"key","value":"A` + "\x80" + `Z"}]`,                          // an alternate JSON input containing the invalid utf8 byte that should coerce to the same result
			expectedJSON:         `[{"key":"key","value":"A` + stdlibSerializedReplacementChar + `Z"}]`, // coerced to utf8 replacement character (\ufffd) on marshal
			expectedRoundTripped: []*KeyValue{{Key: "key", Value: []byte("A\ufffdZ")}},                  // round-trips to replacement character (\ufffd) on unmarshal
		},
	}

	// 遍历所有用例，逐个以子测试形式运行，便于定位失败的具体场景。
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 第一步：把输入编码为 JSON。
			data, err := json.Marshal(tc.envs)
			if err != nil {
				t.Fatal(err)
			}

			// 第二步：与期望的 JSON 字符串做精确比对，验证编码格式兼容旧版本。
			if string(data) != tc.expectedJSON {
				t.Fatalf("json differed:\nwant: %s\ngot:  %s", tc.expectedJSON, string(data))
			}

			// 第三步：把刚编码的结果反序列化，验证往返后值保持一致。
			verifyJSON(t, data, tc.expectedRoundTripped)
			// 第四步：若提供了替代 JSON 输入，再单独验证该输入的解码行为。
			// 这覆盖了"输入 JSON 中直接带有非法 UTF-8 字节"的场景。
			if len(tc.variantJSON) > 0 {
				verifyJSON(t, []byte(tc.variantJSON), tc.expectedRoundTripped)
			}
		})
	}
}

// verifyJSON 验证给定的 JSON 数据反序列化后与期望的 KeyValue 切片一致。
// 它先检查 nil 性，再检查长度，最后逐项比较 Key 和 Value 两个字段。
func verifyJSON(t *testing.T, data []byte, expectedRoundTripped []*KeyValue) {
	t.Helper()

	var rt []*KeyValue
	// 反序列化失败直接终止当前测试。
	if err := json.Unmarshal(data, &rt); err != nil {
		t.Fatal(err)
	}
	// 先比较 nil 性：区分 nil 与空切片，因为二者在 JSON 中分别对应 null 和 []。
	if (rt == nil) != (expectedRoundTripped == nil) {
		t.Fatalf("expected value (%#v) does not match actual round-tripped value (%#v) for nil", expectedRoundTripped, rt)
	}
	// 期望为 nil 时已经比对通过，无需继续。
	if rt == nil {
		return
	}
	// 再比较切片长度，避免下面按下标访问时越界。
	if len(rt) != len(expectedRoundTripped) {
		t.Fatalf("length of expected value (%#v) does not match length of actual round-tripped value (%#v)", expectedRoundTripped, rt)
	}
	// 最后逐项比较：Key 用字符串比较，Value 是字节切片，需要用 bytes.Equal。
	for i := range expectedRoundTripped {
		if want, got := expectedRoundTripped[i].Key, rt[i].Key; want != got {
			t.Fatalf("item[%d].key does not match: %s vs %s", i, want, got)
		}
		if want, got := expectedRoundTripped[i].Value, rt[i].Value; !bytes.Equal(want, got) {
			t.Fatalf("item[%d].value does not match: %v vs %v", i, want, got)
		}
	}
}
