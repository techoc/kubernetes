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

package json

import (
	gojson "encoding/json"
	"strings"
	"testing"

	utiljson "k8s.io/apimachinery/pkg/util/json"
)

// testcase 描述一个 JSON 解析压力用例：输入数据、期望的错误判定方式，
// 以及是否只用于基准测试（benchmark 用例在 TestJSONLimits 中被跳过）。
type testcase struct {
	name     string
	data     []byte
	checkErr func(t testing.TB, err error)

	benchmark bool
}

// testcases 构造覆盖各类嵌套结构的 3MB 输入。目的是比较标准库与
// apimachinery 的 JSON 实现在深层嵌套下的行为与性能上限。
func testcases() []testcase {
	// 只要报错即可：这类输入不保证有具体错误信息。
	nonNilError := func(t testing.TB, err error) {
		if err == nil {
			t.Errorf("expected error, got none")
		}
	}
	// 解析要么成功，要么因超过最大深度被拒绝——两者都是可接受的安全结果。
	successOrMaxDepthError := func(t testing.TB, err error) {
		if err != nil && !strings.Contains(err.Error(), "max depth") {
			t.Errorf("expected success or error containing 'max depth', got: %v", err)
		}
	}

	return []testcase{
		{
			name:     "3MB of deeply nested slices",
			checkErr: successOrMaxDepthError,
			data:     []byte(`{"a":` + strings.Repeat(`[`, 3*1024*1024/2) + strings.Repeat(`]`, 3*1024*1024/2) + "}"),
		},
		{
			name:     "3MB of unbalanced nested slices",
			checkErr: nonNilError,
			data:     []byte(`{"a":` + strings.Repeat(`[`, 3*1024*1024)),
		},
		{
			name:     "3MB of deeply nested maps",
			checkErr: successOrMaxDepthError,
			data:     []byte(strings.Repeat(`{"":`, 3*1024*1024/5/2) + "{}" + strings.Repeat(`}`, 3*1024*1024/5/2)),
		},
		{
			name:     "3MB of unbalanced nested maps",
			checkErr: nonNilError,
			data:     []byte(strings.Repeat(`{"":`, 3*1024*1024/5)),
		},
		{
			name:      "3MB of empty slices",
			data:      []byte(`{"a":[` + strings.Repeat(`[],`, 3*1024*1024/3-2) + `[]]}`),
			benchmark: true,
		},
		{
			name:      "3MB of slices",
			data:      []byte(`{"a":[` + strings.Repeat(`[0],`, 3*1024*1024/4-2) + `[0]]}`),
			benchmark: true,
		},
		{
			name:      "3MB of empty maps",
			data:      []byte(`{"a":[` + strings.Repeat(`{},`, 3*1024*1024/3-2) + `{}]}`),
			benchmark: true,
		},
		{
			name:      "3MB of maps",
			data:      []byte(`{"a":[` + strings.Repeat(`{"a":0},`, 3*1024*1024/8-2) + `{"a":0}]}`),
			benchmark: true,
		},
		{
			name:      "3MB of ints",
			data:      []byte(`{"a":[` + strings.Repeat(`0,`, 3*1024*1024/2-2) + `0]}`),
			benchmark: true,
		},
		{
			name:      "3MB of floats",
			data:      []byte(`{"a":[` + strings.Repeat(`0.0,`, 3*1024*1024/4-2) + `0.0]}`),
			benchmark: true,
		},
		{
			name:      "3MB of bools",
			data:      []byte(`{"a":[` + strings.Repeat(`true,`, 3*1024*1024/5-2) + `true]}`),
			benchmark: true,
		},
		{
			name:      "3MB of empty strings",
			data:      []byte(`{"a":[` + strings.Repeat(`"",`, 3*1024*1024/3-2) + `""]}`),
			benchmark: true,
		},
		{
			name:      "3MB of strings",
			data:      []byte(`{"a":[` + strings.Repeat(`"abcdefghijklmnopqrstuvwxyz012",`, 3*1024*1024/30-2) + `"abcdefghijklmnopqrstuvwxyz012"]}`),
			benchmark: true,
		},
		{
			name:      "3MB of nulls",
			data:      []byte(`{"a":[` + strings.Repeat(`null,`, 3*1024*1024/5-2) + `null]}`),
			benchmark: true,
		},
	}
}

// decoders 是被比较的两个 JSON 解码器：标准库与 apimachinery 的替换实现。
var decoders = map[string]func([]byte, interface{}) error{
	"gojson":   gojson.Unmarshal,
	"utiljson": utiljson.Unmarshal,
}

// TestJSONLimits 用两个解码器分别解析深层嵌套/不平衡的 JSON，验证二者对
// 最大深度与畸形输入的处理一致（不会因实现差异而出现未受控的递归或崩溃）。
func TestJSONLimits(t *testing.T) {
	for _, tc := range testcases() {
		if tc.benchmark {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			for decoderName, decoder := range decoders {
				t.Run(decoderName, func(t *testing.T) {
					v := map[string]interface{}{}
					err := decoder(tc.data, &v)

					if tc.checkErr != nil {
						tc.checkErr(t, err)
					} else if err != nil {
						t.Errorf("unexpected error: %v", err)
					}
				})
			}
		})
	}
}

// BenchmarkJSONLimits 用同一组 3MB 输入对两个解码器做基准测试，
// 关注大输入下的吞吐与分配差异。
func BenchmarkJSONLimits(b *testing.B) {
	for _, tc := range testcases() {
		b.Run(tc.name, func(b *testing.B) {
			for decoderName, decoder := range decoders {
				b.Run(decoderName, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						v := map[string]interface{}{}
						err := decoder(tc.data, &v)

						if tc.checkErr != nil {
							tc.checkErr(b, err)
						} else if err != nil {
							b.Errorf("unexpected error: %v", err)
						}
					}
				})
			}
		})
	}
}
