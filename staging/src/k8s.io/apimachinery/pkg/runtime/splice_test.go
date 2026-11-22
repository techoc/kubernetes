/*
Copyright 2023 The Kubernetes Authors.

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

package runtime_test

import (
	"bytes"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

// TestSpliceBuffer 验证 runtime.Splice 在写入、零拷贝拼接、重置以及三者
// 交叉组合下的行为与 bytes.Buffer 一致。Splice 用于把已有字节切片直接
// 拼接到输出中而不复制。
func TestSpliceBuffer(t *testing.T) {
	testBytes0 := []byte{0x01, 0x02, 0x03, 0x04}
	testBytes1 := []byte{0x04, 0x03, 0x02, 0x02}

	testCases := []struct {
		name string
		run  func(sb runtime.Splice, buf *bytes.Buffer)
	}{
		{
			name: "Basic Write",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				sb.Write(testBytes0)
				buf.Write(testBytes0)
			},
		},
		{
			// 逐字节写入：验证多次 Write 的累积结果。
			name: "Multiple Writes",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				for _, b := range testBytes0 {
					sb.Write([]byte{b})
					buf.Write([]byte{b})
				}
			},
		},
		{
			// Reset 之后缓冲区必须回到空状态，与 bytes.Buffer 一致。
			name: "Write and Reset",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				sb.Write(testBytes0)
				buf.Write(testBytes0)

				sb.Reset()
				buf.Reset()
			},
		},
		{
			// Splice 零拷贝拼接必须与 Write 的可见结果相同。
			name: "Write/Splice",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				sb.Splice(testBytes0)
				buf.Write(testBytes0)
			},
		},
		{
			// 拼接后再 Reset，不能残留之前的 splice 引用。
			name: "Write/Splice and Reset",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				sb.Splice(testBytes0)
				buf.Write(testBytes0)

				sb.Reset()
				buf.Reset()
			},
		},
		{
			// 重置后再次拼接，验证缓冲区状态被彻底清理。
			name: "Write/Splice, Reset, Write/Splice",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				sb.Splice(testBytes0)
				buf.Write(testBytes0)

				sb.Reset()
				buf.Reset()

				sb.Splice(testBytes1)
				buf.Write(testBytes1)
			},
		},
		{
			// 先 Write 再 Reset 再 Splice，覆盖两种写入方式的状态切换。
			name: "Write, Reset, Splice",
			run: func(sb runtime.Splice, buf *bytes.Buffer) {
				sb.Write(testBytes0)
				buf.Write(testBytes0)

				sb.Reset()
				buf.Reset()

				sb.Splice(testBytes1)
				buf.Write(testBytes1)
			},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			sb := runtime.NewSpliceBuffer()
			buf := &bytes.Buffer{}
			tt.run(sb, buf)

			if sb.Bytes() == nil {
				t.Errorf("Unexpected nil")
			}
			// 与 bytes.Buffer 的字节序列逐字节比较。
			if string(sb.Bytes()) != string(buf.Bytes()) {
				t.Errorf("Expected sb.Bytes() == %q, buf.Bytes() == %q", sb.Bytes(), buf.Bytes())
			}
		})

	}

}
