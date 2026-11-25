/*
Copyright 2025 The Kubernetes Authors.

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

package yaml

import (
	"io"
	"strings"
	"testing"
)

// srt = StreamReaderTest
// srtStep 描述对 StreamReader 的一次操作及其期望结果；
// expected 只对读类操作有意义，size 对不同操作分别是缓冲区大小、回退/消费字节数或期望计数。
type srtStep struct {
	op       string
	expected string
	err      error
	size     int
}

// srtRead 构造一次 Read 操作步骤。
func srtRead(size int, expected string, err error) srtStep {
	return srtStep{op: "Read", size: size, expected: expected, err: err}
}

// srtReadN 构造一次 ReadN（读满或报错）操作步骤。
func srtReadN(size int, expected string, err error) srtStep {
	return srtStep{op: "ReadN", size: size, expected: expected, err: err}
}

// srtPeek 构造一次 Peek 操作步骤。
func srtPeek(size int, expected string, err error) srtStep {
	return srtStep{op: "Peek", size: size, expected: expected, err: err}
}

// srtRewind 构造一次回退到已消费起点的操作。
func srtRewind() srtStep {
	return srtStep{op: "Rewind"}
}

// srtRewindN 构造一次回退 N 字节的操作。
func srtRewindN(size int) srtStep {
	return srtStep{op: "RewindN", size: size}
}

// srtConsume 构造一次跳过/丢弃 N 字节的操作。
func srtConsume(size int) srtStep {
	return srtStep{op: "Consume", size: size}
}

// srtConsumed 构造一次断言当前已消费字节数的操作。
func srtConsumed(exp int) srtStep {
	return srtStep{op: "Consumed", size: exp}
}

// srtRun 按顺序执行步骤；错误用 == 比较，因为测试依赖具体的 io.EOF 等哨兵值。
func srtRun(t *testing.T, reader *StreamReader, steps []srtStep) {
	t.Helper()

	checkRead := func(i int, step srtStep, buf []byte, err error) {
		t.Helper()
		if err != nil && step.err == nil {
			t.Errorf("step %d: unexpected error: %v", i, err)
		} else if err == nil && step.err != nil {
			t.Errorf("step %d: expected error %v", i, step.err)
		} else if err != nil && err != step.err { //nolint:errorlint
			t.Errorf("step %d: expected error %v, got %v", i, step.err, err)
		}
		if got := string(buf); got != step.expected {
			t.Errorf("step %d: expected %q, got %q", i, step.expected, got)
		}
	}

	for i, step := range steps {
		switch step.op {
		case "Read":
			buf := make([]byte, step.size)
			n, err := reader.Read(buf)
			buf = buf[:n]
			checkRead(i, step, buf, err)
		case "ReadN":
			buf, err := reader.ReadN(step.size)
			checkRead(i, step, buf, err)
		case "Peek":
			buf, err := reader.Peek(step.size)
			checkRead(i, step, buf, err)
		case "Rewind":
			reader.Rewind()
		case "RewindN":
			reader.RewindN(step.size)
		case "Consume":
			reader.Consume(step.size)
		case "Consumed":
			if n := reader.Consumed(); n != step.size {
				t.Errorf("step %d: expected %d consumed, got %d", i, step.size, n)
			}
		default:
			t.Fatalf("step %d: unknown operation %q", i, step.op)
		}
	}
}

// TestStreamReader_Read 覆盖 Read 的基本契约：
// 读满请求量时无错误；到达 EOF 时返回剩余数据（短读）且 err 为 nil，
// 再次读取才返回 io.EOF。
func TestStreamReader_Read(t *testing.T) {
	tests := []struct {
		name  string
		input string
		steps []srtStep
	}{{
		name:  "empty input",
		input: "",
		steps: []srtStep{
			srtRead(1, "", io.EOF),
			srtRead(1, "", io.EOF), //仍为 EOF
		},
	}, {
		name:  "simple reads",
		input: "0123456789",
		steps: []srtStep{
			srtRead(5, "01234", nil),
			srtRead(5, "56789", nil),
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "short read at EOF",
		input: "0123456789",
		steps: []srtStep{
			srtRead(8, "01234567", nil),
			srtRead(8, "89", nil), //短读但无错误
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "short reads from buffer",
		input: "0123456789",
		steps: []srtStep{
			srtRead(3, "012", nil), //填充缓冲区
			srtRewind(),
			// 回退后缓冲区里只有 3 字节，Read 不会为了满足请求而继续读底层，
			// 这是 StreamReader 有意保留的"短读"语义。
			srtRead(4, "012", nil), //从缓冲区短读
			srtRewind(),
			srtRead(4, "012", nil),  //仍然不足
			srtRead(4, "3456", nil), //从 reader 读取
			srtRewind(),
			srtRead(10, "0123456", nil), //从缓冲区短读
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamReader(strings.NewReader(tt.input), 4) //较小的初始缓冲区
			srtRun(t, reader, tt.steps)
		})
	}
}

// TestStreamReader_Rewind 验证 Rewind 能回到"已消费位置"而不是流的绝对起点，
// 因此多次 Rewind 后仍能读到尚未消费的数据。
func TestStreamReader_Rewind(t *testing.T) {
	tests := []struct {
		name  string
		input string
		steps []srtStep
	}{{
		name:  "simple read and rewind",
		input: "0123456789",
		steps: []srtStep{
			srtRead(4, "0123", nil),
			srtRead(4, "4567", nil),
			srtRead(4, "89", nil),
			srtRead(1, "", io.EOF),
			srtRewind(),
			srtRead(4, "0123", nil),
			srtRead(4, "4567", nil),
			srtRead(4, "89", nil),
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "multiple rewinds",
		input: "01234",
		steps: []srtStep{
			srtRead(2, "01", nil),
			srtRewind(),
			srtRead(2, "01", nil),
			srtRead(2, "23", nil),
			srtRewind(),
			srtRead(2, "01", nil),
			srtRead(2, "23", nil),
			srtRead(2, "4", nil),
			srtRead(1, "", io.EOF),
			srtRewind(),
			srtRead(100, "01234", nil),
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "empty input",
		input: "",
		steps: []srtStep{
			srtRead(1, "", io.EOF),
			srtRewind(),
			srtRead(1, "", io.EOF),
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamReader(strings.NewReader(tt.input), 4) //较小的初始缓冲区
			srtRun(t, reader, tt.steps)
		})
	}
}

// TestStreamReader_RewindN 验证按字节回退，且回退量超过已读内容时会被钳制，
// 不会 panic 或读到错误位置。
func TestStreamReader_RewindN(t *testing.T) {
	tests := []struct {
		name  string
		input string
		steps []srtStep
	}{{
		name:  "simple rewindn",
		input: "0123456789",
		steps: []srtStep{
			srtRead(4, "0123", nil),
			srtRead(4, "4567", nil),
			srtRead(4, "89", nil),
			srtRead(1, "", io.EOF),
			srtRewindN(4),
			srtRead(2, "67", nil),
			srtRewindN(4),
			srtRead(10, "456789", nil),
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "empty input",
		input: "",
		steps: []srtStep{
			srtRead(1, "", io.EOF),
			srtRewindN(100),
			srtRead(1, "", io.EOF),
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamReader(strings.NewReader(tt.input), 4) //较小的初始缓冲区
			srtRun(t, reader, tt.steps)
		})
	}
}

// TestStreamReader_Consume 验证 Consume 永久丢弃字节并推进"已消费"计数；
// 消费量超过剩余数据时计数保持在可消费上限，不会越界。
func TestStreamReader_Consume(t *testing.T) {
	tests := []struct {
		name  string
		input string
		steps []srtStep
	}{{
		name:  "simple consume",
		input: "0123456789",
		steps: []srtStep{
			srtConsumed(0),
			srtRead(4, "0123", nil),
			srtRead(4, "4567", nil),
			srtConsume(2), //丢弃 01
			srtConsumed(2),
			srtRead(4, "89", nil),
			srtRead(1, "", io.EOF),
			srtRewind(),
			srtRead(5, "23456", nil),
			srtRead(5, "789", nil),
			srtRead(1, "", io.EOF),
			srtConsumed(2),
		},
	}, {
		name:  "consume too much",
		input: "01234",
		steps: []srtStep{
			srtConsumed(0),
			srtRead(5, "01234", nil),
			srtConsume(5),
			srtConsumed(5),
			srtConsume(5),
			srtConsumed(5),
			srtRead(1, "", io.EOF),
			srtConsume(5),
			srtConsumed(5),
			srtRead(1, "", io.EOF),
			srtConsumed(5),
		},
	}, {
		name:  "empty input",
		input: "",
		steps: []srtStep{
			srtConsumed(0),
			srtConsume(5),
			srtRead(1, "", io.EOF),
			srtConsumed(0),
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamReader(strings.NewReader(tt.input), 4) //较小的初始缓冲区
			srtRun(t, reader, tt.steps)
		})
	}
}

// TestStreamReader_ReadN 验证 ReadN 与 Read 的关键差异：
// ReadN 会尽量读满请求量（必要时从底层继续读），只有遇到 EOF 才返回短读加 io.EOF。
func TestStreamReader_ReadN(t *testing.T) {
	tests := []struct {
		name  string
		input string
		steps []srtStep
	}{{
		name:  "short read full readN",
		input: "0123456789",
		steps: []srtStep{
			srtRead(3, "012", nil), //填充缓冲区
			srtRewind(),
			srtRead(5, "012", nil), //从缓冲区短读
			srtRewind(),
			srtReadN(5, "01234", nil), //完整 readN
			srtRewind(),
			srtRead(10, "01234", nil), //从缓冲区短读
			srtRewind(),
			srtReadN(10, "0123456789", nil), //完整 readN
			srtRewind(),
			srtRead(10, "0123456789", nil), //从缓冲区完整读取
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "short read consume readN",
		input: "0123456789",
		steps: []srtStep{
			srtRead(3, "012", nil), //填充缓冲区
			srtRewind(),
			srtRead(4, "012", nil), //从缓冲区短读
			srtConsume(1),
			srtRewind(),
			srtRead(4, "12", nil), //从缓冲区短读
			srtRewind(),
			srtReadN(4, "1234", nil), //完整读取
			srtConsume(1),
			srtRewind(),
			srtRead(4, "234", nil), //从缓冲区短读
			srtRewind(),
			srtReadN(10, "23456789", io.EOF), //短读 readN，返回 EOF
			srtRewind(),
			srtRead(10, "23456789", nil), //从缓冲区完整读取
			srtRead(1, "", io.EOF),
		},
	}, {
		name:  "empty input",
		input: "",
		steps: []srtStep{
			srtReadN(1, "", io.EOF),
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamReader(strings.NewReader(tt.input), 4) //较小的初始缓冲区
			srtRun(t, reader, tt.steps)
		})
	}
}

// TestStreamReader_Peek 验证 Peek 只填充缓冲区而不推进读位置，
// 因此随后 Read 仍从头开始；初始缓冲区为 0 时也要能正确扩容。
func TestStreamReader_Peek(t *testing.T) {
	tests := []struct {
		name  string
		input string
		steps []srtStep
	}{{
		name:  "simple peek",
		input: "0123456789",
		steps: []srtStep{
			srtPeek(3, "012", nil), //填充缓冲区
			srtRead(5, "012", nil), //从缓冲区短读
			srtRewind(),
			srtPeek(6, "012345", nil),  //填充缓冲区
			srtRead(10, "012345", nil), //从缓冲区短读
		},
	}, {
		name:  "empty input",
		input: "",
		steps: []srtStep{
			srtPeek(1, "", io.EOF),
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := NewStreamReader(strings.NewReader(tt.input), 0)
			srtRun(t, reader, tt.steps)
		})
	}
}
