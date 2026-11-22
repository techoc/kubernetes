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

package util

import (
	"math/rand"
	"strings"
	"testing"
)

// TestLineBufferWrite 验证 LineBuffer.Write 把字符串和字符串切片拼接为一行，
// 以空格分隔并在末尾补换行；Reset 后缓冲区可复用且行数始终为 1。
func TestLineBufferWrite(t *testing.T) {
	testCases := []struct {
		name     string
		input    []interface{}
		expected string
	}{
		{
			name:     "none",
			input:    []interface{}{},
			expected: "\n",
		},
		{
			name:     "one string",
			input:    []interface{}{"test1"},
			expected: "test1\n",
		},
		{
			name:     "one slice",
			input:    []interface{}{[]string{"test1", "test2"}},
			expected: "test1 test2\n",
		},
		{
			name:     "mixed",
			input:    []interface{}{"s1", "s2", []string{"s3", "s4"}, "", "s5", []string{}, []string{"s6"}, "s7"},
			expected: "s1 s2 s3 s4  s5  s6 s7\n",
		},
	}
	testBuffer := NewLineBuffer()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 所有子用例共用一个缓冲区，必须重置，否则前一行内容会污染断言。
			testBuffer.Reset()
			testBuffer.Write(testCase.input...)
			if want, got := testCase.expected, testBuffer.String(); !strings.EqualFold(want, got) {
				t.Fatalf("write word is %v\n expected: %q, got: %q", testCase.input, want, got)
			}
			if testBuffer.Lines() != 1 {
				t.Fatalf("expected 1 line, got: %d", testBuffer.Lines())
			}
		})
	}
}

// TestLineBufferWritePanic 验证 Write 遇到不支持的类型（如 int）时会 panic，
// 避免静默丢弃本应记录的日志内容。
func TestLineBufferWritePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("did not panic")
		}
	}()
	// 前两个参数类型合法，panic 只能来自第三个 int 参数。
	testBuffer := NewLineBuffer()
	testBuffer.Write("string", []string{"a", "slice"}, 1234)
}

// TestLineBufferWriteBytes 验证 WriteBytes 原样写入字节内容并自动补上换行，
// 空字节输入只产生一个换行。
func TestLineBufferWriteBytes(t *testing.T) {
	testCases := []struct {
		name     string
		bytes    []byte
		expected string
	}{
		{
			name:     "empty bytes",
			bytes:    []byte{},
			expected: "\n",
		},
		{
			name:     "test bytes",
			bytes:    []byte("test write bytes line"),
			expected: "test write bytes line\n",
		},
	}

	testBuffer := NewLineBuffer()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// WriteBytes 与 Write 共用缓冲区，重置可隔离各子用例的结果。
			testBuffer.Reset()
			testBuffer.WriteBytes(testCase.bytes)
			if want, got := testCase.expected, testBuffer.String(); !strings.EqualFold(want, got) {
				t.Fatalf("write bytes is %v\n expected: %s, got: %s", testCase.bytes, want, got)
			}
		})
	}
}

// 字符集取自 https://stackoverflow.com/a/22892986，仅用于生成随机测试内容。
var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// randSeq 生成一个由大小写字母组成的 30 字符随机串，避免重复内容影响行数统计。
func randSeq() string {
	b := make([]rune, 30)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// TestWriteCountLines 验证写入 N 行后 Lines() 返回 N，
// 同时检查普通 LineBuffer 与 DiscardLineBuffer 的计数一致性，覆盖到十万行规模。
func TestWriteCountLines(t *testing.T) {
	testCases := []struct {
		name     string
		expected int
	}{
		{
			name:     "write no line",
			expected: 0,
		},
		{
			name:     "write one line",
			expected: 1,
		},
		{
			name:     "write 100 lines",
			expected: 100,
		},
		{
			name:     "write 1000 lines",
			expected: 1000,
		},
		{
			name:     "write 10000 lines",
			expected: 10000,
		},
		{
			name:     "write 100000 lines",
			expected: 100000,
		},
	}
	testBuffer := NewLineBuffer()
	discardBuffer := NewDiscardLineBuffer()
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 两个缓冲区在子用例间复用，重置确保计数从零开始累积。
			testBuffer.Reset()
			discardBuffer.Reset()
			for i := 0; i < testCase.expected; i++ {
				// 每行使用随机内容，防止因内容相同而掩盖行数统计错误。
				testBuffer.Write(randSeq())
				discardBuffer.Write(randSeq())
			}
			n := testBuffer.Lines()
			if n != testCase.expected {
				t.Fatalf("lines expected: %d, got: %d", testCase.expected, n)
			}
			n = discardBuffer.Lines()
			if n != testCase.expected {
				t.Fatalf("discardBuffer lines expected: %d, got: %d", testCase.expected, n)
			}
		})
	}
}
