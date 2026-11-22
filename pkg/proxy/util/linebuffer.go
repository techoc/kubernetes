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
	"bytes"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
)

// LineBuffer 是将一行行输入写入 bytes.Buffer 的接口。
type LineBuffer interface {
	// Write 接收一系列参数，每个参数为 string 或 []string；它把所有单独的字符串用空格连接，
	// 以换行符结尾后写入缓冲区。任何其他类型的参数都会引发 panic。
	Write(args ...interface{})

	// WriteBytes 将字节写入缓冲区，并以换行符结尾。
	WriteBytes(bytes []byte)

	// Reset 清空缓冲区。
	Reset()

	// Bytes 以 []byte 形式返回缓冲区的内容。
	Bytes() []byte

	// String 以字符串形式返回缓冲区的内容。
	String() string

	// Lines 返回缓冲区的行数。更准确地说，它返回 Write() 或 WriteBytes() 被调用的次数；
	// 这里假定调用方从未自行向缓冲区写入过换行符。
	Lines() int
}

// 编译期断言：确保 realLineBuffer 实现 logr.Marshaler，便于日志系统按行输出内容。
var _ logr.Marshaler = &realLineBuffer{}

// realLineBuffer 是真正保存数据的 LineBuffer 实现。
// 内部使用 bytes.Buffer，其容量会按需自动增长，因此不需要预先估算并分配空间。
type realLineBuffer struct {
	b     bytes.Buffer
	lines int
}

// NewLineBuffer 返回一个新的“真实” LineBuffer。
func NewLineBuffer() LineBuffer {
	return &realLineBuffer{}
}

// Write 是 LineBuffer 接口的组成部分。
func (buf *realLineBuffer) Write(args ...interface{}) {
	for i, arg := range args {
		// 除第一个参数外，在每个参数前补一个空格，使同一行内的各参数彼此分隔。
		if i > 0 {
			buf.b.WriteByte(' ')
		}
		switch x := arg.(type) {
		case string:
			buf.b.WriteString(x)
		case []string:
			for j, s := range x {
				// 切片内的元素之间同样用空格分隔。
				if j > 0 {
					buf.b.WriteByte(' ')
				}
				buf.b.WriteString(s)
			}
		default:
			panic(fmt.Sprintf("unknown argument type: %T", x))
		}
	}
	// 一次 Write 对应一行：补上换行符并同步增加计数，保证 Lines() 与写入次数一致。
	buf.b.WriteByte('\n')
	buf.lines++
}

// WriteBytes 是 LineBuffer 接口的组成部分。
func (buf *realLineBuffer) WriteBytes(bytes []byte) {
	buf.b.Write(bytes)
	// 与 Write 保持一致：原始字节之后同样补换行并计入行数。
	buf.b.WriteByte('\n')
	buf.lines++
}

// Reset 是 LineBuffer 接口的组成部分。
func (buf *realLineBuffer) Reset() {
	buf.b.Reset()
	// 缓冲区清空后计数必须归零，否则 Lines() 会与实际内容不符。
	buf.lines = 0
}

// Bytes 是 LineBuffer 接口的组成部分。
func (buf *realLineBuffer) Bytes() []byte {
	return buf.b.Bytes()
}

// String 是 LineBuffer 接口的组成部分。
func (buf *realLineBuffer) String() string {
	return buf.b.String()
}

// Lines 是 LineBuffer 接口的组成部分。
func (buf *realLineBuffer) Lines() int {
	return buf.lines
}

// 实现 logs.Marshaler 接口，让日志系统按行展示缓冲区内容。
func (buf *realLineBuffer) MarshalLog() any {
	return strings.Split(buf.b.String(), "\n")
}

// discardLineBuffer 只统计写入次数而不保留数据，用于无需真实生成规则内容的场景。
type discardLineBuffer struct {
	lines int
}

// NewDiscardLineBuffer 返回一个假的 LineBuffer：它只统计写入次数，却丢弃全部数据。
// （用于 iptables 代理的部分同步，以统计我们成功避免了多少条规则的同步。）
func NewDiscardLineBuffer() LineBuffer {
	return &discardLineBuffer{}
}

// Write 是 LineBuffer 接口的组成部分。
func (buf *discardLineBuffer) Write(args ...interface{}) {
	// 参数内容无需保存，只为统计同步量而累加次数。
	buf.lines++
}

// WriteBytes 是 LineBuffer 接口的组成部分。
func (buf *discardLineBuffer) WriteBytes(bytes []byte) {
	// 同上：字节内容被丢弃，仅记录写入次数。
	buf.lines++
}

// Reset 是 LineBuffer 接口的组成部分。
func (buf *discardLineBuffer) Reset() {
	buf.lines = 0
}

// Bytes 是 LineBuffer 接口的组成部分。
func (buf *discardLineBuffer) Bytes() []byte {
	// 不保存任何内容，因此始终返回空切片。
	return []byte{}
}

// String 是 LineBuffer 接口的组成部分。
func (buf *discardLineBuffer) String() string {
	// 不保存任何内容，因此始终返回空字符串。
	return ""
}

// Lines 是 LineBuffer 接口的组成部分。
func (buf *discardLineBuffer) Lines() int {
	return buf.lines
}
