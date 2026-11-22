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

package runtime

import (
	"bytes"
	"io"
)

// Splice is the interface that wraps the Splice method.
//
// Splice moves data from given slice without copying the underlying data for
// efficiency purpose. Therefore, the caller should make sure the underlying
// data is not changed later.
//
// Splice 与 io.Writer 的写入结果会合并到同一输出：Bytes 优先返回 Write 写入的内容，
// 没有时才返回 Splice 的数据。这正是编码器"先写头部、再拼接预编码 body"的用法。
type Splice interface {
	Splice([]byte)
	io.Writer
	Reset()
	Bytes() []byte
}

// A spliceBuffer implements Splice and io.Writer interfaces.
type spliceBuffer struct {
	// raw 是不拷贝直接引用的数据；buf 是常规写入累积的数据。
	raw []byte
	buf *bytes.Buffer
}

// NewSpliceBuffer 创建一个空的 Splice 缓冲区。
func NewSpliceBuffer() Splice {
	return &spliceBuffer{}
}

// Splice implements the Splice interface.
func (sb *spliceBuffer) Splice(raw []byte) {
	sb.raw = raw
}

// Write implements the io.Writer interface.
//
// buf 惰性初始化：只用 Splice 的调用方不会产生 bytes.Buffer 分配。
func (sb *spliceBuffer) Write(p []byte) (n int, err error) {
	if sb.buf == nil {
		sb.buf = &bytes.Buffer{}
	}
	return sb.buf.Write(p)
}

// Reset resets the buffer to be empty.
//
// 保留 buf 的底层容量以便复用，但丢弃对 raw 的引用，避免意外延长其生命周期。
func (sb *spliceBuffer) Reset() {
	if sb.buf != nil {
		sb.buf.Reset()
	}
	sb.raw = nil
}

// Bytes returns the data held by the buffer.
//
// 若同时存在写入数据与 splice 数据，只返回写入数据——约定上 Splice 仅在
// 尚未 Write 时使用。
func (sb *spliceBuffer) Bytes() []byte {
	if sb.buf != nil && len(sb.buf.Bytes()) > 0 {
		return sb.buf.Bytes()
	}
	if sb.raw != nil {
		return sb.raw
	}
	return []byte{}
}
