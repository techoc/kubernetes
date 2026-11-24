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

// Splice 表示把已有字节切片直接拼接到输出缓冲区，而不复制底层数据。调用方必须保证被拼接的字节在缓冲区使用期间不会被修改或释放。
type Splice interface {
	Splice([]byte)
	io.Writer
	Reset()
	Bytes() []byte
}

// spliceBuffer 同时实现 Splice 与 io.Writer 接口。
type spliceBuffer struct {
	// raw 是不拷贝直接引用的数据；buf 是常规写入累积的数据。
	raw []byte
	buf *bytes.Buffer
}

// NewSpliceBuffer 创建一个空的 Splice 缓冲区。
func NewSpliceBuffer() Splice {
	return &spliceBuffer{}
}

// Splice 将 raw 保存为待拼接数据，不复制底层字节数组。
func (sb *spliceBuffer) Splice(raw []byte) {
	sb.raw = raw
}

// 把普通数据写入内部 bytes.Buffer。缓冲区采用延迟初始化：如果只调用 Splice 而不调用 Write，就不需要分配 bytes.Buffer。
func (sb *spliceBuffer) Write(p []byte) (n int, err error) {
	if sb.buf == nil {
		sb.buf = &bytes.Buffer{}
	}
	return sb.buf.Write(p)
}

// 清空缓冲区并释放对 raw 切片的引用。bytes.Buffer.Reset 会保留其底层容量以供下次复用，raw 置 nil 则避免意外延长外部数据生命周期。
func (sb *spliceBuffer) Reset() {
	if sb.buf != nil {
		sb.buf.Reset()
	}
	sb.raw = nil
}

// 返回当前缓冲区内容。若存在普通 Write 写入的数据，优先返回 buf；否则返回 Splice 保存的原始切片；两者都没有时返回空切片。
func (sb *spliceBuffer) Bytes() []byte {
	if sb.buf != nil && len(sb.buf.Bytes()) > 0 {
		return sb.buf.Bytes()
	}
	if sb.raw != nil {
		return sb.raw
	}
	return []byte{}
}
