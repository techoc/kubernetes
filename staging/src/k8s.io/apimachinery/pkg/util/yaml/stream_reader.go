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

// Package yaml 提供 YAML/JSON 解码与流式读取工具。
package yaml

import "io"

// StreamReader 用于消费长度可变的流式消息。它会缓存数据直到调用方显式 Consume，
// 并可以回退以重读之前的数据——这是在同一字节流上先探测格式、再真正解析所必需的。
type StreamReader struct {
	r           io.Reader
	buf         []byte
	head        int // 当前在 buf 中的读取偏移
	ttlConsumed int // 已消费的字节总数
}

// NewStreamReader 创建包装给定 io.Reader 的 StreamReader。
// size 是初始缓冲区容量；传 0 时使用 4096 作为合理默认值。
func NewStreamReader(r io.Reader, size int) *StreamReader {
	if size == 0 {
		size = 4096
	}
	return &StreamReader{
		r:   r,
		buf: make([]byte, 0, size), // 以合理容量起步，减少小对象分配
	}
}

// Read 实现 io.Reader。它先返回当前偏移之后的已缓存数据；若已耗尽，则从底层 reader
// 读取并缓存。返回的数据在被调用 Consume 之前不算已消费，因此仍可回退重读。
func (r *StreamReader) Read(p []byte) (n int, err error) {
	// 有缓存数据时优先返回
	if r.head < len(r.buf) {
		n = copy(p, r.buf[r.head:])
		r.head += n
		return n, nil
	}

	// 已经遇到 EOF 时直接返回
	if r.r == nil {
		return 0, io.EOF
	}

	// 从底层 reader 读取
	n, err = r.r.Read(p)
	if n > 0 {
		r.buf = append(r.buf, p[:n]...)
		r.head += n
	}
	if err == nil {
		return n, nil
	}
	if err == io.EOF {
		// 通过把 r 置为 nil 记住已经到达 EOF，避免重复触达底层 reader
		r.r = nil
	}
	return n, err
}

// ReadN 从 reader 精确读取 n 字节，阻塞直到读满或出错。出错时返回已读字节及错误；
// 若未读满 n 字节就遇到 EOF，则返回已读部分和 io.EOF。返回的数据在被调用 Consume
// 之前不算已消费。
func (r *StreamReader) ReadN(want int) ([]byte, error) {
	ret := make([]byte, want)
	off := 0
	for off < want {
		n, err := r.Read(ret[off:])
		if err != nil {
			return ret[:off+n], err
		}
		off += n
	}
	return ret, nil
}

// Peek 返回接下来的 n 字节但不推进 reader。返回的字节在下一次调用 Consume 之前有效。
func (r *StreamReader) Peek(n int) ([]byte, error) {
	buf, err := r.ReadN(n)
	// 无论是否出错都回退，保证 Peek 不改变可读位置。
	r.RewindN(len(buf))
	if err != nil {
		return buf, err
	}
	return buf, nil
}

// Rewind 把 reader 重置到已缓存数据的起始位置。
func (r *StreamReader) Rewind() {
	r.head = 0
}

// RewindN 把 reader 回退 n 字节。若 n 超过当前偏移，则回退到缓冲区起始处，不会越界为负。
func (r *StreamReader) RewindN(n int) {
	r.head -= min(n, r.head)
}

// Consume 从缓冲区头部丢弃最多 n 字节已读数据；被消费的数据将无法再回退读取。
// 若 n 超过当前缓冲区长度，则清空缓冲区。Consume 永远不会去消费底层 reader 的数据。
func (r *StreamReader) Consume(n int) {
	n = min(n, len(r.buf))
	r.buf = r.buf[n:]
	r.head -= n
	r.ttlConsumed += n
}

// Consumed 返回从输入 reader 中累计消费的字节数。
func (r *StreamReader) Consumed() int {
	return r.ttlConsumed
}
