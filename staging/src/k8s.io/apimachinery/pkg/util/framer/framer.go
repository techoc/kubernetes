/*
Copyright 2015 The Kubernetes Authors.

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

// Package framer 为 io.ReadCloser 提供简单的流式分帧与解帧能力。
// 支持 4 字节大端长度前缀协议，以及以连续 JSON 对象边界作为帧边界的协议。
package framer

import (
	"encoding/binary"
	"encoding/json"
	"io"
)

// lengthDelimitedFrameWriter 在每次 Write 前写入 4 字节长度前缀。
type lengthDelimitedFrameWriter struct {
	w io.Writer
	h [4]byte
}

// NewLengthDelimitedFrameWriter 返回 writer，每次 Write 先写入 4 字节大端长度前缀，再写入正文。
func NewLengthDelimitedFrameWriter(w io.Writer) io.Writer {
	return &lengthDelimitedFrameWriter{w: w}
}

// Write 将单帧写入底层 writer。写入正文前先写入 4 字节大端 uint32，表示正文的字节长度。
func (w *lengthDelimitedFrameWriter) Write(data []byte) (int, error) {
	binary.BigEndian.PutUint32(w.h[:], uint32(len(data)))
	n, err := w.w.Write(w.h[:])
	if err != nil {
		return 0, err
	}
	// 短写说明底层 writer 状态不可信，继续写 body 会产生错位数据。
	if n != len(w.h) {
		return 0, io.ErrShortWrite
	}
	return w.w.Write(data)
}

// lengthDelimitedFrameReader 按长度前缀读取一帧；remaining 记录当前帧尚未
// 读出的字节数，跨 Read 调用保持状态。
type lengthDelimitedFrameReader struct {
	r         io.ReadCloser
	remaining int
}

// NewLengthDelimitedFrameReader 返回一个 io.ReadCloser，从字节流中读取带长度前缀的帧。
//
// 协议结构如下：
//
//	stream: message ...
//	message: prefix body
//	prefix: 4 字节大端 uint32，表示 body 的长度
//	body: 长度为 prefix 的原始字节
//
// 如果传给 Read 的缓冲区不足以容纳完整帧，会返回已读取字节数以及 io.ErrShortBuffer；
// 调用方可以继续读取同一帧的剩余部分。
func NewLengthDelimitedFrameReader(r io.ReadCloser) io.ReadCloser {
	return &lengthDelimitedFrameReader{r: r}
}

// Read 尝试将一整帧读入 data。缓冲区不足时返回 io.ErrShortBuffer，后续调用会继续读取
// 当前帧剩余内容；只有完整帧读取成功时才返回 nil 错误。
func (r *lengthDelimitedFrameReader) Read(data []byte) (int, error) {
	if r.remaining <= 0 {
		header := [4]byte{}
		// 先读满 4 字节头部；ReadAtLeast 保证短读时返回错误。
		n, err := io.ReadAtLeast(r.r, header[:4], 4)
		if err != nil {
			return 0, err
		}
		if n != 4 {
			return 0, io.ErrUnexpectedEOF
		}
		frameLength := int(binary.BigEndian.Uint32(header[:]))
		r.remaining = frameLength
	}

	expect := r.remaining
	max := expect
	if max > len(data) {
		max = len(data)
	}
	n, err := io.ReadAtLeast(r.r, data[:max], int(max))
	r.remaining -= n
	if err != nil {
		return n, err
	}
	if r.remaining > 0 {
		// 调用方缓冲区不够大：本帧未读完，下次调用继续。
		return n, io.ErrShortBuffer
	}
	if n != expect {
		return n, io.ErrUnexpectedEOF
	}

	return n, nil
}

func (r *lengthDelimitedFrameReader) Close() error {
	return r.r.Close()
}

// jsonFrameReader 使用 json.Decoder 把连续 JSON 对象拆成独立帧；remaining
// 保存上一次因缓冲区不足而未返回的尾部字节。
type jsonFrameReader struct {
	r         io.ReadCloser
	decoder   *json.Decoder
	remaining []byte
}

// NewJSONFramedReader 返回一个 io.ReadCloser，从连续数据流中逐个读取 JSON 对象。
// 每一帧的边界由完整 JSON 对象决定；遇到 JSON 解析错误时，本次读取终止并返回错误。
func NewJSONFramedReader(r io.ReadCloser) io.ReadCloser {
	return &jsonFrameReader{
		r:       r,
		decoder: json.NewDecoder(r),
	}
}

// Read 读取流中的下一个 JSON 对象。返回的字节切片可能在下一次 Read 调用时被复用或修改，
// 调用方不应在后续读取期间继续依赖其内容，也不应修改该切片。
func (r *jsonFrameReader) Read(data []byte) (int, error) {
	// 优先返回上一帧因缓冲区不足而暂存的剩余字节。
	if n := len(r.remaining); n > 0 {
		if n <= len(data) {
			//nolint:staticcheck // SA4006,SA4010：此处会修改 data 的底层数组。
			data = append(data[0:0], r.remaining...)
			r.remaining = nil
			return n, nil
		}

		n = len(data)
		//nolint:staticcheck // SA4006,SA4010：此处会修改 data 的底层数组。
		data = append(data[0:0], r.remaining[:n]...)
		r.remaining = r.remaining[n:]
		return n, io.ErrShortBuffer
	}

	// json.RawMessage 的解码会向切片追加数据。先将长度重置为 0；解码结果要么写入 data 的
	// 底层数组，要么因容量不足而分配新的数组。
	m := json.RawMessage(data[:0])
	if err := r.decoder.Decode(&m); err != nil {
		return 0, err
	}

	// 如果 data 的容量小于完整消息长度，解码器会分配新切片并让 m 指向新数组。
	// 此时需要把当前缓冲区能容纳的部分复制回 data，并暂存剩余部分供后续 Read 返回。
	if len(m) > cap(data) {
		copy(data, m)
		r.remaining = m[len(data):]
		return len(data), io.ErrShortBuffer
	}

	if len(m) > len(data) {
		// 超出 len(data) 的字节写入了 data 的底层数组；函数返回后该数组归调用方所有，
		// 因此必须复制剩余数据，避免后续被调用方复用时覆盖。
		r.remaining = append([]byte(nil), m[len(data):]...)
		return len(data), io.ErrShortBuffer
	}

	return len(m), nil
}

func (r *jsonFrameReader) Close() error {
	return r.r.Close()
}
