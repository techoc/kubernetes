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

// Package streaming 实现了通过 io.Writer/Reader 传输
// runtime.Object 流的编码器与解码器。
package streaming

import (
	"bytes"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Encoder 是作用在流上的 runtime.Encoder。
type Encoder interface {
	// Encode 会将给定对象写入流，或返回错误。它遵循与
	// runtime.VersionedEncoder 相同的约定。
	Encode(obj runtime.Object) error
}

// Decoder 是从流中读取的 runtime.Decoder。
type Decoder interface {
	// 当没有更多对象可用时，Decode 将返回 io.EOF。
	Decode(defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error)
	// Close 关闭底层流。
	Close() error
}

// Serializer 是用于创建面向流工作的编码器和解码器的工厂。
type Serializer interface {
	NewEncoder(w io.Writer) Encoder
	NewDecoder(r io.ReadCloser) Decoder
}

type decoder struct {
	reader    io.ReadCloser
	decoder   runtime.Decoder
	buf       []byte
	maxBytes  int
	resetRead bool
}

// NewDecoder 创建一个流式解码器，它从 r 读取对象数据块并用 d 进行解码。
// 如果提供的缓冲区不足以读取完整对象，reader 应返回 ErrShortRead。
func NewDecoder(r io.ReadCloser, d runtime.Decoder) Decoder {
	return &decoder{
		reader:   r,
		decoder:  d,
		buf:      make([]byte, 1024),
		maxBytes: 16 * 1024 * 1024,
	}
}

var ErrObjectTooLarge = fmt.Errorf("object to decode was longer than maximum allowed size")

// Decode 从流中读取下一个对象并解码。
func (d *decoder) Decode(defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	base := 0
	for {
		// 当帧放不下时，reader 可能返回 io.ErrShortBuffer；
		// 此时扩大缓冲区并重试。
		n, err := d.reader.Read(d.buf[base:])
		if err == io.ErrShortBuffer {
			if n == 0 {
				// 没有进展的短读取会导致无限循环。
				return nil, nil, fmt.Errorf("got short buffer with n=0, base=%d, cap=%d", base, cap(d.buf))
			}
			if d.resetRead {
				// 仍在排空超大帧；继续消费，直到 reader
				// 不再返回 ErrShortBuffer。
				continue
			}
			// 将缓冲区大小加倍，最大不超过 maxBytes
			if len(d.buf) < d.maxBytes {
				base += n
				d.buf = append(d.buf, make([]byte, len(d.buf))...)
				continue
			}
			// 必须读完帧的剩余部分（直到不再收到 ErrShortBuffer）。
			// 现在退出会让流停留在帧中间，破坏下一次解码，
			// 所以先排空再报告错误。
			d.resetRead = true
			return nil, nil, ErrObjectTooLarge
		}
		if err != nil {
			return nil, nil, err
		}
		if d.resetRead {
			// 已排空超大的读取内容，继续执行
			d.resetRead = false
			continue
		}
		base += n
		break
	}
	return d.decoder.Decode(d.buf[:base], defaults, into)
}

func (d *decoder) Close() error {
	return d.reader.Close()
}

type encoder struct {
	writer  io.Writer
	encoder runtime.Encoder
	buf     *bytes.Buffer
}

// NewEncoder 返回一个新的流式编码器。
func NewEncoder(w io.Writer, e runtime.Encoder) Encoder {
	return &encoder{
		writer:  w,
		encoder: e,
		buf:     &bytes.Buffer{},
	}
}

// Encode 将给定对象写入内层的 writer。
func (e *encoder) Encode(obj runtime.Object) error {
	// 先编码到中间缓冲区，避免部分写入破坏流：
	// 整个帧通过一次 Write 调用写出。
	if err := e.encoder.Encode(obj, e.buf); err != nil {
		return err
	}
	_, err := e.writer.Write(e.buf.Bytes())
	e.buf.Reset()
	return err
}
