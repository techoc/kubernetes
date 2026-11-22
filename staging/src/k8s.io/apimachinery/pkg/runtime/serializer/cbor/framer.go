/*
Copyright 2024 The Kubernetes Authors.

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

package cbor

import (
	"io"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/fxamacker/cbor/v2"
)

// NewFramer returns a runtime.Framer based on RFC 8742 CBOR Sequences. Each frame contains exactly
// one encoded CBOR data item.
func NewFramer() runtime.Framer {
	return framer{}
}

var _ runtime.Framer = framer{}

// framer 实现 RFC 8742 CBOR Sequence 的分帧：每个数据项自定界，无需额外长度前缀。
type framer struct{}

// NewFrameReader 用 CBOR decoder 逐项读取 frame。
func (framer) NewFrameReader(rc io.ReadCloser) io.ReadCloser {
	return &frameReader{
		decoder: cbor.NewDecoder(rc),
		closer:  rc,
	}
}

// NewFrameWriter 直接返回底层 writer。
func (framer) NewFrameWriter(w io.Writer) io.Writer {
	// Each data item in a CBOR sequence is self-delimiting (like JSON objects).
	return w
}

// frameReader 把 CBOR Sequence 适配成 io.Reader：每次 Read 返回一个完整数据项。
type frameReader struct {
	decoder *cbor.Decoder
	closer  io.Closer

	// overflow 保存上一次 Read 因目标缓冲区过小而剩余的数据。
	overflow []byte
}

// Read 读取一个 CBOR 数据项并写入 dst；若数据项大于 dst，返回
// io.ErrShortBuffer 并把剩余字节暂存到 overflow。
func (fr *frameReader) Read(dst []byte) (int, error) {
	if len(fr.overflow) > 0 {
		// We read a frame that was too large for the destination slice in a previous call
		// to Read and have bytes left over.
		n := copy(dst, fr.overflow)
		if n < len(fr.overflow) {
			fr.overflow = fr.overflow[n:]
			return n, io.ErrShortBuffer
		}
		fr.overflow = nil
		return n, nil
	}

	// The Reader contract allows implementations to use all of dst[0:len(dst)] as scratch
	// space, even if n < len(dst), but it does not allow implementations to use
	// dst[len(dst):cap(dst)]. Slicing it up-front allows us to append to it without worrying
	// about overwriting dst[len(dst):cap(dst)].
	//
	// 三索引切片把容量限制在 len(dst)，decoder 需要扩容时会自行分配新数组，
	// 从而不会越界写入调用方缓冲区的尾部。
	m := cbor.RawMessage(dst[0:0:len(dst)])
	if err := fr.decoder.Decode(&m); err != nil {
		return 0, err
	}

	if len(m) > len(dst) {
		// The frame was too big, m has a newly-allocated underlying array to accommodate
		// it.
		fr.overflow = m[len(dst):]
		return copy(dst, m), io.ErrShortBuffer
	}

	return len(m), nil
}

// Close 关闭底层 reader。
func (fr *frameReader) Close() error {
	return fr.closer.Close()
}
