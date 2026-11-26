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

package cbor_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"

	"github.com/google/go-cmp/cmp"
)

// TestFrameReaderReadError 验证帧读取器遇到输入流中的格式错误后不会继续读取。
// RFC 8742 第 2.8 节规定："[...] 序列中任一数据项格式不正确，就无法可靠解码其余部分。"
//
// TestFrameReaderReadError 验证 frame reader 遇到输入流中的格式错误后不会恢复读取：
// 按 RFC 8742 §2.8，序列中任一数据项格式非法时，无法可靠解码后续内容。
func TestFrameReaderReadError(t *testing.T) {
	input := []byte{
		0xff, // 非法的初始 break
		0xa0, // 后跟格式正确的空 map
	}
	fr := cbor.NewFramer().NewFrameReader(io.NopCloser(bytes.NewReader(input)))
	for i := 0; i < 3; i++ {
		n, err := fr.Read(nil)
		if err == nil || errors.Is(err, io.ErrShortBuffer) {
			t.Fatalf("expected a non-nil error other than io.ErrShortBuffer, got: %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 bytes read on error, got %d", n)
		}
	}
}

// TestFrameReaderRead 验证 frame reader 在多种分块方式下正确重组每个 frame：
// 覆盖连续 frame、零长度目标缓冲、以及跨多次读取溢出的 frame；
// 并检查读取不会写到目标切片长度之外。
func TestFrameReaderRead(t *testing.T) {
	// ChunkedFrame 是单个 frame 按读取次数切分后的字节块序列。
	type ChunkedFrame [][]byte

	for _, tc := range []struct {
		Name   string
		Frames []ChunkedFrame
	}{
		{
			Name: "consecutive frames",
			Frames: []ChunkedFrame{
				[][]byte{{0xa0}},
				[][]byte{{0xa0}},
			},
		},
		{
			Name: "zero-length destination buffer",
			Frames: []ChunkedFrame{
				[][]byte{{}, {0xa0}},
			},
		},
		{
			Name: "overflow",
			Frames: []ChunkedFrame{
				[][]byte{
					{0x43},
					{'x'},
					{'y', 'z'},
				},
				[][]byte{
					{0xa1, 0x43, 'f', 'o', 'o'},
					{'b'},
					{'a', 'r'},
				},
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			var concatenation []byte
			for _, f := range tc.Frames {
				for _, c := range f {
					concatenation = append(concatenation, c...)
				}
			}

			fr := cbor.NewFramer().NewFrameReader(io.NopCloser(bytes.NewReader(concatenation)))

			for _, frame := range tc.Frames {
				var want, got []byte
				for i, chunk := range frame {
					// 目标切片长度与 chunk 相同、容量加倍，并在容量区间预填 0xff，
					// 以便检测读取是否越界写入。
					dst := make([]byte, len(chunk), 2*len(chunk))
					for i := len(dst); i < cap(dst); i++ {
						dst[:cap(dst)][i] = 0xff
					}
					n, err := fr.Read(dst)
					if n != len(chunk) {
						t.Errorf("expected %d bytes read, got %d", len(chunk), n)
					}
					if i == len(frame)-1 && err != nil {
						t.Errorf("unexpected non-nil error on last read of frame: %v", err)
					} else if i < len(frame)-1 && !errors.Is(err, io.ErrShortBuffer) {
						t.Errorf("expected io.ErrShortBuffer on all but the last read of a frame, got: %v", err)
					}
					for i := len(dst); i < cap(dst); i++ {
						if dst[:cap(dst)][i] != 0xff {
							t.Errorf("read mutated underlying array beyond slice length: %#v", dst[len(dst):cap(dst)])
							break
						}
					}
					want = append(want, chunk...)
					got = append(got, dst...)
				}
				if diff := cmp.Diff(want, got); diff != "" {
					t.Errorf("reassembled frame differs:\n%s", diff)
				}
			}
		})
	}
}

// fakeReadCloser 是用于测试 Close 行为的 ReadCloser，Read 总是返回 (0, nil)。
type fakeReadCloser struct {
	err error
}

func (rc fakeReadCloser) Read(_ []byte) (int, error) {
	return 0, nil
}

func (rc fakeReadCloser) Close() error {
	return rc.err
}

// TestFrameReaderClose 验证 Close 会把底层 ReadCloser 的错误原样返回。
func TestFrameReaderClose(t *testing.T) {
	want := errors.New("test")
	if got := cbor.NewFramer().NewFrameReader(fakeReadCloser{err: want}).Close(); !errors.Is(got, want) {
		t.Errorf("got error %v, want %v", got, want)
	}
}
