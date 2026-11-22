/*
Copyright 2016 The Kubernetes Authors.

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

package framer

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	netutil "k8s.io/apimachinery/pkg/util/net"
)

// TestRead 用 4 字节大端长度前缀的帧流验证读取语义：
// 缓冲区小于帧时返回 ErrShortBuffer，恰好/大于帧时正常返回，空帧返回 0 字节，
// 流末尾返回 io.EOF。
func TestRead(t *testing.T) {
	data := []byte{
		0x00, 0x00, 0x00, 0x04,
		0x01, 0x02, 0x03, 0x04,
		0x00, 0x00, 0x00, 0x03,
		0x05, 0x06, 0x07,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x08,
	}
	b := bytes.NewBuffer(data)
	r := NewLengthDelimitedFrameReader(io.NopCloser(b))
	buf := make([]byte, 1)
	if n, err := r.Read(buf); err != io.ErrShortBuffer && n != 1 && bytes.Equal(buf, []byte{0x01}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	if n, err := r.Read(buf); err != io.ErrShortBuffer && n != 1 && bytes.Equal(buf, []byte{0x02}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read the remaining frame
	buf = make([]byte, 2)
	if n, err := r.Read(buf); err != nil && n != 2 && bytes.Equal(buf, []byte{0x03, 0x04}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read with buffer equal to frame
	buf = make([]byte, 3)
	if n, err := r.Read(buf); err != nil && n != 3 && bytes.Equal(buf, []byte{0x05, 0x06, 0x07}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read empty frame
	buf = make([]byte, 3)
	if n, err := r.Read(buf); err != nil && n != 0 && bytes.Equal(buf, []byte{}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read with larger buffer than frame
	buf = make([]byte, 3)
	if n, err := r.Read(buf); err != nil && n != 1 && bytes.Equal(buf, []byte{0x08}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read EOF
	if n, err := r.Read(buf); err != io.EOF && n != 0 {
		t.Fatalf("unexpected: %v %d", err, n)
	}
}

// TestReadLarge 使用远大于各帧的缓冲区，验证每次 Read 只返回一帧、不会跨帧拼接。
func TestReadLarge(t *testing.T) {
	data := []byte{
		0x00, 0x00, 0x00, 0x04,
		0x01, 0x02, 0x03, 0x04,
		0x00, 0x00, 0x00, 0x03,
		0x05, 0x06, 0x07,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x08,
	}
	b := bytes.NewBuffer(data)
	r := NewLengthDelimitedFrameReader(io.NopCloser(b))
	buf := make([]byte, 40)
	if n, err := r.Read(buf); err != nil && n != 4 && bytes.Equal(buf, []byte{0x01, 0x02, 0x03, 0x04}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil && n != 3 && bytes.Equal(buf, []byte{0x05, 0x06, 0x7}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil && n != 0 && bytes.Equal(buf, []byte{}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil && n != 1 && bytes.Equal(buf, []byte{0x08}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read EOF
	if n, err := r.Read(buf); err != io.EOF && n != 0 {
		t.Fatalf("unexpected: %v %d", err, n)
	}
}

// TestReadInvalidFrame 验证帧被截断时：先返回已读到的部分数据，再以 io.ErrUnexpectedEOF 结束。
func TestReadInvalidFrame(t *testing.T) {
	data := []byte{
		0x00, 0x00, 0x00, 0x04,
		0x01, 0x02,
	}
	b := bytes.NewBuffer(data)
	r := NewLengthDelimitedFrameReader(io.NopCloser(b))
	buf := make([]byte, 1)
	if n, err := r.Read(buf); err != io.ErrShortBuffer && n != 1 && bytes.Equal(buf, []byte{0x01}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read the remaining frame
	buf = make([]byte, 3)
	if n, err := r.Read(buf); err != io.ErrUnexpectedEOF && n != 1 && bytes.Equal(buf, []byte{0x02}) {
		t.Fatalf("unexpected: %v %d %v", err, n, buf)
	}
	// read EOF
	if n, err := r.Read(buf); err != io.EOF && n != 0 {
		t.Fatalf("unexpected: %v %d", err, n)
	}
}

// TestReadClientTimeout 验证底层 HTTP 连接超时会被原样透出，
// 使调用方能通过 netutil.IsTimeout 识别并重试，而不会误判为格式错误。
func TestReadClientTimeout(t *testing.T) {
	header := []byte{
		0x00, 0x00, 0x00, 0x04,
	}
	data := []byte{
		0x01, 0x02, 0x03, 0x04,
		0x00, 0x00, 0x00, 0x03,
		0x05, 0x06, 0x07,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x01,
		0x08,
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(header)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(1 * time.Second)
		_, _ = w.Write(data)
	}))
	defer server.Close()

	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}

	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	r := NewLengthDelimitedFrameReader(resp.Body)
	buf := make([]byte, 1)
	if n, err := r.Read(buf); err == nil || !netutil.IsTimeout(err) {
		t.Fatalf("unexpected: %v %d", err, n)
	}
}

// TestJSONFrameReader 验证 JSON 帧读取器按空白/换行切分连续 JSON 值，
// 支持对象、数字、数组等非自定界值。
func TestJSONFrameReader(t *testing.T) {
	b := bytes.NewBufferString("{\"test\":true}\n1\n[\"a\"]")
	r := NewJSONFramedReader(io.NopCloser(b))
	buf := make([]byte, 20)
	if n, err := r.Read(buf); err != nil || n != 13 || string(buf[:n]) != `{"test":true}` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil || n != 1 || string(buf[:n]) != `1` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil || n != 5 || string(buf[:n]) != `["a"]` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != io.EOF || n != 0 {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
}

// TestJSONFrameReaderShortBuffer 验证缓冲区小于一个 JSON 值时，
// 读取器会分块返回并附带 io.ErrShortBuffer，直到该值读完。
func TestJSONFrameReaderShortBuffer(t *testing.T) {
	b := bytes.NewBufferString("{\"test\":true}\n1\n[\"a\"]")
	r := NewJSONFramedReader(io.NopCloser(b))
	buf := make([]byte, 3)

	if n, err := r.Read(buf); err != io.ErrShortBuffer || n != 3 || string(buf[:n]) != `{"t` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != io.ErrShortBuffer || n != 3 || string(buf[:n]) != `est` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != io.ErrShortBuffer || n != 3 || string(buf[:n]) != `":t` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != io.ErrShortBuffer || n != 3 || string(buf[:n]) != `rue` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil || n != 1 || string(buf[:n]) != `}` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}

	if n, err := r.Read(buf); err != nil || n != 1 || string(buf[:n]) != `1` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}

	if n, err := r.Read(buf); err != io.ErrShortBuffer || n != 3 || string(buf[:n]) != `["a` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	if n, err := r.Read(buf); err != nil || n != 2 || string(buf[:n]) != `"]` {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}

	if n, err := r.Read(buf); err != io.EOF || n != 0 {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
}

// TestJSONFrameReaderShortBufferNoUnderlyingArrayReuse 验证读取器不会复用调用方的底层数组：
// 在 ShortBuffer 之后改写缓冲内容，剩余数据仍能正确读出。
func TestJSONFrameReaderShortBufferNoUnderlyingArrayReuse(t *testing.T) {
	b := bytes.NewBufferString("{}")
	r := NewJSONFramedReader(io.NopCloser(b))
	buf := make([]byte, 1, 2) // cap(buf) > len(buf) && cap(buf) <= len("{}")

	if n, err := r.Read(buf); !errors.Is(err, io.ErrShortBuffer) || n != 1 || string(buf[:n]) != "{" {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
	buf = append(buf, make([]byte, 1)...) // stomps the second byte of the backing array
	if n, err := r.Read(buf[1:]); err != nil || n != 1 || string(buf[1:1+n]) != "}" {
		t.Fatalf("unexpected: %v %d %q", err, n, buf)
	}
}
