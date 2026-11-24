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

package portforward

import (
	"net/http"
	"testing"
	"time"

	"k8s.io/streaming/pkg/httpstream"
)

// TestHTTPStreamReceived 验证 httpStreamReceived 对端口头与流类型头的校验：
// 覆盖头缺失、端口非法、类型非法等失败场景，以及合法流被成功投递到通道的场景。
func TestHTTPStreamReceived(t *testing.T) {
	// 表驱动用例：port 与 streamType 模拟流携带的头；
	// expectedError 非空表示期望校验失败，且其文本必须与返回错误完全一致。
	tests := map[string]struct {
		port          string
		streamType    string
		expectedError string
	}{
		"missing port": {
			expectedError: `"port" header is required`,
		},
		"unable to parse port": {
			port:          "abc",
			expectedError: `unable to parse "abc" as a port: strconv.ParseUint: parsing "abc": invalid syntax`,
		},
		"negative port": {
			port:          "-1",
			expectedError: `unable to parse "-1" as a port: strconv.ParseUint: parsing "-1": invalid syntax`,
		},
		"missing stream type": {
			port:          "80",
			expectedError: `"streamType" header is required`,
		},
		"valid port with error stream": {
			port:       "80",
			streamType: "error",
		},
		"valid port with data stream": {
			port:       "80",
			streamType: "data",
		},
		"invalid stream type": {
			port:          "80",
			streamType:    "foo",
			expectedError: `invalid stream type "foo"`,
		},
	}
	// 逐用例构造带指定头的假流，调用校验回调，再按期望检查错误或流是否被投递。
	for name, test := range tests {
		streams := make(chan httpstream.Stream, 1)
		f := httpStreamReceived(streams)
		stream := newFakeHTTPStream()
		if len(test.port) > 0 {
			stream.headers.Set("port", test.port)
		}
		if len(test.streamType) > 0 {
			stream.headers.Set("streamType", test.streamType)
		}
		replySent := make(chan struct{})
		err := f(stream, replySent)
		close(replySent)
		if len(test.expectedError) > 0 {
			// 期望失败的用例必须返回错误，且错误文本与预期逐字一致。
			if err == nil {
				t.Errorf("%s: expected err=%q, but it was nil", name, test.expectedError)
			}
			if e, a := test.expectedError, err.Error(); e != a {
				t.Errorf("%s: expected err=%q, got %q", name, e, a)
			}
			continue
		}
		// 期望成功的用例不应返回错误。
		if err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
			continue
		}
		// 校验通过后，同一条流应被原样送入通道。
		if s := <-streams; s != stream {
			t.Errorf("%s: expected stream %#v, got %#v", name, stream, s)
		}
	}
}

// fakeConn 是 httpstream.Connection 的最小测试替身，仅记录注销流的方法是否被调用。
type fakeConn struct {
	// removeStreamsCalled 标记 RemoveStreams 是否被调用过。
	removeStreamsCalled bool
}

// 以下是 httpstream.Connection 接口所需方法的空实现；
// 其中 RemoveStreams 会置位 removeStreamsCalled，供测试断言底层流已被注销。
func (*fakeConn) CreateStream(headers http.Header) (httpstream.Stream, error) { return nil, nil }
func (*fakeConn) Close() error                                                { return nil }
func (*fakeConn) CloseChan() <-chan bool                                      { return nil }
func (*fakeConn) SetIdleTimeout(timeout time.Duration)                        {}
func (f *fakeConn) RemoveStreams(streams ...httpstream.Stream)                { f.removeStreamsCalled = true }

// TestGetStreamPair 验证流对的完整生命周期：新建、按 requestID 复用，
// 以及在两条流收齐或等待超时后从表中移除并注销底层流。
func TestGetStreamPair(t *testing.T) {
	// timeout 通道由测试手动控制，用来模拟监控协程等待超时。
	timeout := make(chan time.Time)

	conn := &fakeConn{}
	h := &httpStreamHandler{
		streamPairs: make(map[string]*httpStreamPair),
		conn:        conn,
	}

	// 首次创建流对：应返回新对象且数据/错误流都尚未设置。
	p, created := h.getStreamPair("1")
	if p == nil {
		t.Fatalf("unexpected nil pair")
	}
	if !created {
		t.Fatal("expected created=true")
	}
	if p.dataStream != nil {
		t.Errorf("unexpected non-nil data stream")
	}
	if p.errorStream != nil {
		t.Errorf("unexpected non-nil error stream")
	}

	// 启动监控协程，稍后由 complete 或 timeout 触发清理。
	monitorDone := make(chan struct{})
	go func() {
		h.monitorStreamPair(p, timeout)
		close(monitorDone)
	}()

	if !h.hasStreamPair("1") {
		t.Fatal("This should still be true")
	}

	// 同一 requestID 再次查询应返回同一个流对，且 created 为 false。
	p2, created := h.getStreamPair("1")
	if created {
		t.Fatal("expected created=false")
	}
	if p != p2 {
		t.Fatalf("retrieving an existing pair: expected %#v, got %#v", p, p2)
	}

	// 清理路径一：先加入数据流（此时还不完整），再加入错误流（此时收齐）。
	dataStream := newFakeHTTPStream()
	dataStream.headers.Set(StreamType, StreamTypeData)
	complete, err := p.add(dataStream)
	if err != nil {
		t.Fatalf("unexpected error adding data stream to pair: %v", err)
	}
	if complete {
		t.Fatalf("unexpected complete")
	}

	errorStream := newFakeHTTPStream()
	errorStream.headers.Set(StreamType, StreamTypeError)
	complete, err = p.add(errorStream)
	if err != nil {
		t.Fatalf("unexpected error adding error stream to pair: %v", err)
	}
	if !complete {
		t.Fatal("unexpected incomplete")
	}

	// 等待监控协程结束，确保清理动作已经完成。
	<-monitorDone

	if !conn.removeStreamsCalled {
		t.Fatalf("connection remove stream not called")
	}
	conn.removeStreamsCalled = false

	// 断言流对已从表中删除。
	if h.hasStreamPair("1") {
		t.Fatal("expected removal of pair after both data and error streams received")
	}

	// 清理路径二：新建流对后不放入任何流，直接让监控协程因超时而退出。
	p, created = h.getStreamPair("2")
	if !created {
		t.Fatal("expected created=true")
	}
	if p == nil {
		t.Fatal("expected p not to be nil")
	}

	monitorDone = make(chan struct{})
	go func() {
		h.monitorStreamPair(p, timeout)
		close(monitorDone)
	}()
	// 关闭 timeout 通道模拟超时发生。
	close(timeout)
	// 等待监控协程结束，再断言超时路径同样完成了清理。
	<-monitorDone
	if h.hasStreamPair("2") {
		t.Fatal("expected stream pair to be removed")
	}
	if !conn.removeStreamsCalled {
		t.Fatalf("connection remove stream not called")
	}
}

// TestRequestID 验证请求 ID 的解析优先级：
// 有 requestID 头时直接采用；没有时错误流用自身流 ID、数据流用流 ID 减 2 推导。
func TestRequestID(t *testing.T) {
	h := &httpStreamHandler{}

	// 错误流 ID 为 1，期望推导出 requestID "1"。
	s := newFakeHTTPStream()
	s.headers.Set(StreamType, StreamTypeError)
	s.id = 1
	if e, a := "1", h.requestID(s); e != a {
		t.Errorf("expected %q, got %q", e, a)
	}

	// 数据流 ID 为 3，减去 2 后同样应得到 "1"，从而与上面的错误流配对。
	s.headers.Set(StreamType, StreamTypeData)
	s.id = 3
	if e, a := "1", h.requestID(s); e != a {
		t.Errorf("expected %q, got %q", e, a)
	}

	// 显式携带 requestID 头时应优先采用，忽略流类型与流 ID。
	s.id = 7
	s.headers.Set(PortForwardRequestIDHeader, "2")
	if e, a := "2", h.requestID(s); e != a {
		t.Errorf("expected %q, got %q", e, a)
	}
}

// fakeHTTPStream 是 httpstream.Stream 的测试替身：只保存头和流 ID。
// headers 保存流头（供校验逻辑读取），id 保存流标识符（供 requestID 推导使用）。
type fakeHTTPStream struct {
	headers http.Header
	id      uint32
}

// newFakeHTTPStream 创建一条带头映射、ID 为零的假流。
func newFakeHTTPStream() *fakeHTTPStream {
	return &fakeHTTPStream{
		headers: make(http.Header),
	}
}

// 编译期断言 fakeHTTPStream 满足 httpstream.Stream 接口。
var _ httpstream.Stream = &fakeHTTPStream{}

// Read 是空实现，测试中不读取流内容。
func (s *fakeHTTPStream) Read(data []byte) (int, error) {
	return 0, nil
}

// Write 是空实现，测试中不写入流内容。
func (s *fakeHTTPStream) Write(data []byte) (int, error) {
	return 0, nil
}

// Close 是空实现，测试中不检查关闭行为。
func (s *fakeHTTPStream) Close() error {
	return nil
}

// Reset 是空实现，测试中不检查重置行为。
func (s *fakeHTTPStream) Reset() error {
	return nil
}

// Headers 返回假流保存的头映射。
func (s *fakeHTTPStream) Headers() http.Header {
	return s.headers
}

// Identifier 返回假流的流标识符。
func (s *fakeHTTPStream) Identifier() uint32 {
	return s.id
}
