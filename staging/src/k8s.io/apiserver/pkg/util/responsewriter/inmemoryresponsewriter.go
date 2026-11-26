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

// Package responsewriter 提供 http.ResponseWriter 的辅助实现，主要用于测试：
// 把响应写入内存而非网络，便于断言状态码、头与响应体。
package responsewriter

import (
	"fmt"
	"net/http"
)

// inMemoryResponseWriter 是把响应保存在内存中的 http.Writer。
// 不实现 http.Flusher/Hijacker 等可选接口，因此调用方不应依赖流式行为。
type inMemoryResponseWriter struct {
	writeHeaderCalled bool
	header            http.Header
	respCode          int
	data              []byte
}

// NewInMemoryResponseWriter 创建一个 header 已初始化的内存 writer。
func NewInMemoryResponseWriter() *inMemoryResponseWriter {
	return &inMemoryResponseWriter{header: http.Header{}}
}

// Header 返回可修改的响应头。
func (r *inMemoryResponseWriter) Header() http.Header {
	return r.header
}

// RespCode 返回已写入的状态码；未写入时为 0。
func (r *inMemoryResponseWriter) RespCode() int {
	return r.respCode
}

// Data 返回已写入的响应体。
func (r *inMemoryResponseWriter) Data() []byte {
	return r.data
}

// WriteHeader 记录状态码；与 net/http 不同，这里不禁止重复调用，
// 后一次会覆盖前一次，测试中应注意。
func (r *inMemoryResponseWriter) WriteHeader(code int) {
	r.writeHeaderCalled = true
	r.respCode = code
}

// Write 追加响应体；若尚未写状态码则隐式写入 200，与标准库行为一致。
func (r *inMemoryResponseWriter) Write(in []byte) (int, error) {
	if !r.writeHeaderCalled {
		r.WriteHeader(http.StatusOK)
	}
	r.data = append(r.data, in...)
	return len(in), nil
}

// String 输出便于测试失败时阅读的响应摘要。
func (r *inMemoryResponseWriter) String() string {
	s := fmt.Sprintf("ResponseCode: %d", r.respCode)
	if r.data != nil {
		s += fmt.Sprintf(", Body: %s", string(r.data))
	}
	if r.header != nil {
		s += fmt.Sprintf(", Header: %s", r.header)
	}
	return s
}
