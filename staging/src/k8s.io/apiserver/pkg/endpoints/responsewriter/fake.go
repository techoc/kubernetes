/*
Copyright 2021 The Kubernetes Authors.

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

// Package responsewriter 提供包装 http.ResponseWriter 的工具，用于在不破坏底层
// 连接语义（Flush/Hijack 等）的前提下增加功能。
package responsewriter

import (
	"bufio"
	"net"
	"net/http"
)

var _ http.ResponseWriter = &FakeResponseWriter{}

// FakeResponseWriter 实现 http.ResponseWriter，仅用于测试。
type FakeResponseWriter struct{}

// Header 返回一个全新的空 header，调用方写入的内容不会被保留。
func (fw *FakeResponseWriter) Header() http.Header { return http.Header{} }

// WriteHeader 忽略状态码，测试关注的是调用序列而非响应内容。
func (fw *FakeResponseWriter) WriteHeader(code int) {}

// Write 假装写入成功，始终返回完整长度。
func (fw *FakeResponseWriter) Write(bs []byte) (int, error) { return len(bs), nil }

// FakeResponseWriterFlusherCloseNotifier 用于测试：模拟 HTTP/2 下 http.ResponseWriter
// 同时实现 http.Flusher 和 http.CloseNotifier 的情形。
type FakeResponseWriterFlusherCloseNotifier struct {
	*FakeResponseWriter
}

// Flush 空实现，仅用于满足接口。
func (fw *FakeResponseWriterFlusherCloseNotifier) Flush() {}

// CloseNotify 返回 nil channel，测试中不会触发连接关闭通知。
func (fw *FakeResponseWriterFlusherCloseNotifier) CloseNotify() <-chan bool { return nil }

// FakeResponseWriterFlusherCloseNotifierHijacker 用于测试：模拟 HTTP/1.x 下 http.ResponseWriter
// 同时实现 http.Flusher、http.CloseNotifier 和 http.Hijacker 的情形。
type FakeResponseWriterFlusherCloseNotifierHijacker struct {
	*FakeResponseWriterFlusherCloseNotifier
}

// Hijack 返回零值，测试中不会真正接管连接。
func (fw *FakeResponseWriterFlusherCloseNotifierHijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, nil
}
