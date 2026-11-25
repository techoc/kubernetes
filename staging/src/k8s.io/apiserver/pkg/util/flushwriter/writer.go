/*
Copyright 2014 The Kubernetes Authors.

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

package flushwriter

import (
	"io"
	"net/http"
)

// Wrap 把 io.Writer 包装成“每次写入后，若底层 writer 实现了 Flusher 接口就 flush”的 writer。
// 无论 w 是否实现 Flusher 都返回包装器，这样调用方无需做类型分支；
// 不实现时退化为普通写透传。
func Wrap(w io.Writer) io.Writer {
	fw := &flushWriter{
		writer: w,
	}
	if flusher, ok := w.(http.Flusher); ok {
		fw.flusher = flusher
	}
	return fw
}

// flushWriter provides wrapper for responseWriter with HTTP streaming capabilities
type flushWriter struct {
	flusher http.Flusher
	writer  io.Writer
}

// Write 是 flushWriter 对 io.Writer 的实现，把缓冲数据发送给客户端。
// Write 失败时不 flush，避免把半截数据推给客户端后掩盖真实错误。
func (fw *flushWriter) Write(p []byte) (n int, err error) {
	n, err = fw.writer.Write(p)
	if err != nil {
		return
	}
	if fw.flusher != nil {
		fw.flusher.Flush()
	}
	return
}
