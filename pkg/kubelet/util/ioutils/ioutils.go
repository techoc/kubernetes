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

package ioutils

import "io"

// LimitWriter 是标准库 ioutils.LimitReader 在 Writer 接口上的对应实现。
// 返回的 Writer 会写入 w，但在写满 n 字节后停止并返回 EOF。
// 底层实现是 *LimitedWriter。
func LimitWriter(w io.Writer, n int64) io.Writer { return &LimitedWriter{w, n} }

// LimitedWriter 写入 W，但将写入的数据量限制为 N 字节。
// 每次 Write 都会更新 N 以反映剩余额度；
// 当 N <= 0 或底层 W 返回 EOF 时，Write 返回 EOF。
type LimitedWriter struct {
	W io.Writer // underlying writer
	N int64     // max bytes remaining
}

// Write 写入数据并在达到上限时截断，同时返回 ErrShortWrite。
func (l *LimitedWriter) Write(p []byte) (n int, err error) {
	if l.N <= 0 {
		return 0, io.ErrShortWrite
	}
	truncated := false
	if int64(len(p)) > l.N {
		p = p[0:l.N]
		truncated = true
	}
	n, err = l.W.Write(p)
	l.N -= int64(n)
	if err == nil && truncated {
		err = io.ErrShortWrite
	}
	return
}
