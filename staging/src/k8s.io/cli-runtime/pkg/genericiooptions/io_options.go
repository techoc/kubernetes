/*
Copyright 2023 The Kubernetes Authors.

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

// Package genericiooptions 定义 CLI 命令的标准输入/输出/错误流集合。
// 之所以抽成结构体，是为了让命令实现可被单测注入内存缓冲区，
// 而无需直接依赖 os.Stdin/Stdout/Stderr。
package genericiooptions

import (
	"bytes"
	"io"
)

// IOStreams provides the standard names for iostreams.  This is useful for embedding and for unit testing.
// Inconsistent and different names make it hard to read and review code
//
// 三路流固定命名为 In/Out/ErrOut，避免各命令自行发明 stdin/stdout/stderr
// 之类的别名而增加审阅成本。
type IOStreams struct {
	// In think, os.Stdin
	In io.Reader
	// Out think, os.Stdout
	Out io.Writer
	// ErrOut think, os.Stderr
	ErrOut io.Writer
}

// NewTestIOStreams returns a valid IOStreams and in, out, errout buffers for unit tests
//
// 同时返回三个缓冲区，便于断言命令的提示/警告确实写到了预期的流上。
func NewTestIOStreams() (IOStreams, *bytes.Buffer, *bytes.Buffer, *bytes.Buffer) {
	in := &bytes.Buffer{}
	out := &bytes.Buffer{}
	errOut := &bytes.Buffer{}

	return IOStreams{
		In:     in,
		Out:    out,
		ErrOut: errOut,
	}, in, out, errOut
}

// NewTestIOStreamsDiscard returns a valid IOStreams that just discards
//
// Out/ErrOut 指向 io.Discard，但仍保留可写的 In，
// 因为部分命令即使在测试中也会读取输入（例如确认提示）。
func NewTestIOStreamsDiscard() IOStreams {
	in := &bytes.Buffer{}
	return IOStreams{
		In:     in,
		Out:    io.Discard,
		ErrOut: io.Discard,
	}
}
