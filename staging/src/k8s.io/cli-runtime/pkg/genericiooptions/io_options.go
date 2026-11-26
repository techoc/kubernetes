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

// IOStreams 统一了标准输入输出流的命名，便于嵌入和单元测试；
// 命名不一致会让代码难以阅读和评审。
//
// 三路流固定命名为 In/Out/ErrOut，避免各命令自行发明 stdin/stdout/stderr
// 之类的别名而增加审阅成本。
type IOStreams struct {
	// In 对应 os.Stdin。
	In io.Reader
	// Out 对应 os.Stdout。
	Out io.Writer
	// ErrOut 对应 os.Stderr。
	ErrOut io.Writer
}

// NewTestIOStreams 返回单元测试可用的 IOStreams 及 in、out、errout 缓冲区。
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

// NewTestIOStreamsDiscard 返回丢弃所有输出的 IOStreams。
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
