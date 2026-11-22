/*
Copyright 2019 The Kubernetes Authors.

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

// Package term 提供终端相关的小工具，目前只用于探测终端窗口尺寸。
package term

import (
	"fmt"
	"io"

	"github.com/moby/term"
)

// TerminalSize returns the current width and height of the user's terminal. If it isn't a terminal,
// nil is returned. On error, zero values are returned for width and height.
// Usually w must be the stdout of the process. Stderr won't work.
//
// TerminalSize 返回用户终端的宽和高。w 不是终端时返回错误，
// 查询失败时宽高均为零。
// 通常 w 必须是进程的 stdout：stderr 可能已被重定向，无法取得终端文件描述符。
func TerminalSize(w io.Writer) (int, int, error) {
	// 先确认 writer 背后确实是终端，否则后续 ioctl 没有意义。
	outFd, isTerminal := term.GetFdInfo(w)
	if !isTerminal {
		return 0, 0, fmt.Errorf("given writer is no terminal")
	}
	// 通过 TIOCGWINSZ 读取当前窗口大小；失败时错误原样返回，便于调用方区分原因。
	winsize, err := term.GetWinsize(outFd)
	if err != nil {
		return 0, 0, err
	}
	return int(winsize.Width), int(winsize.Height), nil
}
