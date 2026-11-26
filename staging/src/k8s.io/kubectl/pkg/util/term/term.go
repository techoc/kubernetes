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

package term

import (
	"errors"
	"io"
	"os"

	"k8s.io/cli-runtime/pkg/printers"

	"github.com/moby/term"

	"k8s.io/kubectl/pkg/util/interrupt"
)

// 由 TTY 调用的函数类型。
type SafeFunc func() error

// 调用函数并在结束时恢复终端状态，即使进程被终止；
// 同时为远程命令执行/attach 提供终端尺寸变化支持。
type TTY struct {
	// 表示 stdin 的 reader，必填。
	In io.Reader
	// 表示 stdout 的 writer；要支持终端尺寸变化必须设置，
	// 但本身可选。
	Out io.Writer
	// 为 true 时把终端设为 raw 模式。
	Raw bool
	// 输入不是文件描述符时，尝试打开 /dev/tty。
	TryDev bool
	// 可选的中断处理器；提供时在终端状态恢复后调用，
	// 未提供时，TTY 期间收到信号会执行 os.Exit(0)。
	Parent *interrupt.Handler

	// 调用 MonitorSize() 后设置，用于监听用户终端尺寸变化的 SIGWINCH。
	sizeQueue *sizeQueue
}

// t.In 是终端时返回 true；即使 TryDev 为真
// 也不会检查 /dev/tty。
func (t TTY) IsTerminalIn() bool {
	return printers.IsTerminal(t.In)
}

// t.Out 是终端时返回 true；即使 TryDev 为真
// 也不会检查 /dev/tty。
func (t TTY) IsTerminalOut() bool {
	return printers.IsTerminal(t.Out)
}

// 判断给定对象是否为终端。
// Deprecated: use printers.IsTerminal instead.
var IsTerminal = printers.IsTerminal

// 仅当 writer 是终端且环境支持并希望使用颜色时
// 返回 true。
// Deprecated: use printers.AllowsColorOutput instead.
var AllowsColorOutput = printers.AllowsColorOutput

// 执行给定函数，并尽量保证函数返回或收到终止信号时
// 终端状态恢复到调用前；
// 设置了 t.Raw 时，调用前进入 raw 模式。
// 输入不是 TTY 且 TryDev 为真时，尝试打开 /dev/tty
// （若可用）。
func (t TTY) Safe(fn SafeFunc) error {
	inFd, isTerminal := term.GetFdInfo(t.In)

	if !isTerminal && t.TryDev {
		if f, err := os.Open("/dev/tty"); err == nil {
			defer f.Close()
			inFd = f.Fd()
			isTerminal = term.IsTerminal(inFd)
		}
	}
	if !isTerminal {
		return fn()
	}

	var state *term.State
	var err error
	if t.Raw {
		state, err = term.MakeRaw(inFd)
	} else {
		state, err = term.SaveState(inFd)
	}
	if err != nil {
		return err
	}
	return interrupt.Chain(t.Parent, func() {
		if t.sizeQueue != nil {
			t.sizeQueue.stop()
		}

		term.RestoreTerminal(inFd, state)
	}).Run(fn)
}

type detachableReader struct {
	escapeProxy io.Reader
}

func NewDetachableReader(r io.Reader, detachKeys string) (io.Reader, error) {
	detachKeyBytes, err := term.ToBytes(detachKeys)
	if err != nil {
		return nil, err
	}
	return &detachableReader{
		escapeProxy: term.NewEscapeProxy(r, detachKeyBytes),
	}, nil
}

func (r *detachableReader) Read(p []byte) (n int, err error) {
	n, err = r.escapeProxy.Read(p)
	if errors.Is(err, term.EscapeError{}) {
		// 用户按下 detach 键序列时会出现 EscapeError，属于预期行为；
		// 改为返回 io.EOF，让 attach 会话正常结束。
		err = io.EOF
	}
	return n, err
}
