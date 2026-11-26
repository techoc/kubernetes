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
	"fmt"

	"github.com/moby/term"
	"k8s.io/apimachinery/pkg/util/runtime"
)

// 表示终端的宽和高。
// 与 staging/src/k8s.io/client-go/tools/remotecommand.TerminalSize 相同，
// 复制一份以解耦：终端相关包不应依赖 API 客户端，反之亦然。
type TerminalSize struct {
	Width  uint16
	Height uint16
}

// 可在终端尺寸变化时返回相应事件。
// 与 staging/src/k8s.io/client-go/tools/remotecommand.TerminalSizeQueue 相同，
// 复制一份以解耦：终端相关包不应依赖 API 客户端，反之亦然。
type TerminalSizeQueue interface {
	// 返回终端调整后的新尺寸；停止监听时返回 nil。
	Next() *TerminalSize
}

// 返回用户终端当前尺寸；不是终端时
// 返回 nil。
func (t TTY) GetSize() *TerminalSize {
	outFd, isTerminal := term.GetFdInfo(t.Out)
	if !isTerminal {
		return nil
	}
	return GetSize(outFd)
}

// 返回 fd 关联终端的当前尺寸。
func GetSize(fd uintptr) *TerminalSize {
	winsize, err := term.GetWinsize(fd)
	if err != nil {
		runtime.HandleError(fmt.Errorf("unable to get terminal size: %v", err))
		return nil
	}

	return &TerminalSize{Width: winsize.Width, Height: winsize.Height}
}

// 监听终端尺寸，返回预先填入 initialSizes 的 TerminalSizeQueue；
// 没有 TTY 时返回 nil。
func (t *TTY) MonitorSize(initialSizes ...*TerminalSize) TerminalSizeQueue {
	outFd, isTerminal := term.GetFdInfo(t.Out)
	if !isTerminal {
		return nil
	}

	t.sizeQueue = &sizeQueue{
		t: *t,
		// 使用带缓冲通道，以便在开始流式传输前无阻塞地写入初始尺寸。
		resizeChan:   make(chan TerminalSize, len(initialSizes)),
		stopResizing: make(chan struct{}),
	}

	t.sizeQueue.monitorSize(outFd, initialSizes...)

	return t.sizeQueue
}

// 实现 remotecommand.TerminalSizeQueue。
type sizeQueue struct {
	t TTY
	// 每次终端尺寸变化时收到一个 Size。
	resizeChan   chan TerminalSize
	stopResizing chan struct{}
}

// 确保 sizeQueue 实现 TerminalSizeQueue 接口。
var _ TerminalSizeQueue = &sizeQueue{}

// 先用 initialSizes 填充 resizeChan，再监听尺寸变化；
// 每次事件都把当前尺寸写入 resizeChan。
func (s *sizeQueue) monitorSize(outFd uintptr, initialSizes ...*TerminalSize) {
	// 发送初始尺寸。
	for i := range initialSizes {
		if initialSizes[i] != nil {
			s.resizeChan <- *initialSizes[i]
		}
	}

	resizeEvents := make(chan TerminalSize, 1)

	monitorResizeEvents(outFd, resizeEvents, s.stopResizing)

	// 后台监听尺寸变化事件。
	go func() {
		defer runtime.HandleCrash()

		for {
			select {
			case size, ok := <-resizeEvents:
				if !ok {
					return
				}

				select {
				// 尝试发送尺寸，但不阻塞。
				case s.resizeChan <- size:
					// 发送成功。
				default:
					// 发送失败，不做处理。
				}
			case <-s.stopResizing:
				return
			}
		}
	}()
}

// 返回终端调整后的新尺寸；停止监听时返回 nil。
func (s *sizeQueue) Next() *TerminalSize {
	size, ok := <-s.resizeChan
	if !ok {
		return nil
	}
	return &size
}

// 停止后台监听终端尺寸变化的 goroutine。
func (s *sizeQueue) stop() {
	close(s.stopResizing)
}
