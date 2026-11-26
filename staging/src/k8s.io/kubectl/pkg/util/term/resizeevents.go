//go:build !windows

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
	"os"
	"os/signal"

	"golang.org/x/sys/unix"
	"k8s.io/apimachinery/pkg/util/runtime"
)

// 启动 goroutine 等待 SIGWINCH 信号（表示终端尺寸变化）；
// 收到后取终端尺寸，尝试写入 resizeEvents 通道；
// stop 通道关闭时退出。
func monitorResizeEvents(fd uintptr, resizeEvents chan<- TerminalSize, stop chan struct{}) {
	go func() {
		defer runtime.HandleCrash()

		winch := make(chan os.Signal, 1)
		signal.Notify(winch, unix.SIGWINCH)
		defer signal.Stop(winch)

		for {
			select {
			case <-winch:
				size := GetSize(fd)
				if size == nil {
					return
				}

				// 尝试发送尺寸。
				select {
				case resizeEvents <- *size:
					// 发送成功。
				default:
					// 未发送。
				}
			case <-stop:
				return
			}
		}
	}()
}
