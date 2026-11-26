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
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
)

// 启动 goroutine 定期读取终端尺寸，变化时写入 resizeEvents 通道；
// stop 通道关闭时退出。
func monitorResizeEvents(fd uintptr, resizeEvents chan<- TerminalSize, stop chan struct{}) {
	go func() {
		defer runtime.HandleCrash()

		size := GetSize(fd)
		if size == nil {
			return
		}
		lastSize := *size

		for {
			// 检查是否需要退出。
			select {
			case <-stop:
				return
			default:
			}

			size := GetSize(fd)
			if size == nil {
				return
			}

			if size.Height != lastSize.Height || size.Width != lastSize.Width {
				lastSize.Height = size.Height
				lastSize.Width = size.Width
				resizeEvents <- *size
			}

			// 休眠避免忙循环。
			time.Sleep(250 * time.Millisecond)
		}
	}()
}
