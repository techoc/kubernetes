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

package remotecommand

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
)

// streamProtocolV3 实现 attach 和 exec 流式协议的第 3 版。
// 该版本新增了对容器终端尺寸调整（resize）的支持。
type streamProtocolV3 struct {
	*streamProtocolV2

	// resizeStream 仅在启用 TTY 且调用方提供了 TerminalSizeQueue 时才会创建，
	// 客户端通过它把窗口尺寸事件推送回容器。
	resizeStream io.Writer
}

// 编译期断言：v3 处理器满足统一的协议接口。
var _ streamProtocolHandler = &streamProtocolV3{}

func newStreamProtocolV3(options StreamOptions) streamProtocolHandler {
	return &streamProtocolV3{
		streamProtocolV2: newStreamProtocolV2(options).(*streamProtocolV2),
	}
}

func (p *streamProtocolV3) createStreams(conn streamCreator) error {
	// v3 在 v2 的 stdin/stdout/stderr/error 之外只增加了 resize 流，先复用父实现。
	if err := p.streamProtocolV2.createStreams(conn); err != nil {
		return err
	}

	// 建立 resize 流
	if p.Tty {
		headers := http.Header{}
		headers.Set(v1.StreamType, v1.StreamTypeResize)
		var err error
		p.resizeStream, err = conn.CreateStream(headers)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *streamProtocolV3) handleResizes(logger klog.Logger) {
	// 非 TTY 或没有尺寸来源时无需转发，避免启动无意义的 goroutine。
	if p.resizeStream == nil || p.TerminalSizeQueue == nil {
		return
	}
	go func() {
		defer runtime.HandleCrashWithLogger(logger)

		encoder := json.NewEncoder(p.resizeStream)
		for {
			size := p.TerminalSizeQueue.Next()
			// Next 返回 nil 表示队列已关闭或调用方不再提供尺寸，转发循环随之终止。
			if size == nil {
				return
			}
			if err := encoder.Encode(&size); err != nil {
				// 单个尺寸编码失败不应中断会话，记录后继续尝试后续事件。
				runtime.HandleErrorWithLogger(logger, err, "Encoding terminal size failed")
			}
		}
	}()
}

func (p *streamProtocolV3) stream(logger klog.Logger, conn streamCreator, ready chan<- struct{}) error {
	if err := p.createStreams(conn); err != nil {
		return err
	}

	// 通知所有流均已创建。
	if ready != nil {
		close(ready)
	}

	// 所有流创建完毕后，开始读取与拷贝

	// 错误流异步读取，主协程随后并行拷贝 stdin/stdout/stderr。
	errorChan := watchErrorStream(logger, p.errorStream, &errorDecoderV3{})

	p.handleResizes(logger)

	p.copyStdin(logger)

	var wg sync.WaitGroup
	p.copyStdout(logger, &wg)
	p.copyStderr(logger, &wg)

	// 等待 stdout/stderr 拷贝完成
	wg.Wait()

	// 等待 errorStream 读取结束，返回错误或 nil
	// error 流的结果是最终的退出状态：stdout/stderr 拷贝完不代表命令成功。
	return <-errorChan
}

// errorDecoderV3 沿用 v2 的错误帧格式，此处仅作为协议版本标识。
type errorDecoderV3 struct {
	errorDecoderV2
}
