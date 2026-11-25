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
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
)

// errorStreamDecoder 解读错误通道上的数据，并据此构造一个 Go 错误对象。
type errorStreamDecoder interface {
	decode(message []byte) error
}

// watchErrorStream 监听 errorStream 上的远端命令错误数据，用给定的
// errorStreamDecoder 解码，把解码后的错误（远端命令成功退出时为 nil）
// 发送到返回的 error 通道并关闭它。本函数立即返回。
func watchErrorStream(logger klog.Logger, errorStream io.Reader, d errorStreamDecoder) chan error {
	// 无缓冲：读取方负责消费一次结果，避免调用方遗漏时 goroutine 永久阻塞。
	errorChan := make(chan error)

	go func() {
		// 远端流异常可能引发 panic，记录后仍需关闭 channel 让主流程收尾。
		defer runtime.HandleCrashWithLogger(logger)

		// errorStream 的契约是读到 EOF 才算完整消息；单次 Read 可能截断协议帧。
		message, err := io.ReadAll(errorStream)
		switch {
		case err != nil && err != io.EOF:
			errorChan <- fmt.Errorf("error reading from error stream: %w", err)
		case len(message) > 0:
			// 有内容则按协议解码，可能表示远端命令以非零状态退出。
			errorChan <- d.decode(message)
		default:
			// 空消息且读到 EOF：命令正常结束。
			errorChan <- nil
		}
		close(errorChan)
	}()

	return errorChan
}
