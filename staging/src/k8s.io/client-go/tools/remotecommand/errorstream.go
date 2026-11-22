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

// errorStreamDecoder interprets the data on the error channel and creates a go error object from it.
type errorStreamDecoder interface {
	decode(message []byte) error
}

// watchErrorStream watches the errorStream for remote command error data,
// decodes it with the given errorStreamDecoder, sends the decoded error (or nil if the remote
// command exited successfully) to the returned error channel, and closes it.
// This function returns immediately.
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
