/*
Copyright 2025 The Kubernetes Authors.

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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// TestHandleResizeEvents 验证 handleResizeEvents 在「流中有无数据」「channel 是否被读取」「context 是否取消」
// 四种组合下都能正确退出，且不会泄漏 goroutine；有数据时还能把解码出的尺寸送达 channel。
func TestHandleResizeEvents(t *testing.T) {
	// 用 TerminalSize 零值作为期望数据，并序列化成 resize 流上的 JSON 输入。
	var testTerminalSize TerminalSize
	rawTerminalSize, err := json.Marshal(&testTerminalSize)
	require.NoError(t, err)

	// name 描述场景，resizeStreamData 是流输入，cancelContext 控制是否提前取消，
	// readFromChannel 控制是否消费 channel（用于覆盖 channel 无人读取的情形）。
	testCases := []struct {
		name             string
		resizeStreamData []byte
		cancelContext    bool
		readFromChannel  bool
	}{
		// 有数据但无人读取：只能靠取消 context 让发送方退出。
		{
			name:             "data attempted to be sent on the channel; channel not read; context canceled",
			resizeStreamData: rawTerminalSize,
			cancelContext:    true,
		},
		// 有数据且被读取：正常送达后流结束，goroutine 应自行退出。
		{
			name:             "data attempted to be sent on the channel; channel read; context not canceled",
			resizeStreamData: rawTerminalSize,
			readFromChannel:  true,
		},
		// 无数据且取消 context：解码阻塞在空流上，需由取消操作唤醒退出。
		{
			name:          "no data attempted to be sent on the channel; context canceled",
			cancelContext: true,
		},
		// 无数据也不取消：流立即结束，循环自然退出。
		{
			name: "no data attempted to be sent on the channel; context not canceled",
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 每个用例构造独立的 context 与连接上下文，resizeStream 用内存 reader 模拟。
			ctx, cancel := context.WithCancel(context.Background())
			connCtx := connectionContext{
				resizeStream: io.NopCloser(bytes.NewReader(testCase.resizeStreamData)),
				resizeChan:   make(chan TerminalSize),
			}

			go handleResizeEvents(ctx, connCtx.resizeStream, connCtx.resizeChan)
			// 读取路径下断言解码出的尺寸与序列化前的零值一致。
			if testCase.readFromChannel {
				gotTerminalSize := <-connCtx.resizeChan
				require.Equal(t, gotTerminalSize, testTerminalSize)
			}
			if testCase.cancelContext {
				cancel()
			}

			// 核心断言：被测 goroutine 在所有路径下都不应残留。
			goleak.VerifyNone(t)
			cancel()
		})
	}
}
