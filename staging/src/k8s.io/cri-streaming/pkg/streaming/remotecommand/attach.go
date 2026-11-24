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
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"k8s.io/streaming/pkg/runtime"
)

// Attacher 知道如何附着到 Pod 中正在运行的容器。
type Attacher interface {
	// AttachContainer 附着到 Pod 中正在运行的容器，在 in/out/err 与容器的 stdin/stdout/stderr 之间搬运数据。
	AttachContainer(ctx context.Context, name string, uid string, container string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan TerminalSize) error
}

// ServeAttach 处理附着容器的请求。创建或接收所需的流之后，把真正的附着工作委托给 attacher。
func ServeAttach(w http.ResponseWriter, req *http.Request, attacher Attacher, podName string, uid string, container string, streamOpts *Options, idleTimeout, streamCreationTimeout time.Duration, supportedProtocols []string) {
	// 先完成协议协商并建立底层连接及各条流；失败时 createStreams 已经写过错误响应。
	ctx, ok := createStreams(req, w, streamOpts, supportedProtocols, idleTimeout, streamCreationTimeout)
	if !ok {
		// 错误已由 createStreams 处理，这里直接返回。
		return
	}
	// attach 结束后关闭底层连接；ctx 中的各条流都由该连接统一管理。
	defer ctx.conn.Close()

	// 把协商好的流交给具体实现；resizeChan 仅在客户端协商了 resize 流时才有消费者。
	err := attacher.AttachContainer(req.Context(), podName, uid, container, ctx.stdinStream, ctx.stdoutStream, ctx.stderrStream, ctx.tty, ctx.resizeChan)
	if err != nil {
		// 附着失败：包装并记录错误，再通过 error 流把内部错误返回给客户端。
		err = fmt.Errorf("error attaching to container: %v", err)
		runtime.HandleError(err)
		ctx.writeStatus(newInternalError(err))
	} else {
		// 附着正常结束：在 error 流上写入成功状态。
		ctx.writeStatus(&streamStatusError{ErrStatus: streamStatus{
			Status: statusSuccess,
		}})
	}
}
