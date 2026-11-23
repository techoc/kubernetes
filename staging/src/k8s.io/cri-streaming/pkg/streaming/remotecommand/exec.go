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
	utilexec "k8s.io/utils/exec"
)

// Executor knows how to execute a command in a container in a pod.
// Executor 知道如何在 Pod 的容器中执行命令。
type Executor interface {
	// ExecInContainer executes a command in a container in the pod, copying data
	// between in/out/err and the container's stdin/stdout/stderr.
	// ExecInContainer 在 Pod 的容器中执行 cmd，在 in/out/err 与容器的 stdin/stdout/stderr 之间搬运数据。
	ExecInContainer(ctx context.Context, name string, uid string, container string, cmd []string, in io.Reader, out, err io.WriteCloser, tty bool, resize <-chan TerminalSize, timeout time.Duration) error
}

// ServeExec handles requests to execute a command in a container. After
// creating/receiving the required streams, it delegates the actual execution
// to the executor.
// ServeExec 处理在容器中执行命令的请求。创建或接收所需的流之后，把真正的执行工作委托给 executor。
func ServeExec(w http.ResponseWriter, req *http.Request, executor Executor, podName string, uid string, container string, cmd []string, streamOpts *Options, idleTimeout, streamCreationTimeout time.Duration, supportedProtocols []string) {
	// 先完成协议协商并建立底层连接与各条流；失败时 createStreams 已经写过错误响应。
	ctx, ok := createStreams(req, w, streamOpts, supportedProtocols, idleTimeout, streamCreationTimeout)
	if !ok {
		// error is handled by createStreams
		// 错误已由 createStreams 处理，这里直接返回。
		return
	}
	// exec 结束后关闭底层连接，顺带关闭其上的所有流。
	defer ctx.conn.Close()

	// 把协商好的流和参数交给具体实现；最后一个参数传 0 表示不在此处设置命令超时。
	err := executor.ExecInContainer(req.Context(), podName, uid, container, cmd, ctx.stdinStream, ctx.stdoutStream, ctx.stderrStream, ctx.tty, ctx.resizeChan, 0)
	if err != nil {
		// 区分「命令已运行但退出码非零」与「执行本身失败」：前者是预期的业务结果，需把退出码回传客户端。
		if exitErr, ok := err.(utilexec.ExitError); ok && exitErr.Exited() {
			// 取出退出码，放进 Status 的 ExitCode cause，供客户端还原命令的退出状态。
			rc := exitErr.ExitStatus()
			ctx.writeStatus(&streamStatusError{ErrStatus: streamStatus{
				Status: statusFailure,
				Reason: NonZeroExitCodeReason,
				Details: &streamStatusDetails{
					Causes: []streamStatusCause{
						{
							Type:    ExitCodeCauseType,
							Message: fmt.Sprintf("%d", rc),
						},
					},
				},
				Message: fmt.Sprintf("command terminated with non-zero exit code: %v", exitErr),
			}})
		} else {
			// 真正的执行错误：包装成内部错误，记录日志并通过 error 流返回。
			err = fmt.Errorf("error executing command in container: %v", err)
			runtime.HandleError(err)
			ctx.writeStatus(newInternalError(err))
		}
	} else {
		// 命令成功结束：在 error 流上写入成功状态。
		ctx.writeStatus(&streamStatusError{ErrStatus: streamStatus{
			Status: statusSuccess,
		}})
	}
}
