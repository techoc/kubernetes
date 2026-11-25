/*
Copyright 2015 The Kubernetes Authors.

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
	"net/http"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
)

// streamProtocolV2 实现 attach 和 exec 流式协议的第 2 版。
// 最初的流式协议是 metav1，因此这一版被称为 version 2，
// 尽管它其实是第一个真正带版本号的协议。
// 与 v1 的关键差异：流可以直接读写 io.Reader/io.ReadWriteCloser，stdin 关闭与错误解码
// 都有明确语义，因而能正确处理非交互 stdin 和执行错误。
type streamProtocolV2 struct {
	StreamOptions

	errorStream  io.Reader
	remoteStdin  io.ReadWriteCloser
	remoteStdout io.Reader
	remoteStderr io.Reader
}

// 编译期断言：v2 必须满足协议处理器接口。
var _ streamProtocolHandler = &streamProtocolV2{}

func newStreamProtocolV2(options StreamOptions) streamProtocolHandler {
	return &streamProtocolV2{
		StreamOptions: options,
	}
}

// createStreams 先建错误流，再按需建立 stdin/stdout/stderr。
// 所有流必须全部建好后再开始拷贝，原因同 v1：服务端要收齐 stream 才会启动处理。
func (p *streamProtocolV2) createStreams(conn streamCreator) error {
	var err error
	headers := http.Header{}

	// 建立错误流
	headers.Set(v1.StreamType, v1.StreamTypeError)
	p.errorStream, err = conn.CreateStream(headers)
	if err != nil {
		return err
	}

	// 建立 stdin 流
	if p.Stdin != nil {
		headers.Set(v1.StreamType, v1.StreamTypeStdin)
		p.remoteStdin, err = conn.CreateStream(headers)
		if err != nil {
			return err
		}
	}

	// 建立 stdout 流
	if p.Stdout != nil {
		headers.Set(v1.StreamType, v1.StreamTypeStdout)
		p.remoteStdout, err = conn.CreateStream(headers)
		if err != nil {
			return err
		}
	}

	// 建立 stderr 流
	if p.Stderr != nil && !p.Tty {
		headers.Set(v1.StreamType, v1.StreamTypeStderr)
		p.remoteStderr, err = conn.CreateStream(headers)
		if err != nil {
			return err
		}
	}
	return nil
}

// copyStdin 同时启动两个 goroutine：一个把本地输入送进远端，另一个消费远端 stdin
// 直到服务端关闭它。两者的关闭都用 sync.Once 保护，避免重复 Close。
func (p *streamProtocolV2) copyStdin(logger klog.Logger) {
	if p.Stdin != nil {
		var once sync.Once

		// 从客户端 stdin 拷贝到容器 stdin
		go func() {
			defer runtime.HandleCrashWithLogger(logger)

			// 如果 p.Stdin 是非交互式的，例如 `echo abc | kubectl exec -i <pod> -- cat`，
			// 要确保从 p.Stdin 到 remoteStdin 的拷贝一结束就立即关闭 remoteStdin。
			// 否则被执行的命令会一直运行下去。
			defer once.Do(func() { p.remoteStdin.Close() })

			if _, err := io.Copy(p.remoteStdin, readerWrapper{p.Stdin}); err != nil {
				runtime.HandleErrorWithLogger(logger, err, "Copying stdin failed")
			}
		}()

		// 从 remoteStdin 持续读取直到流关闭。这对于干净地退出交互式会话、
		// 不泄漏协程、不挂住客户端终端来说是必不可少的。
		//
		// TODO 我们已经不再使用 go-dockerclient；重新审视这段逻辑，确认
		// engine-api 是否仍然需要它。
		//
		// go-dockerclient 当前的 hijack 实现
		// (https://github.com/fsouza/go-dockerclient/blob/89f3d56d93788dfe85f864a44f85d9738fca0670/client.go#L564)
		// 会等待三个流（stdin/stdout/stderr）全部拷贝完成才返回。当 hijack
		// 完成 stdout/stderr 的拷贝后，会在它那一侧对 remoteStdin 调用 Close()，
		// 从而使这里的拷贝得以完成。此时我们也必须在自己的这一侧对 remoteStdin
		// 调用 Close()，让 hijack 中的拷贝完成、hijack 得以返回。
		go func() {
			defer runtime.HandleCrashWithLogger(logger)
			defer once.Do(func() { p.remoteStdin.Close() })

			// 这个"拷贝"并不真正读取任何内容——只是在这里等待
			// 服务端关闭 remoteStdin。
			if _, err := io.Copy(io.Discard, p.remoteStdin); err != nil {
				runtime.HandleErrorWithLogger(logger, err, "Waiting for server to close stdin failed")
			}
		}()
	}
}

// copyStdout 在 wg 上登记一条输出流。defer 中先排空远端再 Done，
// 以防服务端仍在写入时本地提前返回导致流阻塞。
func (p *streamProtocolV2) copyStdout(logger klog.Logger, wg *sync.WaitGroup) {
	if p.Stdout == nil {
		return
	}

	wg.Add(1)
	go func() {
		defer runtime.HandleCrashWithLogger(logger)
		defer wg.Done()
		// 确保队列中的数据包能被消费。
		// 队列积压可能在 conn.server 中造成死锁
		// 议题：https://github.com/kubernetes/kubernetes/issues/96339
		defer io.Copy(io.Discard, p.remoteStdout)

		if _, err := io.Copy(p.Stdout, p.remoteStdout); err != nil {
			runtime.HandleErrorWithLogger(logger, err, "Copying stdout failed")
		}
	}()
}

// copyStderr 与 copyStdout 对称；TTY 模式下 stderr 已合并进 stdout，故直接返回。
func (p *streamProtocolV2) copyStderr(logger klog.Logger, wg *sync.WaitGroup) {
	if p.Stderr == nil || p.Tty {
		return
	}

	wg.Add(1)
	go func() {
		defer runtime.HandleCrashWithLogger(logger)
		defer wg.Done()
		defer io.Copy(io.Discard, p.remoteStderr)

		if _, err := io.Copy(p.Stderr, p.remoteStderr); err != nil {
			runtime.HandleErrorWithLogger(logger, err, "Copying stderr failed")
		}
	}()
}

// stream 的完成条件：stdout/stderr 拷贝结束，并且错误流给出最终结果（成功时读到 EOF）。
func (p *streamProtocolV2) stream(logger klog.Logger, conn streamCreator, ready chan<- struct{}) error {
	if err := p.createStreams(conn); err != nil {
		return err
	}

	// 通知所有流均已创建。
	if ready != nil {
		close(ready)
	}

	// 所有流创建完毕后，开始读取与拷贝

	errorChan := watchErrorStream(logger, p.errorStream, &errorDecoderV2{})

	p.copyStdin(logger)

	var wg sync.WaitGroup
	p.copyStdout(logger, &wg)
	p.copyStderr(logger, &wg)

	// 等待 stdout/stderr 拷贝完成
	wg.Wait()

	// 等待 errorStream 读取结束，返回错误或 nil
	return <-errorChan
}

// errorDecoderV2 将错误通道的数据按纯文本解读。
//
// v2 的错误流是纯文本；v3/v4 才会使用带状态的结构化编码。
type errorDecoderV2 struct{}

func (d *errorDecoderV2) decode(message []byte) error {
	return fmt.Errorf("error executing remote command: %s", message)
}
