/*
Copyright 2022 The Kubernetes Authors.

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

package v1

import (
	"bufio"
	"fmt"
	"io"
	"sync"

	"github.com/go-logr/logr"

	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2/textlogger"
)

// textFactory 生成 klog 文本格式 logger 实例。
type textFactory struct{}

var _ LogFormatFactory = textFactory{}

// Feature 返回控制该格式可用性的 feature gate；文本格式属于稳定功能。
func (f textFactory) Feature() featuregate.Feature {
	return LoggingStableOptions
}

// Create 根据配置构造文本 logger，并返回运行时控制句柄。
// 当启用 splitStream 时，按日志级别把 info 与 error 分流到不同输出流。
func (f textFactory) Create(c LoggingConfiguration, o LoggingOptions) (logr.Logger, RuntimeControl) {
	output := o.ErrorStream
	var flush func()
	if c.Options.Text.SplitStream {
		r := &klogMsgRouter{
			info:  o.InfoStream,
			error: o.ErrorStream,
		}
		size := c.Options.Text.InfoBufferSize.Value()
		if size > 0 {
			// 限制缓冲区上限，避免超大配置导致整数溢出或内存异常。
			if size > 2*1024*1024*1024 {
				size = 2 * 1024 * 1024 * 1024
			}
			info := newBufferedWriter(r.info, int(size))
			flush = info.Flush
			r.info = info
		}
		output = r
	}

	options := []textlogger.ConfigOption{
		textlogger.Verbosity(int(c.Verbosity)),
		textlogger.Output(output),
	}
	loggerConfig := textlogger.NewConfig(options...)

	// 这里的字符串由 VModuleConfigurationPflag 生成，格式总是合法的。
	_ = loggerConfig.VModule().Set(VModuleConfigurationPflag(&c.VModule).String())

	return textlogger.NewLogger(loggerConfig),
		RuntimeControl{
			// 运行时改日志级别用于动态调整 verbose 输出，避免重启组件。
			SetVerbosityLevel: func(v uint32) error {
				return loggerConfig.Verbosity().Set(fmt.Sprintf("%d", v))
			},
			Flush: flush,
		}
}

// klogMsgRouter 根据文本日志首字节的路由标记把消息分发到 info 或 error 流。
type klogMsgRouter struct {
	info, error io.Writer
}

var _ io.Writer = &klogMsgRouter{}

// Write 按 klog 文本格式的首字节路由：'I' 为 info，其余（E/F/W）写入 error 流。
func (r *klogMsgRouter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if p[0] == 'I' {
		return r.info.Write(p)
	}
	return r.error.Write(p)
}

// bufferedWriter 是一个在内存中缓冲写入的 io.Writer，
// 达到容量上限或被显式 Flush 时才写入底层 writer。
type bufferedWriter struct {
	mu     sync.Mutex
	writer *bufio.Writer
	out    io.Writer
}

// newBufferedWriter 创建指定缓冲大小的 writer。
func newBufferedWriter(out io.Writer, size int) *bufferedWriter {
	return &bufferedWriter{
		writer: bufio.NewWriterSize(out, size),
		out:    out,
	}
}

// Write 在锁保护下写入缓冲；为避免底层 writer 出现部分写，先保证整条消息
// 要么落入缓冲、要么先刷缓冲再写入。
func (b *bufferedWriter) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// 缓冲不足以容纳新数据且已有数据时，先刷新，避免把一条消息切成两段。
	if len(p) > b.writer.Available() && b.writer.Buffered() > 0 {
		if err := b.writer.Flush(); err != nil {
			return 0, err
		}
	}

	// 仍然放不下就直接写底层，绕过（此时已空的）缓冲区。
	if len(p) > b.writer.Available() {
		return b.out.Write(p)
	}

	// 正常情况下写入缓冲区。
	return b.writer.Write(p)
}

// Flush 将缓冲区内容刷到底层 writer。
func (b *bufferedWriter) Flush() {
	b.mu.Lock()
	defer b.mu.Unlock()

	_ = b.writer.Flush()
}
