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

// Package localupcluster 包含调用 hack/local-up-cluster.sh
// 以及管理所创建集群的封装代码。
package localupcluster

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

type Cmd struct {
	// Name 是一个简短的描述性名称。
	Name string

	// CommandLine 是完整的命令行，包括命令本身。
	CommandLine []string

	// AdditionalEnv 会被添加到当前环境变量中。
	AdditionalEnv map[string]string

	// LogOutput 在输出被打印时将其记录下来。
	LogOutput bool

	// ProcessOutput 如果非 nil，则命令在 stderr 或 stdout 上打印的
	// 每一行都会调用它。
	// 该行不包含末尾的换行符。
	//
	// 当输出处理停止时，会以 EOF 为 true 调用它。这意味着
	// 命令已停止或输出处理失败。
	// line in this case is the output processing error.
	ProcessOutput func(output Output)

	// GatherOutput 将输出收集到一个字符串缓冲区中。收集到的输出由 Wait 和 Stop 返回。
	// 如果禁用，这些方法将返回空字符串。
	GatherOutput bool

	// LogFile 指定将输出写入的文件。
	// 可以与其他输出处理选项组合使用。
	// 如果它是唯一选项，
	// 则命令会直接写入该文件。
	LogFile string

	// KeepRunning 确保命令在其 context 结束后仍继续运行，
	// 即忽略 context 取消。此类命令必须显式停止。
	KeepRunning bool

	cancel    func(string)
	cmd       *exec.Cmd
	wg        sync.WaitGroup
	running   atomic.Bool
	result    error
	gathering bool

	mutex  sync.RWMutex
	output strings.Builder
}

type Output struct {
	Line string
	EOF  bool
}

func (c *Cmd) Start(tCtx ktesting.TContext) {
	tCtx.Helper()
	tCtx.Logf("running command %s: %s", c.Name, strings.Join(c.CommandLine, " "))
	if c.KeepRunning {
		tCtx = tCtx.WithoutCancel()
	}
	tCtx = tCtx.WithCancel()
	c.cancel = tCtx.Cancel
	c.cmd = exec.CommandContext(tCtx, c.CommandLine[0], c.CommandLine[1:]...)
	c.gathering = false

	c.cmd.Env = os.Environ()
	for k, v := range c.AdditionalEnv {
		c.cmd.Env = append(c.cmd.Env, k+"="+v)
	}

	var reader io.Reader
	var writer io.WriteCloser

	c.gathering = false
	switch {
	case c.LogOutput || c.ProcessOutput != nil || c.GatherOutput:
		// 通过内存管道逐行处理输出。
		reader, writer = io.Pipe()
		c.gathering = true
	case c.LogFile != "":
		// 让命令直接写入文件。
		f, err := os.Create(c.LogFile)
		tCtx.ExpectNoError(err, "create log file")
		writer = f
	}
	c.cmd.Stdout = writer
	c.cmd.Stderr = writer

	tCtx.ExpectNoError(c.cmd.Start(), "start %s command", c.Name)
	c.running.Store(true)

	if reader != nil {
		scanner := bufio.NewScanner(reader)
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for scanner.Scan() {
				line := scanner.Text()
				line = strings.TrimSuffix(line, "\n")
				if c.LogOutput {
					tCtx.Logf("%s: %s", c.Name, line)
				}
				if c.ProcessOutput != nil {
					c.ProcessOutput(Output{Line: line})
				}
				if c.GatherOutput {
					c.mutex.Lock()
					c.output.WriteString(line)
					c.output.WriteByte('\n')
					c.mutex.Unlock()
				}
			}
			if c.ProcessOutput != nil {
				if err := scanner.Err(); err != nil {
					c.ProcessOutput(Output{Line: err.Error(), EOF: true})
				} else {
					c.ProcessOutput(Output{EOF: true})
				}
			}
		}()
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		// tCtx.Logf("Starting to wait for termination of command %s", c.Name)
		c.result = c.cmd.Wait()
		tCtx.Logf("Command %s terminated, result: %v", c.Name, c.result)
		now := time.Now()
		if reader != nil {
			// 必须关闭以停止输出处理，否则 scanner
			// 会继续读取，因为可能还会有人写入内容。
			_ = writer.Close()
		}
		if c.LogFile != "" {
			f, err := os.OpenFile(c.LogFile, os.O_WRONLY|os.O_APPEND, 0666)
			if err == nil {
				defer func() {
					_ = f.Close()
				}()
				_, _ = fmt.Fprintf(f, "%s: terminated, result: %v\n", now, c.result)
				if err := context.Cause(tCtx); err != nil {
					_, _ = fmt.Fprintf(f, "%s: killed because command context was canceled: %v\n", now, err)
				}
			}
		}
		c.running.Store(false)
	}()
}

func (c *Cmd) Wait(tCtx ktesting.TContext) string {
	return c.wait(tCtx, false)
}

func (c *Cmd) Stop(tCtx ktesting.TContext, reason string) string {
	tCtx.Helper()
	if c.cancel == nil {
		// 尚未启动……
		return ""
	}
	if c.LogFile != "" {
		f, err := os.OpenFile(c.LogFile, os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			defer func() {
				_ = f.Close()
			}()
			_, _ = fmt.Fprintf(f, "%s: killing: %s\n", time.Now(), reason)
		}
	}
	c.cancel(reason)
	return c.wait(tCtx, true)
}

func (c *Cmd) wait(tCtx ktesting.TContext, killed bool) string {
	tCtx.Helper()
	c.wg.Wait()
	if c.running.Load() {
		tCtx.Fatalf("command %s should have stopped but didn't", c.Name)
	}
	if !killed {
		tCtx.ExpectNoError(c.result, fmt.Sprintf("%s command failed, output:\n%s", c.Name, c.output.String()))
	}
	return c.output.String()
}

func (c *Cmd) Running() bool {
	return c.running.Load()
}

func (c *Cmd) Output(tCtx ktesting.TContext) string {
	if c.gathering {
		c.mutex.Lock()
		defer c.mutex.Unlock()
		return c.output.String()
	}

	if c.LogFile != "" {
		f, err := os.Open(c.LogFile)
		tCtx.ExpectNoError(err, "open log file")
		content, err := io.ReadAll(f)
		tCtx.ExpectNoError(err, "read log file")
		return string(content)
	}

	return ""
}
