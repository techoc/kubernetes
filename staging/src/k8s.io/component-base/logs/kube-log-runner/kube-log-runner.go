/*
Copyright 2020 The Kubernetes Authors.

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

// kube-log-runner 是一个极薄的进程包装器：启动子进程，把它的 stdout/stderr
// 重定向到文件（可选按大小轮转、按时间清理），并把终止信号转发给子进程。
// 容器中的组件借此把日志交给 kubelet 的日志基础设施处理。
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/component-base/logs/kube-log-runner/internal/logrotation"
)

// OpenFunc 打开日志文件；抽成类型是为了在测试中替换成内存 writer，
// 从而无需真正触碰磁盘即可验证轮转与重定向逻辑。
type OpenFunc func(filePath string, flushInterval time.Duration, maxSize int64, maxAge time.Duration) (io.WriteCloser, error)

// 命令行参数在 initFlags 中绑定；用指针是为了在 flag.Parse 之后读取解析结果。
var (
	flushInterval  *time.Duration
	logFilePath    *string
	logFileSize    *resource.QuantityValue
	logFileAge     *time.Duration
	alsoToStdOut   *bool
	redirectStderr *bool
)

// initFlags 注册所有标志。默认值保持“直接透传”，
// 即不写文件、stderr 跟随 stdout，保证未配置时行为与直接执行命令一致。
func initFlags() {
	logFileSize = &resource.QuantityValue{}
	flushInterval = flag.Duration("flush-interval", 0, "interval to flush log file to disk, default value 0, if non-zero, flush log file every interval")
	logFilePath = flag.String("log-file", "", "If non-empty, save stdout to this file")
	flag.Var(logFileSize, "log-file-size", "useful with log-file, in format of resource.quantity, default value 0, if non-zero, rotate log file when it reaches this size")
	logFileAge = flag.Duration("log-file-age", 0, "useful with log-file-size, in format of timeDuration, if non-zero, remove log files older than this duration")
	alsoToStdOut = flag.Bool("also-stdout", false, "useful with log-file, log to standard output as well as the log file")
	redirectStderr = flag.Bool("redirect-stderr", true, "treat stderr same as stdout")
}

func main() {
	initFlags()
	flag.Parse()

	if err := configureAndRun(logrotation.Open); err != nil {
		log.Fatal(err)
	}
}
func configureAndRun(open OpenFunc) error {
	// 参数校验在启动子进程之前完成，避免留下半初始化的日志文件。
	var (
		outputStream io.Writer = os.Stdout
		errStream    io.Writer = os.Stderr
	)

	args := flag.Args()
	if len(args) == 0 {
		return fmt.Errorf("not enough arguments to run")
	}

	maxSize := logFileSize.Value()
	// 负数没有意义，且会让轮转逻辑行为不可预期。
	if maxSize < 0 {
		return fmt.Errorf("log-file-size must be non-negative quantity")
	}

	// 检查 time.Duration 是否为负数。
	if *logFileAge < 0 {
		return fmt.Errorf("log-file-age must be non-negative")
	}

	if logFilePath != nil && *logFilePath != "" {
		logFile, err := open(*logFilePath, *flushInterval, maxSize, *logFileAge)

		if err != nil {
			return fmt.Errorf("failed to create log file %v: %w", *logFilePath, err)
		}
		if *alsoToStdOut {
			// MultiWriter 同时写两处，便于调试时既在容器输出又保留文件。
			outputStream = io.MultiWriter(os.Stdout, logFile)
		} else {
			outputStream = logFile
		}
	}

	if *redirectStderr {
		// 默认把 stderr 也导向同一输出，保持日志时序在一个流中。
		errStream = outputStream
	}

	exe := args[0]
	var exeArgs []string
	if len(args) > 1 {
		exeArgs = args[1:]
	}
	cmd := exec.Command(exe, exeArgs...)
	cmd.Stdout = outputStream
	cmd.Stderr = errStream

	log.Printf("Running command:\n%v", cmdInfo(cmd))
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("starting command: %w", err)
	}

	// 处理信号并优雅关闭进程。
	// 信号转发放到独立 goroutine，主 goroutine 专注等待子进程退出。
	go setupSigHandler(cmd.Process)
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("running command: %w", err)
	}
	return nil
}

// cmdInfo 生成便于打印和调试的命令信息。
//
// cmdInfo 把实际生效的配置与命令拼成可读文本，方便排查参数未生效的问题。
func cmdInfo(cmd *exec.Cmd) string {
	return fmt.Sprintf(
		`Command env: (enable-flush=%v, log-file=%v, log-file-size=%v, log-file-age=%v, also-stdout=%v, redirect-stderr=%v)
Run from directory: %v
Executable path: %v
Args (comma-delimited): %v`, *flushInterval, *logFilePath, logFileSize.Value(), *logFileAge, *alsoToStdOut, *redirectStderr,
		cmd.Dir, cmd.Path, strings.Join(cmd.Args, ","),
	)
}

// setupSigHandler 将终止信号转发给进程。
//
// setupSigHandler 接收终止信号并原样转发给子进程；自身不直接退出，
// 这样保留子进程自己的优雅关闭逻辑（flush、清理临时文件等）。
func setupSigHandler(process *os.Process) {
	// terminationSignals 是使程序在受支持平台（linux、darwin、windows）上退出的信号。
	terminationSignals := []os.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT}

	// 带缓冲 1 的 channel 足以容纳一次信号，转发前重复到达也不会阻塞发送方。
	c := make(chan os.Signal, 1)
	signal.Notify(c, terminationSignals...)

	// 阻塞直到收到信号。
	log.Println("Now listening for interrupts")
	s := <-c
	log.Printf("Got signal: %v. Sending down to process (PID: %v)", s, process.Pid)
	if err := process.Signal(s); err != nil {
		log.Fatalf("Failed to signal process: %v", err)
	}
	log.Printf("Signalled process %v successfully.", process.Pid)
}
