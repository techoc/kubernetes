/*
Copyright 2023 The Kubernetes Authors.

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

package ktesting

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"testing"
)

var (
	// 在调用 init 之前，defaultProgressReporter 处于未激活状态。
	defaultProgressReporter = &progressReporter{}

	// interruptCtx 用于跟踪进程是否因 SIGINT 而被中断。
	// 发生中断时，会调用 interrupted，以合适的消息取消
	// interruptCtx。
	//
	// 它在每个进程中只设置一次，且永远不会重置。
	interruptCtx, interrupted = context.WithCancelCause(context.Background())
)

const ginkgoSpecContextKey = "GINKGO_SPEC_CONTEXT"

type ginkgoReporter interface {
	AttachProgressReporter(reporter func() string) func()
}

type progressReporter struct {
	// initMutex 保护 reporter 的初始化和收尾。
	initMutex sync.Mutex

	usageCount      int64
	wg              sync.WaitGroup
	testCtx         context.Context
	signalChannel   chan os.Signal
	progressChannel chan os.Signal

	// reportMutex 保护报告的生成及相关设置。
	reportMutex     sync.Mutex
	reporterCounter int64
	reporters       map[int64]func() string
	out             io.Writer
	closeOut        func() error
}

var _ ginkgoReporter = &progressReporter{}

// init 由 Init 调用。它返回供新 TContext 使用的
// context。
//
// 默认情况下，它就是 context.Background。在 Go 单元测试中，
// 它是一个与 os.Interrupt 关联的 context。
//
// 在 Go 单元测试中按上述方式激活后，progressReporter 支持
// 通过向运行中的测试发送 USR1 信号来触发进度报告，
// 类似于 Ginkgo 的相应功能。
//
// 该支持会一直有效，直到最后一个测试结束。
//
// 在 bubble 内，如果（且仅如果）首次调用 signal.Notify，
// 它会以 "select on synctest channel from outside bubble" 失败，
// 因此为安全起见，我们必须避免设置信号处理。
func (p *progressReporter) init(tb TB, isSyncTest bool) context.Context {
	if isSyncTest {
		return context.Background()
	}
	if _, ok := tb.(testing.TB); !ok {
		// 不在 Go 单元测试中。
		return context.Background()
	}

	tb.Helper()

	// 如果已经被中断，就不要启动新的测试。
	// 这是必要的，因为通常 CTRL-C 会立即退出
	// 整个进程。现在我们继续运行，
	// 以便进行清理。
	if interruptCtx.Err() != nil {
		tb.Fatalf("testing has been interrupted: %v", context.Cause(interruptCtx))
	}

	p.initMutex.Lock()
	defer p.initMutex.Unlock()

	p.usageCount++
	tb.Cleanup(p.finalize)
	if p.usageCount > 1 {
		// 已经初始化过了。
		return p.testCtx
	}

	// 可能是为了测试而设置的。
	if p.out == nil {
		// "go test" 会重定向 os.Stderr。必须使用
		// "go test -v" 才能在测试运行时看到这些输出。
		//
		// 在 init 时打开 /dev/tty 可以避免重定向。
		// 根据操作系统的不同，这可能失败，此时
		// 会使用 os.Stderr。
		if console, err := os.OpenFile("/dev/tty", os.O_RDWR|os.O_APPEND, 0); err == nil {
			p.out = console
			p.closeOut = console.Close

		} else {
			p.out = os.Stdout
			p.closeOut = nil
		}
	}

	p.signalChannel = make(chan os.Signal)
	signal.Notify(p.signalChannel, os.Interrupt)
	p.wg.Go(func() {
		_, ok := <-p.signalChannel
		if ok {
			_, _ = fmt.Fprint(p.out, "\n\nINFO: canceling test context: received interrupt signal\n\n")
			interrupted(errors.New("received interrupt signal"))
		}
	})

	// 这里重新实现了 Ginkgo 与 Gomega 之间关于进度报告的约定。
	// 使用 Ginkgo context 时，该约定由 Ginkgo 实现。这里是为 "go test" 准备的。
	//
	// nolint:staticcheck // It complains about using a plain string. This can only be fixed
	// 这只能由 Ginkgo 和 Gomega 正式化该接口并定义一个类型（某个地方...
	// 可能既不能放在 Ginkgo 里，也不能放在 Gomega 里）。
	p.testCtx = context.WithValue(interruptCtx, ginkgoSpecContextKey, defaultProgressReporter)

	p.progressChannel = make(chan os.Signal, 1)
	// 在 Windows 上 progressSignals 为空。
	if len(progressSignals) > 0 {
		signal.Notify(p.progressChannel, progressSignals...)
	}

	p.wg.Go(p.run)

	return p.testCtx
}

func (p *progressReporter) finalize() {
	p.initMutex.Lock()
	defer p.initMutex.Unlock()

	p.usageCount--
	if p.usageCount > 0 {
		// 仍在使用中。
		return
	}

	signal.Stop(p.signalChannel)
	close(p.signalChannel)
	signal.Stop(p.progressChannel)
	close(p.progressChannel)
	p.wg.Wait()

	// 既然所有 goroutine 都已停止，就可以再做些清理。
	if p.closeOut != nil {
		_ = p.closeOut()
		p.out = nil
	}
}

// AttachProgressReporter 实现了 Gomega 的 contextWithAttachProgressReporter。
func (p *progressReporter) AttachProgressReporter(reporter func() string) func() {
	p.reportMutex.Lock()
	defer p.reportMutex.Unlock()

	// TODO (?): 识别调用方并记录到 dumpProgress，供其使用。
	p.reporterCounter++
	id := p.reporterCounter
	if p.reporters == nil {
		p.reporters = make(map[int64]func() string)
	}
	p.reporters[id] = reporter
	return func() {
		p.detachProgressReporter(id)
	}
}

func (p *progressReporter) detachProgressReporter(id int64) {
	p.reportMutex.Lock()
	defer p.reportMutex.Unlock()

	delete(p.reporters, id)
}

func (p *progressReporter) run() {
	for {
		_, ok := <-p.progressChannel
		if !ok {
			// 关闭。
			return
		}
		p.dumpProgress()
	}
}

// dumpProgress 不如 Ginkgo 的进度报告有用。我们无法解决
// 不知道当前正在运行哪些测试的问题，只能依靠
// "go test -v" 来了解这一点。
//
// 不过，转储 goroutine 及其调用栈或许仍然有用？TODO：
// 看看 Ginkgo 是怎么做的，并借鉴其中一部分。
func (p *progressReporter) dumpProgress() {
	p.reportMutex.Lock()
	defer p.reportMutex.Unlock()

	var buffer strings.Builder
	buffer.WriteString("You requested a progress report.\n")
	if len(p.reporters) == 0 {
		buffer.WriteString("Currently there is no information about test progress available.\n")
	}
	for _, reporter := range p.reporters {
		report := reporter()
		buffer.WriteRune('\n')
		buffer.WriteString(report)
		if !strings.HasSuffix(report, "\n") {
			buffer.WriteRune('\n')
		}
	}

	_, _ = p.out.Write([]byte(buffer.String()))
}
