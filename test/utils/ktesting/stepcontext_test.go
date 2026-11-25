/*
Copyright 2024 The Kubernetes Authors.

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
	"io"
	"os"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"go.uber.org/goleak"
)

func TestStepContext(t *testing.T) {
	for name, tc := range map[string]testcase{
		"output": {
			cb: func(tCtx TContext) {
				tCtx = tCtx.WithStep("step")
				tCtx.Log("Log", "a", "b", 42)
				tCtx.Logf("Logf %s %s %d", "a", "b", 42)
				tCtx.Error("Error", "a", "b", 42)
				tCtx.Errorf("Errorf %s %s %d", "a", "b", 42)
			},
			expectTrace: `(LOG) <klog header>: step: Log a b 42
(LOG) <klog header>: step: Logf a b 42
(ERROR) ERROR: <klog header>:
	step: Error a b 42
(ERROR) ERROR: <klog header>:
	step: Errorf a b 42
`,
		},
		"fatal": {
			cb: func(tCtx TContext) {
				tCtx = tCtx.WithStep("step")
				tCtx.Fatal("Error", "a", "b", 42)
				// 不会执行到
				tCtx.Log("Log")
			},
			expectTrace: `(FATAL) FATAL ERROR: <klog header>:
	step: Error a b 42
`,
		},
		"fatalf": {
			cb: func(tCtx TContext) {
				tCtx = tCtx.WithStep("step")
				tCtx.Fatalf("Error %s %s %d", "a", "b", 42)
				// 不会执行到
				tCtx.Log("Log")
			},
			expectTrace: `(FATAL) FATAL ERROR: <klog header>:
	step: Error a b 42
`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			tc.run(t)
		})
	}
}

func TestProgressReport(t *testing.T) {
	oldOut := defaultProgressReporter.out
	out := newOutputStream()
	defaultProgressReporter.out = out
	t.Cleanup(func() {
		goleak.VerifyNone(t)
		defaultProgressReporter.out = oldOut

		// 如果执行到这里，defaultProgressReporter 已不再处于活动状态，
		// 但中断上下文仍应已被取消。
		gomega.NewGomegaWithT(t).Expect(defaultProgressReporter.usageCount).To(gomega.Equal(int64(0)), "usage count")
		gomega.NewGomegaWithT(t).Expect(context.Cause(interruptCtx)).To(gomega.MatchError(gomega.Equal("received interrupt signal")), "interrupted persistently")

		// 为下一个测试重置。
		interruptCtx, interrupted = context.WithCancelCause(context.Background())
	})

	// 这里必须使用真正的 testing.T，否则 Init 不会初始化信号处理。
	tCtx := Init(t)
	tCtx = tCtx.WithStep("step")
	removeReporter := tCtx.Value("GINKGO_SPEC_CONTEXT").(ginkgoReporter).AttachProgressReporter(func() string { return "hello world" })
	defer removeReporter()
	tCtx.Expect(tCtx.Value("some other key")).To(gomega.BeNil(), "value for unknown context value key")

	// 触发报告并等待其完成。
	defaultProgressReporter.progressChannel <- os.Interrupt
	report := <-out.stream
	tCtx.Expect(report).To(gomega.Equal(`You requested a progress report.

step: hello world
`), "report")

	gomega.NewGomegaWithT(t).Expect(context.Cause(interruptCtx)).To(gomega.Succeed(), "not interrupted yet")
	defaultProgressReporter.signalChannel <- os.Interrupt
	message := <-out.stream
	tCtx.Expect(message).To(gomega.Equal(`

INFO: canceling test context: received interrupt signal

`))
	gomega.NewGomegaWithT(t).Eventually(func() error { return context.Cause(tCtx) }).WithTimeout(30*time.Second).To(gomega.MatchError(gomega.Equal("received interrupt signal")), "interrupted")
}

// outputStream 将一个 Write 调用转发到流中。
// 第二次 Write 调用是错误的，会触发 panic。
type outputStream struct {
	stream chan string
}

var _ io.Writer = &outputStream{}

func newOutputStream() *outputStream {
	return &outputStream{
		stream: make(chan string),
	}
}

func (s *outputStream) Write(buf []byte) (int, error) {
	s.stream <- string(buf)
	return len(buf), nil
}
