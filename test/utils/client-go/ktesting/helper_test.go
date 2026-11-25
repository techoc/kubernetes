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
	"fmt"
	"regexp"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// testcase 包装一个回调函数，该函数会通过一个拦截错误和日志输出的 TContext 来调用。
// 随后会对这些内容进行比较。
type testcase struct {
	cb             func(TContext)
	expectDuration time.Duration
	expectTrace    string
}

func (tc testcase) run(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buffer := &mockTB{}
		tCtx := Init(buffer)
		start := time.Now()
		func() {
			defer func() {
				if r := recover(); r != nil && r != logBufferStop {
					panic(r)
				}
			}()
			tc.cb(tCtx)
		}()
		duration := time.Since(start)

		trace := buffer.log.String()
		t.Logf("Trace:\n%s\n", trace)
		assert.Equal(t, tc.expectDuration, duration, "callback invocation duration %s")
		assert.Equal(t, tc.expectTrace, normalize(trace))
	})
}

// normalize 将消息文本中可能变化的部分替换为常量字符串。
func normalize(msg string) string {
	// 持续时间
	msg = regexp.MustCompile(`[[:digit:]]+\.[[:digit:]]+s`).ReplaceAllString(msg, "x.y s")
	// 十六进制指针值
	msg = regexp.MustCompile(`0x[[:xdigit:]]+`).ReplaceAllString(msg, "0xXXXX")
	// 每个测试的 klog 头部
	msg = regexp.MustCompile(`[EI][[:digit:]]{4} [[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}\.[[:digit:]]{6}\]`).ReplaceAllString(msg, "<klog header>:")
	return msg
}

// mockTB 记录发生了哪些调用（类型与参数）。
//
// 最终得到的字符串与 `go test -v` 中可见的输出类似，
// 区别在于可以看到“香肠是怎么做出来的”（Fatal 与 Log + FailNow）。
// 对于 testing.T，Log+FailNow 与 Fatal 是等价的，但对于以 Ginkgo 作为底层 TB 的情况则不然，
// 因为只有使用 Fatal 时它才能正确捕获失败消息。
type mockTB struct {
	log strings.Builder
}

func (m *mockTB) Attr(key, value string) {
	fmt.Fprintf(&m.log, "(ATTR) %q %q\n", key, value)
}

func (m *mockTB) Chdir(dir string) {
	fmt.Fprintf(&m.log, "(CHDIR) %q\n", dir)
}

func (m *mockTB) Cleanup(func()) {
	// 总会被 Init 调用，但不记录下来，因为它会干扰视线。
	// m.log.WriteString("(CLEANUP)\n")
}

func (m *mockTB) Error(args ...any) {
	fmt.Fprintln(&m.log, append([]any{"(ERROR)"}, args...)...)
}

func (m *mockTB) Errorf(format string, args ...any) {
	fmt.Fprintf(&m.log, "(ERRORF) "+format+"\n", args)
}

func (m *mockTB) Fail() {
	m.log.WriteString("(FAIL)\n")
}

func (m *mockTB) FailNow() {
	m.log.WriteString("(FAILNOW)\n")
	panic(logBufferStop)
}

func (m *mockTB) Failed() bool {
	m.log.WriteString("(FAILED)\n")
	return false
}

func (m *mockTB) Fatal(args ...any) {
	fmt.Fprintln(&m.log, append([]any{"(FATAL)"}, args...)...)
	panic(logBufferStop)
}

func (m *mockTB) Fatalf(format string, args ...any) {
	fmt.Fprintf(&m.log, "(FATALF) "+format+"\n", args)
	panic(logBufferStop)
}

func (m *mockTB) Helper() {
	// TODO: 加入堆栈回溯，以验证 Helper 在正确的位置被调用。
	// 仅仅记录它是不够的。
	// m.log.WriteString("HELPER\n")
}

func (m *mockTB) Log(args ...any) {
	fmt.Fprintln(&m.log, append([]any{"(LOG)"}, args...)...)
}

func (m *mockTB) Logf(format string, args ...any) {
	fmt.Fprintf(&m.log, "(LOGF) "+format+"\n", args)
}

func (m *mockTB) Name() string {
	// 总会被 Init 调用，但不记录下来，因为它会干扰视线。
	// m.log.WriteString("(NAME)\n")
	return "logBufferT"
}

func (m *mockTB) Setenv(key, value string) {
	m.log.WriteString("(SETENV)\n")
}

func (m *mockTB) Skip(args ...any) {
	fmt.Fprintln(&m.log, append([]any{"(SKIP)"}, args...)...)
	panic(logBufferStop)
}

func (m *mockTB) SkipNow() {
	m.log.WriteString("(SKIPNOW)\n")
	panic(logBufferStop)
}

func (m *mockTB) Skipf(format string, args ...any) {
	fmt.Fprintf(&m.log, "(SKIPF) "+format+"\n", args)
	panic(logBufferStop)
}

func (m *mockTB) Skipped() bool {
	m.log.WriteString("(SKIPPED)\n")
	return false
}

func (m *mockTB) TempDir() string {
	m.log.WriteString("(TEMPDIR)\n")
	return "/no-such-dir"
}

var (
	logBufferStop = "STOP"
)
