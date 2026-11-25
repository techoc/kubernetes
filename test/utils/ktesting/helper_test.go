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

	"github.com/onsi/gomega"
)

// testcase 包装了一个回调函数，该函数会接收一个拦截错误和日志输出的 TContext。
// 这些内容随后会被用于比较。
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
		tCtx.Assert(duration).To(gomega.Equal(tc.expectDuration), "callback invocation duration %s")
		tCtx.Assert(normalize(trace)).To(gomega.Equal(tc.expectTrace))
	})
}

// normalize 将消息文本中可能变化的部分替换为固定字符串。
func normalize(msg string) string {
	// 时长
	msg = regexp.MustCompile(`[[:digit:]]+\.[[:digit:]]+s`).ReplaceAllString(msg, "x.y s")
	// 十六进制指针值
	msg = regexp.MustCompile(`0x[[:xdigit:]]+`).ReplaceAllString(msg, "0xXXXX")
	// 每个测试的 klog 头
	msg = regexp.MustCompile(`[EI][[:digit:]]{4} [[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}\.[[:digit:]]{6}\]`).ReplaceAllString(msg, "<klog header>:")
	return msg
}

// mockTB 记录发生了哪些调用（类型和参数）。
//
// 最终得到的字符串看起来与 `go test -v` 中可见的输出类似，
// 只是能看到“香肠是怎么做出来的”（Fatal 与 Log + FailNow 的区别）。
// 对于 testing.T，Log+FailNow 和 Fatal 是等价的，
// 但如果底层 TB 是 Ginkgo 则不然，因为只有使用 Fatal 时
// 它才能正确捕获失败消息。
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
	// Init 总是会调用它，但不记录到日志，因为会造成干扰。
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
	// TODO：加入栈回溯，以验证 Helper 是否在正确的位置被调用。
	// 仅仅记录日志还不够。
	// m.log.WriteString("HELPER\n")
}

func (m *mockTB) Log(args ...any) {
	fmt.Fprintln(&m.log, append([]any{"(LOG)"}, args...)...)
}

func (m *mockTB) Logf(format string, args ...any) {
	fmt.Fprintf(&m.log, "(LOGF) "+format+"\n", args)
}

func (m *mockTB) Name() string {
	// Init 总是会调用它，但不记录到日志，因为会造成干扰。
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
