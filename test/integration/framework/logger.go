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

package framework

import (
	"flag"
	"io"
	"testing"

	"k8s.io/klog/v2"
)

// RedirectKlog 修改全局 klog logger，使其通过给定的 writer 写入。
// 这仅在不同测试顺序执行时才有效。
//
// 返回的清理函数会恢复之前的状态。注意它不是线程安全的，
// 所有调用 klog 的 goroutine 都必须已经停止。
func RedirectKlog(tb testing.TB, output io.Writer) func() {
	expectNoError := func(err error) {
		if err != nil {
			tb.Fatalf("unexpected error: %v", err)
		}
	}

	state := klog.CaptureState()
	defer func() {
		if r := recover(); r != nil {
			state.Restore()
			panic(r)
		}
	}()
	var fs flag.FlagSet
	klog.InitFlags(&fs)
	expectNoError(fs.Set("log_file", "/dev/null"))
	expectNoError(fs.Set("logtostderr", "false"))
	expectNoError(fs.Set("alsologtostderr", "false"))
	expectNoError(fs.Set("stderrthreshold", "10"))
	expectNoError(fs.Set("one_output", "true"))
	klog.SetOutput(output)
	return state.Restore
}

// NewTBWriter 创建一个 io.Writer，将每次写入转换为一次 tb.Log 调用。
//
// 注意，这里不会尝试确定实际的调用位置，因为我们的调用方不知道 TB 实例，
// 因此无法将自身标记为 helper。所以这里的代码也不这样做，
// 从而在测试输出中显示为调用位置。要避免这种情况，必须使用
// contextual logging 和 ktesting。
func NewTBWriter(tb testing.TB) io.Writer {
	return testingWriter{TB: tb}
}

type testingWriter struct {
	testing.TB
}

func (tw testingWriter) Write(data []byte) (int, error) {
	logLen := len(data)
	if logLen == 0 {
		return 0, nil
	}
	// 去掉行尾的换行符？Log 会自行添加。
	if data[logLen-1] == '\n' {
		logLen--
	}
	// 我们确实可以在这里调用 TB.Helper，但这并没有什么帮助，
	// 因为那样会被报告为调用方的是 klog 中的代码，这同样不正确。
	// klog 必须自己调用 TB.Helper，但它并不知道 TB 实例。
	tw.Log(string(data[:logLen]))
	return len(data), nil
}

var _ io.Writer = testingWriter{}
