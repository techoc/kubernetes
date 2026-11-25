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

package exec

import (
	"fmt"
	"io"
	"strings"
	"testing"

	remote "k8s.io/cri-client/pkg"
	"k8s.io/kubernetes/pkg/probe"
)

// FakeCmd 模拟 CRI 的 Cmd 接口，用于测试 exec 探针而不实际执行命令。
// 通过配置 out、stdout、err 字段来控制 Probe 的行为。
type FakeCmd struct {
	out    []byte
	stdout []byte
	err    error
	writer io.Writer
}

func (f *FakeCmd) Run() error {
	return nil
}

// CombinedOutput 返回预置输出和错误，模拟 exec 探针读取合并输出的路径。
func (f *FakeCmd) CombinedOutput() ([]byte, error) {
	return f.out, f.err
}

// Output 返回预置标准输出和错误，便于单独验证 stdout 处理。
func (f *FakeCmd) Output() ([]byte, error) {
	return f.stdout, f.err
}

func (f *FakeCmd) SetDir(dir string) {}

func (f *FakeCmd) SetStdin(in io.Reader) {}

func (f *FakeCmd) SetStdout(out io.Writer) {
	f.writer = out
}

func (f *FakeCmd) SetStderr(out io.Writer) {
	f.writer = out
}

func (f *FakeCmd) SetEnv(env []string) {}

func (f *FakeCmd) Stop() {}

// Start 在配置 writer 时同步写入预置输出，模拟探针启动命令后的输出读取。
func (f *FakeCmd) Start() error {
	if f.writer != nil {
		f.writer.Write(f.out)
		return f.err
	}
	return f.err
}

func (f *FakeCmd) Wait() error { return nil }

func (f *FakeCmd) StdoutPipe() (io.ReadCloser, error) {
	return nil, nil
}

func (f *FakeCmd) StderrPipe() (io.ReadCloser, error) {
	return nil, nil
}

// fakeExitError 模拟进程退出错误，实现 Exited() 和 ExitStatus() 方法，
// 用于测试 Probe 对退出码的判断逻辑（0=Success, 非0=Failure）。
type fakeExitError struct {
	exited     bool
	statusCode int
}

func (f *fakeExitError) String() string {
	return f.Error()
}

func (f *fakeExitError) Error() string {
	return "fake exit"
}

func (f *fakeExitError) Exited() bool {
	return f.exited
}

func (f *fakeExitError) ExitStatus() int {
	return f.statusCode
}

// TestExec 验证 exec 探针的各种场景：
//
//  1. 成功执行（无错误/退出码0）
//  2. 输出截断（超过10KB 只返回前10KB）
//  3. 执行错误（非退出错误，返回 Unknown）
//  4. 非零退出码（返回 Failure）
//  5. 命令超时（返回 Failure）
func TestExec(t *testing.T) {
	prober := New()

	tenKilobyte := strings.Repeat("logs-123", 128*10)      // 8*128*10=10240，即 10KB 文本。
	elevenKilobyte := strings.Repeat("logs-123", 8*128*11) // 8*128*11=11264，即 11KB 文本。

	tests := []struct {
		expectedStatus probe.Result
		expectError    bool
		input          string
		output         string
		err            error
	}{
		// 成功：无错误
		{probe.Success, false, "OK", "OK", nil},
		// 成功：退出码为 0
		{probe.Success, false, "OK", "OK", &fakeExitError{true, 0}},
		// 成功：输出超过10KB 被截断为10KB
		{probe.Success, false, elevenKilobyte, tenKilobyte, nil},
		// 未知：非退出错误（如命令执行失败）
		{probe.Unknown, true, "", "", fmt.Errorf("test error")},
		// 失败：非零退出码
		{probe.Failure, false, "Fail", "", &fakeExitError{true, 1}},
		// 失败：命令超时
		{probe.Failure, false, "", remote.ErrCommandTimedOut.Error() + ": command testcmd timed out", fmt.Errorf("%w: command testcmd timed out", remote.ErrCommandTimedOut)},
	}

	for i, test := range tests {
		fake := FakeCmd{
			out: []byte(test.output),
			err: test.err,
		}
		status, output, err := prober.Probe(&fake)
		if status != test.expectedStatus {
			t.Errorf("[%d] expected %v, got %v", i, test.expectedStatus, status)
		}
		if err != nil && !test.expectError {
			t.Errorf("[%d] unexpected error: %v", i, err)
		}
		if err == nil && test.expectError {
			t.Errorf("[%d] unexpected non-error", i)
		}
		if test.output != output {
			t.Errorf("[%d] expected %s, got %s", i, test.output, output)
		}
	}
}
