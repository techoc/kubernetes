/*
Copyright 2014 The Kubernetes Authors.

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

package kubectl

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"k8s.io/client-go/tools/clientcmd"
	uexec "k8s.io/utils/exec"

	"k8s.io/kubernetes/test/e2e/framework"
)

// KubectlBuilder 用于构建、定制并执行 kubectl 命令。
// 需要更多定制能力时，继续往 builder 上加函数。
type KubectlBuilder struct {
	cmd *exec.Cmd
	// appendedEnv 只含 AppendEnv(...) 设置的值，不含 os.Environ()
	// 打印 os.Environ 基本是冗余且嘈杂的
	// 打印我们改动的环境变量，才能看清实际运行环境和默认环境的差异
	appendedEnv []string
	timeout     <-chan time.Time
}

// NewKubectlCommand 返回一个用于运行 kubectl 的 KubectlBuilder。
func NewKubectlCommand(namespace string, args ...string) *KubectlBuilder {
	b := new(KubectlBuilder)
	tk := NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, namespace)
	b.cmd = tk.KubectlCmd(args...)
	return b
}

// AppendEnv 追加给定的环境变量并返回自身。
func (b *KubectlBuilder) AppendEnv(env []string) *KubectlBuilder {
	if b.cmd.Env == nil {
		b.cmd.Env = os.Environ()
	}
	b.cmd.Env = append(b.cmd.Env, env...)
	b.appendedEnv = append(b.appendedEnv, env...)
	return b
}

// WithTimeout 设置给定的超时并返回自身。
func (b *KubectlBuilder) WithTimeout(t <-chan time.Time) *KubectlBuilder {
	b.timeout = t
	return b
}

// WithStdinData 把给定数据设为 stdin 并返回自身。
func (b KubectlBuilder) WithStdinData(data string) *KubectlBuilder {
	b.cmd.Stdin = strings.NewReader(data)
	return &b
}

// WithStdinReader 设置给定的 reader 并返回自身。
func (b KubectlBuilder) WithStdinReader(reader io.Reader) *KubectlBuilder {
	b.cmd.Stdin = reader
	return &b
}

// ExecOrDie 运行 kubectl 可执行文件，出错时终止测试。
func (b KubectlBuilder) ExecOrDie(namespace string) string {
	str, err := b.Exec()
	// 遇到 I/O 超时错误时，先等 2s 再访问一次 apiserver，然后再决定是否终止。
	// 重试后仍会终止，但这样可以留下更多线索供进一步排查。
	if isTimeout(err) {
		framework.Logf("Hit i/o timeout error, talking to the server 2s later to see if it's temporary.")
		time.Sleep(2 * time.Second)
		retryStr, retryErr := RunKubectl(namespace, "version")
		framework.Logf("stdout: %q", retryStr)
		framework.Logf("err: %v", retryErr)
	}
	framework.ExpectNoError(err)
	return str
}

func isTimeout(err error) bool {
	switch err := err.(type) {
	case *url.Error:
		if err, ok := err.Err.(net.Error); ok && err.Timeout() {
			return true
		}
	case net.Error:
		if err.Timeout() {
			return true
		}
	}
	return false
}

// Exec 运行 kubectl 可执行文件。
func (b KubectlBuilder) Exec() (string, error) {
	stdout, _, err := b.ExecWithFullOutput()
	return stdout, err
}

// ExecWithFullOutput 运行 kubectl 可执行文件，返回 stdout 和 stderr。
func (b KubectlBuilder) ExecWithFullOutput() (string, string, error) {
	var stdout, stderr bytes.Buffer
	cmd := b.cmd
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	if len(b.appendedEnv) > 0 {
		framework.Logf("Running '%s %s %s'", strings.Join(b.appendedEnv, " "), cmd.Path, strings.Join(cmd.Args[1:], " ")) // 跳过 arg[0]，因为它会单独打印
	} else {
		framework.Logf("Running '%s %s'", cmd.Path, strings.Join(cmd.Args[1:], " ")) // 跳过 arg[0]，因为它会单独打印
	}
	if err := cmd.Start(); err != nil {
		return "", "", fmt.Errorf("error starting %v:\nCommand stdout:\n%v\nstderr:\n%v\nerror:\n%v", cmd, cmd.Stdout, cmd.Stderr, err)
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- cmd.Wait()
	}()
	select {
	case err := <-errCh:
		if err != nil {
			var rc = 127
			if ee, ok := err.(*exec.ExitError); ok {
				rc = int(ee.Sys().(syscall.WaitStatus).ExitStatus())
				framework.Logf("rc: %d", rc)
			}
			return stdout.String(), stderr.String(), uexec.CodeExitError{
				Err:  fmt.Errorf("error running %v:\nCommand stdout:\n%v\nstderr:\n%v\nerror:\n%v", cmd, cmd.Stdout, cmd.Stderr, err),
				Code: rc,
			}
		}
	case <-b.timeout:
		b.cmd.Process.Kill()
		return "", "", fmt.Errorf("timed out waiting for command %v:\nCommand stdout:\n%v\nstderr:\n%v", cmd, cmd.Stdout, cmd.Stderr)
	}
	framework.Logf("stderr: %q", stderr.String())
	framework.Logf("stdout: %q", stdout.String())
	return stdout.String(), stderr.String(), nil
}

// RunKubectlOrDie 是 kubectlBuilder 的便捷包装。
func RunKubectlOrDie(namespace string, args ...string) string {
	return NewKubectlCommand(namespace, args...).ExecOrDie(namespace)
}

// RunKubectl 是 kubectlBuilder 的便捷包装。
func RunKubectl(namespace string, args ...string) (string, error) {
	return NewKubectlCommand(namespace, args...).Exec()
}

// RunKubectlWithFullOutput 是 kubectlBuilder 的便捷包装，
// 同时返回命令的 stderr。
func RunKubectlWithFullOutput(namespace string, args ...string) (string, string, error) {
	return NewKubectlCommand(namespace, args...).ExecWithFullOutput()
}

// RunKubectlOrDieInput 是 kubectlBuilder 的便捷包装，接受 stdin 输入。
func RunKubectlOrDieInput(namespace string, data string, args ...string) string {
	return NewKubectlCommand(namespace, args...).WithStdinData(data).ExecOrDie(namespace)
}

// RunKubectlInput 是 kubectlBuilder 的便捷包装，接受 stdin 输入。
func RunKubectlInput(namespace string, data string, args ...string) (string, error) {
	return NewKubectlCommand(namespace, args...).WithStdinData(data).Exec()
}

// RunKubemciWithKubeconfig 是 RunKubemciCmd 的便捷包装，会带上 kubeconfig 参数。
func RunKubemciWithKubeconfig(args ...string) (string, error) {
	if framework.TestContext.KubeConfig != "" {
		args = append(args, "--"+clientcmd.RecommendedConfigPathFlag+"="+framework.TestContext.KubeConfig)
	}
	return RunKubemciCmd(args...)
}

// RunKubemciCmd 是 kubectlBuilder 的便捷包装，用于运行 kubemci。
// 假定 kubemci 已在 PATH 中。
func RunKubemciCmd(args ...string) (string, error) {
	// 假定 kubemci 在 PATH 中。
	kubemci := "kubemci"
	b := new(KubectlBuilder)
	args = append(args, "--gcp-project="+framework.TestContext.CloudConfig.ProjectID)

	b.cmd = exec.Command(kubemci, args...)
	return b.Exec()
}
