/*
Copyright 2016 The Kubernetes Authors.

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

package pod

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	clientexec "k8s.io/client-go/util/exec"
	"k8s.io/kubectl/pkg/cmd/exec"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// ExecOptions 控制 [Exec] 如何在 pod 容器内运行命令。
// 至少要设置 Command、Namespace 和 PodName。
type ExecOptions struct {
	Command       []string
	Namespace     string
	PodName       string
	ContainerName string
	Stdin         io.Reader
	CaptureStdout bool
	CaptureStderr bool
	// 为 false 时，会去掉 std{err,out} 中的空白。
	PreserveWhitespace bool
	Quiet              bool
}

// Exec 在指定容器中执行命令，
// 返回 stdout、stderr 和 error。`options` 允许
// 传入额外参数。
func Exec(tCtx ktesting.TContext, options ExecOptions) (string, string, error) {
	if !options.Quiet {
		tCtx.Logf("Exec %+v", options)
	}

	const tty = false
	tCtx.Logf("Exec: Clientset creation")
	req := tCtx.Client().CoreV1().RESTClient().Post().
		Resource("pods").
		Name(options.PodName).
		Namespace(options.Namespace).
		SubResource("exec")
	req.VersionedParams(&v1.PodExecOptions{
		Container: options.ContainerName,
		Command:   options.Command,
		Stdin:     options.Stdin != nil,
		Stdout:    options.CaptureStdout,
		Stderr:    options.CaptureStderr,
		TTY:       tty,
	}, scheme.ParameterCodec)

	var stdout, stderr bytes.Buffer
	tCtx.Logf("Exec: execute(%s)", req.URL())
	executor := exec.DefaultRemoteExecutor{}
	err := executor.ExecuteWithContext(tCtx, req.URL(), tCtx.RESTConfig(), options.Stdin, &stdout, &stderr, tty, nil)

	if options.PreserveWhitespace {
		return stdout.String(), stderr.String(), err
	}
	return strings.TrimSpace(stdout.String()), strings.TrimSpace(stderr.String()), err
}

// ExecCommandInContainerWithFullOutput 在
// 指定容器中执行命令并返回 stdout、stderr 和 error
func ExecCommandInContainerWithFullOutput(f *framework.Framework, podName, containerName string, cmd ...string) (string, string, error) {
	// TODO (pohly): 添加 context 支持
	return Exec(f.TContext(context.Background()), ExecOptions{
		Command:            cmd,
		Namespace:          f.Namespace.Name,
		PodName:            podName,
		ContainerName:      containerName,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: false,
	})
}

// ExecCommandInContainer 在指定容器中执行命令。
func ExecCommandInContainer(f *framework.Framework, podName, containerName string, cmd ...string) string {
	stdout, stderr, err := ExecCommandInContainerWithFullOutput(f, podName, containerName, cmd...)
	framework.Logf("Exec stderr: %q", stderr)
	framework.ExpectNoError(err,
		"failed to execute command in pod %v, container %v: %v",
		podName, containerName, err)
	return stdout
}

// ExecShellInContainer 在 pod 的容器上执行指定命令。
func ExecShellInContainer(f *framework.Framework, podName, containerName string, cmd string) string {
	return ExecCommandInContainer(f, podName, containerName, "/bin/sh", "-c", cmd)
}

// ExecShellInPod 在 pod 上执行指定命令。
func ExecShellInPod(ctx context.Context, f *framework.Framework, podName string, cmd string) string {
	pod, err := NewPodClient(f).Get(ctx, podName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get pod %v", podName)
	gomega.Expect(pod.Spec.Containers).NotTo(gomega.BeEmpty())
	return ExecCommandInContainer(f, podName, pod.Spec.Containers[0].Name, "/bin/sh", "-c", cmd)
}

// ExecShellInPodWithFullOutput 在 Pod 上执行指定命令并返回 stdout、stderr 和 error。
func ExecShellInPodWithFullOutput(ctx context.Context, f *framework.Framework, podName string, cmd string) (string, string, error) {
	pod, err := NewPodClient(f).Get(ctx, podName, metav1.GetOptions{})
	framework.ExpectNoError(err, "failed to get pod %v", podName)
	gomega.Expect(pod.Spec.Containers).NotTo(gomega.BeEmpty())
	return ExecCommandInContainerWithFullOutput(f, podName, pod.Spec.Containers[0].Name, "/bin/sh", "-c", cmd)
}

// VerifyExecInPodSucceed 验证目标 pod 中的 shell 命令成功
func VerifyExecInPodSucceed(ctx context.Context, f *framework.Framework, pod *v1.Pod, shExec string) error {
	stdout, stderr, err := ExecShellInPodWithFullOutput(ctx, f, pod.Name, shExec)
	if err != nil {
		var exitError clientexec.CodeExitError
		if errors.As(err, &exitError) {
			exitCode := exitError.ExitStatus()
			return fmt.Errorf("%q should succeed, but failed with exit code %d and error message %w\nstdout: %s\nstderr: %s",
				shExec, exitCode, exitError, stdout, stderr)
		} else {
			return fmt.Errorf("%q should succeed, but failed with error message %w\nstdout: %s\nstderr: %s",
				shExec, err, stdout, stderr)
		}
	}
	return nil
}

// VerifyExecInPodFail 验证目标 pod 中的 shell 命令以指定退出码失败
func VerifyExecInPodFail(ctx context.Context, f *framework.Framework, pod *v1.Pod, shExec string, exitCode int) error {
	stdout, stderr, err := ExecShellInPodWithFullOutput(ctx, f, pod.Name, shExec)
	if err != nil {
		var exitError clientexec.CodeExitError
		if errors.As(err, &exitError) {
			actualExitCode := exitError.ExitStatus()
			if actualExitCode == exitCode {
				return nil
			}
			return fmt.Errorf("%q should fail with exit code %d, but failed with exit code %d and error message %w\nstdout: %s\nstderr: %s",
				shExec, exitCode, actualExitCode, exitError, stdout, stderr)
		} else {
			return fmt.Errorf("%q should fail with exit code %d, but failed with error message %w\nstdout: %s\nstderr: %s",
				shExec, exitCode, err, stdout, stderr)
		}
	}
	return fmt.Errorf("%q should fail with exit code %d, but exit without error", shExec, exitCode)
}
