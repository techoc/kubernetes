/*
Copyright 2019 The Kubernetes Authors.

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

package utils

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/exec"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

// Result 保存远程命令的执行结果。
type Result struct {
	Host   string
	Cmd    string
	Stdout string
	Stderr string
	Code   int
}

// LogResult 记录执行结果日志。
func LogResult(result Result) {
	remote := result.Host
	framework.Logf("exec %s: command:   %s", remote, result.Cmd)
	framework.Logf("exec %s: stdout:    %q", remote, result.Stdout)
	framework.Logf("exec %s: stderr:    %q", remote, result.Stderr)
	framework.Logf("exec %s: exit code: %d", remote, result.Code)
}

// HostExec 定义了在远程主机上执行命令所需的接口。
type HostExec interface {
	Execute(ctx context.Context, cmd string, node *v1.Node) (Result, error)
	IssueCommandWithResult(ctx context.Context, cmd string, node *v1.Node) (string, error)
	IssueCommand(ctx context.Context, cmd string, node *v1.Node) error
	Cleanup(ctx context.Context)
}

// hostExecutor 实现 HostExec。
type hostExecutor struct {
	*framework.Framework
	nodeExecPods map[string]*v1.Pod
}

// NewHostExec 返回一个 HostExec。
func NewHostExec(framework *framework.Framework) HostExec {
	return &hostExecutor{
		Framework:    framework,
		nodeExecPods: make(map[string]*v1.Pod),
	}
}

// launchNodeExecPod 为本地 PV 启动一个 hostexec Pod，并等待其进入 Running 状态。
func (h *hostExecutor) launchNodeExecPod(ctx context.Context, node string) *v1.Pod {
	f := h.Framework
	cs := f.ClientSet
	ns := f.Namespace

	hostExecPod := e2epod.NewExecPodSpec(ns.Name, "", true)
	hostExecPod.GenerateName = fmt.Sprintf("hostexec-%s-", node)

	if framework.TestContext.NodeE2E {
		// E2E node 测试不运行 scheduler，因此直接指定节点名称。
		hostExecPod.Spec.NodeName = node
	} else {
		// 使用 NodeAffinity 而不是 NodeName，这样在空间不足时 Pod
		// 不会被 kubelet 立即置为 Failed，而是由 scheduler 保持 Pending，
		// 直到有空间释放。
		e2epod.SetNodeAffinity(&hostExecPod.Spec, node)

	}
	hostExecPod.Spec.Volumes = []v1.Volume{
		{
			// 通过 nsenter 进入宿主机 mount namespace 所必需。
			Name: "rootfs",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{
					Path: "/",
				},
			},
		},
	}
	hostExecPod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
		{
			Name:      "rootfs",
			MountPath: "/rootfs",
			ReadOnly:  true,
		},
	}
	hostExecPod.Spec.Containers[0].SecurityContext = &v1.SecurityContext{
		Privileged: func(privileged bool) *bool {
			return &privileged
		}(true),
	}
	pod, err := cs.CoreV1().Pods(ns.Name).Create(ctx, hostExecPod, metav1.CreateOptions{})
	framework.ExpectNoError(err)
	err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, cs, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err)
	return pod
}

// Execute 在指定节点上执行命令。如果远程命令执行没有错误，
// 则返回 stdout、stderr 和退出码。
// 其行为类似 ssh.SSH(...) 工具。
func (h *hostExecutor) Execute(ctx context.Context, cmd string, node *v1.Node) (Result, error) {
	result, err := h.exec(ctx, cmd, node)
	if codeExitErr, ok := err.(exec.CodeExitError); ok {
		// 提取远程命令的退出码，并抑制非零退出码错误。
		result.Code = codeExitErr.ExitStatus()
		err = nil
	}
	return result, err
}

func (h *hostExecutor) exec(ctx context.Context, cmd string, node *v1.Node) (Result, error) {
	result := Result{
		Host: node.Name,
		Cmd:  cmd,
	}
	pod, ok := h.nodeExecPods[node.Name]
	if !ok {
		pod = h.launchNodeExecPod(ctx, node.Name)
		if pod == nil {
			return result, fmt.Errorf("failed to create hostexec pod for node %q", node)
		}
		h.nodeExecPods[node.Name] = pod
	}
	args := []string{
		"nsenter",
		"--mount=/rootfs/proc/1/ns/mnt",
		"--",
		"sh",
		"-c",
		cmd,
	}
	containerName := pod.Spec.Containers[0].Name
	var err error
	result.Stdout, result.Stderr, err = e2epod.Exec(h.Framework.TContext(ctx), e2epod.ExecOptions{
		Command:            args,
		Namespace:          pod.Namespace,
		PodName:            pod.Name,
		ContainerName:      containerName,
		Stdin:              nil,
		CaptureStdout:      true,
		CaptureStderr:      true,
		PreserveWhitespace: true,
	})
	return result, err
}

// IssueCommandWithResult 在指定节点上执行命令并将 stdout 作为结果返回。
// 如果命令执行出错或以非零状态退出，则返回错误。
func (h *hostExecutor) IssueCommandWithResult(ctx context.Context, cmd string, node *v1.Node) (string, error) {
	result, err := h.exec(ctx, cmd, node)
	if err != nil {
		LogResult(result)
	}
	return result.Stdout, err
}

// IssueCommand 与 IssueCommandWithResult 类似，但丢弃结果。
func (h *hostExecutor) IssueCommand(ctx context.Context, cmd string, node *v1.Node) error {
	_, err := h.IssueCommandWithResult(ctx, cmd, node)
	return err
}

// Cleanup 清理测试期间创建的资源。
// 注意大多数情况下无需调用它，因为 Pod 创建在测试命名空间下，
// 会在 teardown 阶段被销毁。
func (h *hostExecutor) Cleanup(ctx context.Context) {
	for _, pod := range h.nodeExecPods {
		e2epod.DeletePodOrFail(ctx, h.Framework.ClientSet, pod.Namespace, pod.Name)
	}
	h.nodeExecPods = make(map[string]*v1.Pod)
}
