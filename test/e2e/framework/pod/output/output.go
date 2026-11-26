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

package output

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	gomegatypes "github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

// 已弃用的常量。请改用 framework.Framework 中的超时设置。
const (
	// Poll 是轮询 Pod、Node 和 claim 的频率。
	Poll = 2 * time.Second
)

// LookForStringInPodExec 在指定 Pod 的第一个容器中执行命令，并在输出中查找给定字符串。
// 在指定 Pod 的第一个容器中执行的命令输出中查找给定字符串。
func LookForStringInPodExec(ns, podName string, command []string, expectedString string, timeout time.Duration) (result string, err error) {
	return LookForStringInPodExecToContainer(ns, podName, "", command, expectedString, timeout)
}

// LookForStringInPodExecToContainer 在指定 Pod 的指定容器中执行命令并在输出中查找给定字符串，
// 未指定容器时查第一个容器。
func LookForStringInPodExecToContainer(ns, podName, containerName string, command []string, expectedString string, timeout time.Duration) (result string, err error) {
	return lookForString(expectedString, timeout, func() string {
		args := []string{"exec", podName, fmt.Sprintf("--namespace=%v", ns)}
		if len(containerName) > 0 {
			args = append(args, fmt.Sprintf("--container=%s", containerName))
		}
		args = append(args, "--")
		args = append(args, command...)
		return e2ekubectl.RunKubectlOrDie(ns, args...)
	})
}

// lookForString 在 fn 的输出中查找给定字符串，反复调用 fn 直到
// 超时或找到字符串。返回最后一次日志，未找到时可能还返回错误。
func lookForString(expectedString string, timeout time.Duration, fn func() string) (result string, err error) {
	for t := time.Now(); time.Since(t) < timeout; time.Sleep(Poll) {
		result = fn()
		if strings.Contains(result, expectedString) {
			return
		}
	}
	err = fmt.Errorf("Failed to find \"%s\", last result: \"%s\"", expectedString, result)
	return
}

// RunHostCmd 用 `kubectl exec` 在给定 Pod 的上下文中、在 shell 内执行给定 cmd。
// 在 shell 内执行。
func RunHostCmd(ns, name, cmd string) (string, error) {
	return e2ekubectl.RunKubectl(ns, "exec", name, "--", "/bin/sh", "-x", "-c", cmd)
}

// RunHostCmdWithFullOutput 用 `kubectl exec` 在给定 Pod 的上下文中、在 shell 内执行给定 cmd，
// 同时返回命令的 stderr。
func RunHostCmdWithFullOutput(ns, name, cmd string) (string, string, error) {
	return e2ekubectl.RunKubectlWithFullOutput(ns, "exec", name, "--", "/bin/sh", "-x", "-c", cmd)
}

// RunHostCmdOrDie 调用 RunHostCmd，出错则终止。
func RunHostCmdOrDie(ns, name, cmd string) string {
	stdout, err := RunHostCmd(ns, name, cmd)
	framework.Logf("stdout: %v", stdout)
	framework.ExpectNoError(err)
	return stdout
}

// RunHostCmdWithRetries 调用 RunHostCmd 并重试所有错误，
// 直到成功或超时。
// 可配合幂等命令消除瞬时 Node 问题造成的 flake。
func RunHostCmdWithRetries(ns, name, cmd string, interval, timeout time.Duration) (string, error) {
	start := time.Now()
	for {
		out, err := RunHostCmd(ns, name, cmd)
		if err == nil {
			return out, nil
		}
		if elapsed := time.Since(start); elapsed > timeout {
			return out, fmt.Errorf("RunHostCmd still failed after %v: %w", elapsed, err)
		}
		framework.Logf("Waiting %v to retry failed RunHostCmd: %v", interval, err)
		time.Sleep(interval)
	}
}

// LookForStringInLog 在特定 Pod 容器的日志中查找给定字符串
func LookForStringInLog(ns, podName, container, expectedString string, timeout time.Duration) (result string, err error) {
	return lookForString(expectedString, timeout, func() string {
		return e2ekubectl.RunKubectlOrDie(ns, "logs", podName, container)
	})
}

// LookForStringInLogWithoutKubectl 在不使用 kubectl 的情况下，在特定 Pod 容器的日志中查找给定字符串
func LookForStringInLogWithoutKubectl(ctx context.Context, client clientset.Interface, ns string, podName string, container string, expectedString string, timeout time.Duration) (result string, err error) {
	return lookForString(expectedString, timeout, func() string {
		podLogs, err := e2epod.GetPodLogs(ctx, client, ns, podName, container)
		framework.ExpectNoError(err)
		return podLogs
	})
}

// CreateEmptyFileOnPod 在 Pod 上的给定路径创建空文件。
func CreateEmptyFileOnPod(namespace string, podName string, filePath string) error {
	_, err := e2ekubectl.RunKubectl(namespace, "exec", podName, "--", "/bin/sh", "-c", fmt.Sprintf("touch %s", filePath))
	return err
}

// DumpDebugInfo 输出测试的调试信息。
func DumpDebugInfo(ctx context.Context, c clientset.Interface, ns string) {
	sl, _ := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: labels.Everything().String()})
	for _, s := range sl.Items {
		desc, _ := e2ekubectl.RunKubectl(ns, "describe", "po", s.Name)
		framework.Logf("\nOutput of kubectl describe %v:\n%v", s.Name, desc)

		l, _ := e2ekubectl.RunKubectl(ns, "logs", s.Name, "--tail=100")
		framework.Logf("\nLast 100 log lines of %v:\n%v", s.Name, l)
	}
}

// MatchContainerOutput 创建 Pod，等待其所有容器成功退出，
// 再用 matcher 校验每个 expectedOutput 与指定容器输出的匹配情况。
func MatchContainerOutput(
	ctx context.Context,
	f *framework.Framework,
	pod *v1.Pod,
	containerName string,
	expectedOutput []string,
	matcher func(string, ...interface{}) gomegatypes.GomegaMatcher) error {

	return MatchMultipleContainerOutputs(ctx, f, pod, map[string][]string{containerName: expectedOutput}, matcher)
}

func MatchMultipleContainerOutputs(
	ctx context.Context,
	f *framework.Framework,
	pod *v1.Pod,
	expectedOutputs map[string][]string, // 容器名 -> 预期输出
	matcher func(string, ...interface{}) gomegatypes.GomegaMatcher) error {
	ns := pod.ObjectMeta.Namespace
	if ns == "" {
		ns = f.Namespace.Name
	}
	podClient := e2epod.PodClientNS(f, ns)

	createdPod := podClient.Create(ctx, pod)
	defer func() {
		ginkgo.By("delete the pod")
		podClient.DeleteSync(ctx, createdPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
	}()

	// 等待 client Pod 完成。
	podErr := e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, createdPod.Name, ns, f.Timeouts.PodStart)

	// 获取其日志，先拿到 host。
	podStatus, err := podClient.Get(ctx, createdPod.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get pod status: %w", err)
	}

	if podErr != nil {
		// Pod 失败。dump 所有容器的日志以排查问题
		_ = podutils.VisitContainers(&podStatus.Spec, podutils.AllContainers, func(c *v1.Container, containerType podutils.ContainerType) bool {
			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, ns, podStatus.Name, c.Name)
			if err != nil {
				framework.Logf("Failed to get logs from node %q pod %q container %q: %v",
					podStatus.Spec.NodeName, podStatus.Name, c.Name, err)
			} else {
				framework.Logf("Output of node %q pod %q container %q: %s", podStatus.Spec.NodeName, podStatus.Name, c.Name, logs)
			}
			return true
		})
		return fmt.Errorf("expected pod %q success: %v", createdPod.Name, podErr)
	}

	for cName, expectedOutput := range expectedOutputs {
		framework.Logf("Trying to get logs from node %s pod %s container %s: %v",
			podStatus.Spec.NodeName, podStatus.Name, cName, err)

		// 有时实际容器需要一秒才启动，尝试获取日志 60 秒
		logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, ns, podStatus.Name, cName)
		if err != nil {
			framework.Logf("Failed to get logs from node %q pod %q container %q. %v",
				podStatus.Spec.NodeName, podStatus.Name, cName, err)
			return fmt.Errorf("failed to get logs from %s for %s: %w", podStatus.Name, cName, err)
		}

		for _, expected := range expectedOutput {
			m := matcher(expected)
			matches, err := m.Match(logs)
			if err != nil {
				return fmt.Errorf("expected %q in container output: %w", expected, err)
			} else if !matches {
				return fmt.Errorf("expected %q in container output: %s", expected, m.FailureMessage(logs))
			}
		}
	}

	return nil
}

// TestContainerOutput 在给定命名空间中运行给定 Pod，等待
// podSpec 中所有容器进入 'Success' 状态，并用 substring matcher 校验
// 指定容器日志与预期输出。
func TestContainerOutput(ctx context.Context, f *framework.Framework, scenarioName string, pod *v1.Pod, containerIndex int, expectedOutput []string) {
	TestContainerOutputMatcher(ctx, f, scenarioName, pod, containerIndex, expectedOutput, gomega.ContainSubstring)
}

// TestContainerOutputRegexp 在给定命名空间中运行给定 Pod，等待
// podSpec 中所有容器进入 'Success' 状态，并用 regexp matcher 校验
// 指定容器日志与预期输出。
func TestContainerOutputRegexp(ctx context.Context, f *framework.Framework, scenarioName string, pod *v1.Pod, containerIndex int, expectedOutput []string) {
	TestContainerOutputsRegexp(ctx, f, scenarioName, pod, map[int][]string{containerIndex: expectedOutput})
}

func TestContainerOutputsRegexp(ctx context.Context, f *framework.Framework, scenarioName string, pod *v1.Pod, expectedOutputs map[int][]string) {
	TestContainerOutputsMatcher(ctx, f, scenarioName, pod, expectedOutputs, gomega.MatchRegexp)
}

// TestContainerOutputMatcher 在给定命名空间中运行给定 Pod，等待
// podSpec 中所有容器进入 'Success' 状态，并用给定 matcher 校验
// 指定容器日志与预期输出。
func TestContainerOutputMatcher(ctx context.Context, f *framework.Framework,
	scenarioName string,
	pod *v1.Pod,
	containerIndex int,
	expectedOutput []string,
	matcher func(string, ...interface{}) gomegatypes.GomegaMatcher) {
	ginkgo.By(fmt.Sprintf("Creating a pod to test %v", scenarioName))
	if containerIndex < 0 || containerIndex >= len(pod.Spec.Containers) {
		framework.Failf("Invalid container index: %d", containerIndex)
	}
	framework.ExpectNoError(MatchContainerOutput(ctx, f, pod, pod.Spec.Containers[containerIndex].Name, expectedOutput, matcher))
}

func TestContainerOutputsMatcher(ctx context.Context, f *framework.Framework,
	scenarioName string,
	pod *v1.Pod,
	expectedOutputs map[int][]string,
	matcher func(string, ...interface{}) gomegatypes.GomegaMatcher) {

	ginkgo.By(fmt.Sprintf("Creating a pod to test %v", scenarioName))

	expectedNameOutputs := make(map[string][]string, len(expectedOutputs))
	for containerIndex, expectedOutput := range expectedOutputs {
		if containerIndex < 0 || containerIndex >= len(pod.Spec.Containers) {
			framework.Failf("Invalid container index: %d", containerIndex)
		}
		expectedNameOutputs[pod.Spec.Containers[containerIndex].Name] = expectedOutput
	}
	framework.ExpectNoError(MatchMultipleContainerOutputs(ctx, f, pod, expectedNameOutputs, matcher))

}
