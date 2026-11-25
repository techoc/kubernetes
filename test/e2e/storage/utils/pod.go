/*
Copyright 2020 The Kubernetes Authors.

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
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	"k8s.io/kubernetes/test/e2e/storage/podlogs"
)

// StartPodLogs 开始捕获 framework 的 namespace 中当前及后续 pod 的日志输出和事件，
// 直到调用返回的清理函数为止。
//
// 输出会写入日志文件（使用 --report-dir 时，如 CI 中），
// 否则写入输出流。
func StartPodLogs(ctx context.Context, f *framework.Framework, driverNamespace *v1.Namespace) func() {
	ctx, cancel := context.WithCancel(ctx)
	cs := f.ClientSet

	ns := driverNamespace.Name

	var podEventLog io.Writer = ginkgo.GinkgoWriter
	var podEventLogCloser io.Closer
	to := podlogs.LogOutput{
		StatusWriter: ginkgo.GinkgoWriter,
	}
	if framework.TestContext.ReportDir == "" {
		to.LogWriter = ginkgo.GinkgoWriter
	} else {
		test := ginkgo.CurrentSpecReport()
		// 清理每一段文本，使其只包含文件名中可用的字符。
		reg := regexp.MustCompile("[^a-zA-Z0-9_-]+")
		var testName []string
		for _, text := range test.ContainerHierarchyTexts {
			testName = append(testName, reg.ReplaceAllString(text, "_"))
			if len(test.LeafNodeText) > 0 {
				testName = append(testName, reg.ReplaceAllString(test.LeafNodeText, "_"))
			}
		}

		// 确保每个目录名在 Linux 和 Windows 上都不会过长
		for i, testNameComponent := range testName {
			testName[i] = ShortenFileName(testNameComponent)
		}

		// 前缀以斜杠结尾，确保所有日志都落在以当前测试命名的目录中。
		//
		// 每段名称对应一个目录层级，避免根目录的 artifact 过于杂乱，
		// 同时让目录名更短（完整的测试名曾超过 256 个字符，
		// 对某些文件系统来说太长了）。
		logDir := filepath.Join(framework.TestContext.ReportDir, filepath.Join(testName...))
		to.LogPathPrefix = logDir + "/"

		err := os.MkdirAll(logDir, 0755)
		framework.ExpectNoError(err, "create pod log directory")
		f, err := os.Create(path.Join(logDir, "pod-event.log"))
		framework.ExpectNoError(err, "create pod events log file")
		podEventLog = f
		podEventLogCloser = f
	}
	podlogs.CopyAllLogs(ctx, cs, ns, to)

	// driver pod 运行在独立的 namespace 中，framework 感知不到，
	// 因此需要自行捕获其事件。
	podlogs.WatchPods(ctx, cs, ns, podEventLog, podEventLogCloser)

	return cancel
}

// KubeletCommand 在目标 pod 所在 node 的 kubelet 上执行 `start`、`restart` 或 `stop`，
// 并等待达到期望状态。
// 允许的 kubeletOps 为 `KStart`、`KStop` 和 `KRestart`。
func KubeletCommand(ctx context.Context, kOp KubeletOpt, c clientset.Interface, pod *v1.Pod) {
	nodeIP, err := getHostAddress(ctx, c, pod)
	framework.ExpectNoError(err)
	nodeIP = nodeIP + ":22"

	commandTemplate := "systemctl %s kubelet"
	sudoPresent := isSudoPresent(ctx, nodeIP, framework.TestContext.Provider)
	if sudoPresent {
		commandTemplate = "sudo " + commandTemplate
	}

	runCmd := func(cmd string) {
		command := fmt.Sprintf(commandTemplate, cmd)
		framework.Logf("Attempting `%s`", command)
		sshResult, err := e2essh.SSH(ctx, command, nodeIP, framework.TestContext.Provider)
		framework.ExpectNoError(err, fmt.Sprintf("SSH to Node %q errored.", pod.Spec.NodeName))
		e2essh.LogResult(sshResult)
		gomega.Expect(sshResult.Code).To(gomega.BeZero(), "Failed to [%s] kubelet:\n%#v", cmd, sshResult)
	}

	if kOp == KStop || kOp == KRestart {
		runCmd("stop")
	}
	if kOp == KStop {
		return
	}

	if kOp == KStart && getKubeletRunning(ctx, nodeIP) {
		framework.Logf("Kubelet is already running on node %q", pod.Spec.NodeName)
		// 直接跳过，否则可能无法及时获得新的心跳。
		return
	}

	node, err := c.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	heartbeatTime := e2enode.GetNodeHeartbeatTime(node)

	runCmd("start")
	// 等待下一次心跳，它必须由新的 kubelet 进程发出。
	e2enode.WaitForNodeHeartbeatAfter(ctx, c, pod.Spec.NodeName, heartbeatTime, NodeStateTimeout)
	// 然后等待运行新进程的 Node 变为 Ready。
	if ok := e2enode.WaitForNodeToBeReady(ctx, c, pod.Spec.NodeName, NodeStateTimeout); !ok {
		framework.Failf("Node %s failed to enter Ready state", pod.Spec.NodeName)
	}
}

// getHostAddress 获取 pod 所在的 node 并返回其第一个地址。
// 如果 pod 所在的 node 没有地址，则返回错误。
func getHostAddress(ctx context.Context, client clientset.Interface, p *v1.Pod) (string, error) {
	node, err := client.CoreV1().Nodes().Get(ctx, p.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	// 优先尝试 externalAddress
	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeExternalIP {
			if address.Address != "" {
				return address.Address, nil
			}
		}
	}
	// 如果找不到 externalAddress，则尝试 internalAddress
	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeInternalIP {
			if address.Address != "" {
				return address.Address, nil
			}
		}
	}

	// 都找不到则返回错误
	return "", fmt.Errorf("No address for pod %v on node %v",
		p.Name, p.Spec.NodeName)
}
