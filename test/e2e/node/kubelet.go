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

package node

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/pkg/features"
	admissionapi "k8s.io/pod-security-admission/api"

	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2erc "k8s.io/kubernetes/test/e2e/framework/rc"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	"k8s.io/kubernetes/test/e2e/storage/utils"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"

	"github.com/onsi/ginkgo/v2"
)

const (
	// 对节点上 /runningpods 执行 framework.Poll 的间隔
	pollInterval = 1 * time.Second
	// 对节点上 /stats/container 执行 framework.Poll 的间隔
	containerStatsPollingInterval = 5 * time.Second
	// 我们限制的最大节点数
	maxNodesToCheck = 10
)

// getPodMatches 返回给定节点上匹配
// podNamePrefix 和 namespace 的一组 pod 名称。
func getPodMatches(ctx context.Context, c clientset.Interface, nodeName string, podNamePrefix string, namespace string) sets.String {
	matches := sets.NewString()
	framework.Logf("Checking pods on node %v via /runningpods endpoint", nodeName)
	runningPods, err := e2ekubelet.GetKubeletRunningPods(ctx, c, nodeName)
	if err != nil {
		framework.Logf("Error checking running pods on %v: %v", nodeName, err)
		return matches
	}
	for _, pod := range runningPods.Items {
		if pod.Namespace == namespace && strings.HasPrefix(pod.Name, podNamePrefix) {
			matches.Insert(pod.Name)
		}
	}
	return matches
}

// waitTillNPodsRunningOnNodes 轮询 kubelet 的 /runningpods 端点，直到
// 找到匹配给定条件（namespace 和
// podNamePrefix）的 targetNumPods 个 pod。注意我们通常用 label selector
// 过滤属于同一 RC 的 pod。这里改用 podNamePrefix 加 namespace，
// 因为 /runningpods 返回的 pod 不含原始 label
// 信息，而是通过检查容器运行时重建出来的。在本
// 测试范围内，我们不预期 pod 命名冲突，因此
// podNamePrefix 足以识别这些 pod。
func waitTillNPodsRunningOnNodes(ctx context.Context, c clientset.Interface, nodeNames sets.String, podNamePrefix string, namespace string, targetNumPods int, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, pollInterval, timeout, false, func(ctx context.Context) (bool, error) {
		matchCh := make(chan sets.String, len(nodeNames))
		for _, nodeName := range nodeNames.List() {
			// 每个节点启动一个 goroutine 检查节点上运行的 pod。
			go func() {
				matchCh <- getPodMatches(ctx, c, nodeName, podNamePrefix, namespace)
			}()
		}

		seen := sets.NewString()
		for i := 0; i < len(nodeNames.List()); i++ {
			seen = seen.Union(<-matchCh)
		}
		if seen.Len() == targetNumPods {
			return true, nil
		}
		framework.Logf("Waiting for %d pods to be running on the node; %d are currently running;", targetNumPods, seen.Len())
		return false, nil
	})
}

// 创建一个挂载 nfs-server pod 提供的 nfs volume 的 pod。容器
// 将执行传入的 shell 命令。等待 pod 启动。
// 注意：nfs 插件是内联定义的，没有 PV 或 PVC。
func createPodUsingNfs(ctx context.Context, f *framework.Framework, c clientset.Interface, ns, nfsIP, cmd string) *v1.Pod {
	ginkgo.By("create pod using nfs volume")

	isPrivileged := true
	cmdLine := []string{"-c", cmd}
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pod-nfs-vol-",
			Namespace:    ns,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "pod-nfs-vol",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sh"},
					Args:    cmdLine,
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "nfs-vol",
							MountPath: "/mnt",
						},
					},
					SecurityContext: &v1.SecurityContext{
						Privileged: &isPrivileged,
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever, // don't restart pod
			Volumes: []v1.Volume{
				{
					Name: "nfs-vol",
					VolumeSource: v1.VolumeSource{
						NFS: &v1.NFSVolumeSource{
							Server:   nfsIP,
							Path:     "/exports",
							ReadOnly: false,
						},
					},
				},
			},
		},
	}
	rtnPod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err)

	err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, rtnPod.Name, f.Namespace.Name, framework.PodStartTimeout) // running & ready
	framework.ExpectNoError(err)

	rtnPod, err = c.CoreV1().Pods(ns).Get(ctx, rtnPod.Name, metav1.GetOptions{}) // return fresh pod
	framework.ExpectNoError(err)
	return rtnPod
}

// getHostExternalAddress 获取 pod 所在的 node 并返回第一个 External
// 地址。如果 pod 所在的 node 没有 External
// 地址，则返回错误。
func getHostExternalAddress(ctx context.Context, client clientset.Interface, p *v1.Pod) (externalAddress string, err error) {
	node, err := client.CoreV1().Nodes().Get(ctx, p.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeExternalIP {
			if address.Address != "" {
				externalAddress = address.Address
				break
			}
		}
	}
	if externalAddress == "" {
		e2eskipper.Skipf("No NodeExternalIP for pod %v on node %v, test requires SSH-reachable worker nodes",
			p.Name, p.Spec.NodeName)
	}
	return
}

// 检查 pod 宿主机上残留的 nfs 挂载和/或 uid 目录。使用宿主机 IP
// 以便本测试在 GCE 上运行，那里 SSH 似乎无法解析主机名。
// 如果 expectClean 为 true，预期节点已被清理，因此像
// `ls <uid-dir>` 这样的命令应失败（目录已删除）。expectClean 为 false 时预期
// 节点未被清理，`ls <uid-dir>` 应成功。我们等待
// kubelet 完成清理，之后报告错误。
func checkPodCleanup(ctx context.Context, c clientset.Interface, pod *v1.Pod, expectClean bool) {
	timeout := 5 * time.Minute
	poll := 20 * time.Second
	podDir := filepath.Join("/var/lib/kubelet/pods", string(pod.UID))
	mountDir := filepath.Join(podDir, "volumes", "kubernetes.io~nfs")
	// 在 GCE 中用 IP 而非 hostname
	nodeIP, err := getHostExternalAddress(ctx, c, pod)
	framework.ExpectNoError(err)

	condMsg := "deleted"
	if !expectClean {
		condMsg = "present"
	}

	// 要执行的宿主机测试表格（顺序可能重要，所以不用 map）
	type testT struct {
		feature string // feature to test
		cmd     string // remote command to execute on node
	}
	tests := []testT{
		{
			feature: "pod UID directory",
			cmd:     fmt.Sprintf("sudo ls %v", podDir),
		},
		{
			feature: "pod nfs mount",
			cmd:     fmt.Sprintf("sudo mount | grep %v", mountDir),
		},
	}

	for _, test := range tests {
		framework.Logf("Wait up to %v for host's (%v) %q to be %v", timeout, nodeIP, test.feature, condMsg)
		err = wait.PollUntilContextTimeout(ctx, poll, timeout, false, func(ctx context.Context) (bool, error) {
			result, err := e2essh.NodeExec(ctx, nodeIP, test.cmd, framework.TestContext.Provider)
			framework.ExpectNoError(err)
			e2essh.LogResult(result)
			ok := result.Code == 0 && len(result.Stdout) > 0 && len(result.Stderr) == 0
			if expectClean && ok { // keep trying
				return false, nil
			}
			if !expectClean && !ok { // stop wait loop
				return true, fmt.Errorf("%v is gone but expected to exist", test.feature)
			}
			return true, nil // done, host is as expected
		})
		framework.ExpectNoError(err, fmt.Sprintf("Host (%v) cleanup error: %v. Expected %q to be %v", nodeIP, err, test.feature, condMsg))
	}

	if expectClean {
		framework.Logf("Pod's host has been cleaned up")
	} else {
		framework.Logf("Pod's host has not been cleaned up (per expectation)")
	}
}

var _ = SIGDescribe("kubelet", func() {
	var (
		c  clientset.Interface
		ns string
	)
	f := framework.NewDefaultFramework("kubelet")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		c = f.ClientSet
		ns = f.Namespace.Name
	})

	ginkgo.Describe("Clean up pods on node", func() {
		var (
			numNodes        int
			nodeNames       sets.String
			nodeLabels      map[string]string
			resourceMonitor *e2ekubelet.ResourceMonitor
		)
		type DeleteTest struct {
			podsPerNode int
			timeout     time.Duration
		}

		deleteTests := []DeleteTest{
			{podsPerNode: 10, timeout: 1 * time.Minute},
		}

		// 必须在每个 It 中用测试 context 调用。
		start := func(ctx context.Context) {
			// 用 node label 把 pod 限制只分配到
			// 我们最初观察到的那些节点上。
			nodeLabels = make(map[string]string)
			nodeLabels["kubelet_cleanup"] = "true"
			nodes, err := e2enode.GetBoundedReadySchedulableNodes(ctx, c, maxNodesToCheck)
			numNodes = len(nodes.Items)
			framework.ExpectNoError(err)
			nodeNames = sets.NewString()
			for i := 0; i < len(nodes.Items); i++ {
				nodeNames.Insert(nodes.Items[i].Name)
			}
			for nodeName := range nodeNames {
				for k, v := range nodeLabels {
					e2enode.AddOrUpdateLabelOnNode(c, nodeName, k, v)
					ginkgo.DeferCleanup(e2enode.RemoveLabelOffNode, c, nodeName, k)
				}
			}

			// 虽然测试只使用有限数量的节点，但我们需要知道
			// 集群中的实际节点数，以免在大规模集群上运行 resourceMonitor。
			// 只在小集群中启动 resourceMonitor。
			actualNodes, err := e2enode.GetReadySchedulableNodes(ctx, c)
			framework.ExpectNoError(err)

			// Start resourceMonitor only in small clusters.
			if len(actualNodes.Items) <= maxNodesToCheck {
				resourceMonitor = e2ekubelet.NewResourceMonitor(f.ClientSet, e2ekubelet.TargetContainers(), containerStatsPollingInterval)
				resourceMonitor.Start(ctx)
				ginkgo.DeferCleanup(resourceMonitor.Stop)
			}
		}

		for _, itArg := range deleteTests {
			name := fmt.Sprintf(
				"kubelet should be able to delete %d pods per node in %v.", itArg.podsPerNode, itArg.timeout)
			ginkgo.It(name, func(ctx context.Context) {
				start(ctx)
				totalPods := itArg.podsPerNode * numNodes
				ginkgo.By(fmt.Sprintf("Creating a RC of %d pods and wait until all pods of this RC are running", totalPods))
				rcName := fmt.Sprintf("cleanup%d-%s", totalPods, string(uuid.NewUUID()))

				err := e2erc.RunRC(ctx, testutils.RCConfig{
					Client:       f.ClientSet,
					Name:         rcName,
					Namespace:    f.Namespace.Name,
					Image:        imageutils.GetPauseImageName(),
					Replicas:     totalPods,
					NodeSelector: nodeLabels,
				})
				framework.ExpectNoError(err)
				// 做一次健全性检查，确认 kubelet 认为所有期望的 pod 都在
				// 节点上运行。这里超时只设 30 秒，因为 e2erc.RunRC
				// 已经等待所有 pod 转为 running 状态。
				//
				err = waitTillNPodsRunningOnNodes(ctx, f.ClientSet, nodeNames, rcName, ns, totalPods, time.Second*30)
				framework.ExpectNoError(err)
				if resourceMonitor != nil {
					resourceMonitor.LogLatest()
				}

				ginkgo.By("Deleting the RC")
				e2erc.DeleteRCAndWaitForGC(ctx, f.ClientSet, f.Namespace.Name, rcName)
				// 通过查询节点上的 /runningpods 检查 pod 确实已消失。
				// /runningpods handler 检查容器运行时（或其
				// cache）并返回运行中 pod 的列表。失败的可能原因包括：
				//
				// - kubelet 死锁
				// - 优雅终止的 bug（如果启用了）
				// - docker 删除 pod 慢（或资源问题导致变慢）
				start := time.Now()
				err = waitTillNPodsRunningOnNodes(ctx, f.ClientSet, nodeNames, rcName, ns, 0, itArg.timeout)
				framework.ExpectNoError(err)
				framework.Logf("Deleting %d pods on %d nodes completed in %v after the RC was deleted", totalPods, len(nodeNames),
					time.Since(start))
				if resourceMonitor != nil {
					resourceMonitor.LogCPUSummary()
				}
			})
		}
	})

	// 测试 volume 环境被破坏时的宿主机清理。
	f.Describe("host cleanup with volume mounts [HostCleanup]", func() {

		type hostCleanupTest struct {
			itDescr string
			podCmd  string
		}

		// 客户端 pod 访问 nfs volume 后，破坏 nfs-server pod。
		// 注意：nfs-server 是停止而非删除，目的是保留其 IP 地址。
		// 如果删除 nfs-server pod，客户端 pod 的挂载无法卸载。
		// 如果删除再重建 nfs-server pod，因为 IP 地址变了，
		// 客户端 pod 的挂载仍无法卸载。
		f.Context("Host cleanup after disrupting NFS volume [NFS]", f.WithProvider(framework.ProvidersWithSSH...), func() {
			// issue #31272
			var (
				nfsServerPod *v1.Pod
				nfsIP        string
				pod          *v1.Pod // client pod
			)

			// 填充这个 context 的测试切片
			testTbl := []hostCleanupTest{
				{
					itDescr: "after stopping the nfs-server and deleting the (sleeping) client pod, the NFS mount and the pod's UID directory should be removed.",
					podCmd:  "sleep 6000", // keep pod running
				},
				{
					itDescr: "after stopping the nfs-server and deleting the (active) client pod, the NFS mount and the pod's UID directory should be removed.",
					podCmd:  "while true; do echo FeFieFoFum >>/mnt/SUCCESS; sleep 1; cat /mnt/SUCCESS; done",
				},
			}

			ginkgo.BeforeEach(func(ctx context.Context) {
				_, nfsServerPod, nfsIP = e2evolume.NewNFSServer(ctx, c, ns, []string{"-G", "777", "/exports"})
			})

			ginkgo.AfterEach(func(ctx context.Context) {
				e2epod.DeletePodsWithWait(ctx, c, []*v1.Pod{pod, nfsServerPod})
			})

			// 执行上表中的 It 块
			for _, t := range testTbl {
				ginkgo.It(t.itDescr, func(ctx context.Context) {
					pod = createPodUsingNfs(ctx, f, c, ns, nfsIP, t.podCmd)

					ginkgo.By("Stop the NFS server")
					e2evolume.StopNFSServer(ctx, f, nfsServerPod)

					ginkgo.By("Delete the pod mounted to the NFS volume -- expect failure")
					err := e2epod.DeletePodWithWait(ctx, c, pod)
					gomega.Expect(err).To(gomega.HaveOccurred())
					// pod 对象现在已过期，但故意不设为 nil

					ginkgo.By("Check if pod's host has been cleaned up -- expect not")
					checkPodCleanup(ctx, c, pod, false)

					ginkgo.By("Restart the nfs server")
					e2evolume.RestartNFSServer(ctx, f, nfsServerPod)

					ginkgo.By("Verify that the deleted client pod is now cleaned up")
					checkPodCleanup(ctx, c, pod, true)
				})
			}
		})
	})

	// NodeLogQuery 特性的测试
	f.Describe("kubectl get --raw \"/api/v1/nodes/<insert-node-name-here>/proxy/logs/?query=/<insert-log-file-name-here>", feature.NodeLogQuery, func() {
		var linuxNodeName string
		var windowsNodeName string

		ginkgo.BeforeEach(func(ctx context.Context) {
			allNodes, err := e2enode.GetReadyNodesIncludingTainted(ctx, c)
			framework.ExpectNoError(err)
			if len(allNodes.Items) == 0 {
				framework.Fail("Expected at least one node to be present")
			}
			// 复制一份 node 列表，因为 getLinuxNodes 会过滤掉 Windows 节点
			nodes := allNodes.DeepCopy()

			linuxNodes := getLinuxNodes(nodes)
			if len(linuxNodes.Items) == 0 {
				framework.Fail("Expected at least one Linux node to be present")
			}
			linuxNodeName = linuxNodes.Items[0].Name

			windowsNodes := getWindowsNodes(allNodes)
			if len(windowsNodes.Items) == 0 {
				framework.Logf("No Windows node found")
			} else {
				windowsNodeName = windowsNodes.Items[0].Name
			}

		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-node-name-here>/proxy/logs/?query"
			是否返回错误！
		*/

		ginkgo.It("should return the error with an empty --query option", func() {
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query", linuxNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			_, _, err := framework.StartCmdAndStreamOutput(cmd)
			if err != nil {
				framework.Failf("Failed to start kubectl command! Error: %v", err)
			}
			err = cmd.Wait()
			gomega.Expect(err).To(gomega.HaveOccurred(), "Command kubectl get --raw "+queryCommand+" was expected to return an error!")
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-linux-node-name-here>/proxy/logs/?query=kubelet"
			是否返回 kubelet 日志
		*/

		ginkgo.It("should return the kubelet logs", func(ctx context.Context) {
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=kubelet", linuxNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			assertContains("kubelet", result)
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-linux-node-name-here>/proxy/logs/?query=kubelet&boot=0"
			是否返回本次启动的 kubelet 日志
		*/

		ginkgo.It("should return the kubelet logs for the current boot", func(ctx context.Context) {
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=kubelet&boot=0", linuxNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			assertContains("kubelet", result)
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-linux-node-name-here>/proxy/logs/?query=kubelet&tailLines=3"
			是否返回 kubelet 日志的最后三行
		*/

		f.It("should return the last three lines of the kubelet logs", f.WithProvider(framework.ProvidersWithSSH...), func(ctx context.Context) {
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=kubelet&tailLines=3", linuxNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			logs := journalctlCommandOnNode(linuxNodeName, "-u kubelet -n 3")
			if result != logs {
				framework.Failf("Failed to receive the correct kubelet logs or the correct amount of lines of logs")
			}
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-linux-node-name-here>/proxy/logs/?query=kubelet&pattern=container"
			是否返回本次启动中包含 container 模式的 kubelet 日志
		*/

		f.It("should return the kubelet logs for the current boot with the pattern container", f.WithProvider(framework.ProvidersWithSSH...), func(ctx context.Context) {
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=kubelet&boot=0&pattern=container", linuxNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			logs := journalctlCommandOnNode(linuxNodeName, "-u kubelet --grep container --boot 0")
			if result != logs {
				framework.Failf("Failed to receive the correct kubelet logs")
			}
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-linux-node-name-here>/proxy/logs/?query=kubelet&sinceTime=<now>"
			是否返回当前日期时间以来的 kubelet 日志。返回 "-- No entries --" 也是正确的。
		*/

		ginkgo.It("should return the kubelet logs since the current date and time", func() {
			ginkgo.By("Starting the command")
			start := time.Now().UTC()
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			currentTime := start.Format(time.RFC3339)
			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=kubelet&sinceTime=%s", linuxNodeName, currentTime)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			journalctlDateLayout := "2006-1-2 15:4:5"
			result := runKubectlCommand(cmd)
			logs := journalctlCommandOnNode(linuxNodeName, fmt.Sprintf("-u kubelet --since \"%s\"", start.Format(journalctlDateLayout)))
			if result != logs {
				framework.Failf("Failed to receive the correct kubelet logs or the correct amount of lines of logs")
			}
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-windows-node-name-here>/proxy/logs/?query="Microsoft-Windows-Security-SPP"
			是否返回 Microsoft-Windows-Security-SPP 日志
		*/

		ginkgo.It("should return the Microsoft-Windows-Security-SPP logs", func(ctx context.Context) {
			if len(windowsNodeName) == 0 {
				ginkgo.Skip("No Windows node found")
			}
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=Microsoft-Windows-Security-SPP", windowsNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			assertContains("ProviderName: Microsoft-Windows-Security-SPP", result)
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-windows-node-name-here>/proxy/logs/?query=Microsoft-Windows-Security-SPP&tailLines=3"
			是否返回 Microsoft-Windows-Security-SPP 日志的最后三行
		*/

		f.It("should return the last three lines of the Microsoft-Windows-Security-SPP logs", f.WithProvider(framework.ProvidersWithSSH...), func(ctx context.Context) {
			if len(windowsNodeName) == 0 {
				ginkgo.Skip("No Windows node found")
			}
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=Microsoft-Windows-Security-SPP&tailLines=3", windowsNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			logs := getWinEventCommandOnNode(windowsNodeName, "Microsoft-Windows-Security-SPP", " -MaxEvents 3")
			if trimSpaceNewlineInString(result) != trimSpaceNewlineInString(logs) {
				framework.Failf("Failed to receive the correct Microsoft-Windows-Security-SPP logs or the correct amount of lines of logs")
			}
		})

		/*
			测试 kubectl get --raw "/api/v1/nodes/<insert-windows-node-name-here>/proxy/logs/?query=Microsoft-Windows-Security-SPP&pattern=Health"
			是否返回 Microsoft-Windows-Security-SPP 日志中包含 Health 模式的行
		*/

		f.It("should return the Microsoft-Windows-Security-SPP logs with the pattern Health", f.WithProvider(framework.ProvidersWithSSH...), func(ctx context.Context) {
			if len(windowsNodeName) == 0 {
				ginkgo.Skip("No Windows node found")
			}
			ginkgo.By("Starting the command")
			tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

			queryCommand := fmt.Sprintf("/api/v1/nodes/%s/proxy/logs/?query=Microsoft-Windows-Security-SPP&pattern=Health", windowsNodeName)
			cmd := tk.KubectlCmd("get", "--raw", queryCommand)
			result := runKubectlCommand(cmd)
			logs := getWinEventCommandOnNode(windowsNodeName, "Microsoft-Windows-Security-SPP", "  | Where-Object -Property Message -Match Health")
			if trimSpaceNewlineInString(result) != trimSpaceNewlineInString(logs) {
				framework.Failf("Failed to receive the correct Microsoft-Windows-Security-SPP logs or the correct amount of lines of logs")
			}
		})
	})
})

var _ = SIGDescribe("specific log stream", feature.PodLogsQuerySplitStreams, framework.WithFeatureGate(features.PodLogsQuerySplitStreams), func() {
	var (
		c  clientset.Interface
		ns string
	)
	f := framework.NewDefaultFramework("pod-log-stream")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func() {
		c = f.ClientSet
		ns = f.Namespace.Name
	})

	ginkgo.It("kubectl get --raw /api/v1/namespaces/default/pods/<pod-name>/log?stream", func(ctx context.Context) {
		ginkgo.By("create pod")

		pod := &v1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "log-stream-",
				Namespace:    ns,
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:    "log-stream",
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sh"},
						Args:    []string{"-c", "echo out1; echo err1 >&2; tail -f /dev/null"},
					},
				},
				RestartPolicy: v1.RestartPolicyNever, // don't restart pod
			},
		}
		rtnPod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
		framework.ExpectNoError(err)

		err = e2epod.WaitTimeoutForPodReadyInNamespace(ctx, f.ClientSet, rtnPod.Name, f.Namespace.Name, framework.PodStartTimeout) // running & ready
		framework.ExpectNoError(err)

		rtnPod, err = c.CoreV1().Pods(ns).Get(ctx, rtnPod.Name, metav1.GetOptions{}) // return fresh pod
		framework.ExpectNoError(err)

		ginkgo.By("Starting the command")
		tk := e2ekubectl.NewTestKubeconfig(framework.TestContext.CertDir, framework.TestContext.Host, framework.TestContext.KubeConfig, framework.TestContext.KubeContext, framework.TestContext.KubectlPath, ns)

		queryCommand := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?stream=All", rtnPod.Namespace, rtnPod.Name)
		cmd := tk.KubectlCmd("get", "--raw", queryCommand)
		result := runKubectlCommand(cmd)
		// log 的顺序不确定
		assertContains("out1", result)
		assertContains("err1", result)

		queryCommand = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?stream=Stdout", rtnPod.Namespace, rtnPod.Name)
		cmd = tk.KubectlCmd("get", "--raw", queryCommand)
		result = runKubectlCommand(cmd)
		assertContains("out1", result)
		assertNotContains("err1", result)

		queryCommand = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log?stream=Stderr", rtnPod.Namespace, rtnPod.Name)
		cmd = tk.KubectlCmd("get", "--raw", queryCommand)
		result = runKubectlCommand(cmd)
		assertContains("err1", result)
		assertNotContains("out1", result)
	})
})

func getLinuxNodes(nodes *v1.NodeList) *v1.NodeList {
	filteredNodes := nodes
	e2enode.Filter(filteredNodes, func(node v1.Node) bool {
		return isNode(&node, "linux")
	})
	return filteredNodes
}

func getWindowsNodes(nodes *v1.NodeList) *v1.NodeList {
	filteredNodes := nodes
	e2enode.Filter(filteredNodes, func(node v1.Node) bool {
		return isNode(&node, "windows")
	})
	return filteredNodes
}

func isNode(node *v1.Node, os string) bool {
	if node == nil {
		return false
	}
	if foundOS, found := node.Labels[v1.LabelOSStable]; found {
		return os == foundOS
	}
	return false
}

func runKubectlCommand(cmd *exec.Cmd) (result string) {
	stdout, stderr, err := framework.StartCmdAndStreamOutput(cmd)
	var buf bytes.Buffer
	if err != nil {
		framework.Failf("Failed to start kubectl command! Stderr: %v, error: %v", stderr, err)
	}
	defer stdout.Close()
	defer stderr.Close()
	defer framework.TryKill(cmd)

	b_read, err := io.Copy(&buf, stdout)
	if err != nil {
		framework.Failf("Expected output from kubectl alpha node-logs %s: %v\n Stderr: %v", cmd.Args, err, stderr)
	}
	out := ""
	if b_read >= 0 {
		out = buf.String()
	}

	framework.Logf("Kubectl output: %s", out)
	return out
}

func assertContains(expectedString string, result string) {
	if strings.Contains(result, expectedString) {
		return
	}
	framework.Failf("Failed to find \"%s\"", expectedString)
}

func assertNotContains(expectedString string, result string) {
	if !strings.Contains(result, expectedString) {
		return
	}
	framework.Failf("Found unexpected \"%s\"", expectedString)
}

func commandOnNode(nodeName string, cmd string) string {
	result, err := e2essh.NodeExec(context.Background(), nodeName, cmd, framework.TestContext.Provider)
	framework.ExpectNoError(err)
	e2essh.LogResult(result)
	return result.Stdout
}

func journalctlCommandOnNode(nodeName string, args string) string {
	return commandOnNode(nodeName, "journalctl --utc --no-pager --output=short-precise "+args)
}

func getWinEventCommandOnNode(nodeName string, providerName, args string) string {
	output := commandOnNode(nodeName, "Get-WinEvent -FilterHashtable @{LogName='Application'; ProviderName='"+providerName+"'}"+args+" | Sort-Object TimeCreated | Format-Table -AutoSize -Wrap")
	return output
}

func trimSpaceNewlineInString(s string) string {
	// 去掉 Windows 换行符
	re := regexp.MustCompile(` +\r?\n +`)
	s = re.ReplaceAllString(s, "")
	// 把空格替换掉，以处理 "\r\n " 之类可能导致假阴性的情况
	return strings.ReplaceAll(s, " ", "")
}

func pidOfKubelet(ctx context.Context, hostExec utils.HostExec, node *v1.Node) int {
	cmd := "pidof kubelet"
	kubeletPid, err := hostExec.IssueCommandWithResult(ctx, cmd, node)
	framework.ExpectNoError(err, "Checking kubelet pid")
	framework.Logf("pidof kubelet returns %s", kubeletPid)
	// 在 CI 中偶尔看到 kubelet 有两个 pid。
	// 调查后确认列表最后一项才是我们要的
	// 较大的 pid 更可能是在第一个之后创建的
	kubeletPids := strings.Split(strings.TrimSuffix(kubeletPid, "\n"), " ")
	kubeletPidChosen := kubeletPids[len(kubeletPids)-1]
	pid, err := strconv.Atoi(kubeletPidChosen)
	framework.ExpectNoError(err, "Converting kubelet pid to int")
	return pid
}
