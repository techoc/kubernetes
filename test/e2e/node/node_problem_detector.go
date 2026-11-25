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

package node

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	testutils "k8s.io/kubernetes/test/utils"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// 此测试检查 node-problem-detector（NPD）在集群中最多 10 个节点上
// 是否正常运行无错误。NPD 的功能在 e2e_node 测试中验证。
var _ = SIGDescribe("NodeProblemDetector", feature.NodeProblemDetector, framework.WithProvider(framework.ProvidersWithSSH...), framework.WithProvider("gce"), func() {
	const (
		pollInterval      = 1 * time.Second
		pollTimeout       = 1 * time.Minute
		maxNodesToProcess = 10
	)
	f := framework.NewDefaultFramework("node-problem-detector")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		e2eskipper.SkipUnlessSSHKeyPresent()
		e2eskipper.SkipUnlessNodeOSDistroIs("gci", "ubuntu")
		e2enode.WaitForTotalHealthy(ctx, f.ClientSet, time.Minute)
	})

	ginkgo.It("should run without error", func(ctx context.Context) {
		ginkgo.By("Getting all nodes and their SSH-able IP addresses")
		readyNodes, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
		framework.ExpectNoError(err)

		nodes := []v1.Node{}
		hosts := []string{}
		for _, node := range readyNodes.Items {
			host := ""
			for _, addr := range node.Status.Addresses {
				if addr.Type == v1.NodeExternalIP {
					host = net.JoinHostPort(addr.Address, "22")
					break
				}
			}
			// 并非每个节点都必须有外部 IP 地址。
			if len(host) > 0 {
				nodes = append(nodes, node)
				hosts = append(hosts, host)
			}
		}

		if len(nodes) == 0 {
			ginkgo.Skip("Skipping test due to lack of ready nodes with public IP")
		}

		if len(nodes) > maxNodesToProcess {
			nodes = nodes[:maxNodesToProcess]
			hosts = hosts[:maxNodesToProcess]
		}

		isStandaloneMode := make(map[string]bool)
		cpuUsageStats := make(map[string][]float64)
		uptimeStats := make(map[string][]float64)
		rssStats := make(map[string][]float64)
		workingSetStats := make(map[string][]float64)

		// 某些测试套件会运行数天。
		// 此测试未标记为 Disruptive 或 Serial，因此不想
		// 在测试期间重启 kubelet 来检查 KubeletStart 事件
		// 检测。这里使用启发式方法判断是否需要验证
		// KubeletStart 事件，因为没有简便方法检查测试何时真正开始。
		checkForKubeletStart := false

		for _, host := range hosts {
			cpuUsageStats[host] = []float64{}
			uptimeStats[host] = []float64{}
			rssStats[host] = []float64{}
			workingSetStats[host] = []float64{}

			cmd := "systemctl status node-problem-detector.service"
			result, err := e2essh.SSH(ctx, cmd, host, framework.TestContext.Provider)
			isStandaloneMode[host] = (err == nil && result.Code == 0)

			if isStandaloneMode[host] {
				ginkgo.By(fmt.Sprintf("Check node %q has node-problem-detector process", host))
				// 使用方括号 "[n]" 是一种技巧，防止 grep 命令自身
				// 出现在结果中，因为字符串 "[n]ode-problem-detector" 不会
				// 匹配正则 "[n]ode-problem-detector"。
				psCmd := "ps aux | grep [n]ode-problem-detector"
				result, err = e2essh.SSH(ctx, psCmd, host, framework.TestContext.Provider)
				framework.ExpectNoError(err)
				gomega.Expect(result.Code).To(gomega.Equal(0))
				gomega.Expect(result.Stdout).To(gomega.ContainSubstring("node-problem-detector"))

				ginkgo.By(fmt.Sprintf("Check node-problem-detector is running fine on node %q", host))
				journalctlCmd := "sudo journalctl -r -u node-problem-detector"
				result, err = e2essh.SSH(ctx, journalctlCmd, host, framework.TestContext.Provider)
				framework.ExpectNoError(err)
				gomega.Expect(result.Code).To(gomega.Equal(0))
				gomega.Expect(result.Stdout).NotTo(gomega.ContainSubstring("node-problem-detector.service: Failed"))

				// 只有此处日期解析成功时才检查 KubeletStart 事件。
				ginkgo.By(fmt.Sprintf("Check when node-problem-detector started on node %q", host))
				npdStartTimeCommand := "sudo systemctl show --timestamp=utc node-problem-detector -P ActiveEnterTimestamp"
				result, err = e2essh.SSH(ctx, npdStartTimeCommand, host, framework.TestContext.Provider)
				framework.ExpectNoError(err)
				gomega.Expect(result.Code).To(gomega.Equal(0))

				// 时间格式与 systemd 格式一致。
				// 'utc': 'Day YYYY-MM-DD HH:MM:SS UTC (see https://www.freedesktop.org/software/systemd/man/systemd.time.html)
				st, err := time.Parse("Mon 2006-01-02 15:04:05 MST", result.Stdout)
				if err != nil {
					framework.Logf("Failed to parse when NPD started. Got exit code: %v and stdout: %v, error: %v. Will skip check for kubelet start event.", result.Code, result.Stdout, err)
				} else {
					checkForKubeletStart = time.Since(st) < time.Hour
				}

				cpuUsage, uptime := getCPUStat(ctx, f, host)
				cpuUsageStats[host] = append(cpuUsageStats[host], cpuUsage)
				uptimeStats[host] = append(uptimeStats[host], uptime)

			}
			ginkgo.By(fmt.Sprintf("Inject log to trigger DockerHung on node %q", host))
			log := "INFO: task docker:12345 blocked for more than 120 seconds."
			injectLogCmd := "sudo sh -c \"echo 'kernel: " + log + "' >> /dev/kmsg\""
			result, err = e2essh.SSH(ctx, injectLogCmd, host, framework.TestContext.Provider)
			framework.ExpectNoError(err)
			gomega.Expect(result.Code).To(gomega.Equal(0))
		}

		ginkgo.By("Gather node-problem-detector cpu and memory stats")
		numIterations := 60
		for i := 1; i <= numIterations; i++ {
			for j, host := range hosts {
				if isStandaloneMode[host] {
					rss, workingSet := getMemoryStat(ctx, f, host)
					rssStats[host] = append(rssStats[host], rss)
					workingSetStats[host] = append(workingSetStats[host], workingSet)
					if i == numIterations {
						cpuUsage, uptime := getCPUStat(ctx, f, host)
						cpuUsageStats[host] = append(cpuUsageStats[host], cpuUsage)
						uptimeStats[host] = append(uptimeStats[host], uptime)
					}
				} else {
					cpuUsage, rss, workingSet := getNpdPodStat(ctx, f, nodes[j].Name)
					cpuUsageStats[host] = append(cpuUsageStats[host], cpuUsage)
					rssStats[host] = append(rssStats[host], rss)
					workingSetStats[host] = append(workingSetStats[host], workingSet)
				}
			}
			time.Sleep(time.Second)
		}

		cpuStatsMsg := "CPU (core):"
		rssStatsMsg := "RSS (MB):"
		workingSetStatsMsg := "WorkingSet (MB):"
		for i, host := range hosts {
			if isStandaloneMode[host] {
				// 在 standalone 模式下，NPD 作为 systemd service 运行。我们
				// 根据 cgroup cpuacct 值的差值计算其 CPU 使用率。
				cpuUsage := cpuUsageStats[host][1] - cpuUsageStats[host][0]
				totaltime := uptimeStats[host][1] - uptimeStats[host][0]
				cpuStatsMsg += fmt.Sprintf(" %s[%.3f];", nodes[i].Name, cpuUsage/totaltime)
			} else {
				sort.Float64s(cpuUsageStats[host])
				cpuStatsMsg += fmt.Sprintf(" %s[%.3f|%.3f|%.3f];", nodes[i].Name,
					cpuUsageStats[host][0], cpuUsageStats[host][len(cpuUsageStats[host])/2], cpuUsageStats[host][len(cpuUsageStats[host])-1])
			}

			sort.Float64s(rssStats[host])
			rssStatsMsg += fmt.Sprintf(" %s[%.1f|%.1f|%.1f];", nodes[i].Name,
				rssStats[host][0], rssStats[host][len(rssStats[host])/2], rssStats[host][len(rssStats[host])-1])

			sort.Float64s(workingSetStats[host])
			workingSetStatsMsg += fmt.Sprintf(" %s[%.1f|%.1f|%.1f];", nodes[i].Name,
				workingSetStats[host][0], workingSetStats[host][len(workingSetStats[host])/2], workingSetStats[host][len(workingSetStats[host])-1])
		}
		framework.Logf("Node-Problem-Detector CPU and Memory Stats:\n\t%s\n\t%s\n\t%s", cpuStatsMsg, rssStatsMsg, workingSetStatsMsg)

		ginkgo.By("Check node-problem-detector can post conditions and events to API server")
		for _, node := range nodes {
			ginkgo.By(fmt.Sprintf("Check node-problem-detector posted KernelDeadlock condition on node %q", node.Name))
			gomega.Eventually(ctx, func() error {
				return verifyNodeCondition(ctx, f, "KernelDeadlock", v1.ConditionTrue, "DockerHung", node.Name)
			}, pollTimeout, pollInterval).Should(gomega.Succeed())

			ginkgo.By(fmt.Sprintf("Check node-problem-detector posted DockerHung event on node %q", node.Name))
			eventListOptions := metav1.ListOptions{FieldSelector: fields.Set{"involvedObject.kind": "Node"}.AsSelector().String()}
			gomega.Eventually(ctx, func(ctx context.Context) error {
				return verifyEvents(ctx, f, eventListOptions, 1, "DockerHung", node.Name)
			}, pollTimeout, pollInterval).Should(gomega.Succeed())

			if checkForKubeletStart {
				// 从 NPD v0.7.0 起，Node problem detector 会自动报告 kubelet 启动事件。
				// 由于节点启动后 Kubelet 可能重启数次，我们只检查
				// 是否检测到事件，不检查 Kubelet 启动了多少次。
				//
				// 某些测试套件运行数小时，KubeletStart 事件可能已被清理
				ginkgo.By(fmt.Sprintf("Check node-problem-detector posted KubeletStart event on node %q", node.Name))
				gomega.Eventually(ctx, func(ctx context.Context) error {
					return verifyEventExists(ctx, f, eventListOptions, "KubeletStart", node.Name)
				}, pollTimeout, pollInterval).Should(gomega.Succeed())
			} else {
				ginkgo.By("KubeletStart event will NOT be checked")
			}
		}

	})
})

func verifyEvents(ctx context.Context, f *framework.Framework, options metav1.ListOptions, num int, reason, nodeName string) error {
	events, err := f.ClientSet.CoreV1().Events(metav1.NamespaceDefault).List(ctx, options)
	if err != nil {
		return err
	}
	count := 0
	for _, event := range events.Items {
		if event.Reason != reason || event.Source.Host != nodeName {
			continue
		}
		count += int(event.Count)
	}
	if count != num {
		return fmt.Errorf("expect event number %d, got %d: %v", num, count, events.Items)
	}
	return nil
}

func verifyEventExists(ctx context.Context, f *framework.Framework, options metav1.ListOptions, reason, nodeName string) error {
	events, err := f.ClientSet.CoreV1().Events(metav1.NamespaceDefault).List(ctx, options)
	if err != nil {
		return err
	}
	for _, event := range events.Items {
		if event.Reason == reason && event.Source.Host == nodeName && event.Count > 0 {
			return nil
		}
	}
	return fmt.Errorf("Event %s does not exist: %v", reason, events.Items)
}

func verifyNodeCondition(ctx context.Context, f *framework.Framework, condition v1.NodeConditionType, status v1.ConditionStatus, reason, nodeName string) error {
	node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	_, c := testutils.GetNodeCondition(&node.Status, condition)
	if c == nil {
		return fmt.Errorf("node condition %q not found", condition)
	}
	if c.Status != status || c.Reason != reason {
		return fmt.Errorf("unexpected node condition %q: %+v", condition, c)
	}
	return nil
}

func getMemoryStat(ctx context.Context, f *framework.Framework, host string) (rss, workingSet float64) {
	var memCmd string

	isCgroupV2 := isHostRunningCgroupV2(ctx, f, host)
	if isCgroupV2 {
		memCmd = "cat /sys/fs/cgroup/system.slice/node-problem-detector.service/memory.current && cat /sys/fs/cgroup/system.slice/node-problem-detector.service/memory.stat"
	} else {
		memCmd = "cat /sys/fs/cgroup/memory/system.slice/node-problem-detector.service/memory.usage_in_bytes && cat /sys/fs/cgroup/memory/system.slice/node-problem-detector.service/memory.stat"
	}

	result, err := e2essh.SSH(ctx, memCmd, host, framework.TestContext.Provider)
	framework.ExpectNoError(err)
	gomega.Expect(result.Code).To(gomega.Equal(0))
	lines := strings.Split(result.Stdout, "\n")

	memoryUsage, err := strconv.ParseFloat(lines[0], 64)
	framework.ExpectNoError(err)

	var rssToken, inactiveFileToken string
	if isCgroupV2 {
		// 在 cgroupv2 上，cAdvisor 使用 Anon 内存作为 RSS
		// 参见 https://github.com/google/cadvisor/blob/a9858972e75642c2b1914c8d5428e33e6392c08a/container/libcontainer/handler.go#L799
		rssToken = "anon"
		inactiveFileToken = "inactive_file"
	} else {
		rssToken = "total_rss"
		inactiveFileToken = "total_inactive_file"
	}

	var totalInactiveFile float64
	for _, line := range lines[1:] {
		tokens := strings.Split(line, " ")

		if tokens[0] == rssToken {
			rss, err = strconv.ParseFloat(tokens[1], 64)
			framework.ExpectNoError(err)
		}
		if tokens[0] == inactiveFileToken {
			totalInactiveFile, err = strconv.ParseFloat(tokens[1], 64)
			framework.ExpectNoError(err)
		}
	}

	workingSet = memoryUsage
	if workingSet < totalInactiveFile {
		workingSet = 0
	} else {
		workingSet -= totalInactiveFile
	}

	// 转换为 MB
	rss = rss / 1024 / 1024
	workingSet = workingSet / 1024 / 1024
	return
}

func getCPUStat(ctx context.Context, f *framework.Framework, host string) (usage, uptime float64) {
	var cpuCmd string
	if isHostRunningCgroupV2(ctx, f, host) {
		cpuCmd = " cat /sys/fs/cgroup/cpu.stat | grep 'usage_usec' | sed 's/[^0-9]*//g' && cat /proc/uptime | awk '{print $1}'"
	} else {
		cpuCmd = "cat /sys/fs/cgroup/cpu/system.slice/node-problem-detector.service/cpuacct.usage && cat /proc/uptime | awk '{print $1}'"
	}

	result, err := e2essh.SSH(ctx, cpuCmd, host, framework.TestContext.Provider)
	framework.ExpectNoError(err)
	gomega.Expect(result.Code).To(gomega.Equal(0))
	lines := strings.Split(result.Stdout, "\n")

	usage, err = strconv.ParseFloat(lines[0], 64)
	framework.ExpectNoError(err, "Cannot parse float for usage")
	uptime, err = strconv.ParseFloat(lines[1], 64)
	framework.ExpectNoError(err, "Cannot parse float for uptime")

	// 从纳秒转换为秒
	usage *= 1e-9
	return
}

func isHostRunningCgroupV2(ctx context.Context, f *framework.Framework, host string) bool {
	result, err := e2essh.SSH(ctx, "stat -fc %T /sys/fs/cgroup/", host, framework.TestContext.Provider)
	framework.ExpectNoError(err)
	gomega.Expect(result.Code).To(gomega.Equal(0))

	// 0x63677270 == CGROUP2_SUPER_MAGIC
	// https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html
	return strings.Contains(result.Stdout, "cgroup2") || strings.Contains(result.Stdout, "0x63677270")
}

func getNpdPodStat(ctx context.Context, f *framework.Framework, nodeName string) (cpuUsage, rss, workingSet float64) {
	summary, err := e2ekubelet.GetStatsSummary(ctx, f.ClientSet, nodeName)
	framework.ExpectNoError(err)

	hasNpdPod := false
	for _, pod := range summary.Pods {
		if !strings.HasPrefix(pod.PodRef.Name, "node-problem-detector") {
			continue
		}
		if pod.CPU != nil && pod.CPU.UsageNanoCores != nil {
			cpuUsage = float64(*pod.CPU.UsageNanoCores) * 1e-9
		}
		if pod.Memory != nil {
			if pod.Memory.RSSBytes != nil {
				rss = float64(*pod.Memory.RSSBytes) / 1024 / 1024
			}
			if pod.Memory.WorkingSetBytes != nil {
				workingSet = float64(*pod.Memory.WorkingSetBytes) / 1024 / 1024
			}
		}
		hasNpdPod = true
		break
	}
	if !hasNpdPod {
		framework.Failf("No node-problem-detector pod is present in %+v", summary.Pods)
	}
	return
}
