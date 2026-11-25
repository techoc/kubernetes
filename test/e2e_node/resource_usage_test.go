//go:build linux

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

package e2enode

import (
	"context"
	"fmt"
	"strings"
	"time"

	clientset "k8s.io/client-go/kubernetes"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2eperf "k8s.io/kubernetes/test/e2e/framework/perf"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
)

var _ = SIGDescribe("Resource-usage", framework.WithSerial(), framework.WithSlow(), func() {
	const (
		// 轮询节点 /stats/container 的间隔
		containerStatsPollingPeriod = 10 * time.Second
	)

	var (
		rc *ResourceCollector
		om *e2ekubelet.RuntimeOperationMonitor
	)

	f := framework.NewDefaultFramework("resource-usage")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		om = e2ekubelet.NewRuntimeOperationMonitor(ctx, f.ClientSet)
		// 测试从独立的 Cadvisor Pod 收集资源使用情况。
		// Kubelet 的 Cadvsior 的 housekeeping 间隔为 10 秒，太长，
		// 无法显示资源使用峰值。但修改其间隔会增加
		// kubelet 的开销，因此我们使用单独的 Cadvisor Pod。
		e2epod.NewPodClient(f).CreateSync(ctx, getCadvisorPod())
		rc = NewResourceCollector(containerStatsPollingPeriod)
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		result := om.GetLatestRuntimeOperationErrorRate(ctx)
		framework.Logf("runtime operation error metrics:\n%s", e2ekubelet.FormatRuntimeOperationErrorRate(result))
	})

	// 此测试测量并验证节点的稳定资源使用量是否在上限内
	// 它从 housekeeping 间隔为 1 秒的独立 Cadvisor 收集数据。
	// 验证 CPU 百分位数和最新的内存使用量。
	ginkgo.Context("regular resource usage tracking", func() {
		rTests := []resourceTest{
			{
				podsNr: 10,
				cpuLimits: e2ekubelet.ContainersCPUSummary{
					kubeletstatsv1alpha1.SystemContainerKubelet: {0.50: 0.30, 0.95: 0.35},
					kubeletstatsv1alpha1.SystemContainerRuntime: {0.50: 0.30, 0.95: 0.40},
				},
				memLimits: e2ekubelet.ResourceUsagePerContainer{
					kubeletstatsv1alpha1.SystemContainerKubelet: &e2ekubelet.ContainerResourceUsage{MemoryRSSInBytes: 200 * 1024 * 1024},
					kubeletstatsv1alpha1.SystemContainerRuntime: &e2ekubelet.ContainerResourceUsage{MemoryRSSInBytes: 400 * 1024 * 1024},
				},
			},
		}

		for _, itArg := range rTests {
			desc := fmt.Sprintf("resource tracking for %d pods per node", itArg.podsNr)
			ginkgo.It(desc, func(ctx context.Context) {
				testInfo := getTestNodeInfo(f, itArg.getTestName(), desc)

				runResourceUsageTest(ctx, f, rc, itArg)

				// 记录并验证资源使用量
				logAndVerifyResource(ctx, f, rc, itArg.cpuLimits, itArg.memLimits, testInfo, true)
			})
		}
	})

	ginkgo.Context("regular resource usage tracking", func() {
		rTests := []resourceTest{
			{
				podsNr: 0,
			},
			{
				podsNr: 10,
			},
			{
				podsNr: 35,
			},
			{
				podsNr: 90,
			},
		}

		for _, testArg := range rTests {
			desc := fmt.Sprintf("resource tracking for %d pods per node [Benchmark]", testArg.podsNr)
			ginkgo.It(desc, func(ctx context.Context) {
				testInfo := getTestNodeInfo(f, testArg.getTestName(), desc)

				runResourceUsageTest(ctx, f, rc, testArg)

				// 记录并验证资源使用量
				logAndVerifyResource(ctx, f, rc, testArg.cpuLimits, testArg.memLimits, testInfo, false)
			})
		}
	})
})

type resourceTest struct {
	podsNr    int
	cpuLimits e2ekubelet.ContainersCPUSummary
	memLimits e2ekubelet.ResourceUsagePerContainer
}

func (rt *resourceTest) getTestName() string {
	return fmt.Sprintf("resource_%d", rt.podsNr)
}

// runResourceUsageTest 运行资源使用量测试
func runResourceUsageTest(ctx context.Context, f *framework.Framework, rc *ResourceCollector, testArg resourceTest) {
	const (
		// 单个测试的监控时长
		monitoringTime = 10 * time.Minute
		// 定期报告周期
		reportingPeriod = 5 * time.Minute
		// 在此休眠一段时间以测量稳定数据
		sleepAfterCreatePods = 10 * time.Second
	)
	pods := newTestPods(testArg.podsNr, true, imageutils.GetPauseImageName(), "test_pod")

	rc.Start()
	// 显式删除 Pod，防止命名空间控制器清理超时
	ginkgo.DeferCleanup(deletePodsSync, f, append(pods, getCadvisorPod()))
	ginkgo.DeferCleanup(rc.Stop)

	ginkgo.By("Creating a batch of Pods")
	e2epod.NewPodClient(f).CreateBatch(ctx, pods)

	// 等待一段时间让节点稳定
	time.Sleep(sleepAfterCreatePods)

	// 记录一次并刷新统计信息。
	rc.LogLatest()
	rc.Reset()

	ginkgo.By("Start monitoring resource usage")
	// 定期 dump CPU 摘要直到截止时间。
	// 注意，如果不调用 e2ekubelet.ResourceMonitor.Reset()，这些统计信息
	// 会占用越来越多内存。对当前测试时长来说这应该没问题，
	// 但如果计划监控更长时间（例如 8 小时），应回收
	// 这些条目。
	deadline := time.Now().Add(monitoringTime)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		timeLeft := time.Until(deadline)
		framework.Logf("Still running...%v left", timeLeft)
		if timeLeft < reportingPeriod {
			time.Sleep(timeLeft)
		} else {
			time.Sleep(reportingPeriod)
		}
		logPods(ctx, f.ClientSet)
	}

	ginkgo.By("Reporting overall resource usage")
	logPods(ctx, f.ClientSet)
}

// logAndVerifyResource 将资源使用量作为性能数据打印，并验证其是否满足上限。
func logAndVerifyResource(ctx context.Context, f *framework.Framework, rc *ResourceCollector, cpuLimits e2ekubelet.ContainersCPUSummary,
	memLimits e2ekubelet.ResourceUsagePerContainer, testInfo map[string]string, isVerify bool) {
	nodeName := framework.TestContext.NodeName

	// 获取内存 PerfData
	usagePerContainer, err := rc.GetLatest()
	framework.ExpectNoError(err)
	framework.Logf("%s", formatResourceUsageStats(usagePerContainer))

	usagePerNode := make(e2ekubelet.ResourceUsagePerNode)
	usagePerNode[nodeName] = usagePerContainer

	// Obtain CPU PerfData
	cpuSummary := rc.GetCPUSummary()
	framework.Logf("%s", formatCPUSummary(cpuSummary))

	cpuSummaryPerNode := make(e2ekubelet.NodesCPUSummary)
	cpuSummaryPerNode[nodeName] = cpuSummary

	// 打印资源使用量
	logPerfData(e2eperf.ResourceUsageToPerfDataWithLabels(usagePerNode, testInfo), "memory")
	logPerfData(e2eperf.CPUUsageToPerfDataWithLabels(cpuSummaryPerNode, testInfo), "cpu")

	// 验证资源使用量
	if isVerify {
		verifyMemoryLimits(ctx, f.ClientSet, memLimits, usagePerNode)
		verifyCPULimits(cpuLimits, cpuSummaryPerNode)
	}
}

func verifyMemoryLimits(ctx context.Context, c clientset.Interface, expected e2ekubelet.ResourceUsagePerContainer, actual e2ekubelet.ResourceUsagePerNode) {
	if expected == nil {
		return
	}
	var errList []string
	for nodeName, nodeSummary := range actual {
		var nodeErrs []string
		for cName, expectedResult := range expected {
			container, ok := nodeSummary[cName]
			if !ok {
				nodeErrs = append(nodeErrs, fmt.Sprintf("container %q: missing", cName))
				continue
			}

			expectedValue := expectedResult.MemoryRSSInBytes
			actualValue := container.MemoryRSSInBytes
			if expectedValue != 0 && actualValue > expectedValue {
				nodeErrs = append(nodeErrs, fmt.Sprintf("container %q: expected RSS memory (MB) < %d; got %d",
					cName, expectedValue, actualValue))
			}
		}
		if len(nodeErrs) > 0 {
			errList = append(errList, fmt.Sprintf("node %v:\n %s", nodeName, strings.Join(nodeErrs, ", ")))
			heapStats, err := e2ekubelet.GetKubeletHeapStats(ctx, c, nodeName)
			if err != nil {
				framework.Logf("Unable to get heap stats from %q", nodeName)
			} else {
				framework.Logf("Heap stats on %q\n:%v", nodeName, heapStats)
			}
		}
	}
	if len(errList) > 0 {
		framework.Failf("Memory usage exceeding limits:\n %s", strings.Join(errList, "\n"))
	}
}

func verifyCPULimits(expected e2ekubelet.ContainersCPUSummary, actual e2ekubelet.NodesCPUSummary) {
	if expected == nil {
		return
	}
	var errList []string
	for nodeName, perNodeSummary := range actual {
		var nodeErrs []string
		for cName, expectedResult := range expected {
			perContainerSummary, ok := perNodeSummary[cName]
			if !ok {
				nodeErrs = append(nodeErrs, fmt.Sprintf("container %q: missing", cName))
				continue
			}
			for p, expectedValue := range expectedResult {
				actualValue, ok := perContainerSummary[p]
				if !ok {
					nodeErrs = append(nodeErrs, fmt.Sprintf("container %q: missing percentile %v", cName, p))
					continue
				}
				if actualValue > expectedValue {
					nodeErrs = append(nodeErrs, fmt.Sprintf("container %q: expected %.0fth%% usage < %.3f; got %.3f",
						cName, p*100, expectedValue, actualValue))
				}
			}
		}
		if len(nodeErrs) > 0 {
			errList = append(errList, fmt.Sprintf("node %v:\n %s", nodeName, strings.Join(nodeErrs, ", ")))
		}
	}
	if len(errList) > 0 {
		framework.Failf("CPU usage exceeding limits:\n %s", strings.Join(errList, "\n"))
	}
}

func logPods(ctx context.Context, c clientset.Interface) {
	nodeName := framework.TestContext.NodeName
	podList, err := e2ekubelet.GetKubeletRunningPods(ctx, c, nodeName)
	if err != nil {
		framework.Logf("Unable to retrieve kubelet pods for node %v", nodeName)
	}
	framework.Logf("%d pods are running on node %v", len(podList.Items), nodeName)
}
