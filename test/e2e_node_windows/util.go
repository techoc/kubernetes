//go:build windows

/*
Copyright The Kubernetes Authors.

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

package e2enodewindows

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	internalapi "k8s.io/cri-api/pkg/apis"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/cluster/ports"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

var (
	startServices = flag.Bool("start-services", true, "If true, start local node services")
	stopServices  = flag.Bool("stop-services", true, "If true, stop local node services after running tests")
	busyboxImage  = imageutils.GetE2EImage(imageutils.BusyBox)

	// kubeletCfg 是测试所针对的 kubelet 配置。
	kubeletCfg *kubeletconfig.KubeletConfiguration

	kubeletHealthCheckURL = fmt.Sprintf("http://127.0.0.1:%d/healthz", ports.KubeletHealthzPort)
)

const (
	kubeletServiceName = "kubelet"

	// CPU/memory manager 的状态文件位于 kubelet 根目录
	//（--root-dir，即 /var/lib/kubelet）。当 kubelet 被重新配置且
	// 使持久化的 checkpoint 失效时（例如切换
	// static CPU manager 的 strict-cpu-reservation 选项），必须删除它们，否则
	// kubelet 会拒绝启动并报 "invalid state, please drain node and
	// remove policy state file"。
	cpuManagerStateFile    = "/var/lib/kubelet/cpu_manager_state"
	memoryManagerStateFile = "/var/lib/kubelet/memory_manager_state"
)

// getKubeletServicePID 返回 kubelet service 的 PID，如果无法确定则返回 0
// （例如 service 已不再运行）。
func getKubeletServicePID() int {
	cmdLine := []string{"sc.exe", "queryex", kubeletServiceName}

	stdout, err := exec.Command(cmdLine[0], cmdLine[1:]...).CombinedOutput()
	if err != nil {
		return 0
	}

	regex := regexp.MustCompile(`PID\s*:\s*(\d+)`)
	matches := regex.FindStringSubmatch(string(stdout))
	if len(matches) <= 1 {
		return 0
	}

	pid, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0
	}
	return pid
}

// killProcessByPID 杀死给定 PID 的进程。"process not found"
// 视为成功 —— 执行 taskkill 时 kubelet 可能
// 已自行退出（例如响应了之前的 sc.exe stop）。
func killProcessByPID(pid int) {
	if pid <= 0 {
		return
	}
	cmdLine := []string{"taskkill", "/F", "/PID", strconv.Itoa(pid)}

	stdout, err := exec.Command(cmdLine[0], cmdLine[1:]...).CombinedOutput()
	if err == nil {
		return
	}
	// 当进程已退出时，taskkill 返回非零并提示 "not found" / "no running instance"。
	// 这正是我们想要的状态，因此忽略该错误。
	out := strings.ToLower(string(stdout))
	if strings.Contains(out, "not found") || strings.Contains(out, "no running") {
		return
	}
	framework.ExpectNoError(err, "taskkill failed for PID %d: %s", pid, string(stdout))
}

// findKubeletServiceState 查询 kubelet service 的状态。
func findKubeletServiceState() string {
	cmdLine := []string{"sc.exe", "query", kubeletServiceName}

	// 假定 kubelet service 已注册
	stdout, err := exec.Command(cmdLine[0], cmdLine[1:]...).CombinedOutput()
	framework.ExpectNoError(err)

	regex := regexp.MustCompile(`(?m)STATE\s*:\s*\d+\s+(\w+)`)
	matches := regex.FindStringSubmatch(string(stdout))
	gomega.Expect(len(matches)).To(gomega.BeNumerically(">", 1), "Found the matched state: %q", stdout)
	state := matches[1]

	return state
}

// stopKubeletService 停止 kubelet Windows service，并等待 SCM
// 报告为 STOPPED。sc.exe stop 是异步的：service 通常
// 经历 RUNNING -> STOP_PENDING -> STOPPED。如果在 SCM 仍认为 service 运行时
// 执行 sc.exe start，会产生错误 1056
// （"An instance of the service is already running"），因此下一次启动必须以
// SCM 状态为准，而不是 HTTP 健康探测（后者在 SCM 完成状态转换前
// 早已不可用）。
func stopKubeletService(ctx context.Context) {
	state := findKubeletServiceState()
	if strings.EqualFold(state, "STOPPED") {
		return
	}

	if strings.EqualFold(state, "RUNNING") {
		stdout, err := exec.CommandContext(ctx, "sc.exe", "stop", kubeletServiceName).CombinedOutput()
		framework.ExpectNoError(err, "Failed to stop kubelet service: %v, %s", err, string(stdout))
	}

	// 等待 SCM 报告 STOPPED。如果长时间停留在 STOP_PENDING，
	// 则回退到强制杀死 kubelet 进程。
	const (
		stopTimeout = 30 * time.Second
		stopPoll    = 250 * time.Millisecond
	)
	deadline := time.Now().Add(stopTimeout)
	for time.Now().Before(deadline) {
		if strings.EqualFold(findKubeletServiceState(), "STOPPED") {
			return
		}
		time.Sleep(stopPoll)
	}

	// 卡在 STOP_PENDING —— 强制杀死进程并重新检查。
	killProcessByPID(getKubeletServicePID())
	gomega.Eventually(ctx, func() string {
		return strings.ToUpper(findKubeletServiceState())
	}, 10*time.Second, stopPoll).Should(gomega.Equal("STOPPED"), "kubelet service did not reach STOPPED state")
}

// startKubeletService 启动 kubelet Windows service 并等待
// kubelet HTTP 健康检查成功。
func startKubeletService(ctx context.Context, f *framework.Framework) {
	stdout, err := exec.CommandContext(ctx, "sc.exe", "start", kubeletServiceName).CombinedOutput()
	framework.ExpectNoError(err, "Failed to start kubelet service with sc.exe: %v, %s", err, string(stdout))
	waitForKubeletToStart(ctx, f)
}

// restartKubelet 重启当前的 kubelet service。
// "当前" kubelet service 指由本次 e2e_node 测试运行管理的实例。
// 如果 `running` 为 true，仅在当前 kubelet 确实运行时才重启。有时 kubelet
// 可能已退出或可被停止，通常是因为测试中先前有意停止了它，
// 或者只是崩溃了。
// 警告："当前" kubelet 定义不清晰，通常假定为最近的 kubelet service 单元，
// 换言之，并没有唯一 ID 能将某个 kubelet 实例
// 明确绑定到一次测试运行。
func restartKubelet(ctx context.Context, running bool) {
	stopKubeletService(ctx)

	stdout, err := exec.CommandContext(ctx, "sc.exe", "start", kubeletServiceName).CombinedOutput()
	framework.ExpectNoError(err, "Failed to restart kubelet service with sc.exe: %v, %s", err, string(stdout))
}

// mustStopKubelet 杀死正在运行的 kubelet，并返回一个可再次启动进程的 func
func mustStopKubelet(ctx context.Context, f *framework.Framework) func(ctx context.Context) {
	stopKubeletService(ctx)

	// 双保险：返回前确保 HTTP 健康 endpoint 也已关闭，
	// 因为下一个测试会探测该接口。
	gomega.Eventually(ctx, func() bool {
		return e2enode.HealthCheck(kubeletHealthCheckURL)
	}, f.Timeouts.PodStart, f.Timeouts.Poll).Should(gomega.BeFalseBecause("kubelet was expected to be stopped but it is still running"))

	return func(ctx context.Context) {
		startKubeletService(ctx, f)
	}
}

// TODO: add the windows part implementation
func stopContainerRuntime() error {
	return nil
}

func startContainerRuntime() error {
	return nil
}

// deleteStateFile 删除指定文件名的状态文件。
func deleteStateFile(stateFileName string) {
	err := os.Remove(stateFileName)
	if err != nil && !os.IsNotExist(err) {
		framework.ExpectNoError(err, "failed to delete the state file %q", stateFileName)
	}
}

// systemValidation 验证系统规格。
func systemValidation(systemSpecFile *string) {
	klog.Warningf("system spec validation is not supported on platform other than linux yet")
}

func getNodeSummary(ctx context.Context) (*stats.Summary, error) {
	kubeletConfig, err := getCurrentKubeletConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current kubelet config")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s/stats/summary", net.JoinHostPort(kubeletConfig.Address, strconv.Itoa(int(kubeletConfig.ReadOnlyPort)))), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to build http request: %w", err)
	}
	req.Header.Add("Accept", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get /stats/summary: %w", err)
	}

	defer resp.Body.Close()
	contentsBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read /stats/summary: %+v", resp)
	}

	decoder := json.NewDecoder(strings.NewReader(string(contentsBytes)))
	summary := stats.Summary{}
	err = decoder.Decode(&summary)
	if err != nil {
		return nil, fmt.Errorf("failed to parse /stats/summary to go struct: %+v", resp)
	}
	return &summary, nil
}

func addAfterEachForCleaningUpPods(f *framework.Framework) {
	ginkgo.AfterEach(func(ctx context.Context) {
		ginkgo.By("Deleting any Pods created by the test in namespace: " + f.Namespace.Name)
		l, err := e2epod.NewPodClient(f).List(ctx, metav1.ListOptions{})
		framework.ExpectNoError(err)
		for _, p := range l.Items {
			if p.Namespace != f.Namespace.Name {
				continue
			}
			framework.Logf("Deleting pod: %s", p.Name)
			e2epod.NewPodClient(f).DeleteSync(ctx, p.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
		}
	})
}

func waitForKubeletToStart(ctx context.Context, f *framework.Framework) {
	// 等待 kubelet 健康检查成功
	gomega.Eventually(ctx, func() bool {
		return e2enode.HealthCheck(kubeletHealthCheckURL)
	}, 2*time.Minute, 5*time.Second).Should(gomega.BeTrueBecause("expected kubelet to be in healthy state"))

	// 等待 Kubelet 就绪。
	gomega.Eventually(ctx, func(ctx context.Context) error {
		nodes, err := e2enode.TotalReady(ctx, f.ClientSet)
		if err != nil {
			return fmt.Errorf("error getting ready nodes: %w", err)
		}
		if nodes != 1 {
			return fmt.Errorf("expected 1 ready node, got %d", nodes)
		}
		return nil
	}, time.Minute, time.Second).Should(gomega.Succeed())
}

// listNamespaceEvents 列出指定命名空间中的事件。
func listNamespaceEvents(ctx context.Context, c clientset.Interface, ns string) error {
	ls, err := c.CoreV1().Events(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, event := range ls.Items {
		klog.Infof("Event(%#v): type: '%v' reason: '%v' %v", event.InvolvedObject, event.Type, event.Reason, event.Message)
	}
	return nil
}

func logPodEvents(ctx context.Context, f *framework.Framework) {
	framework.Logf("Summary of pod events during the test:")
	err := listNamespaceEvents(ctx, f.ClientSet, f.Namespace.Name)
	framework.ExpectNoError(err)
}

func logNodeEvents(ctx context.Context, f *framework.Framework) {
	framework.Logf("Summary of node events during the test:")
	err := listNamespaceEvents(ctx, f.ClientSet, "")
	framework.ExpectNoError(err)
}

func getLocalNode(ctx context.Context, f *framework.Framework) *v1.Node {
	nodeList, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
	framework.ExpectNoError(err)
	gomega.Expect(nodeList.Items).Should(gomega.HaveLen(1), "Unexpected number of node objects for node e2e. Expects only one node.")
	return &nodeList.Items[0]
}

// getLocalTestNode 获取描述 e2e_node 基础设施建立的本地 worker 节点的 node 对象及其就绪状态。
func getLocalTestNode(ctx context.Context, f *framework.Framework) (*v1.Node, bool) {
	logger := klog.FromContext(ctx)
	node, err := f.ClientSet.CoreV1().Nodes().Get(ctx, framework.TestContext.NodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	ready := e2enode.IsNodeReady(logger, node)
	schedulable := e2enode.IsNodeSchedulable(logger, node)
	framework.Logf("node %q ready=%v schedulable=%v", node.Name, ready, schedulable)
	return node, ready && schedulable
}

func getLocalNodeCPUDetails(ctx context.Context, f *framework.Framework) (cpuCapVal int64, cpuAllocVal int64, cpuResVal int64) {
	localNodeCap := getLocalNode(ctx, f).Status.Capacity
	cpuCap := localNodeCap[v1.ResourceCPU]
	localNodeAlloc := getLocalNode(ctx, f).Status.Allocatable
	cpuAlloc := localNodeAlloc[v1.ResourceCPU]
	cpuRes := cpuCap.DeepCopy()
	cpuRes.Sub(cpuAlloc)

	// 对预留的 CPU 向上取整，只保留整数核心。
	cpuRes.RoundUp(0)

	return cpuCap.Value(), cpuCap.Value() - cpuRes.Value(), cpuRes.Value()
}

func nodeNameOrIP() string {
	return "localhost"
}

// logKubeletLatencyMetrics 记录从当前节点暴露的 Prometheus 指标中
// 计算得出、由 metricNames 标识的 KubeletLatencyMetrics。
// Kubelet 子系统前缀会自动加到这些指标名之前。
func logKubeletLatencyMetrics(ctx context.Context, metricNames ...string) {
	metricSet := sets.NewString()
	for _, key := range metricNames {
		metricSet.Insert(kubeletmetrics.KubeletSubsystem + "_" + key)
	}
	metric, err := e2emetrics.GrabKubeletMetricsWithoutProxy(ctx, fmt.Sprintf("%s:%d", nodeNameOrIP(), ports.KubeletReadOnlyPort), "/metrics")
	if err != nil {
		framework.Logf("Error getting kubelet metrics: %v", err)
	} else {
		framework.Logf("Kubelet Metrics: %+v", e2emetrics.GetKubeletLatencyMetrics(metric, metricSet))
	}
}

// getCRIClient 连接 CRI，返回 CRI runtime service client 和 image service client。
func getCRIClient(ctx context.Context) (internalapi.RuntimeService, internalapi.ImageManagerService, error) {
	// CRI service 连接的超时时间
	const connectionTimeout = 2 * time.Minute
	runtimeEndpoint := framework.TestContext.ContainerRuntimeEndpoint
	useStreaming := utilfeature.DefaultFeatureGate.Enabled(features.CRIListStreaming)
	r, err := remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(runtimeEndpoint).
		WithConnectionTimeout(connectionTimeout).
		WithUseStreaming(useStreaming).
		Build(ctx)
	if err != nil {
		return nil, nil, err
	}
	imageManagerEndpoint := runtimeEndpoint
	if framework.TestContext.ImageServiceEndpoint != "" {
		// 如果未显式指定，ImageServiceEndpoint 与 ContainerRuntimeEndpoint 相同。
		// 显式指定。
		imageManagerEndpoint = framework.TestContext.ImageServiceEndpoint
	}
	i, err := remote.NewRemoteImageServiceBuilder().
		WithEndpoint(imageManagerEndpoint).
		WithConnectionTimeout(connectionTimeout).
		WithUseStreaming(useStreaming).
		Build(ctx)
	if err != nil {
		return nil, nil, err
	}
	return r, i, nil
}
