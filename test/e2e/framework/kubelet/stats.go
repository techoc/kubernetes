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

package kubelet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"

	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
)

const (
	// 代理请求的超时时间。
	proxyTimeout = 2 * time.Minute

	// dockerOperationsKey 是 docker 操作指标的键。
	// 复制自 k8s.io/kubernetes/pkg/kubelet/dockershim/metrics
	dockerOperationsKey = "docker_operations_total"

	// dockerOperationsErrorsKey 是操作错误指标的键。
	// 复制自 k8s.io/kubernetes/pkg/kubelet/dockershim/metrics
	dockerOperationsErrorsKey = "docker_operations_errors_total"

	// dockerOperationsTimeoutKey 是操作超时指标的键。
	// 复制自 k8s.io/kubernetes/pkg/kubelet/dockershim/metrics
	dockerOperationsTimeoutKey = "docker_operations_timeout_total"
)

// ContainerResourceUsage 用于收集容器资源使用情况。
type ContainerResourceUsage struct {
	Name                    string
	Timestamp               time.Time
	CPUUsageInCores         float64
	MemoryUsageInBytes      uint64
	MemoryWorkingSetInBytes uint64
	MemoryRSSInBytes        uint64
	// 计算 CPUUsageInCores 时采用的时间间隔。
	CPUInterval time.Duration
}

// ResourceUsagePerContainer 是以 ContainerResourceUsage 为值的映射。
type ResourceUsagePerContainer map[string]*ContainerResourceUsage

// ResourceUsagePerNode 是以 ResourceUsagePerContainer 为值的映射。
type ResourceUsagePerNode map[string]ResourceUsagePerContainer

// ContainersCPUSummary 以容器名为索引，每项是一个
// （百分位，值）映射。
type ContainersCPUSummary map[string]map[float64]float64

// NodesCPUSummary 以节点名为索引，每项是一个
// ContainersCPUSummary 映射。
type NodesCPUSummary map[string]ContainersCPUSummary

// RuntimeOperationMonitor 用于获取并解析 docker 操作指标。
type RuntimeOperationMonitor struct {
	client          clientset.Interface
	nodesRuntimeOps map[string]NodeRuntimeOperationErrorRate
}

// NodeRuntimeOperationErrorRate 表示单个节点上的 runtime 操作错误率。
type NodeRuntimeOperationErrorRate map[string]*RuntimeOperationErrorRate

// RuntimeOperationErrorRate 表示指定 runtime 操作的错误率。
type RuntimeOperationErrorRate struct {
	TotalNumber float64
	ErrorRate   float64
	TimeoutRate float64
}

// ProxyRequest 根据节点名和 rest client 对节点代理端点执行 GET 请求。
func ProxyRequest(ctx context.Context, c clientset.Interface, node, endpoint string, port int) (restclient.Result, error) {
	// Node 未就绪时代理请求有时会挂起，因此为该调用加上人为的超时限制。#22165
	var result restclient.Result
	finished := make(chan struct{}, 1)
	go func() {
		result = c.CoreV1().RESTClient().Get().
			Resource("nodes").
			SubResource("proxy").
			Name(fmt.Sprintf("%v:%v", node, port)).
			Suffix(endpoint).
			Do(ctx)

		finished <- struct{}{}
	}()
	select {
	case <-finished:
		return result, nil
	case <-time.After(proxyTimeout):
		return restclient.Result{}, nil
	}
}

// NewRuntimeOperationMonitor 返回一个新的 RuntimeOperationMonitor。
func NewRuntimeOperationMonitor(ctx context.Context, c clientset.Interface) *RuntimeOperationMonitor {
	m := &RuntimeOperationMonitor{
		client:          c,
		nodesRuntimeOps: make(map[string]NodeRuntimeOperationErrorRate),
	}
	nodes, err := m.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Failf("RuntimeOperationMonitor: unable to get list of nodes: %v", err)
	}
	for _, node := range nodes.Items {
		m.nodesRuntimeOps[node.Name] = make(NodeRuntimeOperationErrorRate)
	}
	// 初始化 runtime 操作错误率。
	m.GetRuntimeOperationErrorRate(ctx)
	return m
}

// GetRuntimeOperationErrorRate 从 kubelet 指标中获取 runtime 操作记录，并计算
// 所有 runtime 操作的错误率。
func (m *RuntimeOperationMonitor) GetRuntimeOperationErrorRate(ctx context.Context) map[string]NodeRuntimeOperationErrorRate {
	for node := range m.nodesRuntimeOps {
		nodeResult, err := getNodeRuntimeOperationErrorRate(ctx, m.client, node)
		if err != nil {
			framework.Logf("GetRuntimeOperationErrorRate: unable to get kubelet metrics from node %q: %v", node, err)
			continue
		}
		m.nodesRuntimeOps[node] = nodeResult
	}
	return m.nodesRuntimeOps
}

// GetLatestRuntimeOperationErrorRate 根据上一次观测到的 RuntimeOperationErrorRate 计算最新的错误率和超时率。
func (m *RuntimeOperationMonitor) GetLatestRuntimeOperationErrorRate(ctx context.Context) map[string]NodeRuntimeOperationErrorRate {
	result := make(map[string]NodeRuntimeOperationErrorRate)
	for node := range m.nodesRuntimeOps {
		result[node] = make(NodeRuntimeOperationErrorRate)
		oldNodeResult := m.nodesRuntimeOps[node]
		curNodeResult, err := getNodeRuntimeOperationErrorRate(ctx, m.client, node)
		if err != nil {
			framework.Logf("GetLatestRuntimeOperationErrorRate: unable to get kubelet metrics from node %q: %v", node, err)
			continue
		}
		for op, cur := range curNodeResult {
			t := *cur
			if old, found := oldNodeResult[op]; found {
				t.ErrorRate = (t.ErrorRate*t.TotalNumber - old.ErrorRate*old.TotalNumber) / (t.TotalNumber - old.TotalNumber)
				t.TimeoutRate = (t.TimeoutRate*t.TotalNumber - old.TimeoutRate*old.TotalNumber) / (t.TotalNumber - old.TotalNumber)
				t.TotalNumber -= old.TotalNumber
			}
			result[node][op] = &t
		}
		m.nodesRuntimeOps[node] = curNodeResult
	}
	return result
}

// FormatRuntimeOperationErrorRate 将 runtime 操作错误率格式化为字符串。
func FormatRuntimeOperationErrorRate(nodesResult map[string]NodeRuntimeOperationErrorRate) string {
	lines := []string{}
	for node, nodeResult := range nodesResult {
		lines = append(lines, fmt.Sprintf("node %q runtime operation error rate:", node))
		for op, result := range nodeResult {
			line := fmt.Sprintf("operation %q: total - %.0f; error rate - %f; timeout rate - %f", op,
				result.TotalNumber, result.ErrorRate, result.TimeoutRate)
			lines = append(lines, line)
		}
		lines = append(lines, fmt.Sprintln())
	}
	return strings.Join(lines, "\n")
}

// getNodeRuntimeOperationErrorRate 从指定节点获取 runtime 操作错误率。
func getNodeRuntimeOperationErrorRate(ctx context.Context, c clientset.Interface, node string) (NodeRuntimeOperationErrorRate, error) {
	result := make(NodeRuntimeOperationErrorRate)
	ms, err := e2emetrics.GetKubeletMetrics(ctx, c, node)
	if err != nil {
		return result, err
	}
	// 若找不到相应的指标，返回的样本将为空，后续循环自然会被跳过。
	allOps := ms[dockerOperationsKey]
	errOps := ms[dockerOperationsErrorsKey]
	timeoutOps := ms[dockerOperationsTimeoutKey]
	for _, sample := range allOps {
		operation := string(sample.Metric["operation_type"])
		result[operation] = &RuntimeOperationErrorRate{TotalNumber: float64(sample.Value)}
	}
	for _, sample := range errOps {
		operation := string(sample.Metric["operation_type"])
		// 理论上总能找到对应项，这里仅作兜底。
		if _, found := result[operation]; found {
			result[operation].ErrorRate = float64(sample.Value) / result[operation].TotalNumber
		}
	}
	for _, sample := range timeoutOps {
		operation := string(sample.Metric["operation_type"])
		if _, found := result[operation]; found {
			result[operation].TimeoutRate = float64(sample.Value) / result[operation].TotalNumber
		}
	}
	return result, nil
}

// GetStatsSummary 向 kubelet 请求容器信息。
func GetStatsSummary(ctx context.Context, c clientset.Interface, nodeName string) (*kubeletstatsv1alpha1.Summary, error) {
	ctx, cancel := context.WithTimeout(ctx, framework.SingleCallTimeout)
	defer cancel()

	data, err := c.CoreV1().RESTClient().Get().
		Resource("nodes").
		SubResource("proxy").
		Name(fmt.Sprintf("%v:%v", nodeName, framework.KubeletPort)).
		Suffix("stats/summary").
		Do(ctx).Raw()

	if err != nil {
		return nil, err
	}

	summary := kubeletstatsv1alpha1.Summary{}
	err = json.Unmarshal(data, &summary)
	if err != nil {
		return nil, err
	}
	return &summary, nil
}

func getNodeStatsSummary(ctx context.Context, c clientset.Interface, nodeName string) (*kubeletstatsv1alpha1.Summary, error) {
	data, err := c.CoreV1().RESTClient().Get().
		Resource("nodes").
		SubResource("proxy").
		Name(fmt.Sprintf("%v:%v", nodeName, framework.KubeletPort)).
		Suffix("stats/summary").
		SetHeader("Content-Type", "application/json").
		Do(ctx).Raw()

	if err != nil {
		return nil, err
	}

	var summary *kubeletstatsv1alpha1.Summary
	err = json.Unmarshal(data, &summary)
	if err != nil {
		return nil, err
	}
	return summary, nil
}

func getSystemContainerStats(summary *kubeletstatsv1alpha1.Summary) map[string]*kubeletstatsv1alpha1.ContainerStats {
	statsList := summary.Node.SystemContainers
	statsMap := make(map[string]*kubeletstatsv1alpha1.ContainerStats)
	for i := range statsList {
		statsMap[statsList[i].Name] = &statsList[i]
	}

	// 用 stats.NodeStats 中的数据构造 root 容器的统计信息，
	// 因为二者的类型不同，必须单独构造。
	statsMap[rootContainerName] = &kubeletstatsv1alpha1.ContainerStats{
		CPU:    summary.Node.CPU,
		Memory: summary.Node.Memory,
	}
	return statsMap
}

const (
	rootContainerName = "/"
)

// TargetContainers 返回需要收集资源使用情况的容器列表。
func TargetContainers() []string {
	return []string{
		rootContainerName,
		kubeletstatsv1alpha1.SystemContainerRuntime,
		kubeletstatsv1alpha1.SystemContainerKubelet,
	}
}

func formatResourceUsageStats(nodeName string, containerStats ResourceUsagePerContainer) string {
	// 输出示例：
	//
	// 节点 "e2e-test-foo-node-abcde" 的资源用量：
	// container        cpu(cores)  memory(MB)
	// "/"              0.363       2942.09
	// "/docker-daemon" 0.088       521.80
	// "/kubelet"       0.086       424.37
	// "/system"        0.007       119.88
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 1, 0, 1, ' ', 0)
	fmt.Fprintf(w, "container\tcpu(cores)\tmemory_working_set(MB)\tmemory_rss(MB)\n")
	for name, s := range containerStats {
		fmt.Fprintf(w, "%q\t%.3f\t%.2f\t%.2f\n", name, s.CPUUsageInCores, float64(s.MemoryWorkingSetInBytes)/(1024*1024), float64(s.MemoryRSSInBytes)/(1024*1024))
	}
	w.Flush()
	return fmt.Sprintf("Resource usage on node %q:\n%s", nodeName, buf.String())
}

// GetKubeletHeapStats 返回 kubelet 堆的统计信息。
func GetKubeletHeapStats(ctx context.Context, c clientset.Interface, nodeName string) (string, error) {
	client, err := ProxyRequest(ctx, c, nodeName, "debug/pprof/heap", framework.KubeletPort)
	if err != nil {
		return "", err
	}
	raw, errRaw := client.Raw()
	if errRaw != nil {
		return "", errRaw
	}
	kubeletstatsv1alpha1 := string(raw)
	// 只输出 runtime.MemStats 的数值，避免污染日志。
	numLines := 23
	lines := strings.Split(kubeletstatsv1alpha1, "\n")
	return strings.Join(lines[len(lines)-numLines:], "\n"), nil
}

func computeContainerResourceUsage(name string, oldStats, newStats *kubeletstatsv1alpha1.ContainerStats) *ContainerResourceUsage {
	return &ContainerResourceUsage{
		Name:                    name,
		Timestamp:               newStats.CPU.Time.Time,
		CPUUsageInCores:         float64(*newStats.CPU.UsageCoreNanoSeconds-*oldStats.CPU.UsageCoreNanoSeconds) / float64(newStats.CPU.Time.Time.Sub(oldStats.CPU.Time.Time).Nanoseconds()),
		MemoryUsageInBytes:      *newStats.Memory.UsageBytes,
		MemoryWorkingSetInBytes: *newStats.Memory.WorkingSetBytes,
		MemoryRSSInBytes:        *newStats.Memory.RSSBytes,
		CPUInterval:             newStats.CPU.Time.Time.Sub(oldStats.CPU.Time.Time),
	}
}

// resourceCollector 定期轮询节点，收集指定容器的统计信息，
// 并计算、缓存每个容器的资源使用情况（最多 maxEntriesPerContainer 条）。
type resourceCollector struct {
	lock            sync.RWMutex
	node            string
	containers      []string
	client          clientset.Interface
	buffers         map[string][]*ContainerResourceUsage
	pollingInterval time.Duration
	stop            func()
}

func newResourceCollector(c clientset.Interface, nodeName string, containerNames []string, pollingInterval time.Duration) *resourceCollector {
	buffers := make(map[string][]*ContainerResourceUsage)
	return &resourceCollector{
		node:            nodeName,
		containers:      containerNames,
		client:          c,
		buffers:         buffers,
		pollingInterval: pollingInterval,
	}
}

// Start 启动一个 goroutine，按 pollingInterval 定期轮询节点。
func (r *resourceCollector) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r.stop = cancel
	// 保留最近一次观测的统计信息，用于后续比较。
	oldStats := make(map[string]*kubeletstatsv1alpha1.ContainerStats)
	go wait.UntilWithContext(ctx, func(ctx context.Context) { r.collectStats(ctx, oldStats) }, r.pollingInterval)
}

// Stop 发出信号，终止收集统计信息的 goroutine。
func (r *resourceCollector) Stop() {
	r.stop()
}

// collectStats 从 kubelet stats summary API 获取最新统计信息，
// 计算资源使用情况并写入缓冲区。
func (r *resourceCollector) collectStats(ctx context.Context, oldStatsMap map[string]*kubeletstatsv1alpha1.ContainerStats) {
	summary, err := getNodeStatsSummary(ctx, r.client, r.node)
	if err != nil {
		framework.Logf("Error getting node stats summary on %q, err: %v", r.node, err)
		return
	}
	cStatsMap := getSystemContainerStats(summary)
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, name := range r.containers {
		cStats, ok := cStatsMap[name]
		if !ok {
			framework.Logf("Missing info/stats for container %q on node %q", name, r.node)
			return
		}

		if oldStats, ok := oldStatsMap[name]; ok {
			if oldStats.CPU == nil || cStats.CPU == nil || oldStats.Memory == nil || cStats.Memory == nil {
				continue
			}
			if oldStats.CPU.Time.Equal(&cStats.CPU.Time) {
				// 数据未更新，跳过本次统计。
				continue
			}
			r.buffers[name] = append(r.buffers[name], computeContainerResourceUsage(name, oldStats, cStats))
		}
		// 更新上一次的统计信息。
		oldStatsMap[name] = cStats
	}
}

func (r *resourceCollector) GetLatest() (ResourceUsagePerContainer, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	kubeletstatsv1alpha1 := make(ResourceUsagePerContainer)
	for _, name := range r.containers {
		contStats, ok := r.buffers[name]
		if !ok || len(contStats) == 0 {
			return nil, fmt.Errorf("Resource usage on node %q is not ready yet", r.node)
		}
		kubeletstatsv1alpha1[name] = contStats[len(contStats)-1]
	}
	return kubeletstatsv1alpha1, nil
}

// Reset 清空统计数据，重新开始收集。
func (r *resourceCollector) Reset() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, name := range r.containers {
		r.buffers[name] = []*ContainerResourceUsage{}
	}
}

type resourceUsageByCPU []*ContainerResourceUsage

func (r resourceUsageByCPU) Len() int           { return len(r) }
func (r resourceUsageByCPU) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r resourceUsageByCPU) Less(i, j int) bool { return r[i].CPUUsageInCores < r[j].CPUUsageInCores }

// 需要报告的百分位。
var percentiles = [...]float64{0.05, 0.20, 0.50, 0.70, 0.90, 0.95, 0.99}

// GetBasicCPUStats 返回 containerName 的 CPU 使用量（单位：核）的各个百分位。
// 该方法会检查缓冲区中当前的所有数据。
func (r *resourceCollector) GetBasicCPUStats(containerName string) map[float64]float64 {
	r.lock.RLock()
	defer r.lock.RUnlock()
	result := make(map[float64]float64, len(percentiles))
	usages := r.buffers[containerName]
	sort.Sort(resourceUsageByCPU(usages))
	for _, q := range percentiles {
		index := int(float64(len(usages))*q) - 1
		if index < 0 {
			// 数据量不足。
			result[q] = 0
			continue
		}
		result[q] = usages[index].CPUUsageInCores
	}
	return result
}

// ResourceMonitor 为每个节点管理一个 resourceCollector。
type ResourceMonitor struct {
	client          clientset.Interface
	containers      []string
	pollingInterval time.Duration
	collectors      map[string]*resourceCollector
}

// NewResourceMonitor 返回一个新的 ResourceMonitor。
func NewResourceMonitor(c clientset.Interface, containerNames []string, pollingInterval time.Duration) *ResourceMonitor {
	return &ResourceMonitor{
		containers:      containerNames,
		client:          c,
		pollingInterval: pollingInterval,
	}
}

// Start 启动所有收集器。
func (r *ResourceMonitor) Start(ctx context.Context) {
	// 监控不可调度的 Node 应该也没有问题。
	nodes, err := r.client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Failf("ResourceMonitor: unable to get list of nodes: %v", err)
	}
	r.collectors = make(map[string]*resourceCollector, 0)
	for _, node := range nodes.Items {
		collector := newResourceCollector(r.client, node.Name, r.containers, r.pollingInterval)
		r.collectors[node.Name] = collector
		collector.Start(ctx)
	}
}

// Stop 停止所有收集器。
func (r *ResourceMonitor) Stop() {
	for _, collector := range r.collectors {
		collector.Stop()
	}
}

// Reset 重置所有收集器。
func (r *ResourceMonitor) Reset() {
	for _, collector := range r.collectors {
		collector.Reset()
	}
}

// LogLatest 将最新的资源使用情况输出到日志。
func (r *ResourceMonitor) LogLatest() {
	summary, err := r.GetLatest()
	if err != nil {
		framework.Logf("%v", err)
	}
	framework.Logf("%s", r.FormatResourceUsage(summary))
}

// FormatResourceUsage 为 LogLatest() 返回格式化后的字符串。
// TODO(oomichi): 待 test/e2e/node/kubelet_perf.go 改为直接使用 LogLatest 后，可将其改为局部函数。
func (r *ResourceMonitor) FormatResourceUsage(s ResourceUsagePerNode) string {
	summary := []string{}
	for node, usage := range s {
		summary = append(summary, formatResourceUsageStats(node, usage))
	}
	return strings.Join(summary, "\n")
}

// GetLatest 返回最新的资源使用情况。
func (r *ResourceMonitor) GetLatest() (ResourceUsagePerNode, error) {
	result := make(ResourceUsagePerNode)
	errs := []error{}
	for key, collector := range r.collectors {
		s, err := collector.GetLatest()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		result[key] = s
	}
	return result, utilerrors.NewAggregate(errs)
}

// GetMasterNodeLatest 返回 master 与 node 的最新资源使用情况。
func (r *ResourceMonitor) GetMasterNodeLatest(usagePerNode ResourceUsagePerNode) ResourceUsagePerNode {
	result := make(ResourceUsagePerNode)
	var masterUsage ResourceUsagePerContainer
	var nodesUsage []ResourceUsagePerContainer
	for node, usage := range usagePerNode {
		if strings.HasSuffix(node, "master") {
			masterUsage = usage
		} else {
			nodesUsage = append(nodesUsage, usage)
		}
	}
	nodeAvgUsage := make(ResourceUsagePerContainer)
	for _, nodeUsage := range nodesUsage {
		for c, usage := range nodeUsage {
			if _, found := nodeAvgUsage[c]; !found {
				nodeAvgUsage[c] = &ContainerResourceUsage{Name: usage.Name}
			}
			nodeAvgUsage[c].CPUUsageInCores += usage.CPUUsageInCores
			nodeAvgUsage[c].MemoryUsageInBytes += usage.MemoryUsageInBytes
			nodeAvgUsage[c].MemoryWorkingSetInBytes += usage.MemoryWorkingSetInBytes
			nodeAvgUsage[c].MemoryRSSInBytes += usage.MemoryRSSInBytes
		}
	}
	for c := range nodeAvgUsage {
		nodeAvgUsage[c].CPUUsageInCores /= float64(len(nodesUsage))
		nodeAvgUsage[c].MemoryUsageInBytes /= uint64(len(nodesUsage))
		nodeAvgUsage[c].MemoryWorkingSetInBytes /= uint64(len(nodesUsage))
		nodeAvgUsage[c].MemoryRSSInBytes /= uint64(len(nodesUsage))
	}
	result["master"] = masterUsage
	result["node"] = nodeAvgUsage
	return result
}

// FormatCPUSummary 根据指定的汇总数据生成便于阅读的 CPU 汇总字符串。
func (r *ResourceMonitor) FormatCPUSummary(summary NodesCPUSummary) string {
	// 单个节点的输出示例（百分位可能不同）：
	// 节点 "e2e-test-foo-node-0vj7" 上各容器的 CPU 用量：
	// container        5th%  50th% 90th% 95th%
	// "/"              0.051 0.159 0.387 0.455
	// "/runtime        0.000 0.000 0.146 0.166
	// "/kubelet"       0.036 0.053 0.091 0.154
	// "/misc"          0.001 0.001 0.001 0.002
	var summaryStrings []string
	var header []string
	header = append(header, "container")
	for _, p := range percentiles {
		header = append(header, fmt.Sprintf("%.0fth%%", p*100))
	}
	for nodeName, containers := range summary {
		buf := &bytes.Buffer{}
		w := tabwriter.NewWriter(buf, 1, 0, 1, ' ', 0)
		fmt.Fprintf(w, "%s\n", strings.Join(header, "\t"))
		for _, containerName := range TargetContainers() {
			var s []string
			s = append(s, fmt.Sprintf("%q", containerName))
			data, ok := containers[containerName]
			for _, p := range percentiles {
				value := "N/A"
				if ok {
					value = fmt.Sprintf("%.3f", data[p])
				}
				s = append(s, value)
			}
			fmt.Fprintf(w, "%s\n", strings.Join(s, "\t"))
		}
		w.Flush()
		summaryStrings = append(summaryStrings, fmt.Sprintf("CPU usage of containers on node %q\n:%s", nodeName, buf.String()))
	}
	return strings.Join(summaryStrings, "\n")
}

// LogCPUSummary 将 CPU 汇总信息输出到日志。
func (r *ResourceMonitor) LogCPUSummary() {
	summary := r.GetCPUSummary()
	framework.Logf("%s", r.FormatCPUSummary(summary))
}

// GetCPUSummary 返回 CPU 汇总信息。
func (r *ResourceMonitor) GetCPUSummary() NodesCPUSummary {
	result := make(NodesCPUSummary)
	for nodeName, collector := range r.collectors {
		result[nodeName] = make(ContainersCPUSummary)
		for _, containerName := range TargetContainers() {
			data := collector.GetBasicCPUStats(containerName)
			result[nodeName][containerName] = data
		}
	}
	return result
}

// GetMasterNodeCPUSummary 返回 master 节点的 CPU 汇总信息。
func (r *ResourceMonitor) GetMasterNodeCPUSummary(summaryPerNode NodesCPUSummary) NodesCPUSummary {
	result := make(NodesCPUSummary)
	var masterSummary ContainersCPUSummary
	var nodesSummaries []ContainersCPUSummary
	for node, summary := range summaryPerNode {
		if strings.HasSuffix(node, "master") {
			masterSummary = summary
		} else {
			nodesSummaries = append(nodesSummaries, summary)
		}
	}

	nodeAvgSummary := make(ContainersCPUSummary)
	for _, nodeSummary := range nodesSummaries {
		for c, summary := range nodeSummary {
			if _, found := nodeAvgSummary[c]; !found {
				nodeAvgSummary[c] = map[float64]float64{}
			}
			for perc, value := range summary {
				nodeAvgSummary[c][perc] += value
			}
		}
	}
	for c := range nodeAvgSummary {
		for perc := range nodeAvgSummary[c] {
			nodeAvgSummary[c][perc] /= float64(len(nodesSummaries))
		}
	}
	result["master"] = masterSummary
	result["node"] = nodeAvgSummary
	return result
}
