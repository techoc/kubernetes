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

package debug

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"

	"k8s.io/kubernetes/test/e2e/framework"
	e2essh "k8s.io/kubernetes/test/e2e/framework/ssh"
	"k8s.io/utils/ptr"
)

// ResourceConstraint 保存资源上限约束。
type ResourceConstraint struct {
	CPUConstraint    float64
	MemoryConstraint uint64
}

// SingleContainerSummary 保存单个容器的资源使用摘要。
type SingleContainerSummary struct {
	Name string
	CPU  float64
	Mem  uint64
}

// ContainerResourceUsage 保存采集到的容器资源使用数据。
type ContainerResourceUsage struct {
	Name                    string
	Timestamp               time.Time
	CPUUsageInCores         float64
	MemoryUsageInBytes      uint64
	MemoryWorkingSetInBytes uint64
	MemoryRSSInBytes        uint64
	// 计算 CPUUsageInCores 时采用的时间区间。
	CPUInterval time.Duration
}

// ResourceUsagePerContainer 是按容器名索引的 ContainerResourceUsage map。
type ResourceUsagePerContainer map[string]*ContainerResourceUsage

// ResourceUsageSummary 保存资源使用摘要。
// 这里的 key 不能用 int，因为 JSON 不接受整数 key。
type ResourceUsageSummary map[string][]SingleContainerSummary

// PrintHumanReadable 以易读格式输出资源使用摘要。
func (s *ResourceUsageSummary) PrintHumanReadable() string {
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 1, 0, 1, ' ', 0)
	for perc, summaries := range *s {
		buf.WriteString(fmt.Sprintf("%v percentile:\n", perc))
		fmt.Fprintf(w, "container\tcpu(cores)\tmemory(MB)\n")
		for _, summary := range summaries {
			fmt.Fprintf(w, "%q\t%.3f\t%.2f\n", summary.Name, summary.CPU, float64(summary.Mem)/(1024*1024))
		}
		w.Flush()
	}
	return buf.String()
}

// PrintJSON 以 JSON 格式输出资源使用摘要。
func (s *ResourceUsageSummary) PrintJSON() string {
	return framework.PrettyPrintJSON(*s)
}

// SummaryKind 返回 ResourceUsageSummary 的类别名。
func (s *ResourceUsageSummary) SummaryKind() string {
	return "ResourceUsageSummary"
}

type uint64arr []uint64

func (a uint64arr) Len() int           { return len(a) }
func (a uint64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64arr) Less(i, j int) bool { return a[i] < a[j] }

type usageDataPerContainer struct {
	cpuData        []float64
	memUseData     []uint64
	memWorkSetData []uint64
}

func computePercentiles(timeSeries []ResourceUsagePerContainer, percentilesToCompute []int) map[int]ResourceUsagePerContainer {
	if len(timeSeries) == 0 {
		return make(map[int]ResourceUsagePerContainer)
	}
	dataMap := make(map[string]*usageDataPerContainer)
	for i := range timeSeries {
		for name, data := range timeSeries[i] {
			if dataMap[name] == nil {
				dataMap[name] = &usageDataPerContainer{
					cpuData:        make([]float64, 0, len(timeSeries)),
					memUseData:     make([]uint64, 0, len(timeSeries)),
					memWorkSetData: make([]uint64, 0, len(timeSeries)),
				}
			}
			dataMap[name].cpuData = append(dataMap[name].cpuData, data.CPUUsageInCores)
			dataMap[name].memUseData = append(dataMap[name].memUseData, data.MemoryUsageInBytes)
			dataMap[name].memWorkSetData = append(dataMap[name].memWorkSetData, data.MemoryWorkingSetInBytes)
		}
	}
	for _, v := range dataMap {
		sort.Float64s(v.cpuData)
		sort.Sort(uint64arr(v.memUseData))
		sort.Sort(uint64arr(v.memWorkSetData))
	}

	result := make(map[int]ResourceUsagePerContainer)
	for _, perc := range percentilesToCompute {
		data := make(ResourceUsagePerContainer)
		for k, v := range dataMap {
			percentileIndex := int(math.Ceil(float64(len(v.cpuData)*perc)/100)) - 1
			data[k] = &ContainerResourceUsage{
				Name:                    k,
				CPUUsageInCores:         v.cpuData[percentileIndex],
				MemoryUsageInBytes:      v.memUseData[percentileIndex],
				MemoryWorkingSetInBytes: v.memWorkSetData[percentileIndex],
			}
		}
		result[perc] = data
	}
	return result
}

func leftMergeData(left, right map[int]ResourceUsagePerContainer) map[int]ResourceUsagePerContainer {
	result := make(map[int]ResourceUsagePerContainer)
	for percentile, data := range left {
		result[percentile] = data
		if _, ok := right[percentile]; !ok {
			continue
		}
		for k, v := range right[percentile] {
			result[percentile][k] = v
		}
	}
	return result
}

type resourceGatherWorker struct {
	c                           clientset.Interface
	nodeName                    string
	wg                          *sync.WaitGroup
	containerIDs                []string
	stopCh                      chan struct{}
	dataSeries                  []ResourceUsagePerContainer
	finished                    bool
	inKubemark                  bool
	resourceDataGatheringPeriod time.Duration
	probeDuration               time.Duration
	printVerboseLogs            bool
}

func (w *resourceGatherWorker) singleProbe(ctx context.Context) {
	data := make(ResourceUsagePerContainer)
	if w.inKubemark {
		kubemarkData := getKubemarkMasterComponentsResourceUsage(ctx)
		if kubemarkData == nil {
			return
		}
		for k, v := range kubemarkData {
			data[k] = &ContainerResourceUsage{
				Name:                    v.Name,
				MemoryWorkingSetInBytes: v.MemoryWorkingSetInBytes,
				CPUUsageInCores:         v.CPUUsageInCores,
			}
		}
	} else {
		nodeUsage, err := getOneTimeResourceUsageOnNode(w.c, w.nodeName, w.probeDuration, func() []string { return w.containerIDs })
		if err != nil {
			framework.Logf("Error while reading data from %v: %v", w.nodeName, err)
			return
		}
		for k, v := range nodeUsage {
			data[k] = v
			if w.printVerboseLogs {
				framework.Logf("Get container %v usage on node %v. CPUUsageInCores: %v, MemoryUsageInBytes: %v, MemoryWorkingSetInBytes: %v", k, w.nodeName, v.CPUUsageInCores, v.MemoryUsageInBytes, v.MemoryWorkingSetInBytes)
			}
		}
	}
	w.dataSeries = append(w.dataSeries, data)
}

// getOneTimeResourceUsageOnNode 查询节点的 /stats/summary 端点，
// 返回过去 cpuInterval 内所有 containerNames 的资源使用情况。
// interval 的允许范围是 2s~120s。注意 interval 和容器数越大，kubelet 的响应
// 可能越大：例如约 20 个容器的 60s 统计约有 1.5MB。不要用频繁的大请求压垮节点。
//
// cadvisor 以纳秒记录累计 CPU 使用量，因此需要两个统计点才能算出区间内的
// CPU 使用量。假设 cadvisor 每秒轮询一次，N 秒区间就需要 N 个统计点。
// 这只是近似值，可能不准确，所以我们也会记录实际用于计算的时间区间
// （依据统计点的时间戳，见 ContainerResourceUsage.CPUInterval）。
//
// containerNames 返回用户关心的容器名集合。
func getOneTimeResourceUsageOnNode(
	c clientset.Interface,
	nodeName string,
	cpuInterval time.Duration,
	containerNames func() []string,
) (ResourceUsagePerContainer, error) {
	const (
		// cadvisor 大约每秒记录一次统计。
		cadvisorStatsPollingIntervalInSeconds float64 = 1.0
		// cadvisor 最多缓存 2 分钟的统计（由 kubelet 配置）。
		maxNumStatsToRequest int = 120
	)

	numStats := int(float64(cpuInterval.Seconds()) / cadvisorStatsPollingIntervalInSeconds)
	if numStats < 2 || numStats > maxNumStatsToRequest {
		return nil, fmt.Errorf("numStats needs to be > 1 and < %d", maxNumStatsToRequest)
	}
	// 获取节点上所有容器的信息。
	summary, err := getStatsSummary(c, nodeName)
	if err != nil {
		return nil, err
	}

	f := func(name string, newStats *kubeletstatsv1alpha1.ContainerStats) *ContainerResourceUsage {
		if newStats == nil || newStats.CPU == nil || newStats.Memory == nil {
			return nil
		}
		return &ContainerResourceUsage{
			Name:                    name,
			Timestamp:               newStats.StartTime.Time,
			CPUUsageInCores:         float64(ptr.Deref(newStats.CPU.UsageNanoCores, 0)) / 1000000000,
			MemoryUsageInBytes:      ptr.Deref(newStats.Memory.UsageBytes, 0),
			MemoryWorkingSetInBytes: ptr.Deref(newStats.Memory.WorkingSetBytes, 0),
			MemoryRSSInBytes:        ptr.Deref(newStats.Memory.RSSBytes, 0),
			CPUInterval:             0,
		}
	}
	// 只处理我们关心的容器信息。
	containers := containerNames()
	usageMap := make(ResourceUsagePerContainer, len(containers))
	for _, pod := range summary.Pods {
		for _, container := range pod.Containers {
			isInteresting := false
			for _, interestingContainerName := range containers {
				if container.Name == interestingContainerName {
					isInteresting = true
					break
				}
			}
			if !isInteresting {
				continue
			}
			if usage := f(pod.PodRef.Name+"/"+container.Name, &container); usage != nil {
				usageMap[pod.PodRef.Name+"/"+container.Name] = usage
			}
		}
	}
	return usageMap, nil
}

// getStatsSummary 向 kubelet 请求容器信息。
func getStatsSummary(c clientset.Interface, nodeName string) (*kubeletstatsv1alpha1.Summary, error) {
	ctx, cancel := context.WithTimeout(context.Background(), framework.SingleCallTimeout)
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

func (w *resourceGatherWorker) gather(ctx context.Context, initialSleep time.Duration) {
	defer utilruntime.HandleCrash()
	defer w.wg.Done()
	defer framework.Logf("Closing worker for %v", w.nodeName)
	defer func() { w.finished = true }()
	select {
	case <-time.After(initialSleep):
		w.singleProbe(ctx)
		for {
			select {
			case <-time.After(w.resourceDataGatheringPeriod):
				w.singleProbe(ctx)
			case <-ctx.Done():
				return
			case <-w.stopCh:
				return
			}
		}
	case <-ctx.Done():
		return
	case <-w.stopCh:
		return
	}
}

// ContainerResourceGatherer 负责采集容器资源使用数据。
type ContainerResourceGatherer struct {
	client       clientset.Interface
	stopCh       chan struct{}
	workers      []resourceGatherWorker
	workerWg     sync.WaitGroup
	containerIDs []string
	options      ResourceGathererOptions
}

// ResourceGathererOptions 保存资源采集的选项。
type ResourceGathererOptions struct {
	InKubemark                  bool
	Nodes                       NodesSet
	ResourceDataGatheringPeriod time.Duration
	ProbeDuration               time.Duration
	PrintVerboseLogs            bool
}

// NodesSet 表示要采集的节点集合。
type NodesSet int

const (
	// AllNodes 表示所有节点上的所有容器。
	AllNodes NodesSet = 0
	// MasterNodes 表示只采集 Master 节点上的所有容器。
	MasterNodes NodesSet = 1
	// MasterAndDNSNodes 表示 Master 节点上的所有容器以及其他节点上的 DNS 容器。
	MasterAndDNSNodes NodesSet = 2
)

// nodeHasControlPlanePods 在指定节点上运行着 control plane pod
// （kube-scheduler 和/或 kube-controller-manager）时返回 true。
func nodeHasControlPlanePods(ctx context.Context, c clientset.Interface, nodeName string) (bool, error) {
	regKubeScheduler := regexp.MustCompile("kube-scheduler-.*")
	regKubeControllerManager := regexp.MustCompile("kube-controller-manager-.*")

	podList, err := c.CoreV1().Pods(metav1.NamespaceSystem).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("spec.nodeName", nodeName).String(),
	})
	if err != nil {
		return false, err
	}
	if len(podList.Items) < 1 {
		framework.Logf("Can't find any pods in namespace %s to grab metrics from", metav1.NamespaceSystem)
	}
	for _, pod := range podList.Items {
		if regKubeScheduler.MatchString(pod.Name) || regKubeControllerManager.MatchString(pod.Name) {
			return true, nil
		}
	}
	return false, nil
}

// NewResourceUsageGatherer 返回一个新的 ContainerResourceGatherer。
func NewResourceUsageGatherer(ctx context.Context, c clientset.Interface, options ResourceGathererOptions, pods *v1.PodList) (*ContainerResourceGatherer, error) {
	g := ContainerResourceGatherer{
		client:       c,
		stopCh:       make(chan struct{}),
		containerIDs: make([]string, 0),
		options:      options,
	}

	if options.InKubemark {
		g.workerWg.Add(1)
		g.workers = append(g.workers, resourceGatherWorker{
			inKubemark:                  true,
			stopCh:                      g.stopCh,
			wg:                          &g.workerWg,
			finished:                    false,
			resourceDataGatheringPeriod: options.ResourceDataGatheringPeriod,
			probeDuration:               options.ProbeDuration,
			printVerboseLogs:            options.PrintVerboseLogs,
		})
		return &g, nil
	}

	// 没有传入有效 PodList 时，跟踪 kube-system 下的 pod。
	var err error
	if pods == nil {
		pods, err = c.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{})
		if err != nil {
			framework.Logf("Error while listing Pods: %v", err)
			return nil, err
		}
	}
	dnsNodes := make(map[string]bool)
	for _, pod := range pods.Items {
		if options.Nodes == MasterNodes {
			isControlPlane, err := nodeHasControlPlanePods(ctx, c, pod.Spec.NodeName)
			if err != nil {
				return nil, err
			}
			if !isControlPlane {
				continue
			}
		}
		if options.Nodes == MasterAndDNSNodes {
			isControlPlane, err := nodeHasControlPlanePods(ctx, c, pod.Spec.NodeName)
			if err != nil {
				return nil, err
			}
			if !isControlPlane && pod.Labels["k8s-app"] != "kube-dns" {
				continue
			}
		}
		for _, container := range pod.Status.InitContainerStatuses {
			g.containerIDs = append(g.containerIDs, container.Name)
		}
		for _, container := range pod.Status.ContainerStatuses {
			g.containerIDs = append(g.containerIDs, container.Name)
		}
		if options.Nodes == MasterAndDNSNodes {
			dnsNodes[pod.Spec.NodeName] = true
		}
	}
	nodeList, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		framework.Logf("Error while listing Nodes: %v", err)
		return nil, err
	}

	for _, node := range nodeList.Items {
		isControlPlane, err := nodeHasControlPlanePods(ctx, c, node.Name)
		if err != nil {
			return nil, err
		}
		if options.Nodes == AllNodes || isControlPlane || dnsNodes[node.Name] {
			g.workerWg.Add(1)
			g.workers = append(g.workers, resourceGatherWorker{
				c:                           c,
				nodeName:                    node.Name,
				wg:                          &g.workerWg,
				containerIDs:                g.containerIDs,
				stopCh:                      g.stopCh,
				finished:                    false,
				inKubemark:                  false,
				resourceDataGatheringPeriod: options.ResourceDataGatheringPeriod,
				probeDuration:               options.ProbeDuration,
				printVerboseLogs:            options.PrintVerboseLogs,
			})
			if options.Nodes == MasterNodes {
				break
			}
		}
	}
	return &g, nil
}

// StartGatheringData 为每个要跟踪的节点启动一个统计采集 worker，
// 并阻塞直到 StopAndSummarize 被调用。
func (g *ContainerResourceGatherer) StartGatheringData(ctx context.Context) {
	if len(g.workers) == 0 {
		return
	}
	delayPeriod := g.options.ResourceDataGatheringPeriod / time.Duration(len(g.workers))
	delay := time.Duration(0)
	for i := range g.workers {
		go g.workers[i].gather(ctx, delay)
		delay += delayPeriod
	}
	g.workerWg.Wait()
}

// StopAndSummarize 停止统计采集 worker，处理采集到的数据，
// 按传入的百分位生成资源摘要并返回。
// 任一百分位的资源使用超出指定约束时返回错误。
func (g *ContainerResourceGatherer) StopAndSummarize(percentiles []int, constraints map[string]ResourceConstraint) (*ResourceUsageSummary, error) {
	close(g.stopCh)
	framework.Logf("Closed stop channel. Waiting for %v workers", len(g.workers))
	finished := make(chan struct{}, 1)
	go func() {
		g.workerWg.Wait()
		finished <- struct{}{}
	}()
	select {
	case <-finished:
		framework.Logf("Waitgroup finished.")
	case <-time.After(2 * time.Minute):
		unfinished := make([]string, 0)
		for i := range g.workers {
			if !g.workers[i].finished {
				unfinished = append(unfinished, g.workers[i].nodeName)
			}
		}
		framework.Logf("Timed out while waiting for waitgroup, some workers failed to finish: %v", unfinished)
	}

	if len(percentiles) == 0 {
		framework.Logf("Warning! Empty percentile list for stopAndPrintData.")
		return &ResourceUsageSummary{}, fmt.Errorf("Failed to get any resource usage data")
	}
	data := make(map[int]ResourceUsagePerContainer)
	for i := range g.workers {
		if g.workers[i].finished {
			stats := computePercentiles(g.workers[i].dataSeries, percentiles)
			data = leftMergeData(stats, data)
		}
	}

	// worker 已停止，现在需要汇总它们保存的数据。
	sortedKeys := []string{}
	for name := range data[percentiles[0]] {
		sortedKeys = append(sortedKeys, name)
	}
	sort.Strings(sortedKeys)
	violatedConstraints := make([]string, 0)
	summary := make(ResourceUsageSummary)
	for _, perc := range percentiles {
		for _, name := range sortedKeys {
			usage := data[perc][name]
			summary[strconv.Itoa(perc)] = append(summary[strconv.Itoa(perc)], SingleContainerSummary{
				Name: name,
				CPU:  usage.CPUUsageInCores,
				Mem:  usage.MemoryWorkingSetInBytes,
			})

			// 校验资源使用的第 99 百分位
			if perc != 99 {
				continue
			}
			// 名称格式为：<pod_name>/<container_name>
			containerName := strings.Split(name, "/")[1]
			constraint, ok := constraints[containerName]
			if !ok {
				continue
			}
			if usage.CPUUsageInCores > constraint.CPUConstraint {
				violatedConstraints = append(
					violatedConstraints,
					fmt.Sprintf("Container %v is using %v/%v CPU",
						name,
						usage.CPUUsageInCores,
						constraint.CPUConstraint,
					),
				)
			}
			if usage.MemoryWorkingSetInBytes > constraint.MemoryConstraint {
				violatedConstraints = append(
					violatedConstraints,
					fmt.Sprintf("Container %v is using %v/%v MB of memory",
						name,
						float64(usage.MemoryWorkingSetInBytes)/(1024*1024),
						float64(constraint.MemoryConstraint)/(1024*1024),
					),
				)
			}
		}
	}
	if len(violatedConstraints) > 0 {
		return &summary, errors.New(strings.Join(violatedConstraints, "\n"))
	}
	return &summary, nil
}

// kubemarkResourceUsage 跟踪 kubemark 的资源使用情况。
type kubemarkResourceUsage struct {
	Name                    string
	MemoryWorkingSetInBytes uint64
	CPUUsageInCores         float64
}

func getMasterUsageByPrefix(ctx context.Context, prefix string) (string, error) {
	sshResult, err := e2essh.SSH(ctx, fmt.Sprintf("ps ax -o %%cpu,rss,command | tail -n +2 | grep %v | sed 's/\\s+/ /g'", prefix), framework.APIAddress()+":22", framework.TestContext.Provider)
	if err != nil {
		return "", err
	}
	return sshResult.Stdout, nil
}

// getKubemarkMasterComponentsResourceUsage 返回 kubemark 的资源使用情况，
// 其中包含每个 pod 名称对应的多组 CPU 和内存使用数据。
func getKubemarkMasterComponentsResourceUsage(ctx context.Context) map[string]*kubemarkResourceUsage {
	result := make(map[string]*kubemarkResourceUsage)
	// 获取 Kubernetes 组件的资源使用情况
	sshResult, err := getMasterUsageByPrefix(ctx, "kube")
	if err != nil {
		framework.Logf("Error when trying to SSH to master machine. Skipping probe. %v", err)
		return nil
	}
	scanner := bufio.NewScanner(strings.NewReader(sshResult))
	for scanner.Scan() {
		var cpu float64
		var mem uint64
		var name string
		fmt.Sscanf(strings.TrimSpace(scanner.Text()), "%f %d /usr/local/bin/kube-%s", &cpu, &mem, &name)
		if name != "" {
			// gatherer 要求 pod_name/container_name 格式
			fullName := name + "/" + name
			result[fullName] = &kubemarkResourceUsage{Name: fullName, MemoryWorkingSetInBytes: mem * 1024, CPUUsageInCores: cpu / 100}
		}
	}
	// 获取 etcd 的资源使用情况
	sshResult, err = getMasterUsageByPrefix(ctx, "bin/etcd")
	if err != nil {
		framework.Logf("Error when trying to SSH to master machine. Skipping probe")
		return nil
	}
	scanner = bufio.NewScanner(strings.NewReader(sshResult))
	for scanner.Scan() {
		var cpu float64
		var mem uint64
		var etcdKind string
		fmt.Sscanf(strings.TrimSpace(scanner.Text()), "%f %d /bin/sh -c /usr/local/bin/etcd", &cpu, &mem)
		dataDirStart := strings.Index(scanner.Text(), "--data-dir")
		if dataDirStart < 0 {
			continue
		}
		fmt.Sscanf(scanner.Text()[dataDirStart:], "--data-dir=/var/%s", &etcdKind)
		if etcdKind != "" {
			// gatherer 要求 pod_name/container_name 格式
			fullName := "etcd/" + etcdKind
			result[fullName] = &kubemarkResourceUsage{Name: fullName, MemoryWorkingSetInBytes: mem * 1024, CPUUsageInCores: cpu / 100}
		}
	}
	return result
}
