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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/opencontainers/cgroups"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e_node/perftype"

	"github.com/onsi/ginkgo/v2"
)

const (
	// 资源监控
	cadvisorImageName = "gcr.io/cadvisor/cadvisor:v0.47.2"
	cadvisorPodName   = "cadvisor"
	cadvisorPort      = 8090
	// cAdvisor 的 housekeeping 间隔（秒）
	houseKeepingInterval = 1
)

var (
	systemContainers map[string]string
)

// ResourceCollector 定期从 cAdvisor 采集资源使用情况
type ResourceCollector struct {
	pollingInterval time.Duration
	buffers         map[string][]*e2ekubelet.ContainerResourceUsage
	lock            sync.RWMutex
	stopCh          chan struct{}
}

// cadvisorContainerInfo / cadvisorContainerStats 是本采集器从独立 cAdvisor pod 的
// v2 /api/v2.1/stats 响应中读取的最小子集。解码到这些本地类型可以避免
// test/e2e_node 依赖 cAdvisor Go client 及其 v2 API 类型，直接通过 HTTP+JSON 与 pod 通信
type cadvisorContainerInfo struct {
	Stats []*cadvisorContainerStats `json:"stats"`
}

type cadvisorContainerStats struct {
	Timestamp time.Time `json:"timestamp"`
	CPU       struct {
		Usage struct {
			Total uint64 `json:"total"`
		} `json:"usage"`
	} `json:"cpu"`
	Memory struct {
		Usage      uint64 `json:"usage"`
		WorkingSet uint64 `json:"working_set"`
		RSS        uint64 `json:"rss"`
	} `json:"memory"`
}

// cadvisorStats 从独立 cAdvisor pod 的 v2 REST API 获取容器的最新 stats 样本
// （取代 cAdvisor v2 Go client）
func cadvisorStats(name string) (map[string]*cadvisorContainerInfo, error) {
	u := fmt.Sprintf("http://localhost:%d/api/v2.1/stats%s?count=1&type=name&recursive=false", cadvisorPort, name)
	resp, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cadvisor stats for %q: HTTP %d", name, resp.StatusCode)
	}
	out := map[string]*cadvisorContainerInfo{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// NewResourceCollector 创建一个定期从 cAdvisor 采集资源使用情况的采集器
func NewResourceCollector(interval time.Duration) *ResourceCollector {
	buffers := make(map[string][]*e2ekubelet.ContainerResourceUsage)
	return &ResourceCollector{
		pollingInterval: interval,
		buffers:         buffers,
	}
}

// Start 启动采集器并连接独立 cAdvisor pod，随后反复运行 collectStats
func (r *ResourceCollector) Start() {
	// 获取 kubelet 和 runtime 对应的 cgroup 容器名
	kubeletContainer, err1 := getContainerNameForProcess(kubeletProcessName, "")
	runtimeContainer, err2 := getContainerNameForProcess(framework.TestContext.ContainerRuntimeProcessName, framework.TestContext.ContainerRuntimePidFile)
	if err1 == nil && err2 == nil && kubeletContainer != "" && runtimeContainer != "" {
		systemContainers = map[string]string{
			kubeletstatsv1alpha1.SystemContainerKubelet: kubeletContainer,
			kubeletstatsv1alpha1.SystemContainerRuntime: runtimeContainer,
		}
	} else {
		framework.Failf("Failed to get runtime container name in test-e2e-node resource collector.")
	}

	// 等待独立 cAdvisor pod 的 v2 REST API 可访问
	err := wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 1*time.Minute, false, func(context.Context) (bool, error) {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%d/api/v2.1/machine", cadvisorPort))
		if err != nil {
			return false, nil
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK, nil
	})
	framework.ExpectNoError(err, "cadvisor not ready")

	r.stopCh = make(chan struct{})

	oldStatsMap := make(map[string]*cadvisorContainerStats)
	go wait.Until(func() { r.collectStats(oldStatsMap) }, r.pollingInterval, r.stopCh)
}

// Stop 停止采集器采集 stats，但不清空缓冲区
func (r *ResourceCollector) Stop() {
	close(r.stopCh)
}

// Reset 清空采集器的 stats 缓冲区
func (r *ResourceCollector) Reset() {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, name := range systemContainers {
		r.buffers[name] = []*e2ekubelet.ContainerResourceUsage{}
	}
}

// GetCPUSummary 按百分位返回 CPU 使用量
func (r *ResourceCollector) GetCPUSummary() e2ekubelet.ContainersCPUSummary {
	result := make(e2ekubelet.ContainersCPUSummary)
	for key, name := range systemContainers {
		data := r.GetBasicCPUStats(name)
		result[key] = data
	}
	return result
}

// LogLatest 记录最新的资源使用情况
func (r *ResourceCollector) LogLatest() {
	summary, err := r.GetLatest()
	if err != nil {
		framework.Logf("%v", err)
	}
	framework.Logf("%s", formatResourceUsageStats(summary))
}

// collectStats 从 cAdvisor 采集资源使用情况
func (r *ResourceCollector) collectStats(oldStatsMap map[string]*cadvisorContainerStats) {
	for _, name := range systemContainers {
		ret, err := cadvisorStats(name)
		if err != nil {
			framework.Logf("Error getting container stats, err: %v", err)
			return
		}
		cStats, ok := ret[name]
		if !ok || len(cStats.Stats) == 0 {
			framework.Logf("Missing info/stats for container %q", name)
			return
		}

		newStats := cStats.Stats[0]

		if oldStats, ok := oldStatsMap[name]; ok && oldStats.Timestamp.Before(newStats.Timestamp) {
			r.buffers[name] = append(r.buffers[name], computeContainerResourceUsage(name, oldStats, newStats))
		}
		oldStatsMap[name] = newStats
	}
}

// computeContainerResourceUsage 根据新的数据样本计算资源使用量
func computeContainerResourceUsage(name string, oldStats, newStats *cadvisorContainerStats) *e2ekubelet.ContainerResourceUsage {
	return &e2ekubelet.ContainerResourceUsage{
		Name:                    name,
		Timestamp:               newStats.Timestamp,
		CPUUsageInCores:         float64(newStats.CPU.Usage.Total-oldStats.CPU.Usage.Total) / float64(newStats.Timestamp.Sub(oldStats.Timestamp).Nanoseconds()),
		MemoryUsageInBytes:      newStats.Memory.Usage,
		MemoryWorkingSetInBytes: newStats.Memory.WorkingSet,
		MemoryRSSInBytes:        newStats.Memory.RSS,
		CPUInterval:             newStats.Timestamp.Sub(oldStats.Timestamp),
	}
}

// GetLatest 从 stats 缓冲区获取最新的资源使用量
func (r *ResourceCollector) GetLatest() (e2ekubelet.ResourceUsagePerContainer, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()
	resourceUsage := make(e2ekubelet.ResourceUsagePerContainer)
	for key, name := range systemContainers {
		contStats, ok := r.buffers[name]
		if !ok || len(contStats) == 0 {
			return nil, fmt.Errorf("No resource usage data for %s container (%s)", key, name)
		}
		resourceUsage[key] = contStats[len(contStats)-1]
	}
	return resourceUsage, nil
}

type resourceUsageByCPU []*e2ekubelet.ContainerResourceUsage

func (r resourceUsageByCPU) Len() int           { return len(r) }
func (r resourceUsageByCPU) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r resourceUsageByCPU) Less(i, j int) bool { return r[i].CPUUsageInCores < r[j].CPUUsageInCores }

// 要报告的百分位
var percentiles = [...]float64{0.50, 0.90, 0.95, 0.99, 1.00}

// GetBasicCPUStats 返回 containerName 的 CPU 使用量（核数）百分位。
// 该方法检查缓冲区中当前的所有数据
func (r *ResourceCollector) GetBasicCPUStats(containerName string) map[float64]float64 {
	r.lock.RLock()
	defer r.lock.RUnlock()
	result := make(map[float64]float64, len(percentiles))

	// 必须复制数组，否则会改变时间序列的顺序
	usages := make([]*e2ekubelet.ContainerResourceUsage, 0)
	usages = append(usages, r.buffers[containerName]...)

	sort.Sort(resourceUsageByCPU(usages))
	for _, q := range percentiles {
		index := int(float64(len(usages))*q) - 1
		if index < 0 {
			// 数据不足
			result[q] = 0
			continue
		}
		result[q] = usages[index].CPUUsageInCores
	}
	return result
}

func formatResourceUsageStats(containerStats e2ekubelet.ResourceUsagePerContainer) string {
	// Example output:
	//
	// Resource usage:
	//container cpu(cores) memory_working_set(MB) memory_rss(MB)
	//"kubelet" 0.068      27.92                  15.43
	//"runtime" 0.664      89.88                  68.13

	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 1, 0, 1, ' ', 0)
	fmt.Fprintf(w, "container\tcpu(cores)\tmemory_working_set(MB)\tmemory_rss(MB)\n")
	for name, s := range containerStats {
		fmt.Fprintf(w, "%q\t%.3f\t%.2f\t%.2f\n", name, s.CPUUsageInCores, float64(s.MemoryWorkingSetInBytes)/(1024*1024), float64(s.MemoryRSSInBytes)/(1024*1024))
	}
	w.Flush()
	return fmt.Sprintf("Resource usage:\n%s", buf.String())
}

func formatCPUSummary(summary e2ekubelet.ContainersCPUSummary) string {
	// 某个节点的输出示例（百分位可能不同）：
	// CPU usage of containers:
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

	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, 1, 0, 1, ' ', 0)
	fmt.Fprintf(w, "%s\n", strings.Join(header, "\t"))

	for _, containerName := range e2ekubelet.TargetContainers() {
		var s []string
		s = append(s, fmt.Sprintf("%q", containerName))
		data, ok := summary[containerName]
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
	summaryStrings = append(summaryStrings, fmt.Sprintf("CPU usage of containers:\n%s", buf.String()))

	return strings.Join(summaryStrings, "\n")
}

// getCadvisorPod 创建一个独立 cAdvisor pod，用于细粒度资源监控
func getCadvisorPod() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: cadvisorPodName,
		},
		Spec: v1.PodSpec{
			// 使用 host port 供测试采集数据；
			// test-e2e-node 目前无法使用端口映射
			HostNetwork:     true,
			SecurityContext: &v1.PodSecurityContext{},
			Containers: []v1.Container{
				{
					Image: cadvisorImageName,
					Name:  cadvisorPodName,
					Ports: []v1.ContainerPort{
						{
							Name:          "http",
							HostPort:      cadvisorPort,
							ContainerPort: cadvisorPort,
							Protocol:      v1.ProtocolTCP,
						},
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "sys",
							ReadOnly:  true,
							MountPath: "/sys",
						},
						{
							Name:      "var-run",
							ReadOnly:  false,
							MountPath: "/var/run",
						},
						{
							Name:      "docker",
							ReadOnly:  true,
							MountPath: "/var/lib/docker/",
						},
						{
							Name:      "rootfs",
							ReadOnly:  true,
							MountPath: "/rootfs",
						},
					},
					Args: []string{
						"--profiling",
						fmt.Sprintf("--housekeeping_interval=%ds", houseKeepingInterval),
						fmt.Sprintf("--port=%d", cadvisorPort),
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name:         "rootfs",
					VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/"}},
				},
				{
					Name:         "var-run",
					VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/var/run"}},
				},
				{
					Name:         "sys",
					VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/sys"}},
				},
				{
					Name:         "docker",
					VolumeSource: v1.VolumeSource{HostPath: &v1.HostPathVolumeSource{Path: "/var/lib/docker"}},
				},
			},
		},
	}
}

// deletePodsSync 删除一组 pod，并阻塞等待它们消失
func deletePodsSync(ctx context.Context, f *framework.Framework, pods []*v1.Pod) {
	var wg sync.WaitGroup
	for i := range pods {
		pod := pods[i]
		wg.Add(1)
		go func() {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()

			e2epod.NewPodClient(f).DeleteSync(ctx, pod.ObjectMeta.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
		}()
	}
	wg.Wait()
	return
}

// newTestPods 为测试创建一组 pod spec
func newTestPods(numPods int, volume bool, imageName, podType string) []*v1.Pod {
	var pods []*v1.Pod
	for range numPods {
		podName := "test-" + string(uuid.NewUUID())
		labels := map[string]string{
			"type": podType,
			"name": podName,
		}
		if volume {
			pods = append(pods,
				&v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:   podName,
						Labels: labels,
					},
					Spec: v1.PodSpec{
						// 重启策略使用默认的 Always
						Containers: []v1.Container{
							{
								Image: imageName,
								Name:  podName,
								VolumeMounts: []v1.VolumeMount{
									{MountPath: "/test-volume-mnt", Name: podName + "-volume"},
								},
							},
						},
						Volumes: []v1.Volume{
							{Name: podName + "-volume", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
						},
					},
				})
		} else {
			pods = append(pods,
				&v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:   podName,
						Labels: labels,
					},
					Spec: v1.PodSpec{
						// 重启策略使用默认的 Always
						Containers: []v1.Container{
							{
								Image: imageName,
								Name:  podName,
							},
						},
					},
				})
		}

	}
	return pods
}

// GetResourceTimeSeries 获取每个容器资源使用情况的时间序列
func (r *ResourceCollector) GetResourceTimeSeries() map[string]*perftype.ResourceSeries {
	resourceSeries := make(map[string]*perftype.ResourceSeries)
	for key, name := range systemContainers {
		newSeries := &perftype.ResourceSeries{Units: map[string]string{
			"cpu":    "mCPU",
			"memory": "MB",
		}}
		resourceSeries[key] = newSeries
		for _, usage := range r.buffers[name] {
			newSeries.Timestamp = append(newSeries.Timestamp, usage.Timestamp.UnixNano())
			newSeries.CPUUsageInMilliCores = append(newSeries.CPUUsageInMilliCores, int64(usage.CPUUsageInCores*1000))
			newSeries.MemoryRSSInMegaBytes = append(newSeries.MemoryRSSInMegaBytes, int64(float64(usage.MemoryUsageInBytes)/(1024*1024)))
		}
	}
	return resourceSeries
}

const kubeletProcessName = "kubelet"

func getContainerNameForProcess(name, pidFile string) (string, error) {
	pids, err := getPidsForProcess(name, pidFile)
	if err != nil {
		return "", fmt.Errorf("failed to detect process id for %q - %v", name, err)
	}
	if len(pids) == 0 {
		return "", nil
	}
	cont, err := getContainer(pids[0])
	if err != nil {
		return "", err
	}
	return cont, nil
}

// getContainer 返回指定 pid 关联的 cgroup。
// 它要求 memory 和 cpu cgroup 处于统一的层级结构中。
// 在 systemd 环境中，使用指定 pid 的 name=systemd cgroup
func getContainer(pid int) (string, error) {
	cgs, err := cgroups.ParseCgroupFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}

	if cgroups.IsCgroup2UnifiedMode() {
		unified, found := cgs[""]
		if !found {
			return "", cgroups.NewNotFoundError("unified")
		}
		return unified, nil
	}

	cpu, found := cgs["cpu"]
	if !found {
		return "", cgroups.NewNotFoundError("cpu")
	}
	memory, found := cgs["memory"]
	if !found {
		return "", cgroups.NewNotFoundError("memory")
	}

	// 由于要用这个 cgroup 做资源核算，必须确保它是统一的层级结构
	if cpu != memory {
		return "", fmt.Errorf("cpu and memory cgroup hierarchy not unified.  cpu: %s, memory: %s", cpu, memory)
	}

	// 在 systemd 上，每个 pid 都位于统一的 cgroup 层级中（在 systemd-cgls 中显示为 name=systemd）；
	// cpu 和 memory accounting 默认关闭，用户可以按 unit 或全局启用。
	// 可通过 /etc/systemd/system.conf 全局启用（DefaultCPUAccounting=true DefaultMemoryAccounting=true），
	// 也可通过 CPUAccounting=true 和 MemoryAccounting=true 为单个 unit 启用。
	// 这里仅在 CPU 或 memory accounting 未启用时告警，以免破坏在终端启动 kubelet 的本地开发流程。
	// 例如用户会话的 cgroup 类似 /user.slice/user-X.slice/session-X.scope，但在运行 docker 容器的系统上，
	// cpu 和 memory cgroup 是实际执行 accounting 的最近祖先（很可能是 /）。
	// 因此在这些系统上拿不到 kubelet 的 cpu 或 memory accounting 统计。
	// 此外，除非 runtime 所在的 unit（或全局）启用了 accounting，否则也拿不到 runtime 的统计
	if systemd, found := cgs["name=systemd"]; found {
		if systemd != cpu {
			log.Printf("CPUAccounting not enabled for pid: %d", pid)
		}
		if systemd != memory {
			log.Printf("MemoryAccounting not enabled for pid: %d", pid)
		}
		return systemd, nil
	}

	return cpu, nil
}
