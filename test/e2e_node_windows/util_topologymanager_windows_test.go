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
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/cluster/ports"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
)

// nodeID 是 gstruct.MatchAllElements 用于 node 级 kubelet 指标的身份提取器：
// 这些样本没有区分性的 label，因此
// 每个样本映射到相同的（空）key。它对应 Linux e2e_node
// 中同名的 helper。
func nodeID(element interface{}) string {
	return ""
}

// timelessSample 只按值匹配单个 Prometheus 样本，忽略
// 指标 label、时间戳和 histogram。它对应 Linux e2e_node 的 helper，
// 使 Windows topology manager 指标的断言写法一致。
func timelessSample(value interface{}) types.GomegaMatcher {
	return gstruct.PointTo(gstruct.MatchAllFields(gstruct.Fields{
		// 匹配 Id 时已经检查过 Metric 了。
		"Metric":    gstruct.Ignore(),
		"Value":     gomega.BeNumerically("==", value),
		"Timestamp": gstruct.Ignore(),
		"Histogram": gstruct.Ignore(),
	}))
}

// getWindowsKubeletMetrics 从本地节点的只读端口抓取
// kubelet 的 Prometheus 指标。它是 Linux 的 getKubeletMetrics helper
// 的 Windows 对应版本，用作 gomega.Eventually 的轮询函数。
func getWindowsKubeletMetrics(ctx context.Context) (e2emetrics.KubeletMetrics, error) {
	ginkgo.By("Getting Kubelet metrics from the metrics API")
	return e2emetrics.GrabKubeletMetricsWithoutProxy(ctx, fmt.Sprintf("%s:%d", nodeNameOrIP(), ports.KubeletReadOnlyPort), "/metrics")
}

// windowsDetectCPUsPerNUMANode 返回 NUMA 节点编号到该节点逻辑 CPU 数的
// map，数据来自 Windows 内核。它从 0 开始遍历 NUMA
// 节点编号，在第一个报告没有 CPU（或操作系统拒绝）的节点处停止，
// 该节点即为已填充节点的末尾。
func windowsDetectCPUsPerNUMANode() map[int]int {
	perNode := map[int]int{}
	for node := 0; node <= 0xffff; node++ {
		affinities, err := winstats.GetCPUsforNUMANode(uint16(node))
		if err != nil {
			break
		}
		count := 0
		for _, aff := range affinities {
			count += len(aff.Processors())
		}
		if count == 0 {
			break
		}
		perNode[node] = count
	}
	return perNode
}

// buildWindowsMemoryManagerKubeletConfig 返回一个 KubeletConfiguration，
// 同时开启静态 CPU manager（feature gate 打开）和 memory manager 的
// BestEffort 策略。memory manager 启动时校验各 NUMA 节点的
// ReservedMemory 之和等于节点的可分配内存预留
// （SystemReserved + KubeReserved + hard-eviction）；它不关心
// 总量如何分布，所以像 Linux 的 memory_manager_test 一样，把
// 全部预留放在 NUMA 节点 0 上。
func buildWindowsMemoryManagerKubeletConfig(oldCfg *kubeletconfig.KubeletConfiguration) *kubeletconfig.KubeletConfiguration {
	const (
		systemReservedMem = "500Mi"
		kubeReservedMem   = "500Mi"
		evictionHardMem   = "100Mi"
		// 总预留量 = systemReserved + kubeReserved + hard-eviction。
		totalReservedMem = "1100Mi"
	)

	newCfg := buildWindowsCPUManagerKubeletConfig(oldCfg, true)
	newCfg.MemoryManagerPolicy = "BestEffort"
	// 让 topology manager 不参与准入决策，这样 memory
	// manager 的 Allocate 总会运行。如果用 single-numa-node/restricted，
	// topology manager 会在 Allocate 调用前的 hint-merge 阶段
	// 就拒绝无法满足的请求，pinning 计数器永远不会
	// 递增。"none"（和 Linux memory manager metrics 测试一样）让 pod
	// 进入 allocate 阶段，从而覆盖失败路径。
	newCfg.TopologyManagerPolicy = topologymanager.PolicyNone

	if newCfg.SystemReserved == nil {
		newCfg.SystemReserved = map[string]string{}
	}
	newCfg.SystemReserved[string(v1.ResourceMemory)] = systemReservedMem

	if newCfg.KubeReserved == nil {
		newCfg.KubeReserved = map[string]string{}
	}
	newCfg.KubeReserved[string(v1.ResourceMemory)] = kubeReservedMem

	if newCfg.EvictionHard == nil {
		newCfg.EvictionHard = map[string]string{}
	}
	newCfg.EvictionHard["memory.available"] = evictionHardMem

	newCfg.ReservedMemory = []kubeletconfig.MemoryReservation{
		{
			NumaNode: 0,
			Limits: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse(totalReservedMem),
			},
		},
	}
	return newCfg
}

// makeGuaranteedMemoryManagerPodWindows 构建一个 Guaranteed-QoS 的 Windows pod，
// 其中只有一个名为 "gu-ctr" 的容器，其 CPU 和内存 requests 等于
// limits。它用于覆盖 memory manager 的分配路径（例如
// 任何 NUMA 节点都无法满足的超大内存 request）。
func makeGuaranteedMemoryManagerPodWindows(name, cpu, memory string) *v1.Pod {
	rl := v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse(cpu),
		v1.ResourceMemory: resource.MustParse(memory),
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:  "gu-ctr",
					Image: busyboxImage,
					Resources: v1.ResourceRequirements{
						Requests: rl,
						Limits:   rl,
					},
					// 长时间运行的 sleep；powershell.exe 在 Windows BusyBox 镜像中可用。
					Command: []string{"powershell.exe", "-Command", "Start-Sleep -Seconds 86400"},
				},
			},
			NodeSelector: map[string]string{"kubernetes.io/os": "windows"},
		},
	}
}
