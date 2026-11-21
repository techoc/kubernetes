/*
Copyright 2018 The Kubernetes Authors.

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

package cm

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
)

// 用于跨平台类型检查。
// 编译期类型断言，确保各平台的实现签名一致。
var _ func(int64, int64) int64 = MilliCPUToQuota
var _ func(int64) uint64 = MilliCPUToShares
var _ func(uint64, uint64) bool = CPUSharesEqualAfterV2RoundTrip
var _ func(*v1.Pod, bool, uint64, bool, kubeletconfig.MemoryReservationPolicy) *ResourceConfig = ResourceConfigForPod
var _ func() (*CgroupSubsystems, error) = GetCgroupSubsystems
var _ func(string) ([]int, error) = getCgroupProcs
var _ func(types.UID) string = GetPodCgroupNameSuffix
var _ func(string, bool, string) string = NodeAllocatableRoot
var _ func(klog.Logger, string) (string, error) = GetKubeletContainer

// hardEvictionReservation 根据硬驱逐阈值计算需要预留的资源列表。
func hardEvictionReservation(thresholds []evictionapi.Threshold, capacity v1.ResourceList) v1.ResourceList {
	if len(thresholds) == 0 {
		return nil
	}
	// 只处理 LessThan 类型的信号。
	ret := v1.ResourceList{}
	for _, threshold := range thresholds {
		if threshold.Operator != evictionapi.OpLessThan {
			continue
		}
		// 目前只支持内存与节点文件系统可用量两类阈值。
		switch threshold.Signal {
		case evictionapi.SignalMemoryAvailable:
			memoryCapacity := capacity[v1.ResourceMemory]
			value := evictionapi.GetThresholdQuantity(threshold.Value, &memoryCapacity)
			ret[v1.ResourceMemory] = *value
		case evictionapi.SignalNodeFsAvailable:
			storageCapacity := capacity[v1.ResourceEphemeralStorage]
			value := evictionapi.GetThresholdQuantity(threshold.Value, &storageCapacity)
			ret[v1.ResourceEphemeralStorage] = *value
		}
	}
	return ret
}

// buildContainerMapAndRunningSetFromRuntime 从运行时恢复容器映射与运行中容器集合。
func buildContainerMapAndRunningSetFromRuntime(ctx context.Context, runtimeService internalapi.RuntimeService) (containermap.ContainerMap, sets.Set[string]) {
	logger := klog.FromContext(ctx)
	podSandboxMap := make(map[string]string)
	// 忽略错误：这里只要求尽力恢复状态。
	podSandboxList, _ := runtimeService.ListPodSandbox(ctx, nil)
	for _, p := range podSandboxList {
		podSandboxMap[p.Id] = p.Metadata.Uid
	}

	runningSet := sets.New[string]()
	containerMap := containermap.NewContainerMap()
	containerList, _ := runtimeService.ListContainers(ctx, nil)
	// 只处理能找到所属 sandbox 的容器。
	for _, c := range containerList {
		if _, exists := podSandboxMap[c.PodSandboxId]; !exists {
			logger.Info("No PodSandBox found for the container", "podSandboxId", c.PodSandboxId, "containerName", c.Metadata.Name, "containerId", c.Id)
			continue
		}
		podUID := podSandboxMap[c.PodSandboxId]
		containerMap.Add(podUID, c.Metadata.Name, c.Id)
		// 记录运行中的容器 ID。
		if c.State == runtimeapi.ContainerState_CONTAINER_RUNNING {
			logger.V(4).Info("Container reported running", "podSandboxId", c.PodSandboxId, "podUID", podUID, "containerName", c.Metadata.Name, "containerId", c.Id)
			runningSet.Insert(c.Id)
		}
	}
	return containerMap, runningSet
}
