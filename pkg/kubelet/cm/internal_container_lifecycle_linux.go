//go:build linux

/*
Copyright 2021 The Kubernetes Authors.

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
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

// PreCreateContainer 在创建容器前把 CPU manager 分配的 CPU 与 memory manager 分配的 NUMA 节点
// 写入容器配置，确保容器创建时即绑定正确的 cpuset。
func (i *internalContainerLifecycleImpl) PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	// CPU 亲和性来自 cpu manager 的静态分配结果，为空表示未分配独占 CPU。
	if i.cpuManager != nil {
		allocatedCPUs := i.cpuManager.GetCPUAffinity(string(pod.UID), container.Name)
		if !allocatedCPUs.IsEmpty() {
			containerConfig.Linux.Resources.CpusetCpus = allocatedCPUs.String()
		}
	}

	// 内存绑定使用 memory manager 计算出的 NUMA 节点集合。
	if i.memoryManager != nil {
		numaNodes := i.memoryManager.GetMemoryNUMANodes(logger, pod, container)
		if numaNodes.Len() > 0 {
			var affinity []string
			for _, numaNode := range sets.List(numaNodes) {
				affinity = append(affinity, strconv.Itoa(numaNode))
			}
			containerConfig.Linux.Resources.CpusetMems = strings.Join(affinity, ",")
		}
	}

	return nil
}
