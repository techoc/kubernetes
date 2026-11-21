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

package cadvisor

import (
	"strings"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// CrioSocketSuffix 是 CRI-O socket 的路径后缀。
	// 请与 github.com/google/cadvisor/tree/master/container/crio/client.go 保持一致。
	// 这里只匹配后缀，因为 /var/run 通常是 /run 的符号链接，
	// 用户可能指定任意一种路径。
	CrioSocketSuffix = "run/crio/crio.sock"
)

// CapacityFromMachineInfo 根据机器信息返回节点资源容量。
func CapacityFromMachineInfo(info *cadvisorapi.MachineInfo) v1.ResourceList {
	c := v1.ResourceList{
		v1.ResourceCPU: *resource.NewMilliQuantity(
			int64(info.NumCores*1000),
			resource.DecimalSI),
		v1.ResourceMemory: *resource.NewQuantity(
			int64(info.MemoryCapacity),
			resource.BinarySI),
	}

	// 若启用了 huge pages，则把它们作为节点上的可调度资源上报。
	// 把每种页大小的 hugepage 作为可调度资源上报。
	for _, hugepagesInfo := range info.HugePages {
		pageSizeBytes := int64(hugepagesInfo.PageSize * 1024)
		hugePagesBytes := pageSizeBytes * int64(hugepagesInfo.NumPages)
		pageSizeQuantity := resource.NewQuantity(pageSizeBytes, resource.BinarySI)
		c[v1helper.HugePageResourceName(*pageSizeQuantity)] = *resource.NewQuantity(hugePagesBytes, resource.BinarySI)
	}

	return c
}

// EphemeralStorageCapacityFromFsInfo 根据文件系统信息返回临时存储容量。
func EphemeralStorageCapacityFromFsInfo(info cadvisorapi.FsInfo) v1.ResourceList {
	c := v1.ResourceList{
		v1.ResourceEphemeralStorage: *resource.NewQuantity(
			int64(info.Capacity),
			resource.DecimalSI),
	}
	return c
}

// UsingLegacyCadvisorStats 判断容器统计是否由 cAdvisor 而非 CRI 提供。
// CRI 集成应通过 CRI 获取容器指标。
// TODO: cri-o relies on cadvisor as a temporary workaround. The code should
// be removed. Related issue:
// https://github.com/kubernetes/kubernetes/issues/51798
func UsingLegacyCadvisorStats(runtimeEndpoint string) bool {
	// 若启用 PodAndContainerStatsFromCRI 特性，则认为用户希望使用 CRI 统计；
	// 该特性开启后不再需要上述临时方案。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI) {
		return false
	}
	// 只有 CRI-O 仍依赖 cAdvisor 统计。
	return strings.HasSuffix(runtimeEndpoint, CrioSocketSuffix)
}
