//go:build !windows

/*
Copyright 2017 The Kubernetes Authors.

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

package cpumanager

import (
	"context"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/utils/cpuset"
)

// updateContainerCPUSet 通过运行时更新容器的 CPU 亲和性；非 Linux/Windows 平台同样走通用运行时接口。
func (m *manager) updateContainerCPUSet(ctx context.Context, containerID string, cpus cpuset.CPUSet) error {
	// TODO：考虑增加一个 `ResourceConfigForContainer` 辅助函数，
	// 建议在 helpers_linux.go 中新增类似 Pod 现有实现的 `ResourceConfigForContainer` 辅助函数；
	// 传入完整容器资源比这里这种补丁式的部分资源更清晰。

	return m.containerRuntime.UpdateContainerResources(
		ctx,
		containerID,
		&runtimeapi.ContainerResources{
			Linux: &runtimeapi.LinuxContainerResources{
				CpusetCpus: cpus.String(),
			},
		})
}
