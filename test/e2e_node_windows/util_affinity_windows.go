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
	"math/bits"
	"strings"

	v1 "k8s.io/api/core/v1"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// containerIDForContainer 返回 pod 中 ctnName 的运行时容器 ID，
// 去掉运行时 scheme 前缀（"containerd://"）。
func containerIDForContainer(pod *v1.Pod, ctnName string) (string, error) {
	for _, cs := range append(pod.Status.ContainerStatuses, pod.Status.InitContainerStatuses...) {
		if cs.Name != ctnName {
			continue
		}
		_, id, found := strings.Cut(cs.ContainerID, "://")
		if !found {
			return "", fmt.Errorf("unsupported containerID format: %q", cs.ContainerID)
		}
		return id, nil
	}
	return "", fmt.Errorf("container %q not found in pod %s/%s status", ctnName, pod.Namespace, pod.Name)
}

// getWindowsContainerCPUAffinity 向 CRI 查询当前
// 指定容器上设置的 CPU group affinity。未设置 affinity 时返回 nil。
func getWindowsContainerCPUAffinity(
	ctx context.Context,
	criClient internalapi.RuntimeService,
	pod *v1.Pod,
	ctnName string,
) ([]*runtimeapi.WindowsCpuGroupAffinity, error) {
	cntID, err := containerIDForContainer(pod, ctnName)
	if err != nil {
		return nil, err
	}

	resp, err := criClient.ContainerStatus(ctx, cntID, false)
	if err != nil {
		return nil, fmt.Errorf("ContainerStatus(%q): %w", cntID, err)
	}
	if resp == nil || resp.Status == nil || resp.Status.Resources == nil || resp.Status.Resources.Windows == nil {
		return nil, nil
	}
	return resp.Status.Resources.Windows.AffinityCpus, nil
}

// countCPUsInAffinities 统计所有 WindowsCpuGroupAffinity 条目中
// 掩码置位比特的总数，返回逻辑 CPU 总数。
func countCPUsInAffinities(affinities []*runtimeapi.WindowsCpuGroupAffinity) int {
	total := 0
	for _, a := range affinities {
		total += bits.OnesCount64(a.CpuMask)
	}
	return total
}

// 两组 CPU group affinity 至少共享一个 CPU 时
// （同一 processor group 中同一比特置位），windowsAffinitiesOverlap 返回 true。
func windowsAffinitiesOverlap(a, b []*runtimeapi.WindowsCpuGroupAffinity) bool {
	maskA := make(map[uint32]uint64, len(a))
	for _, aff := range a {
		maskA[aff.CpuGroup] |= aff.CpuMask
	}
	for _, aff := range b {
		if maskA[aff.CpuGroup]&aff.CpuMask != 0 {
			return true
		}
	}
	return false
}
