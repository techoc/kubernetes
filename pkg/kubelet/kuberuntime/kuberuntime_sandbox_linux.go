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

package kuberuntime

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"

	resourcehelper "k8s.io/component-helpers/resource"
)

// convertOverheadToLinuxResources 把 Pod overhead 转换为 Linux 容器资源；
// overhead 视为 guaranteed 资源（requests == limits）。
func (m *kubeGenericRuntimeManager) convertOverheadToLinuxResources(pod *v1.Pod) *runtimeapi.LinuxContainerResources {
	resources := &runtimeapi.LinuxContainerResources{}
	if pod.Spec.Overhead != nil {
		cpu := pod.Spec.Overhead.Cpu()
		memory := pod.Spec.Overhead.Memory()

		// overhead 不区分 requests 与 limits，按 guaranteed 处理，即 requests == limits。
		resources = m.calculateLinuxResources(cpu, cpu, memory, false)
	}

	return resources
}

// calculateSandboxResources 计算 sandbox 需要的 Linux 资源；
// 计算时排除 overhead，并按特性门控决定是否纳入 Pod 级资源。
func (m *kubeGenericRuntimeManager) calculateSandboxResources(ctx context.Context, pod *v1.Pod) *runtimeapi.LinuxContainerResources {
	logger := klog.FromContext(ctx)
	opts := resourcehelper.PodResourcesOptions{
		ExcludeOverhead:                          true,
		SkipPodLevelResources:                    !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
		UseDRANodeAllocatableResourceClaimStatus: utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
	}
	req := resourcehelper.PodRequests(pod, opts)
	lim := resourcehelper.PodLimits(pod, opts)
	var cpuRequest *resource.Quantity
	if _, cpuRequestExists := req[v1.ResourceCPU]; cpuRequestExists {
		cpuRequest = req.Cpu()
	}

	// Pod 独占 CPU 时，sandbox 不再强制 CFS quota。
	disableCPUQuota := utilfeature.DefaultFeatureGate.Enabled(features.DisableCPUQuotaWithExclusiveCPUs) && m.containerManager.PodHasExclusiveCPUs(logger, pod)

	logger.V(5).Info("Enforcing CFS quota", "pod", klog.KObj(pod), "unlimited", disableCPUQuota)
	return m.calculateLinuxResources(cpuRequest, lim.Cpu(), lim.Memory(), disableCPUQuota)
}

// applySandboxResources 把计算出的资源与 overhead 写入 PodSandboxConfig。
func (m *kubeGenericRuntimeManager) applySandboxResources(ctx context.Context, pod *v1.Pod, config *runtimeapi.PodSandboxConfig) error {

	// 非 Linux 运行时没有 Linux 资源字段，直接返回。
	if config.Linux == nil {
		return nil
	}
	config.Linux.Resources = m.calculateSandboxResources(ctx, pod)
	config.Linux.Overhead = m.convertOverheadToLinuxResources(pod)

	return nil
}
