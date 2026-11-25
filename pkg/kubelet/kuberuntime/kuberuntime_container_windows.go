//go:build windows

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

package kuberuntime

import (
	"context"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
	"k8s.io/kubernetes/pkg/securitycontext"
)

// applyPlatformSpecificContainerConfig 为 ContainerConfig 填充 Windows 平台配置。
func (m *kubeGenericRuntimeManager) applyPlatformSpecificContainerConfig(ctx context.Context, config *runtimeapi.ContainerConfig, container *v1.Container, pod *v1.Pod, uid *int64, username string, _ *kubecontainer.ContainerID) error {
	windowsConfig, err := m.generateWindowsContainerConfig(ctx, container, pod, uid, username)
	if err != nil {
		return err
	}
	config.Windows = windowsConfig

	return nil
}

// generateContainerResources 生成 Windows 平台的容器资源配置。
func (m *kubeGenericRuntimeManager) generateContainerResources(ctx context.Context, pod *v1.Pod, container *v1.Container) *runtimeapi.ContainerResources {
	return &runtimeapi.ContainerResources{
		Windows: m.generateWindowsContainerResources(ctx, pod, container),
	}
}

// generateUpdatePodSandboxResourcesRequest 在 Windows 上返回 nil（不支持该请求）。
func (m *kubeGenericRuntimeManager) generateUpdatePodSandboxResourcesRequest(sandboxID string, pod *v1.Pod, podResources *cm.ResourceConfig) *runtimeapi.UpdatePodSandboxResourcesRequest {
	return nil
}

// generateWindowsContainerResources 根据容器 limits 生成 Windows 容器资源配置。
func (m *kubeGenericRuntimeManager) generateWindowsContainerResources(ctx context.Context, pod *v1.Pod, container *v1.Container) *runtimeapi.WindowsContainerResources {
	wcr := m.calculateWindowsResources(ctx, container.Resources.Limits.Cpu(), container.Resources.Limits.Memory())

	return wcr
}

// calculateWindowsResources 根据 CPU/内存 limits 构造 WindowsContainerResources。
func (m *kubeGenericRuntimeManager) calculateWindowsResources(ctx context.Context, cpuLimit, memoryLimit *resource.Quantity) *runtimeapi.WindowsContainerResources {
	resources := runtimeapi.WindowsContainerResources{}

	// 内存限制直接使用 limit 的字节数。
	memLimit := memoryLimit.Value()

	// CPU 未设置时不生成 CpuMaximum，避免覆盖默认行为。
	if !cpuLimit.IsZero() {
		// 由于 Kubernetes API 只有 limits/reserves 而没有权重概念，只设置 CpuMaximum
		// 更符合用户意图；早期曾设置 CpuWeights，结果导致 limits 失效。
		//
		// 该机制分三部分：
		// 一、Windows 内核：进程隔离通过 JOB_OBJECT_CPU_RATE_CONTROL_ENABLE 应用 CPU 限制，
		// WEIGHT_BASED 与 HARD_CAP 互斥。cpuMaximum 的取值范围还与 CPU 数相关。
		// https://docs.microsoft.com/en-us/virtualization/windowscontainers/manage-containers/resource-controls
		// https://docs.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-jobobject_cpu_rate_control_information
		// 二、Docker：同时传入 CpuWeight 与 CpuMaximum 时会启用 WEIGHT_BASED 并忽略 CpuMaximum。
		// 方案一的 HostConfig.CpuPercent 使用整百分比，分辨率随核数变化；方案二的
		// HostConfig.NanoCpus 使用 10e-9 CPU 单位，Moby 会缩放到 1-10000，分辨率更高。
		// https://docs.docker.com/engine/api/v1.30
		// https://github.com/moby/moby/blob/10866714412aea1bb587d1ad14b2ce1ba4cf4308/daemon/oci_windows.go#L426
		// 三、CRI 与 containerd：Linux 直接写 cgroup，Windows 需经 CRI 传递。
		// cpu_maximum 范围为百分数*100（1-10000）；同时设置 CpuWeight 与 CpuMaximum 会被 containerd 拒绝。
		// https://github.com/kubernetes/kubernetes/blob/56d1c3b96d0a544130a82caad33dd57629b8a7f8/staging/src/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto#L681-L682
		// https://github.com/opencontainers/runtime-spec/blob/ad53dcdc39f1f7f7472b10aa0a45648fe4865496/config-windows.md#cpu
		// 把 CPU limit 换算为 Windows 的 1-10000 范围。
		resources.CpuMaximum = calculateCPUMaximum(cpuLimit, int64(winstats.ProcessorCount()))
	}

	// Windows Server 容器的处理器资源控制互斥，优先级为 CPUCount 高于 CPUMaximum。
	// 两者同时存在时清空 CpuMaximum 并记录日志。
	if resources.CpuCount > 0 {
		if resources.CpuMaximum > 0 {
			resources.CpuMaximum = 0
			logger := klog.FromContext(ctx)
			logger.Info("Mutually exclusive options: CPUCount priority > CPUMaximum priority on Windows Server Containers. CPUMaximum should be ignored")
		}
	}

	// 内存限制非零时才设置。
	if memLimit != 0 {
		resources.MemoryLimitInBytes = memLimit
	}

	return &resources
}

// generateWindowsContainerConfig 生成 kubelet runtime v1 的 Windows 容器配置，
// 参见 https://github.com/kubernetes/community/blob/master/contributors/design-proposals/node/cri-windows.md。
func (m *kubeGenericRuntimeManager) generateWindowsContainerConfig(ctx context.Context, container *v1.Container, pod *v1.Pod, uid *int64, username string) (*runtimeapi.WindowsContainerConfig, error) {
	wc := &runtimeapi.WindowsContainerConfig{
		Resources:       m.generateWindowsContainerResources(ctx, pod, container),
		SecurityContext: &runtimeapi.WindowsContainerSecurityContext{},
	}

	// 设置安全上下文。
	effectiveSc := securitycontext.DetermineEffectiveSecurityContext(pod, container)

	if username != "" {
		wc.SecurityContext.RunAsUsername = username
	}
	if effectiveSc.WindowsOptions != nil &&
		effectiveSc.WindowsOptions.GMSACredentialSpec != nil {
		wc.SecurityContext.CredentialSpec = *effectiveSc.WindowsOptions.GMSACredentialSpec
	}

	// WindowsOptions 中的用户名优先于传入的用户名。
	if effectiveSc.WindowsOptions != nil && effectiveSc.WindowsOptions.RunAsUserName != nil {
		wc.SecurityContext.RunAsUsername = *effectiveSc.WindowsOptions.RunAsUserName
	}

	if securitycontext.HasWindowsHostProcessRequest(pod, container) {
		wc.SecurityContext.HostProcess = true
	}

	return wc, nil
}

// calculateCPUMaximum 根据 CPU 限制与 CPU 数计算最大值，并裁剪到 [1,10000]。
func calculateCPUMaximum(cpuLimit *resource.Quantity, cpuCount int64) int64 {
	cpuMaximum := 10 * cpuLimit.MilliValue() / cpuCount

	// 保证 cpuMaximum 落在 [1, 10000]。
	if cpuMaximum < 1 {
		cpuMaximum = 1
	} else if cpuMaximum > 10000 {
		cpuMaximum = 10000
	}
	return cpuMaximum
}

// toKubeContainerResources 把运行时返回的 Windows 资源换算回 kubelet 的容器资源。
func toKubeContainerResources(statusResources *runtimeapi.ContainerResources) *kubecontainer.ContainerResources {
	var cStatusResources *kubecontainer.ContainerResources
	runtimeStatusResources := statusResources.GetWindows()
	if runtimeStatusResources != nil {
		var memLimit, cpuLimit *resource.Quantity

		// 使用 calculateCPUMaximum 的逆公式。
		if runtimeStatusResources.CpuMaximum > 0 {
			cpuLimitValue := runtimeStatusResources.CpuMaximum * int64(winstats.ProcessorCount()) / 10
			cpuLimit = resource.NewMilliQuantity(cpuLimitValue, resource.DecimalSI)
		}

		if runtimeStatusResources.MemoryLimitInBytes > 0 {
			memLimit = resource.NewQuantity(runtimeStatusResources.MemoryLimitInBytes, resource.BinarySI)
		}

		if cpuLimit != nil || memLimit != nil {
			cStatusResources = &kubecontainer.ContainerResources{
				CPULimit:    cpuLimit,
				MemoryLimit: memLimit,
			}
		}
	}
	return cStatusResources
}

// toKubeContainerUser 在 Windows 上返回 nil（不支持）。
func toKubeContainerUser(statusUser *runtimeapi.ContainerUser) *kubecontainer.ContainerUser {
	return nil
}

// GetContainerSwapBehavior 在 Windows 上始终返回 NoSwap。
func (m *kubeGenericRuntimeManager) GetContainerSwapBehavior(pod *v1.Pod, container *v1.Container) types.SwapBehavior {
	return types.NoSwap
}

// initSwapControllerAvailabilityCheck 返回始终为 false 的函数（Windows 不支持 swap 控制器）。
func initSwapControllerAvailabilityCheck(ctx context.Context) func() bool {
	return func() bool { return false }
}

// isMemoryQoSEnforced 在 Windows 上始终返回 false。
func (m *kubeGenericRuntimeManager) isMemoryQoSEnforced() bool {
	return false
}

// applyPodLevelMemoryHigh 在 Windows 上是空操作。
func (m *kubeGenericRuntimeManager) applyPodLevelMemoryHigh(_ *v1.Pod, _ *cm.ResourceConfig) {
}
