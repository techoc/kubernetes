/*
Copyright 2014 The Kubernetes Authors.

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

package securitycontext

import (
	"fmt"
	"os"
	"sync"

	v1 "k8s.io/api/core/v1"
)

// HasWindowsHostProcessRequest 判断容器是否应作为 HostProcess 容器运行。
// HostProcess 是 Windows 上的一种特殊容器模式，容器与主机共享命名空间。
// 需要检查多层 nil，因为 WindowsOptions 和 HostProcess 字段都是可选的。
func HasWindowsHostProcessRequest(pod *v1.Pod, container *v1.Container) bool {
	effectiveSc := DetermineEffectiveSecurityContext(pod, container)

	if effectiveSc.WindowsOptions == nil {
		return false
	}
	if effectiveSc.WindowsOptions.HostProcess == nil {
		return false
	}
	return *effectiveSc.WindowsOptions.HostProcess
}

// DetermineEffectiveSecurityContext 计算容器的有效安全上下文。
// 合并 Pod 和 Container 级别的配置，Container 的字段优先级更高。
// 返回新的 SecurityContext 对象，不修改原始对象。
// 设计原因：Kubelet 和容器运行时需要一个完整的安全上下文视图，
// 而不是分别处理 Pod 和 Container 级别的部分配置。
func DetermineEffectiveSecurityContext(pod *v1.Pod, container *v1.Container) *v1.SecurityContext {
	effectiveSc := securityContextFromPodSecurityContext(pod)
	containerSc := container.SecurityContext

	if effectiveSc == nil && containerSc == nil {
		return &v1.SecurityContext{}
	}
	if effectiveSc != nil && containerSc == nil {
		return effectiveSc
	}
	if effectiveSc == nil && containerSc != nil {
		return containerSc
	}

	if containerSc.SELinuxOptions != nil {
		effectiveSc.SELinuxOptions = new(v1.SELinuxOptions)
		*effectiveSc.SELinuxOptions = *containerSc.SELinuxOptions
	}

	if containerSc.WindowsOptions != nil {
		// 仅覆盖 Container 级别设置的字段，而不是整个 WindowsOptions 结构体。
		// 因为 Pod 级别可能设置了某些字段（如 GMSACredentialSpec），
		// Container 级别只设置了其他字段（如 RunAsUserName），需要保留两者的值。
		if effectiveSc.WindowsOptions == nil {
			effectiveSc.WindowsOptions = &v1.WindowsSecurityContextOptions{}
		}
		if containerSc.WindowsOptions.GMSACredentialSpecName != nil || containerSc.WindowsOptions.GMSACredentialSpec != nil {
			// both GMSA fields go hand in hand
			effectiveSc.WindowsOptions.GMSACredentialSpecName = containerSc.WindowsOptions.GMSACredentialSpecName
			effectiveSc.WindowsOptions.GMSACredentialSpec = containerSc.WindowsOptions.GMSACredentialSpec
		}
		if containerSc.WindowsOptions.RunAsUserName != nil {
			effectiveSc.WindowsOptions.RunAsUserName = containerSc.WindowsOptions.RunAsUserName
		}
		if containerSc.WindowsOptions.HostProcess != nil {
			effectiveSc.WindowsOptions.HostProcess = containerSc.WindowsOptions.HostProcess
		}
	}

	if containerSc.Capabilities != nil {
		effectiveSc.Capabilities = new(v1.Capabilities)
		*effectiveSc.Capabilities = *containerSc.Capabilities
	}

	if containerSc.Privileged != nil {
		effectiveSc.Privileged = new(bool)
		*effectiveSc.Privileged = *containerSc.Privileged
	}

	if containerSc.RunAsUser != nil {
		effectiveSc.RunAsUser = new(int64)
		*effectiveSc.RunAsUser = *containerSc.RunAsUser
	}

	if containerSc.RunAsGroup != nil {
		effectiveSc.RunAsGroup = new(int64)
		*effectiveSc.RunAsGroup = *containerSc.RunAsGroup
	}

	if containerSc.RunAsNonRoot != nil {
		effectiveSc.RunAsNonRoot = new(bool)
		*effectiveSc.RunAsNonRoot = *containerSc.RunAsNonRoot
	}

	if containerSc.ReadOnlyRootFilesystem != nil {
		effectiveSc.ReadOnlyRootFilesystem = new(bool)
		*effectiveSc.ReadOnlyRootFilesystem = *containerSc.ReadOnlyRootFilesystem
	}

	if containerSc.AllowPrivilegeEscalation != nil {
		effectiveSc.AllowPrivilegeEscalation = new(bool)
		*effectiveSc.AllowPrivilegeEscalation = *containerSc.AllowPrivilegeEscalation
	}

	if containerSc.ProcMount != nil {
		effectiveSc.ProcMount = new(v1.ProcMountType)
		*effectiveSc.ProcMount = *containerSc.ProcMount
	}

	return effectiveSc
}

// DetermineEffectiveRunAsUser 返回有效的 RunAsUser UID 和是否存在标志。
// Container 的 RunAsUser 优先级高于 Pod。如果两者都未设置，返回 (nil, false)。
// 比 DetermineEffectiveSecurityContext 更轻量，仅关注 RunAsUser 字段。
func DetermineEffectiveRunAsUser(pod *v1.Pod, container *v1.Container) (*int64, bool) {
	var runAsUser *int64
	if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.RunAsUser != nil {
		runAsUser = new(int64)
		*runAsUser = *pod.Spec.SecurityContext.RunAsUser
	}
	if container.SecurityContext != nil && container.SecurityContext.RunAsUser != nil {
		runAsUser = new(int64)
		*runAsUser = *container.SecurityContext.RunAsUser
	}
	if runAsUser == nil {
		return nil, false
	}
	return runAsUser, true
}

// securityContextFromPodSecurityContext 将 PodSecurityContext 转换为 SecurityContext 格式。
// 仅复制 Pod 级别会继承到 Container 的字段（SELinuxOptions, WindowsOptions, RunAsUser, RunAsGroup, RunAsNonRoot）。
// 其他 Pod 特有字段（如 FSGroup, SupplementalGroups）不会复制到 Container 的安全上下文中。
func securityContextFromPodSecurityContext(pod *v1.Pod) *v1.SecurityContext {
	if pod.Spec.SecurityContext == nil {
		return nil
	}

	synthesized := &v1.SecurityContext{}

	if pod.Spec.SecurityContext.SELinuxOptions != nil {
		synthesized.SELinuxOptions = &v1.SELinuxOptions{}
		*synthesized.SELinuxOptions = *pod.Spec.SecurityContext.SELinuxOptions
	}

	if pod.Spec.SecurityContext.WindowsOptions != nil {
		synthesized.WindowsOptions = &v1.WindowsSecurityContextOptions{}
		*synthesized.WindowsOptions = *pod.Spec.SecurityContext.WindowsOptions
	}

	if pod.Spec.SecurityContext.RunAsUser != nil {
		synthesized.RunAsUser = new(int64)
		*synthesized.RunAsUser = *pod.Spec.SecurityContext.RunAsUser
	}

	if pod.Spec.SecurityContext.RunAsGroup != nil {
		synthesized.RunAsGroup = new(int64)
		*synthesized.RunAsGroup = *pod.Spec.SecurityContext.RunAsGroup
	}

	if pod.Spec.SecurityContext.RunAsNonRoot != nil {
		synthesized.RunAsNonRoot = new(bool)
		*synthesized.RunAsNonRoot = *pod.Spec.SecurityContext.RunAsNonRoot
	}

	return synthesized
}

// AddNoNewPrivileges 判断是否应添加 no_new_privs 安全选项。
// 当 AllowPrivilegeEscalation 明确设置为 false 时，返回 true。
// no_new_privs 防止进程通过 execve 获取新的权限，是安全最佳实践。
func AddNoNewPrivileges(sc *v1.SecurityContext) bool {
	if sc == nil {
		return false
	}

	// 用户未设置 AllowPrivilegeEscalation 时，不强制添加 no_new_privs。
	// 保持默认行为，由容器运行时决定。
	if sc.AllowPrivilegeEscalation == nil {
		return false
	}

	// AllowPrivilegeEscalation 为 false 时，需要 no_new_privs 来阻止权限提升。
	// 为 true 时，不需要 no_new_privs。
	return !*sc.AllowPrivilegeEscalation
}

var (
	// defaultMaskedPaths 和 defaultReadonlyPaths 定义 /proc 的默认掩码路径。
	// 必须与 Docker (moby/moby) 保持同步，确保 Kubernetes 容器与 Docker 容器的安全行为一致。
	// 参考：https://github.com/moby/moby/blob/ecb03c4cdae6f323150fc11b303dcc5dc4d82416/oci/defaults.go#L190-L218
	// MaskedPaths 将指定路径替换为空只读文件，ReadonlyPaths 将指定路径设为只读挂载。
	// 使用 sync.OnceValue 延迟初始化，仅在需要时解析 CPU thermal_throttle 路径。
	defaultMaskedPaths = sync.OnceValue(func() []string {
		maskedPaths := []string{
			"/proc/asound",
			"/proc/acpi",
			"/proc/interrupts",
			"/proc/kcore",
			"/proc/keys",
			"/proc/latency_stats",
			"/proc/timer_list",
			"/proc/timer_stats",
			"/proc/sched_debug",
			"/proc/scsi",
			"/sys/firmware",
			"/sys/devices/virtual/powercap",
		}

		// 动态添加每个 CPU 核心的 thermal_throttle 路径。
		// 因为 CPU 数量因主机而异，无法硬编码，需要运行时检测。
		// thermal_throttle 暴露 CPU 节流信息，属于敏感内核数据，需要 mask。
		for _, cpu := range possibleCPUs() {
			path := fmt.Sprintf("/sys/devices/system/cpu/cpu%d/thermal_throttle", cpu)
			if _, err := os.Stat(path); err == nil {
				maskedPaths = append(maskedPaths, path)
			}
		}

		return maskedPaths
	})
	defaultReadonlyPaths = []string{
		"/proc/bus",
		"/proc/fs",
		"/proc/irq",
		"/proc/sys",
		"/proc/sysrq-trigger",
	}
)

// ConvertToRuntimeMaskedPaths 将 ProcMountType 转换为运行时 MaskedPaths。
// 如果 ProcMount 设置为 Unmasked，则返回空切片（不 mask 任何路径）。
// 否则返回默认 mask 路径列表，限制容器对敏感 /proc 路径的访问。
func ConvertToRuntimeMaskedPaths(opt *v1.ProcMountType) []string {
	if opt != nil && *opt == v1.UnmaskedProcMount {
		// Unmasked 模式下不 mask 任何路径，容器可以完全访问 /proc。
		// 仅用于特权场景，如系统调试容器。
		return []string{}
	}

	// 默认模式下，添加所有默认 mask 路径，限制容器对敏感 /proc 路径的访问。
	return defaultMaskedPaths()
}

// ConvertToRuntimeReadonlyPaths 将 ProcMountType 转换为运行时 ReadonlyPaths。
// 与 ConvertToRuntimeMaskedPaths 类似，但控制只读路径而非 mask 路径。
func ConvertToRuntimeReadonlyPaths(opt *v1.ProcMountType) []string {
	if opt != nil && *opt == v1.UnmaskedProcMount {
		// Unmasked 模式下不设置只读路径。
		return []string{}
	}

	// 默认模式下，添加所有默认只读路径。
	return defaultReadonlyPaths
}
