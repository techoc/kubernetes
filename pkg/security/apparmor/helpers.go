/*
Copyright 2016 The Kubernetes Authors.

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

// Package apparmor 提供 AppArmor 配置文件的验证和辅助功能。
// AppArmor 是 Linux 内核的安全模块，用于限制进程的能力。
// 该包主要功能：
//   - 验证节点是否支持 AppArmor（内核和构建层面）
//   - 验证 Pod 请求的 AppArmor profile 是否有效
//   - 获取容器应使用的 AppArmor profile（支持优先级：container > annotation > pod）
package apparmor

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
)

// isRequired 检查 Pod 是否需要 AppArmor 支持。
// 当 Pod 或其任何容器指定了非 Unconfined 的 AppArmor profile 时，视为需要 AppArmor。
// 同时兼容已废弃的 annotation 方式（DeprecatedAppArmorBetaContainerAnnotationKeyPrefix）。
func isRequired(pod *v1.Pod) bool {
	if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.AppArmorProfile != nil &&
		pod.Spec.SecurityContext.AppArmorProfile.Type != v1.AppArmorProfileTypeUnconfined {
		return true
	}

	inUse := !podutil.VisitContainers(&pod.Spec, podutil.AllContainers, func(c *v1.Container, _ podutil.ContainerType) bool {
		if c.SecurityContext != nil && c.SecurityContext.AppArmorProfile != nil &&
			c.SecurityContext.AppArmorProfile.Type != v1.AppArmorProfileTypeUnconfined {
			return false // 已在使用 AppArmor，提前终止遍历
		}
		return true
	})
	if inUse {
		return true
	}

	for key, value := range pod.Annotations {
		if strings.HasPrefix(key, v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix) {
			return value != v1.DeprecatedAppArmorBetaProfileNameUnconfined
		}
	}
	return false
}

// GetProfile 返回容器应使用的 AppArmor profile。
// 优先级顺序：container.SecurityContext > deprecated annotation > pod.SecurityContext。
// 这种设计确保容器级别的配置最高优先级，同时兼容静态 Pod（可能未将 annotation 同步到字段）
// 和 Pod 级别的默认 profile。
func GetProfile(pod *v1.Pod, container *v1.Container) *v1.AppArmorProfile {
	if container.SecurityContext != nil && container.SecurityContext.AppArmorProfile != nil {
		return container.SecurityContext.AppArmorProfile
	}

	// 静态 Pod 可能未将 annotation 同步到字段，因此在 Pod profile 之前回退到 annotation。
	// 这是为了兼容静态 Pod 的特殊场景，其 annotation 可能未被控制器同步。
	if profile := getProfileFromPodAnnotations(pod.Annotations, container.Name); profile != nil {
		return profile
	}

	// 最后回退到 Pod 级别的 AppArmor profile 作为默认值。
	if pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.AppArmorProfile != nil {
		return pod.Spec.SecurityContext.AppArmorProfile
	}

	return nil
}

// getProfileFromPodAnnotations 从已废弃的 Pod annotation 中获取容器的 AppArmor profile。
// 支持三种格式：runtime-default、unconfined、localhost/<profile-name>。
// 无效 annotation 返回 nil，由调用方继续尝试其他来源。
func getProfileFromPodAnnotations(annotations map[string]string, containerName string) *v1.AppArmorProfile {
	val, ok := annotations[v1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix+containerName]
	if !ok {
		return nil
	}

	switch {
	case val == v1.DeprecatedAppArmorBetaProfileRuntimeDefault:
		// runtime-default: 使用容器运行时默认的 AppArmor profile。
		return &v1.AppArmorProfile{Type: v1.AppArmorProfileTypeRuntimeDefault}

	case val == v1.DeprecatedAppArmorBetaProfileNameUnconfined:
		// unconfined: 明确禁用 AppArmor，不应用任何 profile。
		return &v1.AppArmorProfile{Type: v1.AppArmorProfileTypeUnconfined}

	case strings.HasPrefix(val, v1.DeprecatedAppArmorBetaProfileNamePrefix):
		// localhost/<profile>: 使用节点上已加载的自定义 profile。
		// 注意：空的 localhost profile 会被 kubelet admission 拒绝，此处不做额外检查。
		profileName := strings.TrimPrefix(val, v1.DeprecatedAppArmorBetaProfileNamePrefix)
		return &v1.AppArmorProfile{
			Type:             v1.AppArmorProfileTypeLocalhost,
			LocalhostProfile: &profileName,
		}

	default:
		// 无效的 annotation 值，返回 nil 让调用方尝试其他来源。
		return nil
	}
}
