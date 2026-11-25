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

package policy

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/pod-security-admission/api"
)

/*

默认的 /proc 掩码用于缩小攻击面，基线策略必须保留；
但用户命名空间（"hostUsers: false"）已隔离宿主，因此不再强制要求。

**受限字段：**
spec.containers[*].securityContext.procMount
spec.initContainers[*].securityContext.procMount

**允许值：** 未定义/null、"Default" (or "hostUsers" 为 false 时允许任意值)
*/

func init() {
	addCheck(CheckProcMountBaseline)
}

// CheckProcMountBaseline 返回基线级检查，从 1.0 起要求 securityContext.procMount 使用 DefaultProcMount。
// 从 1.35 起，用户命名空间 Pod（"hostUsers: false"）允许任意取值。
// 限制 procMount 可收窄 /proc 的攻击面；用户命名空间隔离了宿主，故 1.35 起对其放行。
func CheckProcMountBaseline() Check {
	return Check{
		ID:    "procMount",
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       procMount_1_0,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 35),
				CheckPod:       procMount1_35baseline,
			},
		},
	}
}

// procMount_1_0 无条件阻止未掩码的 procMount，保持旧版本行为。
// 此处不区分命名空间，因为旧版本检查必须与历史行为保持一致。
func procMount_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	var badContainers []string
	forbiddenProcMountTypes := sets.NewString()
	visitContainers(podSpec, func(container *corev1.Container) {
		// securityContext 为空时继承运行时默认设置，予以放行。
		if container.SecurityContext == nil {
			return
		}
		// 未设置 procMount 时沿用默认掩码，予以放行。
		if container.SecurityContext.ProcMount == nil {
			return
		}
		// 已设置时只接受 Default，其余值可能暴露未掩码的 /proc。
		if *container.SecurityContext.ProcMount != corev1.DefaultProcMount {
			badContainers = append(badContainers, container.Name)
			forbiddenProcMountTypes.Insert(string(*container.SecurityContext.ProcMount))
		}
	})
	if len(badContainers) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "procMount",
			ForbiddenDetail: fmt.Sprintf(
				"%s %s must not set securityContext.procMount to %s",
				pluralize("container", "containers", len(badContainers)),
				joinQuote(badContainers),
				joinQuote(forbiddenProcMountTypes.List()),
			),
		}
	}
	return CheckResult{Allowed: true}
}

// procMount1_35baseline 仅对未使用用户命名空间的 Pod 阻止未掩码 procMount。
// 作为新版本检查，先识别用户命名空间再复用旧逻辑，避免维护两份相同的容器遍历代码。
func procMount1_35baseline(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	if relaxPolicyForUserNamespacePod(podSpec) {
		return CheckResult{Allowed: true}
	}
	// 未使用用户命名空间时沿用受限口径，避免误放行。
	return procMount_1_0(podMetadata, podSpec)
}
