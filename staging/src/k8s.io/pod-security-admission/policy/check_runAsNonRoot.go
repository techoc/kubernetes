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
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

/*
容器必须以非 root 用户运行；用户命名空间（"hostUsers: false"）已隔离宿主，故可例外。

**受限字段：**

spec.securityContext.runAsNonRoot
spec.containers[*].securityContext.runAsNonRoot
spec.initContainers[*].securityContext.runAsNonRoot

**允许值：**
true
Pod 级设为 true 时，容器级可为未定义/null
"hostUsers" 为 false 时允许任意值
*/
// 本文件定义 runAsNonRoot 检查；1.35 起对启用 user namespace 的 Pod 放宽，其余沿用 1.0 逻辑。

func init() {
	addCheck(CheckRunAsNonRoot)
}

// CheckRunAsNonRoot 返回受限级检查，从 1.0 起要求 runAsNonRoot=true。
// 版本列表按时间递增，求值时选择不晚于策略版本的最新实现。
func CheckRunAsNonRoot() Check {
	return Check{
		ID:    "runAsNonRoot",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       runAsNonRoot1_0,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 35),
				CheckPod:       runAsNonRoot1_35,
			},
		},
	}
}

func runAsNonRoot1_35(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 参见 KEP-127： https://github.com/kubernetes/enhancements/blob/308ba8d/keps/sig-node/127-user-namespaces/README.md?plain=1#L411-L447
	// 1.0 策略中的这项放宽曾受永久 Alpha 特性门控约束。
	// 旧版本策略保持不变，只在新版本放开，避免追溯改写已固化的判定。
	if relaxPolicyForUserNamespacePod(podSpec) {
		return CheckResult{Allowed: true}
	}

	return runAsNonRoot1_0(podMetadata, podSpec)
}

// 显式填 false 与仅未填写要分开报：前者是明确违规，后者需提示补齐设置。
func runAsNonRoot1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 显式设置 runAsNonRoot=false 的来源
	var badSetters []string

	podRunAsNonRoot := false
	if podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsNonRoot != nil {
		if !*podSpec.SecurityContext.RunAsNonRoot {
			badSetters = append(badSetters, "pod")
		} else {
			podRunAsNonRoot = true
		}
	}

	// 显式设置 runAsNonRoot=false 的容器
	var explicitlyBadContainers []string
	// 未设置 runAsNonRoot 且没有 Pod 级 true 可继承的容器
	// 容器未填时允许继承 Pod 级设置，但显式 false 不能被 Pod 级 true 覆盖。
	var implicitlyBadContainers []string

	visitContainers(podSpec, func(container *corev1.Container) {
		if container.SecurityContext != nil && container.SecurityContext.RunAsNonRoot != nil {
			// 容器显式设置了 runAsNonRoot
			if !*container.SecurityContext.RunAsNonRoot {
				// 容器显式将 runAsNonRoot 设为非法值
				explicitlyBadContainers = append(explicitlyBadContainers, container.Name)
			}
		} else {
			// 容器未显式设置 runAsNonRoot
			if !podRunAsNonRoot {
				// 没有 Pod 级 runAsNonRoot=true，容器继承不到有效设置
				implicitlyBadContainers = append(implicitlyBadContainers, container.Name)
			}
		}
	})

	if len(explicitlyBadContainers) > 0 {
		badSetters = append(
			badSetters,
			fmt.Sprintf(
				"%s %s",
				pluralize("container", "containers", len(explicitlyBadContainers)),
				joinQuote(explicitlyBadContainers),
			),
		)
	}
	// Pod 或容器显式设置了 runAsNonRoot=false
	// 显式错误优先报出，避免用户只补齐遗漏项后仍被提示。
	if len(badSetters) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "runAsNonRoot != true",
			ForbiddenDetail: fmt.Sprintf("%s must not set securityContext.runAsNonRoot=false", strings.Join(badSetters, " and ")),
		}
	}

	// Pod 未设置 runAsNonRoot，且并非所有容器都显式开启
	// 既没有 Pod 级兜底、容器自身也没开启，需提示至少一处显式设为 true。
	if len(implicitlyBadContainers) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "runAsNonRoot != true",
			ForbiddenDetail: fmt.Sprintf(
				"pod or %s %s must set securityContext.runAsNonRoot=true",
				pluralize("container", "containers", len(implicitlyBadContainers)),
				joinQuote(implicitlyBadContainers),
			),
		}
	}

	return CheckResult{Allowed: true}
}
