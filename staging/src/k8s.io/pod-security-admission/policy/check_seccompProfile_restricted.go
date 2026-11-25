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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/pod-security-admission/api"
)

/*

必须显式指定 seccomp profile，且只允许运行时默认 profile 和 localhost profile。

v1.19+：
**受限字段：**
spec.securityContext.seccompProfile.type
spec.containers[*].securityContext.seccompProfile.type
spec.initContainers[*].securityContext.seccompProfile.type

**允许值：** 'RuntimeDefault', 'Localhost'
注意：Pod 级已指定时，容器级字段可为未定义。

*/
// 本文件把 seccomp 要求做成版本化检查；1.25 分支隔离 Windows Pod，避免误拒。

func init() {
	addCheck(CheckSeccompProfileRestricted)
}

// 1.19 之前 seccompProfile 字段不存在，检查从该版本开始才有意义。
// CheckSeccompProfileRestricted 返回受限级检查，要求显式配置合规的 seccomp profile。
func CheckSeccompProfileRestricted() Check {
	return Check{
		ID:    "seccompProfile_restricted",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				MinimumVersion:   api.MajorMinorVersion(1, 19),
				CheckPod:         seccompProfileRestricted_1_19,
				OverrideCheckIDs: []CheckID{checkSeccompBaselineID},
			},
			// 从 1.25 起，pod.spec.os 为 windows 的 Pod 通过 Windows 分支豁免此项检查。
			{
				MinimumVersion:   api.MajorMinorVersion(1, 25),
				CheckPod:         seccompProfileRestricted_1_25,
				OverrideCheckIDs: []CheckID{checkSeccompBaselineID},
			},
		},
	}
}

// seccompProfileRestricted_1_19 按受限策略检查 securityContext.seccompProfile 字段。
func seccompProfileRestricted_1_19(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 显式将 seccompProfile.type 设为非法值的来源
	// 显式非法值需要列出具体取值，帮助用户定位到要改的字段。
	var badSetters []string
	badValues := sets.NewString()

	podSeccompSet := false

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.SeccompProfile != nil {
		if !validSeccomp(podSpec.SecurityContext.SeccompProfile.Type) {
			badSetters = append(badSetters, "pod")
			badValues.Insert(string(podSpec.SecurityContext.SeccompProfile.Type))
		} else {
			podSeccompSet = true
		}
	}

	// 显式将 seccompProfile.type 设为非法值的容器
	var explicitlyBadContainers []string
	// 未设置 seccompProfile 且没有 Pod 级配置可继承的容器
	var implicitlyBadContainers []string

	visitContainers(podSpec, func(c *corev1.Container) {
		if c.SecurityContext != nil && c.SecurityContext.SeccompProfile != nil {
			// 容器显式设置了 seccompProfile
			if !validSeccomp(c.SecurityContext.SeccompProfile.Type) {
				// 容器显式将 seccompProfile 设为非法值
				explicitlyBadContainers = append(explicitlyBadContainers, c.Name)
				badValues.Insert(string(c.SecurityContext.SeccompProfile.Type))
			}
		} else {
			// 容器未显式设置 seccompProfile
			if !podSeccompSet {
				// 没有有效的 Pod 级 seccompProfile，容器继承不到合规设置
				implicitlyBadContainers = append(implicitlyBadContainers, c.Name)
			}
		}
	})

	// 容器名和 Pod 分别收集，最后拼成一句诊断，避免每个容器单独报一次。
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
	// Pod 或容器显式设置了非法 seccomp profile
	if len(badSetters) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "seccompProfile",
			ForbiddenDetail: fmt.Sprintf(
				"%s must not set securityContext.seccompProfile.type to %s",
				strings.Join(badSetters, " and "),
				joinQuote(badValues.List()),
			),
		}
	}

	// Pod 未设置 seccompProfile，且并非所有容器都显式配置
	// 未显式声明也必须拒绝：restricted 要求明确选择 RuntimeDefault/Localhost，而不是依赖运行时默认行为。
	if len(implicitlyBadContainers) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "seccompProfile",
			ForbiddenDetail: fmt.Sprintf(
				`pod or %s %s must set securityContext.seccompProfile.type to "RuntimeDefault" or "Localhost"`,
				pluralize("container", "containers", len(implicitlyBadContainers)),
				joinQuote(implicitlyBadContainers),
			),
		}
	}

	return CheckResult{Allowed: true}
}

// seccompProfileRestricted_1_25 按受限策略检查 Kubernetes 1.25 及以上版本的
// securityContext.seccompProfile 字段，并豁免 Windows Pod。
func seccompProfileRestricted_1_25(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 若 podOS == Windows 且已设置 seccompProfile，Pod API 校验会先行拒绝；
	// 因而这里可放行未显式设置该字段的 Windows Pod。
	if podSpec.OS != nil && podSpec.OS.Name == corev1.Windows {
		return CheckResult{Allowed: true}
	}
	return seccompProfileRestricted_1_19(podMetadata, podSpec)
}
