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
	"k8s.io/pod-security-admission/api"
)

/*
Privilege escalation (such as via set-user-ID or set-group-ID file mode) should not be allowed.

**Restricted Fields:**

spec.containers[*].securityContext.allowPrivilegeEscalation
spec.initContainers[*].securityContext.allowPrivilegeEscalation

**Allowed Values:** false
*/
// 本文件检查 allowPrivilegeEscalation；Windows Pod 因接口差异自 1.25 起单独豁免。

func init() {
	addCheck(CheckAllowPrivilegeEscalation)
}

// CheckAllowPrivilegeEscalation returns a restricted level check
// that requires allowPrivilegeEscalation=false in 1.8+
// 新版本复用旧版实现，仅为 Windows 场景增加豁免，避免复制检查逻辑。
func CheckAllowPrivilegeEscalation() Check {
	return Check{
		ID:    "allowPrivilegeEscalation",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				// Field added in 1.8:
				// https://github.com/kubernetes/kubernetes/blob/v1.8.0/staging/src/k8s.io/api/core/v1/types.go#L4797-L4804
				MinimumVersion: api.MajorMinorVersion(1, 8),
				CheckPod:       allowPrivilegeEscalation_1_8,
			},
			{
				// Starting 1.25, windows pods would be exempted from this check using pod.spec.os field when set to windows.
				// 版本化而非直接改旧实现，保证 1.8 策略的历史判定不被改写。
				MinimumVersion: api.MajorMinorVersion(1, 25),
				CheckPod:       allowPrivilegeEscalation_1_25,
			},
		},
	}
}

// 未设置与设置为 true 都视为违规：宽松默认值会放大提权风险，必须显式关掉。
func allowPrivilegeEscalation_1_8(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	var badContainers []string
	visitContainers(podSpec, func(container *corev1.Container) {
		if container.SecurityContext == nil || container.SecurityContext.AllowPrivilegeEscalation == nil || *container.SecurityContext.AllowPrivilegeEscalation {
			badContainers = append(badContainers, container.Name)
		}
	})

	if len(badContainers) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "allowPrivilegeEscalation != false",
			ForbiddenDetail: fmt.Sprintf(
				"%s %s must set securityContext.allowPrivilegeEscalation=false",
				pluralize("container", "containers", len(badContainers)),
				joinQuote(badContainers),
			),
		}
	}
	return CheckResult{Allowed: true}
}

func allowPrivilegeEscalation_1_25(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// Pod API validation would have failed if podOS == Windows and if privilegeEscalation has been set.
	// We can admit the Windows pod even if privilegeEscalation has not been set.
	// Windows 上该字段语义不同，API 校验已挡住显式开启的情况，这里无需重复判定。
	if podSpec.OS != nil && podSpec.OS.Name == corev1.Windows {
		return CheckResult{Allowed: true}
	}
	return allowPrivilegeEscalation_1_8(podMetadata, podSpec)
}
