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
容器不得设置 runAsUser: 0；用户命名空间（"hostUsers: false"）已隔离宿主，故可例外。

**受限字段：**

spec.securityContext.runAsUser
spec.containers[*].securityContext.runAsUser
spec.initContainers[*].securityContext.runAsUser

**允许值：**
非零值
未定义/null
"hostUsers" 为 false 时允许任意值

*/

func init() {
	// 注册到默认集合，使受限策略默认禁止以 root 身份运行容器。
	addCheck(CheckRunAsUser)
}

// CheckRunAsUser 返回受限级检查，从 1.23 起禁止 runAsUser=0。
// 按版本注册两套实现：1.35 起用户命名空间成为稳定特性，
// 放宽规则不再依赖 Alpha 特性门控，因此需要独立的版本化检查。
func CheckRunAsUser() Check {
	return Check{
		ID:    "runAsUser",
		Level: api.LevelRestricted,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 23),
				CheckPod:       runAsUser1_23,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 35),
				CheckPod:       runAsUser1_35,
			},
		},
	}
}

func runAsUser1_35(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 参见 KEP-127： https://github.com/kubernetes/enhancements/blob/308ba8d/keps/sig-node/127-user-namespaces/README.md?plain=1#L411-L447
	// 1.23 策略中的这项放宽曾受永久 Alpha 特性门控约束。
	// 用户命名空间把容器内 root 映射为节点上的非 root，此时 runAsUser=0 不再意味着节点提权，
	// 所以 1.35 版本无需特性门控即可放行，但仍要复用 1.23 的实现处理非命名空间 Pod。
	if relaxPolicyForUserNamespacePod(podSpec) {
		return CheckResult{Allowed: true}
	}
	return runAsUser1_23(podMetadata, podSpec)

}

// runAsUser1_23 只检查显式设为 0 的情况：
// 未设置字段交由运行时的默认非 root 策略处理，策略本身不替用户做隐式推断。
func runAsUser1_23(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 显式设置 runAsUser=0 的来源
	var badSetters []string

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.RunAsUser != nil && *podSpec.SecurityContext.RunAsUser == 0 {
		badSetters = append(badSetters, "pod")
	}

	// 显式设置 runAsUser=0 的容器
	var explicitlyBadContainers []string

	visitContainers(podSpec, func(container *corev1.Container) {
		if container.SecurityContext != nil && container.SecurityContext.RunAsUser != nil && *container.SecurityContext.RunAsUser == 0 {
			explicitlyBadContainers = append(explicitlyBadContainers, container.Name)
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
	// Pod 或容器显式设置了 runAsUser=0
	// 统一汇总 Pod 级与容器级的违规者，让用户一次看到所有需要修改的位置。
	if len(badSetters) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "runAsUser=0",
			ForbiddenDetail: fmt.Sprintf("%s must not set runAsUser=0", strings.Join(badSetters, " and ")),
		}
	}

	return CheckResult{Allowed: true}
}
