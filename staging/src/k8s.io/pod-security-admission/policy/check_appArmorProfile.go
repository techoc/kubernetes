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
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/pod-security-admission/api"
)

/*
在支持的主机上，运行时默认应用 'runtime/default' AppArmor profile。
基线策略禁止覆盖或禁用默认 profile，只允许替换为明确列出的 profile，
以免工作负载绕过主机的强制访问控制。

**受限字段：**
metadata.annotations['container.apparmor.security.beta.kubernetes.io/*']

**允许值：** 'runtime/default'、'localhost/*'、空值、未定义

**受限字段：**
spec.securityContext.appArmorProfile.type
spec.containers[*].securityContext.appArmorProfile.type
spec.initContainers[*].securityContext.appArmorProfile.type
spec.ephemeralContainers[*].securityContext.appArmorProfile.type

**允许值：** 'RuntimeDefault'、'Localhost'、未定义
*/
func init() {
	// 自动注册到默认检查集合，使基线策略开箱即用地阻止关闭 AppArmor 的配置。
	addCheck(CheckAppArmorProfile)
}

// CheckAppArmorProfile 返回基线级检查，从 1.0 起限制 AppArmor profile 的取值。
// 同时兼容旧的注解写法与新的 appArmorProfile 字段：
// 集群升级期间两种写法可能并存，任一方式设置越界配置都应被拦截。
func CheckAppArmorProfile() Check {
	return Check{
		ID:    "appArmorProfile",
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       appArmorProfile_1_0,
			},
		},
	}
}

// allowedAnnotationValue 判断旧注解形式的 profile 是否安全：
// 空值表示回落到默认 profile，runtime/default 与 localhost/ 前缀都不会关闭内核强制访问控制，
// 其余写法（尤其是 unconfined）可能绕过默认保护。
func allowedAnnotationValue(profile string) bool {
	return len(profile) == 0 ||
		profile == corev1.DeprecatedAppArmorBetaProfileRuntimeDefault ||
		strings.HasPrefix(profile, corev1.DeprecatedAppArmorBetaProfileNamePrefix)
}

// allowedProfileType 只放行 RuntimeDefault 与 Localhost：
// Unconfined 会主动关闭 AppArmor，与基线“不得禁用默认 profile”的目标直接冲突。
func allowedProfileType(profile corev1.AppArmorProfileType) bool {
	switch profile {
	case corev1.AppArmorProfileTypeRuntimeDefault,
		corev1.AppArmorProfileTypeLocalhost:
		return true
	default:
		return false
	}
}

// appArmorProfile_1_0 把 Pod 级、容器级字段与旧注解汇总到一次检查里。
// 注解按 key 排序后再输出，保证同一份 Pod 的拒绝详情稳定一致，便于审计与比对。
func appArmorProfile_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	var badSetters []string // 显式将 appArmorProfile.type 设为非法值的来源
	badValues := sets.NewString()

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.AppArmorProfile != nil {
		if !allowedProfileType(podSpec.SecurityContext.AppArmorProfile.Type) {
			badSetters = append(badSetters, "pod")
			badValues.Insert(string(podSpec.SecurityContext.AppArmorProfile.Type))
		}
	}

	var badContainers []string // 将 appArmorProfile.type 设为非法值的容器
	visitContainers(podSpec, func(c *corev1.Container) {
		if c.SecurityContext != nil && c.SecurityContext.AppArmorProfile != nil {
			if !allowedProfileType(c.SecurityContext.AppArmorProfile.Type) {
				badContainers = append(badContainers, c.Name)
				badValues.Insert(string(c.SecurityContext.AppArmorProfile.Type))
			}
		}
	})

	if len(badContainers) > 0 {
		badSetters = append(
			badSetters,
			fmt.Sprintf(
				"%s %s",
				pluralize("container", "containers", len(badContainers)),
				joinQuote(badContainers),
			),
		)
	}

	var forbiddenAnnotations []string
	for k, v := range podMetadata.Annotations {
		if strings.HasPrefix(k, corev1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix) && !allowedAnnotationValue(v) {
			forbiddenAnnotations = append(forbiddenAnnotations, fmt.Sprintf("%s=%q", k, v))
		}
	}

	// 注解的 key 是无序 map 遍历得到的，排序后才能给出确定性的拒绝原因，便于测试断言与日志比对。
	badValueList := badValues.List()
	if len(forbiddenAnnotations) > 0 {
		sort.Strings(forbiddenAnnotations)
		badValueList = append(badValueList, forbiddenAnnotations...)
		badSetters = append(badSetters, pluralize("annotation", "annotations", len(forbiddenAnnotations)))
	}

	// Pod 或容器显式设置了非法 appArmor profile
	if len(badSetters) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: pluralize("forbidden AppArmor profile", "forbidden AppArmor profiles", len(badValueList)),
			ForbiddenDetail: fmt.Sprintf(
				"%s must not set AppArmor profile type to %s",
				strings.Join(badSetters, " and "),
				joinQuote(badValueList),
			),
		}
	}

	return CheckResult{Allowed: true}
}
