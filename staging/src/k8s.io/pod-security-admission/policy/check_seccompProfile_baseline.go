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
如果指定 seccomp profile，只允许运行时默认 profile 和 localhost profile。

v1.0 - v1.18：
**受限字段：**
metadata.annotations['seccomp.security.alpha.kubernetes.io/pod']
metadata.annotations['container.seccomp.security.alpha.kubernetes.io/*']

**允许值：** 'runtime/default', 'docker/default', 'localhost/*', undefined

v1.19+：
**受限字段：**
spec.securityContext.seccompProfile.type
spec.containers[*].securityContext.seccompProfile.type
spec.initContainers[*].securityContext.seccompProfile.type

**允许值：** 'RuntimeDefault'、'Localhost'、未定义
*/
// 两个注解键分别对应 Pod 级和容器级设置；前缀拼接容器名即可定位对应容器的配置。
const (
	annotationKeyPod             = "seccomp.security.alpha.kubernetes.io/pod"
	annotationKeyContainerPrefix = "container.seccomp.security.alpha.kubernetes.io/"

	checkSeccompBaselineID CheckID = "seccompProfile_baseline"
)

func init() {
	// 在包初始化时注册，构造 Evaluator 时无需调用方显式列出每一项检查。
	addCheck(CheckSeccompBaseline)
}

// CheckSeccompBaseline 返回基线级 seccomp 检查：
// v1.19 前看注解，之后看 securityContext.seccompProfile 字段。
func CheckSeccompBaseline() Check {
	return Check{
		ID:    checkSeccompBaselineID,
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       seccompProfileBaseline_1_0,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 19),
				CheckPod:       seccompProfileBaseline_1_19,
			},
		},
	}
}

// validSeccomp 只放行两种不会绕过隔离的类型：localhost 和 runtime/default。
// Unconfined 会让容器拿到过多系统调用权限，必须拒绝。
func validSeccomp(t corev1.SeccompProfileType) bool {
	return t == corev1.SeccompProfileTypeLocalhost ||
		t == corev1.SeccompProfileTypeRuntimeDefault
}

// validSeccompAnnotationValue 放行新旧两种默认档取值以及 localhost 前缀，
// 保留 docker/default 是为了兼容 v1.19 之前按旧约定写配置的用户。
func validSeccompAnnotationValue(v string) bool {
	return v == corev1.SeccompProfileRuntimeDefault ||
		v == corev1.DeprecatedSeccompProfileDockerDefault ||
		strings.HasPrefix(v, corev1.SeccompLocalhostProfileNamePrefix)
}

// seccompProfileBaseline_1_0 按基线策略检查 seccomp Alpha 注解。
// 只检查显式设置的注解；没写注解由运行时自行决定，不算违规。
func seccompProfileBaseline_1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	forbidden := sets.NewString()

	if val, ok := podMetadata.Annotations[annotationKeyPod]; ok {
		if !validSeccompAnnotationValue(val) {
			forbidden.Insert(fmt.Sprintf("%s=%q", annotationKeyPod, val))
		}
	}

	visitContainers(podSpec, func(c *corev1.Container) {
		annotation := annotationKeyContainerPrefix + c.Name
		if val, ok := podMetadata.Annotations[annotation]; ok {
			if !validSeccompAnnotationValue(val) {
				forbidden.Insert(fmt.Sprintf("%s=%q", annotation, val))
			}
		}
	})

	if len(forbidden) > 0 {
		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "seccompProfile",
			ForbiddenDetail: fmt.Sprintf(
				"forbidden %s %s",
				pluralize("annotation", "annotations", len(forbidden)),
				strings.Join(forbidden.List(), ", "),
			),
		}
	}

	return CheckResult{Allowed: true}
}

// seccompProfileBaseline_1_19 按基线策略检查 securityContext.seccompProfile 字段。
// 新字段语义与旧注解不同，单独实现一套检查，避免历史行为被悄悄改变。
func seccompProfileBaseline_1_19(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// 显式将 seccompProfile.type 设为非法值的来源
	// 分别记录 Pod 级和容器级的违规来源，报错时用户能直接定位该改哪一层。
	var badSetters []string
	badValues := sets.NewString()

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.SeccompProfile != nil {
		if !validSeccomp(podSpec.SecurityContext.SeccompProfile.Type) {
			badSetters = append(badSetters, "pod")
			badValues.Insert(string(podSpec.SecurityContext.SeccompProfile.Type))
		}
	}

	// 显式将 seccompProfile.type 设为非法值的容器
	var explicitlyBadContainers []string

	visitContainers(podSpec, func(c *corev1.Container) {
		if c.SecurityContext != nil && c.SecurityContext.SeccompProfile != nil {
			// 容器显式设置了 seccompProfile
			if !validSeccomp(c.SecurityContext.SeccompProfile.Type) {
				// 容器显式将 seccompProfile 设为非法值
				explicitlyBadContainers = append(explicitlyBadContainers, c.Name)
				badValues.Insert(string(c.SecurityContext.SeccompProfile.Type))
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
	// Pod 或容器显式设置了非法 seccomp profile
	// 同时汇总所有违规值，用户一次就能看清哪些取值不被允许，不必逐个容器试错。
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

	return CheckResult{Allowed: true}
}
