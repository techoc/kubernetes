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
If seccomp profiles are specified, only runtime default and localhost profiles are allowed.

v1.0 - v1.18:
**Restricted Fields:**
metadata.annotations['seccomp.security.alpha.kubernetes.io/pod']
metadata.annotations['container.seccomp.security.alpha.kubernetes.io/*']

**Allowed Values:** 'runtime/default', 'docker/default', 'localhost/*', undefined

v1.19+:
**Restricted Fields:**
spec.securityContext.seccompProfile.type
spec.containers[*].securityContext.seccompProfile.type
spec.initContainers[*].securityContext.seccompProfile.type

**Allowed Values:** 'RuntimeDefault', 'Localhost', undefined
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

// CheckSeccompBaseline 提供 baseline 档的 seccomp 检查：
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

// seccompProfileBaseline_1_0 checks baseline policy on seccomp alpha annotation
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

// seccompProfileBaseline_1_19 checks baseline policy on securityContext.seccompProfile field
// 新字段语义与旧注解不同，单独实现一套检查，避免历史行为被悄悄改变。
func seccompProfileBaseline_1_19(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	// things that explicitly set seccompProfile.type to a bad value
	// 分别记录 Pod 级和容器级的违规来源，报错时用户能直接定位该改哪一层。
	var badSetters []string
	badValues := sets.NewString()

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.SeccompProfile != nil {
		if !validSeccomp(podSpec.SecurityContext.SeccompProfile.Type) {
			badSetters = append(badSetters, "pod")
			badValues.Insert(string(podSpec.SecurityContext.SeccompProfile.Type))
		}
	}

	// containers that explicitly set seccompProfile.type to a bad value
	var explicitlyBadContainers []string

	visitContainers(podSpec, func(c *corev1.Container) {
		if c.SecurityContext != nil && c.SecurityContext.SeccompProfile != nil {
			// container explicitly set seccompProfile
			if !validSeccomp(c.SecurityContext.SeccompProfile.Type) {
				// container explicitly set seccompProfile to a bad value
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
	// pod or containers explicitly set bad seccompProfiles
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
