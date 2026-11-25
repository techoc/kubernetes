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
SELinux type 只能取白名单值；禁止设置自定义 SELinux user 或 role，以免绕过容器标签隔离。

**受限字段：**
spec.securityContext.seLinuxOptions.type
spec.containers[*].securityContext.seLinuxOptions.type
spec.initContainers[*].securityContext.seLinuxOptions.type

**允许值：**
未定义/空
container_t
container_init_t
container_kvm_t

**受限字段：**
spec.securityContext.seLinuxOptions.user
spec.containers[*].securityContext.seLinuxOptions.user
spec.initContainers[*].securityContext.seLinuxOptions.user
spec.securityContext.seLinuxOptions.role
spec.containers[*].securityContext.seLinuxOptions.role
spec.initContainers[*].securityContext.seLinuxOptions.role

**允许值：** 未定义/空
*/

// 这里的字段清单既是实现约束也是文档：新增可写 SELinux 字段时，得先决定它在基线里的处置。

func init() {
	addCheck(CheckSELinuxOptions)
}

// CheckSELinuxOptions 返回基线级检查，从 1.0 起限制 seLinuxOptions 的 type、user 和 role。
// 1.31 单列一个版本，是因为那之后才补进 container_engine_t 这个合法 type；
// 老版本仍按旧白名单判，避免历史策略的判定结果被后来的放宽改动改写。
func CheckSELinuxOptions() Check {
	return Check{
		ID:    "seLinuxOptions",
		Level: api.LevelBaseline,
		Versions: []VersionedCheck{
			{
				MinimumVersion: api.MajorMinorVersion(1, 0),
				CheckPod:       seLinuxOptions1_0,
			},
			{
				MinimumVersion: api.MajorMinorVersion(1, 31),
				CheckPod:       seLinuxOptions1_31,
			},
		},
	}
}

var (
	// 空串代表未指定；把白名单写死而不是用「已知的坏值」取反，是为了遇到新 type 时默认拒绝。
	selinuxAllowedTypes1_0  = sets.New("", "container_t", "container_init_t", "container_kvm_t")
	selinuxAllowedTypes1_31 = sets.New("", "container_t", "container_init_t", "container_kvm_t", "container_engine_t")
)

// podMetadata 参数在这里用不上，保留它是为了符合 CheckPod 的统一签名。
func seLinuxOptions1_0(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return seLinuxOptions(podMetadata, podSpec, selinuxAllowedTypes1_0)
}

func seLinuxOptions1_31(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) CheckResult {
	return seLinuxOptions(podMetadata, podSpec, selinuxAllowedTypes1_31)
}

// 把 Pod 级和容器级选项交给同一段逻辑，只是记录来源时分开——报错要指到具体是谁设的。
func seLinuxOptions(podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec, allowedTypes sets.Set[string]) CheckResult {
	var (
		// 设置非法 seLinuxOptions 的来源
		// 用名字而不是数量汇总，用户才能定位到需要改的那个容器。
		badSetters []string

		// 非法 type 值去重集合
		// 用 set 去重，多个容器设同一个非法 type 时只报一次。
		badTypes = sets.NewString()
		// 是否设置了 user；自定义值会绕过容器标签隔离。
		// 只需一个布尔标记：详情里说「不许设」就够了，不必把每个值都列出来。
		setUser = false
		// 是否设置了 role；只需标记，无需列出具体值。
		// 同上，role 的取值本身不是诊断重点。
		setRole = false
	)

	validSELinuxOptions := func(opts *corev1.SELinuxOptions) bool {
		valid := true
		if !allowedTypes.Has(opts.Type) {
			valid = false
			badTypes.Insert(opts.Type)
		}
		if len(opts.User) > 0 {
			valid = false
			setUser = true
		}
		if len(opts.Role) > 0 {
			valid = false
			setRole = true
		}
		return valid
	}

	if podSpec.SecurityContext != nil && podSpec.SecurityContext.SELinuxOptions != nil {
		if !validSELinuxOptions(podSpec.SecurityContext.SELinuxOptions) {
			badSetters = append(badSetters, "pod")
		}
	}

	var badContainers []string
	visitContainers(podSpec, func(container *corev1.Container) {
		if container.SecurityContext != nil && container.SecurityContext.SELinuxOptions != nil {
			if !validSELinuxOptions(container.SecurityContext.SELinuxOptions) {
				badContainers = append(badContainers, container.Name)
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

	if len(badSetters) > 0 {
		// 先列违规字段再列违规值，两段拼起来才能既说明「谁」也说明「错在哪」。
		var badData []string
		if len(badTypes) > 0 {
			badData = append(badData, fmt.Sprintf(
				"%s %s",
				pluralize("type", "types", len(badTypes)),
				joinQuote(badTypes.List()),
			))
		}
		if setUser {
			badData = append(badData, "user may not be set")
		}
		if setRole {
			badData = append(badData, "role may not be set")
		}

		return CheckResult{
			Allowed:         false,
			ForbiddenReason: "seLinuxOptions",
			ForbiddenDetail: fmt.Sprintf(
				`%s set forbidden securityContext.seLinuxOptions: %s`,
				strings.Join(badSetters, " and "),
				strings.Join(badData, "; "),
			),
		}
	}
	return CheckResult{Allowed: true}
}
