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

package test

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/pod-security-admission/api"
)

/*
说明：字段夹具共用 seccomp profile 常量和构造 helper；检查实现按版本读取对应配置。
*/

// 本文件只注册 1.19 起的字段形式，旧注解由基线夹具覆盖。
func init() {
	fixtureData_restricted_1_19 := fixtureGenerator{
		expectErrorSubstring: "seccompProfile",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 先把各层 securityContext 补齐，才能稳定地设置字段值。
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = seccompProfileLocalhost("testing")
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = nil
					p.Spec.Containers[0].SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
					p.Spec.InitContainers[0].SecurityContext.SeccompProfile = seccompProfileLocalhost("testing")
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// restricted 拒绝 Unconfined；Pod 与容器均未设置时也没有可继承的有效配置。
				// 以下失败样例覆盖配置缺失和 Unconfined。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = nil
				}),
				// Pod 级显式使用 Unconfined。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = seccompProfileUnconfined
				}),
				// init 容器未显式设置。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = nil
					p.Spec.Containers[0].SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
				}),
				// 普通容器未显式设置。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = nil
					p.Spec.InitContainers[0].SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
				}),
				// init 容器显式使用 Unconfined。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = nil
					p.Spec.Containers[0].SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
					p.Spec.InitContainers[0].SecurityContext.SeccompProfile = seccompProfileUnconfined
				}),
			}
		},
	}

	// seccomp 字段从 v1.19 才引入，因此夹具从该版本起注册。
	registerFixtureGenerator(
		fixtureKey{level: api.LevelRestricted, version: api.MajorMinorVersion(1, 19), check: "seccompProfile_restricted"},
		fixtureData_restricted_1_19,
	)
}
