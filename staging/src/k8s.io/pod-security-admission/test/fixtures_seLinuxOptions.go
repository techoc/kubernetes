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
字段路径清单暂留在注释里：反射式测试还没接上，先记下 type/user/role 的覆盖范围，避免实现时遗漏。
TODO: 将字段路径纳入反射式单元测试。

podFields: []string{
	`spec.securityContext.seLinuxOptions.type`,
	`spec.securityContext.seLinuxOptions.user`,
	`spec.securityContext.seLinuxOptions.role`,
},
containerFields: []string{
	`securityContext.seLinuxOptions.type`,
	`securityContext.seLinuxOptions.user`,
	`securityContext.seLinuxOptions.role`,
},
*/

// type 只允许三个容器域取值，user/role 完全禁止自定义；pass/fail 两组 fixture 分别钉住这两条规则。
func init() {
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "seLinuxOptions",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSELinuxOptions(p)
			return []*corev1.Pod{
				// 安全上下文未设置 SELinux 选项。
				tweak(p, func(p *corev1.Pod) {
					// Pod 级未设置。
					p.Spec.SecurityContext.SELinuxOptions = nil
					// 容器级未设置。
					p.Spec.Containers[0].SecurityContext.SELinuxOptions = nil
					// init 容器保留空选项对象。
					p.Spec.InitContainers[0].SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{}
				}),
				tweak(p, func(p *corev1.Pod) {
					// 三个允许的 type 分别放在不同层级。
					p.Spec.SecurityContext.SELinuxOptions.Type = "container_t"
					p.Spec.Containers[0].SecurityContext.SELinuxOptions.Type = "container_init_t"
					p.Spec.InitContainers[0].SecurityContext.SELinuxOptions.Type = "container_kvm_t"
					// Level 可为空或任意值，不受该检查限制。
					p.Spec.SecurityContext.SELinuxOptions.Level = ""
					p.Spec.Containers[0].SecurityContext.SELinuxOptions.Level = "somevalue"
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSELinuxOptions(p)
			return []*corev1.Pod{
				// Pod 级 type 不在允许集合内。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions.Type = "somevalue" }),
				// 普通容器 type 不在允许集合内。
				tweak(p, func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.SELinuxOptions.Type = "somevalue" }),
				// init 容器 type 不在允许集合内。
				tweak(p, func(p *corev1.Pod) { p.Spec.InitContainers[0].SecurityContext.SELinuxOptions.Type = "somevalue" }),
				// Pod 级 user 不在允许集合内。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions.User = "somevalue" }),
				// Pod 级 role 不在允许集合内。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.SELinuxOptions.Role = "somevalue" }),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "seLinuxOptions"},
		fixtureData_1_0,
	)
}
