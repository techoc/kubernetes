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
TODO: 将字段路径纳入反射式单元测试。

containerFields: []string{
	`securityContext.capabilities.add`,
},
*/

// ensureCapabilities 为 Pod、普通容器和 init 容器补齐 capabilities 对象。
// 夹具只修改 add 字段，提前补齐结构可让后续 tweak 无需层层判空。
func ensureCapabilities(p *corev1.Pod) *corev1.Pod {
	p = ensureSecurityContext(p)
	for i := range p.Spec.Containers {
		if p.Spec.Containers[i].SecurityContext.Capabilities == nil {
			p.Spec.Containers[i].SecurityContext.Capabilities = &corev1.Capabilities{}
		}
	}
	for i := range p.Spec.InitContainers {
		if p.Spec.InitContainers[i].SecurityContext.Capabilities == nil {
			p.Spec.InitContainers[i].SecurityContext.Capabilities = &corev1.Capabilities{}
		}
	}
	return p
}

// 基线只允许一组受控的 capability，这里用完整的通过集合与多个失败变体
// 分别验证白名单本身、init 容器覆盖，以及大小写/CAP_ 前缀等别名写法不能绕过检查。
func init() {
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "capabilities",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 最小合法 Pod 已 drop ALL 时，任何新增 capability 都可能被拒绝，不再构造通过夹具。
			if p.Spec.Containers[0].SecurityContext != nil && p.Spec.Containers[0].SecurityContext.Capabilities != nil {
				for _, capability := range p.Spec.Containers[0].SecurityContext.Capabilities.Drop {
					if capability == corev1.Capability("ALL") {
						return nil
					}
				}
			}

			p = ensureCapabilities(p)
			return []*corev1.Pod{
				// 一次加回全部允许能力，普通容器与 init 容器都覆盖。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{
						"AUDIT_WRITE", "CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL", "MKNOD", "NET_BIND_SERVICE", "SETFCAP", "SETGID", "SETPCAP", "SETUID", "SYS_CHROOT",
					}
					p.Spec.InitContainers[0].SecurityContext.Capabilities.Add = []corev1.Capability{
						"AUDIT_WRITE", "CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID", "KILL", "MKNOD", "NET_BIND_SERVICE", "SETFCAP", "SETGID", "SETPCAP", "SETUID", "SYS_CHROOT",
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureCapabilities(p)
			return []*corev1.Pod{
				// 不在基线白名单中的能力。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"NET_RAW"}
				}),
				// init 容器也不能新增白名单外的能力。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.InitContainers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"NET_RAW"}
				}),
				// 能力名大小写敏感，小写形式不能当作合法能力放行。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"chown"}
				}),
				// 白名单按裸能力名匹配，CAP_ 前缀别名不能绕过检查。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{"CAP_CHOWN"}
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "capabilities_baseline"},
		fixtureData_1_0,
	)
}
