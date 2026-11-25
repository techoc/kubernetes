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
	"k8s.io/utils/ptr"
)

/*
字段路径留档待补进反射式测试；这里覆盖 Pod 与容器两级设置，以及完全未设置的情况。
TODO: 将字段路径纳入反射式单元测试。

podFields: []string{
	`securityContext.runAsNonRoot`,
},
containerFields: []string{
	`securityContext.runAsNonRoot`,
},

*/

// 通过夹具覆盖 Pod 级和所有容器级两种合法设置；未设置或显式 false 都可能以 root 启动，均判失败。
func init() {

	fixtureData_1_0 := fixtureGenerator{
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// Pod 级设置为 true，容器级留空以验证继承。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.RunAsNonRoot = ptr.To(true)
					p.Spec.Containers[0].SecurityContext.RunAsNonRoot = nil
					p.Spec.InitContainers[0].SecurityContext.RunAsNonRoot = nil
				}),
				// Pod 级留空，所有容器显式设置为 true。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.RunAsNonRoot = nil
					p.Spec.Containers[0].SecurityContext.RunAsNonRoot = ptr.To(true)
					p.Spec.InitContainers[0].SecurityContext.RunAsNonRoot = ptr.To(true)
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// 所有层级均未设置。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.RunAsNonRoot = nil
					p.Spec.Containers[0].SecurityContext.RunAsNonRoot = nil
					p.Spec.InitContainers[0].SecurityContext.RunAsNonRoot = nil
				}),
				// Pod 级显式设置为 false。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsNonRoot = ptr.To(false) }),
				// 普通容器显式设置为 false。
				tweak(p, func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsNonRoot = ptr.To(false) }),
				tweak(p, func(p *corev1.Pod) { p.Spec.InitContainers[0].SecurityContext.RunAsNonRoot = ptr.To(false) }),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelRestricted, version: api.MajorMinorVersion(1, 0), check: "runAsNonRoot"},
		fixtureData_1_0,
	)
}
