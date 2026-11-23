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
TODO: include field paths in reflect-based unit test

podFields: []string{
	`securityContext.runAsNonRoot`,
},
containerFields: []string{
	`securityContext.runAsNonRoot`,
},

*/

// 只在 Pod 或所有容器上显式为 true 才算通过；未设置与显式 false 都会让 root 容器有机会启动，故判失败。
func init() {

	fixtureData_1_0 := fixtureGenerator{
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// set at pod level
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.RunAsNonRoot = ptr.To(true)
					p.Spec.Containers[0].SecurityContext.RunAsNonRoot = nil
					p.Spec.InitContainers[0].SecurityContext.RunAsNonRoot = nil
				}),
				// set on all containers
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
				// unset everywhere
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.RunAsNonRoot = nil
					p.Spec.Containers[0].SecurityContext.RunAsNonRoot = nil
					p.Spec.InitContainers[0].SecurityContext.RunAsNonRoot = nil
				}),
				// explicit false on pod
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsNonRoot = ptr.To(false) }),
				// explicit false on containers
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
