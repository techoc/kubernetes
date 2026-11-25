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
TODO: 将字段路径纳入反射式单元测试。

podFields: []string{
	`securityContext.runAsUser`,
},
containerFields: []string{
	`securityContext.runAsUser`,
},

*/

// runAsUser 限制从 v1.23 起生效；显式夹具覆盖 Pod、普通容器和 init 容器的非零 UID 与 UID 0。
func init() {

	fixtureData_1_23 := fixtureGenerator{
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.RunAsUser = ptr.To[int64](1000)
					p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To[int64](1000)
					p.Spec.InitContainers[0].SecurityContext.RunAsUser = ptr.To[int64](1000)
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// Pod 级显式设置为 UID 0。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.RunAsUser = ptr.To[int64](0) }),
				// 普通容器显式设置为 UID 0。
				tweak(p, func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.RunAsUser = ptr.To[int64](0) }),
				tweak(p, func(p *corev1.Pod) { p.Spec.InitContainers[0].SecurityContext.RunAsUser = ptr.To[int64](0) }),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelRestricted, version: api.MajorMinorVersion(1, 23), check: "runAsUser"},
		fixtureData_1_23,
	)
}
