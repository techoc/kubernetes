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
TODO: 将字段路径纳入反射式单元测试；下列路径留档仍需核对。

containerFields: []string{
	`securityContext.capabilities.add`,
},
*/

// 本文件为 hostPorts 检查生成基准/受限共用的夹具，区分容器端口与主机端口。
func init() {
	// 失败夹具覆盖普通容器、init 容器和混合端口，确保检查不走漏任何一类容器。
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "hostPort",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			return []*corev1.Pod{
				// 只声明容器端口，不占用主机端口。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].Ports = []corev1.ContainerPort{
						{
							ContainerPort: 12345,
						},
					}
					p.Spec.InitContainers[0].Ports = []corev1.ContainerPort{
						{
							ContainerPort: 12346,
						},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			return []*corev1.Pod{
				// 普通容器声明了主机端口。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].Ports = []corev1.ContainerPort{
						{
							ContainerPort: 12345,
							HostPort:      12345,
						},
					}
				}),
				// init 容器同样不能占用主机端口。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.InitContainers[0].Ports = []corev1.ContainerPort{
						{
							ContainerPort: 12346,
							HostPort:      12346,
						},
					}
				}),
				// 主机端口与普通容器端口混合。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].Ports = []corev1.ContainerPort{
						{
							ContainerPort: 12345,
							HostPort:      12345,
						},
						{
							ContainerPort: 12347,
						},
					}
					p.Spec.InitContainers[0].Ports = []corev1.ContainerPort{
						{
							ContainerPort: 12346,
							HostPort:      12346,
						},
						{
							ContainerPort: 12348,
						},
					}
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "hostPorts"},
		fixtureData_1_0,
	)
}
