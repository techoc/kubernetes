/*
Copyright 2024 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/pod-security-admission/api"
)

// 本文件覆盖 HTTPGet/TCPSocket 探针与 postStart 生命周期，并验证 Host 为空或非空时的不同判定。
// 失败样例包含域名、IPv4、IPv6 回环地址及 init 容器，避免实现只按单一地址形态判断。
func init() {
	fixtureDataV1Dot34 := fixtureGenerator{
		expectErrorSubstring: "probe or lifecycle host",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			return []*corev1.Pod{
				p, // 无探针的 Pod 应通过。
				// 探针只设 Port、不设 Host，确保空 Host 不会被误判为访问宿主。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].LivenessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							TCPSocket: &corev1.TCPSocketAction{
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			return []*corev1.Pod{
				// 失败样例覆盖 HTTPGet/TCPSocket、postStart、init 容器，以及域名和多种 IP 写法。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].LivenessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Host: "bad.host",
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
				tweak(p, func(p *corev1.Pod) {
					// 边车式 init 容器要配置 ReadinessProbe，API 要求其重启策略为 Always。
					restartPolicy := corev1.ContainerRestartPolicyAlways
					p.Spec.InitContainers[0].RestartPolicy = &restartPolicy
					p.Spec.InitContainers[0].ReadinessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							TCPSocket: &corev1.TCPSocketAction{
								Host: "8.8.8.8",
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{
						PostStart: &corev1.LifecycleHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Host: "bad.host",
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].LivenessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							HTTPGet: &corev1.HTTPGetAction{
								Host: "127.0.0.1",
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].ReadinessProbe = &corev1.Probe{
						ProbeHandler: corev1.ProbeHandler{
							TCPSocket: &corev1.TCPSocketAction{
								Host: "::1",
								Port: intstr.FromInt32(8080),
							},
						},
					}
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 34), check: "hostProbesAndHostLifecycle"},
		fixtureDataV1Dot34,
	)
}
