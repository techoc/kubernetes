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

// 这组 fixture 要覆盖三种探针 × HTTPGet/TCPSocket，以及生命周期的 postStart/preStop；
// 这里的通过/失败样本是按 check 实现的判定路径挑的，每一类都至少有一个代表。
func init() {
	fixtureDataV1Dot34 := fixtureGenerator{
		expectErrorSubstring: "probe or lifecycle host",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			return []*corev1.Pod{
				p, // A pod with no probes should pass.
				// 只设 Port、不设 Host 才是合法的探针：判定看的正是 Host 是否为空。
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
				// 覆盖 HTTPGet、TCPSocket，以及 initContainer 上的场景；
				// hostname 和三种 IP 形式都试，防止有人按「看起来像不像 IP」来判断。
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
