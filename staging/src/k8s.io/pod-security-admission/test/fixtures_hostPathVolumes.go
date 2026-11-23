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
TODO: include field paths in reflect-based unit test
*/

// 本文件为已弃用的 hostPathVolumes 基线检查保留夹具。
// 最小合法 Pod 不含任何 hostPath，因此通过夹具无需额外构造；
// 失败夹具同时覆盖混合卷与纯 hostPath 两种写法，防止只在单一形态下命中规则。
func init() {

	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "hostPath",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// minimal valid pod already captures all valid combinations
			// 最小合法 Pod 本身无卷，已覆盖“不使用 hostPath”这一唯一合法形态，
			// 无需再生成重复夹具。
			return nil
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			return []*corev1.Pod{
				// mix of hostPath and non-hostPath volumes
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{
						{
							Name: "volume-emptydir",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{},
							},
						},
						{
							Name: "volume-hostpath",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/a",
								},
							},
						},
					}
				}),
				// just hostPath volumes
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Volumes = []corev1.Volume{
						{
							Name: "volume-hostpath-a",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/a",
								},
							},
						},
						{
							Name: "volume-hostpath-b",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/b",
								},
							},
						},
					}
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "hostPathVolumes"},
		fixtureData_1_0,
	)
}
