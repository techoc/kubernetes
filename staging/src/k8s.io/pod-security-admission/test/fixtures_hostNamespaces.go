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
*/

// 本文件为 hostNamespaces 检查注册夹具，逐个开启宿主命名空间以定位具体违规字段。
func init() {

	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "host namespaces",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 最小合法 Pod 已覆盖合法组合，无需重复生成通过夹具。
			return nil
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					p.Spec.HostIPC = true
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.HostNetwork = true
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.HostPID = true
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "hostNamespaces"},
		fixtureData_1_0,
	)
}
