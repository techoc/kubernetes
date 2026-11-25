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
	`securityContext.windowsOptions.hostProcess`,
},
containerFields: []string{
	`securityContext.windowsOptions.hostProcess`,
},

*/

// 本文件为 windowsHostProcess 检查注册失败夹具；最小合法 Pod 已覆盖允许组合。
// hostProcess 达到 GA 后，再补 Pod 级和容器级显式 false 的通过夹具。
func init() {

	fixtureData_1_0 := fixtureGenerator{
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 最小合法 Pod 已覆盖合法组合，无需重复生成通过夹具。
			return nil
		},
		expectErrorSubstring: "hostProcess",
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			if p.Spec.SecurityContext.WindowsOptions == nil {
				p.Spec.SecurityContext.WindowsOptions = &corev1.WindowsSecurityContextOptions{}
			}
			if p.Spec.Containers[0].SecurityContext.WindowsOptions == nil {
				p.Spec.Containers[0].SecurityContext.WindowsOptions = &corev1.WindowsSecurityContextOptions{}
			}
			if p.Spec.InitContainers[0].SecurityContext.WindowsOptions == nil {
				p.Spec.InitContainers[0].SecurityContext.WindowsOptions = &corev1.WindowsSecurityContextOptions{}
			}
			return []*corev1.Pod{
				// Pod 级开启 HostProcess。
				tweak(p, func(p *corev1.Pod) {
					// HostProcess Pod 必须开启 HostNetwork 才能通过 API 校验，进而测试准入检查。
					p.Spec.HostNetwork = true
					p.Spec.SecurityContext.WindowsOptions.HostProcess = ptr.To(true)
				}),
				// 普通容器和 init 容器开启 HostProcess。
				tweak(p, func(p *corev1.Pod) {
					// HostProcess Pod 必须开启 HostNetwork 才能通过 API 校验，进而测试准入检查。
					p.Spec.HostNetwork = true
					p.Spec.Containers[0].SecurityContext.WindowsOptions.HostProcess = ptr.To(true)
					p.Spec.InitContainers[0].SecurityContext.WindowsOptions.HostProcess = ptr.To(true)
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "windowsHostProcess"},
		fixtureData_1_0,
	)
	// TODO: hostProcess 达到 GA 后，补充 Pod 级和容器级显式 false 的通过夹具。
}
