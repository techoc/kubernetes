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
说明：注解与字段夹具共用 seccomp profile 常量和构造 helper；检查实现按版本读取对应配置。
*/

// 两个版本各注册一份生成器：1.0 使用旧注解，1.19 使用原生字段，覆盖配置形式的切换。
func init() {
	fixtureData_baseline_1_0 := fixtureGenerator{
		expectErrorSubstring: "seccompProfile",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 基准 Pod 已有 Pod 级 seccomp 配置时，跳过这组额外通过夹具。
			if val, ok := p.Annotations[annotationKeyPod]; ok &&
				val == corev1.SeccompProfileRuntimeDefault {
				return nil
			}

			p = ensureAnnotation(p)
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					// Pod 级使用运行时默认配置。
					p.Annotations[annotationKeyPod] = corev1.SeccompProfileRuntimeDefault
					// 容器级使用 localhost profile。
					p.Annotations[annotationKeyContainer(p.Spec.Containers[0])] = corev1.SeccompLocalhostProfileNamePrefix + "testing"
					// init 容器故意留空，验证未显式设置时可继承 Pod 级配置。
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureAnnotation(p)
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					p.Annotations[annotationKeyPod] = corev1.SeccompProfileNameUnconfined
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Annotations[annotationKeyContainer(p.Spec.Containers[0])] = corev1.SeccompProfileNameUnconfined
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Annotations[annotationKeyContainer(p.Spec.InitContainers[0])] = corev1.SeccompProfileNameUnconfined
				}),
			}
		},
	}

	fixtureData_baseline_1_19 := fixtureGenerator{
		expectErrorSubstring: "seccompProfile",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 基准 Pod 已配置 seccomp 时，无需重复生成通过夹具。
			if p.Spec.SecurityContext != nil &&
				p.Spec.SecurityContext.SeccompProfile != nil &&
				p.Spec.SecurityContext.SeccompProfile.Type == corev1.SeccompProfileTypeRuntimeDefault {
				return nil
			}

			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					// Pod 级使用运行时默认配置。
					p.Spec.SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
					// 容器级沿用运行时默认配置。
					p.Spec.Containers[0].SecurityContext.SeccompProfile = seccompProfileRuntimeDefault
					// init 容器故意留空，验证未显式设置时可继承 Pod 级配置。
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.SeccompProfile = seccompProfileUnconfined
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].SecurityContext.SeccompProfile = seccompProfileUnconfined
				}),
				tweak(p, func(p *corev1.Pod) {
					p.Spec.InitContainers[0].SecurityContext.SeccompProfile = seccompProfileUnconfined
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "seccompProfile_baseline"},
		fixtureData_baseline_1_0,
	)

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 19), check: "seccompProfile_baseline"},
		fixtureData_baseline_1_19,
	)
}
