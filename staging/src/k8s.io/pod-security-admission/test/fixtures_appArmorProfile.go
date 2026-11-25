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

func init() {
	appArmorFixture_1_0 := fixtureGenerator{
		expectErrorSubstring: "forbidden AppArmor profile",
		// 通过样例验证非 unconfined 的 AppArmor 配置不会被拦截。
		generatePass: func(pod *corev1.Pod, _ api.Level) []*corev1.Pod {
			pod = ensureAnnotation(pod)
			return []*corev1.Pod{
				// 同一键先写 runtime/default 再写 localhost/foo，后者覆盖前者；
				// 复用容器名可避免为注释放置额外容器，最终只验证 localhost/foo。
				tweak(pod, func(copy *corev1.Pod) {
					containerName := copy.Spec.Containers[0].Name
					copy.Annotations[corev1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix+containerName] = "runtime/default"

					initContainerName := copy.Spec.Containers[0].Name
					copy.Annotations[corev1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix+initContainerName] = "localhost/foo"
				}),
			}
		},
		// 普通容器与 init 容器分别设置 unconfined，确保两层都被检查。
		generateFail: func(pod *corev1.Pod) []*corev1.Pod {
			pod = ensureAnnotation(pod)
			return []*corev1.Pod{
				// 普通容器使用 unconfined 注解。
				tweak(pod, func(copy *corev1.Pod) {
					name := copy.Spec.Containers[0].Name
					copy.Annotations[corev1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix+name] = "unconfined"
				}),

				// init 容器使用 unconfined 注解。
				tweak(pod, func(copy *corev1.Pod) {
					name := copy.Spec.InitContainers[0].Name
					copy.Annotations[corev1.DeprecatedAppArmorBetaContainerAnnotationKeyPrefix+name] = "unconfined"
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "appArmorProfile"},
		appArmorFixture_1_0,
	)
}
