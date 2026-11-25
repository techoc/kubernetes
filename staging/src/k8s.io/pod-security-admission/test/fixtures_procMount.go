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

// 本文件登记 procMount 基线夹具，以及 1.35 起按基线/受限级别分别收紧的夹具。
// hostUsers 会改变 Unmasked 的合法性，版本和级别变化时必须分别注册，不能沿用旧预期。
func init() {
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "procMount",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// 普通容器和 init 容器都使用合法值。
				tweak(p, func(copy *corev1.Pod) {
					validProcMountType := corev1.DefaultProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// 普通容器使用禁止值；固定 hostUsers=false，排除该放宽分支的干扰。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
				// init 容器使用禁止值。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "procMount"},
		fixtureData_1_0,
	)

	// 1.35 基线在 hostUsers=false 时放宽 Unmasked，因此通过夹具按 level 区分；
	// restricted 不加入该变体，避免与受限级别的失败夹具冲突。
	fixtureData1_35baseline := fixtureGenerator{
		expectErrorSubstring: "procMount",
		generatePass: func(p *corev1.Pod, level api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			retval := []*corev1.Pod{
				// 普通容器和 init 容器都使用合法值。
				tweak(p, func(copy *corev1.Pod) {
					validProcMountType := corev1.DefaultProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
			}

			if level != api.LevelRestricted {
				// restricted 下不应期待该 Pod 通过。
				retval = append(retval, tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}))
			}

			return retval
		},
		// 唯一会触发 1.35 基线检查失败的组合也违反 API 校验，服务端会直接拒绝而非返回警告。
		failRequiresError: true, // 控制器测试因此必须预期硬拒绝。
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// 普通容器使用禁止值。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(true)
				}),
				// init 容器使用禁止值。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(true)
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 35), check: "procMount"},
		fixtureData1_35baseline,
	)

	// 1.35 受限级别不采纳基线对 hostUsers=false 的放宽，Unmasked 始终被拒绝；
	// 通过夹具只保留 Default，失败夹具用 hostUsers=false 单独构造。
	fixtureData1_35restricted := fixtureGenerator{
		expectErrorSubstring: "procMount",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// 普通容器和 init 容器都使用合法值。
				tweak(p, func(copy *corev1.Pod) {
					validProcMountType := corev1.DefaultProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// 普通容器使用禁止值；固定 hostUsers=false，排除该放宽分支的干扰。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
				// init 容器使用禁止值。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelRestricted, version: api.MajorMinorVersion(1, 35), check: "procMount_restricted"},
		fixtureData1_35restricted,
	)
}
