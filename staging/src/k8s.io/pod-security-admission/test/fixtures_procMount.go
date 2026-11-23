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

// 本文件登记 procMount 的基线夹具与 1.35 起新增的限制夹具。
// 1.35 的通过/失败组合依赖 hostUsers 字段，因此必须按版本分别注册，
// 否则早期版本夹具会因新字段语义变化而给出错误预期。
func init() {
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "procMount",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// set proc mount of container and init container to a valid value
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
				// set proc mount of container to a forbidden value
				// 把 hostUsers 固定为 false，排除用户命名空间分支对这条失败夹具的干扰，
				// 确保失败原因确实来自 procMount 本身。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
				// set proc mount of init container to a forbidden value
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

	// 1.35 基线允许在用户命名空间内使用 Unmasked，因此通过夹具要按 level 区分：
	// restricted 下不加入这一变体，否则会与更严格级别自己的失败夹具冲突。
	fixtureData1_35baseline := fixtureGenerator{
		expectErrorSubstring: "procMount",
		generatePass: func(p *corev1.Pod, level api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			retval := []*corev1.Pod{
				// set proc mount of container and init container to a valid value
				tweak(p, func(copy *corev1.Pod) {
					validProcMountType := corev1.DefaultProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &validProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
			}

			if level != api.LevelRestricted {
				// don't expect the unmasked namespace user pod to pass in restricted mode
				retval = append(retval, tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.InitContainers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}))
			}

			return retval
		},
		// 1.35 基线中唯一能触发该检查失败的组合同时违反 API 校验，
		// 因此失败夹具会被服务端直接拒绝；控制器测试必须按“拒绝”而非“带警告接纳”来断言。
		failRequiresError: true, // the only combination that can fail the 1.35 baseline check also fails validation
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// set proc mount of container to a forbidden value
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(true)
				}),
				// set proc mount of init container to a forbidden value
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

	// 1.35 受限级别收紧：用户命名空间内也不允许 Unmasked，
	// 因此通过夹具只保留 Default，失败夹具用 hostUsers=false 单独构造。
	fixtureData1_35restricted := fixtureGenerator{
		expectErrorSubstring: "procMount",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			p = ensureSecurityContext(p)
			return []*corev1.Pod{
				// set proc mount of container and init container to a valid value
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
				// set proc mount of container to a forbidden value
				// 把 hostUsers 固定为 false，排除用户命名空间分支对这条失败夹具的干扰，
				// 确保失败原因确实来自 procMount 本身。
				tweak(p, func(copy *corev1.Pod) {
					unmaskedProcMountType := corev1.UnmaskedProcMount
					copy.Spec.Containers[0].SecurityContext.ProcMount = &unmaskedProcMountType
					copy.Spec.HostUsers = ptr.To(false)
				}),
				// set proc mount of init container to a forbidden value
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
