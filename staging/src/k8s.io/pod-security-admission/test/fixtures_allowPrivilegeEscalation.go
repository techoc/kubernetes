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

containerFields: []string{
	`securityContext.allowPrivilegeEscalation`,
},

*/

// 本文件注册 allowPrivilegeEscalation 检查的失败夹具。
// Windows 不支持该字段，需显式跳过；nil 表示未关闭提权，也属于违规。
func init() {
	fixtureData_1_8 := fixtureGenerator{
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			// 最小合法 Pod 已覆盖合法组合，无需重复生成通过夹具。
			return nil
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			if p.Spec.OS != nil && p.Spec.OS.Name == corev1.Windows {
				return []*corev1.Pod{}
			}
			return []*corev1.Pod{
				// 显式允许提权。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = ptr.To(true)
				}),
				// init 容器同样受该检查约束。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.InitContainers[0].SecurityContext.AllowPrivilegeEscalation = ptr.To(true)
				}),
				// 字段未设置。
				tweak(p, func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext.AllowPrivilegeEscalation = nil }),
				// 整个安全上下文缺失。
				tweak(p, func(p *corev1.Pod) { p.Spec.Containers[0].SecurityContext = nil }),
			}
		},
	}

	registerFixtureGenerator(
		fixtureKey{level: api.LevelRestricted, version: api.MajorMinorVersion(1, 8), check: "allowPrivilegeEscalation"},
		fixtureData_1_8,
	)
}
