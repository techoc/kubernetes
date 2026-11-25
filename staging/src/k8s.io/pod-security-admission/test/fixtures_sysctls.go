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

podFields: []string{
	`spec.securityContext.sysctls.name`,
},
*/

// 本文件的夹具按 sysctl 白名单的扩容版本逐段登记：
// 每新增一批被允许的 sysctl，就用新版本生成器覆盖，旧版本生成器继续为更早版本服务。
func init() {
	fixtureData_1_0 := fixtureGenerator{
		expectErrorSubstring: "forbidden sysctl",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 安全上下文未设置 sysctl。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.Sysctls = nil }),
				// sysctl 名称："kernel.shm_rmid_forced"、"net.ipv4.ip_local_port_range"、
				// "net.ipv4.tcp_syncookies"、"net.ipv4.ping_group_range"、"net.ipv4.ip_unprivileged_port_start"。
				// 逐项列出白名单，便于发现允许集合变化后夹具与实现脱节。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{
						{Name: "kernel.shm_rmid_forced", Value: "0"},
						{Name: "net.ipv4.ip_local_port_range", Value: "1024 65535"},
						{Name: "net.ipv4.tcp_syncookies", Value: "0"},
						{Name: "net.ipv4.ping_group_range", Value: "1 0"},
						{Name: "net.ipv4.ip_unprivileged_port_start", Value: "1024"},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 不在白名单中的名称，各版本共用同一失败样例。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "othersysctl", Value: "other"}}
				}),
			}
		},
	}
	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 0), check: "sysctls"},
		fixtureData_1_0,
	)

	fixtureData_1_27 := fixtureGenerator{
		expectErrorSubstring: "forbidden sysctl",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 安全上下文未设置 sysctl。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.Sysctls = nil }),
				// sysctl 名称：基础白名单项加上 "net.ipv4.ip_local_reserved_ports"。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{
						{Name: "kernel.shm_rmid_forced", Value: "0"},
						{Name: "net.ipv4.ip_local_port_range", Value: "1024 65535"},
						{Name: "net.ipv4.tcp_syncookies", Value: "0"},
						{Name: "net.ipv4.ping_group_range", Value: "1 0"},
						{Name: "net.ipv4.ip_unprivileged_port_start", Value: "1024"},
						{Name: "net.ipv4.ip_local_reserved_ports", Value: "1024-4999"},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 不在白名单中的名称，各版本共用同一失败样例。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "othersysctl", Value: "other"}}
				}),
			}
		},
	}
	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 27), check: "sysctls"},
		fixtureData_1_27,
	)

	fixtureDataV1Dot29 := fixtureGenerator{
		expectErrorSubstring: "forbidden sysctl",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 安全上下文未设置 sysctl。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.Sysctls = nil }),
				// 在 1.27 白名单基础上加入 tcp_keepalive_time、tcp_fin_timeout、
				// tcp_keepalive_intvl、tcp_keepalive_probes 四个 net.ipv4 sysctl。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{
						{Name: "kernel.shm_rmid_forced", Value: "0"},
						{Name: "net.ipv4.ip_local_port_range", Value: "1024 65535"},
						{Name: "net.ipv4.tcp_syncookies", Value: "0"},
						{Name: "net.ipv4.ping_group_range", Value: "1 0"},
						{Name: "net.ipv4.ip_unprivileged_port_start", Value: "1024"},
						{Name: "net.ipv4.ip_local_reserved_ports", Value: "1024-4999"},
						{Name: "net.ipv4.tcp_keepalive_time", Value: "7200"},
						{Name: "net.ipv4.tcp_fin_timeout", Value: "60"},
						{Name: "net.ipv4.tcp_keepalive_intvl", Value: "75"},
						{Name: "net.ipv4.tcp_keepalive_probes", Value: "9"},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 不在白名单中的名称，各版本共用同一失败样例。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "othersysctl", Value: "other"}}
				}),
			}
		},
	}
	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 29), check: "sysctls"},
		fixtureDataV1Dot29,
	)

	fixtureDataV1Dot32 := fixtureGenerator{
		expectErrorSubstring: "forbidden sysctl",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 安全上下文未设置 sysctl。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.Sysctls = nil }),
				// sysctl 名称："net.ipv4.tcp_rmem"、"net.ipv4.tcp_wmem"。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{
						{Name: "net.ipv4.tcp_rmem", Value: "4096 87380 16777216"},
						{Name: "net.ipv4.tcp_wmem", Value: "4096 65536 16777216"},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 不在白名单中的名称，各版本共用同一失败样例。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "othersysctl", Value: "other"}}
				}),
			}
		},
	}
	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 32), check: "sysctls"},
		fixtureDataV1Dot32,
	)

	// 1.37 新增 tcp_slow_start_after_idle 与 tcp_notsent_lowat 两项白名单，
	// 需单独注册以免旧版本夹具误把新 sysctl 判为合法。
	fixtureDataV1Dot37 := fixtureGenerator{
		expectErrorSubstring: "forbidden sysctl",
		generatePass: func(p *corev1.Pod, _ api.Level) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 安全上下文未设置 sysctl。
				tweak(p, func(p *corev1.Pod) { p.Spec.SecurityContext.Sysctls = nil }),
				// sysctl 名称："net.ipv4.tcp_slow_start_after_idle"、"net.ipv4.tcp_notsent_lowat"。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{
						{Name: "net.ipv4.tcp_slow_start_after_idle", Value: "0"},
						{Name: "net.ipv4.tcp_notsent_lowat", Value: "16384"},
					}
				}),
			}
		},
		generateFail: func(p *corev1.Pod) []*corev1.Pod {
			if p.Spec.SecurityContext == nil {
				p.Spec.SecurityContext = &corev1.PodSecurityContext{}
			}
			return []*corev1.Pod{
				// 不在白名单中的名称，各版本共用同一失败样例。
				tweak(p, func(p *corev1.Pod) {
					p.Spec.SecurityContext.Sysctls = []corev1.Sysctl{{Name: "othersysctl", Value: "other"}}
				}),
			}
		},
	}
	registerFixtureGenerator(
		fixtureKey{level: api.LevelBaseline, version: api.MajorMinorVersion(1, 37), check: "sysctls"},
		fixtureDataV1Dot37,
	)
}
