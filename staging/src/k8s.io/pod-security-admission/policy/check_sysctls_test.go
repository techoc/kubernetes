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

package policy

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

// 1.0 白名单仅有五项，以此锁定最初的安全集合；虚构名字 a/b 用于验证多项违规的稳定拼接顺序。
func TestSysctls(t *testing.T) {
	// allowed 省略即视为 false：表中大多数用例都是拒绝场景。
	tests := []struct {
		name         string
		pod          *corev1.Pod
		allowed      bool
		expectReason string
		expectDetail string
	}{
		{
			name: "forbidden sysctls",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "a"}, {Name: "b"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `a, b`,
		},
		{
			// 后续版本新增的“安全” sysctl，不能倒灌到旧版本策略里。
			name: "new supported sysctls not supported: net.ipv4.ip_local_reserved_ports",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.ip_local_reserved_ports", Value: "1024-4999"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.ip_local_reserved_ports`,
		},
		{
			name: "new supported sysctls not supported: net.ipv4.tcp_keepalive_time",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_keepalive_time", Value: "7200"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.tcp_keepalive_time`,
		},
		{
			name: "new supported sysctls not supported: net.ipv4.tcp_fin_timeout",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_fin_timeout", Value: "60"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.tcp_fin_timeout`,
		},
		{
			name: "new supported sysctls not supported: net.ipv4.tcp_keepalive_intvl",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_keepalive_intvl", Value: "75"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.tcp_keepalive_intvl`,
		},
		{
			name: "new supported sysctls not supported: net.ipv4.tcp_keepalive_probes",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_keepalive_probes", Value: "9"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.tcp_keepalive_probes`,
		},
		{
			name: "new supported sysctls not supported: net.ipv4.tcp_rmem",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_rmem", Value: "4096 87380 16777216"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.tcp_rmem`,
		},
		{
			name: "new supported sysctls not supported: net.ipv4.tcp_wmem",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_wmem", Value: "4096 87380 16777216"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `net.ipv4.tcp_wmem`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sysctlsV1Dot0(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if !tc.allowed {
				if result.Allowed {
					t.Fatal("expected disallowed")
				}
				if e, a := tc.expectReason, result.ForbiddenReason; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
				if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
			} else if !result.Allowed {
				t.Fatal("expected allowed")
			}
		})
	}
}

// 1.27 只放宽 ip_local_reserved_ports，防止“新增白名单顺手放行所有网络 sysctl”的回退。
func TestSysctls_1_27(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		allowed      bool
		expectReason string
		expectDetail string
	}{
		{
			name: "forbidden sysctls",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "a"}, {Name: "b"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `a, b`,
		},
		{
			name: "new supported sysctls",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.ip_local_reserved_ports", Value: "1024-4999"}},
				},
			}},
			allowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sysctlsV1Dot27(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if !tc.allowed {
				if result.Allowed {
					t.Fatal("expected disallowed")
				}
				if e, a := tc.expectReason, result.ForbiddenReason; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
				if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
			} else if !result.Allowed {
				t.Fatal("expected allowed")
			}
		})
	}
}

// 1.29 新增四项 TCP keepalive / timeout，逐项列出避免实现时用前缀匹配蒙混过关。
func TestSysctls_1_29(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		allowed      bool
		expectReason string
		expectDetail string
	}{
		{
			name: "forbidden sysctls",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "a"}, {Name: "b"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `a, b`,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_keepalive_time",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_keepalive_time", Value: "7200"}},
				},
			}},
			allowed: true,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_fin_timeout",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_fin_timeout", Value: "60"}},
				},
			}},
			allowed: true,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_keepalive_intvl",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_keepalive_intvl", Value: "75"}},
				},
			}},
			allowed: true,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_keepalive_probes",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_keepalive_probes", Value: "9"}},
				},
			}},
			allowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sysctlsV1Dot29(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if !tc.allowed {
				if result.Allowed {
					t.Fatal("expected disallowed")
				}
				if e, a := tc.expectReason, result.ForbiddenReason; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
				if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
			} else if !result.Allowed {
				t.Fatal("expected allowed")
			}
		})
	}
}

// 1.32 放行 tcp_rmem / tcp_wmem，保留最早的禁止用例作为回归基线。
func TestSysctls_1_32(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		allowed      bool
		expectReason string
		expectDetail string
	}{
		{
			name: "forbidden sysctls",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "a"}, {Name: "b"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `a, b`,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_rmem",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_rmem", Value: "4096 87380 16777216"}},
				},
			}},
			allowed: true,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_wmem",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_wmem", Value: "4096 65536 16777216"}},
				},
			}},
			allowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sysctlsV1Dot32(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if !tc.allowed {
				if result.Allowed {
					t.Fatal("expected disallowed")
				}
				if e, a := tc.expectReason, result.ForbiddenReason; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
				if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
			} else if !result.Allowed {
				t.Fatal("expected allowed")
			}
		})
	}
}

// 1.37 新增两项拥塞控制相关 sysctl，沿用同样的表格驱动断言，避免为每个版本复制校验逻辑。
func TestSysctls_1_37(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		allowed      bool
		expectReason string
		expectDetail string
	}{
		{
			name: "forbidden sysctls",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "a"}, {Name: "b"}},
				},
			}},
			allowed:      false,
			expectReason: `forbidden sysctls`,
			expectDetail: `a, b`,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_slow_start_after_idle",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_slow_start_after_idle", Value: "0"}},
				},
			}},
			allowed: true,
		},
		{
			name: "new supported sysctls: net.ipv4.tcp_notsent_lowat",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					Sysctls: []corev1.Sysctl{{Name: "net.ipv4.tcp_notsent_lowat", Value: "16384"}},
				},
			}},
			allowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := sysctlsV1Dot37(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if !tc.allowed {
				if result.Allowed {
					t.Fatal("expected disallowed")
				}
				if e, a := tc.expectReason, result.ForbiddenReason; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
				if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
					t.Errorf("expected\n%s\ngot\n%s", e, a)
				}
			} else if !result.Allowed {
				t.Fatal("expected allowed")
			}
		})
	}
}
