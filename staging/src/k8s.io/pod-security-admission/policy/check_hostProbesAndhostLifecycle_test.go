/*
Copyright 2024 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"
)

// TestHostProbesAndHostLifecycleEmulation 验证主机探测/生命周期检查的版本模拟开关：
// 该限制是 1.34 才加入基线的，因此模拟 1.34 及以上版本时才会拒绝显式 host，
// 模拟更早版本时必须放行，防止旧版本配置被新策略追溯性拒绝。
func TestHostProbesAndHostLifecycleEmulation(t *testing.T) {
	testcases := []struct {
		name            string
		emulateVersion  *api.Version
		hostCheckActive bool
	}{
		{
			name:            "no emulation",
			emulateVersion:  nil,
			hostCheckActive: true,
		},
		{
			name:            "emulate 1.34",
			emulateVersion:  ptr.To(api.MajorMinorVersion(1, 34)),
			hostCheckActive: true,
		},
		{
			name:            "emulate 1.33",
			emulateVersion:  ptr.To(api.MajorMinorVersion(1, 33)),
			hostCheckActive: false,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := NewEvaluator(DefaultChecks(), tc.emulateVersion)
			if err != nil {
				t.Fatal(err)
			}

			// 探针显式设置 host 的 Pod
			// 显式 host 是触发该检查的必要条件；留空时探测目标是 Pod IP，属于安全用法。
			podMetadata := &metav1.ObjectMeta{}
			podSpec := &corev1.PodSpec{Containers: []corev1.Container{{StartupProbe: &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Host: "localhost"}}}}}}

			// 仅当 host 检查未启用时，求值 "version=latest" 与 "version=1.34" 才应放行
			// 用 "latest" 和 "1.34" 两个版本交叉验证，确保模拟版本只影响模拟目标版本之前的历史策略，
			// 而不会改变当前版本自身的判断。
			expectAllowed := tc.hostCheckActive == false
			if result := AggregateCheckResults(e.EvaluatePod(api.LevelVersion{Level: api.LevelBaseline, Version: api.LatestVersion()}, podMetadata, podSpec)); result.Allowed != expectAllowed {
				t.Fatalf("evaluating with 'latest' expected allowed=%v, got %v: %v", expectAllowed, result.Allowed, result.ForbiddenReasons)
			}
			if result := AggregateCheckResults(e.EvaluatePod(api.LevelVersion{Level: api.LevelBaseline, Version: api.MajorMinorVersion(1, 34)}, podMetadata, podSpec)); result.Allowed != expectAllowed {
				t.Fatalf("evaluating with '1.34' expected allowed=%v, got %v: %v", expectAllowed, result.Allowed, result.ForbiddenReasons)
			}

			// 求值 "version=1.33" 时必须始终放行
			// 1.33 早于该限制引入的版本，无论模拟开关如何都必须放行，
			// 这是防止策略被追溯应用到旧版本的关键回归点。
			if result := AggregateCheckResults(e.EvaluatePod(api.LevelVersion{Level: api.LevelBaseline, Version: api.MajorMinorVersion(1, 33)}, podMetadata, podSpec)); !result.Allowed {
				t.Fatalf("evaluating with '1.33' expected allowed=true, got %v: %v", result.Allowed, result.ForbiddenReasons)
			}
		})
	}

}

// TestHostProbesAndHostLifecycle 覆盖 1.34 引入的显式 host 限制的各类取值形态：
// localhost、环回地址、任意域名、IPv4/IPv6，并覆盖三种探测与两种生命周期钩子，
// 确保无论 host 写在哪个字段都会被同一条规则捕获。
func TestHostProbesAndHostLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		expectReason string
		expectDetail string
	}{
		{
			// host 留空时探测走 Pod IP，不引入节点网络访问，因此应当被基线策略允许。
			name: "valid pod with unset hosts",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "a",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{}, // 未设置 host，默认使用 Pod IP
							},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{}, // 未设置 host，默认使用 Pod IP
							},
						},
						Lifecycle: &corev1.Lifecycle{
							PostStart: &corev1.LifecycleHandler{
								HTTPGet: &corev1.HTTPGetAction{}, // 未设置 host，默认使用 Pod IP
							},
						},
					},
				},
			}},
		},
		{
			// 环回地址（含 IPv6 ::1）与 localhost 都指向节点自身，
			// 是绕过 Pod 网络边界访问节点服务的最常见写法，必须拒绝。
			name: "invalid pod with local host IP as probe host",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "a",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "127.0.0.1"},
							},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Host: "::1"},
							},
						},
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "localhost"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "a" uses probe or lifecycle hosts "127.0.0.1", "::1", "localhost"`,
		},
		{
			name: "invalid httpget host in liveness probe",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "a",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "invalid.host"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "a" uses probe or lifecycle host "invalid.host"`,
		},
		{
			name: "invalid tcpsocket host in readiness probe",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "b",
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Host: "invalid.host"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "b" uses probe or lifecycle host "invalid.host"`,
		},
		{
			name: "invalid httpget host in startup probe",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "c",
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "invalid.host"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "c" uses probe or lifecycle host "invalid.host"`,
		},
		{
			name: "invalid poststart tcpsocket host",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "d",
						Lifecycle: &corev1.Lifecycle{
							PostStart: &corev1.LifecycleHandler{
								TCPSocket: &corev1.TCPSocketAction{Host: "invalid.host"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "d" uses probe or lifecycle host "invalid.host"`,
		},
		{
			name: "invalid prestop httpget host",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "e",
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "another.invalid.host"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "e" uses probe or lifecycle host "another.invalid.host"`,
		},
		{
			// 多容器场景验证诊断信息按容器名和 host 去重汇总，
			// 便于用户一次定位所有违规点，而不是逐个修正后再提交。
			name: "multiple containers with multiple invalid hosts",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "valid"},
					{
						Name: "invalid1",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Host: "a.com"}},
						},
					},
					{
						Name: "invalid2",
						Lifecycle: &corev1.Lifecycle{
							PreStop: &corev1.LifecycleHandler{TCPSocket: &corev1.TCPSocketAction{Host: "b.com"}},
						},
						StartupProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Host: "a.com"}},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `containers "invalid1", "invalid2" use probe or lifecycle hosts "a.com", "b.com"`,
		},
		{
			name: "invalid ipv4 host in probe",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "a",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "8.8.8.8"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "a" uses probe or lifecycle host "8.8.8.8"`,
		},
		{
			name: "invalid ipv6 host in probe",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "a",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Host: "2001:4860:4860::8888"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "a" uses probe or lifecycle host "2001:4860:4860::8888"`,
		},
		{
			// initContainer 与常规容器受同一策略约束；
			// init 阶段往往拥有更高权限，遗漏这一路会成为提权入口。
			name: "invalid host in initcontainer",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{
					{
						Name: "init",
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{Host: "invalid.init.host"},
							},
						},
					},
				},
			}},
			expectReason: "probe or lifecycle host",
			expectDetail: `container "init" uses probe or lifecycle host "invalid.init.host"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := hostProbesAndHostLifecycleV1Dot34(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if tc.expectReason == "" {
				if !result.Allowed {
					t.Fatalf("expected allowed, but got disallowed: %s", result.ForbiddenDetail)
				}
			} else {
				if result.Allowed {
					t.Fatal("expected disallowed, but got allowed")
				}
				if e, a := tc.expectReason, result.ForbiddenReason; e != a {
					t.Errorf("expected reason\n%s\ngot\n%s", e, a)
				}
				if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
					t.Errorf("expected detail\n%s\ngot\n%s", e, a)
				}
			}
		})
	}
}
