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

// 1.25 起 Windows Pod 直接放行；用例锁定该豁免边界，防止后续改动误拒 Windows 工作负载。
func TestCapabilitiesRestricted_1_25(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		expectReason string
		expectDetail string
		allowed      bool
	}{
		{
			name: "multiple containers",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "a", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"FOO", "BAR"}}}},
					{Name: "b", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"BAR", "BAZ"}}}},
					{Name: "c", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"NET_BIND_SERVICE", "CHOWN"}, Drop: []corev1.Capability{"ALL", "FOO"}}}},
				}}},
			expectReason: `unrestricted capabilities`,
			expectDetail: `containers "a", "b" must set securityContext.capabilities.drop=["ALL"]; containers "a", "b", "c" must not include "BAR", "BAZ", "CHOWN", "FOO" in securityContext.capabilities.add`,
		},
		{
			name: "windows pod, admit without checking capabilities",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				OS: &corev1.PodOS{Name: corev1.Windows},
				Containers: []corev1.Container{
					{Name: "a"},
				}}},
			allowed: true,
		},
		{
			name: "linux pod, reject if security context is not set",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				OS: &corev1.PodOS{Name: corev1.Linux},
				Containers: []corev1.Container{
					{Name: "a"},
				}}},
			expectReason: `unrestricted capabilities`,
			expectDetail: `container "a" must set securityContext.capabilities.drop=["ALL"]`,
			allowed:      false,
		},
	}
	// 多容器同时违规时，drop 与 add 的诊断合并后按分号分隔。
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := capabilitiesRestricted_1_25(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if result.Allowed && !tc.allowed {
				t.Fatal("expected disallowed")
			}
			if e, a := tc.expectReason, result.ForbiddenReason; e != a {
				t.Errorf("expected\n%s\ngot\n%s", e, a)
			}
			if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
				t.Errorf("expected\n%s\ngot\n%s", e, a)
			}
		})
	}
}

// 旧版本行为单独测：没有 OS 字段也能按 Linux 规则拒绝。
func TestCapabilitiesRestricted_1_22(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		expectReason string
		expectDetail string
	}{
		{
			name: "multiple containers",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "a", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"FOO", "BAR"}}}},
					{Name: "b", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"BAR", "BAZ"}}}},
					{Name: "c", SecurityContext: &corev1.SecurityContext{Capabilities: &corev1.Capabilities{Add: []corev1.Capability{"NET_BIND_SERVICE", "CHOWN"}, Drop: []corev1.Capability{"ALL", "FOO"}}}},
				}}},
			expectReason: `unrestricted capabilities`,
			expectDetail: `containers "a", "b" must set securityContext.capabilities.drop=["ALL"]; containers "a", "b", "c" must not include "BAR", "BAZ", "CHOWN", "FOO" in securityContext.capabilities.add`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := capabilitiesRestricted_1_22(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if result.Allowed {
				t.Fatal("expected disallowed")
			}
			if e, a := tc.expectReason, result.ForbiddenReason; e != a {
				t.Errorf("expected\n%s\ngot\n%s", e, a)
			}
			if e, a := tc.expectDetail, result.ForbiddenDetail; e != a {
				t.Errorf("expected\n%s\ngot\n%s", e, a)
			}
		})
	}
}
