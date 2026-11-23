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
	"k8s.io/utils/ptr"
)

// TestWindowsHostProcess 验证 Windows hostProcess 检查的判定边界：
// Pod 级设置 true 或容器级显式 true 都要拒绝，而 nil、false、空 WindowsOptions
// 都不算违规，避免把“未设置”误判成开启宿主进程权限。
func TestWindowsHostProcess(t *testing.T) {
	tests := []struct {
		name         string
		pod          *corev1.Pod
		expectReason string
		expectDetail string
	}{
		{
			// 同一 Pod 内混合多种 SecurityContext 形态，
			// 一次运行即可确认只有显式 true 的容器被列入拒绝详情。
			name: "host process",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				SecurityContext: &corev1.PodSecurityContext{
					WindowsOptions: &corev1.WindowsSecurityContextOptions{HostProcess: ptr.To(true)},
				},
				Containers: []corev1.Container{
					{Name: "a", SecurityContext: nil},
					{Name: "b", SecurityContext: &corev1.SecurityContext{}},
					{Name: "c", SecurityContext: &corev1.SecurityContext{WindowsOptions: &corev1.WindowsSecurityContextOptions{}}},
					{Name: "d", SecurityContext: &corev1.SecurityContext{WindowsOptions: &corev1.WindowsSecurityContextOptions{HostProcess: ptr.To(false)}}},
					{Name: "e", SecurityContext: &corev1.SecurityContext{WindowsOptions: &corev1.WindowsSecurityContextOptions{HostProcess: ptr.To(true)}}},
					{Name: "f", SecurityContext: &corev1.SecurityContext{WindowsOptions: &corev1.WindowsSecurityContextOptions{HostProcess: ptr.To(true)}}},
				},
			}},
			expectReason: `hostProcess`,
			expectDetail: `pod and containers "e", "f" must not set securityContext.windowsOptions.hostProcess=true`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := windowsHostProcess_1_0(&tc.pod.ObjectMeta, &tc.pod.Spec)
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
