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

// 同一 Pod 混放 nil、默认、Unmasked 和其他值，一次覆盖全部判定分支；
// 第二个用例专门验证 userns 打开后规则放宽——这是 baseline 在 1.35 的行为。
func TestProcMountBaseline(t *testing.T) {
	defaultValue := corev1.DefaultProcMount
	unmaskedValue := corev1.UnmaskedProcMount
	otherValue := corev1.ProcMountType("other")

	tests := []struct {
		name          string
		pod           *corev1.Pod
		expectReason  string
		expectDetail  string
		expectAllowed bool
	}{
		{
			name: "procMount",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "a", SecurityContext: nil},
					{Name: "b", SecurityContext: &corev1.SecurityContext{}},
					{Name: "c", SecurityContext: &corev1.SecurityContext{ProcMount: &defaultValue}},
					{Name: "d", SecurityContext: &corev1.SecurityContext{ProcMount: &unmaskedValue}},
					{Name: "e", SecurityContext: &corev1.SecurityContext{ProcMount: &otherValue}},
				},
				HostUsers: ptr.To(true),
			}},
			expectReason:  `procMount`,
			expectAllowed: false,
			expectDetail:  `containers "d", "e" must not set securityContext.procMount to "Unmasked", "other"`,
		},
		{
			// HostUsers=false 表示启用 userns，此时 Unmasked 不再算违规。
			name: "procMount with userns",
			pod: &corev1.Pod{Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "a", SecurityContext: nil},
					{Name: "b", SecurityContext: &corev1.SecurityContext{}},
					{Name: "c", SecurityContext: &corev1.SecurityContext{ProcMount: &defaultValue}},
					{Name: "d", SecurityContext: &corev1.SecurityContext{ProcMount: &unmaskedValue}},
					{Name: "e", SecurityContext: &corev1.SecurityContext{ProcMount: &otherValue}},
				},
				HostUsers: ptr.To(false),
			}},
			expectReason:  "",
			expectDetail:  "",
			expectAllowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := procMount1_35baseline(&tc.pod.ObjectMeta, &tc.pod.Spec)
			if result.Allowed != tc.expectAllowed {
				t.Fatalf("expected Allowed to be %v was %v", tc.expectAllowed, result.Allowed)
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
