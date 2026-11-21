/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	"testing"

	v1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/scheduling"
)

// TestIsKnownSystemPriorityClass 验证 IsKnownSystemPriorityClass 能准确识别系统内置 PriorityClass，
// 覆盖名称/值匹配但被篡改配置的反例，防止仅凭名称或优先级值就误判为系统类。
func TestIsKnownSystemPriorityClass(t *testing.T) {
	tests := []struct {
		name     string
		pc       *v1.PriorityClass
		expected bool
	}{
		{
			name:     "system priority class",
			pc:       SystemPriorityClasses()[0],
			expected: true,
		},
		{
			name: "non-system priority class",
			pc: &v1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: scheduling.SystemNodeCritical,
				},
				Value:       scheduling.SystemCriticalPriority, // 取系统 cluster critical 的优先级值，但名称并非内置类
				Description: "Used for system critical pods that must not be moved from their current node.",
			},
			expected: false,
		},
		{
			name: "not a known system priority class",
			pc: &v1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "unknown",
				},
				Value:       scheduling.SystemCriticalPriority,
				Description: "Used for system critical pods that must run in the cluster, but can be moved to another node if necessary.",
			},
			expected: false,
		},
		{
			name: "global default changed",
			pc: &v1.PriorityClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: scheduling.SystemClusterCritical,
				},
				GlobalDefault: true,
				Value:         scheduling.SystemCriticalPriority,
				Description:   "Used for system critical pods that must run in the cluster, but can be moved to another node if necessary.",
			},
			expected: false,
		},
	}

	for _, test := range tests {
		// 逐一校验名称 + 值 + GlobalDefault 的组合，任一属性不符都应返回 false。
		if is, err := IsKnownSystemPriorityClass(test.pc.Name, test.pc.Value, test.pc.GlobalDefault); test.expected != is {
			t.Errorf("Test [%v]: Expected %v, but got %v. Error: %v", test.name, test.expected, is, err)
		}
	}
}
