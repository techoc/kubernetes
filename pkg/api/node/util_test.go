/*
Copyright 2022 The Kubernetes Authors.

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

package node

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/kubernetes/pkg/apis/node"
)

// TestWarnings 验证 GetWarningsForRuntimeClass 只在 RuntimeClass 的
// scheduling.nodeSelector 使用了弃用节点标签时给出警告；nil 输入和普通标签
// 都不应产生警告。
func TestWarnings(t *testing.T) {
	testcases := []struct {
		name     string
		template *node.RuntimeClass
		expected []string
	}{
		// nil 对象应安全返回，不产生警告。
		{
			name:     "null",
			template: nil,
			expected: nil,
		},
		// 没有 scheduling 配置时无警告。
		{
			name: "no warning",
			template: &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
			},
			expected: nil,
		},
		// 两个 beta 标签都已弃用，各产生一条带替代建议的警告。
		{
			name: "warning",
			template: &node.RuntimeClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				Scheduling: &node.Scheduling{
					NodeSelector: map[string]string{
						"beta.kubernetes.io/arch": "amd64",
						"beta.kubernetes.io/os":   "linux",
					},
				},
			},
			expected: []string{
				`scheduling.nodeSelector: deprecated since v1.14; use "kubernetes.io/arch" instead`,
				`scheduling.nodeSelector: deprecated since v1.14; use "kubernetes.io/os" instead`,
			},
		},
	}

	for _, tc := range testcases {
		t.Run("podspec_"+tc.name, func(t *testing.T) {
			// 用集合比较而非顺序比较：nodeSelector 是 map，警告顺序不确定。
			actual := sets.New[string](GetWarningsForRuntimeClass(tc.template)...)
			expected := sets.New[string](tc.expected...)
			for _, missing := range sets.List[string](expected.Difference(actual)) {
				t.Errorf("missing: %s", missing)
			}
			for _, extra := range sets.List[string](actual.Difference(expected)) {
				t.Errorf("extra: %s", extra)
			}
		})

	}
}
