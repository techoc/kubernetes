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

package storage

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/storage"
)

// TestStorageClassWarnings 验证 GetWarningsForStorageClass 只针对 allowedTopologies
// 中使用了弃用节点标签的表达式告警，nil 对象和无弃用字段的对象均不告警。
func TestStorageClassWarnings(t *testing.T) {
	testcases := []struct {
		name     string
		template *storage.StorageClass
		expected []string
	}{
		// nil 对象应安全返回，不产生警告。
		{
			name:     "null",
			template: nil,
			expected: nil,
		},
		// 没有 allowedTopologies 时不告警。
		{
			name: "no warning",
			template: &storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
			},
			expected: nil,
		},
		// 两个拓扑表达式都用了 beta 标签，应各产生一条带替代建议的警告。
		{
			name: "warning",
			template: &storage.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				AllowedTopologies: []core.TopologySelectorTerm{
					{
						MatchLabelExpressions: []core.TopologySelectorLabelRequirement{
							{
								Key:    "beta.kubernetes.io/arch",
								Values: []string{"amd64"},
							},
							{
								Key:    "beta.kubernetes.io/os",
								Values: []string{"linux"},
							},
						},
					},
				},
			},
			expected: []string{
				`allowedTopologies[0].matchLabelExpressions[0].key: deprecated since v1.14; use "kubernetes.io/arch" instead`,
				`allowedTopologies[0].matchLabelExpressions[1].key: deprecated since v1.14; use "kubernetes.io/os" instead`,
			},
		},
	}

	for _, tc := range testcases {
		t.Run("podspec_"+tc.name, func(t *testing.T) {
			// 用集合比较，避免依赖警告的生成顺序。
			actual := sets.New[string](GetWarningsForStorageClass(tc.template)...)
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

// TestCSIStorageCapacityWarnings 验证 GetWarningsForCSIStorageCapacity 能检查
// nodeTopology 的两种写法：matchLabels 和 matchExpressions 中的弃用节点标签都要告警。
func TestCSIStorageCapacityWarnings(t *testing.T) {
	testcases := []struct {
		name     string
		template *storage.CSIStorageCapacity
		expected []string
	}{
		// nil 对象应安全返回，不产生警告。
		{
			name:     "null",
			template: nil,
			expected: nil,
		},
		// 没有 nodeTopology 时不告警。
		{
			name: "no warning",
			template: &storage.CSIStorageCapacity{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
			},
			expected: nil,
		},
		// matchLabels 的 key 直接作为字段路径的一段，两个 beta 标签各告警一次。
		{
			name: "MatchLabels warning",
			template: &storage.CSIStorageCapacity{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				NodeTopology: &metav1.LabelSelector{
					MatchLabels: map[string]string{
						"beta.kubernetes.io/arch": "amd64",
						"beta.kubernetes.io/os":   "linux",
					},
				},
			},
			expected: []string{
				`nodeTopology.matchLabels.beta.kubernetes.io/arch: deprecated since v1.14; use "kubernetes.io/arch" instead`,
				`nodeTopology.matchLabels.beta.kubernetes.io/os: deprecated since v1.14; use "kubernetes.io/os" instead`,
			},
		},
		// matchExpressions 用索引定位具体的 key，两个 beta 标签各告警一次。
		{
			name: "MatchExpressions warning",
			template: &storage.CSIStorageCapacity{
				ObjectMeta: metav1.ObjectMeta{
					Name: "foo",
				},
				NodeTopology: &metav1.LabelSelector{
					MatchExpressions: []metav1.LabelSelectorRequirement{
						{
							Key:      "beta.kubernetes.io/arch",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"amd64"},
						},
						{
							Key:      "beta.kubernetes.io/os",
							Operator: metav1.LabelSelectorOpIn,
							Values:   []string{"linux"},
						},
					},
				},
			},
			expected: []string{
				`nodeTopology.matchExpressions[0].key: beta.kubernetes.io/arch is deprecated since v1.14; use "kubernetes.io/arch" instead`,
				`nodeTopology.matchExpressions[1].key: beta.kubernetes.io/os is deprecated since v1.14; use "kubernetes.io/os" instead`,
			},
		},
	}

	for _, tc := range testcases {
		t.Run("podspec_"+tc.name, func(t *testing.T) {
			// 用集合比较：matchLabels 来自 map，遍历顺序不固定。
			actual := sets.New[string](GetWarningsForCSIStorageCapacity(tc.template)...)
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
