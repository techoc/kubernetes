/*
Copyright 2018 The Kubernetes Authors.

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

package runtime

import (
	"testing"

	"reflect"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestPreferredVersionsAllGroups 验证版本优先级计算：
//   - 只观测到版本（observedOnly）：按出现顺序保留；
//   - 只显式指定优先级（specifiedOnly）：以指定顺序为准；
//   - 两者并存（both）：显式优先级在前，其余观测版本按发现顺序追加。
//
// 同时覆盖 PrioritizedVersionsForGroup / PrioritizedVersionsAllGroups /
// PreferredVersionAllGroups 三个入口的一致性。
func TestPreferredVersionsAllGroups(t *testing.T) {
	tests := []struct {
		name                string
		versionPriority     map[string][]string
		observedVersions    []schema.GroupVersion
		expectedPrioritized map[string][]schema.GroupVersion
		expectedPreferred   map[schema.GroupVersion]bool
	}{
		{
			// 没有配置优先级时，按观测顺序排序，每组第一个为 preferred。
			name: "observedOnly",
			observedVersions: []schema.GroupVersion{
				{Group: "", Version: "v3"},
				{Group: "foo", Version: "v1"},
				{Group: "foo", Version: "v2"},
				{Group: "", Version: "v1"},
			},
			expectedPrioritized: map[string][]schema.GroupVersion{
				"": {
					{Group: "", Version: "v3"},
					{Group: "", Version: "v1"},
				},
				"foo": {
					{Group: "foo", Version: "v1"},
					{Group: "foo", Version: "v2"},
				},
			},
			expectedPreferred: map[schema.GroupVersion]bool{
				{Group: "", Version: "v3"}:    true,
				{Group: "foo", Version: "v1"}: true,
			},
		},
		{
			// 只配置优先级时，完全按配置排序，不依赖观测数据。
			name: "specifiedOnly",
			versionPriority: map[string][]string{
				"":    {"v3", "v1"},
				"foo": {"v1", "v2"},
			},
			expectedPrioritized: map[string][]schema.GroupVersion{
				"": {
					{Group: "", Version: "v3"},
					{Group: "", Version: "v1"},
				},
				"foo": {
					{Group: "foo", Version: "v1"},
					{Group: "foo", Version: "v2"},
				},
			},
			expectedPreferred: map[schema.GroupVersion]bool{
				{Group: "", Version: "v3"}:    true,
				{Group: "foo", Version: "v1"}: true,
			},
		},
		{
			// 显式优先级优先，未列出的观测版本按发现顺序补在后面。
			name: "both",
			versionPriority: map[string][]string{
				"":    {"v3", "v1"},
				"foo": {"v1", "v2"},
			},
			observedVersions: []schema.GroupVersion{
				{Group: "", Version: "v1"},
				{Group: "", Version: "v3"},
				{Group: "", Version: "v4"},
				{Group: "", Version: "v5"},
				{Group: "bar", Version: "v1"},
				{Group: "bar", Version: "v2"},
			},
			expectedPrioritized: map[string][]schema.GroupVersion{
				"": {
					{Group: "", Version: "v3"},
					{Group: "", Version: "v1"},
					{Group: "", Version: "v4"},
					{Group: "", Version: "v5"},
				},
				"foo": {
					{Group: "foo", Version: "v1"},
					{Group: "foo", Version: "v2"},
				},
				"bar": {
					{Group: "bar", Version: "v1"},
					{Group: "bar", Version: "v2"},
				},
			},
			expectedPreferred: map[schema.GroupVersion]bool{
				{Group: "", Version: "v3"}:    true,
				{Group: "foo", Version: "v1"}: true,
				{Group: "bar", Version: "v1"}: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := NewScheme()
			// 直接注入内部字段，避免依赖 discovery 流程。
			scheme.versionPriority = test.versionPriority
			scheme.observedVersions = test.observedVersions

			for group, expected := range test.expectedPrioritized {
				actual := scheme.PrioritizedVersionsForGroup(group)
				if !reflect.DeepEqual(expected, actual) {
					t.Error(cmp.Diff(expected, actual))
				}
			}

			// 全局入口必须按 group 聚合后与分组结果一致。
			prioritizedAll := scheme.PrioritizedVersionsAllGroups()
			actualPrioritizedAll := map[string][]schema.GroupVersion{}
			for _, actual := range prioritizedAll {
				actualPrioritizedAll[actual.Group] = append(actualPrioritizedAll[actual.Group], actual)
			}
			if !reflect.DeepEqual(test.expectedPrioritized, actualPrioritizedAll) {
				t.Error(cmp.Diff(test.expectedPrioritized, actualPrioritizedAll))
			}

			preferredAll := scheme.PreferredVersionAllGroups()
			actualPreferredAll := map[schema.GroupVersion]bool{}
			for _, actual := range preferredAll {
				actualPreferredAll[actual] = true
			}
			if !reflect.DeepEqual(test.expectedPreferred, actualPreferredAll) {
				t.Error(cmp.Diff(test.expectedPreferred, actualPreferredAll))
			}
		})
	}
}
