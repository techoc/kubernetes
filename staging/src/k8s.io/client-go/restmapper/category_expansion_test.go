/*
Copyright 2017 The Kubernetes Authors.

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

package restmapper

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestCategoryExpansion 验证静态 SimpleCategoryExpander 的展开行为：
// 未配置的分类返回 (nil, false)，已配置的分类按注册顺序返回全部资源。
func TestCategoryExpansion(t *testing.T) {
	tests := []struct {
		name string
		arg  string

		expected   []schema.GroupResource
		expectedOk bool
	}{
		{
			// 普通资源名不是分类，不应展开。
			name:     "no-replacement",
			arg:      "service",
			expected: nil,
		},
		{
			// "all" 是内置聚合分类，展开后应包含所有组下的资源。
			name: "all-replacement",
			arg:  "all",
			expected: []schema.GroupResource{
				{Resource: "one"},
				{Resource: "two"},
				{Resource: "three", Group: "alpha"},
				{Resource: "four", Group: "bravo"},
			},
			expectedOk: true,
		},
	}

	for _, test := range tests {
		simpleCategoryExpander := SimpleCategoryExpander{
			Expansions: map[string][]schema.GroupResource{
				"all": {
					{Group: "", Resource: "one"},
					{Group: "", Resource: "two"},
					{Group: "alpha", Resource: "three"},
					{Group: "bravo", Resource: "four"},
				},
			},
		}

		actual, actualOk := simpleCategoryExpander.Expand(test.arg)
		if e, a := test.expected, actual; !reflect.DeepEqual(e, a) {
			t.Errorf("%s:  expected %s, got %s", test.name, e, a)
		}
		if e, a := test.expectedOk, actualOk; e != a {
			t.Errorf("%s:  expected %v, got %v", test.name, e, a)
		}
	}
}

// TestDiscoveryCategoryExpander 验证基于服务端发现信息的分类展开：
// 只有显式声明了对应 category 的资源才会被返回。
func TestDiscoveryCategoryExpander(t *testing.T) {
	tests := []struct {
		category       string
		serverResponse []*metav1.APIResourceList
		expected       []schema.GroupResource
	}{
		{
			// jobs 声明了 categories: all，因此应被展开。
			category: "all",
			serverResponse: []*metav1.APIResourceList{
				{
					GroupVersion: "batch/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "jobs",
							ShortNames: []string{"jz"},
							Categories: []string{"all"},
						},
					},
				},
			},
			expected: []schema.GroupResource{
				{
					Group:    "batch",
					Resource: "jobs",
				},
			},
		},
		{
			// 资源未声明任何分类，因此展开结果为空。
			category: "all",
			serverResponse: []*metav1.APIResourceList{
				{
					GroupVersion: "batch/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "jobs",
							ShortNames: []string{"jz"},
						},
					},
				},
			},
		},
		{
			// 请求不存在的分类，即使资源声明了其他分类也不应命中。
			category: "targaryens",
			serverResponse: []*metav1.APIResourceList{
				{
					GroupVersion: "batch/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "jobs",
							ShortNames: []string{"jz"},
							Categories: []string{"all"},
						},
					},
				},
			},
		},
	}

	dc := &fakeDiscoveryClient{}
	for _, test := range tests {
		// 每轮替换发现结果，验证展开器总是基于当前服务端状态计算。
		dc.serverResourcesHandler = func() ([]*metav1.APIResourceList, error) {
			return test.serverResponse, nil
		}
		expander := NewDiscoveryCategoryExpander(dc)
		expanded, _ := expander.Expand(test.category)
		if !reflect.DeepEqual(expanded, test.expected) {
			t.Errorf("expected %v, got %v", test.expected, expanded)
		}
	}

}
