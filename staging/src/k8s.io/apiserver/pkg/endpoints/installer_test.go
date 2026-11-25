/*
Copyright 2015 The Kubernetes Authors.

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

package endpoints

import (
	"testing"

	"github.com/stretchr/testify/require"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestIsVowel 验证用于生成 “a/an” 冠词的元音判断（大小写不敏感）。
func TestIsVowel(t *testing.T) {
	tests := []struct {
		name string
		arg  rune
		want bool
	}{
		{
			name: "yes",
			arg:  'E',
			want: true,
		},
		{
			name: "no",
			arg:  'n',
			want: false,
		},
	}
	for _, tt := range tests {
		if got := isVowel(tt.arg); got != tt.want {
			t.Errorf("%q. IsVowel() = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// TestGetArticleForNoun 验证为资源名词生成正确冠词：复数与不可数形式不添加冠词，
// 单数按首字母元音/辅音选择 a/an。
func TestGetArticleForNoun(t *testing.T) {
	tests := []struct {
		noun    string
		padding string
		want    string
	}{
		{
			noun:    "Frog",
			padding: " ",
			want:    " a ",
		},
		{
			noun:    "frogs",
			padding: " ",
			want:    " ",
		},
		{
			noun:    "apple",
			padding: "",
			want:    "an",
		},
		{
			noun:    "Apples",
			padding: " ",
			want:    " ",
		},
		{
			noun:    "Ingress",
			padding: " ",
			want:    " an ",
		},
		{
			noun:    "Class",
			padding: " ",
			want:    " a ",
		},
		{
			noun:    "S",
			padding: " ",
			want:    " a ",
		},
		{
			noun:    "O",
			padding: " ",
			want:    " an ",
		},
	}
	for _, tt := range tests {
		if got := GetArticleForNoun(tt.noun, tt.padding); got != tt.want {
			t.Errorf("%q. GetArticleForNoun() = %v, want %v", tt.noun, got, tt.want)
		}
	}
}

// TestConvertAPIResourceToDiscovery 验证把传统 metav1.APIResource 列表转换为
// 聚合发现 v2 的结构：子资源（name 含 "/"）需挂到父资源下，作用域必须与父资源一致，
// 缺少父资源时仍要生成占位项以避免下游 nil 解引用。
func TestConvertAPIResourceToDiscovery(t *testing.T) {
	tests := []struct {
		name                     string
		resources                []metav1.APIResource
		wantAPIResourceDiscovery []apidiscoveryv2.APIResourceDiscovery
		wantErr                  bool
	}{
		{
			name: "Basic Test",
			resources: []metav1.APIResource{
				{

					Name:       "pods",
					Namespaced: true,
					Kind:       "Pod",
					ShortNames: []string{"po"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "pods",
					Scope:    apidiscoveryv2.ScopeNamespace,
					ResponseKind: &metav1.GroupVersionKind{
						Kind: "Pod",
					},
					ShortNames: []string{"po"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
		},
		{
			name: "Basic Group Version Test",
			resources: []metav1.APIResource{
				{
					Name:       "cronjobs",
					Namespaced: true,
					Group:      "batch",
					Version:    "v1",
					Kind:       "CronJob",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "cronjobs",
					Scope:    apidiscoveryv2.ScopeNamespace,
					ResponseKind: &metav1.GroupVersionKind{
						Group:   "batch",
						Version: "v1",
						Kind:    "CronJob",
					},
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
		},
		{
			// 子资源 name 形如 "<resource>/<subresource>"，需拆分并归入父资源。
			name: "Test with subresource",
			resources: []metav1.APIResource{
				{
					Name:       "cronjobs",
					Namespaced: true,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
				{
					Name:       "cronjobs/status",
					Namespaced: true,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "cronjobs",
					Scope:    apidiscoveryv2.ScopeNamespace,
					ResponseKind: &metav1.GroupVersionKind{
						Group:   "batch",
						Version: "v1",
						Kind:    "CronJob",
					},
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					Subresources: []apidiscoveryv2.APISubresourceDiscovery{{
						Subresource: "status",
						ResponseKind: &metav1.GroupVersionKind{
							Group:   "batch",
							Version: "v1",
							Kind:    "CronJob",
						},
						Verbs: []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					}},
				},
			},
		},
		{
			name: "Test multiple resources and subresources",
			resources: []metav1.APIResource{
				{
					Name:       "cronjobs",
					Namespaced: true,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
				{
					Name:       "cronjobs/status",
					Namespaced: true,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
				{
					Name:       "deployments",
					Namespaced: true,
					Kind:       "Deployment",
					Group:      "apps",
					Version:    "v1",
					ShortNames: []string{"deploy"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
				{
					Name:       "deployments/status",
					Namespaced: true,
					Kind:       "Deployment",
					Group:      "apps",
					Version:    "v1",
					ShortNames: []string{"deploy"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "cronjobs",
					Scope:    apidiscoveryv2.ScopeNamespace,
					ResponseKind: &metav1.GroupVersionKind{
						Group:   "batch",
						Version: "v1",
						Kind:    "CronJob",
					},
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					Subresources: []apidiscoveryv2.APISubresourceDiscovery{{
						Subresource: "status",
						ResponseKind: &metav1.GroupVersionKind{
							Group:   "batch",
							Version: "v1",
							Kind:    "CronJob",
						},
						Verbs: []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					}},
				}, {
					Resource: "deployments",
					Scope:    apidiscoveryv2.ScopeNamespace,
					ResponseKind: &metav1.GroupVersionKind{
						Group:   "apps",
						Version: "v1",
						Kind:    "Deployment",
					},
					ShortNames: []string{"deploy"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					Subresources: []apidiscoveryv2.APISubresourceDiscovery{{
						Subresource: "status",
						ResponseKind: &metav1.GroupVersionKind{
							Group:   "apps",
							Version: "v1",
							Kind:    "Deployment",
						},
						Verbs: []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					}},
				},
			},
		}, {
			// 缺少父资源时仍生成父项（ResponseKind 为空），保证发现输出结构完整。
			name: "Test with subresource with no parent",
			resources: []metav1.APIResource{
				{
					Name:       "cronjobs/status",
					Namespaced: true,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "cronjobs",
					Scope:    apidiscoveryv2.ScopeNamespace,
					// 填充该字段以避免 nil panic
					ResponseKind: &metav1.GroupVersionKind{},
					Subresources: []apidiscoveryv2.APISubresourceDiscovery{{
						Subresource: "status",
						ResponseKind: &metav1.GroupVersionKind{
							Group:   "batch",
							Version: "v1",
							Kind:    "CronJob",
						},
						Verbs: []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					}},
				},
			},
		},
		{
			name: "Test with subresource with missing kind",
			resources: []metav1.APIResource{
				{
					Name:       "cronjobs/status",
					Namespaced: true,
					Group:      "batch",
					Version:    "v1",
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "cronjobs",
					Scope:    apidiscoveryv2.ScopeNamespace,
					// 填充该字段以避免 nil panic
					ResponseKind: &metav1.GroupVersionKind{},
					Subresources: []apidiscoveryv2.APISubresourceDiscovery{{
						Subresource: "status",
						// 填充该字段以避免 nil panic
						ResponseKind: &metav1.GroupVersionKind{},
						Verbs:        []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
					}},
				},
			},
		},
		{
			// 父资源与子资源的 Namespaced 不一致属于非法注册，应报错而非静默修正。
			name: "Test with mismatch parent and subresource scope",
			resources: []metav1.APIResource{
				{
					Name:       "cronjobs",
					Namespaced: true,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
				{
					Name:       "cronjobs/status",
					Namespaced: false,
					Kind:       "CronJob",
					Group:      "batch",
					Version:    "v1",
					ShortNames: []string{"cj"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{},
			wantErr:                  true,
		},
		{
			name: "Cluster Scope Test",
			resources: []metav1.APIResource{
				{
					Name:       "nodes",
					Namespaced: false,
					Kind:       "Node",
					ShortNames: []string{"no"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "nodes",
					Scope:    apidiscoveryv2.ScopeCluster,
					ResponseKind: &metav1.GroupVersionKind{
						Kind: "Node",
					},
					ShortNames: []string{"no"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
		},
		{
			name: "Namespace Scope Test",
			resources: []metav1.APIResource{
				{
					Name:       "nodes",
					Namespaced: true,
					Kind:       "Node",
					ShortNames: []string{"no"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource: "nodes",
					Scope:    apidiscoveryv2.ScopeNamespace,
					ResponseKind: &metav1.GroupVersionKind{
						Kind: "Node",
					},
					ShortNames: []string{"no"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
		},
		{
			name: "Singular Resource Name",
			resources: []metav1.APIResource{
				{
					Name:         "nodes",
					SingularName: "node",
					Kind:         "Node",
					ShortNames:   []string{"no"},
					Verbs:        []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
			wantAPIResourceDiscovery: []apidiscoveryv2.APIResourceDiscovery{
				{
					Resource:         "nodes",
					SingularResource: "node",
					Scope:            apidiscoveryv2.ScopeCluster,
					ResponseKind: &metav1.GroupVersionKind{
						Kind: "Node",
					},
					ShortNames: []string{"no"},
					Verbs:      []string{"create", "delete", "deletecollection", "get", "list", "patch", "update", "watch"},
				},
			},
		},
	}

	for _, tt := range tests {
		discoveryAPIResources, err := ConvertGroupVersionIntoToDiscovery(tt.resources)
		// 期望出错时不比较结果：部分用例返回空列表，部分可能返回已构造的中间结果。
		if err != nil {
			if tt.wantErr == false {
				t.Error(err)
			}
		} else {
			require.Equal(t, tt.wantAPIResourceDiscovery, discoveryAPIResources)
		}
	}
}
