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

package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	apidiscovery "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestSplitGroupsAndResources 验证聚合发现（apidiscovery/v2）到传统
// APIGroupList/APIResourceList 结构的拆分逻辑。每个用例同时断言三部分结果：
// 组列表、按 GV 归并的资源列表，以及失效 GV 的错误集合。
func TestSplitGroupsAndResources(t *testing.T) {
	// 每个用例的 agg 是服务端返回的聚合发现数据，其余三个字段是期望的拆分结果。
	tests := []struct {
		name                string
		agg                 apidiscovery.APIGroupDiscoveryList
		expectedGroups      metav1.APIGroupList
		expectedGVResources map[schema.GroupVersion]*metav1.APIResourceList
		expectedFailedGVs   map[schema.GroupVersion]error
	}{
		{
			// 核心组（group 为空）只有一个版本和一个命名空间级资源。
			name: "Aggregated discovery: core/v1 group and pod resource",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "v1",
							Version:      "v1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "", Version: "v1"}: {
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "pods",
							Namespaced: true,
							Group:      "",
							Version:    "v1",
							Kind:       "Pod",
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 一个组含多个版本时，首选版本取聚合列表中的第一个非 stale 版本。
			name: "Aggregated discovery: 1 group/1 resources at /api, 1 group/2 versions/1 resources at /apis",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v2",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v2",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "apps/v2",
								Version:      "v2",
							},
							{
								GroupVersion: "apps/v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "apps/v2",
							Version:      "v2",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "apps", Version: "v1"}: {
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "deployments",
							Namespaced: true,
							Group:      "apps",
							Version:    "v1",
							Kind:       "Deployment",
						},
					},
				},
				{Group: "apps", Version: "v2"}: {
					GroupVersion: "apps/v2",
					APIResources: []metav1.APIResource{
						{
							Name:       "deployments",
							Namespaced: true,
							Group:      "apps",
							Version:    "v2",
							Kind:       "Deployment",
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 核心组与 apps 组各自独立归并，资源列表按 GV 分开返回。
			name: "Aggregated discovery: 1 group/2 resources at /api, 1 group/2 resources at /apis",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "services",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Service",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "StatefulSet",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "v1",
							Version:      "v1",
						},
					},
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "apps/v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "apps/v1",
							Version:      "v1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "", Version: "v1"}: {
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "pods",
							Namespaced: true,
							Group:      "",
							Version:    "v1",
							Kind:       "Pod",
						},
						{
							Name:       "services",
							Namespaced: true,
							Group:      "",
							Version:    "v1",
							Kind:       "Service",
						},
					},
				},
				{Group: "apps", Version: "v1"}: {
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "deployments",
							Namespaced: true,
							Group:      "apps",
							Version:    "v1",
							Kind:       "Deployment",
						},
						{
							Name:       "statefulsets",
							Namespaced: true,
							Group:      "apps",
							Version:    "v1",
							Kind:       "StatefulSet",
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 集群级资源（ScopeCluster）必须映射为 Namespaced=false。
			name: "Aggregated discovery: multiple groups with cluster-scoped resources",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "namespaces",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Namespace",
										},
										Scope: apidiscovery.ScopeCluster,
									},
								},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "rbac.authorization.k8s.io",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "roles",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "rbac.authorization.k8s.io",
											Version: "v1",
											Kind:    "Role",
										},
										Scope: apidiscovery.ScopeCluster,
									},
									{
										Resource: "clusterroles",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "rbac.authorization.k8s.io",
											Version: "v1",
											Kind:    "ClusterRole",
										},
										Scope: apidiscovery.ScopeCluster,
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "v1",
							Version:      "v1",
						},
					},
					{
						Name: "rbac.authorization.k8s.io",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "rbac.authorization.k8s.io/v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "rbac.authorization.k8s.io/v1",
							Version:      "v1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "", Version: "v1"}: {
					GroupVersion: "v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "pods",
							Namespaced: true,
							Group:      "",
							Version:    "v1",
							Kind:       "Pod",
						},
						{
							Name:       "namespaces",
							Namespaced: false,
							Group:      "",
							Version:    "v1",
							Kind:       "Namespace",
						},
					},
				},
				{Group: "rbac.authorization.k8s.io", Version: "v1"}: {
					GroupVersion: "rbac.authorization.k8s.io/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "roles",
							Namespaced: false,
							Group:      "rbac.authorization.k8s.io",
							Version:    "v1",
							Kind:       "Role",
						},
						{
							Name:       "clusterroles",
							Namespaced: false,
							Group:      "rbac.authorization.k8s.io",
							Version:    "v1",
							Kind:       "ClusterRole",
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 子资源会被展开成 "parent/subresource" 形式的独立 APIResource；
			// 父资源的 shortNames/categories/verbs 只在父条目上出现。
			name: "Aggregated discovery with single subresource",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope:            apidiscovery.ScopeNamespace,
										SingularResource: "deployment",
										ShortNames:       []string{"deploy"},
										Verbs:            []string{"parentverb1", "parentverb2", "parentverb3", "parentverb4"},
										Categories:       []string{"all", "testcategory"},
										Subresources: []apidiscovery.APISubresourceDiscovery{
											{
												Subresource: "scale",
												ResponseKind: &metav1.GroupVersionKind{
													Group:   "apps",
													Version: "v1",
													Kind:    "Deployment",
												},
												Verbs: []string{"get", "patch", "update"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "apps/v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "apps/v1",
							Version:      "v1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "apps", Version: "v1"}: {
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{
							Name:         "deployments",
							SingularName: "deployment",
							Namespaced:   true,
							Group:        "apps",
							Version:      "v1",
							Kind:         "Deployment",
							Verbs:        []string{"parentverb1", "parentverb2", "parentverb3", "parentverb4"},
							ShortNames:   []string{"deploy"},
							Categories:   []string{"all", "testcategory"},
						},
						{
							Name:         "deployments/scale",
							SingularName: "deployment",
							Namespaced:   true,
							Group:        "apps",
							Version:      "v1",
							Kind:         "Deployment",
							Verbs:        []string{"get", "patch", "update"},
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 父资源缺少 GVK 时仍需容忍：只返回子资源，父条目被跳过。
			name: "Aggregated discovery with single subresource and parent missing GVK",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "external.metrics.k8s.io",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1beta1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										// 父资源 GVK 为 nil 时也要能容忍。
										Resource:         "*",
										Scope:            apidiscovery.ScopeNamespace,
										SingularResource: "",
										Subresources: []apidiscovery.APISubresourceDiscovery{
											{
												Subresource: "other-external-metric",
												ResponseKind: &metav1.GroupVersionKind{
													Kind: "MetricValueList",
												},
												Verbs: []string{"get"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "external.metrics.k8s.io",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "external.metrics.k8s.io/v1beta1",
								Version:      "v1beta1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "external.metrics.k8s.io/v1beta1",
							Version:      "v1beta1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "external.metrics.k8s.io", Version: "v1beta1"}: {
					GroupVersion: "external.metrics.k8s.io/v1beta1",
					APIResources: []metav1.APIResource{
						// 父资源 GVK 为 nil，因此不返回父条目，只返回子资源。
						{
							Name:         "*/other-external-metric",
							SingularName: "",
							Namespaced:   true,
							Group:        "",
							Version:      "",
							Kind:         "MetricValueList",
							Verbs:        []string{"get"},
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 父 GVK 为空与非 nil 但字段全空等价，处理方式与缺少 GVK 相同。
			name: "Aggregated discovery with single subresource and parent empty GVK",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "external.metrics.k8s.io",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1beta1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										// 父资源 GVK 为空时也要能容忍。
										Resource:         "*",
										Scope:            apidiscovery.ScopeNamespace,
										SingularResource: "",
										ResponseKind:     &metav1.GroupVersionKind{},
										Subresources: []apidiscovery.APISubresourceDiscovery{
											{
												Subresource: "other-external-metric",
												ResponseKind: &metav1.GroupVersionKind{
													Kind: "MetricValueList",
												},
												Verbs: []string{"get"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "external.metrics.k8s.io",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "external.metrics.k8s.io/v1beta1",
								Version:      "v1beta1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "external.metrics.k8s.io/v1beta1",
							Version:      "v1beta1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "external.metrics.k8s.io", Version: "v1beta1"}: {
					GroupVersion: "external.metrics.k8s.io/v1beta1",
					APIResources: []metav1.APIResource{
						// 父资源 GVK 为 nil，因此不返回父条目，只返回子资源。
						{
							Name:         "*/other-external-metric",
							SingularName: "",
							Namespaced:   true,
							Group:        "",
							Version:      "",
							Kind:         "MetricValueList",
							Verbs:        []string{"get"},
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 多个子资源保持聚合列表中的顺序，父资源仍排在最前。
			name: "Aggregated discovery with multiple subresources",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope:            apidiscovery.ScopeNamespace,
										SingularResource: "deployment",
										Subresources: []apidiscovery.APISubresourceDiscovery{
											{
												Subresource: "scale",
												ResponseKind: &metav1.GroupVersionKind{
													Group:   "apps",
													Version: "v1",
													Kind:    "Deployment",
												},
												Verbs: []string{"get", "patch", "update"},
											},
											{
												Subresource: "status",
												ResponseKind: &metav1.GroupVersionKind{
													Group:   "apps",
													Version: "v1",
													Kind:    "Deployment",
												},
												Verbs: []string{"get", "patch", "update"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "apps/v1",
								Version:      "v1",
							},
						},
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "apps/v1",
							Version:      "v1",
						},
					},
				},
			},
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "apps", Version: "v1"}: {
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{
							Name:         "deployments",
							SingularName: "deployment",
							Namespaced:   true,
							Group:        "apps",
							Version:      "v1",
							Kind:         "Deployment",
						},
						{
							Name:         "deployments/scale",
							SingularName: "deployment",
							Namespaced:   true,
							Group:        "apps",
							Version:      "v1",
							Kind:         "Deployment",
							Verbs:        []string{"get", "patch", "update"},
						},
						{
							Name:         "deployments/status",
							SingularName: "deployment",
							Namespaced:   true,
							Group:        "apps",
							Version:      "v1",
							Kind:         "Deployment",
							Verbs:        []string{"get", "patch", "update"},
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{},
		},
		{
			// 唯一的核心 GV 已失效：组仍保留但无版本，资源为空，并报告失败 GV。
			name: "Aggregated discovery: single failed GV at /api",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "pods",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Pod",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "services",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "",
											Version: "v1",
											Kind:    "Service",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
						},
					},
				},
			},
			// 核心组唯一的 GroupVersion 已失效，因此组内没有可用版本。
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{{Name: ""}},
			},
			// 核心组唯一的 GroupVersion 已失效，因此不应返回任何资源。
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{},
			expectedFailedGVs: map[schema.GroupVersion]error{
				{Group: "", Version: "v1"}: StaleGroupVersionError{gv: schema.GroupVersion{Group: "", Version: "v1"}},
			},
		},
		{
			// apps 组唯一版本失效，行为与上例一致。
			name: "Aggregated discovery: single failed GV at /apis",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
									{
										Resource: "statefulsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "StatefulSets",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
						},
					},
				},
			},
			// apps/v1 是组内唯一 GroupVersion 且已失效，因此组内没有可用版本。
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{{Name: "apps"}},
			},
			// apps/v1 是唯一 GroupVersion 且已失效，因此不应返回任何资源。
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{},
			expectedFailedGVs: map[schema.GroupVersion]error{
				{Group: "apps", Version: "v1"}: StaleGroupVersionError{gv: schema.GroupVersion{Group: "apps", Version: "v1"}},
			},
		},
		{
			// 同一组中只有部分版本失效：失效版本不出现在组内，也不产生资源；
			// 首选版本回退到仍有效的 v1。
			name: "Aggregated discovery: 1 group/2 versions/1 failed GV at /apis",
			agg: apidiscovery.APIGroupDiscoveryList{
				Items: []apidiscovery.APIGroupDiscovery{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name: "apps",
						},
						Versions: []apidiscovery.APIVersionDiscovery{
							// v2 已失效，应作为失败 GV 上报。
							{
								Version: "v2",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "daemonsets",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v2",
											Kind:    "DaemonSets",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
								Freshness: apidiscovery.DiscoveryFreshnessStale,
							},
							{
								Version: "v1",
								Resources: []apidiscovery.APIResourceDiscovery{
									{
										Resource: "deployments",
										ResponseKind: &metav1.GroupVersionKind{
											Group:   "apps",
											Version: "v1",
											Kind:    "Deployment",
										},
										Scope: apidiscovery.ScopeNamespace,
									},
								},
							},
						},
					},
				},
			},
			// 只有 apps/v1 未失效，应作为唯一的 GroupVersion。
			expectedGroups: metav1.APIGroupList{
				Groups: []metav1.APIGroup{
					{
						Name: "apps",
						Versions: []metav1.GroupVersionForDiscovery{
							{
								GroupVersion: "apps/v1",
								Version:      "v1",
							},
						},
						// 首选版本必须是仍有效的 apps/v1。
						PreferredVersion: metav1.GroupVersionForDiscovery{
							GroupVersion: "apps/v1",
							Version:      "v1",
						},
					},
				},
			},
			// 只应返回 apps/v1 的资源。
			expectedGVResources: map[schema.GroupVersion]*metav1.APIResourceList{
				{Group: "apps", Version: "v1"}: {
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{
							Name:       "deployments",
							Namespaced: true,
							Group:      "apps",
							Version:    "v1",
							Kind:       "Deployment",
						},
					},
				},
			},
			expectedFailedGVs: map[schema.GroupVersion]error{
				{Group: "apps", Version: "v2"}: StaleGroupVersionError{gv: schema.GroupVersion{Group: "apps", Version: "v2"}},
			},
		},
	}

	for _, test := range tests {
		// SplitGroupsAndResources 返回的组列表是指针，可能为 nil，这里按约定解引用后比较。
		apiGroups, resourcesByGV, failedGVs := SplitGroupsAndResources(test.agg)
		assert.Equal(t, test.expectedFailedGVs, failedGVs)
		assert.Equal(t, test.expectedGroups, *apiGroups)
		assert.Equal(t, test.expectedGVResources, resourcesByGV)
	}
}
