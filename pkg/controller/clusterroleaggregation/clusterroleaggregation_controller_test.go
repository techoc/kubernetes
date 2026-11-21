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

package clusterroleaggregation

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	rbaclisters "k8s.io/client-go/listers/rbac/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"k8s.io/kubernetes/pkg/controller"
)

// TestSyncClusterRole 验证聚合 ClusterRole 的规则同步：只保留当前匹配选择器的源角色规则，
// 并按名称排序、去重，无变化时不发出 patch 请求。
func TestSyncClusterRole(t *testing.T) {
	// hammerRules / chiselRules / sawRules 提供不同名称与内容的规则集合，便于验证筛选与排序。
	hammerRules := func() []rbacv1.PolicyRule {
		return []rbacv1.PolicyRule{
			{Verbs: []string{"hammer"}, Resources: []string{"nails"}},
			{Verbs: []string{"hammer"}, Resources: []string{"wedges"}},
		}
	}
	chiselRules := func() []rbacv1.PolicyRule {
		return []rbacv1.PolicyRule{
			{Verbs: []string{"chisel"}, Resources: []string{"mortises"}},
		}
	}
	sawRules := func() []rbacv1.PolicyRule {
		return []rbacv1.PolicyRule{
			{Verbs: []string{"saw"}, Resources: []string{"boards"}},
		}
	}
	// role 构造一个普通的源 ClusterRole。
	role := func(name string, labels map[string]string, rules []rbacv1.PolicyRule) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
			Rules:      rules,
		}
	}
	// combinedRole 构造带 AggregationRule 的聚合 ClusterRole。
	combinedRole := func(selectors []map[string]string, rules ...[]rbacv1.PolicyRule) *rbacv1.ClusterRole {
		ret := &rbacv1.ClusterRole{
			ObjectMeta:      metav1.ObjectMeta{Name: "combined"},
			AggregationRule: &rbacv1.AggregationRule{},
		}
		for _, selector := range selectors {
			ret.AggregationRule.ClusterRoleSelectors = append(ret.AggregationRule.ClusterRoleSelectors,
				metav1.LabelSelector{MatchLabels: selector})
		}
		for _, currRules := range rules {
			ret.Rules = append(ret.Rules, currRules...)
		}
		return ret
	}

	// combinedApplyRole 构造期望的 apply 请求体，用于与 patch 动作的内容比对。
	combinedApplyRole := func(selectors []map[string]string, rules ...[]rbacv1.PolicyRule) *rbacv1ac.ClusterRoleApplyConfiguration {
		ret := rbacv1ac.ClusterRole("combined")

		var r []*rbacv1ac.PolicyRuleApplyConfiguration
		for _, currRules := range rules {
			r = append(r, toApplyPolicyRules(currRules)...)
		}
		ret.WithRules(r...)
		return ret
	}

	tests := []struct {
		// name 是用例名称；startingClusterRoles 是初始存在的 ClusterRole 集合；
		// clusterRoleToSync 是待同步的聚合角色名；expectedClusterRole 是期望的聚合结果（仅供参考）；
		// expectedClusterRoleApply 是期望的 apply 请求体，nil 表示不应发出请求。
		name                     string
		startingClusterRoles     []*rbacv1.ClusterRole
		clusterRoleToSync        string
		expectedClusterRole      *rbacv1.ClusterRole
		expectedClusterRoleApply *rbacv1ac.ClusterRoleApplyConfiguration
	}{
		{
			name: "remove dead rules",
			startingClusterRoles: []*rbacv1.ClusterRole{
				role("hammer", map[string]string{"foo": "bar"}, hammerRules()),
				combinedRole([]map[string]string{{"foo": "bar"}}, sawRules()),
			},
			clusterRoleToSync:        "combined",
			expectedClusterRole:      combinedRole([]map[string]string{{"foo": "bar"}}, hammerRules()),
			expectedClusterRoleApply: combinedApplyRole([]map[string]string{{"foo": "bar"}}, hammerRules()),
		},
		{
			name: "strip rules",
			startingClusterRoles: []*rbacv1.ClusterRole{
				role("hammer", map[string]string{"foo": "not-bar"}, hammerRules()),
				combinedRole([]map[string]string{{"foo": "bar"}}, hammerRules()),
			},
			clusterRoleToSync:        "combined",
			expectedClusterRole:      combinedRole([]map[string]string{{"foo": "bar"}}),
			expectedClusterRoleApply: combinedApplyRole([]map[string]string{{"foo": "bar"}}),
		},
		{
			name: "select properly and put in order",
			startingClusterRoles: []*rbacv1.ClusterRole{
				role("hammer", map[string]string{"foo": "bar"}, hammerRules()),
				role("chisel", map[string]string{"foo": "bar"}, chiselRules()),
				role("saw", map[string]string{"foo": "not-bar"}, sawRules()),
				combinedRole([]map[string]string{{"foo": "bar"}}),
			},
			clusterRoleToSync:        "combined",
			expectedClusterRole:      combinedRole([]map[string]string{{"foo": "bar"}}, chiselRules(), hammerRules()),
			expectedClusterRoleApply: combinedApplyRole([]map[string]string{{"foo": "bar"}}, chiselRules(), hammerRules()),
		},
		{
			name: "select properly with multiple selectors",
			startingClusterRoles: []*rbacv1.ClusterRole{
				role("hammer", map[string]string{"foo": "bar"}, hammerRules()),
				role("chisel", map[string]string{"foo": "bar"}, chiselRules()),
				role("saw", map[string]string{"foo": "not-bar"}, sawRules()),
				combinedRole([]map[string]string{{"foo": "bar"}, {"foo": "not-bar"}}),
			},
			clusterRoleToSync:        "combined",
			expectedClusterRole:      combinedRole([]map[string]string{{"foo": "bar"}, {"foo": "not-bar"}}, chiselRules(), hammerRules(), sawRules()),
			expectedClusterRoleApply: combinedApplyRole([]map[string]string{{"foo": "bar"}, {"foo": "not-bar"}}, chiselRules(), hammerRules(), sawRules()),
		},
		{
			name: "select properly remove duplicates",
			startingClusterRoles: []*rbacv1.ClusterRole{
				role("hammer", map[string]string{"foo": "bar"}, hammerRules()),
				role("chisel", map[string]string{"foo": "bar"}, chiselRules()),
				role("saw", map[string]string{"foo": "bar"}, sawRules()),
				role("other-saw", map[string]string{"foo": "not-bar"}, sawRules()),
				combinedRole([]map[string]string{{"foo": "bar"}, {"foo": "not-bar"}}),
			},
			clusterRoleToSync:        "combined",
			expectedClusterRole:      combinedRole([]map[string]string{{"foo": "bar"}, {"foo": "not-bar"}}, chiselRules(), hammerRules(), sawRules()),
			expectedClusterRoleApply: combinedApplyRole([]map[string]string{{"foo": "bar"}, {"foo": "not-bar"}}, chiselRules(), hammerRules(), sawRules()),
		},
		{
			name: "no diff skip",
			startingClusterRoles: []*rbacv1.ClusterRole{
				role("hammer", map[string]string{"foo": "bar"}, hammerRules()),
				combinedRole([]map[string]string{{"foo": "bar"}}, hammerRules()),
			},
			clusterRoleToSync:        "combined",
			expectedClusterRoleApply: nil,
		}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			indexer := cache.NewIndexer(controller.KeyFunc, cache.Indexers{})
			objs := []runtime.Object{}
			for _, obj := range test.startingClusterRoles {
				objs = append(objs, obj)
				if err := indexer.Add(obj); err != nil {
					t.Fatalf("failed to add object to indexer: %v", err)
				}
			}
			fakeClient := fakeclient.NewClientset(objs...)

			// 默认 reactor 不支持 apply，这里注册一个简单 reactor 使 patch 动作成功返回。
			fakeClient.PrependReactor("patch", "clusterroles", func(action clienttesting.Action) (handled bool, ret runtime.Object, err error) {
				// 聚合控制器不关心 patch 返回的对象，无需构造返回值。
				return true, nil, nil
			})
			c := ClusterRoleAggregationController{
				clusterRoleClient: fakeClient.RbacV1(),
				clusterRoleLister: rbaclisters.NewClusterRoleLister(indexer),
			}
			err := c.syncClusterRole(context.TODO(), test.clusterRoleToSync)
			if err != nil {
				t.Fatal(err)
			}

			// 期望无变更时，不应产生任何 API 操作。
			if test.expectedClusterRoleApply == nil {
				if len(fakeClient.Actions()) != 0 {
					t.Fatalf("unexpected actions %#v", fakeClient.Actions())
				}
				return
			}

			expectedActions := 1
			if len(fakeClient.Actions()) != expectedActions {
				t.Fatalf("unexpected actions %#v", fakeClient.Actions())
			}

			// 有变更时应恰好发出一个 patch（apply）动作。
			action := fakeClient.Actions()[0]
			if !action.Matches("patch", "clusterroles") {
				t.Fatalf("unexpected action %#v", action)
			}
			applyAction, ok := action.(clienttesting.PatchAction)
			if !ok {
				t.Fatalf("unexpected action %#v", action)
			}
			// apply 请求体以 YAML 编码在 patch 中，反序列化后做语义比较。
			ac := &rbacv1ac.ClusterRoleApplyConfiguration{}
			if err := yaml.Unmarshal(applyAction.GetPatch(), ac); err != nil {
				t.Fatalf("error unmarshalling apply request: %v", err)
			}
			if !equality.Semantic.DeepEqual(ac, test.expectedClusterRoleApply) {
				t.Fatalf("%v", cmp.Diff(test.expectedClusterRoleApply, ac))
			}
		})
	}
}
