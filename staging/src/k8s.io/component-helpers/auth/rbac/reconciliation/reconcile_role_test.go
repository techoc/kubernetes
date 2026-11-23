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

package reconciliation

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 这些小工厂把用例压缩到只关心“期望/实际/是否 union”三个变量，
// 避免每加一个边界用例就重复写一整坨 ClusterRole 字面量。
func role(rules []rbacv1.PolicyRule, labels map[string]string, annotations map[string]string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		Rules:      rules,
		ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
	}
}

// rules 为每个资源生成一个 get 规则，方便在断言里只比较资源集合，
// 而不用重复声明 API 组和动词；这些字段本来也不是本测试要验证的重点。
func rules(resources ...string) []rbacv1.PolicyRule {
	r := []rbacv1.PolicyRule{}
	for _, resource := range resources {
		r = append(r, rbacv1.PolicyRule{APIGroups: []string{""}, Verbs: []string{"get"}, Resources: []string{resource}})
	}
	return r
}

// ss 只是 map[string]string 的缩写，让表驱动用例的期望值更易读。
type ss map[string]string

// TestComputeReconciledRoleRules 对比两种对账策略：严格模式下实际角色要被改写成
// 期望状态，union 模式下则保留实际角色已有的额外权限；标签和注解并非权限，
// 所以两种模式下都取并集，且键冲突时以实际对象的值为准，避免覆盖别人的声明。
func TestComputeReconciledRoleRules(t *testing.T) {
	tests := map[string]struct {
		expectedRole           *rbacv1.ClusterRole
		actualRole             *rbacv1.ClusterRole
		removeExtraPermissions bool

		expectedReconciledRole       *rbacv1.ClusterRole
		expectedReconciliationNeeded bool
	}{
		"empty": {
			expectedRole:           role(rules(), nil, nil),
			actualRole:             role(rules(), nil, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"match without union": {
			expectedRole:           role(rules("a"), nil, nil),
			actualRole:             role(rules("a"), nil, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"match with union": {
			expectedRole:           role(rules("a"), nil, nil),
			actualRole:             role(rules("a"), nil, nil),
			removeExtraPermissions: false,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"different rules without union": {
			expectedRole:           role(rules("a"), nil, nil),
			actualRole:             role(rules("b"), nil, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), nil, nil),
			expectedReconciliationNeeded: true,
		},
		"different rules with union": {
			expectedRole:           role(rules("a"), nil, nil),
			actualRole:             role(rules("b"), nil, nil),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("b", "a"), nil, nil),
			expectedReconciliationNeeded: true,
		},
		"match labels without union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("a"), ss{"1": "a"}, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"match labels with union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("a"), ss{"1": "a"}, nil),
			removeExtraPermissions: false,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"different labels without union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("a"), ss{"2": "b"}, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), ss{"1": "a", "2": "b"}, nil),
			expectedReconciliationNeeded: true,
		},
		"different labels with union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("a"), ss{"2": "b"}, nil),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("a"), ss{"1": "a", "2": "b"}, nil),
			expectedReconciliationNeeded: true,
		},
		"different labels and rules without union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("b"), ss{"2": "b"}, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), ss{"1": "a", "2": "b"}, nil),
			expectedReconciliationNeeded: true,
		},
		"different labels and rules with union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("b"), ss{"2": "b"}, nil),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("b", "a"), ss{"1": "a", "2": "b"}, nil),
			expectedReconciliationNeeded: true,
		},
		// 同一个键在期望和实际对象里值不同时，以实际对象为准；
		// 这是元数据的归属约定，与权限规则按 union/严格模式切换互不影响。
		"conflicting labels and rules without union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("b"), ss{"1": "b"}, nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), ss{"1": "b"}, nil),
			expectedReconciliationNeeded: true,
		},
		"conflicting labels and rules with union": {
			expectedRole:           role(rules("a"), ss{"1": "a"}, nil),
			actualRole:             role(rules("b"), ss{"1": "b"}, nil),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("b", "a"), ss{"1": "b"}, nil),
			expectedReconciliationNeeded: true,
		},
		"match annotations without union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("a"), nil, ss{"1": "a"}),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"match annotations with union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("a"), nil, ss{"1": "a"}),
			removeExtraPermissions: false,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"different annotations without union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("a"), nil, ss{"2": "b"}),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), nil, ss{"1": "a", "2": "b"}),
			expectedReconciliationNeeded: true,
		},
		"different annotations with union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("a"), nil, ss{"2": "b"}),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("a"), nil, ss{"1": "a", "2": "b"}),
			expectedReconciliationNeeded: true,
		},
		"different annotations and rules without union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("b"), nil, ss{"2": "b"}),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), nil, ss{"1": "a", "2": "b"}),
			expectedReconciliationNeeded: true,
		},
		"different annotations and rules with union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("b"), nil, ss{"2": "b"}),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("b", "a"), nil, ss{"1": "a", "2": "b"}),
			expectedReconciliationNeeded: true,
		},
		"conflicting annotations and rules without union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("b"), nil, ss{"1": "b"}),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), nil, ss{"1": "b"}),
			expectedReconciliationNeeded: true,
		},
		"conflicting annotations and rules with union": {
			expectedRole:           role(rules("a"), nil, ss{"1": "a"}),
			actualRole:             role(rules("b"), nil, ss{"1": "b"}),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("b", "a"), nil, ss{"1": "b"}),
			expectedReconciliationNeeded: true,
		},
		"conflicting labels/annotations and rules without union": {
			expectedRole:           role(rules("a"), ss{"3": "d"}, ss{"1": "a"}),
			actualRole:             role(rules("b"), ss{"4": "e"}, ss{"1": "b"}),
			removeExtraPermissions: true,

			expectedReconciledRole:       role(rules("a"), ss{"3": "d", "4": "e"}, ss{"1": "b"}),
			expectedReconciliationNeeded: true,
		},
		"conflicting labels/annotations and rules with union": {
			expectedRole:           role(rules("a"), ss{"3": "d"}, ss{"1": "a"}),
			actualRole:             role(rules("b"), ss{"4": "e"}, ss{"1": "b"}),
			removeExtraPermissions: false,

			expectedReconciledRole:       role(rules("b", "a"), ss{"3": "d", "4": "e"}, ss{"1": "b"}),
			expectedReconciliationNeeded: true,
		},
		// 混合多个资源、标签和注解，确认并集只做“补齐缺失键”，不会改写已有值。
		"complex labels/annotations and rules without union": {
			expectedRole:           role(rules("pods", "nodes", "secrets"), ss{"env": "prod", "color": "blue"}, ss{"description": "fancy", "system": "true"}),
			actualRole:             role(rules("nodes", "images", "projects"), ss{"color": "red", "team": "pm"}, ss{"system": "false", "owner": "admin", "vip": "yes"}),
			removeExtraPermissions: true,

			expectedReconciledRole: role(
				rules("pods", "nodes", "secrets"),
				ss{"env": "prod", "color": "red", "team": "pm"},
				ss{"description": "fancy", "system": "false", "owner": "admin", "vip": "yes"}),
			expectedReconciliationNeeded: true,
		},
		"complex labels/annotations and rules with union": {
			expectedRole:           role(rules("pods", "nodes", "secrets"), ss{"env": "prod", "color": "blue", "manager": "randy"}, ss{"description": "fancy", "system": "true", "up": "true"}),
			actualRole:             role(rules("nodes", "images", "projects"), ss{"color": "red", "team": "pm"}, ss{"system": "false", "owner": "admin", "vip": "yes", "rate": "down"}),
			removeExtraPermissions: false,

			expectedReconciledRole: role(
				rules("nodes", "images", "projects", "pods", "secrets"),
				ss{"env": "prod", "manager": "randy", "color": "red", "team": "pm"},
				ss{"description": "fancy", "system": "false", "owner": "admin", "vip": "yes", "rate": "down", "up": "true"}),
			expectedReconciliationNeeded: true,
		},
	}

	for k, tc := range tests {
		actualRole := ClusterRoleRuleOwner{ClusterRole: tc.actualRole}
		expectedRole := ClusterRoleRuleOwner{ClusterRole: tc.expectedRole}
		result, err := computeReconciledRole(actualRole, expectedRole, tc.removeExtraPermissions)
		if err != nil {
			t.Errorf("%s: %v", k, err)
			continue
		}
		reconciliationNeeded := result.Operation != ReconcileNone
		if reconciliationNeeded != tc.expectedReconciliationNeeded {
			t.Errorf("%s: Expected\n\t%v\ngot\n\t%v", k, tc.expectedReconciliationNeeded, reconciliationNeeded)
			continue
		}
		if reconciliationNeeded && !apiequality.Semantic.DeepEqual(result.Role.(ClusterRoleRuleOwner).ClusterRole, tc.expectedReconciledRole) {
			t.Errorf("%s: Expected\n\t%#v\ngot\n\t%#v", k, tc.expectedReconciledRole, result.Role)
		}
	}
}

// 聚合角色由 controller-manager 根据选择器回填规则，因此对账时不能拿它的 Rules
// 当真；这两个工厂让用例只声明聚合选择器，便于验证“期望与实际的聚合状态不一致时该如何收敛”。
func aggregatedRole(aggregationRule *rbacv1.AggregationRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		AggregationRule: aggregationRule,
	}
}

// aggregationrule 把若干 selector 列表包装成聚合规则，供聚合用例声明期望/实际的选择器集合。
func aggregationrule(selectors []map[string]string) *rbacv1.AggregationRule {
	ret := &rbacv1.AggregationRule{}
	for _, selector := range selectors {
		ret.ClusterRoleSelectors = append(ret.ClusterRoleSelectors,
			metav1.LabelSelector{MatchLabels: selector})
	}
	return ret
}

// TestComputeReconciledRoleAggregationRules 验证聚合状态切换：期望为普通角色而实际是
// 聚合角色时，必须清掉 AggregationRule，否则控制器会不断把规则改回去造成抖动；
// 实际角色上残留的规则按 union 策略保留，避免直接删掉别人仍在使用的权限。
func TestComputeReconciledRoleAggregationRules(t *testing.T) {
	tests := map[string]struct {
		expectedRole           *rbacv1.ClusterRole
		actualRole             *rbacv1.ClusterRole
		removeExtraPermissions bool

		expectedReconciledRole       *rbacv1.ClusterRole
		expectedReconciliationNeeded bool
	}{
		"empty": {
			expectedRole:           aggregatedRole(&rbacv1.AggregationRule{}),
			actualRole:             aggregatedRole(nil),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"empty-2": {
			expectedRole:           aggregatedRole(&rbacv1.AggregationRule{}),
			actualRole:             aggregatedRole(&rbacv1.AggregationRule{}),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"match without union": {
			expectedRole:           aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			actualRole:             aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			removeExtraPermissions: true,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"match with union": {
			expectedRole:           aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			actualRole:             aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			removeExtraPermissions: false,

			expectedReconciledRole:       nil,
			expectedReconciliationNeeded: false,
		},
		"different rules without union": {
			expectedRole:           aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			actualRole:             aggregatedRole(aggregationrule([]map[string]string{{"alpha": "bravo"}})),
			removeExtraPermissions: true,

			expectedReconciledRole:       aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			expectedReconciliationNeeded: true,
		},
		"different rules with union": {
			expectedRole:           aggregatedRole(aggregationrule([]map[string]string{{"foo": "bar"}})),
			actualRole:             aggregatedRole(aggregationrule([]map[string]string{{"alpha": "bravo"}})),
			removeExtraPermissions: false,

			expectedReconciledRole:       aggregatedRole(aggregationrule([]map[string]string{{"alpha": "bravo"}, {"foo": "bar"}})),
			expectedReconciliationNeeded: true,
		},
		"unexpected aggregation": {
			// 期望的是普通角色（无聚合规则）
			expectedRole: role(rules("pods", "nodes", "secrets"), nil, nil),
			// 实际却是聚合角色
			actualRole:             aggregatedRole(aggregationrule([]map[string]string{{"alpha": "bravo"}})),
			removeExtraPermissions: false,

			// 调和结果应保留期望权限且不再是聚合角色
			expectedReconciledRole:       role(rules("pods", "nodes", "secrets"), nil, nil),
			expectedReconciliationNeeded: true,
		},
		// 实际角色既有聚合规则又有自定义规则，说明有人手动塞过权限；
		// 去掉聚合状态后这些权限应该保留，和普通角色的 union 行为保持一致。
		"unexpected aggregation with differing permissions": {
			// 期望的是普通角色（无聚合规则）
			expectedRole: role(rules("pods", "nodes", "secrets"), nil, nil),
			// 实际是聚合角色，且还带有手工添加的其他权限
			actualRole: func() *rbacv1.ClusterRole {
				r := aggregatedRole(aggregationrule([]map[string]string{{"alpha": "bravo"}}))
				r.Rules = rules("deployments")
				return r
			}(),
			removeExtraPermissions: false,

			// 调和结果应去掉聚合规则，保留手工权限"deployments"，并补上期望的权限
			expectedReconciledRole:       role(rules("deployments", "pods", "nodes", "secrets"), nil, nil),
			expectedReconciliationNeeded: true,
		},
	}

	for k, tc := range tests {
		actualRole := ClusterRoleRuleOwner{ClusterRole: tc.actualRole}
		expectedRole := ClusterRoleRuleOwner{ClusterRole: tc.expectedRole}
		result, err := computeReconciledRole(actualRole, expectedRole, tc.removeExtraPermissions)
		if err != nil {
			t.Errorf("%s: %v", k, err)
			continue
		}
		reconciliationNeeded := result.Operation != ReconcileNone
		if reconciliationNeeded != tc.expectedReconciliationNeeded {
			t.Errorf("%s: Expected\n\t%v\ngot\n\t%v", k, tc.expectedReconciliationNeeded, reconciliationNeeded)
			continue
		}
		if reconciliationNeeded && !apiequality.Semantic.DeepEqual(result.Role.(ClusterRoleRuleOwner).ClusterRole, tc.expectedReconciledRole) {
			t.Errorf("%s: expected %#v, got %#v", k, tc.expectedReconciledRole, result.Role.(ClusterRoleRuleOwner).ClusterRole)
		}
	}
}
