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

package policy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/utils/ptr"
)

// 测试夹具：tim 是普通用户，attrs/rules 覆盖命名空间、集群、非资源、子资源等各类请求。
var (
	tim = &user.DefaultInfo{
		Name:   "tim@k8s.io",
		Groups: []string{"humans", "developers"},
	}
	// attrs 以名字索引请求属性，test() 通过名字选取，便于用例表达“某类请求命中某条规则”。
	attrs = map[string]authorizer.Attributes{
		"namespaced": &authorizer.AttributesRecord{
			User:            tim,
			Verb:            "get",
			Namespace:       "default",
			APIGroup:        "", //Core
			APIVersion:      "v1",
			Resource:        "pods",
			Name:            "busybox",
			ResourceRequest: true,
			Path:            "/api/v1/namespaces/default/pods/busybox",
		},
		"cluster": &authorizer.AttributesRecord{
			User:            tim,
			Verb:            "get",
			APIGroup:        "rbac.authorization.k8s.io", //Core
			APIVersion:      "v1beta1",
			Resource:        "clusterroles",
			Name:            "edit",
			ResourceRequest: true,
			Path:            "/apis/rbac.authorization.k8s.io/v1beta1/clusterroles/edit",
		},
		"nonResource": &authorizer.AttributesRecord{
			User:            tim,
			Verb:            "get",
			ResourceRequest: false,
			Path:            "/logs/kubelet.log",
		},
		"subresource": &authorizer.AttributesRecord{
			User:            tim,
			Verb:            "get",
			Namespace:       "default",
			APIGroup:        "", //Core
			APIVersion:      "v1",
			Resource:        "pods",
			Subresource:     "log",
			Name:            "busybox",
			ResourceRequest: true,
			Path:            "/api/v1/namespaces/default/pods/busybox",
		},
		"Unauthorized": &authorizer.AttributesRecord{
			Verb:            "get",
			Namespace:       "default",
			APIGroup:        "", //Core
			APIVersion:      "v1",
			Resource:        "pods",
			Name:            "busybox",
			ResourceRequest: true,
			Path:            "/api/v1/namespaces/default/pods/busybox",
		},
		"ClusterRoleUpdate": &authorizer.AttributesRecord{
			User:            tim,
			Verb:            "update",
			APIGroup:        "rbac.authorization.k8s.io",
			APIVersion:      "v1beta1",
			Resource:        "clusterroles",
			Subresource:     "status",
			Name:            "somerole",
			ResourceRequest: true,
			Path:            "/apis/rbac.authorization.k8s.io/v1beta1/clusterroles/somerole",
		},
	}

	// rules 覆盖用户/用户组、动词、资源/子资源、通配符、非资源 URL、资源名与 omitStages 等匹配维度。
	rules = map[string]audit.PolicyRule{
		"default": {
			Level: audit.LevelMetadata,
		},
		"create": {
			Level: audit.LevelRequest,
			Verbs: []string{"create"},
		},
		"tims": {
			Level: audit.LevelMetadata,
			Users: []string{"tim@k8s.io"},
		},
		"humans": {
			Level:      audit.LevelMetadata,
			UserGroups: []string{"humans"},
		},
		"serviceAccounts": {
			Level:      audit.LevelRequest,
			UserGroups: []string{"system:serviceaccounts"},
		},
		"getPods": {
			Level:     audit.LevelRequestResponse,
			Verbs:     []string{"get"},
			Resources: []audit.GroupResources{{Resources: []string{"pods"}}},
		},
		"getPodLogs": {
			Level:     audit.LevelRequest,
			Verbs:     []string{"get"},
			Resources: []audit.GroupResources{{Resources: []string{"pods/log"}}},
		},
		"getPodWildcardMatching": {
			Level:     audit.LevelRequest,
			Verbs:     []string{"get"},
			Resources: []audit.GroupResources{{Resources: []string{"*"}}},
		},
		"getPodResourceWildcardMatching": {
			Level:     audit.LevelRequest,
			Verbs:     []string{"get"},
			Resources: []audit.GroupResources{{Resources: []string{"*/log"}}},
		},
		"getPodSubResourceWildcardMatching": {
			Level:     audit.LevelRequest,
			Verbs:     []string{"get"},
			Resources: []audit.GroupResources{{Resources: []string{"pods/*"}}},
		},
		"getClusterRoles": {
			Level: audit.LevelRequestResponse,
			Verbs: []string{"get"},
			Resources: []audit.GroupResources{{
				Group:     "rbac.authorization.k8s.io",
				Resources: []string{"clusterroles"},
			}},
			Namespaces: []string{""},
		},
		"getGroupWildCardMatchingWithSubresourceWildcardMatching": {
			Level: audit.LevelRequestResponse,
			Verbs: []string{"update"},
			Resources: []audit.GroupResources{{
				Group:     "*",
				Resources: []string{"*/status"},
			}},
		},
		"getLogs": {
			Level: audit.LevelRequestResponse,
			Verbs: []string{"get"},
			NonResourceURLs: []string{
				"/logs*",
			},
		},
		"getMetrics": {
			Level: audit.LevelRequest,
			Verbs: []string{"get"},
			NonResourceURLs: []string{
				"/metrics",
			},
		},
		"clusterRoleEdit": {
			Level: audit.LevelRequest,
			Resources: []audit.GroupResources{{
				Group:         "rbac.authorization.k8s.io",
				Resources:     []string{"clusterroles"},
				ResourceNames: []string{"edit"},
			}},
		},
		"omit RequestReceived": {
			Level: audit.LevelRequest,
			OmitStages: []audit.Stage{
				audit.StageRequestReceived,
			},
		},
		"only audit panic": {
			Level: audit.LevelRequest,
			OmitStages: []audit.Stage{
				audit.StageRequestReceived,
				audit.StageResponseStarted,
				audit.StageResponseComplete,
			},
		},
	}
)

// test 组装策略并断言给定请求得到的审计级别与 omitStages。
// 规则按声明顺序求值，第一条匹配即生效，因此 ruleNames 的顺序本身就是被测语义的一部分。
func test(t *testing.T, req string, expLevel audit.Level, policyStages, expOmitStages []audit.Stage, ruleNames ...string) {
	policy := audit.Policy{OmitStages: policyStages}
	for _, rule := range ruleNames {
		require.Contains(t, rules, rule)
		policy.Rules = append(policy.Rules, rules[rule])
	}
	require.Contains(t, attrs, req)
	auditConfig := NewPolicyRuleEvaluator(&policy).EvaluatePolicyRule(attrs[req])
	assert.Equal(t, expLevel, auditConfig.Level, "request:%s rules:%s", req, strings.Join(ruleNames, ","))
	assert.True(t, stageEqual(expOmitStages, auditConfig.OmitStages), "request:%s rules:%s, expected stages: %v, actual stages: %v",
		req, strings.Join(ruleNames, ","), expOmitStages, auditConfig.OmitStages)
}

// testAuditLevel 用同一组规则对四类请求逐一断言，验证规则匹配与“首条匹配优先”语义。
func testAuditLevel(t *testing.T, stages []audit.Stage) {
	test(t, "namespaced", audit.LevelMetadata, stages, stages, "default")
	test(t, "namespaced", audit.LevelNone, stages, stages, "create")
	test(t, "namespaced", audit.LevelMetadata, stages, stages, "tims")
	test(t, "namespaced", audit.LevelMetadata, stages, stages, "humans")
	test(t, "namespaced", audit.LevelNone, stages, stages, "serviceAccounts")
	test(t, "namespaced", audit.LevelRequestResponse, stages, stages, "getPods")
	test(t, "namespaced", audit.LevelNone, stages, stages, "getClusterRoles")
	test(t, "namespaced", audit.LevelNone, stages, stages, "getLogs")
	test(t, "namespaced", audit.LevelNone, stages, stages, "getMetrics")
	test(t, "namespaced", audit.LevelMetadata, stages, stages, "getMetrics", "serviceAccounts", "default")
	test(t, "namespaced", audit.LevelRequestResponse, stages, stages, "getMetrics", "getPods", "default")
	test(t, "namespaced", audit.LevelRequestResponse, stages, stages, "getPodLogs", "getPods")

	test(t, "cluster", audit.LevelMetadata, stages, stages, "default")
	test(t, "cluster", audit.LevelNone, stages, stages, "create")
	test(t, "cluster", audit.LevelMetadata, stages, stages, "tims")
	test(t, "cluster", audit.LevelMetadata, stages, stages, "humans")
	test(t, "cluster", audit.LevelNone, stages, stages, "serviceAccounts")
	test(t, "cluster", audit.LevelNone, stages, stages, "getPods")
	test(t, "cluster", audit.LevelRequestResponse, stages, stages, "getClusterRoles")
	test(t, "cluster", audit.LevelRequest, stages, stages, "clusterRoleEdit", "getClusterRoles")
	test(t, "cluster", audit.LevelNone, stages, stages, "getLogs")
	test(t, "cluster", audit.LevelNone, stages, stages, "getMetrics")
	test(t, "cluster", audit.LevelMetadata, stages, stages, "getMetrics", "serviceAccounts", "default")
	test(t, "cluster", audit.LevelRequestResponse, stages, stages, "getMetrics", "getClusterRoles", "default")
	test(t, "cluster", audit.LevelNone, stages, stages, "getPodLogs", "getPods")

	test(t, "nonResource", audit.LevelMetadata, stages, stages, "default")
	test(t, "nonResource", audit.LevelNone, stages, stages, "create")
	test(t, "nonResource", audit.LevelMetadata, stages, stages, "tims")
	test(t, "nonResource", audit.LevelMetadata, stages, stages, "humans")
	test(t, "nonResource", audit.LevelNone, stages, stages, "serviceAccounts")
	test(t, "nonResource", audit.LevelNone, stages, stages, "getPods")
	test(t, "nonResource", audit.LevelNone, stages, stages, "getClusterRoles")
	test(t, "nonResource", audit.LevelRequestResponse, stages, stages, "getLogs")
	test(t, "nonResource", audit.LevelNone, stages, stages, "getMetrics")
	test(t, "nonResource", audit.LevelMetadata, stages, stages, "getMetrics", "serviceAccounts", "default")
	test(t, "nonResource", audit.LevelRequestResponse, stages, stages, "getLogs", "getClusterRoles", "default")
	test(t, "nonResource", audit.LevelNone, stages, stages, "getPodLogs", "getPods")

	test(t, "subresource", audit.LevelRequest, stages, stages, "getPodLogs", "getPods")
	test(t, "subresource", audit.LevelRequest, stages, stages, "getPodWildcardMatching")
	test(t, "subresource", audit.LevelRequest, stages, stages, "getPodResourceWildcardMatching")
	test(t, "subresource", audit.LevelRequest, stages, stages, "getPodSubResourceWildcardMatching")

	test(t, "Unauthorized", audit.LevelNone, stages, stages, "tims")
	test(t, "Unauthorized", audit.LevelMetadata, stages, stages, "tims", "default")
	test(t, "Unauthorized", audit.LevelNone, stages, stages, "humans")
	test(t, "Unauthorized", audit.LevelMetadata, stages, stages, "humans", "default")
	test(t, "ClusterRoleUpdate", audit.LevelRequestResponse, stages, stages, "getGroupWildCardMatchingWithSubresourceWildcardMatching")
}

// TestChecker 覆盖无全局 omitStages 时的规则匹配，以及规则级 omitStages 的合并。
func TestChecker(t *testing.T) {
	testAuditLevel(t, nil)

	//测试规则中的 omitStages
	test(t, "namespaced", audit.LevelRequest, nil, []audit.Stage{audit.StageRequestReceived}, "omit RequestReceived", "getPods", "default")
	test(t, "namespaced", audit.LevelRequest, nil, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StageResponseComplete}, "only audit panic", "getPods", "default")
	test(t, "cluster", audit.LevelRequest, nil, []audit.Stage{audit.StageRequestReceived}, "omit RequestReceived", "getPods", "default")
	test(t, "cluster", audit.LevelRequest, nil, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StageResponseComplete}, "only audit panic", "getPods", "default")
	test(t, "nonResource", audit.LevelRequest, nil, []audit.Stage{audit.StageRequestReceived}, "omit RequestReceived", "getPods", "default")
	test(t, "nonResource", audit.LevelRequest, nil, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StageResponseComplete}, "only audit panic", "getPods", "default")
}

// TestCheckerPolicyOmitStages 验证策略级 omitStages 会与规则级 omitStages 取并集。
func TestCheckerPolicyOmitStages(t *testing.T) {
	policyStages := []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted}
	testAuditLevel(t, policyStages)

	//测试策略级别的 omitStages
	test(t, "namespaced", audit.LevelRequest, policyStages, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted}, "omit RequestReceived", "getPods", "default")
	test(t, "namespaced", audit.LevelRequest, policyStages, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StageResponseComplete}, "only audit panic", "getPods", "default")
	test(t, "cluster", audit.LevelRequest, policyStages, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted}, "omit RequestReceived", "getPods", "default")
	test(t, "cluster", audit.LevelRequest, policyStages, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StageResponseComplete}, "only audit panic", "getPods", "default")
	test(t, "nonResource", audit.LevelMetadata, policyStages, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted}, "default", "omit RequestReceived", "getPods")
	test(t, "nonResource", audit.LevelRequest, policyStages, []audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StageResponseComplete}, "only audit panic", "getPods", "default")
}

// 若 s1 与 s2 互为超集，stageEqual 返回 true
//
// 用集合比较而非切片比较，因为阶段顺序没有语义。
func stageEqual(s1, s2 []audit.Stage) bool {
	m1 := make(map[audit.Stage]bool)
	m2 := make(map[audit.Stage]bool)
	for _, s := range s1 {
		m1[s] = true
	}
	for _, s := range s2 {
		m2[s] = true
	}
	if len(m1) != len(m2) {
		return false
	}
	for key, value := range m1 {
		if m2[key] != value {
			return false
		}
	}
	return true
}

// TestUnionStages 验证 unionStages 的去重与并集语义，包括 nil、重复元素与完全重叠的情况。
func TestUnionStages(t *testing.T) {
	var testCases = []struct {
		s1, s2, exp []audit.Stage
	}{
		{
			[]audit.Stage{},
			[]audit.Stage{},
			[]audit.Stage{},
		},
		{
			[]audit.Stage{audit.StageRequestReceived},
			[]audit.Stage{},
			[]audit.Stage{audit.StageRequestReceived},
		},
		{
			[]audit.Stage{audit.StageRequestReceived},
			[]audit.Stage{audit.StageRequestReceived},
			[]audit.Stage{audit.StageRequestReceived},
		},
		{
			[]audit.Stage{audit.StageRequestReceived},
			[]audit.Stage{audit.StageResponseStarted},
			[]audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted},
		},
		{
			[]audit.Stage{audit.StageRequestReceived, audit.StageRequestReceived},
			[]audit.Stage{audit.StageRequestReceived, audit.StageRequestReceived},
			[]audit.Stage{audit.StageRequestReceived},
		},
		{
			[]audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted},
			[]audit.Stage{audit.StagePanic, audit.StageRequestReceived},
			[]audit.Stage{audit.StageRequestReceived, audit.StageResponseStarted, audit.StagePanic},
		},
		{
			nil,
			[]audit.Stage{audit.StageRequestReceived},
			[]audit.Stage{audit.StageRequestReceived},
		},
	}

	for _, tc := range testCases {
		result := unionStages(tc.s1, tc.s2)
		assert.Len(t, result, len(tc.exp))
		for _, expStage := range tc.exp {
			ok := false
			for _, resultStage := range result {
				if resultStage == expStage {
					ok = true
					break
				}
			}
			assert.True(t, ok)
		}
	}
}

// TestOmitManagedFields 验证 OmitManagedFields 的三态优先级：
// 规则未设置时继承策略级默认值，设置后覆盖策略级值。
func TestOmitManagedFields(t *testing.T) {
	//此 authorizer.Attributes 应匹配本测试中所有策略规则，
	//避免漏测规则匹配分支。
	attributes := &authorizer.AttributesRecord{
		Verb: "get",
	}
	matchingPolicyRule := audit.PolicyRule{
		Level: audit.LevelRequestResponse,
		Verbs: []string{
			attributes.GetVerb(),
		},
	}

	tests := []struct {
		name   string
		policy func() audit.Policy
		want   bool
	}{
		{
			name: "global policy default is false, rule does not override",
			policy: func() audit.Policy {
				return audit.Policy{
					OmitManagedFields: false,
					Rules: []audit.PolicyRule{
						*matchingPolicyRule.DeepCopy(),
					},
				}
			},
		},
		{
			name: "global policy default is true, rule does not override",
			policy: func() audit.Policy {
				return audit.Policy{
					OmitManagedFields: true,
					Rules: []audit.PolicyRule{
						*matchingPolicyRule.DeepCopy(),
					},
				}
			},
			want: true,
		},
		{
			name: "global policy default is true, rule overrides to false",
			policy: func() audit.Policy {
				rule := matchingPolicyRule.DeepCopy()
				rule.OmitManagedFields = ptr.To(false)
				return audit.Policy{
					OmitManagedFields: true,
					Rules:             []audit.PolicyRule{*rule},
				}
			},
			want: false,
		},
		{
			name: "global policy default is false, rule overrides to true",
			policy: func() audit.Policy {
				rule := matchingPolicyRule.DeepCopy()
				rule.OmitManagedFields = ptr.To(true)
				return audit.Policy{
					OmitManagedFields: false,
					Rules:             []audit.PolicyRule{*rule},
				}
			},
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evaluator := &policyRuleEvaluator{
				Policy: test.policy(),
			}

			got := evaluator.EvaluatePolicyRule(attributes)
			if test.want != got.OmitManagedFields {
				t.Errorf("Expected OmitManagedFields to match, want: %t, got: %t", test.want, got.OmitManagedFields)
			}
		})
	}
}
