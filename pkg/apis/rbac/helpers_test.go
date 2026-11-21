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

package rbac_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/rbac"
	v1 "k8s.io/kubernetes/pkg/apis/rbac/v1"

	// 安装 RBAC 类型，使 scheme 能够执行转换。
	_ "k8s.io/kubernetes/pkg/apis/rbac/install"
)

// TestHelpersRoundTrip 确认 rbac.New* 辅助函数生成的对象与经过转换、默认化的对象一致。
// 这些函数用于创建 bootstrap RBAC 策略；若结果不一致，协调时会在集群策略中
// 重复添加数据。
func TestHelpersRoundTrip(t *testing.T) {
	// 分别构造角色绑定、集群角色绑定以及对应的主体集合。
	rb := rbac.NewRoleBinding("role", "ns").Groups("g").SAs("ns", "sa").Users("u").BindingOrDie()
	rbcr := rbac.NewRoleBindingForClusterRole("role", "ns").Groups("g").SAs("ns", "sa").Users("u").BindingOrDie()
	crb := rbac.NewClusterBinding("role").Groups("g").SAs("ns", "sa").Users("u").BindingOrDie()

	// 同时构造普通角色与集群角色，覆盖规则构建器。
	role := &rbac.Role{
		Rules: []rbac.PolicyRule{
			rbac.NewRule("verb").Groups("g").Resources("foo").RuleOrDie(),
			rbac.NewRule("verb").URLs("/foo").RuleOrDie(),
		},
	}
	clusterRole := &rbac.ClusterRole{
		Rules: []rbac.PolicyRule{
			rbac.NewRule("verb").Groups("g").Resources("foo").RuleOrDie(),
			rbac.NewRule("verb").URLs("/foo").RuleOrDie(),
		},
	}

	// 把内部对象转为外部版本并应用默认化，再转回内部对象。
	for _, internalObj := range []runtime.Object{&rb, &rbcr, &crb, role, clusterRole} {
		v1Obj, err := legacyscheme.Scheme.ConvertToVersion(internalObj, v1.SchemeGroupVersion)
		if err != nil {
			t.Errorf("err on %T: %v", internalObj, err)
			continue
		}
		// 默认化会填充 APIGroup 等字段，往返一致才说明辅助函数输出正确。
		legacyscheme.Scheme.Default(v1Obj)
		roundTrippedObj, err := legacyscheme.Scheme.ConvertToVersion(v1Obj, rbac.SchemeGroupVersion)
		if err != nil {
			t.Errorf("err on %T: %v", internalObj, err)
			continue
		}
		if !reflect.DeepEqual(internalObj, roundTrippedObj) {
			t.Errorf("err on %T: got difference:\n%s", internalObj, cmp.Diff(internalObj, roundTrippedObj))
			continue
		}
	}
}

// TestResourceMatches 校验资源通配符、子资源以及路径片段的匹配规则。
func TestResourceMatches(t *testing.T) {
	tests := []struct {
		name                      string
		ruleResources             []string
		combinedRequestedResource string
		requestedSubresource      string
		expected                  bool
	}{
		{
			// 通配符可匹配任意资源。
			name:                      "all matches 01",
			ruleResources:             []string{"*"},
			combinedRequestedResource: "foo",
			expected:                  true,
		},
		{
			// 只要有一条规则匹配即视为匹配。
			name:                      "checks all rules",
			ruleResources:             []string{"doesn't match", "*"},
			combinedRequestedResource: "foo",
			expected:                  true,
		},
		{
			name:                      "matches exact rule",
			ruleResources:             []string{"foo/bar"},
			combinedRequestedResource: "foo/bar",
			requestedSubresource:      "bar",
			expected:                  true,
		},
		{
			name:                      "matches exact rule 02",
			ruleResources:             []string{"foo/bar"},
			combinedRequestedResource: "foo",
			expected:                  false,
		},
		{
			// 子资源中的星号只匹配路径片段，不能跨段匹配。
			name:                      "matches subresource",
			ruleResources:             []string{"*/scale"},
			combinedRequestedResource: "foo/scale",
			requestedSubresource:      "scale",
			expected:                  true,
		},
		{
			// 部分命中不应算作匹配，避免权限被意外放大。
			name:                      "doesn't match partial subresource hit",
			ruleResources:             []string{"foo/bar", "*/other"},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  false,
		},
		{
			name:                      "matches subresource with multiple slashes",
			ruleResources:             []string{"*/other/segment"},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  true,
		},
		{
			name:                      "doesn't fail on empty",
			ruleResources:             []string{""},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  false,
		},
		{
			name:                      "doesn't fail on slash",
			ruleResources:             []string{"/"},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  false,
		},
		{
			name:                      "doesn't fail on missing subresource",
			ruleResources:             []string{"*/"},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  false,
		},
		{
			name:                      "doesn't match on not star",
			ruleResources:             []string{"*something/other/segment"},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  false,
		},
		{
			name:                      "doesn't match on something else",
			ruleResources:             []string{"something/other/segment"},
			combinedRequestedResource: "foo/other/segment",
			requestedSubresource:      "other/segment",
			expected:                  false,
		},
	}

	// 逐条构造规则并调用 ResourceMatches 验证预期结果。
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := &rbac.PolicyRule{
				Resources: tc.ruleResources,
			}
			actual := rbac.ResourceMatches(rule, tc.combinedRequestedResource, tc.requestedSubresource)
			if tc.expected != actual {
				t.Errorf("expected %v, got %v", tc.expected, actual)
			}

		})
	}
}

// TestPolicyRuleBuilder 校验规则构建器的合法性判断与生成结果。
func TestPolicyRuleBuilder(t *testing.T) {
	tests := []struct {
		testName   string
		verbs      []string
		groups     []string
		resources  []string
		names      []string
		urls       []string
		expected   bool
		policyRule rbac.PolicyRule
	}{
		{
			// 全部字段为空时构建失败。
			testName:   "all empty",
			verbs:      nil,
			groups:     nil,
			resources:  nil,
			names:      nil,
			urls:       nil,
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
		{
			testName:  "normal resource case",
			verbs:     []string{"get"},
			groups:    []string{""},
			resources: []string{"pod"},
			names:     []string{"gakki"},
			urls:      nil,
			expected:  true,
			policyRule: rbac.PolicyRule{
				Verbs:           []string{"get"},
				APIGroups:       []string{""},
				Resources:       []string{"pod"},
				ResourceNames:   []string{"gakki"},
				NonResourceURLs: []string{},
			},
		},
		{
			testName:  "normal noResourceURLs case",
			verbs:     []string{"get"},
			groups:    nil,
			resources: nil,
			names:     nil,
			urls:      []string{"/api/registry/healthz"},
			expected:  true,
			policyRule: rbac.PolicyRule{
				Verbs:           []string{"get"},
				APIGroups:       []string{},
				Resources:       []string{},
				ResourceNames:   []string{},
				NonResourceURLs: []string{"/api/registry/healthz"},
			},
		},
		{
			// 非资源 URL 不能与 API 组、资源或资源名混用。
			testName:   "nonResourceURLs with no-empty groups",
			verbs:      []string{"get"},
			groups:     []string{""},
			resources:  nil,
			names:      nil,
			urls:       []string{"/api/registry/healthz"},
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
		{
			testName:   "nonResourceURLs with no-empty resources",
			verbs:      []string{"get"},
			groups:     nil,
			resources:  []string{"deployments", "secrets"},
			names:      nil,
			urls:       []string{"/api/registry/healthz"},
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
		{
			testName:   "nonResourceURLs with no-empty resourceNames",
			verbs:      []string{"get"},
			groups:     nil,
			resources:  nil,
			names:      []string{"gakki"},
			urls:       []string{"/api/registry/healthz"},
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
		{
			testName:   "resource without apiGroups",
			verbs:      []string{"get"},
			groups:     nil,
			resources:  []string{"pod"},
			names:      []string{""},
			urls:       nil,
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
		{
			// resourceNames 只能与 list/watch/get 等按名访问的动词搭配。
			testName:   "resourceNames with illegal verb",
			verbs:      []string{"list", "watch", "create", "deletecollection"},
			groups:     []string{""},
			resources:  []string{"pod"},
			names:      []string{"gakki"},
			urls:       nil,
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
		{
			testName:   "no nonResourceURLs nor resources",
			verbs:      []string{"get"},
			groups:     []string{"rbac.authorization.k8s.io"},
			resources:  nil,
			names:      []string{"gakki"},
			urls:       nil,
			expected:   false,
			policyRule: rbac.PolicyRule{},
		},
	}
	// 合法用例比较生成的规则，非法用例确认返回错误。
	for _, tc := range tests {
		actual, err := rbac.NewRule(tc.verbs...).Groups(tc.groups...).Resources(tc.resources...).Names(tc.names...).URLs(tc.urls...).Rule()
		if err != nil {
			if tc.expected {
				t.Error(err)
			} else {
				continue
			}
		}
		if !reflect.DeepEqual(actual, tc.policyRule) {
			t.Errorf("Expected %s got %s.", tc.policyRule, actual)
		}
	}
}
