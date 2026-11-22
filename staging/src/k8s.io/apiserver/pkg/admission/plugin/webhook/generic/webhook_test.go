/*
Copyright 2019 The Kubernetes Authors.

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

package generic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	v1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/namespace"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/predicates/object"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

func gvr(group, version, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

func gvk(group, version, kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}
}

var _ matchconditions.Matcher = &fakeMatcher{}

// fakeMatcher 是 matchconditions.Matcher 的替身：throwError 模拟 CEL 求值失败，
// matchResult 控制匹配结果，便于在不构造真实 CEL 环境的情况下覆盖各分支。
type fakeMatcher struct {
	throwError  error
	matchResult bool
}

func (f *fakeMatcher) Match(ctx context.Context, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, authz authorizer.UnconditionalAuthorizer) matchconditions.MatchResult {
	if f.throwError != nil {
		return matchconditions.MatchResult{
			Matches:             true,
			FailedConditionName: "",
			Error:               f.throwError,
		}
	}
	return matchconditions.MatchResult{
		Matches:             f.matchResult,
		FailedConditionName: "",
	}
}

var _ webhook.WebhookAccessor = &fakeWebhookAccessor{}

// fakeWebhookAccessor 包装真实的 webhook accessor，只覆盖匹配器构造，从而把
// ShouldCallHook 的规则/选择器逻辑与 CEL 条件求值隔离开。
type fakeWebhookAccessor struct {
	webhook.WebhookAccessor
	throwError  error
	matchResult bool
}

func (f *fakeWebhookAccessor) GetCompiledMatcher(compiler cel.ConditionCompiler) matchconditions.Matcher {
	return &fakeMatcher{
		throwError:  f.throwError,
		matchResult: f.matchResult,
	}
}

var _ VersionedAttributeAccessor = &fakeVersionedAttributeAccessor{}

type fakeVersionedAttributeAccessor struct{}

func (v *fakeVersionedAttributeAccessor) VersionedAttribute(gvk schema.GroupVersionKind) (*admission.VersionedAttributes, error) {
	return nil, nil
}

// TestShouldCallHook 验证 webhook 规则匹配、等价资源解析与 matchConditions 的组合语义。
// 中文说明：核心不变量是“规则命中后，调用的 Kind/Resource/Subresource 必须是规则意图的
// 那个等价版本”——Exact 只认精确匹配，Equivalent 才允许跨组/跨版本改写，且按规则顺序
// 优先选择；匹配条件出错时按 fail-closed 拒绝请求。
func TestShouldCallHook(t *testing.T) {
	a := &Webhook{
		namespaceMatcher: &namespace.Matcher{},
		objectMatcher:    &object.Matcher{},
	}

	allScopes := v1.AllScopes
	exactMatch := v1.Exact
	equivalentMatch := v1.Equivalent

	// 让 extensions/apps 各组下的 deployments 被视为同一资源，用于覆盖 equivalent 匹配。
	mapper := runtime.NewEquivalentResourceRegistryWithIdentity(func(resource schema.GroupResource) string {
		if resource.Resource == "deployments" {
			// co-locate deployments in all API groups
			return "/deployments"
		}
		return ""
	})
	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "", gvk("extensions", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "", gvk("apps", "v1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "", gvk("apps", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "", gvk("apps", "v1alpha1", "Deployment"))

	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "scale", gvk("extensions", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "scale", gvk("autoscaling", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "scale", gvk("apps", "v1alpha1", "Scale"))

	// register invalid kinds to trigger an error
	// 中文说明：注册空 GVK 会让 kind 查找失败，用于验证匹配阶段的错误处理。
	mapper.RegisterKindFor(gvr("example.com", "v1", "widgets"), "", gvk("", "", ""))
	mapper.RegisterKindFor(gvr("example.com", "v2", "widgets"), "", gvk("", "", ""))

	interfaces := &admission.RuntimeObjectInterfaces{EquivalentResourceMapper: mapper}

	// 每个用例覆盖一类规则/匹配策略组合；expectCall* 描述期望传给 webhook 的
	// 具体资源版本，而 matchResult/matchError 控制 matchConditions 的模拟结果。
	testcases := []struct {
		name string

		webhook *v1.ValidatingWebhook
		attrs   admission.Attributes

		expectCall            bool
		expectErr             string
		expectCallResource    schema.GroupVersionResource
		expectCallSubresource string
		expectCallKind        schema.GroupVersionKind
		matchError            error
		matchResult           bool
	}{
		{
			name:        "no rules (just write)", // 空规则列表意味着该 webhook 不拦截任何请求。
			webhook:     &v1.ValidatingWebhook{NamespaceSelector: &metav1.LabelSelector{}, Rules: []v1.RuleWithOperations{}},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			matchResult: true,
		},
		{
			name: "invalid kind lookup",
			// Equivalent 策略需要把请求 GVK 映射到等价 Kind，空 GVK 注册会令查找失败。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				MatchPolicy:       &equivalentMatch,
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"example.com"}, APIVersions: []string{"v1"}, Resources: []string{"widgets"}, Scope: &allScopes},
				}}},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("example.com", "v2", "Widget"), "ns", "name", gvr("example.com", "v2", "widgets"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			expectErr:   "unknown kind",
			matchResult: true,
		},
		{
			name: "wildcard rule, match as requested",
			// 通配规则不做版本改写，直接使用请求本身的 GVK/GVR。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"*"}, APIVersions: []string{"*"}, Resources: []string{"*"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("apps", "v1", "Deployment"),
			expectCallResource:    gvr("apps", "v1", "deployments"),
			expectCallSubresource: "",
			matchResult:           true,
		},
		{
			name: "specific rules, prefer exact match",
			// 请求本身就是 apps/v1，应优先于其它等价规则使用精确匹配。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("apps", "v1", "Deployment"),
			expectCallResource:    gvr("apps", "v1", "deployments"),
			expectCallSubresource: "",
			matchResult:           true,
		},
		{
			name: "specific rules, match miss",
			// 请求是 apps/v1，而规则只声明 extensions/apps 的 v1beta1；
			// 默认 MatchPolicy 为 Equivalent，但 v1 与 v1beta1 不是等价版本，故不匹配。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}}},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			matchResult: true,
		},
		{
			name: "specific rules, exact match miss",
			// Exact 策略下只认字面资源版本，apps/v1 与规则中的 v1beta1 不同即不调用。
			webhook: &v1.ValidatingWebhook{
				MatchPolicy:       &exactMatch,
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}}},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			matchResult: true,
		},
		{
			name: "specific rules, equivalent match, prefer extensions",
			// 两条等价规则中 extensions 在前，按规则顺序选中它。
			webhook: &v1.ValidatingWebhook{
				MatchPolicy:       &equivalentMatch,
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("extensions", "v1beta1", "Deployment"),
			expectCallResource:    gvr("extensions", "v1beta1", "deployments"),
			expectCallSubresource: "",
			matchResult:           true,
		},
		{
			name: "specific rules, equivalent match, prefer apps",
			// 与上例对称：apps 规则在前时选中 apps/v1beta1，验证顺序敏感而非固定偏好。
			webhook: &v1.ValidatingWebhook{
				MatchPolicy:       &equivalentMatch,
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("apps", "v1beta1", "Deployment"),
			expectCallResource:    gvr("apps", "v1beta1", "deployments"),
			expectCallSubresource: "",
			matchResult:           true,
		},

		{
			name: "specific rules, subresource prefer exact match",
			// 请求为 apps/v1 deployments 的 scale 子资源，Kind 由 mapper 解析成 autoscaling/v1 Scale。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("autoscaling", "v1", "Scale"),
			expectCallResource:    gvr("apps", "v1", "deployments"),
			expectCallSubresource: "scale",
			matchResult:           true,
		},
		{
			name: "specific rules, subresource match miss",
			// 子资源请求在等价策略下仍要求组/版本可对应；这里 apps/v1 无对应 scale 规则。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}}},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			matchResult: true,
		},
		{
			name: "specific rules, subresource exact match miss",
			// Exact 策略下子资源也必须版本精确匹配。
			webhook: &v1.ValidatingWebhook{
				MatchPolicy:       &exactMatch,
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}}},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			matchResult: true,
		},
		{
			name: "specific rules, subresource equivalent match, prefer extensions",
			// 等价匹配会把子资源 Kind 改写成规则声明的版本（extensions/v1beta1 Scale）。
			webhook: &v1.ValidatingWebhook{
				MatchPolicy:       &equivalentMatch,
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("extensions", "v1beta1", "Scale"),
			expectCallResource:    gvr("extensions", "v1beta1", "deployments"),
			expectCallSubresource: "scale",
			matchResult:           true,
		},
		{
			name: "specific rules, subresource equivalent match, prefer apps",
			// 同上，规则顺序决定选中的等价子资源版本。
			webhook: &v1.ValidatingWebhook{
				MatchPolicy:       &equivalentMatch,
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}, {
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
				}}},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("apps", "v1beta1", "Scale"),
			expectCallResource:    gvr("apps", "v1beta1", "deployments"),
			expectCallSubresource: "scale",
			matchResult:           true,
		},
		{
			name: "wildcard rule, match conditions also match",
			// 规则命中且 CEL 条件为真时才真正调用 webhook。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"*"}, APIVersions: []string{"*"}, Resources: []string{"*"}, Scope: &allScopes},
				}},
				MatchConditions: []v1.MatchCondition{
					{
						Name:       "test1",
						Expression: "test expression",
					},
				},
			},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            true,
			expectCallKind:        gvk("apps", "v1", "Deployment"),
			expectCallResource:    gvr("apps", "v1", "deployments"),
			expectCallSubresource: "",
			matchResult:           true,
		},
		{
			name: "wildcard rule, match conditions do not match",
			// 条件为假时跳过调用，属于正常的“不匹配”而非错误。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"*"}, APIVersions: []string{"*"}, Resources: []string{"*"}, Scope: &allScopes},
				}},
				MatchConditions: []v1.MatchCondition{
					{
						Name:       "test1",
						Expression: "test expression",
					},
				},
			},
			attrs:       admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:  false,
			matchResult: false,
		},
		{
			name: "wildcard rule, match conditions error",
			// 条件求值失败必须变成 Forbidden（fail-closed），而不是静默放行。
			webhook: &v1.ValidatingWebhook{
				NamespaceSelector: &metav1.LabelSelector{},
				ObjectSelector:    &metav1.LabelSelector{},
				Rules: []v1.RuleWithOperations{{
					Operations: []v1.OperationType{"*"},
					Rule:       v1.Rule{APIGroups: []string{"*"}, APIVersions: []string{"*"}, Resources: []string{"*"}, Scope: &allScopes},
				}},
				MatchConditions: []v1.MatchCondition{
					{
						Name:       "test1",
						Expression: "test expression",
					},
				},
			},
			attrs:                 admission.NewAttributesRecord(nil, nil, gvk("apps", "v1", "Deployment"), "ns", "name", gvr("apps", "v1", "deployments"), "", admission.Create, &metav1.CreateOptions{}, false, nil),
			expectCall:            false,
			expectErr:             "deployments.apps \"name\" is forbidden: test error",
			expectCallKind:        gvk("apps", "v1", "Deployment"),
			expectCallResource:    gvr("apps", "v1", "deployments"),
			expectCallSubresource: "",
			matchError:            errors.New("test error"),
		},
	}

	for i, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			fakeWebhook := &fakeWebhookAccessor{
				WebhookAccessor: webhook.NewValidatingWebhookAccessor(fmt.Sprintf("webhook-%d", i), fmt.Sprintf("webhook-cfg-%d", i), testcase.webhook),
				matchResult:     testcase.matchResult,
				throwError:      testcase.matchError,
			}

			// 统一按“错误 → 是否调用 → 调用目标”的顺序断言，避免分支间互相掩盖。
			invocation, err := a.ShouldCallHook(context.TODO(), fakeWebhook, testcase.attrs, interfaces, &fakeVersionedAttributeAccessor{})
			if err != nil {
				if len(testcase.expectErr) == 0 {
					t.Fatal(err)
				}
				if !strings.Contains(err.Error(), testcase.expectErr) {
					t.Fatalf("expected error containing %q, got %s", testcase.expectErr, err.Error())
				}
				return
			} else if len(testcase.expectErr) > 0 {
				t.Fatalf("expected error %q, got no error and %#v", testcase.expectErr, invocation)
			}

			if invocation == nil {
				if testcase.expectCall {
					t.Fatal("expected invocation, got nil")
				}
				return
			}

			if !testcase.expectCall {
				t.Fatal("unexpected invocation")
			}

			if invocation.Kind != testcase.expectCallKind {
				t.Fatalf("expected %#v, got %#v", testcase.expectCallKind, invocation.Kind)
			}
			if invocation.Resource != testcase.expectCallResource {
				t.Fatalf("expected %#v, got %#v", testcase.expectCallResource, invocation.Resource)
			}
			if invocation.Subresource != testcase.expectCallSubresource {
				t.Fatalf("expected %#v, got %#v", testcase.expectCallSubresource, invocation.Subresource)
			}
		})
	}
}

// fakeNamespaceLister 只实现 Get，用于让 namespace 选择器在基准测试中走真实解析路径。
type fakeNamespaceLister struct {
	namespaces map[string]*corev1.Namespace
}

func (f fakeNamespaceLister) List(selector labels.Selector) (ret []*corev1.Namespace, err error) {
	return nil, nil
}
func (f fakeNamespaceLister) Get(name string) (*corev1.Namespace, error) {
	ns, ok := f.namespaces[name]
	if ok {
		return ns, nil
	}
	return nil, k8serrors.NewNotFound(corev1.Resource("namespaces"), name)
}

// BenchmarkShouldCallHookWithComplexSelector 用 100 项 namespace 选择器压测匹配路径，
// 用于观察标签选择器解析与 namespace 查询的开销。
func BenchmarkShouldCallHookWithComplexSelector(b *testing.B) {
	allScopes := v1.AllScopes
	equivalentMatch := v1.Equivalent

	namespace1Labels := map[string]string{"ns": "ns1"}
	namespace1 := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ns1",
			Labels: namespace1Labels,
		},
	}
	namespaceLister := fakeNamespaceLister{map[string]*corev1.Namespace{"ns": &namespace1}}

	mapper := runtime.NewEquivalentResourceRegistryWithIdentity(func(resource schema.GroupResource) string {
		if resource.Resource == "deployments" {
			// co-locate deployments in all API groups
			return "/deployments"
		}
		return ""
	})
	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "", gvk("extensions", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "", gvk("apps", "v1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "", gvk("apps", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "", gvk("apps", "v1alpha1", "Deployment"))

	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "scale", gvk("extensions", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "scale", gvk("autoscaling", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "scale", gvk("apps", "v1alpha1", "Scale"))

	mapper.RegisterKindFor(gvr("apps", "v1", "statefulset"), "", gvk("apps", "v1", "StatefulSet"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "statefulset"), "", gvk("apps", "v1beta1", "StatefulSet"))
	mapper.RegisterKindFor(gvr("apps", "v1beta2", "statefulset"), "", gvk("apps", "v1beta2", "StatefulSet"))

	mapper.RegisterKindFor(gvr("apps", "v1", "statefulset"), "scale", gvk("apps", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "statefulset"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha2", "statefulset"), "scale", gvk("apps", "v1beta2", "Scale"))

	nsSelector := make(map[string]string)
	for i := 0; i < 100; i++ {
		nsSelector[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("val-%d", i)
	}

	wb := &v1.ValidatingWebhook{
		MatchPolicy:       &equivalentMatch,
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: nsSelector},
		ObjectSelector:    &metav1.LabelSelector{},
		Rules: []v1.RuleWithOperations{
			{
				Operations: []v1.OperationType{"*"},
				Rule:       v1.Rule{APIGroups: []string{"apps"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
			},
			{
				Operations: []v1.OperationType{"*"},
				Rule:       v1.Rule{APIGroups: []string{"extensions"}, APIVersions: []string{"v1beta1"}, Resources: []string{"deployments", "deployments/scale"}, Scope: &allScopes},
			},
		},
	}

	wbAccessor := &fakeWebhookAccessor{
		WebhookAccessor: webhook.NewValidatingWebhookAccessor("webhook", "webhook-cfg", wb),
		matchResult:     true,
	}
	attrs := admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil)
	interfaces := &admission.RuntimeObjectInterfaces{EquivalentResourceMapper: mapper}
	a := &Webhook{namespaceMatcher: &namespace.Matcher{NamespaceLister: namespaceLister}, objectMatcher: &object.Matcher{}}

	for i := 0; i < b.N; i++ {
		a.ShouldCallHook(context.TODO(), wbAccessor, attrs, interfaces, nil)
	}
}

// BenchmarkShouldCallHookWithComplexRule 用 100 条规则压测规则遍历路径，
// 与选择器基准互补，定位复杂度主要来自哪一侧。
func BenchmarkShouldCallHookWithComplexRule(b *testing.B) {
	allScopes := v1.AllScopes
	equivalentMatch := v1.Equivalent

	namespace1Labels := map[string]string{"ns": "ns1"}
	namespace1 := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ns1",
			Labels: namespace1Labels,
		},
	}
	namespaceLister := fakeNamespaceLister{map[string]*corev1.Namespace{"ns": &namespace1}}

	mapper := runtime.NewEquivalentResourceRegistryWithIdentity(func(resource schema.GroupResource) string {
		if resource.Resource == "deployments" {
			// co-locate deployments in all API groups
			return "/deployments"
		}
		return ""
	})
	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "", gvk("extensions", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "", gvk("apps", "v1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "", gvk("apps", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "", gvk("apps", "v1alpha1", "Deployment"))

	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "scale", gvk("extensions", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "scale", gvk("autoscaling", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "scale", gvk("apps", "v1alpha1", "Scale"))

	mapper.RegisterKindFor(gvr("apps", "v1", "statefulset"), "", gvk("apps", "v1", "StatefulSet"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "statefulset"), "", gvk("apps", "v1beta1", "StatefulSet"))
	mapper.RegisterKindFor(gvr("apps", "v1beta2", "statefulset"), "", gvk("apps", "v1beta2", "StatefulSet"))

	mapper.RegisterKindFor(gvr("apps", "v1", "statefulset"), "scale", gvk("apps", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "statefulset"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha2", "statefulset"), "scale", gvk("apps", "v1beta2", "Scale"))

	wb := &v1.ValidatingWebhook{
		MatchPolicy:       &equivalentMatch,
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
		ObjectSelector:    &metav1.LabelSelector{},
		Rules:             []v1.RuleWithOperations{},
	}

	for i := 0; i < 100; i++ {
		rule := v1.RuleWithOperations{
			Operations: []v1.OperationType{"*"},
			Rule: v1.Rule{
				APIGroups:   []string{fmt.Sprintf("app-%d", i)},
				APIVersions: []string{fmt.Sprintf("v%d", i)},
				Resources:   []string{fmt.Sprintf("resource%d", i), fmt.Sprintf("resource%d/scale", i)},
				Scope:       &allScopes,
			},
		}
		wb.Rules = append(wb.Rules, rule)
	}

	wbAccessor := &fakeWebhookAccessor{
		WebhookAccessor: webhook.NewValidatingWebhookAccessor("webhook", "webhook-cfg", wb),
		matchResult:     true,
	}
	attrs := admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil)
	interfaces := &admission.RuntimeObjectInterfaces{EquivalentResourceMapper: mapper}
	a := &Webhook{namespaceMatcher: &namespace.Matcher{NamespaceLister: namespaceLister}, objectMatcher: &object.Matcher{}}

	for i := 0; i < b.N; i++ {
		a.ShouldCallHook(context.TODO(), wbAccessor, attrs, interfaces, &fakeVersionedAttributeAccessor{})
	}
}

// BenchmarkShouldCallHookWithComplexSelectorAndRule 同时叠加 100 项选择器与 100 条规则，
// 逼近最坏情况，用于评估放大效应。
func BenchmarkShouldCallHookWithComplexSelectorAndRule(b *testing.B) {
	allScopes := v1.AllScopes
	equivalentMatch := v1.Equivalent

	namespace1Labels := map[string]string{"ns": "ns1"}
	namespace1 := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "ns1",
			Labels: namespace1Labels,
		},
	}
	namespaceLister := fakeNamespaceLister{map[string]*corev1.Namespace{"ns": &namespace1}}

	mapper := runtime.NewEquivalentResourceRegistryWithIdentity(func(resource schema.GroupResource) string {
		if resource.Resource == "deployments" {
			// co-locate deployments in all API groups
			return "/deployments"
		}
		return ""
	})
	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "", gvk("extensions", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "", gvk("apps", "v1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "", gvk("apps", "v1beta1", "Deployment"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "", gvk("apps", "v1alpha1", "Deployment"))

	mapper.RegisterKindFor(gvr("extensions", "v1beta1", "deployments"), "scale", gvk("extensions", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1", "deployments"), "scale", gvk("autoscaling", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "deployments"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha1", "deployments"), "scale", gvk("apps", "v1alpha1", "Scale"))

	mapper.RegisterKindFor(gvr("apps", "v1", "statefulset"), "", gvk("apps", "v1", "StatefulSet"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "statefulset"), "", gvk("apps", "v1beta1", "StatefulSet"))
	mapper.RegisterKindFor(gvr("apps", "v1beta2", "statefulset"), "", gvk("apps", "v1beta2", "StatefulSet"))

	mapper.RegisterKindFor(gvr("apps", "v1", "statefulset"), "scale", gvk("apps", "v1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1beta1", "statefulset"), "scale", gvk("apps", "v1beta1", "Scale"))
	mapper.RegisterKindFor(gvr("apps", "v1alpha2", "statefulset"), "scale", gvk("apps", "v1beta2", "Scale"))

	nsSelector := make(map[string]string)
	for i := 0; i < 100; i++ {
		nsSelector[fmt.Sprintf("key-%d", i)] = fmt.Sprintf("val-%d", i)
	}

	wb := &v1.ValidatingWebhook{
		MatchPolicy:       &equivalentMatch,
		NamespaceSelector: &metav1.LabelSelector{MatchLabels: nsSelector},
		ObjectSelector:    &metav1.LabelSelector{},
		Rules:             []v1.RuleWithOperations{},
	}

	for i := 0; i < 100; i++ {
		rule := v1.RuleWithOperations{
			Operations: []v1.OperationType{"*"},
			Rule: v1.Rule{
				APIGroups:   []string{fmt.Sprintf("app-%d", i)},
				APIVersions: []string{fmt.Sprintf("v%d", i)},
				Resources:   []string{fmt.Sprintf("resource%d", i), fmt.Sprintf("resource%d/scale", i)},
				Scope:       &allScopes,
			},
		}
		wb.Rules = append(wb.Rules, rule)
	}

	wbAccessor := &fakeWebhookAccessor{
		WebhookAccessor: webhook.NewValidatingWebhookAccessor("webhook", "webhook-cfg", wb),
		matchResult:     true,
	}
	attrs := admission.NewAttributesRecord(nil, nil, gvk("autoscaling", "v1", "Scale"), "ns", "name", gvr("apps", "v1", "deployments"), "scale", admission.Create, &metav1.CreateOptions{}, false, nil)
	interfaces := &admission.RuntimeObjectInterfaces{EquivalentResourceMapper: mapper}
	a := &Webhook{namespaceMatcher: &namespace.Matcher{NamespaceLister: namespaceLister}, objectMatcher: &object.Matcher{}}

	for i := 0; i < b.N; i++ {
		a.ShouldCallHook(context.TODO(), wbAccessor, attrs, interfaces, nil)
	}
}
