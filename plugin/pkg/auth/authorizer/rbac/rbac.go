/*
Copyright 2016 The Kubernetes Authors.

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

// Package rbac 使用基于角色的访问控制实现 authorizer.Authorizer 接口。
// RBAC 是 Kubernetes 推荐的授权模式，通过 Role/ClusterRole 定义权限，
// 通过 RoleBinding/ClusterRoleBinding 将权限授予用户、组或服务账户。
package rbac

import (
	"bytes"
	"context"
	"fmt"

	"k8s.io/klog/v2"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	rbaclisters "k8s.io/client-go/listers/rbac/v1"
	rbacv1helpers "k8s.io/kubernetes/pkg/apis/rbac/v1"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
)

// RequestToRuleMapper 将主体和命名空间映射为适用的 RBAC 规则。
type RequestToRuleMapper interface {
	// RulesFor 返回所有已知的 PolicyRule 和查找过程中遇到的错误。
	// 返回的规则仍然有效，因为默认拒绝。如果提供的规则足以通过验证，
	// 则不需要失败；如果不能，应该连同拒绝一起返回错误。
	RulesFor(ctx context.Context, subject user.Info, namespace string) ([]rbacv1.PolicyRule, error)

	// VisitRulesFor 对给定用户在给定命名空间中的每个适用规则调用 visitor()，
	// 以及解析这些规则时遇到的每个错误。如果 visitor() 返回 false，则短路停止遍历。
	VisitRulesFor(ctx context.Context, user user.Info, namespace string, visitor func(source fmt.Stringer, rule *rbacv1.PolicyRule, err error) bool)
}

var _ = authorizer.Authorizer(&RBACAuthorizer{})
var _ = authorizer.RuleResolver(&RBACAuthorizer{})

// RBACAuthorizer 通过遍历适用于请求主体的规则实现授权。
// 找到允许规则时立即放行；否则返回 NoOpinion，遵循默认拒绝语义。
type RBACAuthorizer struct {
	authorizationRuleResolver RequestToRuleMapper // 将请求映射到适用规则的解析器
}

// authorizingVisitor 在找到允许规则后短路，并收集解析过程中遇到的错误。
// 用于 Authorize 方法中遍历规则，一旦找到匹配规则就停止。
type authorizingVisitor struct {
	requestAttributes authorizer.Attributes

	allowed bool
	reason  string
	errors  []error
}

func (v *authorizingVisitor) visit(source fmt.Stringer, rule *rbacv1.PolicyRule, err error) bool {
	if rule != nil && RuleAllows(v.requestAttributes, rule) {
		v.allowed = true
		v.reason = fmt.Sprintf("RBAC: allowed by %s", source.String())
		return false // 找到允许规则，短路停止遍历
	}
	if err != nil {
		v.errors = append(v.errors, err)
	}
	return true
}

// Authorize 检查是否存在允许请求的 RBAC 规则，并返回允许来源或规则解析错误。
func (r *RBACAuthorizer) Authorize(ctx context.Context, requestAttributes authorizer.Attributes) (authorizer.Decision, string, error) {
	ruleCheckingVisitor := &authorizingVisitor{requestAttributes: requestAttributes}

	// 遍历所有适用于该用户的规则，查找是否有允许此请求的规则
	r.authorizationRuleResolver.VisitRulesFor(ctx, requestAttributes.GetUser(), requestAttributes.GetNamespace(), ruleCheckingVisitor.visit)
	if ruleCheckingVisitor.allowed {
		return authorizer.DecisionAllow, ruleCheckingVisitor.reason, nil
	}

	// 构建详细的拒绝日志，仅在需要时构建字符串以避免不必要的开销
	if klogV := klog.V(5); klogV.Enabled() {
		var operation string
		if requestAttributes.IsResourceRequest() {
			b := &bytes.Buffer{}
			b.WriteString(`"`)
			b.WriteString(requestAttributes.GetVerb())
			b.WriteString(`" resource "`)
			b.WriteString(requestAttributes.GetResource())
			if len(requestAttributes.GetAPIGroup()) > 0 {
				b.WriteString(`.`)
				b.WriteString(requestAttributes.GetAPIGroup())
			}
			if len(requestAttributes.GetSubresource()) > 0 {
				b.WriteString(`/`)
				b.WriteString(requestAttributes.GetSubresource())
			}
			b.WriteString(`"`)
			if len(requestAttributes.GetName()) > 0 {
				b.WriteString(` named "`)
				b.WriteString(requestAttributes.GetName())
				b.WriteString(`"`)
			}
			operation = b.String()
		} else {
			operation = fmt.Sprintf("%q nonResourceURL %q", requestAttributes.GetVerb(), requestAttributes.GetPath())
		}

		var scope string
		if ns := requestAttributes.GetNamespace(); len(ns) > 0 {
			scope = fmt.Sprintf("in namespace %q", ns)
		} else {
			scope = "cluster-wide"
		}

		klogV.Infof("RBAC: no rules authorize user %q with groups %q to %s %s", requestAttributes.GetUser().GetName(), requestAttributes.GetUser().GetGroups(), operation, scope)
	}

	reason := ""
	if len(ruleCheckingVisitor.errors) > 0 {
		reason = fmt.Sprintf("RBAC: %v", utilerrors.NewAggregate(ruleCheckingVisitor.errors))
	}
	return authorizer.DecisionNoOpinion, reason, nil
}

// ConditionsAwareAuthorize 不支持 conditions-aware 授权，直接转换 Authorize 结果。
func (r *RBACAuthorizer) ConditionsAwareAuthorize(ctx context.Context, requestAttributes authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(r.Authorize(ctx, requestAttributes))
}

// EvaluateConditions 不支持条件评估。
func (*RBACAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// RulesFor 返回指定主体在命名空间内可枚举的资源和非资源规则。
// 规则解析出错时仍返回已获得的规则，并同时返回错误。
func (r *RBACAuthorizer) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	var (
		resourceRules    []authorizer.ResourceRuleInfo
		nonResourceRules []authorizer.NonResourceRuleInfo
	)

	// 获取用户的所有 RBAC 规则，转换为 authorizer 接口需要的格式
	policyRules, err := r.authorizationRuleResolver.RulesFor(ctx, user, namespace)
	for _, policyRule := range policyRules {
		if len(policyRule.Resources) > 0 {
			r := authorizer.DefaultResourceRuleInfo{
				Verbs:         policyRule.Verbs,
				APIGroups:     policyRule.APIGroups,
				Resources:     policyRule.Resources,
				ResourceNames: policyRule.ResourceNames,
			}
			var resourceRule authorizer.ResourceRuleInfo = &r
			resourceRules = append(resourceRules, resourceRule)
		}
		if len(policyRule.NonResourceURLs) > 0 {
			r := authorizer.DefaultNonResourceRuleInfo{
				Verbs:           policyRule.Verbs,
				NonResourceURLs: policyRule.NonResourceURLs,
			}
			var nonResourceRule authorizer.NonResourceRuleInfo = &r
			nonResourceRules = append(nonResourceRules, nonResourceRule)
		}
	}
	return resourceRules, nonResourceRules, false, err
}

// New 创建一个新的 RBAC 授权器。
// 需要 Role、RoleBinding、ClusterRole、ClusterRoleBinding 的获取和列表接口，
// 用于在授权时查找适用的规则。
func New(roles rbacregistryvalidation.RoleGetter, roleBindings rbacregistryvalidation.RoleBindingLister, clusterRoles rbacregistryvalidation.ClusterRoleGetter, clusterRoleBindings rbacregistryvalidation.ClusterRoleBindingLister) *RBACAuthorizer {
	authorizer := &RBACAuthorizer{
		authorizationRuleResolver: rbacregistryvalidation.NewDefaultRuleResolver(
			roles, roleBindings, clusterRoles, clusterRoleBindings,
		),
	}
	return authorizer
}

// RulesAllow 检查请求是否被任一规则允许。
// 用于 Node 授权器等场景，需要检查静态规则列表。
func RulesAllow(requestAttributes authorizer.Attributes, rules ...rbacv1.PolicyRule) bool {
	for i := range rules {
		if RuleAllows(requestAttributes, &rules[i]) {
			return true
		}
	}

	return false
}

// RuleAllows 检查单个规则是否允许给定的请求。
// 对于资源请求，检查 verb、API group、resource 和 resource name 是否匹配。
// 对于非资源请求，检查 verb 和 URL 路径是否匹配。
func RuleAllows(requestAttributes authorizer.Attributes, rule *rbacv1.PolicyRule) bool {
	if requestAttributes.IsResourceRequest() {
		combinedResource := requestAttributes.GetResource()
		if len(requestAttributes.GetSubresource()) > 0 {
			combinedResource = requestAttributes.GetResource() + "/" + requestAttributes.GetSubresource()
		}

		return rbacv1helpers.VerbMatches(rule, requestAttributes.GetVerb()) &&
			rbacv1helpers.APIGroupMatches(rule, requestAttributes.GetAPIGroup()) &&
			rbacv1helpers.ResourceMatches(rule, combinedResource, requestAttributes.GetSubresource()) &&
			rbacv1helpers.ResourceNameMatches(rule, requestAttributes.GetName())
	}

	return rbacv1helpers.VerbMatches(rule, requestAttributes.GetVerb()) &&
		rbacv1helpers.NonResourceURLMatches(rule, requestAttributes.GetPath())
}

// RoleGetter 包装 RoleLister，提供 Role 获取接口。
// 适配 rbacregistryvalidation 包需要的接口。
type RoleGetter struct {
	Lister rbaclisters.RoleLister
}

// GetRole 从底层 lister 获取指定命名空间中的 Role。
func (g *RoleGetter) GetRole(ctx context.Context, namespace, name string) (*rbacv1.Role, error) {
	return g.Lister.Roles(namespace).Get(name)
}

// RoleBindingLister 包装 RoleBindingLister，提供 RoleBinding 列表接口。
type RoleBindingLister struct {
	Lister rbaclisters.RoleBindingLister
}

// ListRoleBindings 列出指定命名空间中的全部 RoleBinding。
func (l *RoleBindingLister) ListRoleBindings(ctx context.Context, namespace string) ([]*rbacv1.RoleBinding, error) {
	return l.Lister.RoleBindings(namespace).List(labels.Everything())
}

// ClusterRoleGetter 包装 ClusterRoleLister，提供 ClusterRole 获取接口。
type ClusterRoleGetter struct {
	Lister rbaclisters.ClusterRoleLister
}

// GetClusterRole 从底层 lister 获取指定 ClusterRole。
func (g *ClusterRoleGetter) GetClusterRole(ctx context.Context, name string) (*rbacv1.ClusterRole, error) {
	return g.Lister.Get(name)
}

// ClusterRoleBindingLister 包装 ClusterRoleBindingLister，提供 ClusterRoleBinding 列表接口。
type ClusterRoleBindingLister struct {
	Lister rbaclisters.ClusterRoleBindingLister
}

// ListClusterRoleBindings 列出集群中的全部 ClusterRoleBinding。
func (l *ClusterRoleBindingLister) ListClusterRoleBindings(ctx context.Context) ([]*rbacv1.ClusterRoleBinding, error) {
	return l.Lister.List(labels.Everything())
}
