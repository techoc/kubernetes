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

package selfsubjectrulesreview

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	authorizationapi "k8s.io/kubernetes/pkg/apis/authorization"
)

// REST 实现 SelfSubjectRulesReview 的无状态 REST 接口，通过 RuleResolver 查询当前用户的授权规则。
type REST struct {
	ruleResolver authorizer.RuleResolver
}

// NewREST 创建 SelfSubjectRulesReview REST 处理器，并注入授权规则解析器。
func NewREST(ruleResolver authorizer.RuleResolver) *REST {
	return &REST{ruleResolver}
}

// NamespaceScoped 实现 rest.Scoper；该 API 本身是集群级资源，但请求内容必须指定目标 Namespace。
func (r *REST) NamespaceScoped() bool {
	return false
}

// New 返回用于解码请求的空 SelfSubjectRulesReview 对象。
func (r *REST) New() runtime.Object {
	return &authorizationapi.SelfSubjectRulesReview{}
}

// Destroy 实现 REST 销毁接口；处理器不持有底层 Store，无需显式释放资源。
func (r *REST) Destroy() {
}

// Create 校验请求用户和目标 Namespace，查询该用户在命名空间内的资源及非资源规则，并返回查询是否完整。
func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	selfSRR, ok := obj.(*authorizationapi.SelfSubjectRulesReview)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("not a SelfSubjectRulesReview: %#v", obj))
	}

	user, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return nil, apierrors.NewBadRequest("no user present on request")
	}

	namespace := selfSRR.Spec.Namespace
	if namespace == "" {
		return nil, apierrors.NewBadRequest("no namespace on request")
	}

	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	resourceInfo, nonResourceInfo, incomplete, err := r.ruleResolver.RulesFor(ctx, user, namespace)

	ret := &authorizationapi.SelfSubjectRulesReview{
		Status: authorizationapi.SubjectRulesReviewStatus{
			ResourceRules:    getResourceRules(resourceInfo),
			NonResourceRules: getNonResourceRules(nonResourceInfo),
			Incomplete:       incomplete,
		},
	}

	if err != nil {
		ret.Status.EvaluationError = err.Error()
	}

	return ret, nil
}

var _ rest.SingularNameProvider = &REST{}

// GetSingularName 返回 SelfSubjectRulesReview 的单数资源名称。
func (r *REST) GetSingularName() string {
	return "selfsubjectrulesreview"
}

// getResourceRules 将 Authorizer 返回的资源规则信息转换为 API ResourceRule 列表。
func getResourceRules(infos []authorizer.ResourceRuleInfo) []authorizationapi.ResourceRule {
	rules := make([]authorizationapi.ResourceRule, len(infos))
	for i, info := range infos {
		rules[i] = authorizationapi.ResourceRule{
			Verbs:         info.GetVerbs(),
			APIGroups:     info.GetAPIGroups(),
			Resources:     info.GetResources(),
			ResourceNames: info.GetResourceNames(),
		}
	}
	return rules
}

// getNonResourceRules 将 Authorizer 返回的非资源规则信息转换为 API NonResourceRule 列表。
func getNonResourceRules(infos []authorizer.NonResourceRuleInfo) []authorizationapi.NonResourceRule {
	rules := make([]authorizationapi.NonResourceRule, len(infos))
	for i, info := range infos {
		rules[i] = authorizationapi.NonResourceRule{
			Verbs:           info.GetVerbs(),
			NonResourceURLs: info.GetNonResourceURLs(),
		}
	}
	return rules
}
