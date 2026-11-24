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

// 本包为 ClusterRole 主存储增加防提权逻辑，并额外保护可聚合 ClusterRole 的 aggregationRule。
package policybased

import (
	"context"
	"errors"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/rest"
	kapihelper "k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/apis/rbac"
	rbacregistry "k8s.io/kubernetes/pkg/registry/rbac"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
)

var groupResource = rbac.Resource("clusterroles")

type Storage struct {
	rest.StandardStorage

	authorizer authorizer.UnconditionalAuthorizer

	ruleResolver rbacregistryvalidation.AuthorizationRuleResolver
}

func NewStorage(s rest.StandardStorage, authorizer authorizer.UnconditionalAuthorizer, ruleResolver rbacregistryvalidation.AuthorizationRuleResolver) *Storage {
	return &Storage{s, authorizer, ruleResolver}
}

// Destroy 在 API Server 关闭时销毁底层标准存储。
func (r *Storage) Destroy() {
	r.StandardStorage.Destroy()
}

func (r *Storage) NamespaceScoped() bool {
	return false
}

func (r *Storage) StorageVersion() runtime.GroupVersioner {
	svp, ok := r.StandardStorage.(rest.StorageVersionProvider)
	if !ok {
		return nil
	}
	return svp.StorageVersion()
}

var _ rest.StorageVersionProvider = &Storage{}

// 修改 aggregationRule 可能间接汇入任意 ClusterRole 权限，因此需要对 * APIGroup/* Resource 拥有完整授权。
var fullAuthority = []rbac.PolicyRule{
	rbac.NewRule("*").Groups("*").Resources("*").RuleOrDie(),
	rbac.NewRule("*").URLs("*").RuleOrDie(),
}

// Create 检查用户已拥有待授予规则；若启用聚合规则，还必须拥有全量集群权限。
func (s *Storage) Create(ctx context.Context, obj runtime.Object, createValidatingAdmission rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if rbacregistry.EscalationAllowed(ctx) || rbacregistry.RoleEscalationAuthorized(ctx, s.authorizer) {
		return s.StandardStorage.Create(ctx, obj, createValidatingAdmission, options)
	}

	clusterRole := obj.(*rbac.ClusterRole)
	rules := clusterRole.Rules
	if err := rbacregistryvalidation.ConfirmNoEscalationInternal(ctx, s.ruleResolver, rules); err != nil {
		return nil, apierrors.NewForbidden(groupResource, clusterRole.Name, err)
	}
	// 设置 aggregationRule 可能聚合任意 ClusterRole 的权限，因此调用者必须拥有所有 API Group 和 Resource 的完整权限。
	if hasAggregationRule(clusterRole) {
		if err := rbacregistryvalidation.ConfirmNoEscalationInternal(ctx, s.ruleResolver, fullAuthority); err != nil {
			return nil, apierrors.NewForbidden(groupResource, clusterRole.Name, errors.New("must have cluster-admin privileges to use the aggregationRule"))
		}
	}

	return s.StandardStorage.Create(ctx, obj, createValidatingAdmission, options)
}

// Update 校验更新后的规则；新旧任一对象存在 aggregationRule 时都要求完整权限，以防通过调整聚合选择器绕过收紧保护。
func (s *Storage) Update(ctx context.Context, name string, obj rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	if rbacregistry.EscalationAllowed(ctx) || rbacregistry.RoleEscalationAuthorized(ctx, s.authorizer) {
		return s.StandardStorage.Update(ctx, name, obj, createValidation, updateValidation, forceAllowCreate, options)
	}

	nonEscalatingInfo := rest.WrapUpdatedObjectInfo(obj, func(ctx context.Context, obj runtime.Object, oldObj runtime.Object) (runtime.Object, error) {
		clusterRole := obj.(*rbac.ClusterRole)
		oldClusterRole := oldObj.(*rbac.ClusterRole)

		// 如果本次更新只修改垃圾回收最终删除对象所需的元数据，则无需重新执行权限覆盖检查。
		if rbacregistry.IsOnlyMutatingGCFields(obj, oldObj, kapihelper.Semantic) {
			return obj, nil
		}

		rules := clusterRole.Rules
		if err := rbacregistryvalidation.ConfirmNoEscalationInternal(ctx, s.ruleResolver, rules); err != nil {
			return nil, apierrors.NewForbidden(groupResource, clusterRole.Name, err)
		}
		// 修改或移除 aggregationRule 可能改变聚合结果并阻止权限收紧，因此调用者必须拥有所有 API Group 和 Resource 的完整权限。
		if hasAggregationRule(clusterRole) || hasAggregationRule(oldClusterRole) {
			if err := rbacregistryvalidation.ConfirmNoEscalationInternal(ctx, s.ruleResolver, fullAuthority); err != nil {
				return nil, apierrors.NewForbidden(groupResource, clusterRole.Name, errors.New("must have cluster-admin privileges to use the aggregationRule"))
			}
		}

		return obj, nil
	})

	return s.StandardStorage.Update(ctx, name, nonEscalatingInfo, createValidation, updateValidation, forceAllowCreate, options)
}

func hasAggregationRule(clusterRole *rbac.ClusterRole) bool {
	return clusterRole.AggregationRule != nil && len(clusterRole.AggregationRule.ClusterRoleSelectors) > 0
}

var _ rest.SingularNameProvider = &Storage{}

func (s *Storage) GetSingularName() string {
	snp, ok := s.StandardStorage.(rest.SingularNameProvider)
	if !ok {
		return ""
	}
	return snp.GetSingularName()
}
