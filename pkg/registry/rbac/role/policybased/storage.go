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

// 本包在通用 Role Storage 外包装防提权检查：普通用户只能创建/更新自己当前已经拥有的权限规则。
package policybased

import (
	"context"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/rest"
	kapihelper "k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/apis/rbac"
	rbacregistry "k8s.io/kubernetes/pkg/registry/rbac"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
)

var groupResource = rbac.Resource("roles")

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
	return true
}

func (r *Storage) StorageVersion() runtime.GroupVersioner {
	svp, ok := r.StandardStorage.(rest.StorageVersionProvider)
	if !ok {
		return nil
	}
	return svp.StorageVersion()
}

var _ rest.StorageVersionProvider = &Storage{}

// Create 对超级用户或拥有 escalate 动词的用户放行；否则确认新 Role 的规则都被当前用户权限覆盖。
func (s *Storage) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if rbacregistry.EscalationAllowed(ctx) || rbacregistry.RoleEscalationAuthorized(ctx, s.authorizer) {
		return s.StandardStorage.Create(ctx, obj, createValidation, options)
	}

	role := obj.(*rbac.Role)
	rules := role.Rules
	if err := rbacregistryvalidation.ConfirmNoEscalationInternal(ctx, s.ruleResolver, rules); err != nil {
		return nil, errors.NewForbidden(groupResource, role.Name, err)
	}
	return s.StandardStorage.Create(ctx, obj, createValidation, options)
}

// Update 用 UpdatedObjectInfo 包装器检查最终对象，确保修改后的规则不产生权限升级；纯 GC 元数据变更可跳过检查。
func (s *Storage) Update(ctx context.Context, name string, obj rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	if rbacregistry.EscalationAllowed(ctx) || rbacregistry.RoleEscalationAuthorized(ctx, s.authorizer) {
		return s.StandardStorage.Update(ctx, name, obj, createValidation, updateValidation, forceAllowCreate, options)
	}

	nonEscalatingInfo := rest.WrapUpdatedObjectInfo(obj, func(ctx context.Context, obj runtime.Object, oldObj runtime.Object) (runtime.Object, error) {
		role := obj.(*rbac.Role)

		// 如果本次更新只修改垃圾回收最终删除对象所需的元数据，则无需重新执行权限覆盖检查。
		if rbacregistry.IsOnlyMutatingGCFields(obj, oldObj, kapihelper.Semantic) {
			return obj, nil
		}

		rules := role.Rules
		if err := rbacregistryvalidation.ConfirmNoEscalationInternal(ctx, s.ruleResolver, rules); err != nil {
			return nil, errors.NewForbidden(groupResource, role.Name, err)
		}
		return obj, nil
	})

	return s.StandardStorage.Update(ctx, name, nonEscalatingInfo, createValidation, updateValidation, forceAllowCreate, options)
}

var _ rest.SingularNameProvider = &Storage{}

func (s *Storage) GetSingularName() string {
	snp, ok := s.StandardStorage.(rest.SingularNameProvider)
	if !ok {
		return ""
	}
	return snp.GetSingularName()
}
