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
// 此文件提供 SubjectLocator 功能，用于查找可以执行特定操作的所有主体。
package rbac

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
)

type RoleToRuleMapper interface {
	// GetRoleReferenceRules 尝试解析 RoleBinding 或 ClusterRoleBinding 的角色引用。
	// 传入的 namespace 是 role binding 的命名空间，如果是 cluster role binding 则为空字符串。
	GetRoleReferenceRules(ctx context.Context, roleRef rbacv1.RoleRef, namespace string) ([]rbacv1.PolicyRule, error)
}

// SubjectLocator 查找可以执行特定操作的所有主体。
// 用于 Webhook 等场景，需要知道哪些用户或组有权限执行某项操作。
type SubjectLocator interface {
	AllowedSubjects(ctx context.Context, attributes authorizer.Attributes) ([]rbacv1.Subject, error)
}

var _ = SubjectLocator(&SubjectAccessEvaluator{})

// SubjectAccessEvaluator 实现 SubjectLocator 接口，
// 通过遍历所有 RoleBinding 和 ClusterRoleBinding 来查找有权限的主体。
type SubjectAccessEvaluator struct {
	superUser string // 超级用户，始终被允许

	roleBindingLister        rbacregistryvalidation.RoleBindingLister
	clusterRoleBindingLister rbacregistryvalidation.ClusterRoleBindingLister
	roleToRuleMapper         RoleToRuleMapper
}

// NewSubjectAccessEvaluator 创建一个新的主题访问评估器。
// superUser 是始终被允许的超级用户（通常是 kube-apiserver 的认证配置中指定的用户）。
func NewSubjectAccessEvaluator(roles rbacregistryvalidation.RoleGetter, roleBindings rbacregistryvalidation.RoleBindingLister, clusterRoles rbacregistryvalidation.ClusterRoleGetter, clusterRoleBindings rbacregistryvalidation.ClusterRoleBindingLister, superUser string) *SubjectAccessEvaluator {
	subjectLocator := &SubjectAccessEvaluator{
		superUser:                superUser,
		roleBindingLister:        roleBindings,
		clusterRoleBindingLister: clusterRoleBindings,
		roleToRuleMapper: rbacregistryvalidation.NewDefaultRuleResolver(
			roles, roleBindings, clusterRoles, clusterRoleBindings,
		),
	}
	return subjectLocator
}

// AllowedSubjects 返回可以执行指定操作的所有主体，以及计算过程中遇到的错误。
// 如果某些 rolebinding 无法解析但其他可以，则可能同时返回主体和错误。
// 始终包含 system:privileged 组和 superUser（如果配置了）。
func (r *SubjectAccessEvaluator) AllowedSubjects(ctx context.Context, requestAttributes authorizer.Attributes) ([]rbacv1.Subject, error) {
	// 始终允许 system:privileged 组（集群管理员）
	subjects := []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: user.SystemPrivilegedGroup}}
	if len(r.superUser) > 0 {
		subjects = append(subjects, rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: r.superUser})
	}
	errorlist := []error{}

	if clusterRoleBindings, err := r.clusterRoleBindingLister.ListClusterRoleBindings(ctx); err != nil {
		errorlist = append(errorlist, err)

	} else {
		for _, clusterRoleBinding := range clusterRoleBindings {
			rules, err := r.roleToRuleMapper.GetRoleReferenceRules(ctx, clusterRoleBinding.RoleRef, "")
			if err != nil {
				// 如果解析失败，记录错误并继续处理。因为规则是累加的，
				// 缺少一个引用虽然不好，但其他 rolebinding 仍然可以产生有效列表。
				errorlist = append(errorlist, err)
			}
			if RulesAllow(requestAttributes, rules...) {
				subjects = append(subjects, clusterRoleBinding.Subjects...)
			}
		}
	}

	if namespace := requestAttributes.GetNamespace(); len(namespace) > 0 {
		if roleBindings, err := r.roleBindingLister.ListRoleBindings(ctx, namespace); err != nil {
			errorlist = append(errorlist, err)

		} else {
			for _, roleBinding := range roleBindings {
				rules, err := r.roleToRuleMapper.GetRoleReferenceRules(ctx, roleBinding.RoleRef, namespace)
				if err != nil {
					// 同上：记录错误并继续
					errorlist = append(errorlist, err)
				}
				if RulesAllow(requestAttributes, rules...) {
					subjects = append(subjects, roleBinding.Subjects...)
				}
			}
		}
	}

	// 去重：同一个主体可能通过多个 binding 获得相同的权限
	dedupedSubjects := []rbacv1.Subject{}
	for _, subject := range subjects {
		found := false
		for _, curr := range dedupedSubjects {
			if curr == subject {
				found = true
				break
			}
		}

		if !found {
			dedupedSubjects = append(dedupedSubjects, subject)
		}
	}

	return subjects, utilerrors.NewAggregate(errorlist)
}
