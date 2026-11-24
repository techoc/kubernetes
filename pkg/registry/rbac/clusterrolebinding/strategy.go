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

package clusterrolebinding

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/rbac"
	"k8s.io/kubernetes/pkg/apis/rbac/validation"
)

// strategy 定义 ClusterRoleBinding 的集群级写入策略；RoleRef 创建后不可更换，Subject 决定获得集群范围权限的身份。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 ClusterRoleBinding 对象时使用的默认 REST 策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 false，因为 ClusterRoleBinding 是集群级资源。
func (strategy) NamespaceScoped() bool {
	return false
}

// AllowCreateOnUpdate 返回 true，允许通过 PUT 更新请求创建尚不存在的 ClusterRoleBinding。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// PrepareForCreate 是 ClusterRoleBinding 创建前的字段预处理入口；当前不需要清理客户端提交的字段。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*rbac.ClusterRoleBinding)
}

// PrepareForUpdate 是 ClusterRoleBinding 更新前的字段预处理入口；当前不需要改写新旧对象字段。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClusterRoleBinding := obj.(*rbac.ClusterRoleBinding)
	oldClusterRoleBinding := old.(*rbac.ClusterRoleBinding)

	_, _ = newClusterRoleBinding, oldClusterRoleBinding
}

func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	clusterRoleBinding := obj.(*rbac.ClusterRoleBinding)
	return validation.ValidateClusterRoleBinding(clusterRoleBinding)
}

// WarningsOnCreate 返回创建 ClusterRoleBinding 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化 ClusterRoleBinding；当前不执行额外转换。
func (strategy) Canonicalize(obj runtime.Object) {
	_ = obj.(*rbac.ClusterRoleBinding)
}

// ValidateUpdate 校验 ClusterRoleBinding 更新，并确保创建后不可变的 RoleRef 没有改变。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newObj := obj.(*rbac.ClusterRoleBinding)
	oldObj := old.(*rbac.ClusterRoleBinding)
	return validation.ValidateClusterRoleBindingUpdate(newObj, oldObj)
}

// WarningsOnUpdate 返回更新 ClusterRoleBinding 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// 短路返回手写 RoleRef 不可变错误，使 declarative 与传统校验对该字段给出一致结果。
func (strategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	// 让声明式校验的短路错误与手写校验的子字段错误保持一致。
	// ClusterRoleBinding.RoleRef 创建后不可变，因此需要保留该错误路径。
	return rest.DeclarativeValidationConfig{
		ShortCircuitMismatch: true,
	}
}

// AllowUnconditionalUpdate 返回 true：请求未提供 ResourceVersion 时，通用更新流程会填入存储中的最新版本；
// 若请求提供了版本，则仍需与 etcd 中的最新对象版本匹配。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
