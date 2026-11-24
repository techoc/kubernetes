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

package rolebinding

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

// strategy 定义 RoleBinding 的命名空间级写入策略；RoleRef 创建后不可更改，Subjects 可以按规则增删。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 RoleBinding 对象时使用的默认 REST 策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 true，因为 RoleBinding 是命名空间级资源。
func (strategy) NamespaceScoped() bool {
	return true
}

// AllowCreateOnUpdate 返回 true，允许通过 PUT 更新请求创建尚不存在的 RoleBinding。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// PrepareForCreate 是 RoleBinding 创建前的字段预处理入口；当前不需要清理客户端提交的字段。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*rbac.RoleBinding)
}

// PrepareForUpdate 是 RoleBinding 更新前的字段预处理入口；当前不需要改写新旧对象字段。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRoleBinding := obj.(*rbac.RoleBinding)
	oldRoleBinding := old.(*rbac.RoleBinding)

	_, _ = newRoleBinding, oldRoleBinding
}

// Validate 校验 RoleRef 的 kind、apiGroup、name，以及 Subject 类型和 ServiceAccount namespace。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	roleBinding := obj.(*rbac.RoleBinding)
	return validation.ValidateRoleBinding(roleBinding)
}

// WarningsOnCreate 返回创建 RoleBinding 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化 RoleBinding；当前不执行额外转换。
func (strategy) Canonicalize(obj runtime.Object) {
	_ = obj.(*rbac.RoleBinding)
}

// ValidateUpdate 校验 RoleBinding 更新，并确保创建后不可变的 RoleRef 没有改变。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {

	newRoleBinding := obj.(*rbac.RoleBinding)
	oldRoleBinding := old.(*rbac.RoleBinding)

	return validation.ValidateRoleBindingUpdate(newRoleBinding, oldRoleBinding)
}

// Declarative 与手写校验不一致时短路返回手写子字段错误，确保 RoleRef 不可变错误路径一致。
func (strategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{
		ShortCircuitMismatch: true,
	}
}

// WarningsOnUpdate 返回更新 RoleBinding 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true：请求未提供 ResourceVersion 时，通用更新流程会填入存储中的最新版本；
// 若请求提供了版本，则仍需与 etcd 中的最新对象版本匹配。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
