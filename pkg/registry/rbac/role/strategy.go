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

package role

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

// strategy 定义 Role 的 REST 创建/更新策略；Role 是命名空间级授权规则集合，本身不直接绑定用户。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 Role 对象时使用的默认 REST 策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 true，因为 Role 是命名空间级资源。
func (strategy) NamespaceScoped() bool {
	return true
}

// AllowCreateOnUpdate 返回 true，允许通过 PUT 更新请求创建尚不存在的 Role。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// PrepareForCreate 是 Role 创建前的字段预处理入口；当前不需要清理客户端提交的字段。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*rbac.Role)
}

// PrepareForUpdate 是 Role 更新前的字段预处理入口；当前不需要改写新旧对象字段。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newRole := obj.(*rbac.Role)
	oldRole := old.(*rbac.Role)

	_, _ = newRole, oldRole
}

// Validate 校验新 Role 中的 PolicyRule，确保 APIGroup、Resource、ResourceName、Verb 和 NonResourceURL 组合合法。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	role := obj.(*rbac.Role)
	return validation.ValidateRole(role)
}

// WarningsOnCreate 返回创建 Role 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化 Role；当前不执行额外转换。
func (strategy) Canonicalize(obj runtime.Object) {
	_ = obj.(*rbac.Role)
}

// ValidateUpdate 使用旧对象和新对象执行 Role 更新语义校验。
// 更新校验沿用相同的规则语法，并结合旧对象执行更新语义检查。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newObj := obj.(*rbac.Role)
	oldObj := old.(*rbac.Role)
	return validation.ValidateRoleUpdate(newObj, oldObj)
}

// WarningsOnUpdate 返回更新 Role 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true：请求未提供 ResourceVersion 时，通用更新流程会填入存储中的最新版本；
// 若请求提供了版本，则仍需与 etcd 中的最新对象版本匹配。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
