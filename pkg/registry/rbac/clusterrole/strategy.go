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

package clusterrole

import (
	"context"

	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/rbac"
	"k8s.io/kubernetes/pkg/apis/rbac/validation"
)

// strategy 定义 ClusterRole 的集群级 REST 策略，除 PolicyRule 校验外，还校验聚合规则中的 LabelSelector。
type strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 ClusterRole 对象时使用的默认 REST 策略。
var Strategy = strategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 false，因为 ClusterRole 是集群级资源。
func (strategy) NamespaceScoped() bool {
	return false
}

// AllowCreateOnUpdate 返回 true，允许通过 PUT 更新请求创建尚不存在的 ClusterRole。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// PrepareForCreate 是 ClusterRole 创建前的字段预处理入口；当前不需要清理客户端提交的字段。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	_ = obj.(*rbac.ClusterRole)
}

// PrepareForUpdate 是 ClusterRole 更新前的字段预处理入口；当前不需要改写新旧对象字段。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClusterRole := obj.(*rbac.ClusterRole)
	oldClusterRole := old.(*rbac.ClusterRole)

	_, _ = newClusterRole, oldClusterRole
}

// Validate 校验 ClusterRole 的 PolicyRule，并检查 aggregationRule 中的 LabelSelector。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	clusterRole := obj.(*rbac.ClusterRole)
	opts := validation.ClusterRoleValidationOptions{
		AllowInvalidLabelValueInSelector: false,
	}
	return validation.ValidateClusterRole(clusterRole, opts)
}

// WarningsOnCreate 返回创建 ClusterRole 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化 ClusterRole；当前不执行额外转换。
func (strategy) Canonicalize(obj runtime.Object) {
	_ = obj.(*rbac.ClusterRole)
}

// ValidateUpdate 校验 ClusterRole 更新；允许保留旧对象中的历史非法 Selector Label 值，但不允许新引入非法值。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newObj := obj.(*rbac.ClusterRole)
	oldObj := old.(*rbac.ClusterRole)
	opts := validation.ClusterRoleValidationOptions{
		AllowInvalidLabelValueInSelector: hasInvalidLabelValueInLabelSelector(oldObj),
	}
	return validation.ValidateClusterRoleUpdate(newObj, oldObj, opts)
}

// WarningsOnUpdate 返回更新 ClusterRole 时的非阻断性警告；当前不产生额外警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true：请求未提供 ResourceVersion 时，通用更新流程会填入存储中的最新版本；
// 若请求提供了版本，则仍需与 etcd 中的最新对象版本匹配。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// 检查旧 ClusterRole 聚合选择器是否包含历史非法值，用于决定更新兼容选项。
func hasInvalidLabelValueInLabelSelector(role *rbac.ClusterRole) bool {
	if role.AggregationRule != nil {
		labelSelectorValidationOptions := metav1validation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: false}
		for _, selector := range role.AggregationRule.ClusterRoleSelectors {
			if len(metav1validation.ValidateLabelSelector(&selector, labelSelectorValidationOptions, nil)) > 0 {
				return true
			}
		}
	}
	return false
}
