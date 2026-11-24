/*
Copyright 2024 The Kubernetes Authors.

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

package leasecandidate

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"

	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/coordination"
	"k8s.io/kubernetes/pkg/apis/coordination/validation"
)

// LeaseCandidateStrategy 定义 LeaseCandidate 资源创建、更新时使用的 REST 策略。
type LeaseCandidateStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 LeaseCandidate 对象时使用的默认策略。
var Strategy = LeaseCandidateStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，表示 LeaseCandidate 必须归属于某个命名空间。
func (LeaseCandidateStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 当前不改写创建对象；字段合法性由 Validate 统一检查。
func (LeaseCandidateStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
}

// PrepareForUpdate 当前不改写新旧对象；更新约束由 ValidateUpdate 负责检查。
func (LeaseCandidateStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
}

// Validate 校验新 LeaseCandidate 的字段格式和语义约束。
func (LeaseCandidateStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	leasecandidate := obj.(*coordination.LeaseCandidate)
	return validation.ValidateLeaseCandidate(leasecandidate)
}

// WarningsOnCreate 返回创建 LeaseCandidate 时的非阻断性警告；当前不产生额外警告。
func (LeaseCandidateStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 LeaseCandidate；当前无需额外处理。
func (LeaseCandidateStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 true，允许客户端通过 PUT 更新请求创建尚不存在的 LeaseCandidate。
func (LeaseCandidateStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// ValidateUpdate 对照旧对象校验 LeaseCandidate 更新是否合法。
func (LeaseCandidateStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateLeaseCandidateUpdate(obj.(*coordination.LeaseCandidate), old.(*coordination.LeaseCandidate))
}

// WarningsOnUpdate 返回更新 LeaseCandidate 时的非阻断性警告；当前不产生额外警告。
func (LeaseCandidateStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，要求更新请求提供 ResourceVersion，以执行乐观并发控制。
func (LeaseCandidateStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// GetAttrs 提取 LeaseCandidate 的标签和可筛选字段，供 List、Watch 等请求执行过滤。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	leasecandidate, ok := obj.(*coordination.LeaseCandidate)
	if !ok {
		return nil, nil, fmt.Errorf("not a leaseCandidate")
	}
	return labels.Set(leasecandidate.ObjectMeta.Labels), ToSelectableFields(leasecandidate), nil
}

// ToSelectableFields 将 LeaseCandidate 转换为字段选择器可查询的字段集合。
// TODO：字段选择器与标签选择器不是同一概念，不应套用标签选择器的校验规则。
func ToSelectableFields(leasecandidate *coordination.LeaseCandidate) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&leasecandidate.ObjectMeta, true)
	specificFieldsSet := fields.Set{
		"spec.leaseName": string(leasecandidate.Spec.LeaseName),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}
