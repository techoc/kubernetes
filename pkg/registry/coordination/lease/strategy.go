/*
Copyright 2018 The Kubernetes Authors.

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

package lease

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/coordination"
	"k8s.io/kubernetes/pkg/apis/coordination/validation"
	"k8s.io/kubernetes/pkg/features"
)

// leaseStrategy 定义 Lease 资源创建、更新时使用的 REST 策略。
type leaseStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 Lease 对象时使用的默认策略。
var Strategy = leaseStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，表示 Lease 必须归属于某个命名空间。
func (leaseStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 在创建 Lease 前根据 CoordinatedLeaderElection 特性门控清理受控字段。
func (leaseStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	lease := obj.(*coordination.Lease)
	if !utilfeature.DefaultFeatureGate.Enabled(features.CoordinatedLeaderElection) {
		// 特性未启用时，不允许创建请求设置协调式选主字段。
		lease.Spec.Strategy = nil
		lease.Spec.PreferredHolder = nil
	}

}

// PrepareForUpdate 在更新 Lease 时保护特性门控关闭期间不可新设的协调式选主字段。
func (leaseStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	oldLease := old.(*coordination.Lease)
	newLease := obj.(*coordination.Lease)
	if !utilfeature.DefaultFeatureGate.Enabled(features.CoordinatedLeaderElection) {
		// 特性关闭时，只有旧对象已具有相应字段的情况下才保留该字段，避免用户借更新请求启用新能力。
		if oldLease == nil || oldLease.Spec.Strategy == nil {
			newLease.Spec.Strategy = nil
		}
		if oldLease == nil || oldLease.Spec.PreferredHolder == nil {
			newLease.Spec.PreferredHolder = nil
		}
	}
}

// Validate 校验新 Lease 的字段格式和语义约束。
func (leaseStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	lease := obj.(*coordination.Lease)
	return validation.ValidateLease(lease)
}

// WarningsOnCreate 返回创建 Lease 时的非阻断性警告；当前不产生额外警告。
func (leaseStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string { return nil }

// Canonicalize 在校验后规范化 Lease；当前无需额外处理。
func (leaseStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 true，允许客户端通过 PUT 更新请求创建尚不存在的 Lease。
func (leaseStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return true
}

// ValidateUpdate 对照旧对象校验 Lease 更新是否合法。
func (leaseStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateLeaseUpdate(obj.(*coordination.Lease), old.(*coordination.Lease))
}

// WarningsOnUpdate 返回更新 Lease 时的非阻断性警告；当前不产生额外警告。
func (leaseStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 false，要求更新请求提供 ResourceVersion，以执行乐观并发控制。
func (leaseStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
