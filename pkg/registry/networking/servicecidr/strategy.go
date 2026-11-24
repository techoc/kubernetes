/*
Copyright 2023 The Kubernetes Authors.

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

package servicecidr

import (
	"context"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/apis/networking/validation"
	"k8s.io/kubernetes/pkg/features"
)

// serviceCIDRStrategy 定义 ServiceCIDR 地址池资源的 REST 策略，负责 CIDR 校验、Status 字段兼容和不可变规则。
type serviceCIDRStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 ServiceCIDR 对象时使用的默认策略。
var Strategy = serviceCIDRStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 确保 Strategy 实现 rest.RESTCreateStrategy 接口。
var _ rest.RESTCreateStrategy = Strategy

// 确保 Strategy 实现 rest.RESTUpdateStrategy 接口。
var _ rest.RESTUpdateStrategy = Strategy

// NamespaceScoped 返回 false，因为 ServiceCIDR 是集群级资源。
func (serviceCIDRStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 根据 ServiceCIDRStatusFieldWiping 特性门控决定是否保护 Status 字段。
func (serviceCIDRStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"networking.k8s.io/v1":      fieldpath.NewSet(),
		"networking.k8s.io/v1beta1": fieldpath.NewSet(),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceCIDRStatusFieldWiping) {
		fields["networking.k8s.io/v1"].Insert(fieldpath.MakePathOrDie("status"))
		fields["networking.k8s.io/v1beta1"].Insert(fieldpath.MakePathOrDie("status"))
	}
	return fields
}

// PrepareForCreate 在创建时根据 ServiceCIDRStatusFieldWiping 特性门控决定是否清空 Status。
func (serviceCIDRStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	cidr := obj.(*networking.ServiceCIDR)
	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceCIDRStatusFieldWiping) {
		cidr.Status = networking.ServiceCIDRStatus{}
	}
}

// PrepareForUpdate 在特性门控开启时恢复旧 Status，防止普通更新绕过控制器写入状态。
func (serviceCIDRStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newServiceCIDR := obj.(*networking.ServiceCIDR)
	oldServiceCIDR := old.(*networking.ServiceCIDR)

	if utilfeature.DefaultFeatureGate.Enabled(features.ServiceCIDRStatusFieldWiping) {
		newServiceCIDR.Status = oldServiceCIDR.Status
	}
}

// Validate 校验新 ServiceCIDR 的 CIDR 格式、地址族及地址范围约束。
func (serviceCIDRStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	cidrConfig := obj.(*networking.ServiceCIDR)
	err := validation.ValidateServiceCIDR(cidrConfig)
	return err
}

// Canonicalize 在校验后规范化 ServiceCIDR；当前无需额外处理。
func (serviceCIDRStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，ServiceCIDR 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (serviceCIDRStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// WarningsOnCreate 返回创建 ServiceCIDR 时的非阻断性警告；当前不产生额外警告。
func (serviceCIDRStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// ValidateUpdate 对照旧对象校验 ServiceCIDR 更新是否合法，并检查地址范围等约束。
func (serviceCIDRStrategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newServiceCIDR := new.(*networking.ServiceCIDR)
	oldServiceCIDR := old.(*networking.ServiceCIDR)
	errList := validation.ValidateServiceCIDRUpdate(newServiceCIDR, oldServiceCIDR)
	return errList
}

// AllowUnconditionalUpdate 返回 true，允许 ServiceCIDR 更新请求不携带 ResourceVersion。
func (serviceCIDRStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// WarningsOnUpdate 返回更新 ServiceCIDR 时的非阻断性警告；当前不产生额外警告。
func (serviceCIDRStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

type serviceCIDRStatusStrategy struct {
	serviceCIDRStrategy
}

// StatusStrategy 定义通过 /status 子资源更新 ServiceCIDR 状态时使用的策略。
var StatusStrategy = serviceCIDRStatusStrategy{Strategy}

// GetResetFields 返回 Status 子资源策略重置的 Metadata 和 Spec 字段集合，确保该入口只管理状态。
func (serviceCIDRStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"networking.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"networking.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}
	return fields
}

// PrepareForUpdate 在 Status 更新时恢复旧 Spec 和受保护的 Metadata，防止绕过分配器改变地址池范围。
func (serviceCIDRStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newServiceCIDR := obj.(*networking.ServiceCIDR)
	oldServiceCIDR := old.(*networking.ServiceCIDR)
	// Status 更新请求不能修改 Spec，必须沿用旧对象中的 CIDR 地址范围。
	newServiceCIDR.Spec = oldServiceCIDR.Spec
	metav1.ResetObjectMetaForStatus(&newServiceCIDR.ObjectMeta, &oldServiceCIDR.ObjectMeta)
}

// ValidateUpdate 对照旧对象校验 ServiceCIDR Status 更新是否合法。
func (serviceCIDRStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateServiceCIDRStatusUpdate(obj.(*networking.ServiceCIDR), old.(*networking.ServiceCIDR))
}

// WarningsOnUpdate 返回 ServiceCIDR Status 更新时的非阻断性警告；当前不产生额外警告。
func (serviceCIDRStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
