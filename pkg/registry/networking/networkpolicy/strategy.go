/*
Copyright 2017 The Kubernetes Authors.

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

package networkpolicy

import (
	"context"
	"k8s.io/apiserver/pkg/registry/rest"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/apis/networking/validation"
)

// networkPolicyStrategy 定义 NetworkPolicy 主资源写入策略，维护命名空间作用域、Generation、选择器和网络规则校验。
type networkPolicyStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 NetworkPolicy 对象时使用的默认策略。
var Strategy = networkPolicyStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，因为 NetworkPolicy 必须归属于某个命名空间。
func (networkPolicyStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 在创建 NetworkPolicy 时初始化 Generation；网络规则由 Spec 完整描述。
func (networkPolicyStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	networkPolicy := obj.(*networking.NetworkPolicy)
	networkPolicy.Generation = 1
}

// PrepareForUpdate 在 Spec 发生变化时递增 Generation，供网络插件识别策略版本。
func (networkPolicyStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNetworkPolicy := obj.(*networking.NetworkPolicy)
	oldNetworkPolicy := old.(*networking.NetworkPolicy)

	// Spec 发生变化时递增 Generation；状态变化不应改变 Generation。
	// 控制器或网络插件可据此判断当前处理的策略版本。
	if !reflect.DeepEqual(oldNetworkPolicy.Spec, newNetworkPolicy.Spec) {
		newNetworkPolicy.Generation = oldNetworkPolicy.Generation + 1
	}
}

// Validate 根据新旧对象可用的兼容选项校验 NetworkPolicy 的选择器、策略类型、端口和对端规则。
func (networkPolicyStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	networkPolicy := obj.(*networking.NetworkPolicy)
	ops := validation.ValidationOptionsForNetworking(networkPolicy, nil)
	return validation.ValidateNetworkPolicy(networkPolicy, ops)
}

// WarningsOnCreate 返回创建 NetworkPolicy 时的非阻断性警告，重点检查 IPBlock 中的 CIDR。
func (networkPolicyStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return networkPolicyWarnings(obj.(*networking.NetworkPolicy))
}

// Canonicalize 在校验后规范化 NetworkPolicy；当前无需额外处理。
func (networkPolicyStrategy) Canonicalize(obj runtime.Object) {}

// AllowCreateOnUpdate 返回 false，NetworkPolicy 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (networkPolicyStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 根据新旧对象计算网络策略兼容选项，并校验更新后的规则是否合法。
func (networkPolicyStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	opts := validation.ValidationOptionsForNetworking(obj.(*networking.NetworkPolicy), old.(*networking.NetworkPolicy))
	return validation.ValidateNetworkPolicyUpdate(obj.(*networking.NetworkPolicy), old.(*networking.NetworkPolicy), opts)
}

// WarningsOnUpdate 返回更新 NetworkPolicy 时的非阻断性警告，检查 Ingress/Egress IPBlock 的 CIDR。
func (networkPolicyStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return networkPolicyWarnings(obj.(*networking.NetworkPolicy))
}

// AllowUnconditionalUpdate 返回 true，允许 NetworkPolicy 更新请求不携带 ResourceVersion。
func (networkPolicyStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// networkPolicyWarnings 遍历 Ingress.From/Egress.To 的 IPBlock，分别校验 CIDR 与 Except 网段并生成 Warning。
func networkPolicyWarnings(networkPolicy *networking.NetworkPolicy) []string {
	var warnings []string
	for i := range networkPolicy.Spec.Ingress {
		for j := range networkPolicy.Spec.Ingress[i].From {
			ipBlock := networkPolicy.Spec.Ingress[i].From[j].IPBlock
			if ipBlock == nil {
				continue
			}
			fldPath := field.NewPath("spec").Child("ingress").Index(i).Child("from").Index(j).Child("ipBlock")
			warnings = append(warnings, utilvalidation.GetWarningsForCIDR(fldPath.Child("cidr"), ipBlock.CIDR)...)
			for k, except := range ipBlock.Except {
				warnings = append(warnings, utilvalidation.GetWarningsForCIDR(fldPath.Child("except").Index(k), except)...)
			}
		}
	}
	for i := range networkPolicy.Spec.Egress {
		for j := range networkPolicy.Spec.Egress[i].To {
			ipBlock := networkPolicy.Spec.Egress[i].To[j].IPBlock
			if ipBlock == nil {
				continue
			}
			fldPath := field.NewPath("spec").Child("egress").Index(i).Child("to").Index(j).Child("ipBlock")
			warnings = append(warnings, utilvalidation.GetWarningsForCIDR(fldPath.Child("cidr"), ipBlock.CIDR)...)
			for k, except := range ipBlock.Except {
				warnings = append(warnings, utilvalidation.GetWarningsForCIDR(fldPath.Child("except").Index(k), except)...)
			}
		}
	}
	return warnings
}
