/*
Copyright 2014 The Kubernetes Authors.

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

package mutatingwebhookconfiguration

import (
	"context"
	"reflect"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
	"k8s.io/kubernetes/pkg/apis/admissionregistration/validation"
	"k8s.io/kubernetes/pkg/kubeapiserver/admission/exclusion"
)

// mutatingWebhookConfigurationStrategy 实现 MutatingWebhookConfiguration 的集群级写入校验策略。
// mutatingWebhookConfigurationStrategy 定义 MutatingWebhookConfiguration 集群级写入策略，管理 Webhook 规则、Generation 和兼容告警。
type mutatingWebhookConfigurationStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 MutatingWebhookConfiguration 时使用的默认写入策略。
var Strategy = mutatingWebhookConfigurationStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，表示 MutatingWebhookConfiguration 是集群级资源。
func (mutatingWebhookConfigurationStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建时初始化 Generation；该资源不通过 Status 子资源维护控制器状态。
// 创建时初始化 Generation；Webhook Configuration 不由 Admission Controller 回写 Status。
func (mutatingWebhookConfigurationStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ic := obj.(*admissionregistration.MutatingWebhookConfiguration)
	ic.Generation = 1
}

// WarningsOnCreate 返回创建 MutatingWebhookConfiguration 时需要提示的静态配置告警。
// 创建时检查静态 Admission 配置名称规则，以及启用特性后被排除的虚拟资源规则。
func (mutatingWebhookConfigurationStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	ic := obj.(*admissionregistration.MutatingWebhookConfiguration)
	var warnings []string
	if !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		warnings = append(warnings, validation.WarningsForStaticSuffix(ic.Name)...)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ExcludeAdmissionWebhookVirtualResources) {
		warnings = append(warnings, validation.WarningsForMutatingWebhookRules(ic.Webhooks, exclusion.Excluded())...)
	}
	return warnings
}

// PrepareForUpdate 仅在 Webhooks 配置发生变化时递增 Generation。
// 仅 Webhooks 列表发生变化时递增 Generation，避免无关 metadata 更新触发 Webhook 配置重新加载。
func (mutatingWebhookConfigurationStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIC := obj.(*admissionregistration.MutatingWebhookConfiguration)
	oldIC := old.(*admissionregistration.MutatingWebhookConfiguration)

	// Webhooks 配置发生变化时递增 Generation，避免无关 metadata 更新触发配置重新加载。
	// Generation 字段的语义详见 metav1.ObjectMeta 文档。
	if !reflect.DeepEqual(oldIC.Webhooks, newIC.Webhooks) {
		newIC.Generation = oldIC.Generation + 1
	}
}

// Validate 校验 MutatingWebhookConfiguration 的静态配置及当前特性门控要求。
// Validate 校验 MutatingWebhook 的规则、客户端配置和 Feature Gate 约束。
func (mutatingWebhookConfigurationStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ic := obj.(*admissionregistration.MutatingWebhookConfiguration)
	errs := validation.ValidateMutatingWebhookConfiguration(ic)
	if utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		errs = append(errs, validation.ValidateStaticSuffix(ic.Name, field.NewPath("metadata", "name"))...)
	}
	return errs
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (mutatingWebhookConfigurationStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，禁止通过 PUT 更新请求创建 MutatingWebhookConfiguration。
func (mutatingWebhookConfigurationStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 校验 MutatingWebhookConfiguration 更新时的新旧 Webhook 配置约束。
func (mutatingWebhookConfigurationStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newIC := obj.(*admissionregistration.MutatingWebhookConfiguration)
	oldIC := old.(*admissionregistration.MutatingWebhookConfiguration)
	errs := validation.ValidateMutatingWebhookConfigurationUpdate(newIC, oldIC)
	return errs
}

// WarningsOnUpdate 返回本次 MutatingWebhookConfiguration 更新涉及的配置告警。
func (mutatingWebhookConfigurationStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newIC := obj.(*admissionregistration.MutatingWebhookConfiguration)
	var warnings []string
	warnings = append(warnings, validation.WarningsForStaticSuffix(newIC.Name)...)
	if utilfeature.DefaultFeatureGate.Enabled(features.ExcludeAdmissionWebhookVirtualResources) {
		warnings = append(warnings, validation.WarningsForMutatingWebhookRules(newIC.Webhooks, exclusion.Excluded())...)
	}
	return warnings
}

// AllowUnconditionalUpdate 返回 false，更新必须携带匹配的资源版本，以保护并发更新安全。
// 更新必须基于匹配的资源版本执行。
func (mutatingWebhookConfigurationStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
