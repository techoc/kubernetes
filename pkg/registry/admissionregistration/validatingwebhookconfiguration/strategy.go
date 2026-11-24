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

package validatingwebhookconfiguration

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

// validatingWebhookConfigurationStrategy 实现 ValidatingWebhookConfiguration 的集群级写入校验策略。
// validatingWebhookConfigurationStrategy 定义 ValidatingWebhookConfiguration 集群级写入策略，负责 Webhook 静态配置校验。
type validatingWebhookConfigurationStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建和更新 ValidatingWebhookConfiguration 时使用的默认写入策略。
var Strategy = validatingWebhookConfigurationStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，表示 ValidatingWebhookConfiguration 是集群级资源，不属于任何命名空间。
func (validatingWebhookConfigurationStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 在创建对象时设置初始 Generation；该资源没有由控制器维护的 Status 子资源。
// 创建时初始化 Generation；Webhook Configuration 无控制器 Status 子资源。
func (validatingWebhookConfigurationStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	ic := obj.(*admissionregistration.ValidatingWebhookConfiguration)
	ic.Generation = 1
}

// PrepareForUpdate 仅在 Webhooks 配置发生变化时递增 Generation。
func (validatingWebhookConfigurationStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newIC := obj.(*admissionregistration.ValidatingWebhookConfiguration)
	oldIC := old.(*admissionregistration.ValidatingWebhookConfiguration)

	// Webhooks 配置发生变化时递增 Generation，避免无关 metadata 更新触发配置重新加载。
	// Generation 字段的语义详见 metav1.ObjectMeta 文档。
	if !reflect.DeepEqual(oldIC.Webhooks, newIC.Webhooks) {
		newIC.Generation = oldIC.Generation + 1
	}
}

// Validate 校验 ValidatingWebhookConfiguration 中各个 Webhook 的规则、客户端配置和执行约束。
// Validate 检查 Webhook 名称、ClientConfig、Rules、FailurePolicy、MatchPolicy、SideEffects 和 Timeout 等约束。
func (validatingWebhookConfigurationStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	ic := obj.(*admissionregistration.ValidatingWebhookConfiguration)
	errs := validation.ValidateValidatingWebhookConfiguration(ic)
	if utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		errs = append(errs, validation.ValidateStaticSuffix(ic.Name, field.NewPath("metadata", "name"))...)
	}
	return errs
}

// WarningsOnCreate 返回创建 ValidatingWebhookConfiguration 时需要提示的静态配置告警。
func (validatingWebhookConfigurationStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	ic := obj.(*admissionregistration.ValidatingWebhookConfiguration)
	var warnings []string
	if !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
		warnings = append(warnings, validation.WarningsForStaticSuffix(ic.Name)...)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ExcludeAdmissionWebhookVirtualResources) {
		warnings = append(warnings, validation.WarningsForValidatingWebhookRules(ic.Webhooks, exclusion.Excluded())...)
	}
	return warnings
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (validatingWebhookConfigurationStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，禁止通过 PUT 更新请求隐式创建 ValidatingWebhookConfiguration。
func (validatingWebhookConfigurationStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 校验 ValidatingWebhookConfiguration 更新时的新旧 Webhook 配置约束。
// 更新时用新旧配置校验规则变化，并保持静态配置名称约束与对象不可变规则一致。
func (validatingWebhookConfigurationStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newIC := obj.(*admissionregistration.ValidatingWebhookConfiguration)
	oldIC := old.(*admissionregistration.ValidatingWebhookConfiguration)
	errs := validation.ValidateValidatingWebhookConfigurationUpdate(newIC, oldIC)
	return errs
}

// WarningsOnUpdate 返回本次 ValidatingWebhookConfiguration 更新涉及的配置告警。
func (validatingWebhookConfigurationStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newIC := obj.(*admissionregistration.ValidatingWebhookConfiguration)
	var warnings []string
	warnings = append(warnings, validation.WarningsForStaticSuffix(newIC.Name)...)
	if utilfeature.DefaultFeatureGate.Enabled(features.ExcludeAdmissionWebhookVirtualResources) {
		warnings = append(warnings, validation.WarningsForValidatingWebhookRules(newIC.Webhooks, exclusion.Excluded())...)
	}
	return warnings
}

// AllowUnconditionalUpdate 返回 false，要求更新携带匹配的资源版本，避免无条件覆盖并发修改。
// 更新必须基于匹配的资源版本执行。
func (validatingWebhookConfigurationStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}
