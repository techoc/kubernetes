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

package podtemplate

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	corevalidation "k8s.io/kubernetes/pkg/apis/core/validation"
)

// podTemplateStrategy 定义 PodTemplate 主资源的 REST 策略，负责模板字段清理、模板校验和 Generation 维护。
type podTemplateStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating PodTemplate
// objects via the REST API.
var Strategy = podTemplateStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped is true for pod templates.
func (podTemplateStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate clears fields that are not allowed to be set by end users on creation.
// 创建时将 Generation 初始化为 1，并根据 Feature Gate 清理当前集群不支持的 PodSpec 字段。
func (podTemplateStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	template := obj.(*api.PodTemplate)
	template.Generation = 1
	pod.DropDisabledTemplateFields(&template.Template, nil)
}

// Validate validates a new pod template.
// Validate 以 PodTemplate.Template 为校验主体，复用 Pod 模板专用的默认值和特性兼容选项。
func (podTemplateStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	template := obj.(*api.PodTemplate)
	opts := pod.GetValidationOptionsFromPodTemplate(&template.Template, nil)
	return corevalidation.ValidatePodTemplate(template, opts)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (podTemplateStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newPodTemplate := obj.(*api.PodTemplate)
	return pod.GetWarningsForPodTemplate(ctx, field.NewPath("template"), &newPodTemplate.Template, nil)
}

// Canonicalize normalizes the object after validation.
func (podTemplateStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate is false for pod templates.
func (podTemplateStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
// 更新时先清理不可用模板字段；只有内嵌 PodTemplate 语义变化时才增加对象 Generation。
func (podTemplateStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newTemplate := obj.(*api.PodTemplate)
	oldTemplate := old.(*api.PodTemplate)

	pod.DropDisabledTemplateFields(&newTemplate.Template, &oldTemplate.Template)

	// Any changes to the template increment the generation number.
	// See metav1.ObjectMeta description for more information on Generation.
	if !apiequality.Semantic.DeepEqual(newTemplate.Template, oldTemplate.Template) {
		newTemplate.Generation = oldTemplate.Generation + 1
	}

}

// ValidateUpdate is the default update validation for an end user.
// ValidateUpdate 将新旧 PodTemplate 一起交给验证器，以支持依赖旧字段值的向后兼容更新规则。
func (podTemplateStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	template := obj.(*api.PodTemplate)
	oldTemplate := old.(*api.PodTemplate)

	// Allow downward api usage of hugepages on pod update if feature is enabled or if the old pod already had used them.
	opts := pod.GetValidationOptionsFromPodTemplate(&template.Template, &oldTemplate.Template)
	return corevalidation.ValidatePodTemplateUpdate(template, oldTemplate, opts)
}

// WarningsOnUpdate returns warnings for the given update.
// 只有模板发生变化时才计算 Pod 模板告警，避免单纯修改 metadata 时重复评估容器配置。
func (podTemplateStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newTemplate := obj.(*api.PodTemplate)
	oldTemplate := old.(*api.PodTemplate)
	if newTemplate.Generation != oldTemplate.Generation {
		warnings = pod.GetWarningsForPodTemplate(ctx, field.NewPath("template"), &newTemplate.Template, &oldTemplate.Template)
	}
	return warnings
}

func (podTemplateStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}
