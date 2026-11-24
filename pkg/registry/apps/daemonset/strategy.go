/*
Copyright 2015 The Kubernetes Authors.

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

package daemonset

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/apps/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// daemonSetStrategy 定义 DaemonSet 主资源策略，重点管理 Pod 模板版本 TemplateGeneration 与对象 Generation。
type daemonSetStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是通过 REST API 创建和更新 DaemonSet 时使用的默认策略。
var Strategy = daemonSetStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 编译期检查：确保 Strategy 实现了垃圾回收删除策略接口。
var _ = rest.GarbageCollectionDeleteStrategy(Strategy)

// DefaultGarbageCollectionPolicy 返回 DeleteDependents：删除 DaemonSet 时级联删除其依赖对象。
func (daemonSetStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	return rest.DeleteDependents
}

// NamespaceScoped 表示 DaemonSet 必须属于某个命名空间。
func (daemonSetStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Status 子资源更新时需要重置、不能由请求修改的字段集合。
func (daemonSetStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 DaemonSet 前清空 Status，并初始化服务端维护的版本字段。
// 创建时清空 Status、初始化 Generation/TemplateGeneration，并清理 PodTemplate 中被关闭的字段。
func (daemonSetStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	daemonSet := obj.(*apps.DaemonSet)
	daemonSet.Status = apps.DaemonSetStatus{}

	daemonSet.Generation = 1
	if daemonSet.Spec.TemplateGeneration < 1 {
		daemonSet.Spec.TemplateGeneration = 1
	}

	pod.DropDisabledTemplateFields(&daemonSet.Spec.Template, nil)
}

// PrepareForUpdate 恢复不允许用户直接修改的字段，并根据 Spec/模板变化维护 Generation。
// 更新时恢复 Status 和服务端维护的 TemplateGeneration；模板变化时两种 Generation 都递增，其他 Spec 变化只递增对象 Generation。
func (daemonSetStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newDaemonSet := obj.(*apps.DaemonSet)
	oldDaemonSet := old.(*apps.DaemonSet)

	pod.DropDisabledTemplateFields(&newDaemonSet.Spec.Template, &oldDaemonSet.Spec.Template)

	// 主资源更新请求不能修改 Status。
	newDaemonSet.Status = oldDaemonSet.Status

	// TemplateGeneration 由 API Server 维护，不能由客户端直接指定。
	newDaemonSet.Spec.TemplateGeneration = oldDaemonSet.Spec.TemplateGeneration

	// Spec 发生变化时递增 Generation；Status 写入者应报告其观察到的 Generation。

	//
	// TODO：对象中其他代表期望状态的字段（如 Labels、Annotations）变化时，也应考虑递增 Generation。

	// PodTemplateGeneration 专门标记模板版本，DaemonSet Controller 可据此判断是否需要触发新一轮 Pod 替换。
	if !apiequality.Semantic.DeepEqual(oldDaemonSet.Spec.Template, newDaemonSet.Spec.Template) {
		newDaemonSet.Spec.TemplateGeneration = oldDaemonSet.Spec.TemplateGeneration + 1
		newDaemonSet.Generation = oldDaemonSet.Generation + 1
		return
	}
	if !apiequality.Semantic.DeepEqual(oldDaemonSet.Spec, newDaemonSet.Spec) {
		newDaemonSet.Generation = oldDaemonSet.Generation + 1
	}
}

// Validate 校验新创建的 DaemonSet。
func (daemonSetStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	daemonSet := obj.(*apps.DaemonSet)
	opts := pod.GetValidationOptionsFromPodTemplate(&daemonSet.Spec.Template, nil)
	return validation.ValidateDaemonSet(daemonSet, opts)
}

// WarningsOnCreate 返回创建 DaemonSet 时发现的非阻断性警告。
func (daemonSetStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newDaemonSet := obj.(*apps.DaemonSet)
	return pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newDaemonSet.Spec.Template, nil)
}

// Canonicalize 在校验后规范化对象；当前策略无需额外处理。
func (daemonSetStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：创建 DaemonSet 必须使用 POST，不能通过更新请求隐式创建。
func (daemonSetStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 执行面向普通用户的 DaemonSet 更新校验。
// 更新校验允许兼容旧对象中历史上无效的 Selector Label 值，但仍校验本次更新规则。
func (daemonSetStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newDaemonSet := obj.(*apps.DaemonSet)
	oldDaemonSet := old.(*apps.DaemonSet)

	opts := pod.GetValidationOptionsFromPodTemplate(&newDaemonSet.Spec.Template, &oldDaemonSet.Spec.Template)
	opts.AllowInvalidLabelValueInSelector = opts.AllowInvalidLabelValueInSelector || metav1validation.LabelSelectorHasInvalidLabelValue(oldDaemonSet.Spec.Selector)

	return validation.ValidateDaemonSetUpdate(newDaemonSet, oldDaemonSet, opts)
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (daemonSetStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newDaemonSet := obj.(*apps.DaemonSet)
	oldDaemonSet := old.(*apps.DaemonSet)
	if newDaemonSet.Spec.TemplateGeneration != oldDaemonSet.Spec.TemplateGeneration {
		warnings = pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newDaemonSet.Spec.Template, &oldDaemonSet.Spec.Template)
	}
	return warnings
}

// AllowUnconditionalUpdate 表示 DaemonSet 允许不携带 resourceVersion 的无条件更新。
func (daemonSetStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type daemonSetStatusStrategy struct {
	daemonSetStrategy
}

// StatusStrategy 专门处理 daemonsets/status；恢复完整 Spec，防止状态写入覆盖期望配置。
var StatusStrategy = daemonSetStatusStrategy{Strategy}

// GetResetFields 返回该策略会重置的字段集合，这些字段不允许用户通过当前子资源请求修改。
func (daemonSetStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}
}

// 状态子资源只接受 Status 变化，Spec 始终从旧对象恢复。
func (daemonSetStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newDaemonSet := obj.(*apps.DaemonSet)
	oldDaemonSet := old.(*apps.DaemonSet)
	newDaemonSet.Spec = oldDaemonSet.Spec
}

// ValidateUpdate 校验 DaemonSet Status 子资源的更新。
func (daemonSetStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateDaemonSetStatusUpdate(obj.(*apps.DaemonSet), old.(*apps.DaemonSet))
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (daemonSetStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
