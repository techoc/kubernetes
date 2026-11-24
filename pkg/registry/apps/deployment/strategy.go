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

package deployment

import (
	"context"
	"fmt"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/apis/apps"
	appsvalidation "k8s.io/kubernetes/pkg/apis/apps/validation"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// deploymentStrategy 定义 Deployment 主资源的写入策略，负责状态隔离、Generation、Pod 模板兼容和校验。
type deploymentStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是 Deployment 主资源的默认 REST 策略；删除时使用级联删除策略，清理其 ReplicaSet/Pod 依赖。
// Strategy 是通过 REST API 创建和更新 Deployment 时使用的默认策略。
var Strategy = deploymentStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// 编译期检查：确保 Strategy 实现 REST 创建策略接口。
var _ = rest.GarbageCollectionDeleteStrategy(Strategy)

// DefaultGarbageCollectionPolicy 对所有当前提供服务的 API 版本返回 DeleteDependents。
// 删除 Deployment 时默认级联删除其控制器依赖对象，避免遗留 ReplicaSet。
func (deploymentStrategy) DefaultGarbageCollectionPolicy(ctx context.Context) rest.GarbageCollectionPolicy {
	return rest.DeleteDependents
}

// NamespaceScoped 表示 Deployment 是命名空间级资源。
func (deploymentStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 返回 Status 子资源更新时需要重置、因而不能由请求修改的字段。
func (deploymentStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清理创建时不允许终端用户设置的字段。
// 创建时由 API Server 初始化 Status 和 Generation，再按 Feature Gate 清理 PodTemplate 中不可用字段。
func (deploymentStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	deployment := obj.(*apps.Deployment)
	deployment.Status = apps.DeploymentStatus{}
	deployment.Generation = 1

	pod.DropDisabledTemplateFields(&deployment.Spec.Template, nil)
}

// Validate 校验新创建的 Deployment。
func (deploymentStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	deployment := obj.(*apps.Deployment)
	opts := pod.GetValidationOptionsFromPodTemplate(&deployment.Spec.Template, nil)
	return appsvalidation.ValidateDeployment(deployment, opts)
}

// WarningsOnCreate 返回创建 Deployment 时发现的非阻断性警告。
func (deploymentStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newDeployment := obj.(*apps.Deployment)
	var warnings []string
	if msgs := utilvalidation.IsDNS1123Label(newDeployment.Name); len(msgs) != 0 {
		warnings = append(warnings, fmt.Sprintf("metadata.name: this is used in Pod names and hostnames, which can result in surprising behavior; a DNS label is recommended: %v", msgs))
	}
	warnings = append(warnings, pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newDeployment.Spec.Template, nil)...)
	return warnings
}

// Canonicalize 在校验后对对象进行规范化；当前策略无需额外处理。
func (deploymentStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false：Deployment 不允许通过更新请求隐式创建。
func (deploymentStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 清理更新时不允许终端用户设置的字段。
// 更新时先恢复服务端维护的 Status，再清理禁用字段；Spec 或 Annotations 变化都会递增 Generation。
func (deploymentStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newDeployment := obj.(*apps.Deployment)
	oldDeployment := old.(*apps.Deployment)
	newDeployment.Status = oldDeployment.Status

	pod.DropDisabledTemplateFields(&newDeployment.Spec.Template, &oldDeployment.Spec.Template)

	// Generation 用于区分期望状态版本：Spec 变化代表控制器需要观察新的目标状态；Annotations 也会复制到 ReplicaSet，因此同样递增。
	// Spec 更新会递增 Generation，以便区分扩缩容事件和 Pod 模板变更；Annotations 也会递增 Generation，
	// 因为 Deployment 的注解会复制到其 ReplicaSet。
	if !apiequality.Semantic.DeepEqual(newDeployment.Spec, oldDeployment.Spec) ||
		!apiequality.Semantic.DeepEqual(newDeployment.Annotations, oldDeployment.Annotations) {
		newDeployment.Generation = oldDeployment.Generation + 1
	}
}

// ValidateUpdate 执行面向普通用户的 Deployment 更新校验。
// ValidateUpdate 同时校验新对象与新旧 PodTemplate，处理不可变字段和模板兼容规则。
func (deploymentStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newDeployment := obj.(*apps.Deployment)
	oldDeployment := old.(*apps.Deployment)

	opts := pod.GetValidationOptionsFromPodTemplate(&newDeployment.Spec.Template, &oldDeployment.Spec.Template)
	allErrs := appsvalidation.ValidateDeploymentUpdate(newDeployment, oldDeployment, opts)

	return allErrs
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (deploymentStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	var warnings []string
	newDeployment := obj.(*apps.Deployment)
	oldDeployment := old.(*apps.Deployment)
	if newDeployment.Generation != oldDeployment.Generation {
		warnings = pod.GetWarningsForPodTemplate(ctx, field.NewPath("spec", "template"), &newDeployment.Spec.Template, &oldDeployment.Spec.Template)
	}
	return warnings
}

func (deploymentStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type deploymentStatusStrategy struct {
	deploymentStrategy
}

// StatusStrategy 专门处理 deployments/status；只允许修改状态，不能借此改动 Spec 或 Labels。
var StatusStrategy = deploymentStatusStrategy{Strategy}

// GetResetFields 返回该策略会重置的字段集合，这些字段不允许用户通过当前子资源请求修改。
func (deploymentStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return map[fieldpath.APIVersion]*fieldpath.Set{
		"apps/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata", "labels"),
		),
	}
}

// PrepareForUpdate 清理 Status 更新请求中不允许终端用户设置的字段。
// 状态更新恢复旧 Spec/Labels，并清理当前 Feature Gate 不允许且旧对象也未使用的状态字段。
func (deploymentStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newDeployment := obj.(*apps.Deployment)
	oldDeployment := old.(*apps.Deployment)
	newDeployment.Spec = oldDeployment.Spec
	newDeployment.Labels = oldDeployment.Labels
	dropDisabledStatusFields(&newDeployment.Status, &oldDeployment.Status)
}

// ValidateUpdate 校验 Deployment Status 子资源的更新。
func (deploymentStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return appsvalidation.ValidateDeploymentStatusUpdate(obj.(*apps.Deployment), old.(*apps.Deployment))
}

// WarningsOnUpdate 返回本次更新产生的非阻断性警告。
func (deploymentStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// dropDisabledStatusFields 从 Deployment Status 中清除当前未启用特性对应的字段。
// 关闭 TerminatingReplicas 特性时，仅当旧状态未使用该字段才清除，保证已有对象升级期间兼容。
func dropDisabledStatusFields(deploymentStatus, oldDeploymentStatus *apps.DeploymentStatus) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) &&
		(oldDeploymentStatus == nil || oldDeploymentStatus.TerminatingReplicas == nil) {
		deploymentStatus.TerminatingReplicas = nil
	}
}
