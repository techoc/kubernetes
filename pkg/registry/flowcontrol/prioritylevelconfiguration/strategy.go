/*
Copyright 2019 The Kubernetes Authors.

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

// 本文件实现 prioritylevelconfiguration 的 REST Strategy，定义 APF 配置对象的字段隔离、校验和更新规则。
package prioritylevelconfiguration

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	"k8s.io/kubernetes/pkg/apis/flowcontrol/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// priorityLevelConfigurationStrategy 定义 PriorityLevelConfiguration 资源创建、更新和校验时使用的 REST 策略。
type priorityLevelConfigurationStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 PriorityLevelConfiguration 对象时使用的默认策略。
var Strategy = priorityLevelConfigurationStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 PriorityLevelConfiguration 是集群级资源，不属于任何命名空间。
func (priorityLevelConfigurationStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回主资源策略重置的 Status 字段集合，用户不能通过主资源接口修改状态。
func (priorityLevelConfigurationStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"flowcontrol.apiserver.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"flowcontrol.apiserver.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"flowcontrol.apiserver.k8s.io/v1beta3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"flowcontrol.apiserver.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建 PriorityLevelConfiguration 前清空客户端提交的 Status，并将 Generation 初始化为 1。
func (priorityLevelConfigurationStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pl := obj.(*flowcontrol.PriorityLevelConfiguration)
	pl.Status = flowcontrol.PriorityLevelConfigurationStatus{}
	pl.Generation = 1
}

// PrepareForUpdate 在更新时保护 Status，并在 Spec 发生变化时递增 Generation。
func (priorityLevelConfigurationStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPriorityLevelConfiguration := obj.(*flowcontrol.PriorityLevelConfiguration)
	oldPriorityLevelConfiguration := old.(*flowcontrol.PriorityLevelConfiguration)

	// 只有 Spec 变化才递增 Generation，以区分规格更新与仅修改状态的请求。
	if !apiequality.Semantic.DeepEqual(newPriorityLevelConfiguration.Spec, oldPriorityLevelConfiguration.Spec) {
		newPriorityLevelConfiguration.Generation = oldPriorityLevelConfiguration.Generation + 1
	}
	newPriorityLevelConfiguration.Status = oldPriorityLevelConfiguration.Status
}

// Validate 调用 PriorityLevelConfiguration 专用校验逻辑检查新对象，并根据请求 API 版本应用兼容性规则。
func (priorityLevelConfigurationStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	// Kubernetes 1.28 的 API Server 不识别零值往返注解，读取 etcd 中 NominalConcurrencyShares 为 0 的对象时会将其默认回 30。
	// 因此，在确认集群所有 API Server 均升级到 1.29 或更高版本、能够正确保留零值之前，
	// 无论通过 v1 还是带有往返注解的 v1beta3 请求，都不能引入 NominalConcurrencyShares=0。
	plc := obj.(*flowcontrol.PriorityLevelConfiguration)
	opts := validation.PriorityLevelValidationOptions{}
	return validation.ValidatePriorityLevelConfiguration(plc, getRequestGroupVersion(ctx), opts)
}

// WarningsOnCreate 返回创建 PriorityLevelConfiguration 时的非阻断性警告；当前不产生额外警告。
func (priorityLevelConfigurationStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 PriorityLevelConfiguration；当前无需额外处理。
func (priorityLevelConfigurationStrategy) Canonicalize(obj runtime.Object) {
}

// AllowUnconditionalUpdate 返回 true，允许 PriorityLevelConfiguration 更新请求不携带 ResourceVersion，由底层存储执行相应更新。
func (priorityLevelConfigurationStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// AllowCreateOnUpdate 返回 false，PriorityLevelConfiguration 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (priorityLevelConfigurationStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 PriorityLevelConfiguration 更新是否合法，并执行跨版本零值兼容性限制。
func (priorityLevelConfigurationStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPL := obj.(*flowcontrol.PriorityLevelConfiguration)

	// Kubernetes 1.28 的 API Server 不识别零值往返注解，读取 etcd 中 NominalConcurrencyShares 为 0 的对象时会将其默认回 30。
	// 因此，在确认集群所有 API Server 均升级到 1.29 或更高版本、能够正确保留零值之前，
	// 无论通过 v1 还是带有往返注解的 v1beta3 请求，都不能引入 NominalConcurrencyShares=0。
	opts := validation.PriorityLevelValidationOptions{}
	return validation.ValidatePriorityLevelConfiguration(newPL, getRequestGroupVersion(ctx), opts)
}

// WarningsOnUpdate 返回 PriorityLevelConfiguration 更新时的非阻断性警告；当前不产生额外警告。
func (priorityLevelConfigurationStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

type priorityLevelConfigurationStatusStrategy struct {
	priorityLevelConfigurationStrategy
}

// StatusStrategy 定义通过 /status 子资源更新 PriorityLevelConfiguration 状态时使用的策略。
var StatusStrategy = priorityLevelConfigurationStatusStrategy{Strategy}

// GetResetFields 返回 Status 子资源策略重置的 Spec 和 Metadata 字段集合，确保该入口只管理状态。
func (priorityLevelConfigurationStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"flowcontrol.apiserver.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
		"flowcontrol.apiserver.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
		"flowcontrol.apiserver.k8s.io/v1beta3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
		"flowcontrol.apiserver.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
			fieldpath.MakePathOrDie("metadata"),
		),
	}

	return fields
}

// PrepareForUpdate 在 Status 更新入口恢复旧 Spec 和 ObjectMeta，同时保留本次请求生成的 ManagedFields，确保该子资源只改变状态字段。
func (priorityLevelConfigurationStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPriorityLevelConfiguration := obj.(*flowcontrol.PriorityLevelConfiguration)
	oldPriorityLevelConfiguration := old.(*flowcontrol.PriorityLevelConfiguration)

	// 保留本次状态更新过程中重新计算的 ManagedFields，以记录字段管理器对 Status 字段的修改。
	managedFields := newPriorityLevelConfiguration.ManagedFields
	newPriorityLevelConfiguration.ObjectMeta = oldPriorityLevelConfiguration.ObjectMeta
	newPriorityLevelConfiguration.ManagedFields = managedFields
	newPriorityLevelConfiguration.Spec = oldPriorityLevelConfiguration.Spec
}

// ValidateUpdate 结合新旧对象校验更新合法性，保护不可变字段并检查引用关系及配置变更约束。
func (priorityLevelConfigurationStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidatePriorityLevelConfigurationStatusUpdate(old.(*flowcontrol.PriorityLevelConfiguration), obj.(*flowcontrol.PriorityLevelConfiguration))
}

// WarningsOnUpdate 返回 PriorityLevelConfiguration Status 更新时的非阻断性警告；当前不产生额外警告。
func (priorityLevelConfigurationStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// getRequestGroupVersion 从请求上下文中解析客户端使用的 API GroupVersion，供版本相关校验和行为选择。
func getRequestGroupVersion(ctx context.Context) schema.GroupVersion {
	if requestInfo, exists := genericapirequest.RequestInfoFrom(ctx); exists {
		return schema.GroupVersion{Group: requestInfo.APIGroup, Version: requestInfo.APIVersion}
	}
	return schema.GroupVersion{}
}
