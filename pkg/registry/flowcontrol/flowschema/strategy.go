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

// 本文件实现 flowschema 的 REST Strategy，定义 APF 配置对象的字段隔离、校验和更新规则。
package flowschema

import (
	"context"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/flowcontrol"
	"k8s.io/kubernetes/pkg/apis/flowcontrol/validation"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// flowSchemaStrategy 定义 FlowSchema 资源创建、更新和校验时使用的 REST 策略。
type flowSchemaStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 FlowSchema 对象时使用的默认策略。
var Strategy = flowSchemaStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 FlowSchema 是集群级资源，不属于任何命名空间。
func (flowSchemaStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回主资源策略重置的 Status 字段集合，用户不能通过主资源接口修改状态。
func (flowSchemaStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
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

// PrepareForCreate 在创建 FlowSchema 前清空客户端提交的 Status，并将 Generation 初始化为 1。
func (flowSchemaStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	fl := obj.(*flowcontrol.FlowSchema)
	fl.Status = flowcontrol.FlowSchemaStatus{}
	fl.Generation = 1
}

// PrepareForUpdate 在更新时保护 Status，并在 Spec 发生变化时递增 Generation。
func (flowSchemaStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newFlowSchema := obj.(*flowcontrol.FlowSchema)
	oldFlowSchema := old.(*flowcontrol.FlowSchema)

	// 只有 Spec 变化才递增 Generation，以区分规格更新与仅修改状态的请求。
	if !apiequality.Semantic.DeepEqual(newFlowSchema.Spec, oldFlowSchema.Spec) {
		newFlowSchema.Generation = oldFlowSchema.Generation + 1
	}
	newFlowSchema.Status = oldFlowSchema.Status
}

// Validate 调用 FlowSchema 专用校验逻辑检查新对象。
func (flowSchemaStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateFlowSchema(obj.(*flowcontrol.FlowSchema))
}

// WarningsOnCreate 返回创建 FlowSchema 时的非阻断性警告；当前不产生额外警告。
func (flowSchemaStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化 FlowSchema；当前无需额外处理。
func (flowSchemaStrategy) Canonicalize(obj runtime.Object) {
}

// AllowUnconditionalUpdate 返回 true，允许 FlowSchema 更新请求不携带 ResourceVersion，由底层存储执行相应更新。
func (flowSchemaStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// AllowCreateOnUpdate 返回 false，FlowSchema 必须通过 POST 创建，不能由 PUT 更新请求隐式创建。
func (flowSchemaStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 FlowSchema 更新是否合法。
func (flowSchemaStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateFlowSchemaUpdate(old.(*flowcontrol.FlowSchema), obj.(*flowcontrol.FlowSchema))
}

// WarningsOnUpdate 返回更新 FlowSchema 时的非阻断性警告；当前不产生额外警告。
func (flowSchemaStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

type flowSchemaStatusStrategy struct {
	flowSchemaStrategy
}

// StatusStrategy 定义通过 /status 子资源更新 FlowSchema 状态时使用的策略。
var StatusStrategy = flowSchemaStatusStrategy{Strategy}

// GetResetFields 返回 Status 子资源策略重置的 Metadata 和 Spec 字段集合，确保该入口只管理状态。
func (flowSchemaStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"flowcontrol.apiserver.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"flowcontrol.apiserver.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"flowcontrol.apiserver.k8s.io/v1beta3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"flowcontrol.apiserver.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 在 Status 更新入口恢复旧 Spec 和 ObjectMeta，同时保留本次请求生成的 ManagedFields，确保该子资源只改变状态字段。
func (flowSchemaStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newFlowSchema := obj.(*flowcontrol.FlowSchema)
	oldFlowSchema := old.(*flowcontrol.FlowSchema)

	// 保留本次状态更新过程中重新计算的 ManagedFields，以记录字段管理器对 Status 字段的修改。
	managedFields := newFlowSchema.ManagedFields
	newFlowSchema.ObjectMeta = oldFlowSchema.ObjectMeta
	newFlowSchema.ManagedFields = managedFields
	newFlowSchema.Spec = oldFlowSchema.Spec
}

// ValidateUpdate 结合新旧对象校验更新合法性，保护不可变字段并检查引用关系及配置变更约束。
func (flowSchemaStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateFlowSchemaStatusUpdate(old.(*flowcontrol.FlowSchema), obj.(*flowcontrol.FlowSchema))
}

// WarningsOnUpdate 返回 FlowSchema Status 更新时的非阻断性警告；当前不产生额外警告。
func (flowSchemaStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
