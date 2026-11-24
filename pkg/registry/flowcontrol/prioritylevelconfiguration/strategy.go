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

// priorityLevelConfigurationStrategy implements verification logic for priority level configurations.
type priorityLevelConfigurationStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy is the default logic that applies when creating and updating priority level configuration objects.
var Strategy = priorityLevelConfigurationStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped returns false because all PriorityClasses are global.
func (priorityLevelConfigurationStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
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

// PrepareForCreate clears the status of a priority-level-configuration before creation.
func (priorityLevelConfigurationStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	pl := obj.(*flowcontrol.PriorityLevelConfiguration)
	pl.Status = flowcontrol.PriorityLevelConfigurationStatus{}
	pl.Generation = 1
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
func (priorityLevelConfigurationStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPriorityLevelConfiguration := obj.(*flowcontrol.PriorityLevelConfiguration)
	oldPriorityLevelConfiguration := old.(*flowcontrol.PriorityLevelConfiguration)

	// Spec updates bump the generation so that we can distinguish between status updates.
	if !apiequality.Semantic.DeepEqual(newPriorityLevelConfiguration.Spec, oldPriorityLevelConfiguration.Spec) {
		newPriorityLevelConfiguration.Generation = oldPriorityLevelConfiguration.Generation + 1
	}
	newPriorityLevelConfiguration.Status = oldPriorityLevelConfiguration.Status
}

// Validate validates a new priority-level.
func (priorityLevelConfigurationStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	// 1.28 server is not aware of the roundtrip annotation, and will
	// default any 0 value persisted (for the NominalConcurrencyShares
	// field of a priority level configuration object) back to 30 when
	// reading from etcd.
	// That means we should not allow 0 values to be introduced, either
	// via v1 or v1beta3(with the roundtrip annotation) until we know
	// all servers are at 1.29+ and will honor the zero value correctly.
	plc := obj.(*flowcontrol.PriorityLevelConfiguration)
	opts := validation.PriorityLevelValidationOptions{}
	return validation.ValidatePriorityLevelConfiguration(plc, getRequestGroupVersion(ctx), opts)
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (priorityLevelConfigurationStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
func (priorityLevelConfigurationStrategy) Canonicalize(obj runtime.Object) {
}

// AllowUnconditionalUpdate 决定是否允许不携带 ResourceVersion 的无条件更新，以控制并发写入语义。
func (priorityLevelConfigurationStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// AllowCreateOnUpdate is false for priority-level-configurations; this means a POST is needed to create one.
func (priorityLevelConfigurationStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate is the default update validation for an end user.
func (priorityLevelConfigurationStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newPL := obj.(*flowcontrol.PriorityLevelConfiguration)

	// 1.28 server is not aware of the roundtrip annotation, and will
	// default any 0 value persisted (for the NominalConcurrencyShares
	// field of a priority level configuration object) back to 30 when
	// reading from etcd.
	// That means we should not allow 0 values to be introduced, either
	// via v1 or v1beta3(with the roundtrip annotation) until we know
	// all servers are at 1.29+ and will honor the zero value correctly.
	opts := validation.PriorityLevelValidationOptions{}
	return validation.ValidatePriorityLevelConfiguration(newPL, getRequestGroupVersion(ctx), opts)
}

// WarningsOnUpdate returns warnings for the given update.
func (priorityLevelConfigurationStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

type priorityLevelConfigurationStatusStrategy struct {
	priorityLevelConfigurationStrategy
}

// StatusStrategy is the default logic that applies when updating priority level configuration objects' status.
var StatusStrategy = priorityLevelConfigurationStatusStrategy{Strategy}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
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

// PrepareForUpdate Status 更新入口恢复旧 Spec，并重置受保护的 metadata，确保 status 子资源只改变状态字段。
func (priorityLevelConfigurationStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newPriorityLevelConfiguration := obj.(*flowcontrol.PriorityLevelConfiguration)
	oldPriorityLevelConfiguration := old.(*flowcontrol.PriorityLevelConfiguration)

	// managedFields must be preserved since it's been modified to
	// track changed fields in the status update.
	managedFields := newPriorityLevelConfiguration.ManagedFields
	newPriorityLevelConfiguration.ObjectMeta = oldPriorityLevelConfiguration.ObjectMeta
	newPriorityLevelConfiguration.ManagedFields = managedFields
	newPriorityLevelConfiguration.Spec = oldPriorityLevelConfiguration.Spec
}

// ValidateUpdate 结合新旧对象校验更新合法性，保护不可变字段并检查引用关系及配置变更约束。
func (priorityLevelConfigurationStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidatePriorityLevelConfigurationStatusUpdate(old.(*flowcontrol.PriorityLevelConfiguration), obj.(*flowcontrol.PriorityLevelConfiguration))
}

// WarningsOnUpdate returns warnings for the given update.
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
