/*
Copyright 2022 The Kubernetes Authors.

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

package resourceclaim

import (
	"context"
	"errors"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/api/resourceclaimspec"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/resource/validation"
	"k8s.io/kubernetes/pkg/features"
	resourceutils "k8s.io/kubernetes/pkg/registry/resource"
	"k8s.io/utils/ptr"
)

// resourceclaimStrategy 定义 ResourceClaim 的创建、更新、校验和 Status 子资源策略。
type resourceclaimStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
	nsClient   v1.NamespaceInterface
	authorizer authorizer.UnconditionalAuthorizer
}

// NewStrategy 创建 ResourceClaim 默认策略，并注入 Namespace 客户端和授权器。
func NewStrategy(nsClient v1.NamespaceInterface, authorizer authorizer.UnconditionalAuthorizer) *resourceclaimStrategy {
	return &resourceclaimStrategy{
		rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		names.SimpleNameGenerator,
		nsClient,
		authorizer,
	}
}

// NamespaceScoped 返回 true，因为 ResourceClaim 属于命名空间。
func (*resourceclaimStrategy) NamespaceScoped() bool {
	return true
}

// GetResetFields 声明主资源写入时需要重置的 Status 字段，避免客户端绕过控制器写入资源分配结果。
func (*resourceclaimStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"resource.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"resource.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"resource.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"resource.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 清空客户端提交的 Status，并根据 Feature Gate 清理 Spec 中不可用的新字段。
func (*resourceclaimStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	claim := obj.(*resource.ResourceClaim)
	// 创建请求不能设置 Status；资源分配状态由调度器和驱动控制器维护。
	claim.Status = resource.ResourceClaimStatus{}

	dropDisabledSpecFields(claim, nil)
}

// Validate 先检查请求是否具备 AdminAccess 权限，再执行 ResourceClaim 字段校验。
func (s *resourceclaimStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	claim := obj.(*resource.ResourceClaim)

	allErrs := resourceutils.AuthorizedForAdmin(ctx, claim.Spec.Devices.Requests, claim.Namespace, s.nsClient)
	allErrs = append(allErrs, validation.ValidateResourceClaim(claim)...)
	return allErrs
}

// DeclarativeValidationConfig 为通用 BeforeCreate/BeforeUpdate 流程提供声明式校验配置。
// TODO：接入统一声明式校验后，Status 子策略通过嵌入 resourceclaimStrategy 继承了本方法，
// 因此 Status 更新也会应用 ResourceNormalizationRules。迁移前，Status 子资源的内联声明式校验
// 并未传入这些规范化规则。当前变化会使 Status 更新的声明式与手写校验差异比较更加严格，
// 需要继续评估；如果 Status 子资源不应使用这些规则，应在 resourceclaimStatusStrategy 中
// 单独覆盖 ValidationConfig。
func (*resourceclaimStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{NormalizationRules: validation.ResourceNormalizationRules}
}

func (*resourceclaimStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (*resourceclaimStrategy) Canonicalize(obj runtime.Object) {
}

func (*resourceclaimStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 恢复旧 Status，并按旧对象保留已使用的特性字段。
func (*resourceclaimStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClaim := obj.(*resource.ResourceClaim)
	oldClaim := old.(*resource.ResourceClaim)
	newClaim.Status = oldClaim.Status

	dropDisabledSpecFields(newClaim, oldClaim)
}

// ValidateUpdate 校验 ResourceClaim 更新；Spec 不可变，因此无需重新执行 AdminAccess 授权检查。
func (s *resourceclaimStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newClaim := obj.(*resource.ResourceClaim)
	oldClaim := old.(*resource.ResourceClaim)
	// Spec 创建后不可变，因此更新时无需再次执行 AuthorizedForAdmin 检查。
	return validation.ValidateResourceClaimUpdate(newClaim, oldClaim)
}

func (*resourceclaimStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// AllowUnconditionalUpdate 返回 true，允许通用更新流程为缺少 ResourceVersion 的请求填入最新版本。
func (*resourceclaimStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type resourceclaimStatusStrategy struct {
	*resourceclaimStrategy
}

// NewStatusStrategy 基于主资源策略创建独立的 ResourceClaim /status 子资源策略。
func NewStatusStrategy(resourceclaimStrategy *resourceclaimStrategy) *resourceclaimStatusStrategy {
	return &resourceclaimStatusStrategy{resourceclaimStrategy}
}

// GetResetFields 声明 Status 子资源写入时需要重置的 Spec 和 metadata，确保该入口只能修改 Status。
func (*resourceclaimStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"resource.k8s.io/v1alpha3": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"resource.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"resource.k8s.io/v1beta2": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"resource.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 恢复旧 Spec 和受保护的 metadata，再清理禁用特性字段及已解除分配的设备状态。
func (*resourceclaimStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newClaim := obj.(*resource.ResourceClaim)
	oldClaim := old.(*resource.ResourceClaim)
	newClaim.Spec = oldClaim.Spec
	metav1.ResetObjectMetaForStatus(&newClaim.ObjectMeta, &oldClaim.ObjectMeta)

	dropDisabledStatusFields(newClaim, oldClaim)
	dropDeallocatedStatusDevices(newClaim, oldClaim) // NOP if fields got dropped, so do this last.
}

// ValidateUpdate 根据 DRAResourceClaimGranularStatusAuthorization 特性门控，分别校验 AdminAccess、绑定和驱动设备状态权限。
func (r *resourceclaimStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	newClaim := obj.(*resource.ResourceClaim)
	oldClaim := old.(*resource.ResourceClaim)
	var newAllocationResult, oldAllocationResult []resource.DeviceRequestAllocationResult
	if newClaim.Status.Allocation != nil {
		newAllocationResult = newClaim.Status.Allocation.Devices.Results
	}
	if oldClaim.Status.Allocation != nil {
		oldAllocationResult = oldClaim.Status.Allocation.Devices.Results
	}
	errs := resourceutils.AuthorizedForAdminStatus(ctx, newAllocationResult, oldAllocationResult, newClaim.Namespace, r.nsClient)

	if utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimGranularStatusAuthorization) {
		errs = append(errs, resourceutils.AuthorizedForBinding(ctx, field.NewPath("status", "allocation"), r.authorizer, newClaim.Status, oldClaim.Status)...)

		// 只有 ResourceClaim 已完成分配时才检查驱动设备状态的授权；解除分配时会清除全部设备状态。
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus) &&
			newClaim.Status.Allocation != nil {
			errs = append(errs, resourceutils.AuthorizedForDeviceStatus(ctx, field.NewPath("status", "devices"), r.authorizer, newClaim.Status, oldClaim.Status)...)
		}
	}
	errs = append(errs, validation.ValidateResourceClaimStatusUpdate(newClaim, oldClaim)...)
	return errs
}

// WarningsOnUpdate 返回 ResourceClaim Status 更新时的非阻断性警告；当前不产生额外警告。
func (*resourceclaimStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// Match 根据标签和字段选择器构造通用存储匹配器。
func Match(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// GetAttrs 提取 ResourceClaim 的标签和字段，供 List、Watch 选择器过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	claim, ok := obj.(*resource.ResourceClaim)
	if !ok {
		return nil, nil, errors.New("not a resourceclaim")
	}
	return labels.Set(claim.Labels), toSelectableFields(claim), nil
}

// toSelectableFields 将 ResourceClaim 转换为可供字段选择器匹配的字段集合。
func toSelectableFields(claim *resource.ResourceClaim) fields.Set {
	fields := generic.ObjectMetaFieldsSet(&claim.ObjectMeta, true)
	return fields
}

// dropDisabledSpecFields 清理 Spec 中由 Feature Gate 控制、当前不允许新设置的字段。
func dropDisabledSpecFields(newClaim, oldClaim *resource.ResourceClaim) {
	var oldClaimSpec *resource.ResourceClaimSpec
	if oldClaim != nil {
		oldClaimSpec = &oldClaim.Spec
	}
	resourceclaimspec.DropDisabledFields(&newClaim.Spec, oldClaimSpec)
}

// dropDisabledStatusFields 清理 Status 中由 Feature Gate 控制、当前不允许新设置的字段。
func dropDisabledStatusFields(newClaim, oldClaim *resource.ResourceClaim) {
	dropDisabledDRAResourceClaimDeviceStatusFields(newClaim, oldClaim)
	dropDisabledDRAAdminAccessStatusFields(newClaim, oldClaim)
	dropDisabledDRAResourceClaimConsumableCapacityStatusFields(newClaim, oldClaim)
	dropDeviceBindingConditionsFields(newClaim, oldClaim)
	dropDisabledDRAOptionalNodeOperationsStatusFields(newClaim, oldClaim)
}

func dropDisabledDRAAdminAccessStatusFields(newClaim, oldClaim *resource.ResourceClaim) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAAdminAccess) {
		// 特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}
	if draAdminAccessFeatureInUse(oldClaim) {
		// 如果旧对象曾经设置过 AdminAccess，即使本次更新与该字段无关，也不能清除它。
		// 否则在功能关闭期间，客户端仍可能通过无关更新新增 AdminAccess=true；
		// 调度器通常会检查特性门控并拒绝调度，但 API Server 本身仍可能接受该状态。
		return
	}

	if newClaim.Status.Allocation == nil {
		return
	}

	for i := range newClaim.Status.Allocation.Devices.Results {
		newClaim.Status.Allocation.Devices.Results[i].AdminAccess = nil
	}
}

// draAdminAccessFeatureInUse 检查旧 Spec 或 Allocation 是否已经使用 AdminAccess，决定降级时是否保留字段。
func draAdminAccessFeatureInUse(claim *resource.ResourceClaim) bool {
	if claim == nil {
		return false
	}

	if resourceclaimspec.DRAAdminAccessFeatureInUse(&claim.Spec) {
		return true
	}

	if allocation := claim.Status.Allocation; allocation != nil {
		for _, result := range allocation.Devices.Results {
			if result.AdminAccess != nil {
				return true
			}
		}
	}

	return false
}

// isDRAResourceClaimDeviceStatusInUse 判断旧 ResourceClaim 是否已经包含设备状态。
func isDRAResourceClaimDeviceStatusInUse(claim *resource.ResourceClaim) bool {
	return claim != nil && len(claim.Status.Devices) > 0
}

// dropDisabledDRAResourceClaimDeviceStatusFields 在特性关闭且旧对象未使用时清空 Status.Devices。
func dropDisabledDRAResourceClaimDeviceStatusFields(newClaim, oldClaim *resource.ResourceClaim) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus) && !isDRAResourceClaimDeviceStatusInUse(oldClaim) {
		newClaim.Status.Devices = nil
	}
}

// dropDeallocatedStatusDevices 移除旧 ResourceClaim 中已分配、但新对象中已经解除分配的设备状态。
// 换言之，它会清理解除分配后残留的 Status.Devices 条目；由 API Server 执行该清理，
// 可以避免要求尚不了解设备状态字段的旧客户端同步升级。
func dropDeallocatedStatusDevices(newClaim, oldClaim *resource.ResourceClaim) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus) && !isDRAResourceClaimDeviceStatusInUse(oldClaim) {
		return
	}

	deallocatedDevices := sets.New[structured.SharedDeviceID]()

	if oldClaim.Status.Allocation != nil {
		// 收集旧 ResourceClaim 分配结果中的全部设备标识。
		for _, result := range oldClaim.Status.Allocation.Devices.Results {
			deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)
			sharedDeviceID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
			deallocatedDevices.Insert(sharedDeviceID)
		}
	}

	// 从待清理集合中移除仍然存在于新分配结果中的设备。
	if newClaim.Status.Allocation != nil {
		for _, result := range newClaim.Status.Allocation.Devices.Results {
			deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)
			sharedDeviceID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
			deallocatedDevices.Delete(sharedDeviceID)
		}
	}

	// 从新对象的 Status.Devices 中移除已经解除分配的设备。
	n := 0
	for _, device := range newClaim.Status.Devices {
		deviceID := structured.MakeDeviceID(device.Driver, device.Pool, device.Device)
		var shareID *types.UID
		if device.ShareID != nil {
			shareID = ptr.To(types.UID(*device.ShareID))
		}
		sharedDeviceID := structured.MakeSharedDeviceID(deviceID, shareID)
		if !deallocatedDevices.Has(sharedDeviceID) {
			newClaim.Status.Devices[n] = device
			n++
		}
	}
	newClaim.Status.Devices = newClaim.Status.Devices[:n]

	if len(newClaim.Status.Devices) == 0 {
		newClaim.Status.Devices = nil
	}
}

// draConsumableCapacityFeatureInUse 检查旧 Spec、Allocation 和设备状态是否已经使用共享设备或消耗容量字段。
func draConsumableCapacityFeatureInUse(claim *resource.ResourceClaim) bool {
	if claim == nil {
		return false
	}

	if resourceclaimspec.DRAConsumableCapacityFeatureInUse(&claim.Spec) {
		return true
	}

	if allocation := claim.Status.Allocation; allocation != nil {
		for _, result := range allocation.Devices.Results {
			if result.ShareID != nil || result.ConsumedCapacity != nil {
				return true
			}
		}
	}
	if devices := claim.Status.Devices; devices != nil {
		for _, device := range devices {
			if device.ShareID != nil {
				return true
			}
		}
	}

	return false
}

// draDeviceBindingConditionsInUse 检查旧分配结果是否已经包含设备绑定条件或绑定失败条件。
func draDeviceBindingConditionsInUse(claim *resource.ResourceClaim) bool {
	if claim == nil {
		return false
	}
	if allocation := claim.Status.Allocation; allocation != nil {
		for _, result := range allocation.Devices.Results {
			if result.BindingConditions != nil || result.BindingFailureConditions != nil {
				return true
			}
		}
	}
	return false
}

// dropDisabledDRAResourceClaimConsumableCapacityStatusFields 在 DRAConsumableCapacity 未启用且旧对象未使用时，
// 清理新对象 Status 中的 ShareID 和 ConsumedCapacity 字段。
func dropDisabledDRAResourceClaimConsumableCapacityStatusFields(newClaim, oldClaim *resource.ResourceClaim) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity) ||
		draConsumableCapacityFeatureInUse(oldClaim) {
		// 特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}

	if allocation := newClaim.Status.Allocation; allocation != nil {
		for i := range allocation.Devices.Results {
			newClaim.Status.Allocation.Devices.Results[i].ShareID = nil
			newClaim.Status.Allocation.Devices.Results[i].ConsumedCapacity = nil
		}
	}

	if devices := newClaim.Status.Devices; devices != nil {
		for i := range devices {
			newClaim.Status.Devices[i].ShareID = nil
		}
	}
}

// dropDeviceBindingConditionsFields 在 DRADeviceBindingConditions 未启用且旧对象未使用时，
// 清理分配结果中的 BindingConditions 和 BindingFailureConditions。
func dropDeviceBindingConditionsFields(newClaim, oldClaim *resource.ResourceClaim) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceBindingConditions) ||
		draDeviceBindingConditionsInUse(oldClaim) {
		// 特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}

	if allocation := newClaim.Status.Allocation; allocation != nil {
		for i := range allocation.Devices.Results {
			newClaim.Status.Allocation.Devices.Results[i].BindingConditions = nil
			newClaim.Status.Allocation.Devices.Results[i].BindingFailureConditions = nil
		}
	}
}

// dropDisabledDRAOptionalNodeOperationsStatusFields 在特性关闭且旧对象未使用时清空 SkipNodeOperations。
func dropDisabledDRAOptionalNodeOperationsStatusFields(newClaim, oldClaim *resource.ResourceClaim) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAOptionalNodeOperations) ||
		draOptionalNodeOperationsStatusFeatureInUse(oldClaim) {
		return
	}

	if allocation := newClaim.Status.Allocation; allocation != nil {
		for i := range allocation.Devices.Results {
			newClaim.Status.Allocation.Devices.Results[i].SkipNodeOperations = nil
		}
	}
}

// draOptionalNodeOperationsStatusFeatureInUse 判断旧 Allocation 是否已经设置 SkipNodeOperations。
func draOptionalNodeOperationsStatusFeatureInUse(claim *resource.ResourceClaim) bool {
	if claim == nil {
		return false
	}
	if allocation := claim.Status.Allocation; allocation != nil {
		for _, result := range allocation.Devices.Results {
			if len(result.SkipNodeOperations) > 0 {
				return true
			}
		}
	}
	return false
}
