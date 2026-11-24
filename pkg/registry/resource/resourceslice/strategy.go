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

package resourceslice

import (
	"context"
	"fmt"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/resource/validation"
	"k8s.io/kubernetes/pkg/features"
)

// resourceSliceStrategy 定义 ResourceSlice 的创建、更新、校验和索引选择器策略。
type resourceSliceStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 ResourceSlice 时使用的默认 REST 策略。
var Strategy = resourceSliceStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 false，因为 ResourceSlice 是集群级资源。
func (resourceSliceStrategy) NamespaceScoped() bool {
	return false
}

// PrepareForCreate 将 Generation 初始化为 1，并按 Feature Gate 清理不可用的新字段。
func (resourceSliceStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	slice := obj.(*resource.ResourceSlice)
	slice.Generation = 1

	dropDisabledFields(slice, nil)
}

// Validate 校验 ResourceSlice 的 Driver、Pool、设备、容量和节点分配信息。
func (resourceSliceStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	slice := obj.(*resource.ResourceSlice)
	return validation.ValidateResourceSlice(slice)
}

// DeclarativeValidationConfig 为通用 BeforeCreate/BeforeUpdate 流程提供声明式校验配置，
// 同时应用 ResourceNormalizationRules 和 DRAPartitionableDevicesType 特性门控。
func (resourceSliceStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{
		NormalizationRules: validation.ResourceNormalizationRules,
		Options: map[string]bool{
			string(features.DRAPartitionableDevicesType): utilfeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevicesType),
		},
	}
}

// WarningsOnCreate 检查 Driver 名称大小写，并返回非阻断性警告。
func (resourceSliceStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newResourceSlice := obj.(*resource.ResourceSlice)
	var warnings []string

	if newResourceSlice.Spec.Driver != strings.ToLower(newResourceSlice.Spec.Driver) {
		warnings = append(warnings,
			fmt.Sprintf("spec.driver: driver names should be lowercase; %q contains uppercase characters", newResourceSlice.Spec.Driver))
	}

	return warnings
}

// Canonicalize 在校验后规范化 ResourceSlice；当前不执行额外转换。
func (resourceSliceStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，ResourceSlice 必须通过 POST 创建。
func (resourceSliceStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForUpdate 在 Spec 变化时递增 Generation，并保留旧对象已经使用的特性字段。
func (resourceSliceStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	slice := obj.(*resource.ResourceSlice)
	oldSlice := old.(*resource.ResourceSlice)

	// Spec 发生变化时递增 Generation。
	if !apiequality.Semantic.DeepEqual(oldSlice.Spec, slice.Spec) {
		slice.Generation = oldSlice.Generation + 1
	}

	dropDisabledFields(slice, oldSlice)
}

// ValidateUpdate 根据新旧对象校验 ResourceSlice 更新是否合法。
func (resourceSliceStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateResourceSliceUpdate(obj.(*resource.ResourceSlice), old.(*resource.ResourceSlice))
}

// WarningsOnUpdate 检查更新后的 Driver 名称大小写，并返回非阻断性警告。
func (resourceSliceStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newResourceSlice := obj.(*resource.ResourceSlice)
	var warnings []string

	if newResourceSlice.Spec.Driver != strings.ToLower(newResourceSlice.Spec.Driver) {
		warnings = append(warnings,
			fmt.Sprintf("spec.driver: driver names should be lowercase; %q contains uppercase characters", newResourceSlice.Spec.Driver))
	}

	return warnings
}

// AllowUnconditionalUpdate 返回 true，允许通用更新流程为缺少 ResourceVersion 的请求填入最新版本。
func (resourceSliceStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

var TriggerFunc = map[string]storage.IndexerFunc{
	// 当前存储缓存索引只支持一个索引字段，因此仅为 ResourceSlice 的 NodeName 建立索引。
	// 具体限制参见 apiserver/pkg/storage/cacher 中的索引触发逻辑。
	resource.ResourceSliceSelectorNodeName: nodeNameTriggerFunc,
}

// nodeNameTriggerFunc 从 ResourceSlice 中提取 NodeName，作为缓存索引触发键。
func nodeNameTriggerFunc(obj runtime.Object) string {
	rs := obj.(*resource.ResourceSlice)
	if rs.Spec.NodeName == nil {
		return ""
	} else {
		return *rs.Spec.NodeName
	}
}

// Indexers 返回 ResourceSlice 的字段索引器，目前仅索引 Spec.NodeName。
func Indexers() *cache.Indexers {
	return &cache.Indexers{
		storage.FieldIndex(resource.ResourceSliceSelectorNodeName): nodeNameIndexFunc,
	}
}

// nodeNameIndexFunc 返回 ResourceSlice 的 NodeName 索引值；未指定节点时使用空字符串。
func nodeNameIndexFunc(obj interface{}) ([]string, error) {
	slice, ok := obj.(*resource.ResourceSlice)
	if !ok {
		return nil, fmt.Errorf("not a ResourceSlice")
	}
	if slice.Spec.NodeName == nil {
		return []string{""}, nil
	}
	return []string{*slice.Spec.NodeName}, nil
}

// GetAttrs 提取 ResourceSlice 的标签和字段，供 List、Watch 选择器过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	slice, ok := obj.(*resource.ResourceSlice)
	if !ok {
		return nil, nil, fmt.Errorf("not a ResourceSlice")
	}
	return labels.Set(slice.ObjectMeta.Labels), toSelectableFields(slice), nil
}

// Match 根据标签和字段选择器构造通用存储匹配器，并启用 NodeName 索引。
func Match(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:       label,
		Field:       field,
		GetAttrs:    GetAttrs,
		IndexFields: []string{resource.ResourceSliceSelectorNodeName},
	}
}

// toSelectableFields 将 ResourceSlice 转换为可供字段选择器匹配的字段集合。
// TODO：字段选择器不是标签选择器，不能套用标签值的校验规则。
func toSelectableFields(slice *resource.ResourceSlice) fields.Set {
	// 预先分配固定容量可减少构造 fields.Set 时的内存分配。
	// 如果在此处增加字段，或 ObjectMeta 相关字段数量发生变化，应同步调整容量。
	fields := make(fields.Set, 4)
	if slice.Spec.NodeName == nil {
		fields[resource.ResourceSliceSelectorNodeName] = ""
	} else {
		fields[resource.ResourceSliceSelectorNodeName] = *slice.Spec.NodeName
	}
	fields[resource.ResourceSliceSelectorDriver] = slice.Spec.Driver
	fields[resource.ResourceSliceSelectorPoolName] = slice.Spec.Pool.Name

	// 再加入 ObjectMeta 中可供选择器匹配的字段。
	return generic.AddObjectMetaFieldsSet(fields, &slice.ObjectMeta, false)
}

// dropDisabledFields 统一清理 ResourceSlice Spec 中由 Feature Gate 控制且旧对象未使用的字段。
func dropDisabledFields(newSlice, oldSlice *resource.ResourceSlice) {
	dropDisabledDRADeviceTaintsFields(newSlice, oldSlice)
	dropDisabledDRAPartitionableDevicesFields(newSlice, oldSlice)
	dropDisabledDRADeviceBindingConditionsFields(newSlice, oldSlice)
	dropDisabledDRAConsumableCapacityFields(newSlice, oldSlice)
	dropDisabledDRADeviceCompatibilityGroupsFields(newSlice, oldSlice)
	dropDisabledDRANodeAllocatableResourcesFields(newSlice, oldSlice)
	dropDisableDRAListTypeAttributesFields(newSlice, oldSlice)
	dropDisabledDRAPartitionableDevicesTypeFields(newSlice, oldSlice)
	dropDisabledDRAOptionalNodeOperationsFields(newSlice, oldSlice)
}

func dropDisabledDRAPartitionableDevicesTypeFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevicesType) || draPartitionableDevicesTypeFeatureInUse(oldSlice) {
		return
	}

	newSlice.Spec.PartitionTypeAttribute = nil
}

func draPartitionableDevicesTypeFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}
	return slice.Spec.PartitionTypeAttribute != nil
}

func dropDisabledDRADeviceTaintsFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaints) || draDeviceTaintsFeatureInUse(oldSlice) {
		return
	}

	for i := range newSlice.Spec.Devices {
		newSlice.Spec.Devices[i].Taints = nil
	}
}

func draDeviceTaintsFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	for _, device := range slice.Spec.Devices {
		if len(device.Taints) > 0 {
			return true
		}
	}
	return false
}

func dropDisabledDRAPartitionableDevicesFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAPartitionableDevices) || draPartitionableDevicesFeatureInUse(oldSlice) {
		return
	}

	newSlice.Spec.SharedCounters = nil
	newSlice.Spec.PerDeviceNodeSelection = nil
	for i := range newSlice.Spec.Devices {
		newSlice.Spec.Devices[i].ConsumesCounters = nil
		newSlice.Spec.Devices[i].NodeName = nil
		newSlice.Spec.Devices[i].NodeSelector = nil
		newSlice.Spec.Devices[i].AllNodes = nil
	}
}

func dropDisabledDRADeviceBindingConditionsFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceBindingConditions) && utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimDeviceStatus) ||
		draBindingConditionsFeatureInUse(oldSlice) {
		return
	}

	for i := range newSlice.Spec.Devices {
		newSlice.Spec.Devices[i].BindingConditions = nil
		newSlice.Spec.Devices[i].BindingFailureConditions = nil
		newSlice.Spec.Devices[i].BindsToNode = nil
	}
}

func draPartitionableDevicesFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	spec := slice.Spec
	if len(spec.SharedCounters) > 0 || spec.PerDeviceNodeSelection != nil {
		return true
	}

	for _, device := range spec.Devices {
		if len(device.ConsumesCounters) > 0 {
			return true
		}
		if device.NodeName != nil || device.NodeSelector != nil || device.AllNodes != nil {
			return true
		}
	}
	return false
}

func draBindingConditionsFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	for _, device := range slice.Spec.Devices {
		if len(device.BindingConditions) > 0 ||
			len(device.BindingFailureConditions) > 0 ||
			device.BindsToNode != nil {
			return true
		}
	}
	return false
}

func draConsumableCapacityFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	spec := slice.Spec
	for _, device := range spec.Devices {
		if device.AllowMultipleAllocations != nil {
			return true
		}
		for _, capacity := range device.Capacity {
			if capacity.RequestPolicy != nil {
				return true
			}
		}
	}

	return false
}

// dropDisabledDRAConsumableCapacityFields 在 DRAConsumableCapacity 未启用且旧对象未使用时，
// 清理 AllowMultipleAllocations 和 RequestPolicy 字段。
func dropDisabledDRAConsumableCapacityFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity) ||
		draConsumableCapacityFeatureInUse(oldSlice) {
		// 特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}

	for i := range newSlice.Spec.Devices {
		newSlice.Spec.Devices[i].AllowMultipleAllocations = nil
		if newSlice.Spec.Devices[i].Capacity != nil {
			for ci, capacity := range newSlice.Spec.Devices[i].Capacity {
				capacity.RequestPolicy = nil
				newSlice.Spec.Devices[i].Capacity[ci] = capacity
			}
		}
	}
}

func draDeviceCompatibilityGroupsFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	for _, device := range slice.Spec.Devices {
		for _, consumption := range device.ConsumesCounters {
			if len(consumption.CompatibilityGroups) > 0 {
				return true
			}
		}
	}
	return false
}

// dropDisabledDRADeviceCompatibilityGroupsFields 在 DRADeviceCompatibilityGroups 未启用且旧对象未使用时，
// 清理每个 device.consumesCounters 条目中的 CompatibilityGroups。
func dropDisabledDRADeviceCompatibilityGroupsFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceCompatibilityGroups) ||
		draDeviceCompatibilityGroupsFeatureInUse(oldSlice) {
		// 特性已启用，或旧对象已经使用该字段，因此无需清理。
		return
	}

	for i := range newSlice.Spec.Devices {
		for j := range newSlice.Spec.Devices[i].ConsumesCounters {
			newSlice.Spec.Devices[i].ConsumesCounters[j].CompatibilityGroups = nil
		}
	}
}

// dropDisabledDRANodeAllocatableResourcesFields 在特性关闭且旧对象未使用时清理设备节点可分配资源信息。
func dropDisabledDRANodeAllocatableResourcesFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources) || draNodeAllocatableResourcesFeatureInUse(oldSlice) {
		return
	}

	for i := range newSlice.Spec.Devices {
		newSlice.Spec.Devices[i].NodeAllocatableResources = nil
	}
}

func draNodeAllocatableResourcesFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	for _, device := range slice.Spec.Devices {
		if len(device.NodeAllocatableResources) > 0 {
			return true
		}
	}
	return false
}

// dropDisableDRAListTypeAttributesFields 在特性关闭且旧对象未使用时清理属性中的多值字段。
func dropDisableDRAListTypeAttributesFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAListTypeAttributes) ||
		draListTypeAttributesFeatureInUse(oldSlice) {
		return
	}

	for i := range newSlice.Spec.Devices {
		for k, deviceAttribute := range newSlice.Spec.Devices[i].Attributes {
			if deviceAttribute.BoolValues != nil {
				deviceAttribute.BoolValues = nil
				newSlice.Spec.Devices[i].Attributes[k] = deviceAttribute
			}
			if deviceAttribute.IntValues != nil {
				deviceAttribute.IntValues = nil
				newSlice.Spec.Devices[i].Attributes[k] = deviceAttribute
			}
			if deviceAttribute.StringValues != nil {
				deviceAttribute.StringValues = nil
				newSlice.Spec.Devices[i].Attributes[k] = deviceAttribute
			}
			if deviceAttribute.VersionValues != nil {
				deviceAttribute.VersionValues = nil
				newSlice.Spec.Devices[i].Attributes[k] = deviceAttribute
			}
		}
	}
}

func draListTypeAttributesFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}

	for _, device := range slice.Spec.Devices {
		for _, deviceAttribute := range device.Attributes {
			if deviceAttribute.BoolValues != nil || deviceAttribute.IntValues != nil || deviceAttribute.StringValues != nil || deviceAttribute.VersionValues != nil {
				return true
			}
		}
	}

	return false
}

// dropDisabledDRAOptionalNodeOperationsFields 在特性关闭且旧对象未使用时清理 SkipNodeOperations。
func dropDisabledDRAOptionalNodeOperationsFields(newSlice, oldSlice *resource.ResourceSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAOptionalNodeOperations) || draOptionalNodeOperationsFeatureInUse(oldSlice) {
		return
	}

	newSlice.Spec.SkipNodeOperations = nil
}

func draOptionalNodeOperationsFeatureInUse(slice *resource.ResourceSlice) bool {
	if slice == nil {
		return false
	}
	return len(slice.Spec.SkipNodeOperations) > 0
}
