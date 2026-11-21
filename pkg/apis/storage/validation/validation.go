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

package validation

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/features"
)

// 校验时使用的尺寸与数量上限集中定义在此，避免各校验函数出现不一致。
// maxProvisionerParameterSize/Len 分别约束供给参数的总字节数与条目数；
// maxAttachedVolumeMetadataSize 约束附加卷元数据的总大小，防止 API 对象被过量数据撑大。
const (
	maxProvisionerParameterSize = 256 * (1 << 10) // 256 kB
	maxProvisionerParameterLen  = 512

	maxAttachedVolumeMetadataSize = 256 * (1 << 10) // 256 kB
	maxVolumeErrorMessageSize     = 1024

	csiNodeIDMaxLength = 256
)

// ValidateStorageClass 校验 StorageClass 对象的整体合法性。
// 依次校验元数据（名称需为合法类名）、provisioner（限定名）、parameters（键非空且总量不超限）、
// reclaimPolicy、volumeBindingMode 与 allowedTopologies。
// 所有字段的错误累积返回而不是遇到首个错误就终止，便于调用方一次性发现并修正全部问题。
func ValidateStorageClass(storageClass *storage.StorageClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&storageClass.ObjectMeta, false, apivalidation.ValidateClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateProvisioner(storageClass.Provisioner, field.NewPath("provisioner"))...)
	allErrs = append(allErrs, validateParameters(storageClass.Parameters, true, field.NewPath("parameters"))...)
	allErrs = append(allErrs, validateReclaimPolicy(storageClass.ReclaimPolicy, field.NewPath("reclaimPolicy"))...)
	allErrs = append(allErrs, validateVolumeBindingMode(storageClass.VolumeBindingMode, field.NewPath("volumeBindingMode"))...)
	allErrs = append(allErrs, validateAllowedTopologies(storageClass.AllowedTopologies, field.NewPath("allowedTopologies"))...)

	return allErrs
}

// ValidateStorageClassUpdate 校验 StorageClass 的更新是否合法。
// 除常规元数据更新校验外，parameters、provisioner、reclaimPolicy、volumeBindingMode 都是不可变字段：
// 存储供给行为依赖这些字段，一旦修改会导致已绑定 PV 的实际行为与类定义不一致。
// 不可变错误使用 WithOrigin("immutable") 标记，便于声明式校验框架识别错误来源。
func ValidateStorageClassUpdate(storageClass, oldStorageClass *storage.StorageClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&storageClass.ObjectMeta, &oldStorageClass.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(storageClass.Parameters, oldStorageClass.Parameters, field.NewPath("parameters")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	allErrs = append(allErrs, apivalidation.ValidateImmutableField(storageClass.Provisioner, oldStorageClass.Provisioner, field.NewPath("provisioner")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	allErrs = append(allErrs, apivalidation.ValidateImmutableField(storageClass.ReclaimPolicy, oldStorageClass.ReclaimPolicy, field.NewPath("reclaimPolicy")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	allErrs = append(allErrs, apivalidation.ValidateImmutableField(storageClass.VolumeBindingMode, oldStorageClass.VolumeBindingMode, field.NewPath("volumeBindingMode")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	return allErrs
}

// validateProvisioner 校验 provisioner 是否为合法的限定名（qualified name）。
// 空值直接报 Required：没有 provisioner 就无法确定由哪个插件供给。
// 非空值先转小写再校验，因为 provisioner 名称语义上不区分大小写，而 DNS 子域风格规则要求小写。
func validateProvisioner(provisioner string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 空 provisioner 无法解析出具体供给插件，直接按缺失处理。
	if len(provisioner) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, provisioner).MarkCoveredByDeclarative())
	}
	if len(provisioner) > 0 {
		allErrs = append(allErrs, apivalidation.ValidateQualifiedName(strings.ToLower(provisioner), fldPath)...)
	}
	return allErrs
}

// validateParameters 校验供给参数：键不能为空，条目数量与键/值总字节数不能超限。
// 条目数明显超限时提前返回，避免继续累加字节数并产生大量重复错误。
// allowEmpty 区分使用场景：StorageClass 允许没有参数，而 VolumeAttributesClass 必须至少有一组键值对，
// 因为它的参数直接决定卷属性，空集合表达不出任何属性。
func validateParameters(params map[string]string, allowEmpty bool, fldPath *field.Path) field.ErrorList {
	var totalSize int64
	allErrs := field.ErrorList{}

	// 条目数超限就提前返回，避免后续逐项累加与重复报错。
	if len(params) > maxProvisionerParameterLen {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, maxProvisionerParameterLen))
		return allErrs
	}

	for k, v := range params {
		// 空键无法在供给参数中定位对应项，而且会与其他空键相互覆盖。
		if len(k) < 1 {
			allErrs = append(allErrs, field.Invalid(fldPath, k, "field can not be empty."))
		}
		totalSize += (int64)(len(k)) + (int64)(len(v))
	}

	if totalSize > maxProvisionerParameterSize {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, maxProvisionerParameterSize))
	}

	if !allowEmpty && len(params) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, "must contain at least one key/value pair"))
	}
	return allErrs
}

// StorageClass 通用校验只接受 Delete 与 Retain：Recycle 已废弃，且部分后端（如 EBS）根本无法回收。
var supportedReclaimPolicy = sets.NewString(string(api.PersistentVolumeReclaimDelete), string(api.PersistentVolumeReclaimRetain))

// validateReclaimPolicy 校验回收策略是否属于支持的集合（Delete 或 Retain）。
// nil 表示未指定，交由默认策略处理，因此直接通过。
// 这里只做通用校验：特定后端能否真正支持某策略（例如 EBS 不支持 Recyclable）由卷插件在供给时自行拒绝。
func validateReclaimPolicy(reclaimPolicy *api.PersistentVolumeReclaimPolicy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if reclaimPolicy == nil {
		return allErrs
	}
	if len(string(*reclaimPolicy)) > 0 {
		if !supportedReclaimPolicy.Has(string(*reclaimPolicy)) {
			allErrs = append(allErrs, field.NotSupported(fldPath, reclaimPolicy, supportedReclaimPolicy.List()))
		}
	}
	return allErrs
}

// ValidateVolumeAttachment 校验 VolumeAttachment 对象，v1 与 v1beta1 版本共用这份逻辑。
// 除元数据、spec、status 外还单独校验 spec.source.persistentVolumeName：
// 该字段是对存量 PV 的引用，需要在字段路径上给出精确定位。
func ValidateVolumeAttachment(volumeAttachment *storage.VolumeAttachment) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&volumeAttachment.ObjectMeta, false, apivalidation.ValidateClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateVolumeAttachmentSpec(&volumeAttachment.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, validateVolumeAttachmentStatus(&volumeAttachment.Status, field.NewPath("status"))...)
	if volumeAttachment.Spec.Source.PersistentVolumeName != nil {
		pvName := *volumeAttachment.Spec.Source.PersistentVolumeName
		for _, msg := range apivalidation.ValidatePersistentVolumeName(pvName, false) {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec.source.persistentVolumeName"), pvName, msg))
		}
	}
	return allErrs
}

// validateVolumeAttachmentSpec 校验 VolumeAttachmentSpec 的字段合法性。
// attacher 必须是合法的 CSI 驱动名，source 必须在内联卷规格与 PV 名之间二选一，nodeName 必须是合法节点名。
func validateVolumeAttachmentSpec(
	spec *storage.VolumeAttachmentSpec, fldPath *field.Path) field.ErrorList {
	allErrs := apivalidation.ValidateCSIDriverName(spec.Attacher, fldPath.Child("attacher"), apivalidation.RequiredCovered, apivalidation.FormatCovered, apivalidation.SizeCovered)
	allErrs = append(allErrs, validateVolumeAttachmentSource(&spec.Source, fldPath.Child("source"))...)
	allErrs = append(allErrs, validateNodeName(spec.NodeName, fldPath.Child("nodeName"))...)
	return allErrs
}

// validateVolumeAttachmentSource 校验卷来源。
// 关键不变量：inlineVolumeSpec 与 persistentVolumeName 必须且只能指定一个，否则无法确定要挂载哪个卷。
// 选择 PV 名时要求非空；选择内联规格时复用 PV 规格校验，保证与静态 PV 遵循同样的约束。
func validateVolumeAttachmentSource(source *storage.VolumeAttachmentSource, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch {
	case source.InlineVolumeSpec == nil && source.PersistentVolumeName == nil:
		allErrs = append(allErrs, field.Required(fldPath, "must specify exactly one of inlineVolumeSpec and persistentVolumeName"))
	case source.InlineVolumeSpec != nil && source.PersistentVolumeName != nil:
		allErrs = append(allErrs, field.Forbidden(fldPath, "must specify exactly one of inlineVolumeSpec and persistentVolumeName"))
	case source.PersistentVolumeName != nil:
		if len(*source.PersistentVolumeName) == 0 {
			// 空字符串虽然通过了指针非 nil 的检查，但作为 PV 名仍然无效，必须单独报错。
			allErrs = append(allErrs, field.Required(fldPath.Child("persistentVolumeName"), "must specify non empty persistentVolumeName"))
		}
	case source.InlineVolumeSpec != nil:
		opts := apivalidation.PersistentVolumeSpecValidationOptions{}
		allErrs = append(allErrs, apivalidation.ValidatePersistentVolumeSpec(source.InlineVolumeSpec, "", true, fldPath.Child("inlineVolumeSpec"), opts)...)
	}
	return allErrs
}

// validateNodeName 校验 VolumeAttachment 中的 nodeName 是否为合法节点名。
// 此处不要求名称前缀（prefix=false），因为引用的是集群中已存在的节点对象名，而非新建对象。
func validateNodeName(nodeName string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range apivalidation.ValidateNodeName(nodeName, false /* prefix */) {
		allErrs = append(allErrs, field.Invalid(fldPath, nodeName, msg))
	}
	return allErrs
}

// validateVolumeAttachmentStatus 校验 VolumeAttachment 的状态字段。
// 分别检查附加元数据大小、attachError 与 detachError 的消息长度和错误码。
// 状态由控制器写入，限制尺寸可防止异常数据撑大 API 对象。
func validateVolumeAttachmentStatus(status *storage.VolumeAttachmentStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validateAttachmentMetadata(status.AttachmentMetadata, fldPath.Child("attachmentMetadata"))...)
	allErrs = append(allErrs, validateVolumeError(status.AttachError, fldPath.Child("attachError"))...)
	allErrs = append(allErrs, validateVolumeError(status.DetachError, fldPath.Child("detachError"))...)
	return allErrs
}

// validateAttachmentMetadata 校验卷附加元数据的总字节数不超过上限。
// CSI 驱动会把设备标识等信息写入该 map，限制总大小可避免单个卷携带过多元数据，
// 进而防止 etcd 中的对象膨胀。
func validateAttachmentMetadata(metadata map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	var size int64
	for k, v := range metadata {
		size += (int64)(len(k)) + (int64)(len(v))
	}
	if size > maxAttachedVolumeMetadataSize {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, maxAttachedVolumeMetadataSize))
	}
	return allErrs
}

// validateVolumeError 校验附加/分离错误的内容。
// nil 表示没有错误；非 nil 时限制消息长度并要求错误码非负（0 到 MaxInt32），
// 因为错误码由 CSI 驱动返回，负值不符合规范也无法映射到 API 字段。
func validateVolumeError(e *storage.VolumeError, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if e == nil {
		return allErrs
	}
	if len(e.Message) > maxVolumeErrorMessageSize {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("message"), "" /*unused*/, maxVolumeErrorMessageSize))
	}

	if e.ErrorCode != nil {
		value := *e.ErrorCode
		if value < 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("errorCode"), value, validation.InclusiveRangeError(0, math.MaxInt32)))
		}
	}
	return allErrs
}

// ValidateVolumeAttachmentUpdate 校验 VolumeAttachment 的更新。
// 先做完整校验，再确认 spec 不可变：挂载目标（attacher、节点、卷来源）改变后，
// 控制器无法安全地从一种挂载状态迁移到另一种，必须删除对象后重建。
func ValidateVolumeAttachmentUpdate(new, old *storage.VolumeAttachment) field.ErrorList {
	allErrs := ValidateVolumeAttachment(new)
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(new.Spec, old.Spec, field.NewPath("spec")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	return allErrs
}

// 支持的绑定模式只有立即绑定与等待首个消费者两种，其他取值调度器与控制器都无法解释。
var supportedVolumeBindingModes = sets.NewString(string(storage.VolumeBindingImmediate), string(storage.VolumeBindingWaitForFirstConsumer))

// validateVolumeBindingMode 校验卷绑定模式是否属于支持的取值。
// nil 视为缺失并报 Required：缺少该字段时控制器无法判断绑定时机，
// 不能在此静默补默认值（默认值由 API 默认值机制负责注入）。
func validateVolumeBindingMode(mode *storage.VolumeBindingMode, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if mode == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else if !supportedVolumeBindingModes.Has(string(*mode)) {
		allErrs = append(allErrs, field.NotSupported(fldPath, mode, supportedVolumeBindingModes.List()))
	}

	return allErrs
}

// validateAllowedTopologies 校验 allowedTopologies 中每个拓扑选择器项的合法性，并检测重复表达式。
// 重复的拓扑项会让供给结果难以预测，因此按语义相等（而非字符串相等）比较表达式集合。
func validateAllowedTopologies(topologies []api.TopologySelectorTerm, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(topologies) == 0 {
		return allErrs
	}

	rawTopologies := make([]map[string]sets.Set[string], len(topologies))
	for i, term := range topologies {
		idxPath := fldPath.Index(i)
		exprMap, termErrs := apivalidation.ValidateTopologySelectorTerm(term, fldPath.Index(i))
		allErrs = append(allErrs, termErrs...)

		// 当前对已见项做线性扫描；拓扑项数量有限，此处优先保证正确性而非极致性能。
		// TODO (verult) consider improving runtime
		for _, t := range rawTopologies {
			if helper.Semantic.DeepEqual(exprMap, t) {
				allErrs = append(allErrs, field.Duplicate(idxPath.Child("matchLabelExpressions"), ""))
			}
		}

		rawTopologies = append(rawTopologies, exprMap)
	}

	return allErrs
}

// ValidateCSINode 校验 CSINode 对象：元数据中的名称必须是合法节点名，spec 中的驱动列表需逐项通过校验。
// CSINode 由 kubelet 按节点创建，名称与节点一一对应是后续调度和卷管理正确工作的前提。
func ValidateCSINode(csiNode *storage.CSINode) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&csiNode.ObjectMeta, false, apivalidation.ValidateNodeName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateCSINodeSpec(&csiNode.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateCSINodeUpdate 校验 CSINode 的更新。
// 对同名驱动的已有条目检查不可变性：驱动的关键信息（NodeID、拓扑键等）变化意味着节点侧身份变更，
// 通常应通过删除并重建驱动条目来表达。
// MutableCSINodeAllocatableCount 特性开启时 Allocatable 允许变动，比较前必须将其从副本中排除，
// 否则会把合法的容量更新误报为不可变字段被修改。
func ValidateCSINodeUpdate(new, old *storage.CSINode) field.ErrorList {
	allErrs := ValidateCSINode(new)

	// 只检查新旧列表中同名的驱动条目：只有同一驱动的修改才涉及不可变字段约束，
	// 新增与删除驱动由其他路径处理。
	for _, oldDriver := range old.Spec.Drivers {
		for _, newDriver := range new.Spec.Drivers {
			if oldDriver.Name == newDriver.Name {
				// 特性开启时先清除副本中的 Allocatable 再比较；
				// 使用 DeepCopy 是为了不改动原始对象，+k8s:verify-mutation 注释说明这是有意的克隆后修改。
				if utilfeature.DefaultFeatureGate.Enabled(features.MutableCSINodeAllocatableCount) {
					oldDriverCopy := oldDriver.DeepCopy()
					newDriverCopy := newDriver.DeepCopy()
					oldDriverCopy.Allocatable = nil // +k8s:verify-mutation:reason=clone
					newDriverCopy.Allocatable = nil // +k8s:verify-mutation:reason=clone

					allErrs = append(allErrs,
						apivalidation.ValidateImmutableField(newDriverCopy, oldDriverCopy, field.NewPath("spec").Child("drivers"))...,
					)
				} else {
					allErrs = append(allErrs,
						apivalidation.ValidateImmutableField(newDriver, oldDriver, field.NewPath("spec").Child("drivers"))...,
					)
				}
			}
		}
	}
	return allErrs
}

// 存储健康状态只允许不可达与降级两种取值，其他状态无法被上层消费方解释。
var validStorageHealthStatusTypes = sets.New(
	storage.StorageUnreachable,
	storage.StorageDegraded,
)

// 健康条件中允许的访问模式与 PV 支持的四种模式保持一致。
var validStorageHealthAccessModes = sets.New(
	api.ReadWriteOnce,
	api.ReadOnlyMany,
	api.ReadWriteMany,
	api.ReadWriteOncePod,
)

// 单个 CSINode 上最多允许 16 条健康条件，防止状态对象无限增长。
const maxStorageHealthConditions = 16

// validateStorageHealthCondition 校验单条存储健康条件。
// 状态必须属于允许集合；reason 必填且需符合 metav1 的格式约定并限制长度；
// message 限制长度；accessMode 与 volumeMode 若提供则必须是受支持的值。
// 这些约束保证控制器写入的状态可预测、可展示，且不会因超长文本撑大对象。
func validateStorageHealthCondition(condition storage.StorageHealthCondition, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if !validStorageHealthStatusTypes.Has(condition.Status) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("status"), condition.Status, sets.List(validStorageHealthStatusTypes)))
	}
	if len(condition.Reason) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("reason"), ""))
	} else {
		for _, msg := range metav1validation.IsValidConditionReason(condition.Reason) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("reason"), condition.Reason, msg))
		}
		if len(condition.Reason) > 256 {
			allErrs = append(allErrs, field.TooLong(fldPath.Child("reason"), condition.Reason, 256))
		}
	}
	if len(condition.Message) > 1024 {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("message"), condition.Message, 1024))
	}
	if condition.AccessMode != nil && !validStorageHealthAccessModes.Has(*condition.AccessMode) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("accessMode"), *condition.AccessMode, sets.List(validStorageHealthAccessModes)))
	}
	if condition.VolumeMode != nil && *condition.VolumeMode != api.PersistentVolumeFilesystem && *condition.VolumeMode != api.PersistentVolumeBlock {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("volumeMode"), *condition.VolumeMode, []api.PersistentVolumeMode{api.PersistentVolumeFilesystem, api.PersistentVolumeBlock}))
	}
	return allErrs
}

// validateStorageHealth 校验某个 CSI 驱动的存储健康信息。
// 名称必须是合法的 CSI 驱动名；条件数量超限时提前返回，避免继续遍历大量条目；
// 其余情况逐条委托给 validateStorageHealthCondition。
func validateStorageHealth(health storage.StorageHealth, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(health.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else {
		allErrs = append(allErrs, apivalidation.ValidateCSIDriverName(health.Name, fldPath.Child("name"))...)
	}
	if len(health.HealthConditions) > maxStorageHealthConditions {
		allErrs = append(allErrs, field.TooMany(fldPath.Child("healthConditions"), len(health.HealthConditions), maxStorageHealthConditions))
		return allErrs
	}
	for i, condition := range health.HealthConditions {
		allErrs = append(allErrs, validateStorageHealthCondition(condition, fldPath.Child("healthConditions").Index(i))...)
	}
	return allErrs
}

// ValidateCSINodeStatusUpdate 校验 CSINode 状态中 storageHealth 部分的更新。
// 同名驱动的健康信息不允许重复出现，否则状态读取方无法确定应以哪一条为准。
// 每条健康信息还需通过字段级校验，保证控制器写入的数据格式可预测。
func ValidateCSINodeStatusUpdate(new, old *storage.CSINode) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&new.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))
	fldPath := field.NewPath("status", "storageHealth")
	seen := sets.New[string]()
	for i, health := range new.Status.StorageHealth {
		idxPath := fldPath.Index(i)
		if seen.Has(health.Name) {
			allErrs = append(allErrs, field.Duplicate(idxPath, health.Name))
		}
		seen.Insert(health.Name)
		allErrs = append(allErrs, validateStorageHealth(health, idxPath)...)
	}
	return allErrs
}

// validateCSINodeSpec 校验 CSINodeSpec：将驱动列表交给 validateCSINodeDrivers 做逐项与重名检查。
func validateCSINodeSpec(spec *storage.CSINodeSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validateCSINodeDrivers(spec.Drivers, fldPath.Child("drivers"))...)
	return allErrs
}

// validateCSINodeDrivers 校验 CSINode 的驱动列表。
// 用共享集合 driverNamesInSpecs 在遍历中检测重名驱动：同一节点上同名驱动只能出现一次，
// 否则节点到驱动的映射会产生歧义。
func validateCSINodeDrivers(drivers []storage.CSINodeDriver, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	driverNamesInSpecs := sets.New[string]()
	for i, driver := range drivers {
		idxPath := fldPath.Index(i)
		allErrs = append(allErrs, validateCSINodeDriver(driver, driverNamesInSpecs, idxPath)...)
	}

	return allErrs
}

// validateCSINodeDriverNodeID 校验 CSINodeDriver 的 NodeID。
// NodeID 是驱动识别节点的标识，必须非空且不超过 csiNodeIDMaxLength，
// 过长的 ID 在 CSI 调用与指标标签中都可能引发问题。
func validateCSINodeDriverNodeID(nodeID string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// nodeID 始终必填：即使驱动侧认为节点标识可选，API 也需要它来定位驱动所在的节点。
	if len(nodeID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, nodeID))
	}

	if len(nodeID) > csiNodeIDMaxLength {
		allErrs = append(allErrs, field.Invalid(fldPath, nodeID, fmt.Sprintf("must be %d characters or less", csiNodeIDMaxLength)))
	}
	return allErrs
}

// validateCSINodeDriverAllocatable 校验驱动的可用卷数量上限。
// 未设置表示不限制；一旦设置则必须非负，负数没有物理含义且会让容量计算失真。
func validateCSINodeDriverAllocatable(a *storage.VolumeNodeResources, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if a == nil || a.Count == nil {
		return allErrs
	}

	allErrs = append(allErrs, apivalidation.ValidateNonnegativeField(int64(*a.Count), fldPath.Child("count"))...)
	return allErrs
}

// validateCSINodeDriver 校验单个 CSINodeDriver 条目。
// 依次检查驱动名、NodeID、Allocatable，还检测驱动名重复以及拓扑键重复或非法：
// 拓扑键用于卷拓扑约束，空键或重复键都会让调度判断出现歧义。
func validateCSINodeDriver(driver storage.CSINodeDriver, driverNamesInSpecs sets.Set[string], fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, apivalidation.ValidateCSIDriverName(driver.Name, fldPath.Child("name"))...)
	allErrs = append(allErrs, validateCSINodeDriverNodeID(driver.NodeID, fldPath.Child("nodeID"))...)
	allErrs = append(allErrs, validateCSINodeDriverAllocatable(driver.Allocatable, fldPath.Child("allocatable"))...)

	// 同名驱动只允许出现一次；集合在遍历过程中持续更新，使后续条目能与之前的比较。
	if driverNamesInSpecs.Has(driver.Name) {
		allErrs = append(allErrs, field.Duplicate(fldPath.Child("name"), driver.Name))
	}
	driverNamesInSpecs.Insert(driver.Name)
	topoKeys := sets.New[string]()
	for _, key := range driver.TopologyKeys {
		if len(key) == 0 {
			allErrs = append(allErrs, field.Required(fldPath, key))
		}

		if topoKeys.Has(key) {
			allErrs = append(allErrs, field.Duplicate(fldPath, key))
		}
		topoKeys.Insert(key)

		allErrs = append(allErrs, apivalidation.ValidateQualifiedName(key, fldPath)...)
	}

	return allErrs
}

// ValidateCSIDriverName 校验 CSIDriver 对象名称：要求符合 DNS 子域格式。
// 该函数值既传给 ValidateObjectMeta，也供其他包复用同一套命名规则。
var ValidateCSIDriverName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateCSIDriver 校验 CSIDriver 对象：元数据名称需符合 DNS 子域规则，spec 需通过逐字段校验。
// 驱动对象一旦注册会被多个组件缓存，名称与规格的合法性是后续挂载行为正确的基础。
func ValidateCSIDriver(csiDriver *storage.CSIDriver) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&csiDriver.ObjectMeta, false, ValidateCSIDriverName, field.NewPath("metadata"))

	allErrs = append(allErrs, validateCSIDriverSpec(&csiDriver.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateCSIDriverUpdate 校验 CSIDriver 的更新。
// spec 本身需要重新校验；AttachRequired 与 VolumeLifecycleModes 为不可变字段：
// 前者决定 kubelet 是否执行 attach 流程，后者决定卷生命周期语义，运行中变更会破坏已有 Pod。
func ValidateCSIDriverUpdate(new, old *storage.CSIDriver) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&new.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateCSIDriverSpec(&new.Spec, field.NewPath("spec"))...)

	// 不可变字段不允许修改：这些字段在驱动注册后已被各组件缓存，变更会导致行为不一致。
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(new.Spec.AttachRequired, old.Spec.AttachRequired, field.NewPath("spec", "attachedRequired"))...)
	allErrs = append(allErrs, apimachineryvalidation.ValidateImmutableField(new.Spec.VolumeLifecycleModes, old.Spec.VolumeLifecycleModes, field.NewPath("spec", "volumeLifecycleModes")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	return allErrs
}

// validateCSIDriverSpec 校验 CSIDriverSpec 的全部字段。
// 多个布尔开关必须显式设置（nil 表示用户未配置，不能静默猜测默认值）；
// 同时校验 tokenRequests、卷生命周期模式、SELinux 挂载及与特性开关相关的条件字段。
// 所有错误累积返回，便于一次性修正配置。
func validateCSIDriverSpec(
	spec *storage.CSIDriverSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, validateAttachRequired(spec.AttachRequired, fldPath.Child("attachedRequired"))...)
	allErrs = append(allErrs, validatePodInfoOnMount(spec.PodInfoOnMount, fldPath.Child("podInfoOnMount"))...)
	allErrs = append(allErrs, validateStorageCapacity(spec.StorageCapacity, fldPath.Child("storageCapacity"))...)
	allErrs = append(allErrs, validateFSGroupPolicy(spec.FSGroupPolicy, fldPath.Child("fsGroupPolicy"))...)
	allErrs = append(allErrs, validateTokenRequests(spec.TokenRequests, fldPath.Child("tokenRequests"))...)
	allErrs = append(allErrs, validateVolumeLifecycleModes(spec.VolumeLifecycleModes, fldPath.Child("volumeLifecycleModes"))...)
	allErrs = append(allErrs, validateSELinuxMount(spec.SELinuxMount, fldPath.Child("seLinuxMount"))...)
	allErrs = append(allErrs, validateNodeAllocatableUpdatePeriodSeconds(spec.NodeAllocatableUpdatePeriodSeconds, fldPath.Child("nodeAllocatableUpdatePeriodSeconds"))...)
	allErrs = append(allErrs, validateServiceAccountTokenInSecrets(spec.ServiceAccountTokenInSecrets, spec.TokenRequests, fldPath.Child("serviceAccountTokenInSecrets"))...)
	allErrs = append(allErrs, validatePreventPodSchedulingIfMissing(spec.PreventPodSchedulingIfMissing, fldPath.Child("preventPodSchedulingIfMissing"))...)
	return allErrs
}

// validateNodeAllocatableUpdatePeriodSeconds 校验节点可分配容量的上报周期。
// 未设置表示使用默认值；一旦设置则不得小于 10 秒，
// 过于频繁的上报会放大 API 请求与控制器处理压力。
func validateNodeAllocatableUpdatePeriodSeconds(period *int64, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 上报周期短于 10 秒会给 API 和控制器带来不必要的压力。
	if period != nil && *period < 10 {
		allErrs = append(allErrs, field.Invalid(fldPath, *period, "must be greater than or equal to 10 seconds"))
	}
	return allErrs
}

// validateAttachRequired 校验 attachRequired 是否已显式设置。
// nil 会报 Required：该字段决定 kubelet 是否走 attach/detach 流程，缺失会让行为不确定。
func validateAttachRequired(attachRequired *bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if attachRequired == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	}

	return allErrs
}

// validatePodInfoOnMount 校验 podInfoOnMount 是否已显式设置。
// nil 会报 Required：是否向驱动暴露 Pod 信息会影响挂载逻辑，不能由校验层替用户猜测。
func validatePodInfoOnMount(podInfoOnMount *bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if podInfoOnMount == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	}

	return allErrs
}

// validateStorageCapacity 校验 storageCapacity 是否已显式设置。
// nil 会报 Required：该开关决定容量感知调度是否生效，需要用户明确表达意图。
func validateStorageCapacity(storageCapacity *bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if storageCapacity == nil {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	}

	return allErrs
}

var supportedFSGroupPolicy = sets.NewString(string(storage.ReadWriteOnceWithFSTypeFSGroupPolicy), string(storage.FileFSGroupPolicy), string(storage.NoneFSGroupPolicy))

// validateFSGroupPolicy 校验 fsGroupPolicy 是否属于支持的策略集合。
// 未设置表示沿用默认行为；设置非法值会让 kubelet 无法决定如何应用 fsGroup。
func validateFSGroupPolicy(fsGroupPolicy *storage.FSGroupPolicy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if fsGroupPolicy == nil {
		// 该字段非必填，未设置时由默认策略决定 fsGroup 的处理方式，直接返回即可。
		return allErrs
	}

	if !supportedFSGroupPolicy.Has(string(*fsGroupPolicy)) {
		allErrs = append(allErrs, field.NotSupported(fldPath, fsGroupPolicy, supportedFSGroupPolicy.List()))
	}

	return allErrs
}

// validateTokenRequests 校验 TokenRequest 列表：audience 不能重复，否则 token 的受众含义会模糊；
// 空 audience 同样作为键参与去重，因此实际上最多只有一条请求可以不指定 audience。
// 过期时间若指定必须在 10 分钟到 2^32 秒之间：过短的 token 撑不过一次挂载流程，
// 过长则超出字段可表达的范围。
func validateTokenRequests(tokenRequests []storage.TokenRequest, fldPath *field.Path) field.ErrorList {
	const min = 10 * time.Minute
	allErrs := field.ErrorList{}
	audiences := make(map[string]bool)
	for i, tokenRequest := range tokenRequests {
		path := fldPath.Index(i)
		// 空 audience 也会作为 map 键参与去重，因此最多只有一条请求能省略 audience。
		audience := tokenRequest.Audience
		if _, ok := audiences[audience]; ok {
			allErrs = append(allErrs, field.Duplicate(path.Child("audience"), audience))
			continue
		}
		audiences[audience] = true

		if tokenRequest.ExpirationSeconds == nil {
			continue
		}
		if *tokenRequest.ExpirationSeconds < int64(min.Seconds()) {
			allErrs = append(allErrs, field.Invalid(path.Child("expirationSeconds"), *tokenRequest.ExpirationSeconds, "may not specify a duration less than 10 minutes"))
		}
		if *tokenRequest.ExpirationSeconds > 1<<32 {
			allErrs = append(allErrs, field.Invalid(path.Child("expirationSeconds"), *tokenRequest.ExpirationSeconds, "may not specify a duration larger than 2^32 seconds"))
		}
	}

	return allErrs
}

// validateVolumeLifecycleModes 校验卷生命周期模式只能是持久卷或临时卷。
// 其他值无法被调度器与 kubelet 解释，必须拒绝而不是静默忽略。
func validateVolumeLifecycleModes(modes []storage.VolumeLifecycleMode, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, mode := range modes {
		switch mode {
		case storage.VolumeLifecyclePersistent, storage.VolumeLifecycleEphemeral:
		default:
			allErrs = append(allErrs, field.NotSupported(fldPath, mode,
				[]string{
					string(storage.VolumeLifecyclePersistent),
					string(storage.VolumeLifecycleEphemeral),
				}))
		}
	}

	return allErrs
}

// validateSELinuxMount 校验 seLinuxMount 是否已显式设置。
// 仅在 SELinuxMountReadWriteOncePod 特性开启时才强制要求：
// 特性未启用时该字段不起作用，强制必填会破坏向后兼容。
func validateSELinuxMount(seLinuxMount *bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if seLinuxMount == nil && utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	}

	return allErrs
}

// validateServiceAccountTokenInSecrets 校验 serviceAccountTokenInSecrets 与 tokenRequests 的依赖关系。
// 要求把 token 写入 Secret 却没有配置任何 tokenRequests 时，该设置没有任何实际效果，
// 属于自相矛盾的配置，直接拒绝以免用户误以为 token 已被注入。
func validateServiceAccountTokenInSecrets(serviceAccountTokenInSecrets *bool, tokenRequests []storage.TokenRequest, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if serviceAccountTokenInSecrets != nil && len(tokenRequests) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, serviceAccountTokenInSecrets, "serviceAccountTokenInSecrets is set but no tokenRequests are specified"))
	}

	return allErrs
}

// validatePreventPodSchedulingIfMissing 校验 preventPodSchedulingIfMissing 字段。
// VolumeLimitScaling 特性开启时该字段必须显式设置，否则控制器无法决定容量不足时是否阻止 Pod 调度；
// 特性未开启时不作要求，以保持旧行为兼容。
func validatePreventPodSchedulingIfMissing(preventPodSchedulingIfMissing *bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if preventPodSchedulingIfMissing == nil && utilfeature.DefaultFeatureGate.Enabled(features.VolumeLimitScaling) {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	}

	return allErrs
}

// ValidateStorageCapacityName 校验 CSIStorageCapacity 对象名称：要求符合 DNS 子域格式。
var ValidateStorageCapacityName = apimachineryvalidation.NameIsDNSSubdomain

// CSIStorageCapacityValidateOptions 控制校验标签选择器时对取值语法的宽松程度。
// 从旧版本转换来的对象可能带有不合规的标签值，读取路径可临时放宽，以免存量对象无法被读取。
type CSIStorageCapacityValidateOptions struct {
	// 允许节点拓扑选择器中出现不符合标签值语法的取值，仅用于兼容存量数据。
	AllowInvalidLabelValueInSelector bool
}

// ValidateCSIStorageCapacity 校验 CSIStorageCapacity 对象。
// 元数据名称需符合 DNS 子域规则，nodeTopology 是标签选择器（可按选项放宽对取值语法的校验），
// storageClassName 需为合法类名，非空容量必须是非负数量。
// 该对象描述某拓扑段上的可用存储容量，字段非法会让容量感知调度做出错误决策。
func ValidateCSIStorageCapacity(capacity *storage.CSIStorageCapacity, opts CSIStorageCapacityValidateOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&capacity.ObjectMeta, true, ValidateStorageCapacityName, field.NewPath("metadata"))
	labelSelectorValidationOptions := metav1validation.LabelSelectorValidationOptions{AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector}
	allErrs = append(allErrs, metav1validation.ValidateLabelSelector(capacity.NodeTopology, labelSelectorValidationOptions, field.NewPath("nodeTopology"))...)
	for _, msg := range apivalidation.ValidateClassName(capacity.StorageClassName, false) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("storageClassName"), capacity.StorageClassName, msg))
	}
	if capacity.Capacity != nil {
		allErrs = append(allErrs, apivalidation.ValidateNonnegativeQuantity(*capacity.Capacity, field.NewPath("capacity"))...)
	}
	return allErrs
}

// ValidateCSIStorageCapacityUpdate 校验 CSIStorageCapacity 的更新。
// nodeTopology 与 storageClassName 是 CSI GetCapacity 的输入，属于不可变字段：
// 它们变化时应创建新对象而不是修改原对象，否则缓存的容量信息会与查询条件不一致。
func ValidateCSIStorageCapacityUpdate(capacity, oldCapacity *storage.CSIStorageCapacity) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&capacity.ObjectMeta, &oldCapacity.ObjectMeta, field.NewPath("metadata"))

	// 这两个字段是 CSI GetCapacity 的输入，因此不可变。
	// 若将来放宽限制，务必在 PrepareForUpdate 中递增 Generation，
	// 让控制器感知查询条件变化并重新计算容量。
	if !apiequality.Semantic.DeepEqual(capacity.NodeTopology, oldCapacity.NodeTopology) {
		allErrs = append(allErrs, field.Invalid(field.NewPath("nodeTopology"), capacity.NodeTopology, "field is immutable"))
	}
	if capacity.StorageClassName != oldCapacity.StorageClassName {
		allErrs = append(allErrs, field.Invalid(field.NewPath("storageClassName"), capacity.StorageClassName, "field is immutable"))
	}

	return allErrs
}

// ValidateVolumeAttributesClass 校验 VolumeAttributesClass 对象。
// 名称需为合法类名，driverName 需为合法限定名，参数不能为空且要满足数量与大小限制：
// 该类直接承载卷属性参数，空参数集合表达不出任何属性。
func ValidateVolumeAttributesClass(volumeAttributesClass *storage.VolumeAttributesClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&volumeAttributesClass.ObjectMeta, false, apivalidation.ValidateClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateProvisioner(volumeAttributesClass.DriverName, field.NewPath("driverName"))...)
	allErrs = append(allErrs, validateParameters(volumeAttributesClass.Parameters, false, field.NewPath("parameters"))...)
	return allErrs
}

// ValidateVolumeAttributesClassUpdate 校验 VolumeAttributesClass 的更新。
// driverName 与 parameters 均禁止修改：已创建的卷按创建时的属性运行，
// 允许变更会让卷的实际属性与类的定义脱节；最后再对新对象做一次完整校验。
func ValidateVolumeAttributesClassUpdate(volumeAttributesClass, oldVolumeAttributesClass *storage.VolumeAttributesClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&volumeAttributesClass.ObjectMeta, &oldVolumeAttributesClass.ObjectMeta, field.NewPath("metadata"))
	if volumeAttributesClass.DriverName != oldVolumeAttributesClass.DriverName {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("driverName"), "updates to driverName are forbidden."))
	}
	if !reflect.DeepEqual(oldVolumeAttributesClass.Parameters, volumeAttributesClass.Parameters) {
		allErrs = append(allErrs, field.Forbidden(field.NewPath("parameters"), "updates to parameters are forbidden."))
	}
	allErrs = append(allErrs, ValidateVolumeAttributesClass(volumeAttributesClass)...)
	return allErrs
}
