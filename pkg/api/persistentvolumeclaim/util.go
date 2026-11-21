/*
Copyright 2017 The Kubernetes Authors.

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

package persistentvolumeclaim

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/features"
)

const (
	pvc                                  string = "PersistentVolumeClaim"
	volumeSnapshot                       string = "VolumeSnapshot"
	deprecatedStorageClassAnnotationsMsg        = `deprecated since v1.8; use "storageClassName" attribute instead`
)

// DropDisabledFields 从 PVC spec 中移除被特性开关禁用的字段。
// 凡是包含 PVC spec 的资源，都应在 PrepareForCreate/PrepareForUpdate 中调用它。
func DropDisabledFields(pvcSpec, oldPVCSpec *core.PersistentVolumeClaimSpec) {
	// VolumeAttributesClass 特性关闭时清除 volumeAttributesClassName，
	// 但如果旧对象已经在用则保留，避免更新时丢数据。
	if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
		if oldPVCSpec == nil || oldPVCSpec.VolumeAttributesClassName == nil {
			pvcSpec.VolumeAttributesClassName = nil
		}
	}

	// CrossNamespaceVolumeDataSource 特性关闭时，清除带 namespace 的 dataSourceRef。
	// 该字段是旧版 dataSource 的替代，namespace 为空时属于同命名空间引用，不受此限制。
	if !utilfeature.DefaultFeatureGate.Enabled(features.CrossNamespaceVolumeDataSource) &&
		pvcSpec.DataSourceRef != nil && pvcSpec.DataSourceRef.Namespace != nil && len(*pvcSpec.DataSourceRef.Namespace) != 0 {
		if !dataSourceRefInUse(oldPVCSpec) {
			pvcSpec.DataSourceRef = nil
		}
	}
}

// EnforceDataSourceBackwardsCompatibility 在特定条件下清除 dataSource 字段，
// 以维持与旧行为的向后兼容（详见 KEP 1495）。
// 具体来说：如果这是对“无数据源 PVC”的更新或新建 PVC，且 dataSourceRef 未填写，
// 那么请求中“无效”的数据源（非 PVC、非 VolumeSnapshot）会被当作空 PVC 一样丢弃。
// 必须在 NormalizeDataSources 之前调用，确保非 PVC/VolumeSnapshot 的数据源只能通过
// dataSourceRef 字段设置，而不能通过 dataSource 字段。
func EnforceDataSourceBackwardsCompatibility(pvcSpec, oldPVCSpec *core.PersistentVolumeClaimSpec) {
	// 旧 PVC 已有数据源时直接返回：老客户端更新时可能不带 dataSourceRef，
	// 此时要保留原有数据源，即使它当初是通过已废弃的字段设置的。
	if dataSourceInUse(oldPVCSpec) {
		return
	}

	// dataSourceRef 非空说明客户端是新版：它要么想创建非空卷，要么想更新带数据源的 PVC。
	// 该数据源是否有效应由校验和 volume populator 判断，我们不能假定它期待旧行为。
	if pvcSpec.DataSourceRef != nil {
		return
	}

	// 历史上 dataSource 字段只允许 PVC 和 VolumeSnapshot，其他值一律静默丢弃。
	if !dataSourceIsPvcOrSnapshot(pvcSpec.DataSource) {
		pvcSpec.DataSource = nil
	}
}

// DropDisabledFieldsFromStatus 从 PVC status 中移除被特性开关禁用的字段，
// 处理逻辑与 DropDisabledFields 类似：旧对象已在用的字段要保留。
func DropDisabledFieldsFromStatus(pvc, oldPVC *core.PersistentVolumeClaim) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
		if oldPVC == nil || oldPVC.Status.CurrentVolumeAttributesClassName == nil {
			pvc.Status.CurrentVolumeAttributesClassName = nil
		}
		if oldPVC == nil || oldPVC.Status.ModifyVolumeStatus == nil {
			pvc.Status.ModifyVolumeStatus = nil
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.RecoverVolumeExpansionFailure) {
		// 只有旧 PVC 没有记录已分配资源时才清除，防止回滚特性开关时丢失状态。
		if !helper.ClaimContainsAllocatedResources(oldPVC) {
			pvc.Status.AllocatedResources = nil
		}
		if !helper.ClaimContainsAllocatedResourceStatus(oldPVC) {
			pvc.Status.AllocatedResourceStatuses = nil
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) {
		if oldPVC == nil || oldPVC.Status.HealthStatus == nil {
			pvc.Status.HealthStatus = nil
		}
	}
}

// dataSourceInUse 判断旧 PVC spec 中是否已经设置了任一数据源字段。
func dataSourceInUse(oldPVCSpec *core.PersistentVolumeClaimSpec) bool {
	if oldPVCSpec == nil {
		return false
	}
	if oldPVCSpec.DataSource != nil || oldPVCSpec.DataSourceRef != nil {
		return true
	}
	return false
}

// dataSourceIsPvcOrSnapshot 判断数据源是否是传统允许的两种类型：
// 核心组的 PVC，或 snapshot.storage.k8s.io 组的 VolumeSnapshot。
// APIGroup 为 nil 表示核心组，等同于空字符串。
func dataSourceIsPvcOrSnapshot(dataSource *core.TypedLocalObjectReference) bool {
	if dataSource != nil {
		apiGroup := ""
		if dataSource.APIGroup != nil {
			apiGroup = *dataSource.APIGroup
		}
		if dataSource.Kind == pvc &&
			apiGroup == "" {
			return true
		}

		if dataSource.Kind == volumeSnapshot && apiGroup == "snapshot.storage.k8s.io" {
			return true
		}
	}
	return false
}

// dataSourceRefInUse 判断旧 PVC spec 中是否已经设置了 dataSourceRef。
func dataSourceRefInUse(oldPVCSpec *core.PersistentVolumeClaimSpec) bool {
	if oldPVCSpec == nil {
		return false
	}
	if oldPVCSpec.DataSourceRef != nil {
		return true
	}
	return false
}

// NormalizeDataSources 在 DataSource 与 DataSourceRef 只有一个被显式设置时，
// 让两者内容保持一致（互相同步）。
// 用于 PVC 的创建和读取路径，不用于更新路径。
func NormalizeDataSources(pvcSpec *core.PersistentVolumeClaimSpec) {
	if pvcSpec.DataSource != nil && pvcSpec.DataSourceRef == nil {
		// 老式写法：只设置了 dataSource，补出对应的 dataSourceRef。
		pvcSpec.DataSourceRef = &core.TypedObjectReference{
			Kind: pvcSpec.DataSource.Kind,
			Name: pvcSpec.DataSource.Name,
		}
		if pvcSpec.DataSource.APIGroup != nil {
			apiGroup := *pvcSpec.DataSource.APIGroup
			pvcSpec.DataSourceRef.APIGroup = &apiGroup
		}
	} else if pvcSpec.DataSourceRef != nil && pvcSpec.DataSource == nil {
		// 新式写法：只有 dataSourceRef 且没有 namespace（同命名空间引用），补出 dataSource。
		// 跨命名空间的 dataSourceRef 不能映射到本地的 dataSource 字段。
		if pvcSpec.DataSourceRef.Namespace == nil || len(*pvcSpec.DataSourceRef.Namespace) == 0 {
			pvcSpec.DataSource = &core.TypedLocalObjectReference{
				Kind: pvcSpec.DataSourceRef.Kind,
				Name: pvcSpec.DataSourceRef.Name,
			}
			if pvcSpec.DataSourceRef.APIGroup != nil {
				apiGroup := *pvcSpec.DataSourceRef.APIGroup
				pvcSpec.DataSource.APIGroup = &apiGroup
			}
		}
	}
}

// GetWarningsForPersistentVolumeClaim 为 PVC 对象生成警告信息。
func GetWarningsForPersistentVolumeClaim(pv *core.PersistentVolumeClaim) []string {
	var warnings []string

	if pv == nil {
		return nil
	}

	// beta storage class 注解已由 spec.storageClassName 取代。
	if _, ok := pv.ObjectMeta.Annotations[core.BetaStorageClassAnnotation]; ok {
		warnings = append(warnings,
			fmt.Sprintf(
				"%s: %s",
				field.NewPath("metadata", "annotations").Key(core.BetaStorageClassAnnotation),
				deprecatedStorageClassAnnotationsMsg,
			),
		)
	}

	warnings = append(warnings, GetWarningsForPersistentVolumeClaimSpec(field.NewPath("spec"), pv.Spec)...)

	return warnings
}

// GetWarningsForPersistentVolumeClaimSpec 检查 PVC spec 中 storage 的请求值和限制值。
// 存储容量必须是整数字节，小数（毫字节）值无效。
func GetWarningsForPersistentVolumeClaimSpec(fieldPath *field.Path, pvSpec core.PersistentVolumeClaimSpec) []string {

	var warnings []string
	// Quantity 内部以毫为单位保存，对 1000 取余可检测出小数字节。
	requestValue := pvSpec.Resources.Requests[core.ResourceStorage]
	if requestValue.MilliValue()%int64(1000) != int64(0) {
		warnings = append(warnings, fmt.Sprintf(
			"%s: fractional byte value %q is invalid, must be an integer",
			fieldPath.Child("resources").Child("requests").Key(core.ResourceStorage.String()), requestValue.String()))
	}
	limitValue := pvSpec.Resources.Limits[core.ResourceStorage]
	if limitValue.MilliValue()%int64(1000) != int64(0) {
		warnings = append(warnings, fmt.Sprintf(
			"%s: fractional byte value %q is invalid, must be an integer",
			fieldPath.Child("resources").Child("limits").Key(core.ResourceStorage.String()), limitValue.String()))
	}
	return warnings
}
