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

package persistentvolume

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	nodeapi "k8s.io/kubernetes/pkg/api/node"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/features"
)

// DropDisabledSpecFields 从 PV spec 中移除被特性开关禁用的字段。
// 凡是包含 PV spec 的资源，都应在 PrepareForCreate/PrepareForUpdate 中调用它。
func DropDisabledSpecFields(pvSpec *api.PersistentVolumeSpec, oldPVSpec *api.PersistentVolumeSpec) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
		// 仅当旧对象没有使用该字段时才清除，避免更新时丢失已有配置。
		if oldPVSpec == nil || oldPVSpec.VolumeAttributesClassName == nil {
			pvSpec.VolumeAttributesClassName = nil
		}
	}
}

// GetWarningsForPersistentVolume 为 PV 对象生成警告信息。
func GetWarningsForPersistentVolume(pv *api.PersistentVolume) []string {
	if pv == nil {
		return nil
	}
	// 顶层 PV 没有传入 fieldPath，函数内部会按需构造；
	// spec 与 metadata 的警告统一由同一个函数处理，便于 PVC 等其他资源复用。
	return warningsForPersistentVolumeSpecAndMeta(nil, &pv.Spec, &pv.ObjectMeta)
}

// deprecatedAnnotations 是 PV 上已弃用的注解及其替代说明。
var deprecatedAnnotations = []struct {
	key     string
	message string
}{
	{
		key:     `volume.beta.kubernetes.io/storage-class`,
		message: `deprecated since v1.8; use "storageClassName" attribute instead`,
	},
	{
		key:     `volume.beta.kubernetes.io/mount-options`,
		message: `deprecated since v1.31; use "mountOptions" attribute instead`,
	},
}

// warningsForPersistentVolumeSpecAndMeta 对 PV 的 spec 和 metadata 执行弃用检查。
// fieldPath 为 nil 时，各检查点会用 field.NewPath 从对应字段名开始构造路径。
func warningsForPersistentVolumeSpecAndMeta(fieldPath *field.Path, pvSpec *api.PersistentVolumeSpec, pvMeta *metav1.ObjectMeta) []string {
	var warnings []string

	// 检查已弃用的注解：只要注解存在就提示，不关心其值。
	for _, deprecated := range deprecatedAnnotations {
		if _, exists := pvMeta.Annotations[deprecated.key]; exists {
			warnings = append(warnings, fmt.Sprintf("%s: %s", fieldPath.Child("metadata", "annotations").Key(deprecated.key), deprecated.message))
		}
	}

	// Recycle 回收策略已弃用，推荐改用动态供给。
	if pvSpec.PersistentVolumeReclaimPolicy == api.PersistentVolumeReclaimRecycle {
		warnings = append(warnings, fmt.Sprintf("%s: The Recycle reclaim policy is deprecated. Instead, the recommended approach is to use dynamic provisioning.", fieldPath.Child("spec", "persistentVolumeReclaimPolicy")))
	}

	// nodeAffinity 中可能引用了弃用的节点标签，交给 node 包的公共逻辑检查。
	if pvSpec.NodeAffinity != nil && pvSpec.NodeAffinity.Required != nil {
		termFldPath := fieldPath.Child("spec", "nodeAffinity", "required", "nodeSelectorTerms")
		for i, term := range pvSpec.NodeAffinity.Required.NodeSelectorTerms {
			// false 表示不检查 label value 的合法性，只检查 key 是否弃用。
			warnings = append(warnings, nodeapi.GetWarningsForNodeSelectorTerm(term, false, termFldPath.Index(i))...)
		}
	}
	// 逐个检查已经弃用或即将失效的 volume plugin。
	if pvSpec.CephFS != nil {
		warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.28, non-functional in v1.31+", fieldPath.Child("spec", "cephfs")))
	}
	if pvSpec.PhotonPersistentDisk != nil {
		warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.11, non-functional in v1.16+", fieldPath.Child("spec", "photonPersistentDisk")))
	}
	if pvSpec.ScaleIO != nil {
		warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.16, non-functional in v1.22+", fieldPath.Child("spec", "scaleIO")))
	}
	if pvSpec.StorageOS != nil {
		warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.22, non-functional in v1.25+", fieldPath.Child("spec", "storageOS")))
	}
	if pvSpec.Glusterfs != nil {
		warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.25, non-functional in v1.26+", fieldPath.Child("spec", "glusterfs")))
	}
	if pvSpec.RBD != nil {
		warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.28, non-functional in v1.31+", fieldPath.Child("spec", "rbd")))
	}
	return warnings
}
