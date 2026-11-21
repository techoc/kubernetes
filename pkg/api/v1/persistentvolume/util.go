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
	corev1 "k8s.io/api/core/v1"
)

// getClaimRefNamespace 返回 PV 所绑定 PVC 的命名空间；未绑定时返回空字符串。
func getClaimRefNamespace(pv *corev1.PersistentVolume) string {
	if pv.Spec.ClaimRef != nil {
		return pv.Spec.ClaimRef.Namespace
	}
	return ""
}

// Visitor 在每个对象上以命名空间和名称被调用，返回值表示是否继续访问后续对象。
type Visitor func(namespace, name string, kubeletVisible bool) (shouldContinue bool)

// skipEmptyNames 包装 Visitor，跳过空名称的条目（它们通常是未填字段而非真实引用）。
func skipEmptyNames(visitor Visitor) Visitor {
	return func(namespace, name string, kubeletVisible bool) bool {
		if len(name) == 0 {
			// 空名称跳过，继续遍历其他引用。
			return true
		}
		// 非空名称交给真正的 visitor 处理。
		return visitor(namespace, name, kubeletVisible)
	}
}

// VisitPVSecretNames 对 PV spec 引用的每个 Secret 调用 visitor。
// visitor 返回 false 会短路遍历。返回 true 表示遍历完成，false 表示被短路。
// 注意区分 kubelet 可见的 Secret 与仅 controller 可见的 Secret（如 CSI controller 用）。
func VisitPVSecretNames(pv *corev1.PersistentVolume, visitor Visitor) bool {
	visitor = skipEmptyNames(visitor)
	source := &pv.Spec.PersistentVolumeSource
	switch {
	case source.AzureFile != nil:
		if source.AzureFile.SecretNamespace != nil && len(*source.AzureFile.SecretNamespace) > 0 {
			if len(source.AzureFile.SecretName) > 0 && !visitor(*source.AzureFile.SecretNamespace, source.AzureFile.SecretName, true /* kubeletVisible */) {
				return false
			}
		} else {
			if len(source.AzureFile.SecretName) > 0 && !visitor(getClaimRefNamespace(pv), source.AzureFile.SecretName, true /* kubeletVisible */) {
				return false
			}
		}
		return true
	case source.CephFS != nil:
		if source.CephFS.SecretRef != nil {
			// 历史遗留的 PV 未显式记录 secret 命名空间，使用 claimRef 的命名空间。
			ns := getClaimRefNamespace(pv)
			if len(source.CephFS.SecretRef.Namespace) > 0 {
				// 新版对象显式设置了命名空间时以它为准。
				ns = source.CephFS.SecretRef.Namespace
			}
			if !visitor(ns, source.CephFS.SecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
	case source.Cinder != nil:
		if source.Cinder.SecretRef != nil && !visitor(source.Cinder.SecretRef.Namespace, source.Cinder.SecretRef.Name, true /* kubeletVisible */) {
			return false
		}
	case source.FlexVolume != nil:
		if source.FlexVolume.SecretRef != nil {
			// 历史遗留的 PV 未显式记录 secret 命名空间，使用 claimRef 的命名空间。
			ns := getClaimRefNamespace(pv)
			if len(source.FlexVolume.SecretRef.Namespace) > 0 {
				// 新版对象显式设置了命名空间时以它为准。
				ns = source.FlexVolume.SecretRef.Namespace
			}
			if !visitor(ns, source.FlexVolume.SecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
	case source.RBD != nil:
		if source.RBD.SecretRef != nil {
			// 历史遗留的 PV 未显式记录 secret 命名空间，使用 claimRef 的命名空间。
			ns := getClaimRefNamespace(pv)
			if len(source.RBD.SecretRef.Namespace) > 0 {
				// 新版对象显式设置了命名空间时以它为准。
				ns = source.RBD.SecretRef.Namespace
			}
			if !visitor(ns, source.RBD.SecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
	case source.ScaleIO != nil:
		if source.ScaleIO.SecretRef != nil {
			ns := getClaimRefNamespace(pv)
			if source.ScaleIO.SecretRef != nil && len(source.ScaleIO.SecretRef.Namespace) > 0 {
				ns = source.ScaleIO.SecretRef.Namespace
			}
			if !visitor(ns, source.ScaleIO.SecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
	case source.ISCSI != nil:
		if source.ISCSI.SecretRef != nil {
			// 历史遗留的 PV 未显式记录 secret 命名空间，使用 claimRef 的命名空间。
			ns := getClaimRefNamespace(pv)
			if len(source.ISCSI.SecretRef.Namespace) > 0 {
				// 新版对象显式设置了命名空间时以它为准。
				ns = source.ISCSI.SecretRef.Namespace
			}
			if !visitor(ns, source.ISCSI.SecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
	case source.StorageOS != nil:
		if source.StorageOS.SecretRef != nil && !visitor(source.StorageOS.SecretRef.Namespace, source.StorageOS.SecretRef.Name, true /* kubeletVisible */) {
			return false
		}
	case source.CSI != nil:
		if source.CSI.ControllerPublishSecretRef != nil {
			if !visitor(source.CSI.ControllerPublishSecretRef.Namespace, source.CSI.ControllerPublishSecretRef.Name, false /* kubeletVisible */) {
				return false
			}
		}
		if source.CSI.ControllerExpandSecretRef != nil {
			if !visitor(source.CSI.ControllerExpandSecretRef.Namespace, source.CSI.ControllerExpandSecretRef.Name, false /* kubeletVisible */) {
				return false
			}
		}

		if source.CSI.NodePublishSecretRef != nil {
			if !visitor(source.CSI.NodePublishSecretRef.Namespace, source.CSI.NodePublishSecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
		if source.CSI.NodeStageSecretRef != nil {
			if !visitor(source.CSI.NodeStageSecretRef.Namespace, source.CSI.NodeStageSecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
		if source.CSI.NodeExpandSecretRef != nil {
			if !visitor(source.CSI.NodeExpandSecretRef.Namespace, source.CSI.NodeExpandSecretRef.Name, true /* kubeletVisible */) {
				return false
			}
		}
	}
	return true
}
