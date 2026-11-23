/*
Copyright 2020 The Kubernetes Authors.

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

package volume

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"
)

// PersistentVolumeClaimHasClass 在给定 PVC 设置了 StorageClassName 字段时返回 true。
// beta 注解优先于 spec 字段，是为了兼容旧对象：老版本把存储类写在注解里，
// 新版才用 StorageClassName，两者可能同时存在且以注解为准。
func PersistentVolumeClaimHasClass(claim *v1.PersistentVolumeClaim) bool {
	// 先读 beta 注解：存量的老对象只认这个字段，新版字段可能还不存在
	if _, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return true
	}

	if claim.Spec.StorageClassName != nil {
		return true
	}

	return false
}

// GetPersistentVolumeClaimClass 返回 StorageClassName；如果未请求存储类，则返回 ""。
// 返回空串代表未指定存储类，调用方据此走默认 StorageClass 的逻辑；
// 仍然先读 beta 注解，与 HasClass 的判定口径保持一致。
func GetPersistentVolumeClaimClass(claim *v1.PersistentVolumeClaim) string {
	// 先读 beta 注解：存量的老对象只认这个字段，新版字段可能还不存在
	if class, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	return ""
}

// GetPersistentVolumeClass 返回 StorageClassName。
// PV 侧同样兼容 beta 注解，但 spec 字段是普通字符串而非指针，
// 空串本身就表示未指定，所以不需要额外的 nil 判断。
func GetPersistentVolumeClass(volume *v1.PersistentVolume) string {
	// 先读 beta 注解：存量的老对象只认这个字段，新版字段可能还不存在
	if class, found := volume.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	return volume.Spec.StorageClassName
}

// CheckNodeAffinity 检查 PV 的节点亲和性，判断节点是否具有对应的标签，
// 确保不会挂载不属于该节点的卷。
//
// 用节点标签临时构造一个 Node 对象来复用 MatchNodeSelectorTerms，是为了跟调度器
// 用完全相同的选择器语义；两边如果各写一套匹配逻辑，迟早会出现“调度允许但挂载失败”的不一致。
func CheckNodeAffinity(pv *v1.PersistentVolume, nodeLabels map[string]string) error {
	if pv.Spec.NodeAffinity == nil {
		return nil
	}

	if pv.Spec.NodeAffinity.Required != nil {
		node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: nodeLabels}}
		terms := pv.Spec.NodeAffinity.Required
		if matches, err := corev1.MatchNodeSelectorTerms(node, terms); err != nil {
			return err
		} else if !matches {
			return fmt.Errorf("no matching NodeSelectorTerms")
		}
	}

	return nil
}
