/*
Copyright 2018 The Kubernetes Authors.

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

package storage

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
)

// TODO(yue9944882): 复制到 k/api 后删除这个辅助包。

// 表示把某个 StorageClass 标记为默认类的注解。
const IsDefaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"

// 是 IsDefaultStorageClassAnnotation 的 beta 版本。
const BetaIsDefaultStorageClassAnnotation = "storageclass.beta.kubernetes.io/is-default-class"

// 注解已设置时返回易读的 Yes/No 字符串。
func IsDefaultAnnotationText(obj metav1.ObjectMeta) string {
	if obj.Annotations[IsDefaultStorageClassAnnotation] == "true" {
		return "Yes"
	}
	if obj.Annotations[BetaIsDefaultStorageClassAnnotation] == "true" {
		return "Yes"
	}

	return "No"
}

// 把访问模式数组转换为字符串。
// 模式出现时顺序固定为 RWO、ROX、RWX、RWOP。
func GetAccessModesAsString(modes []v1.PersistentVolumeAccessMode) string {
	modes = removeDuplicateAccessModes(modes)
	modesStr := []string{}
	if ContainsAccessMode(modes, v1.ReadWriteOnce) {
		modesStr = append(modesStr, "RWO")
	}
	if ContainsAccessMode(modes, v1.ReadOnlyMany) {
		modesStr = append(modesStr, "ROX")
	}
	if ContainsAccessMode(modes, v1.ReadWriteMany) {
		modesStr = append(modesStr, "RWX")
	}
	if ContainsAccessMode(modes, v1.ReadWriteOncePod) {
		modesStr = append(modesStr, "RWOP")
	}
	return strings.Join(modesStr, ",")
}

// 去掉访问模式数组中的重复项。
func removeDuplicateAccessModes(modes []v1.PersistentVolumeAccessMode) []v1.PersistentVolumeAccessMode {
	accessModes := []v1.PersistentVolumeAccessMode{}
	for _, m := range modes {
		if !ContainsAccessMode(accessModes, m) {
			accessModes = append(accessModes, m)
		}
	}
	return accessModes
}

func ContainsAccessMode(modes []v1.PersistentVolumeAccessMode, mode v1.PersistentVolumeAccessMode) bool {
	for _, m := range modes {
		if m == mode {
			return true
		}
	}
	return false
}

// 返回 StorageClassName。
func GetPersistentVolumeClass(volume *v1.PersistentVolume) string {
	// 优先读取 beta 注解。
	if class, found := volume.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	return volume.Spec.StorageClassName
}

// 返回 StorageClassName；未请求存储类时
// 返回 ""。
func GetPersistentVolumeClaimClass(claim *v1.PersistentVolumeClaim) string {
	// 优先读取 beta 注解。
	if class, found := claim.Annotations[v1.BetaStorageClassAnnotation]; found {
		return class
	}

	if claim.Spec.StorageClassName != nil {
		return *claim.Spec.StorageClassName
	}

	return ""
}
