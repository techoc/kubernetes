/*
Copyright 2016 The Kubernetes Authors.

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

package util

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// IsDefaultStorageClassAnnotation 是把某个 StorageClass 标记为默认类的注解键。
const IsDefaultStorageClassAnnotation = "storageclass.kubernetes.io/is-default-class"

// BetaIsDefaultStorageClassAnnotation 是 IsDefaultStorageClassAnnotation 的 beta 版本。
// TODO: 不再使用时移除 Beta 版本。
const BetaIsDefaultStorageClassAnnotation = "storageclass.beta.kubernetes.io/is-default-class"

// IsDefaultAnnotation 判断对象是否被标记为默认 StorageClass，兼容 GA 与 beta 两个版本的注解。
// TODO: 不再需要时移除对 Beta 注解的判断。
func IsDefaultAnnotation(obj metav1.ObjectMeta) bool {
	if obj.Annotations[IsDefaultStorageClassAnnotation] == "true" {
		return true
	}
	if obj.Annotations[BetaIsDefaultStorageClassAnnotation] == "true" {
		return true
	}

	return false
}
