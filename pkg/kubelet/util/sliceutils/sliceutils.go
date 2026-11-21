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

package sliceutils

import (
	v1 "k8s.io/api/core/v1"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// PodsByCreationTime 使 Pod 数组可按创建时间升序排序。
type PodsByCreationTime []*v1.Pod

// Len 返回 Pod 数量。
func (s PodsByCreationTime) Len() int {
	return len(s)
}

// Swap 交换两个 Pod 的位置。
func (s PodsByCreationTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less 按创建时间升序比较两个 Pod。
func (s PodsByCreationTime) Less(i, j int) bool {
	return s[i].CreationTimestamp.Before(&s[j].CreationTimestamp)
}

// ByImageSize 使镜像数组可按大小降序排序。
type ByImageSize []kubecontainer.Image

// Less 按镜像大小降序比较；大小相同时按 ID 降序，保证排序稳定。
func (a ByImageSize) Less(i, j int) bool {
	if a[i].Size == a[j].Size {
		return a[i].ID > a[j].ID
	}
	return a[i].Size > a[j].Size
}

// Len 返回镜像数量。
func (a ByImageSize) Len() int { return len(a) }

// Swap 交换两个镜像的位置。
func (a ByImageSize) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
