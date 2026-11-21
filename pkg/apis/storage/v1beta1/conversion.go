/*
Copyright 2025 The Kubernetes Authors.

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

package v1beta1

import (
	storagev1beta1 "k8s.io/api/storage/v1beta1"
	conversion "k8s.io/apimachinery/pkg/conversion"
	storage "k8s.io/kubernetes/pkg/apis/storage"
)

// Convert_storage_CSINode_To_v1beta1_CSINode 是从内部版本到 v1beta1 的手动转换函数。
// v1beta1 中不存在 Status 字段，需借助该函数显式处理字段差异（由生成的转换逻辑丢弃 Status）。
func Convert_storage_CSINode_To_v1beta1_CSINode(in *storage.CSINode, out *storagev1beta1.CSINode, s conversion.Scope) error {
	return autoConvert_storage_CSINode_To_v1beta1_CSINode(in, out, s)
}
