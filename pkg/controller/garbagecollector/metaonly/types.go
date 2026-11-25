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

package metaonly

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MetadataOnlyObject 只允许解码 JSON 数据中的 apiVersion、kind 和 metadata 字段。
// 垃圾回收器只需资源的身份与属主信息，跳过 spec/status 可大幅降低反序列化和内存开销。
// TODO：为 protobuf 启用仅解码元数据的模式。
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MetadataOnlyObject struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
}

// MetadataOnlyObjectList 只允许解码 JSON 数据中的列表 typemeta、metadata，
// 以及列表中各对象的 typemeta 和 metadata。
// 列表操作同样只关心对象身份，避免解析完整对象带来的开销。
// TODO：为 protobuf 启用仅解码元数据的模式。
//
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MetadataOnlyObjectList struct {
	metav1.TypeMeta `json:""`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []MetadataOnlyObject `json:"items"`
}
