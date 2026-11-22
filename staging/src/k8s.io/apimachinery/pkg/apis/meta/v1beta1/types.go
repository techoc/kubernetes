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

package v1beta1

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Table 是某组 API 资源的表格化表示。服务端把对象转换为便于快速浏览的
// 首选列集合。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +protobuf=false
type Table = v1.Table

// TableColumnDefinition 描述 Table 中返回的一列。
// +protobuf=false
type TableColumnDefinition = v1.TableColumnDefinition

// TableRow 是表格中的单独一行。
// +protobuf=false
type TableRow = v1.TableRow

// TableRowCondition 允许为某行附加额外状态信息。
// +protobuf=false
type TableRowCondition = v1.TableRowCondition

type RowConditionType = v1.RowConditionType

type ConditionStatus = v1.ConditionStatus

type IncludeObjectPolicy = v1.IncludeObjectPolicy

// TableOptions 是调用方请求 Table 时使用的选项。
// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TableOptions = v1.TableOptions

// PartialObjectMetadata 是任何带 ObjectMeta 对象的通用表示。它让客户端
// 无需了解具体版本，即可访问特定的 ObjectMeta 模式。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PartialObjectMetadata = v1.PartialObjectMetadata

// 重要：v1beta1 的 PartialObjectMetadataList 与 v1 的 protobuf 字段号不同，
// 因为 ListMeta 在 1.15 之前被意外遗漏。因此该类型必须保持独立于
// v1.PartialObjectMetadataList，以保留字段映射。

// PartialObjectMetadataList 是只包含元数据的对象列表。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type PartialObjectMetadataList struct {
	v1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	v1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,2,opt,name=metadata"`

	// items 包含列表中的每个条目。
	Items []v1.PartialObjectMetadata `json:"items" protobuf:"bytes,1,rep,name=items"`
}

// 这些常量是 v1 对应常量的别名，保留在此以免破坏旧引用。
const (
	RowCompleted = v1.RowCompleted

	ConditionTrue    = v1.ConditionTrue
	ConditionFalse   = v1.ConditionFalse
	ConditionUnknown = v1.ConditionUnknown

	IncludeNone     = v1.IncludeNone
	IncludeMetadata = v1.IncludeMetadata
	IncludeObject   = v1.IncludeObject
)
