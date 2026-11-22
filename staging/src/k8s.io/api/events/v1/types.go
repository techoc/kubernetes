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

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// Event 是对集群中某处事件的报告，通常表示系统状态发生变化。
// 事件保留时间有限，触发条件与消息可能随时间变化。事件消费者不应假定
// 具有相同 Reason 的事件时序对应稳定的触发原因，也不应假定此类事件持续存在。
// 事件应被视为仅供参考、尽力而为的补充数据。
type Event struct {
	metav1.TypeMeta `json:""`

	// metadata 为标准对象元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`

	// eventTime 为首次观察到该事件的时间，必填。
	EventTime metav1.MicroTime `json:"eventTime" protobuf:"bytes,2,opt,name=eventTime"`

	// series 为此事件所属事件序列的数据；若为单次事件则为 nil。
	// +optional
	Series *EventSeries `json:"series,omitempty" protobuf:"bytes,3,opt,name=series"`

	// reportingController 为发出此事件的控制器名称，例如 `kubernetes.io/kubelet`。
	// 新事件该字段不能为空。
	ReportingController string `json:"reportingController,omitempty" protobuf:"bytes,4,opt,name=reportingController"`

	// reportingInstance 为控制器实例 ID，例如 `kubelet-xyzf`。
	// 新事件该字段不能为空，最长 128 个字符。
	ReportingInstance string `json:"reportingInstance,omitempty" protobuf:"bytes,5,opt,name=reportingInstance"`

	// action 为针对相关对象执行或尝试执行的操作，供机器读取。
	// 新事件该字段不能为空，最长 128 个字符。
	Action string `json:"action,omitempty" protobuf:"bytes,6,name=action"`

	// reason 说明执行该操作的原因，供人阅读。
	// 新事件该字段不能为空，最长 128 个字符。
	Reason string `json:"reason,omitempty" protobuf:"bytes,7,name=reason"`

	// regarding 为此事件所涉及的对象。多数情况下它是上报控制器所操作的对象，
	// 例如 ReplicaSetController 操作 ReplicaSet，因 ReplicaSet 对象发生变化而发出此事件。
	// +optional
	Regarding corev1.ObjectReference `json:"regarding,omitempty" protobuf:"bytes,8,opt,name=regarding"`

	// related 为复杂操作相关的次要对象（可选），例如 regarding 对象触发了
	// related 对象的创建或删除。
	// +optional
	Related *corev1.ObjectReference `json:"related,omitempty" protobuf:"bytes,9,opt,name=related"`

	// note 为操作状态的人工可读描述。
	// 最大长度为 1kB，但库实现应能处理最长 64kB 的值。
	// +optional
	Note string `json:"note,omitempty" protobuf:"bytes,10,opt,name=note"`

	// type 为事件类型（Normal、Warning），未来可能新增类型。
	// 该字段供机器读取，新事件不能为空。
	Type string `json:"type,omitempty" protobuf:"bytes,11,opt,name=type"`

	// deprecatedSource 为已废弃字段，用于保持与 core.v1 Event 类型的向后兼容。
	// +optional
	DeprecatedSource corev1.EventSource `json:"deprecatedSource,omitempty" protobuf:"bytes,12,opt,name=deprecatedSource"`
	// deprecatedFirstTimestamp 为已废弃字段，用于保持与 core.v1 Event 类型的向后兼容。
	// +optional
	DeprecatedFirstTimestamp metav1.Time `json:"deprecatedFirstTimestamp,omitempty" protobuf:"bytes,13,opt,name=deprecatedFirstTimestamp"`
	// deprecatedLastTimestamp 为已废弃字段，用于保持与 core.v1 Event 类型的向后兼容。
	// +optional
	DeprecatedLastTimestamp metav1.Time `json:"deprecatedLastTimestamp,omitempty" protobuf:"bytes,14,opt,name=deprecatedLastTimestamp"`
	// deprecatedCount 为已废弃字段，用于保持与 core.v1 Event 类型的向后兼容。
	// +optional
	DeprecatedCount int32 `json:"deprecatedCount,omitempty" protobuf:"varint,15,opt,name=deprecatedCount"`
}

// EventSeries 包含事件序列的信息，即一段时间内持续发生的事情。
// 更新频率由事件上报方决定。"k8s.io/client-go/tools/events/event_broadcaster.go"
// 中的默认上报器展示了如何在心跳时更新此结构，可供自定义实现参考。
type EventSeries struct {
	// count 为截至上次心跳时该序列的发生次数。
	Count int32 `json:"count" protobuf:"varint,1,opt,name=count"`
	// lastObservedTime 为上次心跳前观察到该序列最后一个事件的时间。
	LastObservedTime metav1.MicroTime `json:"lastObservedTime" protobuf:"bytes,2,opt,name=lastObservedTime"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// EventList 是 Event 对象的列表。
type EventList struct {
	metav1.TypeMeta `json:""`
	// metadata 为标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 为 schema 对象列表。
	Items []Event `json:"items" protobuf:"bytes,2,rep,name=items"`
}
