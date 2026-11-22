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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.22

// Event 是集群中某处发生事件的报告，通常表示系统中的某种状态变化。
// 事件保留时间有限，其触发条件和消息可能随时间变化。事件消费者不应依赖
// 具有给定 Reason 的事件时机来反映一致的底层触发条件，也不应依赖该类事件
// 持续存在。事件应被视为仅供参考、尽力而为的补充数据。
type Event struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`

	// eventTime 是首次观察到该事件的时间，必填。
	EventTime metav1.MicroTime `json:"eventTime" protobuf:"bytes,2,opt,name=eventTime"`

	// series 是该事件所属事件序列的数据；若为单次事件则为 nil。
	// +optional
	Series *EventSeries `json:"series,omitempty" protobuf:"bytes,3,opt,name=series"`

	// reportingController 是发出该事件的控制器名称，例如 `kubernetes.io/kubelet`。
	// 新事件中此字段不能为空。
	// +optional
	ReportingController string `json:"reportingController,omitempty" protobuf:"bytes,4,opt,name=reportingController"`

	// reportingInstance 是控制器实例的 ID，例如 `kubelet-xyzf`。
	// 新事件中此字段不能为空，且最多 128 个字符。
	// +optional
	ReportingInstance string `json:"reportingInstance,omitempty" protobuf:"bytes,5,opt,name=reportingInstance"`

	// action 是对相关对象执行（或尝试执行失败）的动作，机器可读。
	// 最多 128 个字符。
	// +optional
	Action string `json:"action,omitempty" protobuf:"bytes,6,name=action"`

	// reason 是采取该动作的原因，人类可读。
	// 最多 128 个字符。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,7,name=reason"`

	// regarding 包含该事件所涉及的对象。多数情况下是报告控制器所操作的对象，
	// 例如 ReplicaSetController 操作 ReplicaSet，事件因 ReplicaSet 对象的变化而发出。
	// +optional
	Regarding corev1.ObjectReference `json:"regarding,omitempty" protobuf:"bytes,8,opt,name=regarding"`

	// related 是更复杂动作的可选次要对象，例如 regarding 对象触发了 related 对象的创建或删除。
	// +optional
	Related *corev1.ObjectReference `json:"related,omitempty" protobuf:"bytes,9,opt,name=related"`

	// note 是对该操作状态的人类可读描述。
	// note 最大长度为 1kB，但库应能处理最大 64kB 的值。
	// +optional
	Note string `json:"note,omitempty" protobuf:"bytes,10,opt,name=note"`

	// type 是事件类型（Normal、Warning），将来可能新增类型。
	// 机器可读。
	// +optional
	Type string `json:"type,omitempty" protobuf:"bytes,11,opt,name=type"`

	// deprecatedSource 是为兼容 core.v1 Event 类型而保留的废弃字段。
	// +optional
	DeprecatedSource corev1.EventSource `json:"deprecatedSource,omitempty" protobuf:"bytes,12,opt,name=deprecatedSource"`
	// deprecatedFirstTimestamp 是为兼容 core.v1 Event 类型而保留的废弃字段。
	// +optional
	DeprecatedFirstTimestamp metav1.Time `json:"deprecatedFirstTimestamp,omitempty" protobuf:"bytes,13,opt,name=deprecatedFirstTimestamp"`
	// deprecatedLastTimestamp 是为兼容 core.v1 Event 类型而保留的废弃字段。
	// +optional
	DeprecatedLastTimestamp metav1.Time `json:"deprecatedLastTimestamp,omitempty" protobuf:"bytes,14,opt,name=deprecatedLastTimestamp"`
	// deprecatedCount 是为兼容 core.v1 Event 类型而保留的废弃字段。
	// +optional
	DeprecatedCount int32 `json:"deprecatedCount,omitempty" protobuf:"varint,15,opt,name=deprecatedCount"`
}

// EventSeries 包含事件序列的信息，即某段时间内持续发生的事情。
type EventSeries struct {
	// count 是截至上次心跳时该序列中事件发生的次数。
	Count int32 `json:"count" protobuf:"varint,1,opt,name=count"`
	// lastObservedTime 是上次心跳前最后一次观察到该序列中事件的时间。
	LastObservedTime metav1.MicroTime `json:"lastObservedTime" protobuf:"bytes,2,opt,name=lastObservedTime"`

	// +k8s:deprecated=state,protobuf=3
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.8
// +k8s:prerelease-lifecycle-gen:deprecated=1.22

// EventList 是 Event 对象的列表。
type EventList struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 schema 对象的列表。
	Items []Event `json:"items" protobuf:"bytes,2,rep,name=items"`
}
