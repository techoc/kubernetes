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

package testapigroup

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type (
	// ConditionStatus 表示某个状况的取值，语义同 core/v1.ConditionStatus。
	ConditionStatus string
	// CarpConditionType 是状况的类型标识。
	CarpConditionType string
	// CarpPhase 描述 Carp 所处的生命周期阶段。
	CarpPhase string
	// RestartPolicy 描述容器退出后的重启策略。
	RestartPolicy string
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Carp 是一组容器的集合，可作为输入（创建、更新）或输出（列表、获取）使用。
type Carp struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 carp 的行为。
	// +optional
	Spec CarpSpec

	// Status 表示 carp 的当前信息，这些数据可能不是最新的。
	// +optional
	Status CarpStatus
}

// CarpStatus 描述 carp 的状态信息。状态可能滞后于系统的实际状态。
type CarpStatus struct {
	// +optional
	Phase CarpPhase
	// +optional
	Conditions []CarpCondition
	// 一段人类可读的消息，说明 carp 处于当前状态的原因。
	// +optional
	Message string
	// 一段简短的 CamelCase 消息，说明 carp 处于当前状态的原因，例如 'DiskPressure'。
	// +optional
	Reason string

	// +optional
	HostIP string
	// +optional
	CarpIP string

	// 对象被 Kubelet 确认的时间。
	// 该时间早于 Kubelet 为 carp 拉取容器镜像的时间。
	// +optional
	StartTime *metav1.Time

	// CarpInfo 由不同客户端提供，因此使用 map 语义的列表。
	//
	// +listType=map
	// +listKey=a
	// +listKey=b
	// +listKey=c
	Infos []CarpInfo
}

// CarpCondition 描述 carp 的一项状况及其最近一次探测与状态切换的时间。
type CarpCondition struct {
	Type   CarpConditionType
	Status ConditionStatus
	// +optional
	LastProbeTime metav1.Time
	// +optional
	LastTransitionTime metav1.Time
	// +optional
	Reason string
	// +optional
	Message string
}

// CarpInfo 是 Infos 列表的元素。其 A/B/C 三个字段被声明为列表键，
// 因而该列表按这三个字段的组合来识别同一元素。
type CarpInfo struct {
	// A 是第一个 map 键。
	// +required
	A int64
	// B 是第二个 map 键。
	// +required
	B string
	// C 是第三个、可选的 map 键。
	// +optional
	C *string

	// 每个 A 与 B 组合对应的数据。
	Data string
}

// CarpSpec 是 carp 的描述。
type CarpSpec struct {
	// +optional
	RestartPolicy RestartPolicy
	// carp 优雅终止所需的可选时长（秒）。可在删除请求中缩短。
	// 取值必须为非负整数，零表示立即删除。
	// 若为 nil，则使用默认宽限期。
	// 宽限期指从向 carp 中运行的进程发送终止信号，到用 kill 信号强制停止进程之间的时长。
	// 应将该值设为长于预期的清理时间。
	// +optional
	TerminationGracePeriodSeconds *int64
	// 相对于 StartTime 的可选时长（秒），carp 可在此期间内于节点上保持活动，
	// 超过后系统会主动尝试终止 carp；取值必须为正整数。
	// +optional
	ActiveDeadlineSeconds *int64
	// NodeSelector 是一个选择器，只有其条件为真时 carp 才能被调度到该节点。
	// +optional
	NodeSelector map[string]string

	// ServiceAccountName 是运行该 carp 所用的 ServiceAccount 名称。
	// carp 可以使用该 ServiceAccount 引用的 secret。
	ServiceAccountName string

	// NodeName 请求将 carp 调度到指定节点。若非空，调度器会直接把 carp 调度到该节点，
	// 前提是其满足资源需求。
	// +optional
	NodeName string
	// 指定 Carp 的主机名。
	// 若未指定，则 carp 的主机名由系统定义。
	// +optional
	Hostname string
	// 若指定，Carp 的完整限定主机名将为 "<hostname>.<subdomain>.<carp namespace>.svc.<cluster domain>"。
	// 若未指定，carp 将没有域名。
	// +optional
	Subdomain string
	// 若指定，carp 将由指定的调度器调度；否则由默认调度器调度。
	// +optional
	SchedulerName string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CarpList 是 Carp 的列表。
type CarpList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Carp
}
