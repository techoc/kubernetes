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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 以下类型是测试用的枚举/别名类型，模拟真实 API 中常见的字符串枚举与阶段字段。
type (
	ConditionStatus   string
	CarpConditionType string
	CarpPhase         string
	RestartPolicy     string
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1

// Carp 是容器集合，可作为创建、更新的输入，或列表、查询的输出。
type Carp struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// carp 期望行为的规格说明。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec CarpSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// carp 最近一次观测到的状态。数据可能滞后，由系统填充且只读。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status CarpStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// 中文补充：status 由系统填充、只读，且可能滞后于实际状态，因此客户端不应依赖其即时性。
type CarpStatus struct {
	// carp 的当前状态。
	// More info: http://kubernetes.io/docs/user-guide/carp-states#carp-phase
	// +optional
	Phase CarpPhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase,casttype=CarpPhase"`
	// carp 的当前服务状态。
	// More info: http://kubernetes.io/docs/user-guide/carp-states#carp-conditions
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []CarpCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,2,opt,name=conditions"`
	// 人类可读的信息，说明 carp 处于此状态的原因。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// 简短的 CamelCase 原因，说明 carp 处于此状态的原因，例如 'DiskPressure'。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`

	// 分配给 carp 的主机 IP 地址；尚未调度时为空。
	// +optional
	HostIP string `json:"hostIP,omitempty" protobuf:"bytes,5,opt,name=hostIP"`
	// 分配给 carp 的 IP 地址，至少可在集群内路由；尚未分配时为空。
	// +optional
	CarpIP string `json:"carpIP,omitempty" protobuf:"bytes,6,opt,name=carpIP"`

	// Kubelet 确认对象的 RFC 3339 日期时间，早于 Kubelet 拉取 carp 容器镜像的时间。
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty" protobuf:"bytes,7,opt,name=startTime"`

	//
	// 中文补充：listType=map 且以 a/b/c 三个键做联合唯一，说明客户端可能并发写入
	// 同一列表，server-side apply 依赖这些键做合并。
	//
	// +listType=map
	// +listMapKey=a
	// +listMapKey=b
	// +listMapKey=c
	Infos []CarpInfo `json:"infos,omitempty" protobuf:"bytes,8,rep,name=infos"`
}

// CarpCondition 描述 carp 的单个状态条件，字段语义与 core/v1 的 PodCondition 类似。
type CarpCondition struct {
	// Type 是条件类型；目前仅支持 Ready。
	// More info: http://kubernetes.io/docs/user-guide/carp-states#carp-conditions
	Type CarpConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=CarpConditionType"`
	// Status 是条件状态，可为 True、False 或 Unknown。
	// More info: http://kubernetes.io/docs/user-guide/carp-states#carp-conditions
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// 最近一次探测条件的时间。
	// +optional
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty" protobuf:"bytes,3,opt,name=lastProbeTime"`
	// 条件最近一次从一种状态变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// 条件最近一次转换的唯一单词 CamelCase 原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// 关于最近一次状态转换细节的人类可读信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// CarpInfo 是 Infos 列表的元素，a/b 为必填联合键，c 为可选第三键。
type CarpInfo struct {
	// A 是第一个 map 键。
	// +required
	A int64 `json:"a" protobuf:"bytes,1,name=a"`
	// B 是第二个 map 键。
	// +required
	B string `json:"b" protobuf:"bytes,2,name=b"`
	// C 是可选的第三个 map 键。
	// +optional
	C *string `json:"c,omitempty" protobuf:"bytes,4,opt,name=c"`

	// 每对 A 和 B 对应的数据。
	Data string `json:"data" protobuf:"bytes,3,name=data"`
}

// 中文补充：spec 表达期望状态，与 status 分离；字段多与 PodSpec 对应，用于覆盖转换/校验测试。
type CarpSpec struct {
	// carp 内容器的重启策略，可为 Always、OnFailure 或 Never，默认 Always。
	// More info: http://kubernetes.io/docs/user-guide/carp-states#restartpolicy
	// +optional
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty" protobuf:"bytes,3,opt,name=restartPolicy,casttype=RestartPolicy"`
	// carp 优雅终止所需的可选秒数，删除请求中可减小该值。必须为非负整数；
	// 零表示立即删除，nil 表示使用默认宽限期。宽限期从向 carp 内进程发送终止信号
	// 到用 kill 信号强制停止进程之间的时间。该值应长于进程预期的清理时间，默认 30 秒。
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,4,opt,name=terminationGracePeriodSeconds"`
	// 相对于 StartTime，carp 可在节点上活动的可选秒数；超过后系统会尝试将其标记为失败
	// 并终止关联容器。必须为正整数。
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" protobuf:"varint,5,opt,name=activeDeadlineSeconds"`
	// NodeSelector 是 carp 适配节点所必须满足的选择器，必须匹配节点标签才能调度到该节点。
	// More info: http://kubernetes.io/docs/user-guide/node-selection/README
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,7,rep,name=nodeSelector"`

	// ServiceAccountName 是运行此 carp 所用的 ServiceAccount 名称。
	// More info: https://kubernetes.io/docs/concepts/security/service-accounts/
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,8,opt,name=serviceAccountName"`
	// DeprecatedServiceAccount 是 ServiceAccountName 的已弃用别名。
	// Deprecated: 请改用 serviceAccountName。
	// +k8s:conversion-gen=false
	// +optional
	DeprecatedServiceAccount string `json:"deprecatedServiceAccount,omitempty" protobuf:"bytes,9,opt,name=deprecatedServiceAccount"`

	// NodeName 请求将 carp 调度到指定节点。非空时，调度器会直接调度到该节点，
	// 并假定其满足资源要求。
	// +optional
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,10,opt,name=nodeName"`
	// 请求 carp 使用主机网络命名空间，默认 false。
	// +k8s:conversion-gen=false
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty" protobuf:"varint,11,opt,name=hostNetwork"`
	// 使用主机的 pid 命名空间；可选，默认 false。
	// +k8s:conversion-gen=false
	// +optional
	HostPID bool `json:"hostPID,omitempty" protobuf:"varint,12,opt,name=hostPID"`
	// 使用主机的 ipc 命名空间；可选，默认 false。
	// +k8s:conversion-gen=false
	// +optional
	HostIPC bool `json:"hostIPC,omitempty" protobuf:"varint,13,opt,name=hostIPC"`
	// 指定 Carp 的主机名；未指定时由系统设置。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,16,opt,name=hostname"`
	// 指定后，Carp 的完整主机名为 "<hostname>.<subdomain>.<carp namespace>.svc.<cluster domain>"；
	// 未指定时完全没有域名。
	// +optional
	Subdomain string `json:"subdomain,omitempty" protobuf:"bytes,17,opt,name=subdomain"`
	// 指定 carp 使用的调度器；未指定时使用默认调度器。
	// +optional
	SchedulerName string `json:"schedulerName,omitempty" protobuf:"bytes,19,opt,name=schedulerName"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1

// CarpList 是 Carp 列表。
type CarpList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// carp 列表。
	// More info: http://kubernetes.io/docs/user-guide/carps
	Items []Carp `json:"items" protobuf:"bytes,2,rep,name=items"`
}
