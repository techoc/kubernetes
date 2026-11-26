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

type (
	// 这些别名用于验证 casttype 标签在代码生成中的行为，
	// 本身不含额外语义。
	ConditionStatus  string
	PodConditionType string
	PodPhase         string
	RestartPolicy    string
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pod 是容器的集合，既可作为输入（create、update）也可作为输出（list、get）。
// 本包是测试用的示例 API，结构刻意模仿 core/v1 的 Pod，
// 用来覆盖 apiserver 的通用机制（序列化、转换、patch），并非真实资源。
type Pod struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 期望的 Pod 行为规约。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec PodSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 最近一次观测到的 Pod 状态。
	// 该数据可能不是最新的。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status PodStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PodStatus 表示 Pod 的状态信息，status 可能滞后于系统的实际状态。
// status 由系统写入，客户端提交的 status 会被忽略，这一点在
// 存储层的策略中强制，而非依赖类型定义。
type PodStatus struct {
	// Pod 当前的条件状态。
	// 更多信息：http://kubernetes.io/docs/user-guide/pod-states#pod-phase
	// +optional
	Phase PodPhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase,casttype=PodPhase"`
	// Pod 当前的服务状态。
	// 更多信息：http://kubernetes.io/docs/user-guide/pod-states#pod-conditions
	// +optional
	Conditions []PodCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,2,rep,name=conditions"`
	// 一段人类可读的消息，说明 Pod 处于当前状态的具体原因。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// 一段简短的 CamelCase 消息，说明 Pod 处于当前状态的具体原因。
	// 例如：'DiskPressure'
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`

	// Pod 被分配到的宿主机的 IP 地址。尚未调度时为空。
	// +optional
	HostIP string `json:"hostIP,omitempty" protobuf:"bytes,5,opt,name=hostIP"`
	// 分配给 Pod 的 IP 地址。至少在集群内可路由。
	// 尚未分配时为空。
	// +optional
	PodIP string `json:"podIP,omitempty" protobuf:"bytes,6,opt,name=podIP"`

	// Kubelet 确认接收该对象的 RFC 3339 日期和时间。
	// 该时间在 Kubelet 拉取 Pod 的容器镜像之前。
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty" protobuf:"bytes,7,opt,name=startTime"`
}

// PodCondition 描述 Pod 的一个状态条件。
//
// Conditions 使用 merge 策略与 type 作为合并键，保证不同控制面组件
// 可以并发更新各自负责的条件而不互相覆盖。
type PodCondition struct {
	// 条件的类型。
	// 目前仅有 Ready。
	// 更多信息：http://kubernetes.io/docs/user-guide/pod-states#pod-conditions
	Type PodConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=PodConditionType"`
	// 条件的状态。
	// 可以取 True、False、Unknown。
	// 更多信息：http://kubernetes.io/docs/user-guide/pod-states#pod-conditions
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// 上一次探测该条件的时间。
	// +optional
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty" protobuf:"bytes,3,opt,name=lastProbeTime"`
	// 该条件从一个状态迁移到另一个状态的上一次时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// 该条件上一次迁移的原因，要求唯一、单词、CamelCase 形式。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// 说明上一次迁移详情的人类可读消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// PodSpec 是对 Pod 的描述。部分字段关闭了 conversion-gen，说明它们在内部版本中
// 不存在，转换时会丢失——测试正是用来确认这种有意的信息损失被正确处理。
type PodSpec struct {
	// 容器内所有 Pod 的重启策略。
	// 取值：Always、OnFailure、Never。
	// 默认Always。
	// 更多信息：http://kubernetes.io/docs/user-guide/pod-states#restartpolicy
	// +optional
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty" protobuf:"bytes,3,opt,name=restartPolicy,casttype=RestartPolicy"`
	// 优雅终止 Pod 的可选秒数；删除请求可将其调小。
	// 必须是非负整数；0 表示立即删除。
	// 若为 nil，则使用默认宽限期。
	// 宽限期是从向 Pod 内进程发送终止信号，到强制杀死进程之间的秒数。
	// 发送终止信号到进程被强制杀死之间的秒数。
	// 该值应大于进程的预期清理时间。
	// 默认为 30 秒。
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,4,opt,name=terminationGracePeriodSeconds"`
	// Pod 在节点上可保持活动的可选秒数，从 StartTime 起算；超过后系统会将其标记为失败并杀死相关容器。
	// 超过 StartTime 后系统会主动将其标记为失败并杀死相关容器。
	// 必须是正整数。
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" protobuf:"varint,5,opt,name=activeDeadlineSeconds"`
	// NodeSelector 是 Pod 调度到节点时必须满足的选择器。
	// 选择器必须匹配目标节点的标签。
	// 更多信息：http://kubernetes.io/docs/user-guide/node-selection/README
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,7,rep,name=nodeSelector"`

	// ServiceAccountName 是运行此 Pod 所使用的 ServiceAccount 的名称。
	// 更多信息：https://kubernetes.io/docs/concepts/security/service-accounts/
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,8,opt,name=serviceAccountName"`
	// DeprecatedServiceAccount 是 ServiceAccountName 已弃用的别名。
	// Deprecated: Use serviceAccountName instead.
	// +k8s:conversion-gen=false
	// +optional
	DeprecatedServiceAccount string `json:"serviceAccount,omitempty" protobuf:"bytes,9,opt,name=serviceAccount"`

	// NodeName 是请求将 Pod 调度到指定节点。若非空，调度器会直接将其调度到该节点（前提是资源满足）。
	// 调度器会直接把 Pod 调度到该节点（前提是资源满足）。
	// requirements.
	// +optional
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,10,opt,name=nodeName"`
	// 请求此 Pod 使用主机网络命名空间。
	// 默认为 false。
	// +k8s:conversion-gen=false
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty" protobuf:"varint,11,opt,name=hostNetwork"`
	// 使用宿主机的 pid 命名空间。
	// 可选：默认为 false。
	// +k8s:conversion-gen=false
	// +optional
	HostPID bool `json:"hostPID,omitempty" protobuf:"varint,12,opt,name=hostPID"`
	// 使用宿主机的 ipc 命名空间。
	// 可选：默认为 false。
	// +k8s:conversion-gen=false
	// +optional
	HostIPC bool `json:"hostIPC,omitempty" protobuf:"varint,13,opt,name=hostIPC"`
	// 指定 Pod 的 hostname。
	// 未指定时，Pod 的 hostname 由系统设定。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,16,opt,name=hostname"`
	// 若指定，则 Pod 的完整主机名格式为 "<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>"。
	// 未指定时，Pod 完全没有域名。
	// +optional
	Subdomain string `json:"subdomain,omitempty" protobuf:"bytes,17,opt,name=subdomain"`
	// 若指定，则由指定调度器分发该 Pod。
	// 未指定时，由默认调度器分发。
	// +optional
	SchedulerName string `json:"schedulername,omitempty" protobuf:"bytes,19,opt,name=schedulername"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodList 是 Pod 的列表。列表类型需要独立的 TypeMeta/ListMeta，以便分页与 watch 时
// 携带 resourceVersion；Items 不加 omitempty 以区分空列表与缺省字段。
type PodList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Pod 列表。
	// 更多信息：http://kubernetes.io/docs/user-guide/pods
	Items []Pod `json:"items" protobuf:"bytes,2,rep,name=items"`
}
