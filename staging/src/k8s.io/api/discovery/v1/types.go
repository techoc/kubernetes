/*
Copyright 2019 The Kubernetes Authors.

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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21

// EndpointSlice 表示一组服务端点。大多数 EndpointSlice 由 EndpointSlice 控制器创建，
// 用于表示 Service 对象选中的 Pod。一个 Service 可能有多个 EndpointSlice 对象，需要将它们
// 合并才能得到完整端点集合；可通过列出服务命名空间中 `kubernetes.io/service-name`
// 标签包含该服务名的 EndpointSlice 来找到该服务的所有切片。
type EndpointSlice struct {
	metav1.TypeMeta `json:""`

	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// addressType 指定此 EndpointSlice 承载的地址类型，切片中所有地址必须是同一类型。
	// 此字段创建后不可变，当前支持以下类型：
	// * IPv4：表示 IPv4 地址。
	// * IPv6：表示 IPv6 地址。
	// * FQDN：表示完全限定域名（已废弃）。
	// EndpointSlice 控制器只生成 IPv4 和 IPv6 类型的切片，kube-proxy 也只处理这两类；
	// FQDN 类型未定义任何语义。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:immutable
	AddressType AddressType `json:"addressType" protobuf:"bytes,4,rep,name=addressType"`

	// endpoints 是此切片中唯一端点的列表，每个切片最多可包含 1000 个端点。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Endpoints []Endpoint `json:"endpoints" protobuf:"bytes,2,rep,name=endpoints"`

	// ports 指定此切片中每个端点暴露的网络端口列表。每个端口名必须唯一，每个切片最多
	// 可包含 100 个端口。Service 始终至少有 1 个端口，因此由 EndpointSlice 控制器生成的
	// EndpointSlice 也至少有 1 个端口；用于其他用途的 EndpointSlice 则可以有空的端口列表。
	// +optional
	// +listType=atomic
	Ports []EndpointPort `json:"ports" protobuf:"bytes,3,rep,name=ports"`
}

// AddressType 表示端点所指地址的类型。
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
type AddressType string

const (
	// AddressTypeIPv4 表示 IPv4 地址。
	AddressTypeIPv4 = AddressType(v1.IPv4Protocol)

	// AddressTypeIPv6 表示 IPv6 地址。
	AddressTypeIPv6 = AddressType(v1.IPv6Protocol)

	// AddressTypeFQDN 表示完全限定域名。
	AddressTypeFQDN = AddressType("FQDN")
)

// Endpoint 表示实现某个服务的单个逻辑“后端”。
type Endpoint struct {
	// 此端点的地址。对于 addressType 为 "IPv4" 或 "IPv6" 的 EndpointSlice，
	// 值是规范形式的 IP 地址；其他 addressType 的语法和语义未定义。至少包含一个地址，
	// 最多 100 个。EndpointSlice 控制器生成的切片始终恰好有 1 个地址；第一个地址之后的
	// 额外地址未定义任何语义，kube-proxy 也不会查看它们。
	// +listType=set
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:maxItems=100
	Addresses []string `json:"addresses" protobuf:"bytes,1,rep,name=addresses"`

	// conditions 包含端点当前状态的信息。
	Conditions EndpointConditions `json:"conditions,omitempty" protobuf:"bytes,2,opt,name=conditions"`

	// 此端点的 hostname。端点使用方可据此相互区分（例如用于 DNS 名称）。
	// 使用相同 hostname 的多个端点应视为可互换（例如 DNS 中的多个 A 记录）。
	// 必须为小写且通过 DNS Label（RFC 1123）校验。
	// +optional
	Hostname *string `json:"hostname,omitempty" protobuf:"bytes,3,opt,name=hostname"`

	// targetRef 是指向表示此端点的 Kubernetes 对象的引用。
	// +optional
	TargetRef *v1.ObjectReference `json:"targetRef,omitempty" protobuf:"bytes,4,opt,name=targetRef"`

	// deprecatedTopology 包含 v1beta1 API 中的拓扑信息。此字段已废弃，将在 v1beta1
	// API 移除时一并移除（不早于 Kubernetes v1.24）。虽然此字段可以保存值，但无法通过
	// v1 API 写入，任何写入尝试都会被静默忽略。拓扑信息请改用 zone 和 nodeName 字段。
	// +optional
	DeprecatedTopology map[string]string `json:"deprecatedTopology,omitempty" protobuf:"bytes,5,opt,name=deprecatedTopology"`

	// nodeName 表示承载此端点的 Node 名称，可用于判断哪些端点相对某节点是本地的。
	// +optional
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,6,opt,name=nodeName"`

	// zone 是此端点所在可用区的名称。
	// +optional
	Zone *string `json:"zone,omitempty" protobuf:"bytes,7,opt,name=zone"`

	// hints 包含与端点应如何使用相关的信息。
	// +optional
	Hints *EndpointHints `json:"hints,omitempty" protobuf:"bytes,8,opt,name=hints"`
}

// EndpointConditions 表示端点的当前状况。
type EndpointConditions struct {
	// ready 表示根据管理端点的系统判断，此端点已准备好接收流量。nil 值应解释为 "true"。
	// 一般而言，端点正在服务且未终止时应标记为 ready；但某些情况下可以覆盖此行为，
	// 例如关联的 Service 设置了 publishNotReadyAddresses 标志。
	// +optional
	Ready *bool `json:"ready,omitempty" protobuf:"bytes,1,name=ready"`

	// serving 表示根据管理端点的系统判断，此端点能够接收流量。对于由 Pod 支撑的端点，
	// EndpointSlice 控制器会在 Pod 的 Ready 条件为 True 时将其标记为 serving。
	// nil 值应解释为 "true"。
	// +optional
	Serving *bool `json:"serving,omitempty" protobuf:"bytes,2,name=serving"`

	// terminating 表示此端点正在终止，nil 值应解释为 "false"。
	// +optional
	Terminating *bool `json:"terminating,omitempty" protobuf:"bytes,3,name=terminating"`
}

// EndpointHints 提供描述端点应如何使用提示。
type EndpointHints struct {
	// forZones 指示使用拓扑感知路由时，哪些可用区应消费此端点。最多可包含 8 项。
	// +listType=atomic
	ForZones []ForZone `json:"forZones,omitempty" protobuf:"bytes,1,name=forZones"`

	// forNodes 指示使用拓扑感知路由时，哪些节点应消费此端点。最多可包含 8 项。
	// +listType=atomic
	ForNodes []ForNode `json:"forNodes,omitempty" protobuf:"bytes,2,name=forNodes"`
}

// ForZone 提供哪些可用区应消费此端点的信息。
type ForZone struct {
	// name 表示可用区的名称。
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// ForNode 提供哪些节点应消费此端点的信息。
type ForNode struct {
	// name 表示节点的名称。
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// EndpointPort 表示 EndpointSlice 使用的端口。
// +structType=atomic
type EndpointPort struct {
	// name 表示此端口的名称，EndpointSlice 中所有端口名必须唯一。
	// 若 EndpointSlice 源自 Kubernetes Service，则对应 Service.ports[].name。
	// Name 必须为空字符串或通过 DNS_LABEL 校验：长度不超过 63 个字符；
	// 只能包含小写字母数字字符和 '-'；必须以字母数字字符开头和结尾。默认为空字符串。
	Name *string `json:"name,omitempty" protobuf:"bytes,1,name=name"`

	// protocol 表示此端口的 IP 协议，必须是 UDP、TCP 或 SCTP，默认为 TCP。
	Protocol *v1.Protocol `json:"protocol,omitempty" protobuf:"bytes,2,name=protocol"`

	// port 表示端点的端口号。若 EndpointSlice 源自 Kubernetes Service，
	// 此字段必须设置为服务的 target port；用于其他用途的 EndpointSlice 可以没有端口。
	Port *int32 `json:"port,omitempty" protobuf:"bytes,3,opt,name=port"`

	// 此端口的应用层协议。实现方可将其作为提示，为理解的协议提供更丰富的行为。
	// 此字段遵循标准 Kubernetes 标签语法，有效值为：
	//
	// * 无前缀的协议名 —— 保留给 IANA 标准服务名（参见 RFC-6335 及
	// https://www.iana.org/assignments/service-names）。
	//
	// * Kubernetes 定义的前缀名：
	//   * 'kubernetes.io/h2c' —— 明文上的 HTTP/2 先验知识，参见 https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  —— 明文上的 WebSocket，参见 https://www.rfc-editor.org/rfc/rfc6455
	//   * 'kubernetes.io/wss' —— TLS 上的 WebSocket，参见 https://www.rfc-editor.org/rfc/rfc6455
	//
	// * 其他协议应使用实现自定义的前缀名，例如 mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string `json:"appProtocol,omitempty" protobuf:"bytes,4,name=appProtocol"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21

// EndpointSliceList 表示端点切片的列表。
type EndpointSliceList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是端点切片的列表。
	Items []EndpointSlice `json:"items" protobuf:"bytes,2,rep,name=items"`
}
