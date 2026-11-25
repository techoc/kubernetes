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

package discovery

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EndpointSlice 表示实现某个服务的端点的一个子集。
// 对于给定服务可能存在多个 EndpointSlice 对象，通过
// labels 选择，这些切片必须合并起来才能得到完整的端点集合。
type EndpointSlice struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta
	// addressType 指定此 EndpointSlice 所承载地址的类型。
	// 此切片中的所有地址必须是同一类型。该字段创建后
	// 不可变。目前支持以下地址类型：
	// * IPv4：表示 IPv4 地址。
	// * IPv6：表示 IPv6 地址。
	// * FQDN：表示完全限定域名（Fully Qualified Domain Name）。[DEPRECATED]
	AddressType AddressType
	// endpoints 是此切片中唯一端点的列表，每个切片最多可包含 1000 个端点。
	// +listType=atomic
	Endpoints []Endpoint
	// ports 指定此 slice 中每个端点暴露的网络端口列表。每个端口必须有唯一名称。
	// ports 为空表示未定义端口；若端口的 port 值为 nil，则表示“所有端口”。
	// 每个 slice 最多可包含 100 个端口。
	// +optional
	// +listType=atomic
	Ports []EndpointPort
}

// AddressType 表示端点所指地址的类型。
type AddressType string

const (
	// AddressTypeIPv4 表示 IPv4 地址。
	AddressTypeIPv4 = AddressType(api.IPv4Protocol)
	// AddressTypeIPv6 表示 IPv6 地址。
	AddressTypeIPv6 = AddressType(api.IPv6Protocol)
	// AddressTypeFQDN 表示 FQDN。
	AddressTypeFQDN = AddressType("FQDN")
)

// Endpoint 表示实现某个服务的单个逻辑“后端”。
type Endpoint struct {
	// addresses 是此端点的地址。字段内容按对应 EndpointSlice 的 addressType 解释，
	// 消费方需根据自身能力处理不同类型地址。至少一个、最多 100 个地址。
	// +listType=set
	Addresses []string
	// conditions 包含端点当前状态的信息。
	Conditions EndpointConditions
	// hostname 是该端点的主机名。端点的消费方可用它来区分
	// 各个端点（例如在 DNS 名称中）。使用相同 hostname 的
	// 多个端点应被视为可互换（例如 DNS 中的多个 A 记录）。
	// 必须通过 DNS Label（RFC 1123）校验。
	// +optional
	Hostname *string
	// targetRef 是指向表示此端点的 Kubernetes 对象的引用。
	// +optional
	TargetRef *api.ObjectReference
	// deprecatedTopology 已弃用，仅为与 v1beta1 Topology 字段的往返
	// 兼容而保留。当 v1beta1 移除时，该字段也应一并移除。
	// +optional
	DeprecatedTopology map[string]string
	// nodeName 表示承载此端点的节点名称，可用于确定节点本地的端点。
	// +optional
	NodeName *string
	// zone 是此端点所在可用区的名称。
	// +optional
	Zone *string
	// hints 包含与端点应如何使用相关的信息。
	// +featureGate=TopologyAwareHints
	// +optional
	Hints *EndpointHints
}

// EndpointConditions 表示端点的当前状况。
type EndpointConditions struct {
	// ready 依据管理该端点的任何系统所持有的状态，表示此端点是否已准备好
	// 接收流量。nil 表示状态未知。在多数情况下，消费方应将未知状态
	// 理解为 ready。出于兼容性原因，对于正在终止的端点，ready 不应为
	// "true"，除非正常就绪行为被显式覆盖，例如其关联的
	// Service 设置了 publishNotReadyAddresses 标志。
	Ready *bool

	// serving 与 ready 相同，但不考虑端点的终止状态。
	// 处于终止中但仍就绪的端点应将此条件设为 true。
	// 若为 nil，消费方应参考 ready 条件。
	// +optional
	Serving *bool

	// terminating 表示此端点正在终止。nil 表示状态未知，
	// 消费方应将未知状态理解为端点未终止。
	// +optional
	Terminating *bool
}

// EndpointHints 提供描述端点应如何使用提示。
type EndpointHints struct {
	// forZones 指示在使用拓扑感知路由时，该端点应由哪些 zone 消费。
	// 最多可包含 8 个条目。
	ForZones []ForZone

	// forNodes 指示在使用拓扑感知路由时，该端点应由哪些 node 消费。
	// 最多可包含 8 个条目。
	ForNodes []ForNode
}

// ForZone 提供哪些可用区应消费此端点的信息。
type ForZone struct {
	// name 表示可用区的名称。
	Name string
}

// ForNode 提供哪些节点应消费此端点的信息。
type ForNode struct {
	// name 表示节点的名称。
	Name string
}

// EndpointPort 表示 EndpointSlice 使用的一个端口。
type EndpointPort struct {
	// name 表示此端口的名称，EndpointSlice 中所有端口名必须唯一。
	// 若 EndpointSlice 源自 Kubernetes Service，则对应 Service.ports[].name。
	// Name 必须为空字符串或通过 DNS_LABEL 校验：长度不超过 63 个字符；
	// 只能包含小写字母数字字符和 '-'；必须以字母数字字符开头和结尾。默认为空字符串。
	Name *string
	// 此端口的 IP 协议。
	// 必须是 UDP、TCP 或 SCTP。
	Protocol *corev1.Protocol
	// port 表示端点的端口号。
	// 若未指定，则端口不受限制，需由具体消费方的上下文解释。
	Port *int32
	// 此端口的应用层协议。实现方可将其作为提示，为理解的协议提供更丰富的行为。
	// 此字段遵循标准 Kubernetes 标签语法，有效值为：
	// * 无前缀的协议名 —— 保留给 IANA 标准服务名
	// （参见 RFC-6335 及 https://www.iana.org/assignments/service-names）。
	// * Kubernetes 定义的前缀名：
	// * 'kubernetes.io/h2c' —— 明文上的 HTTP/2 prior knowledge，
	//   参见 https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	// * 'kubernetes.io/ws' —— 明文 WebSocket，参见 https://www.rfc-editor.org/rfc/rfc6455
	// * 'kubernetes.io/wss' —— TLS 上的 WebSocket，参见 https://www.rfc-editor.org/rfc/rfc6455
	// * 其他协议应使用实现自定义的前缀名，例如 mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EndpointSliceList 表示端点切片（endpoint slices）的列表。
type EndpointSliceList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta
	// 端点切片（endpoint slices）列表
	Items []EndpointSlice
}
