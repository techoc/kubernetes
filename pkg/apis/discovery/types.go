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

// EndpointSlice represents a subset of the endpoints that implement a service.
// For a given service there may be multiple EndpointSlice objects, selected by
// labels, which must be joined to produce the full set of endpoints.
type EndpointSlice struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta
	// addressType specifies the type of address carried by this EndpointSlice.
	// All addresses in this slice must be the same type. This field is
	// immutable after creation. The following address types are currently
	// supported:
	// * IPv4: Represents an IPv4 Address.
	// * IPv6: Represents an IPv6 Address.
	// * FQDN: Represents a Fully Qualified Domain Name. [DEPRECATED]
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
	// AddressTypeFQDN represents a FQDN.
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
	// hostname of this endpoint. This field may be used by consumers of
	// endpoints to distinguish endpoints from each other (e.g. in DNS names).
	// Multiple endpoints which use the same hostname should be considered
	// fungible (e.g. multiple A values in DNS). Must pass DNS Label (RFC 1123)
	// 校验。
	// +optional
	Hostname *string
	// targetRef 是指向表示此端点的 Kubernetes 对象的引用。
	// +optional
	TargetRef *api.ObjectReference
	// deprecatedTopology is deprecated and only retained for round-trip
	// compatibility with v1beta1 Topology field.  When v1beta1 is removed, this
	// should be removed, too.
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
	// ready indicates that this endpoint is prepared to receive traffic,
	// according to whatever system is managing the endpoint. A nil value
	// indicates an unknown state. In most cases consumers should interpret this
	// unknown state as ready. For compatibility reasons, ready should never be
	// "true" for terminating endpoints, except when the normal readiness
	// behavior is being explicitly overridden, for example when the associated
	// Service has set the publishNotReadyAddresses flag.
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
	// forZones indicates the zone(s) this endpoint should be consumed by when
	// 使用拓扑感知路由。最多可包含 8 个条目。
	ForZones []ForZone

	// forNodes indicates the node(s) this endpoint should be consumed by when
	// 使用拓扑感知路由。最多可包含 8 个条目。
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

// EndpointPort represents a Port used by an EndpointSlice.
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
	// * 无前缀的协议名 —— 保留给 IANA 标准服务名（参见 RFC-6335 及
	// * Kubernetes 定义的前缀名：
	// * 其他协议应使用实现自定义的前缀名，例如 mycompany.com/my-custom-protocol。
	// RFC-6335 and https://www.iana.org/assignments/service-names).
	// * 'kubernetes.io/h2c' - HTTP/2 prior knowledge over cleartext as described in https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	// * 'kubernetes.io/ws'  - WebSocket over cleartext as described in https://www.rfc-editor.org/rfc/rfc6455
	// * 'kubernetes.io/wss' - WebSocket over TLS as described in https://www.rfc-editor.org/rfc/rfc6455
	// +optional
	AppProtocol *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EndpointSliceList represents a list of endpoint slices.
type EndpointSliceList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta
	// List of endpoint slices
	Items []EndpointSlice
}
