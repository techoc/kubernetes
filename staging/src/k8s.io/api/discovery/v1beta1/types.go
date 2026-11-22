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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:removed=1.25
// +k8s:prerelease-lifecycle-gen:replacement=discovery.k8s.io,v1,EndpointSlice

// EndpointSlice 表示实现某个 service 的端点子集。
// 一个 service 可能有多个 EndpointSlice 对象，按标签选择后需合并才能得到完整端点集合。
type EndpointSlice struct {
	metav1.TypeMeta `json:""`

	// 标准对象的元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// addressType 指定此 EndpointSlice 承载的地址类型。
	// 同一 slice 中所有地址必须是同一类型，该字段创建后不可变。
	// 当前支持以下地址类型：
	// * IPv4：表示 IPv4 地址。
	// * IPv6：表示 IPv6 地址。
	// * FQDN：表示完全限定域名。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:immutable
	AddressType AddressType `json:"addressType" protobuf:"bytes,4,rep,name=addressType"`

	// endpoints 是此 slice 中唯一端点的列表，每个 slice 最多可包含 1000 个端点。
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Endpoints []Endpoint `json:"endpoints" protobuf:"bytes,2,rep,name=endpoints"`

	// ports 指定此 slice 中每个端点暴露的网络端口列表。每个端口必须有唯一名称。
	// ports 为空表示未定义端口；若端口的 port 值为 nil，则表示“所有端口”。
	// 每个 slice 最多可包含 100 个端口。
	// +optional
	// +listType=atomic
	Ports []EndpointPort `json:"ports" protobuf:"bytes,3,rep,name=ports"`
}

// AddressType 表示端点引用的地址类型。
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

// Endpoint 表示实现某个 service 的单个逻辑“后端”。
type Endpoint struct {
	// addresses 是此端点的地址。字段内容按对应 EndpointSlice 的 addressType 解释，
	// 消费方需根据自身能力处理不同类型地址。至少一个、最多 100 个地址。
	// 这些地址视为可互换，客户端可以只使用第一个元素。参见：https://issue.k8s.io/106267
	// +listType=set
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:maxItems=100
	Addresses []string `json:"addresses" protobuf:"bytes,1,rep,name=addresses"`

	// conditions 包含端点当前状态的信息。
	Conditions EndpointConditions `json:"conditions,omitempty" protobuf:"bytes,2,opt,name=conditions"`

	// hostname 是此端点的主机名，端点消费方可据此相互区分（如用于 DNS 名称）。
	// 使用相同主机名的多个端点应视为可互换（如 DNS 中的多个 A 记录）。
	// 必须为小写且通过 DNS Label（RFC 1123）校验。
	// +optional
	Hostname *string `json:"hostname,omitempty" protobuf:"bytes,3,opt,name=hostname"`

	// targetRef 是指向表示此端点的 Kubernetes 对象的引用。
	// +optional
	TargetRef *v1.ObjectReference `json:"targetRef,omitempty" protobuf:"bytes,4,opt,name=targetRef"`

	// topology 包含与端点关联的任意拓扑信息，这些键值对必须符合标签格式。
	// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels
	// 最多 16 个键值对，包括但不限于以下 well-known key：
	// * kubernetes.io/hostname：值表示端点所在节点的主机名，应与对应节点标签一致。
	// * topology.kubernetes.io/zone：值表示端点所在可用区，应与对应节点标签一致。
	// * topology.kubernetes.io/region：值表示端点所在地域，应与对应节点标签一致。
	// 该字段已弃用，将在未来的 API 版本中移除。
	// +optional
	Topology map[string]string `json:"topology,omitempty" protobuf:"bytes,5,opt,name=topology"`

	// nodeName 表示承载此端点的节点名称，可用于确定节点本地的端点。
	// +optional
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,6,opt,name=nodeName"`

	// hints 包含与端点消费方式相关的提示信息。
	// +featureGate=TopologyAwareHints
	// +optional
	Hints *EndpointHints `json:"hints,omitempty" protobuf:"bytes,7,opt,name=hints"`
}

// EndpointConditions 表示端点的当前状态。
type EndpointConditions struct {
	// ready 表示根据管理端点的系统判断，此端点已准备好接收流量。
	// nil 表示状态未知，多数情况下消费方应将未知状态视为 ready。
	// 为兼容性考虑，正在终止的端点不应将 ready 设为 "true"。
	// +optional
	Ready *bool `json:"ready,omitempty" protobuf:"bytes,1,name=ready"`

	// serving 与 ready 相同，但不考虑端点的终止状态。
	// 处于终止中但仍就绪的端点应将此条件设为 true。
	// 若为 nil，消费方应参考 ready 条件。
	// +optional
	Serving *bool `json:"serving,omitempty" protobuf:"bytes,2,name=serving"`

	// terminating 表示此端点正在终止。nil 表示状态未知，
	// 消费方应将未知状态理解为端点未终止。
	// +optional
	Terminating *bool `json:"terminating,omitempty" protobuf:"bytes,3,name=terminating"`
}

// EndpointHints 提供关于端点应如何被消费的提示。
type EndpointHints struct {
	// forZones 表示应消费此端点的可用区，用于启用拓扑感知路由。最多 8 项。
	// +listType=atomic
	ForZones []ForZone `json:"forZones,omitempty" protobuf:"bytes,1,name=forZones"`

	// forNodes 表示使用拓扑感知路由时应消费此端点的节点。最多 8 项。
	// +listType=atomic
	ForNodes []ForNode `json:"forNodes,omitempty" protobuf:"bytes,2,name=forNodes"`
}

// ForZone 提供哪些可用区应消费此端点的信息。
type ForZone struct {
	// name 表示可用区名称。
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// ForNode 提供哪些节点应消费此端点的信息。
type ForNode struct {
	// name 表示节点名称。
	Name string `json:"name" protobuf:"bytes,1,name=name"`
}

// EndpointPort 表示 EndpointSlice 使用的端口。
type EndpointPort struct {
	// name 表示此端口的名称。EndpointSlice 中所有端口必须有唯一名称。
	// 若 EndpointSlice 源自 Kubernetes service，则对应 Service.ports[].name。
	// 名称必须为空字符串或通过 DNS_LABEL 校验：
	// * 长度不超过 63 个字符。
	// * 只能包含小写字母数字字符或 '-'。
	// * 必须以字母数字字符开头和结尾。
	// 默认为空字符串。
	Name *string `json:"name,omitempty" protobuf:"bytes,1,name=name"`

	// protocol 表示此端口的 IP 协议，必须是 UDP、TCP 或 SCTP。
	// 默认为 TCP。
	Protocol *v1.Protocol `json:"protocol,omitempty" protobuf:"bytes,2,name=protocol"`

	// port 表示端点的端口号。
	// 若未指定，则端口不受限制，需由具体消费方的上下文解释。
	Port *int32 `json:"port,omitempty" protobuf:"bytes,3,opt,name=port"`

	// appProtocol 表示此端口的应用协议，遵循标准 Kubernetes 标签语法。
	// 无前缀名称保留给 IANA 标准服务名（见 RFC-6335 和 https://www.iana.org/assignments/service-names）。
	// 非标准协议应使用带前缀的名称，如 mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string `json:"appProtocol,omitempty" protobuf:"bytes,4,name=appProtocol"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.16
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:removed=1.25
// +k8s:prerelease-lifecycle-gen:replacement=discovery.k8s.io,v1,EndpointSlice

// EndpointSliceList 表示端点 slice 的列表。
type EndpointSliceList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是端点 slice 的列表。
	Items []EndpointSlice `json:"items" protobuf:"bytes,2,rep,name=items"`
}
