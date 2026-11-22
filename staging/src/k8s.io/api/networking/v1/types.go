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
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.7

// NetworkPolicy 描述一组 Pod 允许哪些网络流量。
type NetworkPolicy struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 表示此 NetworkPolicy 期望行为的规格。
	// +optional
	Spec NetworkPolicySpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 作为墓碑保留，用于说明为何 3 是预留的 protobuf 标签。
	// 该注释掉的字段应保留；将来若决定重新实现 NetworkPolicyStatus，
	// 必须使用不同的 protobuf 名称和标签！
	// Status NetworkPolicyStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PolicyType 字符串描述 NetworkPolicy 的类型。
// 该类型在 1.8 中为 beta 级别。
// +enum
type PolicyType string

const (
	// PolicyTypeIngress 是影响所选 Pod 入站流量的 NetworkPolicy。
	PolicyTypeIngress PolicyType = "Ingress"
	// PolicyTypeEgress 是影响所选 Pod 出站流量的 NetworkPolicy。
	PolicyTypeEgress PolicyType = "Egress"
)

// NetworkPolicySpec 是 NetworkPolicy 的规格。
type NetworkPolicySpec struct {
	// podSelector 选择此 NetworkPolicy 对象适用的 Pod。
	// 规则数组作用于该字段选中的所有 Pod；空选择器匹配该策略命名空间中的所有 Pod。
	// 多个网络策略可以选择同一组 Pod，此时各策略的入口规则会以叠加方式合并。
	// 此字段可选；若未指定，默认为空选择器。
	// +optional
	PodSelector metav1.LabelSelector `json:"podSelector" protobuf:"bytes,1,opt,name=podSelector"`

	// ingress 是应用于所选 Pod 的入口规则列表。
	// 满足以下任一条件即允许流量进入 Pod：没有 NetworkPolicy 选中该 Pod
	// （且集群策略也允许该流量）；流量来自该 Pod 所在的本地节点；或者流量匹配
	// 所有 podSelector 与该 Pod 匹配的 NetworkPolicy 对象中至少一条入口规则。
	// 若此字段为空，则该 NetworkPolicy 不允许任何流量（仅用于确保其选中的 Pod 默认被隔离）。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Ingress []NetworkPolicyIngressRule `json:"ingress,omitempty" protobuf:"bytes,2,rep,name=ingress"`

	// egress 是应用于所选 Pod 的出口规则列表。满足以下任一条件即允许对外流量：
	// 没有 NetworkPolicy 选中该 Pod（且集群策略也允许该流量）；或者流量匹配
	// 所有 podSelector 与该 Pod 匹配的 NetworkPolicy 对象中至少一条出口规则。
	// 若此字段为空，则该 NetworkPolicy 限制所有对外流量（仅用于确保其选中的 Pod 默认被隔离）。
	// 该字段在 1.8 中为 beta 级别。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	Egress []NetworkPolicyEgressRule `json:"egress,omitempty" protobuf:"bytes,3,rep,name=egress"`

	// policyTypes 是此 NetworkPolicy 涉及的规则类型列表。
	// 有效取值为 ["Ingress"]、["Egress"] 或 ["Ingress", "Egress"]。
	// 若未指定此字段，将根据是否存在入口或出口规则推断默认值：
	// 含 egress 段的策略视为影响出站，所有策略（无论是否含 ingress 段）视为影响入站。
	// 若只想编写仅出站的策略，必须显式指定 policyTypes 为 [ "Egress" ]。
	// 同样，若要编写明确禁止任何出站流量的策略，必须指定包含 "Egress" 的 policyTypes
	// （因为这类策略不含 egress 段，否则会默认只取 [ "Ingress" ]）。
	// 该字段在 1.8 中为 beta 级别。
	// +optional
	// +listType=atomic
	PolicyTypes []PolicyType `json:"policyTypes,omitempty" protobuf:"bytes,4,rep,name=policyTypes,casttype=PolicyType"`
}

// NetworkPolicyIngressRule 描述允许进入被 NetworkPolicySpec 的 podSelector 匹配的
// Pod 的一组特定流量。流量必须同时匹配 ports 和 from。
type NetworkPolicyIngressRule struct {
	// ports 是此规则所选 Pod 上应可访问的端口列表。列表中各项按逻辑或组合。
	// 若此字段为空或缺失，则此规则匹配所有端口（流量不受端口限制）。
	// 若此字段存在且至少包含一项，则仅当流量匹配列表中至少一个端口时才允许。
	// +optional
	// +listType=atomic
	Ports []NetworkPolicyPort `json:"ports,omitempty" protobuf:"bytes,1,rep,name=ports"`

	// from 是应能访问此规则所选 Pod 的来源列表。列表中各项按逻辑或组合。
	// 若此字段为空或缺失，则此规则匹配所有来源（流量不受来源限制）。
	// 若此字段存在且至少包含一项，则仅当流量匹配 from 列表中至少一项时才允许。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	From []NetworkPolicyPeer `json:"from,omitempty" protobuf:"bytes,2,rep,name=from"`
}

// NetworkPolicyEgressRule 描述允许从被 NetworkPolicySpec 的 podSelector 匹配的
// Pod 发出的一组特定流量。流量必须同时匹配 ports 和 to。
// 该类型在 1.8 中为 beta 级别。
type NetworkPolicyEgressRule struct {
	// ports 是对外流量的目标端口列表。
	// 列表中各项按逻辑或组合。若此字段为空或缺失，则此规则匹配所有端口（流量不受端口限制）。
	// 若此字段存在且至少包含一项，则仅当流量匹配列表中至少一个端口时才允许。
	// +optional
	// +listType=atomic
	Ports []NetworkPolicyPort `json:"ports,omitempty" protobuf:"bytes,1,rep,name=ports"`

	// to 是此规则所选 Pod 对外流量的目标列表。列表中各项按逻辑或组合。
	// 若此字段为空或缺失，则此规则匹配所有目标（流量不受目标限制）。
	// 若此字段存在且至少包含一项，则仅当流量匹配 to 列表中至少一项时才允许。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	To []NetworkPolicyPeer `json:"to,omitempty" protobuf:"bytes,2,rep,name=to"`
}

// NetworkPolicyPort 描述允许流量通过的端口。
type NetworkPolicyPort struct {
	// protocol 表示流量必须匹配的协议（TCP、UDP 或 SCTP）。
	// 若未指定，默认为 TCP。
	// +optional
	Protocol *v1.Protocol `json:"protocol,omitempty" protobuf:"bytes,1,opt,name=protocol,casttype=k8s.io/api/core/v1.Protocol"`

	// port 表示给定协议上的端口，可以是 Pod 上的数字端口或命名端口。
	// 若未提供此字段，则匹配所有端口名称和数字。
	// 若提供，则仅匹配指定协议 AND 端口上的流量。
	// +optional
	Port *intstr.IntOrString `json:"port,omitempty" protobuf:"bytes,2,opt,name=port"`

	// endPort 表示若设置，则策略应允许从 port 到 endPort（含两端）的端口范围。
	// 若未定义 port 字段，或 port 字段定义为命名（字符串）端口，则不能定义此字段。
	// endPort 必须大于或等于 port。
	// +optional
	EndPort *int32 `json:"endPort,omitempty" protobuf:"bytes,3,opt,name=endPort"`
}

// IPBlock 描述允许访问被 NetworkPolicySpec 的 podSelector 匹配的 Pod 的特定 CIDR
// （例如 "192.168.1.0/24"、"2001:db8::/64"）。except 项描述不应包含在此规则中的 CIDR。
type IPBlock struct {
	// cidr 是表示此 IPBlock 的字符串。
	// 有效示例为 "192.168.1.0/24" 或 "2001:db8::/64"。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	CIDR string `json:"cidr" protobuf:"bytes,1,name=cidr"`

	// except 是不应包含在 IPBlock 中的 CIDR 切片。
	// 有效示例为 "192.168.1.0/24" 或 "2001:db8::/64"。
	// 若 except 的值超出 cidr 范围，将被拒绝。
	// +optional
	// +listType=atomic
	Except []string `json:"except,omitempty" protobuf:"bytes,2,rep,name=except"`
}

// NetworkPolicyPeer 描述允许流量进出的一方。仅允许特定的字段组合。
type NetworkPolicyPeer struct {
	// podSelector 是选择 Pod 的标签选择器，遵循标准标签选择器语义；
	// 若存在但为空，则选择所有 Pod。
	//
	// 若同时设置了 namespaceSelector，则整个 NetworkPolicyPeer 选择
	// NamespaceSelector 所选命名空间中匹配 podSelector 的 Pod。
	// 否则选择该策略自身命名空间中匹配 podSelector 的 Pod。
	// +optional
	PodSelector *metav1.LabelSelector `json:"podSelector,omitempty" protobuf:"bytes,1,opt,name=podSelector"`

	// namespaceSelector 使用集群范围的标签选择命名空间，遵循标准标签选择器语义；
	// 若存在但为空，则选择所有命名空间。
	//
	// 若同时设置了 podSelector，则整个 NetworkPolicyPeer 选择
	// namespaceSelector 所选命名空间中匹配 podSelector 的 Pod。
	// 否则选择 namespaceSelector 所选命名空间中的所有 Pod。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,2,opt,name=namespaceSelector"`

	// ipBlock 针对特定 IPBlock 定义策略。若设置此字段，则其他字段均不能设置。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	IPBlock *IPBlock `json:"ipBlock,omitempty" protobuf:"bytes,3,rep,name=ipBlock"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// NetworkPolicyList 是 NetworkPolicy 对象的列表。
type NetworkPolicyList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 schema 对象的列表。
	Items []NetworkPolicy `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// Ingress 是允许入站连接到达后端所定义端点的一组规则。
// Ingress 可配置为向服务提供外部可访问的 URL、负载均衡流量、终止 SSL、
// 提供基于名称的虚拟主机等。
// +k8s:supportsSubresource="/status"
type Ingress struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 是 Ingress 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec IngressSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// status 是 Ingress 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status IngressStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// IngressList 是 Ingress 的集合。
type IngressList struct {
	metav1.TypeMeta `json:""`

	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 Ingress 的列表。
	Items []Ingress `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// IngressSpec 描述用户期望存在的 Ingress。
type IngressSpec struct {
	// ingressClassName 是 IngressClass 集群资源的名称。Ingress 控制器实现
	// 通过一条间接关联（控制器 -> IngressClass -> Ingress 资源）使用此字段
	// 判断自己是否应服务于该 Ingress 资源。尽管 `kubernetes.io/ingress.class`
	// 注解（一个简单的常量名）从未被正式定义，却被 Ingress 控制器广泛支持，
	// 用于在控制器与 Ingress 资源之间建立直接绑定。新创建的 Ingress 资源应优先使用此字段。
	// 不过，即使该注解已正式弃用，出于向后兼容考虑，若存在该注解，
	// Ingress 控制器仍应予以尊重。
	// +optional
	IngressClassName *string `json:"ingressClassName,omitempty" protobuf:"bytes,4,opt,name=ingressClassName"`

	// defaultBackend 是处理不匹配任何规则的请求的后端。
	// 若未指定 rules，则必须指定 defaultBackend。
	// 若未设置 defaultBackend，不匹配任何规则的请求如何处理由 Ingress 控制器决定。
	// +optional
	DefaultBackend *IngressBackend `json:"defaultBackend,omitempty" protobuf:"bytes,1,opt,name=defaultBackend"`

	// tls 表示 TLS 配置。目前 Ingress 仅支持单一 TLS 端口 443。
	// 若此列表中多项指定了不同主机，且服务于该 Ingress 的控制器支持 SNI，
	// 则这些主机将通过 SNI TLS 扩展按主机名在同一端口上复用。
	// +listType=atomic
	// +optional
	TLS []IngressTLS `json:"tls,omitempty" protobuf:"bytes,2,rep,name=tls"`

	// rules 是用于配置 Ingress 的主机规则列表。若未指定或没有规则匹配，
	// 则所有流量发往默认后端。
	// +listType=atomic
	// +optional
	Rules []IngressRule `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`
}

// IngressTLS 描述与某个 ingress 关联的传输层安全配置。
type IngressTLS struct {
	// hosts 是 TLS 证书中包含的主机列表。此列表中的值必须与 tlsSecret 中使用的
	// 名称一致；若未指定，则默认为服务于此 Ingress 的负载均衡器控制器的通配主机设置。
	// +listType=atomic
	// +optional
	Hosts []string `json:"hosts,omitempty" protobuf:"bytes,1,rep,name=hosts"`

	// secretName 是用于在 443 端口终止 TLS 流量的 Secret 名称。
	// 此字段可选，以允许仅基于 SNI 主机名进行 TLS 路由。
	// 若监听器中的 SNI 主机与 IngressRule 使用的 "Host" 头字段冲突，
	// 则 SNI 主机用于终止 TLS，"Host" 头的值用于路由。
	// +optional
	SecretName string `json:"secretName,omitempty" protobuf:"bytes,2,opt,name=secretName"`
}

// IngressStatus 描述 Ingress 的当前状态。
type IngressStatus struct {
	// loadBalancer 包含负载均衡器的当前状态。
	// +optional
	LoadBalancer IngressLoadBalancerStatus `json:"loadBalancer,omitempty" protobuf:"bytes,1,opt,name=loadBalancer"`
}

// IngressLoadBalancerStatus 表示负载均衡器的状态。
type IngressLoadBalancerStatus struct {
	// ingress 是包含该负载均衡器各入口点的列表。
	// +optional
	// +listType=atomic
	Ingress []IngressLoadBalancerIngress `json:"ingress,omitempty" protobuf:"bytes,1,rep,name=ingress"`
}

// IngressLoadBalancerIngress 表示负载均衡器某个入口点的状态。
type IngressLoadBalancerIngress struct {
	// ip 针对基于 IP 的负载均衡器入口点设置。
	// +optional
	IP string `json:"ip,omitempty" protobuf:"bytes,1,opt,name=ip"`

	// hostname 针对基于 DNS 的负载均衡器入口点设置。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,2,opt,name=hostname"`

	// ports 提供此负载均衡器暴露的端口信息。
	// +listType=atomic
	// +optional
	Ports []IngressPortStatus `json:"ports,omitempty" protobuf:"bytes,4,rep,name=ports"`
}

// IngressPortStatus 表示服务端口的错误状况。
type IngressPortStatus struct {
	// port 是入口端口号。
	Port int32 `json:"port" protobuf:"varint,1,opt,name=port"`

	// protocol 是入口端口的协议。
	// 支持的值为："TCP"、"UDP"、"SCTP"。
	Protocol v1.Protocol `json:"protocol" protobuf:"bytes,2,opt,name=protocol,casttype=Protocol"`

	// error 用于记录服务端口的问题。
	// 错误格式必须遵守以下规则：
	// - 内置错误值应在此文件中指定，且使用 CamelCase 名称；
	// - 云提供商特定的错误值，其名称必须符合 foo.example.com/CamelCase 格式。
	// ---
	// 其匹配的正则为 (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// +optional
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Error *string `json:"error,omitempty" protobuf:"bytes,3,opt,name=error"`
}

// IngressRule 表示将指定主机下的路径映射到相关后端服务的规则。
// 入站请求先检查主机是否匹配，再路由到与匹配的 IngressRuleValue 关联的后端。
type IngressRule struct {
	// host 是 RFC 3986 所定义的网络主机的完全限定域名。
	// 注意，与 RFC 3986 所定义 URI 的 "host" 部分有以下差异：
	// 1. 不允许使用 IP。目前 IngressRuleValue 只能作用于父级 Ingress 的 Spec 中的 IP。
	// 2. 不遵守 `:` 分隔符，因为不允许端口。
	//	 目前 Ingress 的端口隐式取 http 的 :80 和 https 的 :443。
	// 这两点将来都可能变化。
	// 入站请求先与 host 匹配，再匹配 IngressRuleValue。
	// 若未指定 host，则 Ingress 完全按指定的 IngressRuleValue 路由所有流量。
	//
	// host 可以是 "precise"（不带网络主机结尾点的域名，例如 "foo.bar.com"），
	// 也可以是 "wildcard"（带单个通配标签前缀的域名，例如 "*.foo.com"）。
	// 通配符 '*' 必须单独作为第一个 DNS 标签出现，且只匹配单个标签。
	// 不能只有通配标签本身（例如 Host == "*"）。
	// 请求按以下方式与 Host 字段匹配：
	// 1. 若 host 为精确值，当 http host 头等于 Host 时请求匹配此规则。
	// 2. 若 host 为通配符，当 http host 头等于该通配符规则的后缀（去掉第一个标签）时请求匹配此规则。
	// +optional
	Host string `json:"host,omitempty" protobuf:"bytes,1,opt,name=host"`
	// IngressRuleValue 表示此 IngressRule 路由请求的规则。
	// 若未指定，规则默认为 http 全捕获。是将仅匹配主机的流量还是所有流量发往默认后端，
	// 由服务于该 Ingress 的控制器决定。Http 是目前唯一支持的 IngressRuleValue。
	// +optional
	IngressRuleValue `json:"" protobuf:"bytes,2,opt,name=ingressRuleValue"`
}

// IngressRuleValue 表示作用于入站请求的规则。若规则满足，请求被路由到指定后端。
// 目前不允许在单个 Ingress 中混用不同类型的规则，因此以下各项必须且只能设置其一。
type IngressRuleValue struct {
	// http 是 HTTP IngressRuleValue，包含一组 http 选择器。
	// +optional
	HTTP *HTTPIngressRuleValue `json:"http,omitempty" protobuf:"bytes,1,opt,name=http"`
}

// HTTPIngressRuleValue 是指向后端的一组 http 选择器。
// 在示例 http://<host>/<path>?<searchpart> -> backend 中，URL 的各部分对应 RFC 3986，
// 此资源用于匹配最后一个 '/' 之后、第一个 '?' 或 '#' 之前的全部内容。
type HTTPIngressRuleValue struct {
	// paths 是将请求映射到后端的路径集合。
	// +listType=atomic
	Paths []HTTPIngressPath `json:"paths" protobuf:"bytes,1,rep,name=paths"`
}

// PathType 表示 HTTPIngressPath 所指路径的类型。
// +enum
type PathType string

const (
	// PathTypeExact 区分大小写地精确匹配 URL 路径。
	PathTypeExact = PathType("Exact")

	// PathTypePrefix 按以 '/' 分隔的 URL 路径前缀匹配。匹配区分大小写，
	// 且逐路径元素进行。路径元素指路径经 '/' 分隔符拆分后的标签列表。
	// 若请求路径的每个元素都以路径 p 的对应元素为前缀，则该请求匹配路径 p。
	// 注意，若路径的最后一个元素只是请求路径最后一个元素的子串，则不算匹配
	// （例如 /foo/bar 匹配 /foo/bar/baz，但不匹配 /foo/barbaz）。
	// 若 Ingress spec 中存在多个匹配路径，优先取最长的匹配路径。
	// 示例：
	// - /foo/bar 不匹配发往 /foo/barbaz 的请求
	// - /foo/bar 匹配发往 /foo/bar 和 /foo/bar/baz 的请求
	// - /foo 和 /foo/ 都匹配发往 /foo 和 /foo/ 的请求；若两者都出现在 Ingress spec 中，
	//   优先取最长的匹配路径 /foo/。
	PathTypePrefix = PathType("Prefix")

	// PathTypeImplementationSpecific 的匹配方式由 IngressClass 决定。
	// 实现可将其作为独立的 PathType，也可将其等同于 Prefix 或 Exact 类型。
	PathTypeImplementationSpecific = PathType("ImplementationSpecific")
)

// HTTPIngressPath 将路径与后端关联。匹配该路径的入站 URL 被转发到相应后端。
type HTTPIngressPath struct {
	// path 与入站请求的路径匹配。目前它可以包含 RFC 3986 所定义 URL 常规 "path"
	// 部分不允许的字符。路径必须以 '/' 开头，且当 PathType 取 "Exact" 或 "Prefix" 时必须提供。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`

	// pathType 决定路径匹配的解释方式。PathType 可取以下值之一：
	// * Exact：精确匹配 URL 路径。
	// * Prefix：按以 '/' 分隔的 URL 路径前缀匹配，逐路径元素进行。
	//   路径元素指路径经 '/' 分隔符拆分后的标签列表。若请求路径的每个元素
	//   都以路径 p 的对应元素为前缀，则该请求匹配路径 p。注意，若路径的最后一个元素
	//   只是请求路径最后一个元素的子串，则不算匹配（例如 /foo/bar 匹配 /foo/bar/baz，
	//   但不匹配 /foo/barbaz）。
	// * ImplementationSpecific：路径匹配的解释方式由 IngressClass 决定。
	//   实现可将其作为独立的 PathType，也可将其等同于 Prefix 或 Exact 类型。
	// 实现必须支持所有路径类型。
	PathType *PathType `json:"pathType" protobuf:"bytes,3,opt,name=pathType"`

	// backend 定义流量将被转发到的所引用服务端点。
	Backend IngressBackend `json:"backend" protobuf:"bytes,2,opt,name=backend"`
}

// IngressBackend 描述给定服务和端口的所有端点。
type IngressBackend struct {
	// service 将某服务引用为后端。
	// 此设置与 "Resource" 互斥。
	// +optional
	Service *IngressServiceBackend `json:"service,omitempty" protobuf:"bytes,4,opt,name=service"`

	// resource 是指向 Ingress 对象所在命名空间中另一 Kubernetes 资源的 ObjectRef。
	// 若指定了 resource，则不得指定 service.Name 和 service.Port。
	// 此设置与 "Service" 互斥。
	// +optional
	Resource *v1.TypedLocalObjectReference `json:"resource,omitempty" protobuf:"bytes,3,opt,name=resource"`
}

// IngressServiceBackend 将 Kubernetes Service 引用为后端。
type IngressServiceBackend struct {
	// name 是所引用的服务。该服务必须与 Ingress 对象位于同一命名空间。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// port 是所引用服务的端口。IngressServiceBackend 必须提供端口名称或端口号。
	Port ServiceBackendPort `json:"port,omitempty" protobuf:"bytes,2,opt,name=port"`
}

// ServiceBackendPort 是被引用的服务端口。
// +structType=atomic
type ServiceBackendPort struct {
	// name 是 Service 上的端口名称。
	// 此设置与 "Number" 互斥。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// number 是 Service 上的数字端口号（例如 80）。
	// 此设置与 "Name" 互斥。
	// +optional
	Number int32 `json:"number,omitempty" protobuf:"bytes,2,opt,name=number"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// IngressClass 表示 Ingress 的类别（class），由 Ingress Spec 引用。
// 可使用 `ingressclass.kubernetes.io/is-default-class` 注解表明某个 IngressClass 应被视为默认。
// 当且仅当一个 IngressClass 资源将该注解设为 true 时，未指定类别的新 Ingress 资源
// 会被分配此默认类别。
type IngressClass struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 是 IngressClass 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec IngressClassSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// IngressClassSpec 提供某个 Ingress 类别的信息。
type IngressClassSpec struct {
	// controller 指应处理此类别的控制器名称。这允许同一控制器管理不同的"风格"，
	// 例如同一实现控制器可以使用不同参数。该字段应指定为带域名前缀的路径，
	// 长度不超过 250 个字符，例如 "acme.io/ingress-controller"。此字段不可变。
	Controller string `json:"controller,omitempty" protobuf:"bytes,1,opt,name=controller"`

	// parameters 指向一个自定义资源，其中包含该控制器的额外配置。
	// 若控制器不需要额外参数，则可省略。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Parameters *IngressClassParametersReference `json:"parameters,omitempty" protobuf:"bytes,2,opt,name=parameters"`
}

const (
	// IngressClassParametersReferenceScopeNamespace 表示所引用的 Parameters 资源是命名空间范围的。
	IngressClassParametersReferenceScopeNamespace = "Namespace"
	// IngressClassParametersReferenceScopeCluster 表示所引用的 Parameters 资源是集群范围的。
	IngressClassParametersReferenceScopeCluster = "Cluster"
)

// IngressClassParametersReference 标识一个 API 对象，可用于指定集群范围或命名空间范围的资源。
type IngressClassParametersReference struct {
	// apiGroup 是被引用资源所属的组。若未指定 APIGroup，则指定的 Kind 必须属于核心 API 组。
	// 对于其他任何第三方类型，APIGroup 为必填。
	// +optional
	APIGroup *string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=aPIGroup"`

	// kind 是被引用资源的类型。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`

	// name 是被引用资源的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`

	// scope 表示引用的是集群范围还是命名空间范围的资源。
	// 可取 "Cluster"（默认）或 "Namespace"。
	// +optional
	Scope *string `json:"scope" protobuf:"bytes,4,opt,name=scope"`

	// namespace 是被引用资源所在的命名空间。当 scope 为 "Namespace" 时此字段必填，
	// 当 scope 为 "Cluster" 时必须不设置。
	// +optional
	Namespace *string `json:"namespace,omitempty" protobuf:"bytes,5,opt,name=namespace"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19

// IngressClassList 是 IngressClass 的集合。
type IngressClassList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 IngressClass 的列表。
	Items []IngressClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// IPAddress 表示单一 IP 家族中的单个 IP。该对象供操作 IP 地址的 API 使用，
// Service 核心 API 使用它分配 IP 地址。
// IP 地址可以有多种表示形式；为保证 IP 的唯一性，对象名称采用 IP 的规范格式：
// IPv4 为四个以点分隔的十进制数字并去掉前导零，IPv6 采用 RFC 5952 定义的表示形式。
// 有效：192.168.1.5 或 2001:db8::1 或 2001:db8:aaaa:bbbb:cccc:dddd:eeee:1
// 无效：10.01.2.3 或 2001:db8:0:0:0::1
type IPAddress struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 是 IPAddress 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +required
	Spec IPAddressSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// IPAddressSpec 描述 IP 地址的属性。
type IPAddressSpec struct {
	// parentRef 指向 IPAddress 所附属的资源。
	// IPAddress 必须引用一个父对象。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:immutable
	ParentRef *ParentReference `json:"parentRef,omitempty" protobuf:"bytes,1,opt,name=parentRef"`
}

// ParentReference 描述对父对象的引用。
type ParentReference struct {
	// group 是被引用对象所属的组。
	// +optional
	Group string `json:"group,omitempty" protobuf:"bytes,1,opt,name=group"`
	// resource 是被引用对象的资源类型。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Resource string `json:"resource,omitempty" protobuf:"bytes,2,opt,name=resource"`
	// namespace 是被引用对象所在的命名空间。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,3,opt,name=namespace"`
	// name 是被引用对象的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Name string `json:"name,omitempty" protobuf:"bytes,4,opt,name=name"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// IPAddressList 包含 IPAddress 的列表。
type IPAddressList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是 IPAddress 的列表。
	Items []IPAddress `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ServiceCIDR 以 CIDR 格式定义一段 IP 地址范围（例如 192.168.0.0/24 或 2001:db2::/64），
// 该范围用于为 Service 对象分配 ClusterIP。
// +k8s:supportsSubresource="/status"
type ServiceCIDR struct {
	metav1.TypeMeta `json:""`
	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// spec 是 ServiceCIDR 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ServiceCIDRSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
	// status 表示 ServiceCIDR 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ServiceCIDRStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ServiceCIDRSpec 定义用户希望用于为 Service 分配 ClusterIP 的 CIDR。
type ServiceCIDRSpec struct {
	// cidrs 以 CIDR 表示法定义用于分配 Service 集群 IP 的 IP 块
	// （例如 "192.168.0.0/24" 或 "2001:db8::/64"）。最多允许两个 CIDR，每个 IP 家族一个。
	// 此字段不可变。
	// +optional
	// +listType=atomic
	CIDRs []string `json:"cidrs,omitempty" protobuf:"bytes,1,opt,name=cidrs"`
}

const (
	// ServiceCIDRConditionReady 表示 ServiceCIDR 已就绪，
	// 可供 apiserver 用于为 Service 分配 ClusterIP。
	ServiceCIDRConditionReady = "Ready"
	// ServiceCIDRReasonTerminating 表示 ServiceCIDR 因正在被删除而未就绪。
	ServiceCIDRReasonTerminating = "Terminating"
)

// ServiceCIDRStatus 描述 ServiceCIDR 的当前状态。
type ServiceCIDRStatus struct {
	// conditions 包含描述 ServiceCIDR 状态的 metav1.Condition 数组。
	// 当前服务状态。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:opaqueType
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ServiceCIDRList 包含 ServiceCIDR 对象的列表。
type ServiceCIDRList struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是 ServiceCIDR 的列表。
	Items []ServiceCIDR `json:"items" protobuf:"bytes,2,rep,name=items"`
}
