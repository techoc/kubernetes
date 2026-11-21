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

package networking

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkPolicy 描述一组 Pod 允许哪些网络流量。
type NetworkPolicy struct {
	metav1.TypeMeta

	// +optional
	metav1.ObjectMeta

	// spec 表示本 NetworkPolicy 的期望行为规格。
	// +optional
	Spec NetworkPolicySpec
}

// PolicyType 描述 NetworkPolicy 的类型。
// 该类型自 1.8 起为 beta 级别。
type PolicyType string

const (
	// PolicyTypeIngress 表示影响被选中 Pod 入站流量的 NetworkPolicy。
	PolicyTypeIngress PolicyType = "Ingress"
	// PolicyTypeEgress 表示影响被选中 Pod 出站流量的 NetworkPolicy。
	PolicyTypeEgress PolicyType = "Egress"
)

// NetworkPolicySpec 定义 NetworkPolicy 的具体规格。
type NetworkPolicySpec struct {
	// podSelector 选择本 NetworkPolicy 适用的 Pod。规则数组作用于该字段选中的所有 Pod；
	// 空选择器匹配策略所在命名空间中的全部 Pod。
	// 多个 NetworkPolicy 可以选择同一组 Pod，此时各策略的 ingress 规则会叠加生效。
	// 该字段可选；未指定时默认为空选择器。
	// +optional
	PodSelector metav1.LabelSelector

	// ingress 是作用于被选中 Pod 的入站规则列表。
	// 当没有 NetworkPolicy 选中该 Pod（且集群策略允许该流量），或流量来源是该 Pod 所在节点，
	// 或流量匹配 podSelector 选中该 Pod 的所有 NetworkPolicy 中至少一条 ingress 规则时，
	// 流量才被允许。该字段为空表示此 NetworkPolicy 不允许任何入站流量
	// （此时它仅用于确保其选中的 Pod 默认被隔离）。
	// +optional
	Ingress []NetworkPolicyIngressRule

	// egress 是作用于被选中 Pod 的出站规则列表。
	// 当没有 NetworkPolicy 选中该 Pod（且集群策略允许该流量），或流量匹配 podSelector 选中
	// 该 Pod 的所有 NetworkPolicy 中至少一条 egress 规则时，出站流量才被允许。
	// 该字段为空表示此 NetworkPolicy 限制所有出站流量
	// （此时它仅用于确保其选中的 Pod 默认被隔离）。
	// 该类型自 1.8 起为 beta 级别。
	// +optional
	Egress []NetworkPolicyEgressRule

	// policyTypes 是 NetworkPolicy 涉及的规则类型列表。
	// 合法取值为 ["Ingress"]、["Egress"] 或 ["Ingress", "Egress"]。
	// 未指定时，会根据 ingress/egress 规则是否存在推导默认值：含 egress 段的策略被认为影响 egress，
	// 而所有策略（无论是否含 ingress 段）都被认为影响 ingress。
	// 如需编写仅控制 egress 的策略，必须显式指定 policyTypes 为 [ "Egress" ]。
	// 同理，如需编写禁止所有 egress 的策略，也必须显式在 policyTypes 中包含 "Egress"
	// （因为这类策略不含 egress 段，否则会被默认推导为仅 [ "Ingress" ]）。
	// 该类型自 1.8 起为 beta 级别。
	// +optional
	PolicyTypes []PolicyType
}

// NetworkPolicyIngressRule 描述一组允许访问 podSelector 选中 Pod 的流量。流量必须同时匹配 ports 和 from。
type NetworkPolicyIngressRule struct {
	// ports 是本规则选中的 Pod 上应开放的端口列表，列表各项按逻辑“或”组合。
	// 该字段为空或缺失时，规则匹配所有端口（不按端口限制流量）；
	// 存在且至少有一项时，只有流量匹配列表中某个端口才被允许。
	// +optional
	Ports []NetworkPolicyPort

	// from 是可访问本规则选中 Pod 的来源列表，各项按逻辑“或”组合。
	// 该字段为空或缺失时，规则匹配所有来源（不按来源限制流量）；
	// 存在且至少有一项时，只有流量匹配 from 列表中某一项才被允许。
	// +optional
	From []NetworkPolicyPeer
}

// NetworkPolicyEgressRule 描述一组允许从 podSelector 选中 Pod 发出的流量。流量必须同时匹配 ports 和 to。
// 该类型自 1.8 起为 beta 级别。
type NetworkPolicyEgressRule struct {
	// ports 是出站流量的目标端口列表，各项按逻辑“或”组合。
	// 该字段为空或缺失时，规则匹配所有端口（不按端口限制流量）；
	// 存在且至少有一项时，只有流量匹配列表中某个端口才被允许。
	// +optional
	Ports []NetworkPolicyPort

	// to 是本规则选中 Pod 出站流量的目标列表，各项按逻辑“或”组合。
	// 该字段为空或缺失时，规则匹配所有目标（不按目标限制流量）；
	// 存在且至少有一项时，只有流量匹配 to 列表中某一项才被允许。
	// +optional
	To []NetworkPolicyPeer
}

// NetworkPolicyPort 描述允许流量通过的端口。
type NetworkPolicyPort struct {
	// protocol 表示流量必须匹配的协议，可选 TCP、UDP 或 SCTP。未指定时默认为 TCP。
	// +optional
	Protocol *api.Protocol

	// port 表示指定协议上的端口，可以是 Pod 上的数字端口或命名端口。
	// 未提供该字段时匹配所有端口名和端口号；提供时，只匹配同时满足指定协议与指定端口的流量。
	// +optional
	Port *intstr.IntOrString

	// endPort 表示策略允许的端口范围（含端点）的终止端口，范围为 port 到 endPort。
	// 若未定义 port，或 port 是命名（字符串）端口，则不能定义该字段。
	// endPort 必须大于或等于 port。
	// +optional
	EndPort *int32
}

// IPBlock 描述允许访问 podSelector 选中 Pod 的某个 CIDR（如 "192.168.1.0/24"、"2001:db8::/64"）。
// except 列出不应包含在该规则中的 CIDR。
type IPBlock struct {
	// cidr 是表示 IPBlock 的字符串，如 "192.168.1.0/24" 或 "2001:db8::/64"。
	CIDR string

	// except 是不应包含在 IPBlock 中的 CIDR 列表，如 "192.168.1.0/24" 或 "2001:db8::/64"。
	// 超出 cidr 范围的 Except 值会被拒绝。
	// +optional
	Except []string
}

// NetworkPolicyPeer 描述允许与其收发流量的对端。
type NetworkPolicyPeer struct {
	// podSelector 是选择 Pod 的标签选择器，遵循标准标签选择器语义；存在但为空时选择所有 Pod。
	//
	// 若同时设置了 namespaceSelector，则整个 NetworkPolicyPeer 选择 namespaceSelector
	// 选中的命名空间中匹配 podSelector 的 Pod；否则选择策略自身命名空间中匹配 podSelector 的 Pod。
	// +optional
	PodSelector *metav1.LabelSelector

	// namespaceSelector 使用集群级标签选择命名空间，遵循标准标签选择器语义；存在但为空时选择所有命名空间。
	//
	// 若同时设置了 podSelector，则整个 NetworkPolicyPeer 选择 namespaceSelector 选中的
	// 命名空间中匹配 podSelector 的 Pod；否则选择 namespaceSelector 选中命名空间中的所有 Pod。
	// +optional
	NamespaceSelector *metav1.LabelSelector

	// ipBlock 针对特定 IPBlock 定义策略。设置了该字段时，其它两个字段都不能设置。
	// +optional
	IPBlock *IPBlock
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NetworkPolicyList 是 NetworkPolicy 对象的列表。
type NetworkPolicyList struct {
	metav1.TypeMeta

	// +optional
	metav1.ListMeta

	Items []NetworkPolicy
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Ingress 是一组规则，允许入站连接到达后端定义的端点。Ingress 可配置为服务提供外部可访问的 URL、
// 负载均衡流量、终止 SSL、提供基于名称的虚拟主机等。
type Ingress struct {
	metav1.TypeMeta

	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// spec 表示 Ingress 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec IngressSpec

	// status 表示 Ingress 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status IngressStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IngressList 是 Ingress 的集合。
type IngressList struct {
	metav1.TypeMeta

	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 Ingress 的列表。
	Items []Ingress
}

// IngressSpec 描述用户期望的 Ingress。
type IngressSpec struct {
	// ingressClassName 是 IngressClass 集群资源的名称，关联的 IngressClass 决定由哪个控制器实现该资源。
	// 它取代了已废弃的 `kubernetes.io/ingress.class` 注解。为向后兼容，设置该注解时其优先级高于本字段；
	// 当字段值与注解值不同时，控制器可以发出警告。
	// 本 API 的实现应忽略未指定 class 的 Ingress。IngressClass 资源可被标记为默认，
	// 用于为本字段提供默认值。更多信息请参阅 IngressClass 文档。
	// +optional
	IngressClassName *string

	// defaultBackend 是处理不匹配任何规则的请求的后端。
	// 未指定 Rules 时，必须指定 DefaultBackend。若未设置 DefaultBackend，
	// 不匹配任何规则的请求如何处理由 Ingress 控制器决定。
	// +optional
	DefaultBackend *IngressBackend

	// tls 表示 TLS 配置。目前 Ingress 只支持单一 TLS 端口 443。
	// 若列表中的多项指定了不同的主机，且实现该 Ingress 的控制器支持 SNI，
	// 则会根据 SNI TLS 扩展中的主机名在同一端口上进行复用。
	// +listType=atomic
	// +optional
	TLS []IngressTLS

	// rules 是用于配置 Ingress 的主机规则列表。未指定或没有规则匹配时，所有流量发往默认后端。
	// +listType=atomic
	// +optional
	Rules []IngressRule
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IngressClass 表示 Ingress 的类别，由 Ingress 的 spec 引用。`ingressclass.kubernetes.io/is-default-class`
// 注解可用于标记某个 IngressClass 为默认类。当且仅当一个 IngressClass 资源设置了该注解为 true 时，
// 未指定 class 的新 Ingress 资源会被分配这个默认类。
type IngressClass struct {
	metav1.TypeMeta

	metav1.ObjectMeta

	// spec 表示 IngressClass 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec IngressClassSpec
}

// IngressClassSpec 提供 Ingress 类别的相关信息。
type IngressClassSpec struct {
	// controller 是应处理该类别的控制器名称，允许同一控制器管理不同“风味”的配置，
	// 例如同一控制器实现使用不同参数。取值应为带域名前缀的路径，长度不超过 250 个字符，
	// 如 "acme.io/ingress-controller"。该字段不可变。
	Controller string

	// parameters 指向一个自定义资源，其中包含控制器的附加配置。控制器不需要额外参数时该字段可省略。
	// +optional
	Parameters *IngressClassParametersReference
}

const (
	// IngressClassParametersReferenceScopeNamespace 表示被引用的 Parameters 资源是命名空间级的。
	IngressClassParametersReferenceScopeNamespace = "Namespace"
	// IngressClassParametersReferenceScopeCluster 表示被引用的 Parameters 资源是集群级的。
	IngressClassParametersReferenceScopeCluster = "Cluster"
)

// IngressClassParametersReference 标识一个 API 对象，可用于指定集群级或命名空间级的资源。
type IngressClassParametersReference struct {
	// apiGroup 是被引用资源所属的 API 组。未指定 apiGroup 时，指定的 kind 必须属于 core API 组；
	// 其它第三方类型必须指定 apiGroup。
	// +optional
	APIGroup *string

	// kind 是被引用资源的类型。
	Kind string

	// name 是被引用资源的名称。
	Name string

	// scope 表示引用的是集群级还是命名空间级资源，可取 "Cluster"（默认）或 "Namespace"。
	// +optional
	Scope *string

	// namespace 是被引用资源所在的命名空间。scope 为 "Namespace" 时必填，为 "Cluster" 时必须不设置。
	// +optional
	Namespace *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IngressClassList 是 IngressClass 的集合。
type IngressClassList struct {
	metav1.TypeMeta

	// 标准对象元数据。
	// +optional
	metav1.ListMeta

	// items 是 IngressClass 的列表。
	Items []IngressClass
}

// IngressTLS 描述与 Ingress 关联的传输层安全（TLS）配置。
type IngressTLS struct {
	// hosts 是 TLS 证书中包含的主机名列表，取值必须与 tlsSecret 中使用的名称一致。
	// 未指定时，默认使用实现该 Ingress 的负载均衡器控制器的通配符主机设置。
	// +listType=atomic
	// +optional
	Hosts []string

	// secretName 是用于在 443 端口终止 TLS 流量的 Secret 名称。
	// 该字段可选，以支持仅基于 SNI 主机名进行 TLS 路由。若监听器中的 SNI 主机与 IngressRule 使用的
	// "Host" 头字段冲突，则使用 SNI 主机进行 TLS 终止，使用 "Host" 头的值进行路由。
	// +optional
	SecretName string
	// TODO: 考虑支持不同的终止模式、协议等。
}

// IngressStatus 描述 Ingress 的当前状态。
type IngressStatus struct {
	// loadBalancer 包含负载均衡器的当前状态。
	// +optional
	LoadBalancer IngressLoadBalancerStatus
}

// IngressLoadBalancerStatus 表示负载均衡器的状态。
type IngressLoadBalancerStatus struct {
	// ingress 是负载均衡器入口点的列表。
	// +optional
	Ingress []IngressLoadBalancerIngress
}

// IngressLoadBalancerIngress 表示负载均衡器某个入口点的状态。
type IngressLoadBalancerIngress struct {
	// 对于基于 IP 的负载均衡器入口点，设置 ip 字段。
	// +optional
	IP string

	// 对于基于 DNS 的负载均衡器入口点，设置 hostname 字段。
	// +optional
	Hostname string

	// ports 提供该负载均衡器暴露端口的信息。
	// +optional
	Ports []IngressPortStatus
}

// IngressPortStatus 表示 Ingress 端口的错误状况。
type IngressPortStatus struct {
	// port 是 Ingress 端口的端口号。
	Port int32

	// protocol 是 Ingress 端口的协议。
	Protocol api.Protocol

	// error 表示该端口存在的问题。
	// error 的格式必须遵循以下规则：
	// - Kubernetes 定义的错误值使用驼峰命名
	// - 云厂商特定的错误值必须遵循标签名称风格（如 example.com/name）。
	Error *string
}

// IngressRule 表示将指定主机下的路径映射到相应后端服务的规则。
// 入站请求先进行主机匹配，再路由到匹配的 IngressRuleValue 所关联的后端。
type IngressRule struct {
	// host 是 RFC 3986 定义的网络主机完全限定域名。
	// 与 RFC 3986 中 URI 的 "host" 部分相比，有以下差异：
	// 1. 不允许使用 IP。目前 IngressRuleValue 只能作用于父 Ingress spec 中的 IP。
	// 2. 不识别 `:` 分隔符，因为不允许指定端口。目前 Ingress 的端口隐含为 http 的 :80 和 https 的 :443。
	// 这两点在将来可能会变化。
	// 入站请求先与 host 匹配，再匹配 IngressRuleValue。未指定 host 时，Ingress 基于
	// IngressRuleValue 路由全部流量。
	//
	// host 可以是“精确”域名（不带结尾点，如 "foo.bar.com"），也可以是“通配符”域名
	// （以单个通配符标签开头，如 "*.foo.com"）。通配符 '*' 必须单独作为第一个 DNS 标签出现，
	// 且只匹配一个标签；不能只有通配符标签（如 Host == "*"）。
	// 请求与 host 字段的匹配规则如下：
	// 1. host 为精确域名时，HTTP host 头与 Host 相等即匹配该规则。
	// 2. host 为通配符域名时，HTTP host 头等于去掉第一个标签后的后缀即匹配该规则。
	// +optional
	Host string

	// IngressRuleValue 表示此 IngressRule 的请求路由规则。未指定时，规则默认为 HTTP 通配。
	// 究竟是只把匹配 host 的流量发往默认后端，还是把所有流量发往默认后端，
	// 由实现该 Ingress 的控制器决定。目前仅支持 HTTP 这一种 IngressRuleValue。
	// +optional
	IngressRuleValue
}

// IngressRuleValue 表示作用于入站请求的规则。规则满足时，请求被路由到指定后端。
// 目前不允许在同一个 Ingress 中混用不同类型的规则，因此以下字段必须且只能设置一个。
type IngressRuleValue struct {
	//TODO:
	// 1. 考虑为本资源及相关规则改名，使其不再与 Ingress 绑定；它们也可用于集群内部流量路由。
	// 2. 考虑增加特定于 ingress 类型的全局选项字段，供负载均衡器使用，例如 http keep-alive。

	// +optional
	HTTP *HTTPIngressRuleValue
}

// HTTPIngressRuleValue 是一组指向后端的 HTTP 选择器。
// 以 http://<host>/<path>?<searchpart> -> backend 为例，URL 各部分的含义遵循 RFC 3986；
// 本资源用于匹配最后一个 '/' 之后、第一个 '?' 或 '#' 之前的全部内容。
type HTTPIngressRuleValue struct {
	// paths 是把请求映射到后端的路径集合。
	// +listType=atomic
	Paths []HTTPIngressPath
	// TODO: 考虑增加特定于 ingress 类型的全局选项字段，供负载均衡器使用，例如 http keep-alive。
}

// PathType 表示 HTTPIngressPath 引用的路径类型。
type PathType string

const (
	// PathTypeExact 区分大小写、精确匹配 URL 路径。
	PathTypeExact = PathType("Exact")

	// PathTypePrefix 按 '/' 切分 URL 路径后进行前缀匹配。匹配区分大小写，并逐路径元素进行。
	// 路径元素指路径被 '/' 分隔后的各标签。若请求路径的每个元素都以路径 p 的对应元素为前缀，则视为匹配。
	// 注意：路径最后一个元素若是请求路径最后元素的子串，不算匹配
	// （如 /foo/bar 匹配 /foo/bar/baz，但不匹配 /foo/barbaz）。
	// Ingress spec 中若存在多个匹配路径，最长匹配路径优先。
	// 示例：
	// - /foo/bar 不匹配 /foo/barbaz 的请求
	// - /foo/bar 匹配 /foo/bar 和 /foo/bar/baz 的请求
	// - /foo 和 /foo/ 都匹配 /foo 和 /foo/ 的请求；若两者同时出现在 Ingress spec 中，
	//   较长者 /foo/ 优先。
	PathTypePrefix = PathType("Prefix")

	// PathTypeImplementationSpecific 的匹配方式由 IngressClass 自行决定。
	// 实现方可以将其当作独立的 PathType，也可以与 Prefix 或 Exact 同等处理。
	PathTypeImplementationSpecific = PathType("ImplementationSpecific")
)

// HTTPIngressPath 将路径与后端关联，匹配该路径的入站 URL 会被转发到对应后端。
type HTTPIngressPath struct {
	// path 与入站请求的路径进行匹配。目前它可以包含 RFC 3986 所定义的常规 URL "path" 部分
	// 不允许的字符。路径必须以 '/' 开头；当 PathType 为 "Exact" 或 "Prefix" 时必须提供该字段。
	// +optional
	Path string

	// pathType 决定路径匹配的解释方式，可取 Exact、Prefix 或 ImplementationSpecific。
	// 实现方必须支持所有路径类型。
	// +optional
	PathType *PathType

	// backend 定义流量将被转发到的服务端点。
	Backend IngressBackend
}

// IngressBackend 描述给定服务和端口的所有端点。
type IngressBackend struct {
	// service 将某个服务引用为后端，与 "Resource" 互斥。
	// +optional
	Service *IngressServiceBackend

	// resource 是指向 Ingress 对象所在命名空间中另一个 Kubernetes 资源的 ObjectRef。
	// 指定了 resource 时，不得指定 service.Name 和 service.Port，与 "Service" 互斥。
	// +optional
	Resource *api.TypedLocalObjectReference
}

// IngressServiceBackend 把 Kubernetes Service 引用为后端。
type IngressServiceBackend struct {
	// name 是被引用的服务，该服务必须与 Ingress 对象位于同一命名空间。
	Name string

	// port 是被引用服务的端口。IngressServiceBackend 必须提供端口名或端口号。
	Port ServiceBackendPort
}

// ServiceBackendPort 表示被引用的服务端口。
type ServiceBackendPort struct {
	// name 是 Service 上的端口名，必须是 IANA_SVC_NAME（遵循 RFC6335），与 "Number" 互斥。
	// +optional
	Name string

	// number 是 Service 上的数字端口号（如 80），与 "Name" 互斥。
	// +optional
	Number int32
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IPAddress 表示单一 IP 家族中的一个 IP 地址，供操作 IP 地址的 API 使用。
// Service 核心 API 用它来分配 IP 地址。IP 地址可以有多种表示形式；为保证唯一性，
// 对象名称使用 IP 的规范格式：IPv4 为去掉前导零、以点分隔的四段十进制数字，
// IPv6 采用 RFC 5952 定义的格式。
// 合法示例：192.168.1.5、2001:db8::1 或 2001:db8:aaaa:bbbb:cccc:dddd:eeee:1。
// 非法示例：10.01.2.3 或 2001:db8:0:0:0::1。
type IPAddress struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	// +optional
	Spec IPAddressSpec
}

// IPAddressSpec 描述 IP 地址的属性。
type IPAddressSpec struct {
	// ParentRef 指向 IPAddress 所关联的资源。IPAddress 必须关联一个父对象。
	// +required
	ParentRef *ParentReference
}

// ParentReference 是对父对象的引用，包含组、资源类型、命名空间和名称。
type ParentReference struct {
	// Group 是被引用对象所属的 API 组。
	Group string
	// Resource 是被引用对象的资源类型。
	Resource string
	// Namespace 是被引用对象所在的命名空间。
	Namespace string
	// Name 是被引用对象的名称。
	Name string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// IPAddressList 包含 IPAddress 的列表。
type IPAddressList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 IPAddress 的列表。
	Items []IPAddress
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceCIDR 使用 CIDR 格式定义 IP 地址范围（如 192.168.0.0/24 或 2001:db2::/64），
// 该范围用于为 Service 对象分配 ClusterIP。
type ServiceCIDR struct {
	metav1.TypeMeta
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta
	// spec 表示 ServiceCIDR 的期望状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ServiceCIDRSpec
	// status 表示 ServiceCIDR 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ServiceCIDRStatus
}

// ServiceCIDRSpec 定义可分配 Service ClusterIP 的 CIDR 范围。
type ServiceCIDRSpec struct {
	// CIDRs 以 CIDR 表示法定义可从中分配 Service ClusterIP 的 IP 块
	// （如 "192.168.0.0/24" 或 "2001:db8::/64"）。最多允许两个 CIDR，每个 IP 家族各一个。
	// 该字段不可变。
	// +optional
	CIDRs []string
}

// ServiceCIDRStatus 描述 ServiceCIDR 的当前状态。
type ServiceCIDRStatus struct {
	// conditions 保存一组 metav1.Condition，描述 ServiceCIDR 的状态。
	Conditions []metav1.Condition
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.27

// ServiceCIDRList 包含 ServiceCIDR 对象的列表。
type ServiceCIDRList struct {
	metav1.TypeMeta
	// 标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta
	// items 是 ServiceCIDR 的列表。
	Items []ServiceCIDR
}
