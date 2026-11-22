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
	"k8s.io/apimachinery/pkg/util/intstr"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,Ingress

// Ingress 是一组规则，允许入站连接到达 backend 定义的端点。
// Ingress 可配置为给 service 提供外部可达的 URL、负载均衡流量、终止 SSL、
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
// +k8s:prerelease-lifecycle-gen:introduced=1.14
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,IngressList

// IngressList 是 Ingress 的集合。
type IngressList struct {
	metav1.TypeMeta `json:""`

	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 Ingress 的列表。
	Items []Ingress `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// IngressSpec 描述用户期望存在的 Ingress。
type IngressSpec struct {
	// ingressClassName 是 IngressClass 集群资源的名称。
	// 关联的 IngressClass 定义由哪个控制器实现该资源。它取代了已弃用的
	// `kubernetes.io/ingress.class` 注解。为向后兼容，当设置了该注解时，
	// 必须优先于本字段。若字段与注解值不同，控制器可发出警告。
	// 该 API 的实现应忽略未指定 class 的 Ingress。
	// IngressClass 资源可标记为默认，用于为本字段设置默认值。更多信息请参考
	// IngressClass 文档。
	// +optional
	IngressClassName *string `json:"ingressClassName,omitempty" protobuf:"bytes,4,opt,name=ingressClassName"`

	// backend 是默认 backend，用于处理不匹配任何 rule 的请求。
	// 'backend' 或 'rules' 至少需指定一个。该字段可选，以允许负载均衡控制器
	// 或默认值逻辑指定全局默认值。
	// +optional
	Backend *IngressBackend `json:"backend,omitempty" protobuf:"bytes,1,opt,name=backend"`

	// tls 表示 TLS 配置。目前 Ingress 仅支持单一 TLS 端口 443。
	// 若列表中多个成员指定了不同主机，在满足 Ingress 的控制器支持 SNI 时，
	// 将通过 SNI TLS 扩展根据主机名在同一端口上多路复用。
	// +optional
	// +listType=atomic
	TLS []IngressTLS `json:"tls,omitempty" protobuf:"bytes,2,rep,name=tls"`

	// rules 是用于配置 Ingress 的主机规则列表。若未指定或无规则匹配，
	// 所有流量将发送到默认 backend。
	// +optional
	// +listType=atomic
	Rules []IngressRule `json:"rules,omitempty" protobuf:"bytes,3,rep,name=rules"`
	// TODO: Add the ability to specify load-balancer IP through claims
}

// IngressTLS 描述与 Ingress 关联的传输层安全配置。
type IngressTLS struct {
	// hosts 是 TLS 证书中包含的主机列表，列表中的值必须与 tlsSecret 中使用的
	// 名称匹配。若未指定，默认为满足此 Ingress 的负载均衡控制器的通配符主机设置。
	// +optional
	// +listType=atomic
	Hosts []string `json:"hosts,omitempty" protobuf:"bytes,1,rep,name=hosts"`

	// secretName 是用于在 443 端口终止 TLS 流量的 secret 名称。
	// 该字段可选，以允许仅基于 SNI 主机名进行 TLS 路由。
	// 若监听器中的 SNI 主机与 IngressRule 使用的 "Host" 头字段冲突，
	// SNI 主机用于终止 TLS，Host 头的值用于路由。
	// +optional
	SecretName string `json:"secretName,omitempty" protobuf:"bytes,2,opt,name=secretName"`
	// TODO: Consider specifying different modes of termination, protocols etc.
}

// IngressStatus 描述 Ingress 的当前状态。
type IngressStatus struct {
	// loadBalancer 包含负载均衡器的当前状态。
	// +optional
	LoadBalancer IngressLoadBalancerStatus `json:"loadBalancer,omitempty" protobuf:"bytes,1,opt,name=loadBalancer"`
}

// LoadBalancerStatus 表示负载均衡器的状态。
type IngressLoadBalancerStatus struct {
	// ingress 是包含负载均衡器入口点的列表。
	// +optional
	// +listType=atomic
	Ingress []IngressLoadBalancerIngress `json:"ingress,omitempty" protobuf:"bytes,1,rep,name=ingress"`
}

// IngressLoadBalancerIngress 表示负载均衡器入口点的状态。
type IngressLoadBalancerIngress struct {
	// ip 为基于 IP 的负载均衡器入口点设置。
	// +optional
	IP string `json:"ip,omitempty" protobuf:"bytes,1,opt,name=ip"`

	// hostname 为基于 DNS 的负载均衡器入口点设置。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,2,opt,name=hostname"`

	// ports 提供此 LoadBalancer 暴露的端口信息。
	// +listType=atomic
	// +optional
	Ports []IngressPortStatus `json:"ports,omitempty" protobuf:"bytes,4,rep,name=ports"`
}

// IngressPortStatus 表示 service 端口的错误状态。
type IngressPortStatus struct {
	// port 是 ingress 端口的端口号。
	Port int32 `json:"port" protobuf:"varint,1,opt,name=port"`

	// protocol 是 ingress 端口的协议，支持值为 "TCP"、"UDP"、"SCTP"。
	Protocol v1.Protocol `json:"protocol" protobuf:"bytes,2,opt,name=protocol,casttype=Protocol"`

	// error 记录 service 端口的问题。
	// 错误格式须遵循以下规则：
	// - 内置错误值应在本文件中指定，且使用 CamelCase 名称。
	// - 云服务商特定的错误值名称必须符合 foo.example.com/CamelCase 格式。
	// ---
	// 其匹配的正则为 (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// +optional
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Error *string `json:"error,omitempty" protobuf:"bytes,3,opt,name=error"`
}

// IngressRule 表示将指定主机下的路径映射到相关 backend service 的规则。
// 入站请求先按 host 匹配，再路由到匹配 IngressRuleValue 关联的 backend。
type IngressRule struct {
	// host 是 RFC 3986 定义的网络主机的完全限定域名。
	// 注意与 RFC 3986 中 URI 的 "host" 部分存在以下差异：
	// 1. 不允许 IP。目前 IngressRuleValue 只能应用于父 Ingress 的 Spec 中的 IP。
	// 2. 不遵守 `:` 分隔符，因为不允许端口。目前 Ingress 对 http 隐式使用
	//    端口 :80，对 https 隐式使用 :443。
	// 未来这些都可能改变。
	// 入站请求先按 host 匹配，再匹配 IngressRuleValue。若未指定 host，
	// Ingress 将基于指定的 IngressRuleValue 路由所有流量。
	//
	// host 可以是 "精确" 值，即不带结尾点的域名（如 "foo.bar.com"），
	// 也可以是 "通配" 值，即前缀为单个通配符标签的域名（如 "*.foo.com"）。
	// 通配符 '*' 必须单独作为第一个 DNS 标签出现，且只匹配单个标签。
	// 不能仅有通配符标签（如 Host == "*"）。
	// 请求与 Host 字段的匹配规则如下：
	// 1. 若 Host 为精确值，且 http host 头等于 Host，则请求匹配此规则。
	// 2. 若 Host 为通配值，且 http host 头等于通配规则去掉第一个标签后的后缀，则请求匹配此规则。
	// +optional
	Host string `json:"host,omitempty" protobuf:"bytes,1,opt,name=host"`

	// IngressRuleValue 表示此 IngressRule 路由请求的规则。
	// 若未指定，规则默认为 http catch-all。是将匹配主机的流量还是所有流量
	// 发送到默认 backend，由实现 Ingress 的控制器决定。目前仅支持 Http 作为
	// IngressRuleValue。
	// +optional
	IngressRuleValue `json:"" protobuf:"bytes,2,opt,name=ingressRuleValue"`
}

// IngressRuleValue 表示应用于入站请求的规则。若规则满足，请求被路由到指定 backend。
// 目前在单个 Ingress 中混合不同类型的规则是不允许的，因此必须且只能设置以下之一。
type IngressRuleValue struct {
	//TODO:
	// 1. 考虑重命名此资源及相关规则，使其不再与 Ingress 绑定，它们可用于集群内流量路由。
	// 2. 考虑添加特定于 ingress 类型的全局选项字段，供负载均衡器使用，如 http keep-alive。

	// http 是 HTTP IngressRuleValue，包含 http 选择器列表。
	// +optional
	HTTP *HTTPIngressRuleValue `json:"http,omitempty" protobuf:"bytes,1,opt,name=http"`
}

// HTTPIngressRuleValue 是指向 backend 的 http 选择器列表。
// 例如 http://<host>/<path>?<searchpart> -> backend，其中 URL 各部分对应 RFC 3986，
// 本资源用于匹配最后一个 '/' 之后、第一个 '?' 或 '#' 之前的内容。
type HTTPIngressRuleValue struct {
	// paths 是将请求映射到 backend 的路径集合。
	// +listType=atomic
	Paths []HTTPIngressPath `json:"paths" protobuf:"bytes,1,rep,name=paths"`
	// TODO: Consider adding fields for ingress-type specific global
	// options usable by a loadbalancer, like http keep-alive.
}

// PathType 表示 HTTPIngressPath 引用的路径类型。
type PathType string

const (
	// PathTypeExact 区分大小写地精确匹配 URL 路径。
	PathTypeExact = PathType("Exact")

	// PathTypePrefix 基于以 '/' 分割的 URL 路径前缀匹配。匹配区分大小写，
	// 按路径元素逐一进行。路径元素指路径按 '/' 分隔符切分后的标签列表。
	// 若请求路径的每个元素都以路径 p 的对应元素为前缀，则该请求匹配路径 p。
	// 注意若路径最后一个元素只是请求路径最后一个元素的子串，则不匹配
	// （例如 /foo/bar 匹配 /foo/bar/baz，但不匹配 /foo/barbaz）。
	// 若 Ingress spec 中存在多个匹配路径，优先使用最长匹配路径。
	// 示例：
	// - /foo/bar 不匹配对 /foo/barbaz 的请求
	// - /foo/bar 匹配对 /foo/bar 和 /foo/bar/baz 的请求
	// - /foo 和 /foo/ 都匹配对 /foo 和 /foo/ 的请求。若两者都出现在
	//   Ingress spec 中，优先使用最长匹配路径 (/foo/)。
	PathTypePrefix = PathType("Prefix")

	// PathTypeImplementationSpecific 的匹配由 IngressClass 决定。
	// 实现方可将其视为独立的 PathType，也可等同视为 Prefix 或 Exact 类型。
	PathTypeImplementationSpecific = PathType("ImplementationSpecific")
)

// HTTPIngressPath 将路径与 backend 关联。匹配该路径的入站 URL 被转发到 backend。
type HTTPIngressPath struct {
	// path 与入站请求的路径匹配。目前它可以包含 RFC 3986 所定义的常规 URL
	// "path" 部分不允许的字符。路径必须以 '/' 开头，且当 PathType 为 "Exact"
	// 或 "Prefix" 时必须提供。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`

	// pathType 决定路径匹配的解释方式，可取以下值之一：
	// * Exact：精确匹配 URL 路径。
	// * Prefix：基于以 '/' 分割的 URL 路径前缀匹配，按路径元素逐一进行。
	//   路径元素指路径按 '/' 分隔符切分后的标签列表。若请求路径的每个元素
	//   都以路径 p 的对应元素为前缀，则请求匹配路径 p。注意若路径最后一个
	//   元素只是请求路径最后一个元素的子串，则不匹配（例如 /foo/bar 匹配
	//   /foo/bar/baz，但不匹配 /foo/barbaz）。
	// * ImplementationSpecific：路径匹配的解释由 IngressClass 决定。实现方可
	//   将其视为独立的 PathType，也可等同视为 Prefix 或 Exact 类型。
	// 实现方必须支持所有路径类型。默认为 ImplementationSpecific。
	PathType *PathType `json:"pathType,omitempty" protobuf:"bytes,3,opt,name=pathType"`

	// backend 定义流量将被转发到的被引用 service 端点。
	Backend IngressBackend `json:"backend" protobuf:"bytes,2,opt,name=backend"`
}

// IngressBackend 描述给定 service 和端口的所有端点。
type IngressBackend struct {
	// serviceName 指定被引用 service 的名称。
	// +optional
	ServiceName string `json:"serviceName,omitempty" protobuf:"bytes,1,opt,name=serviceName"`

	// servicePort 指定被引用 service 的端口。
	// +optional
	ServicePort intstr.IntOrString `json:"servicePort,omitempty" protobuf:"bytes,2,opt,name=servicePort"`

	// resource 是指向 Ingress 对象所在命名空间中另一 Kubernetes 资源的 ObjectRef。
	// 若指定了 resource，则不得指定 serviceName 和 servicePort。
	// +optional
	Resource *v1.TypedLocalObjectReference `json:"resource,omitempty" protobuf:"bytes,3,opt,name=resource"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.18
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,IngressClassList

// IngressClass 表示 Ingress 的类别，由 Ingress Spec 引用。
// 可使用 `ingressclass.kubernetes.io/is-default-class` 注解表示某个 IngressClass
// 应被视为默认。当单个 IngressClass 资源将该注解设为 true 时，未指定 class 的
// 新 Ingress 资源将被分配此默认类别。
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

// IngressClassSpec 提供 Ingress 类别的信息。
type IngressClassSpec struct {
	// controller 指应处理此类别的控制器名称，允许同一控制器管理不同的“风格”。
	// 例如，同一实现控制器可以有不同的参数。应以域名前缀路径的形式指定，
	// 长度不超过 250 个字符，如 "acme.io/ingress-controller"。该字段不可变。
	Controller string `json:"controller,omitempty" protobuf:"bytes,1,opt,name=controller"`

	// parameters 指向自定义资源的链接，包含控制器的额外配置。
	// 若控制器不需要额外参数，则该字段可选。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Parameters *IngressClassParametersReference `json:"parameters,omitempty" protobuf:"bytes,2,opt,name=parameters"`
}

const (
	// IngressClassParametersReferenceScopeNamespace 表示被引用的 Parameters 资源是命名空间作用域的。
	IngressClassParametersReferenceScopeNamespace = "Namespace"
	// IngressClassParametersReferenceScopeCluster 表示被引用的 Parameters 资源是集群作用域的。
	IngressClassParametersReferenceScopeCluster = "Cluster"
)

// IngressClassParametersReference 标识一个 API 对象，可用于指定集群或命名空间作用域的资源。
type IngressClassParametersReference struct {
	// apiGroup 是被引用资源所属的组。若未指定 APIGroup，则指定的 Kind 必须属于
	// 核心 API 组。对于任何其他第三方类型，APIGroup 是必需的。
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

	// scope 表示引用的是集群还是命名空间作用域的资源，可设为 "Cluster"（默认）或 "Namespace"。
	Scope *string `json:"scope" protobuf:"bytes,4,opt,name=scope"`

	// namespace 是被引用资源所在的命名空间。当 scope 设为 "Namespace" 时必需，
	// 当 scope 设为 "Cluster" 时不得设置。
	// +optional
	Namespace *string `json:"namespace,omitempty" protobuf:"bytes,5,opt,name=namespace"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.18
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=networking.k8s.io,v1,IngressClassList

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
// +k8s:prerelease-lifecycle-gen:introduced=1.31

// IPAddress 表示单个 IP 地址族中的一个 IP。该对象设计用于操作 IP 地址的 API，
// Service 核心 API 使用它分配 IP 地址。IP 地址可以有多种表示形式，
// 为保证 IP 的唯一性，对象名称使用规范格式的 IP 地址：IPv4 为四个十进制数字以点分隔
// 且省略前导零，IPv6 为 RFC 5952 定义的表示形式。
// 有效示例：192.168.1.5 或 2001:db8::1 或 2001:db8:aaaa:bbbb:cccc:dddd:eeee:1
// 无效示例：10.01.2.3 或 2001:db8:0:0:0::1
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
	// parentRef 引用 IPAddress 所附属的资源。IPAddress 必须引用一个父对象。
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
// +k8s:prerelease-lifecycle-gen:introduced=1.31

// IPAddressList 包含 IPAddress 的列表。
type IPAddressList struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是 IPAddress 的列表。
	Items []IPAddress `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.31

// ServiceCIDR 使用 CIDR 格式定义 IP 地址范围（如 192.168.0.0/24 或 2001:db2::/64），
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
	// cidrs 以 CIDR 表示法定义 IP 段（如 "192.168.0.0/24" 或 "2001:db8::/64"），
	// 用于分配 service cluster IP。最多允许两个 CIDR，每个 IP 地址族一个。
	// 该字段不可变。
	// +optional
	// +listType=atomic
	CIDRs []string `json:"cidrs,omitempty" protobuf:"bytes,1,opt,name=cidrs"`
}

const (
	// ServiceCIDRConditionReady 表示 ServiceCIDR 已就绪，可供 apiserver 用于为 Service 分配 ClusterIP。
	ServiceCIDRConditionReady = "Ready"
	// ServiceCIDRReasonTerminating 表示 ServiceCIDR 因正在被删除而未就绪。
	ServiceCIDRReasonTerminating = "Terminating"
)

// ServiceCIDRStatus 描述 ServiceCIDR 的当前状态。
type ServiceCIDRStatus struct {
	// conditions 包含描述 ServiceCIDR 状态的 metav1.Condition 数组。
	// 当前的 service 状态。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:opaqueType
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.31

// ServiceCIDRList 包含 ServiceCIDR 对象的列表。
type ServiceCIDRList struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是 ServiceCIDR 的列表。
	Items []ServiceCIDR `json:"items" protobuf:"bytes,2,rep,name=items"`
}
