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

package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	tracingapi "k8s.io/component-base/tracing/api/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AdmissionConfiguration 提供准入控制器（admission controller）的版本化配置。
//
// 这是所有准入插件配置的统一入口：每个插件通过名字关联一段配置，
// 配置可以内嵌在 Configuration 字段中，也可以放在 Path 指向的单独文件里。
type AdmissionConfiguration struct {
	metav1.TypeMeta

	// Plugins 允许为每个准入控制插件分别指定配置。
	// +optional
	Plugins []AdmissionPluginConfiguration
}

// AdmissionPluginConfiguration 提供单个插件的配置。
//
// 插件名必须与启动参数 --enable-admission-plugins 中使用的注册名一致；
// 找不到配置的插件按默认行为运行。
type AdmissionPluginConfiguration struct {
	// Name 是准入控制器的名称。
	// 必须与注册的准入插件名一致。
	Name string

	// Path 指向一个配置文件的路径，该文件包含此插件的
	// 配置。
	// +optional
	Path string

	// Configuration 是内嵌的配置对象，用作此插件的配置。
	// 若存在，则优先于 Path 指向的配置文件。
	// +optional
	Configuration *runtime.Unknown
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EgressSelectorConfiguration 提供出口选择器（egress selector）客户端的版本化配置。
//
// 出口选择器（egress selector）决定 apiserver 访问 controlplane、etcd、cluster
// 等不同目的地时如何建立连接（直连或经 konnectivity 代理）。
type EgressSelectorConfiguration struct {
	metav1.TypeMeta

	// EgressSelections 包含一组出口选择客户端配置
	EgressSelections []EgressSelection
}

// EgressSelection 提供单个出口选择客户端的配置。
//
// 每个 Name 对应一类目的地（controlplane/etcd/cluster），同一个 Name 只能出现一次。
type EgressSelection struct {
	// Name 是出口选择的名称。
	// 当前支持的取值为 "controlplane"、"etcd" 和 "cluster"
	Name string

	// Connection 是用于配置该出口选择的具体信息
	Connection Connection
}

// Connection 提供单个出口选择客户端的配置。
//
// ProxyProtocol 决定是否需要 Transport：Direct 直接连接，HTTPConnect/GRPC
// 则需要通过 konnectivity 代理。
type Connection struct {
	// Protocol 是客户端连接 konnectivity server 所使用的协议。
	ProxyProtocol ProtocolType

	// Transport 定义拨号 konnectivity server 时使用的传输配置。
	// 当 ProxyProtocol 为 HTTPConnect 或 GRPC 时必填。
	// +optional
	Transport *Transport
}

// ProtocolType 是 Connection.ProtocolType 的一组合法取值
type ProtocolType string

// ProtocolType 针对 konnectivity server 的合法取值
const (
	// 使用 HTTPConnect 连接 konnectivity server
	ProtocolHTTPConnect ProtocolType = "HTTPConnect"
	// 使用 grpc 连接 konnectivity server
	ProtocolGRPC ProtocolType = "GRPC"
	// 直接连接（跳过 konnectivity server）
	ProtocolDirect ProtocolType = "Direct"
)

// Transport 定义拨号 konnectivity server 时使用的传输配置
//
// TCP 与 UDS 至少配置一个；具体用哪个取决于 konnectivity 服务的部署形态。
type Transport struct {
	// TCP 是通过 TCP 与 konnectivity server 通信的 TCP 配置
	// 目前 ProxyProtocol 为 GRPC 时不支持 TCP 传输
	// TCP 或 UDS 至少要设置其一
	// +optional
	TCP *TCPTransport

	// UDS 是通过 UDS 与 konnectivity server 通信的 UDS 配置
	// TCP 或 UDS 至少要设置其一
	// +optional
	UDS *UDSTransport
}

// TCPTransport 提供通过 TCP 连接 konnectivity server 的信息
//
// URL 的 scheme 决定是否启用 TLS：http:// 时不允许配置 TLSConfig。
type TCPTransport struct {
	// URL 是要连接的 konnectivity server 的位置。
	// 例如 "https://127.0.0.1:8131"
	URL string

	// TLSConfig 是连接 konnectivity server 时使用 TLS 所需的配置
	// +optional
	TLSConfig *TLSConfig
}

// UDSTransport 提供通过 UDS 连接 konnectivity server 的信息
//
// UDS 通常用于 node 本地，不经过网络，因此不需要 TLS 配置。
type UDSTransport struct {
	// UDSName 是连接 konnectivity server 所用的 unix domain socket 名称
	// 不使用 unix:// 前缀。（例如：/etc/srv/kubernetes/konnectivity-server/konnectivity-server.socket）
	UDSName string
}

// TLSConfig 提供连接 konnectivity server 的认证信息
// 仅与 TCPTransport 配合使用
//
// 这些字段都是“文件路径”而不是内联内容；apiserver 启动时读取，变更需重启生效。
type TLSConfig struct {
	// caBundle 是用于确定与 konnectivity server 信任关系的 CA 的文件位置。
	// 当 TCPTransport.URL 以 http:// 为前缀时必须缺省/为空
	// 当 TCPTransport.URL 以 https:// 为前缀且缺省时，默认使用系统信任根。
	// +optional
	CABundle string

	// clientKey 是用于向 konnectivity server 认证的客户端私钥的文件位置
	// 当 TCPTransport.URL 以 http:// 为前缀时必须缺省/为空
	// 当 TCPTransport.URL 以 https:// 为前缀时必须配置
	// +optional
	ClientKey string

	// clientCert 是用于向 konnectivity server 认证的客户端证书的文件位置
	// 当 TCPTransport.URL 以 http:// 为前缀时必须缺省/为空
	// 当 TCPTransport.URL 以 https:// 为前缀时必须配置
	// +optional
	ClientCert string

	// tlsServerName 用于校验服务端证书。若 tlsServerName 为空，则使用联系服务端时使用的主机名。
	// +optional
	TLSServerName string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TracingConfiguration 提供追踪客户端的版本化配置。
//
// 直接内嵌 component-base 的 tracing 配置，便于各组件复用同一套 sampling/endpoint 结构。
type TracingConfiguration struct {
	metav1.TypeMeta

	// 内嵌组件配置（component config）的 tracing 配置结构体
	tracingapi.TracingConfiguration
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthenticationConfiguration 提供认证的版本化配置。
//
// 该配置与 --anonymous-auth 互斥：一旦提供此对象，匿名认证只能通过 Anonymous 字段配置。
type AuthenticationConfiguration struct {
	metav1.TypeMeta

	// JWT 认证器按顺序尝试；每个 Issuer.URL 必须全局唯一。
	JWT []JWTAuthenticator

	// 若设置了该字段，则 --anonymous-auth 必须未被设置
	Anonymous *AnonymousAuthConfig
}

// AnonymousAuthConfig 提供匿名认证器的配置。
type AnonymousAuthConfig struct {
	Enabled bool

	// 若设置了本字段，则只有满足以下某个
	// 条件的请求才允许匿名认证。
	//
	// Conditions 为空时对所有请求生效；非空时只对命中的路径生效。
	Conditions []AnonymousAuthCondition
}

// AnonymousAuthCondition 描述应当启用匿名认证的
// 条件。
type AnonymousAuthCondition struct {
	// Path 是启用匿名认证所对应的请求路径。
	Path string
}

// JWTAuthenticator 提供单个 JWT 认证器的配置。
//
// 认证分两步：先按 ClaimValidationRules 校验 claim，再用 ClaimMappings 映射出
// user.Info，最后用 UserValidationRules 对拼装出的用户信息做最终校验。
type JWTAuthenticator struct {
	Issuer               Issuer
	ClaimValidationRules []ClaimValidationRule
	ClaimMappings        ClaimMappings
	UserValidationRules  []UserValidationRule
}

// Issuer 提供外部 provider 特定设置的配置。
//
// 一个 JWT 认证器绑定一个 issuer：iss claim、discovery 返回的 issuer 与这里的
// URL 必须三者一致，否则令牌会被拒绝。
type Issuer struct {
	// url 指向 issuer URL，格式为 https://url 或 https://url/path。
	// 该值必须与所提交 JWT 中的 "iss" claim 以及 discovery 返回的 issuer 一致。
	// 与 --oidc-issuer-url 参数的取值相同。
	// 除非被 discoveryURL 覆盖，discovery 信息将从 "{url}/.well-known/openid-configuration" 获取。
	// 在所有 JWT 认证器之间必须保持唯一。
	// 注意：该网络连接不使用 egress 选择配置。
	// +required
	URL string
	// discoveryURL 若指定，则覆盖用于获取 discovery 信息的 URL，
	// 不再使用 "{url}/.well-known/openid-configuration"。
	// 指定值会被原样使用，因此如有需要，必须在 discoveryURL 中包含
	// "/.well-known/openid-configuration"。
	//
	// 获取到的 discovery 信息中的 "issuer" 字段必须与 AuthenticationConfiguration 中的
	// "issuer.url" 字段一致，并将用于校验所提交 JWT 中的 "iss" claim。
	// 这适用于 well-known 与 jwks 端点与 issuer 不在同一位置（例如托管在集群本地）的场景。
	//
	// Example:
	// 一个通过命名空间 'oidc-namespace' 中的 kubernetes service 'oidc' 暴露的 discovery URL，
	// 且 discovery 信息位于 '/.well-known/openid-configuration'。
	// discoveryURL: "https://oidc.oidc-namespace/.well-known/openid-configuration"
	// certificateAuthority 用于验证 TLS 连接，叶子证书上的 hostname 必须
	// 设置为 'oidc.oidc-namespace'。
	//
	// curl https://oidc.oidc-namespace/.well-known/openid-configuration (.discoveryURL field)
	// {
	//     issuer: "https://oidc.example.com" (.url field)
	// }
	//
	// discoveryURL 必须与 url 不同。
	// 在所有 JWT 认证器之间必须保持唯一。
	// 注意：该网络连接不使用 egress 选择配置。
	// +optional
	DiscoveryURL string
	// CertificateAuthority 是指向 CA bundle 文件的路径，用于校验 issuer 的 TLS 证书。
	CertificateAuthority string
	// Audiences 是允许的 audience 列表；为空表示不校验 aud claim。
	Audiences []string
	// AudienceMatchPolicy 决定多个 audience 的匹配方式，当前仅支持 MatchAny。
	AudienceMatchPolicy AudienceMatchPolicyType
	// EgressSelectorType 指定访问 issuer 时使用的出口选择器类别。
	EgressSelectorType EgressSelectorType
}

// AudienceMatchPolicyType 是 Issuer.AudienceMatchPolicy 的一组合法取值
type AudienceMatchPolicyType string

// AudienceMatchPolicyType 的合法取值
const (
	// AudienceMatchPolicyMatchAny 表示 JWT 的 aud claim 与配置的任一 audience 相同即可。
	AudienceMatchPolicyMatchAny AudienceMatchPolicyType = "MatchAny"
)

// EgressSelectorType 标识访问外部服务时使用的出口类别，取值与
// EgressSelectorConfiguration 中的 Name 对应。
type EgressSelectorType string

const (
	EgressSelectorControlPlane EgressSelectorType = "controlplane"

	EgressSelectorCluster EgressSelectorType = "cluster"
)

// ClaimValidationRule 提供单条 claim 校验规则的配置。
//
// 校验规则要么要求 claim 等于某个固定值（Claim + RequiredValue），要么使用
// CEL 表达式（Expression）进行更复杂的判断；二者必须择一。
type ClaimValidationRule struct {
	Claim         string
	RequiredValue string

	Expression string
	Message    string
}

// ClaimMappings 提供 claim 映射的配置
//
// 定义如何把 JWT 的 claim 映射为 user.Info 的 username、groups、uid 和 extra。
type ClaimMappings struct {
	Username PrefixedClaimOrExpression
	Groups   PrefixedClaimOrExpression
	UID      ClaimOrExpression
	Extra    []ExtraMapping
}

// PrefixedClaimOrExpression 提供单个带前缀 claim 或表达式的配置。
//
// Claim 与 Expression 二选一；Prefix 只对 claim 形式生效，用于拼接用户名前缀。
type PrefixedClaimOrExpression struct {
	Claim  string
	Prefix *string

	Expression string
}

// ClaimOrExpression 提供单个 claim 或表达式的配置。
type ClaimOrExpression struct {
	Claim      string
	Expression string
}

// ExtraMapping 提供单个 extra 映射的配置。
//
// Key 是写入 user.Info.Extra 的键名，ValueExpression 是求值得到字符串列表的 CEL 表达式。
type ExtraMapping struct {
	Key             string
	ValueExpression string
}

// UserValidationRule 提供单条 user 校验规则的配置。
//
// 在映射完成后执行，用于拒绝映射结果（例如禁止非预期的用户名），Expression 必须为 bool。
type UserValidationRule struct {
	Expression string
	Message    string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthorizationConfiguration 是授权链的版本化配置入口。
type AuthorizationConfiguration struct {
	metav1.TypeMeta

	// Authorizers 是按顺序排列的授权器列表，
	// 请求将依次交由它们进行授权判定。
	// 这类似于 kube-apiserver 的 --authorization-modes 参数
	// 至少要配置一个。
	Authorizers []AuthorizerConfiguration `json:"authorizers"`
}

const (
	TypeWebhook                                          AuthorizerType = "Webhook"
	FailurePolicyNoOpinion                               string         = "NoOpinion"
	FailurePolicyDeny                                    string         = "Deny"
	AuthorizationWebhookConnectionInfoTypeKubeConfigFile string         = "KubeConfigFile"
	AuthorizationWebhookConnectionInfoTypeInCluster      string         = "InClusterConfig"
)

// AuthorizerType 标识授权器实现类型；通用 apiserver 只内置 Webhook。
type AuthorizerType string

// AuthorizerConfiguration 描述有序授权链中的单个授权器。
//
// Authorizers 列表顺序决定决策顺序：前面的授权器返回允许或拒绝即终止，
// 返回 NoOpinion 时继续交给下一个。
type AuthorizerConfiguration struct {
	// Type 指授权器的类型。
	// generic API server 支持 "Webhook"。
	// 其他 API server 可以支持额外的授权器
	// 类型，如 Node、RBAC、ABAC 等。
	Type AuthorizerType

	// Name 用于描述该 webhook。
	// 该名称会显式用于监控机制中的指标。
	// 注意：名称必须是 DNS1123 标签（如 `myauthorizername`）或
	//		 子域名（如 `myauthorizer.example.domain`）。
	// 必填，无默认值。
	Name string

	// Webhook 定义 Webhook 授权器的配置。
	// Type=Webhook 时必须定义。
	Webhook *WebhookConfiguration
}

// WebhookConfiguration 配置一个 webhook 授权器，包含缓存 TTL、超时、
// SubjectAccessReview 版本、失败策略与 matchConditions。
type WebhookConfiguration struct {
	// 缓存 webhook 授权器返回的 'authorized' 响应的时长。
	// 等同于设置 `--authorization-webhook-cache-authorized-ttl` 参数
	// Default: 5m0s
	AuthorizedTTL metav1.Duration
	// CacheAuthorizedRequests 指定是否应当缓存已授权（authorized）的请求。
	// 设置为 true 时，可以通过 AuthorizedTTL 字段
	// 配置缓存决策的 TTL。
	// Default: true
	CacheAuthorizedRequests bool
	// 缓存 webhook 授权器返回的 'unauthorized' 响应的时长。
	// 等同于设置 `--authorization-webhook-cache-unauthorized-ttl` 参数
	// Default: 30s
	UnauthorizedTTL metav1.Duration
	// CacheUnauthorizedRequests 指定是否应当缓存未授权（unauthorized）的请求。
	// 设置为 true 时，可以通过 UnauthorizedTTL 字段
	// 配置缓存决策的 TTL。
	// Default: true
	CacheUnauthorizedRequests bool
	// webhook 请求的超时时间
	// 允许的最大值为 30s。
	// 必填，无默认值。
	Timeout metav1.Duration
	// 发送给 webhook 以及期望 webhook 返回的 authorization.k8s.io
	// SubjectAccessReview 的 API 版本。
	// 等同于设置 `--authorization-webhook-version` 参数
	// Valid values: v1beta1, v1
	// 必填，无默认值
	SubjectAccessReviewVersion string
	// MatchConditionSubjectAccessReviewVersion 指定 CEL 表达式求值所针对的
	// SubjectAccessReview 版本
	// Valid values: v1
	// 必填，无默认值
	MatchConditionSubjectAccessReviewVersion string
	// 控制 webhook 请求未能完成、返回格式错误的响应、或求值
	// matchConditions 出错时的授权决策。
	// Valid values:
	//   - NoOpinion: 继续交由后续授权器判断是否有授权器允许该请求
	//   - Deny: 拒绝该请求，不再咨询后续授权器
	// 必填，且无默认值。
	FailurePolicy string

	// ConnectionInfo 定义我们与 webhook 通信的方式
	ConnectionInfo WebhookConnectionInfo

	// matchConditions 是请求必须满足才会被发送到该 webhook 的条件列表。
	// 空的 matchConditions 列表匹配所有请求。
	// 最多允许 64 个匹配条件。
	//
	// 精确的匹配逻辑为（按顺序）：
	//   1. 如果至少一个 matchCondition 求值为 FALSE，则跳过该 webhook。
	//   2. 如果所有 matchConditions 都求值为 TRUE，则调用该 webhook。
	//   3. 如果至少一个 matchCondition 求值出错（但没有一个为 FALSE）：
	//      - 如果 failurePolicy=Deny，则该 webhook 拒绝该请求
	//      - 如果 failurePolicy=NoOpinion，则忽略错误并跳过该 webhook
	MatchConditions []WebhookMatchCondition
}

// WebhookConnectionInfo 描述如何连接到授权 webhook 服务。
type WebhookConnectionInfo struct {
	// 控制 webhook 应如何与服务器通信。
	// Valid values:
	// - KubeConfigFile: 使用 kubeConfigFile 中指定的文件来定位
	//   服务器。
	// - InClusterConfig: 使用集群内配置来调用由 kube-apiserver 托管的
	//   SubjectAccessReview API。kube-apiserver 自身不允许使用此模式。
	Type string

	// 连接信息中 KubeConfigFile 的文件路径
	// 必填，当 connectionInfo.Type 为 KubeConfig 时
	KubeConfigFile *string
}

// WebhookMatchCondition 是一条在调用 webhook 之前求值的 CEL 条件。
// 条件为 false 时跳过 webhook；条件求值出错时按 FailurePolicy 处理。
type WebhookMatchCondition struct {
	// expression 表示将由 CEL 求值的表达式。必须求值为布尔值。
	// CEL 表达式可以访问 v1 版本 SubjectAccessReview 的内容。
	// 如果请求变量中 subjectAccessReviewVersion 指定的版本是 v1beta1，
	// 则在求值 CEL 表达式之前会先转换为 v1 版本。
	//
	// - 'resourceAttributes' 描述资源访问请求的信息，对非资源请求未设置。例如 has(request.resourceAttributes) && request.resourceAttributes.namespace == 'default'
	// - 'nonResourceAttributes' 描述非资源访问请求的信息，对资源请求未设置。例如 has(request.nonResourceAttributes) && request.nonResourceAttributes.path == '/healthz'。
	// - 'user' 是要测试的用户。例如 request.user == 'alice'
	// - 'groups' 是要测试的用户组。例如 ('group1' in request.groups)
	// - 'extra' 对应认证器提供的 user.Info.GetExtra() 方法。
	// - 'uid' 是关于发起请求用户的信息。例如 request.uid == '1'
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	Expression string
}
