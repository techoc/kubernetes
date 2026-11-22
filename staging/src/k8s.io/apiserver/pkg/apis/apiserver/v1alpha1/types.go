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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	tracingapi "k8s.io/component-base/tracing/api/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AdmissionConfiguration 是准入控制器的版本化配置。
// 通过 --admission-control-config-file 加载后会被转换到内部版本。
type AdmissionConfiguration struct {
	metav1.TypeMeta `json:""`

	// Plugins 允许为每个准入插件单独指定配置。
	// +optional
	Plugins []AdmissionPluginConfiguration `json:"plugins"`
}

// AdmissionPluginConfiguration 是单个插件的配置。
type AdmissionPluginConfiguration struct {
	// Name 是准入控制器的名称，必须与已注册的插件名一致。
	Name string `json:"name"`

	// Path 指向包含插件配置的文件。
	// +optional
	Path string `json:"path"`

	// Configuration 是内嵌的插件配置对象；若存在则优先于 Path 使用。
	// 使用 runtime.Unknown 延迟解码，使不同插件可以各自定义配置类型。
	// +optional
	Configuration *runtime.Unknown `json:"configuration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EgressSelectorConfiguration 是 egress selector 客户端的版本化配置。
type EgressSelectorConfiguration struct {
	metav1.TypeMeta `json:""`

	// EgressSelections 是各 egress 客户端配置的列表
	EgressSelections []EgressSelection `json:"egressSelections"`
}

// EgressSelection 是单个 egress 客户端配置。
type EgressSelection struct {
	// Name 是 egress 选择的名称。
	// 目前支持 "controlplane"、"master"、"etcd" 和 "cluster"。
	// "master" 已弃用，应改用 "controlplane"。
	Name string `json:"name"`

	// Connection 是配置该 egress 选择所需的具体信息
	Connection Connection `json:"connection"`
}

// Connection 是单个 egress 客户端的连接配置。
type Connection struct {
	// ProxyProtocol 是从客户端连接到 konnectivity server 所用的协议。
	ProxyProtocol ProtocolType `json:"proxyProtocol,omitempty"`

	// Transport 定义拨号到 konnectivity server 的传输配置。
	// ProxyProtocol 为 HTTPConnect 或 GRPC 时必填。
	// +optional
	Transport *Transport `json:"transport,omitempty"`
}

// ProtocolType 是 Connection.ProxyProtocol 的合法取值集合。
type ProtocolType string

// konnectivity server 的 ProtocolType 合法取值
const (
	// 使用 HTTPConnect 连接 konnectivity server
	ProtocolHTTPConnect ProtocolType = "HTTPConnect"
	// 使用 grpc 连接 konnectivity server
	ProtocolGRPC ProtocolType = "GRPC"
	// 直连（跳过 konnectivity server）
	ProtocolDirect ProtocolType = "Direct"
)

// Transport 定义拨号到 konnectivity server 的传输配置。
type Transport struct {
	// TCP 是通过 TCP 与 konnectivity server 通信的配置。
	// 目前 TCP transport 不支持 GRPC 的 ProxyProtocol。
	// TCP 与 UDS 至少需设置一个。
	// +optional
	TCP *TCPTransport `json:"tcp,omitempty"`

	// UDS 是通过 UDS 与 konnectivity server 通信的配置。
	// TCP 与 UDS 至少需设置一个。
	// +optional
	UDS *UDSTransport `json:"uds,omitempty"`
}

// TCPTransport 提供通过 TCP 连接 konnectivity server 的信息。
type TCPTransport struct {
	// URL 是 konnectivity server 的地址，例如 "https://127.0.0.1:8131"
	URL string `json:"url,omitempty"`

	// TLSConfig 是连接 konnectivity server 时使用 TLS 所需的配置
	// +optional
	TLSConfig *TLSConfig `json:"tlsConfig,omitempty"`
}

// UDSTransport 提供通过 UDS 连接 konnectivity server 的信息。
type UDSTransport struct {
	// UDSName 是连接 konnectivity server 的 unix domain socket 名称。
	// 不带 unix:// 前缀（例如 /etc/srv/kubernetes/konnectivity-server/konnectivity-server.socket）
	UDSName string `json:"udsName,omitempty"`
}

// TLSConfig 是连接 konnectivity server 的认证信息，仅用于 TCPTransport。
type TLSConfig struct {
	// CABundle 是用于确定对 konnectivity server 信任的 CA 文件路径。
	// TCPTransport.URL 以 http:// 开头时必须为空。
	// URL 以 https:// 开头且未设置时，使用系统信任根。
	// +optional
	CABundle string `json:"caBundle,omitempty"`

	// ClientKey 是与 konnectivity server 进行 mTLS 握手时客户端私钥的文件路径。
	// URL 以 http:// 开头时必须为空；以 https:// 开头时必须配置。
	// +optional
	ClientKey string `json:"clientKey,omitempty"`

	// ClientCert 是与 konnectivity server 进行 mTLS 握手时客户端证书的文件路径。
	// URL 以 http:// 开头时必须为空；以 https:// 开头时必须配置。
	// +optional
	ClientCert string `json:"clientCert,omitempty"`

	// TLSServerName 用于校验服务端证书；为空时使用连接时使用的 hostname。
	// +optional
	TLSServerName string `json:"tlsServerName,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TracingConfiguration 是 tracing 客户端的版本化配置。
type TracingConfiguration struct {
	metav1.TypeMeta `json:""`

	// 嵌入 component-base 的 tracing 配置结构体
	tracingapi.TracingConfiguration `json:""`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthenticationConfiguration 是认证的版本化配置。
type AuthenticationConfiguration struct {
	metav1.TypeMeta `json:""`

	// JWT 是使用 JWT 兼容令牌认证 Kubernetes 用户的认证器列表。
	// 认证器会解析原始 ID token，并验证其由配置的 issuer 签名；
	// 验签公钥通过 OIDC discovery 从 issuer 的公开端点获取。
	// 对于传入的 token，各 JWT 认证器按本列表顺序尝试。但其他认证器
	// 可能在 JWT 认证器之前或之后运行，其相对位置既不保证也不稳定。
	// 由于每个 JWT 认证器必须有唯一的 issuer URL，最多只有一个
	// JWT 认证器会真正对 token 做密码学验证。
	//
	// 合法的 JWT payload 至少需要包含以下 claims：
	// {
	//		"iss": "https://issuer.example.com",
	//		"aud": ["audience"],
	//		"exp": 1234567890,
	//		"<username claim>": "username"
	// }
	JWT []JWTAuthenticator `json:"jwt"`

	// 若设置，则 --anonymous-auth 不得同时开启
	Anonymous *AnonymousAuthConfig `json:"anonymous,omitempty"`
}

// AnonymousAuthConfig 是匿名认证器的配置。
type AnonymousAuthConfig struct {
	// Enabled 表示是否启用匿名认证。
	Enabled bool `json:"enabled"`

	// 若设置，匿名认证仅在请求满足其中一个条件时允许。
	Conditions []AnonymousAuthCondition `json:"conditions,omitempty"`
}

// AnonymousAuthCondition 描述匿名认证应启用的条件。
type AnonymousAuthCondition struct {
	// Path 是启用匿名认证的请求路径。
	Path string `json:"path"`
}

// JWTAuthenticator 是单个 JWT 认证器的配置。
type JWTAuthenticator struct {
	// Issuer 包含 OIDC provider 的基础连接选项。
	// +required
	Issuer Issuer `json:"issuer"`

	// ClaimValidationRules 是用于校验 token claims 以认证用户的规则。
	// +optional
	ClaimValidationRules []ClaimValidationRule `json:"claimValidationRules,omitempty"`

	// ClaimMappings 把 token 的 claims 映射为用户属性。
	// +required
	ClaimMappings ClaimMappings `json:"claimMappings"`

	// UserValidationRules 是在完成认证前对最终用户执行的规则。
	// 可用于对传入身份施加不变量，例如禁止使用 Kubernetes 组件常用的
	// system: 前缀。规则之间逻辑与，必须全部返回 true 才能通过校验。
	// +optional
	UserValidationRules []UserValidationRule `json:"userValidationRules,omitempty"`
}

// Issuer 是外部 provider 的特定设置。
type Issuer struct {
	// URL 指向 issuer URL，格式为 https://url 或 https://url/path。
	// 必须与所呈现 JWT 的 "iss" claim 以及 discovery 返回的 issuer 一致。
	// 等价于 --oidc-issuer-url 参数。
	// 除非被 discoveryURL 覆盖，否则从 "{url}/.well-known/openid-configuration" 获取 discovery 信息。
	// 在所有 JWT 认证器中必须唯一。
	// 注意：此网络连接不使用 egress selector 配置。
	// +required
	URL string `json:"url"`

	// DiscoveryURL 若指定，则覆盖用于获取 discovery 信息的 URL，
	// 不再使用 "{url}/.well-known/openid-configuration"。
	// 该值按原样使用，因此如有需要必须自行包含 "/.well-known/openid-configuration"。
	//
	// 获取到的 discovery 信息中的 "issuer" 字段必须与 AuthenticationConfiguration 中的
	// "issuer.url" 一致，并用于校验所呈现 JWT 的 "iss" claim。
	// 这适用于 well-known 与 jwks 端点托管在 issuer 之外（例如集群本地）的场景。
	//
	// 示例：
	// 通过命名空间 oidc-namespace 中的 kubernetes service 'oidc' 暴露 discovery，
	// 且 discovery 信息位于 '/.well-known/openid-configuration'。
	// discoveryURL: "https://oidc.oidc-namespace/.well-known/openid-configuration"
	// certificateAuthority 用于校验 TLS 连接，叶子证书的 hostname 必须为 'oidc.oidc-namespace'。
	//
	// curl https://oidc.oidc-namespace/.well-known/openid-configuration (.discoveryURL field)
	// {
	//     issuer: "https://oidc.example.com" (.url field)
	// }
	//
	// discoveryURL 必须与 url 不同。
	// 在所有 JWT 认证器中必须唯一。
	// 注意：此网络连接不使用 egress selector 配置。
	// +optional
	DiscoveryURL *string `json:"discoveryURL,omitempty"`

	// CertificateAuthority 包含 PEM 编码的 CA 证书，用于校验获取 discovery 信息时的连接。
	// 未设置时使用系统验证器。
	// 等价于 --oidc-ca-file 参数所引用文件的内容。
	// +optional
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// Audiences 是 JWT 必须签发给的可接受受众集合。
	// 至少有一个条目需与所呈现 JWT 的 "aud" claim 匹配。
	// 等价于 --oidc-client-id 参数（但此字段支持数组）。
	// 必须非空。
	// +required
	Audiences []string `json:"audiences"`

	// AudienceMatchPolicy 定义如何用 "audiences" 字段匹配所呈现 JWT 的 "aud" claim。
	// 允许的取值：
	// 1. 指定多个 audiences 时为 "MatchAny"；
	// 2. 指定单个 audience 时为空（或未设置）或 "MatchAny"。
	//
	// - MatchAny：所呈现 JWT 的 "aud" claim 必须至少匹配 "audiences" 中的一个条目。
	// 例如 "audiences" 为 ["foo", "bar"] 时，"aud" claim 必须包含 "foo" 或 "bar"（可同时包含）。
	//
	// - ""：当 "audiences" 只指定一个受众时可为空（或未设置）。"aud" claim 必须包含该受众（可包含其他）。
	//
	// 更精细的受众校验请使用 claimValidationRules。
	//   例如：claimValidationRule[].expression: 'sets.equivalent(claims.aud, ["bar", "foo", "baz"])' 要求完全匹配。
	// +optional
	AudienceMatchPolicy AudienceMatchPolicyType `json:"audienceMatchPolicy,omitempty"`

	// EgressSelectorType 指示与该 issuer 相关的所有流量（discovery、JWKS、distributed claims 等）
	// 应使用哪个 egress selector。未指定时不使用自定义 dialer。
	// 指定时合法取值为 "controlplane" 与 "cluster"，分别对应
	// --egress-selector-config-file 中的同名配置。
	//
	// - controlplane：用于发往控制平面的流量。
	//
	// - cluster：用于发往 Kubernetes 所管理系统的流量。
	//
	// +optional
	EgressSelectorType EgressSelectorType `json:"egressSelectorType,omitempty"`
}

// AudienceMatchPolicyType 是 issuer.audienceMatchPolicy 的合法取值集合。
type AudienceMatchPolicyType string

// AudienceMatchPolicyType 的合法取值
const (
	// MatchAny 表示所呈现 JWT 的 "aud" claim 必须至少匹配 "audiences" 中的一个条目。
	AudienceMatchPolicyMatchAny AudienceMatchPolicyType = "MatchAny"
)

// EgressSelectorType 指示发送流量时应使用哪个 egress selector。
type EgressSelectorType string

const (
	// EgressSelectorControlPlane 表示发往控制平面的流量。
	EgressSelectorControlPlane EgressSelectorType = "controlplane"

	// EgressSelectorCluster 表示发往 Kubernetes 所管理系统的流量。
	EgressSelectorCluster EgressSelectorType = "cluster"
)

// ClaimValidationRule 是单条 claim 校验规则的配置。
type ClaimValidationRule struct {
	// Claim 是必需 claim 的名称。
	// 等价于 --oidc-required-claim 参数。
	// 仅支持字符串类型的 claim key。
	// 与 expression、message 互斥。
	// +optional
	Claim string `json:"claim,omitempty"`
	// RequiredValue 是必需 claim 的取值。
	// 等价于 --oidc-required-claim 参数。
	// 仅支持字符串类型的 claim 值。
	// 若设置了 claim 而未设置 requiredValue，则该 claim 必须存在且值为空字符串。
	// 与 expression、message 互斥。
	// +optional
	RequiredValue string `json:"requiredValue,omitempty"`

	// Expression 是交由 CEL 求值的表达式，必须返回布尔值。
	//
	// CEL 表达式可访问 token claims 的内容，组织为 CEL 变量：
	// - 'claims' 是 claim 名称到 claim 值的映射。
	//   例如名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claim 可用点号访问，例如 'claims.foo.bar'。
	// 必须返回 true 才能通过校验。
	//
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 与 claim、requiredValue 互斥。
	// +optional
	Expression string `json:"expression,omitempty"`
	// Message 用于自定义 expression 返回 false 时的错误信息，必须是字面量字符串。
	// 与 claim、requiredValue 互斥。
	// +optional
	Message string `json:"message,omitempty"`
}

// ClaimMappings 是 claim 映射的配置。
type ClaimMappings struct {
	// Username 是 username 属性的配置选项。
	// 该 claim 的值必须是单个字符串。
	// 等价于 --oidc-username-claim 与 --oidc-username-prefix 参数。
	// 若设置了 username.expression，表达式必须产生字符串值。
	// 若 username.expression 使用 'claims.email'，则必须在
	// username.expression 或 extra[*].valueExpression 或 claimValidationRules[*].expression 中使用 'claims.email_verified'。
	// 一个与“username.claim 设为 'email' 时自动施加的校验”等价的 claim 校验规则表达式为
	// 'claims.?email_verified.orValue(true) == true'。显式与 true 比较可让类型检查看到结果是布尔值，
	// 并确保非布尔的 email_verified claim 会在运行时被捕获。
	//
	// 在基于参数的方式中，--oidc-username-claim 与 --oidc-username-prefix 是可选的；
	// 未设置 --oidc-username-claim 时默认值为 "sub"。在认证配置中没有默认值，
	// claim 与前缀都必须显式设置。
	// 若旧参数方式未设置 --oidc-username-claim，请在认证配置中设置 username.claim="sub"。
	// 前缀：
	//     (1) --oidc-username-prefix="-"，用户名不加前缀。认证配置中对应 username.prefix=""
	//     (2) --oidc-username-prefix="" 且 --oidc-username-claim != "email"，前缀为 "<value of --oidc-issuer-url>#"。
	//         认证配置中对应 username.prefix="<value of issuer.url>#"
	//     (3) --oidc-username-prefix="<value>"。认证配置中对应 username.prefix="<value>"
	// +required
	Username PrefixedClaimOrExpression `json:"username"`
	// Groups 是 groups 属性的配置选项。
	// 该 claim 的值必须是字符串或字符串数组。
	// 若设置了 groups.claim，则必须指定前缀（可为空字符串）。
	// 若设置了 groups.expression，表达式必须产生字符串或字符串数组。
	// "", [] 和 null 视为未提供 group 映射。
	// +optional
	Groups PrefixedClaimOrExpression `json:"groups,omitempty"`

	// UID 是 uid 属性的配置选项。
	// claim 必须是单个字符串 claim。
	// 若设置了 uid.expression，表达式必须产生字符串值。
	// +optional
	UID ClaimOrExpression `json:"uid"`

	// Extra 是 extra 属性的配置选项。
	// expression 必须产生字符串或字符串数组。
	// 若值为空，则不添加该 extra 映射。
	//
	// 硬编码 key/value
	// - key: "foo"
	//   valueExpression: "'bar'"
	// 结果为 extra 属性 - foo: ["bar"]
	//
	// 硬编码 key，value 复制 claim 值
	// - key: "foo"
	//   valueExpression: "claims.some_claim"
	// 结果为 extra 属性 - foo: [value of some_claim]
	//
	// 硬编码 key，value 由 claim 值推导
	// - key: "admin"
	//   valueExpression: '(has(claims.is_admin) && claims.is_admin) ? "true":""'
	// 结果为：
	//  - 若 is_admin claim 存在且为 true，extra 属性 - admin: ["true"]
	//  - 若 is_admin claim 存在但为 false，或不存在，则不添加 extra 属性
	//
	// +optional
	Extra []ExtraMapping `json:"extra,omitempty"`
}

// PrefixedClaimOrExpression 是单个带前缀的 claim 或表达式的配置。
type PrefixedClaimOrExpression struct {
	// Claim 是要使用的 JWT claim。
	// 与 expression 互斥。
	// +optional
	Claim string `json:"claim,omitempty"`
	// Prefix 会加在 claim 值之前，避免与已有名称冲突。
	// 若设置了 claim 则必须设置 prefix，且可为空字符串。
	// 与 expression 互斥。
	// +optional
	Prefix *string `json:"prefix,omitempty"`

	// Expression 是交由 CEL 求值的表达式。
	//
	// CEL 表达式可访问 token claims 的内容，组织为 CEL 变量：
	// - 'claims' 是 claim 名称到 claim 值的映射。
	//   例如名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claim 可用点号访问，例如 'claims.foo.bar'。
	//
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 与 claim、prefix 互斥。
	// +optional
	Expression string `json:"expression,omitempty"`
}

// ClaimOrExpression 是单个 claim 或表达式的配置。
type ClaimOrExpression struct {
	// Claim 是要使用的 JWT claim。
	// claim 与 expression 必须设置其中一个。
	// 与 expression 互斥。
	// +optional
	Claim string `json:"claim,omitempty"`

	// Expression 是交由 CEL 求值的表达式。
	//
	// CEL 表达式可访问 token claims 的内容，组织为 CEL 变量：
	// - 'claims' 是 claim 名称到 claim 值的映射。
	//   例如名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claim 可用点号访问，例如 'claims.foo.bar'。
	//
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 与 claim 互斥。
	// +optional
	Expression string `json:"expression,omitempty"`
}

// ExtraMapping 是单条 extra 映射的配置。
type ExtraMapping struct {
	// Key 是用作 extra 属性 key 的字符串。
	// 必须是 domain-prefix 路径（如 example.org/foo）。第一个 "/" 之前的字符必须是符合
	// RFC 1123 的合法子域名，之后必须是符合 RFC 3986 的合法 HTTP Path 字符。
	// 必须小写。
	// 必须唯一。
	// +required
	Key string `json:"key"`

	// ValueExpression 是提取 extra 属性值的 CEL 表达式。
	// 必须产生字符串或字符串数组。
	// "", [] 和 null 视为不添加该 extra 映射。
	// 字符串数组中的空字符串元素会被过滤。
	//
	// CEL 表达式可访问 token claims 的内容，组织为 CEL 变量：
	// - 'claims' 是 claim 名称到 claim 值的映射。
	//   例如名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claim 可用点号访问，例如 'claims.foo.bar'。
	//
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// +required
	ValueExpression string `json:"valueExpression"`
}

// UserValidationRule 是单条用户信息校验规则的配置。
type UserValidationRule struct {
	// Expression 是交由 CEL 求值的表达式，必须返回 true 才能通过校验。
	//
	// CEL 表达式可访问 UserInfo 的内容，组织为 CEL 变量：
	// - 'user' - authentication.k8s.io/v1, Kind=UserInfo 对象
	//    定义参见 https://github.com/kubernetes/api/blob/release-1.28/authentication/v1/types.go#L105-L122
	//    API 文档：https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#userinfo-v1-authentication-k8s-io
	//
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	//
	// +required
	Expression string `json:"expression"`

	// Message 用于自定义规则返回 false 时的错误信息，必须是字面量字符串。
	// +optional
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthorizationConfiguration 是授权的版本化配置。
type AuthorizationConfiguration struct {
	metav1.TypeMeta `json:""`

	// Authorizers 是按顺序尝试的授权器列表，请求依次与每个授权器比对。
	// 类似 --authorization-modes kube-apiserver 参数。
	// 至少需要一个。
	Authorizers []AuthorizerConfiguration `json:"authorizers"`
}

const (
	// TypeWebhook 是通用 apiserver 支持的授权器类型。
	TypeWebhook AuthorizerType = "Webhook"
	// FailurePolicyNoOpinion 表示 webhook 失败时继续咨询后续授权器。
	FailurePolicyNoOpinion string = "NoOpinion"
	// FailurePolicyDeny 表示 webhook 失败时直接拒绝请求。
	FailurePolicyDeny string = "Deny"
	// AuthorizationWebhookConnectionInfoTypeKubeConfigFile 表示使用 kubeconfig 文件连接 webhook。
	AuthorizationWebhookConnectionInfoTypeKubeConfigFile string = "KubeConfigFile"
	// AuthorizationWebhookConnectionInfoTypeInCluster 表示使用集群内配置连接 webhook。
	AuthorizationWebhookConnectionInfoTypeInCluster string = "InClusterConfig"
)

// AuthorizerType 是授权器的类型。
type AuthorizerType string

// AuthorizerConfiguration 是单个授权器的配置。
type AuthorizerConfiguration struct {
	// Type 是授权器类型。
	// 通用 apiserver 支持 "Webhook"。
	// 其他 apiserver 可能支持 Node、RBAC、ABAC 等额外类型。
	Type string `json:"type"`

	// Name 用于描述该 webhook，会显式出现在监控指标中。
	// 注意：必须是 DNS1123 label（如 `myauthorizername`）或
	//		 子域名（如 `myauthorizer.example.domain`）。
	// 必填，无默认值。
	Name string `json:"name"`

	// Webhook 定义 Webhook 授权器的配置。
	// Type=Webhook 时必须设置；Type!=Webhook 时不得设置。
	Webhook *WebhookConfiguration `json:"webhook,omitempty"`
}

// WebhookConfiguration 是 Webhook 授权器的配置。
type WebhookConfiguration struct {
	// 缓存 webhook 授权器返回的 'authorized' 响应的时长。
	// 等价于设置 `--authorization-webhook-cache-authorized-ttl` 参数。
	// 默认：5m0s
	AuthorizedTTL metav1.Duration `json:"authorizedTTL"`
	// CacheAuthorizedRequests 指定是否缓存已授权的请求。
	// 设为 true 时可通过 AuthorizedTTL 配置缓存时长。
	// 默认：true
	// +optional
	CacheAuthorizedRequests *bool `json:"cacheAuthorizedRequests,omitempty"`
	// 缓存 webhook 授权器返回的 'unauthorized' 响应的时长。
	// 等价于设置 `--authorization-webhook-cache-unauthorized-ttl` 参数。
	// 默认：30s
	UnauthorizedTTL metav1.Duration `json:"unauthorizedTTL"`
	// CacheUnauthorizedRequests 指定是否缓存未授权的请求。
	// 设为 true 时可通过 UnauthorizedTTL 配置缓存时长。
	// 默认：true
	// +optional
	CacheUnauthorizedRequests *bool `json:"cacheUnauthorizedRequests,omitempty"`
	// webhook 请求超时。
	// 最大允许 30s。
	// 必填，无默认值。
	Timeout metav1.Duration `json:"timeout"`
	// 发送给 webhook 并期望其返回的 authorization.k8s.io SubjectAccessReview 版本。
	// 等价于设置 `--authorization-webhook-version` 参数。
	// 合法值：v1beta1、v1
	// 必填，无默认值
	SubjectAccessReviewVersion string `json:"subjectAccessReviewVersion"`
	// MatchConditionSubjectAccessReviewVersion 指定 CEL 表达式求值时所用的 SubjectAccessReview 版本。
	// 合法值：v1
	// 必填，无默认值
	MatchConditionSubjectAccessReviewVersion string `json:"matchConditionSubjectAccessReviewVersion"`
	// 控制 webhook 请求失败、返回格式错误或 matchConditions 求值出错时的授权决定。
	// 合法值：
	//   - NoOpinion：继续咨询后续授权器，看是否有授权器允许该请求
	//   - Deny：直接拒绝请求，不再咨询后续授权器
	// 必填，无默认值。
	FailurePolicy string `json:"failurePolicy"`

	// ConnectionInfo 定义如何与 webhook 通信
	ConnectionInfo WebhookConnectionInfo `json:"connectionInfo"`

	// MatchConditions 是请求发往该 webhook 前必须满足的条件列表。
	// 空列表匹配所有请求。最多允许 64 个条件。
	//
	// 具体匹配逻辑（按顺序）：
	//   1. 若至少一个 matchCondition 求值为 FALSE，则跳过该 webhook。
	//   2. 若所有 matchCondition 求值为 TRUE，则调用该 webhook。
	//   3. 若至少一个 matchCondition 求值出错（但没有 FALSE）：
	//      - failurePolicy=Deny 时，webhook 拒绝请求
	//      - failurePolicy=NoOpinion 时，忽略错误并跳过该 webhook
	MatchConditions []WebhookMatchCondition `json:"matchConditions"`
}

// WebhookConnectionInfo 定义与 webhook 通信的方式。
type WebhookConnectionInfo struct {
	// 控制 webhook 与 server 的通信方式。
	// 合法值：
	// - KubeConfigFile：使用 kubeConfigFile 指定的文件定位 server。
	// - InClusterConfig：使用集群内配置调用 kube-apiserver 托管的
	//   SubjectAccessReview API。kube-apiserver 不允许使用此模式。
	Type string `json:"type"`

	// KubeConfigFile 的路径
	// connectionInfo.Type 为 KubeConfig 时必填
	KubeConfigFile *string `json:"kubeConfigFile"`
}

// WebhookMatchCondition 是单条 webhook 匹配条件。
type WebhookMatchCondition struct {
	// Expression 是交由 CEL 求值的表达式，必须求值为 bool。
	// CEL 表达式可访问 v1 版本的 SubjectAccessReview 内容。
	// 若请求中 subjectAccessReviewVersion 指定的版本为 v1beta1，
	// 内容会先转换为 v1 再求值。
	//
	// - 'resourceAttributes' 描述资源访问请求的信息，非资源请求时未设置。例如 has(request.resourceAttributes) && request.resourceAttributes.namespace == 'default'
	// - 'nonResourceAttributes' 描述非资源访问请求的信息，资源请求时未设置。例如 has(request.nonResourceAttributes) && request.nonResourceAttributes.path == '/healthz'。
	// - 'user' 是要测试的用户。例如 request.user == 'alice'
	// - 'groups' 是要测试的组。例如 ('group1' in request.groups)
	// - 'extra' 对应认证器的 user.Info.GetExtra() 方法。
	// - 'uid' 是请求用户的信息。例如 request.uid == '1'
	//
	// CEL 文档：https://kubernetes.io/docs/reference/using-api/cel/
	Expression string `json:"expression"`
}
