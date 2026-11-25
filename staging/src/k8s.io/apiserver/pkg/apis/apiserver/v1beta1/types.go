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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	tracingapi "k8s.io/component-base/tracing/api/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EgressSelectorConfiguration 是 egress 选择器配置的外部版本：apiserver 在
// 访问控制面、etcd、集群内服务等不同目标时，按此配置选择代理或直连。
type EgressSelectorConfiguration struct {
	metav1.TypeMeta `json:""`

	// 包含一组 egress selection 客户端配置；每个条目对应一种出口用途；同一 name 重复出现属于配置错误，需在校验阶段拒绝。
	EgressSelections []EgressSelection `json:"egressSelections"`
}

// EgressSelection 描述某一类出口流量的连接方式。
type EgressSelection struct {
	// name 是 egress 选择的名称，目前支持 "controlplane"、"master"、"etcd" 和 "cluster"；
	// "master" 为兼容旧配置保留，新配置应使用 "controlplane"。
	Name string `json:"name"`

	// connection 是配置 egress selection 的具体连接参数；未设置时按直连处理。
	Connection Connection `json:"connection"`
}

// Connection 决定与 egress 代理（如 konnectivity server）通信的协议及传输层参数。
type Connection struct {
	// ProxyProtocol 是客户端连接 konnectivity server 使用的协议；
	// 为 HTTPConnect 或 GRPC 时必须提供 Transport；Direct 表示不经代理。
	ProxyProtocol ProtocolType `json:"proxyProtocol,omitempty"`

	// Transport defines the transport configurations we use to dial to the konnectivity server.
	// This is required if ProxyProtocol is HTTPConnect or GRPC.
	// +optional
	Transport *Transport `json:"transport,omitempty"`
}

// ProtocolType 限定可用协议，避免配置中出现拼写错误导致静默直连。
type ProtocolType string

// Valid types for ProtocolType for konnectivity server
const (
	// Use HTTPConnect to connect to konnectivity server
	ProtocolHTTPConnect ProtocolType = "HTTPConnect"
	// Use grpc to connect to konnectivity server
	ProtocolGRPC ProtocolType = "GRPC"
	// Connect directly (skip konnectivity server)
	ProtocolDirect ProtocolType = "Direct"
)

// Transport 是传输层的联合配置，TCP 与 UDS 互斥使用；至少配置其一。
type Transport struct {
	// TCP is the TCP configuration for communicating with the konnectivity server via TCP
	// ProxyProtocol of GRPC is not supported with TCP transport at the moment
	// Requires at least one of TCP or UDS to be set
	// +optional
	TCP *TCPTransport `json:"tcp,omitempty"`

	// UDS is the UDS configuration for communicating with the konnectivity server via UDS
	// Requires at least one of TCP or UDS to be set
	// +optional
	UDS *UDSTransport `json:"uds,omitempty"`
}

// TCPTransport 通过 URL 指向代理地址；https 时会启用 TLS 配置。
type TCPTransport struct {
	// URL is the location of the konnectivity server to connect to.
	// As an example it might be "https://127.0.0.1:8131"
	URL string `json:"url,omitempty"`

	// TLSConfig is the config needed to use TLS when connecting to konnectivity server
	// +optional
	TLSConfig *TLSConfig `json:"tlsConfig,omitempty"`
}

// UDSTransport 仅用于 Unix domain socket，路径不带 unix:// 前缀。
type UDSTransport struct {
	// UDSName is the name of the unix domain socket to connect to konnectivity server
	// This does not use a unix:// prefix. (Eg: /etc/srv/kubernetes/konnectivity-server/konnectivity-server.socket)
	UDSName string `json:"udsName,omitempty"`
}

// TLSConfig 提供连接 konnectivity server 的认证信息，仅与 TCPTransport 配合使用；
// 字段均为文件路径；http:// URL 下必须留空，https:// 下要求
// 同时提供客户端证书与私钥做 mTLS，CABundle 为空则使用系统信任根。
type TLSConfig struct {
	// caBundle is the file location of the CA to be used to determine trust with the konnectivity server.
	// Must be absent/empty if TCPTransport.URL is prefixed with http://
	// If absent while TCPTransport.URL is prefixed with https://, default to system trust roots.
	// +optional
	CABundle string `json:"caBundle,omitempty"`

	// clientKey is the file location of the client key to be used in mtls handshakes with the konnectivity server.
	// Must be absent/empty if TCPTransport.URL is prefixed with http://
	// Must be configured if TCPTransport.URL is prefixed with https://
	// +optional
	ClientKey string `json:"clientKey,omitempty"`

	// clientCert is the file location of the client certificate to be used in mtls handshakes with the konnectivity server.
	// Must be absent/empty if TCPTransport.URL is prefixed with http://
	// Must be configured if TCPTransport.URL is prefixed with https://
	// +optional
	ClientCert string `json:"clientCert,omitempty"`

	// tlsServerName is used to check server certificate. If tlsServerName is empty, the hostname used to contact the server is used.
	// +optional
	TLSServerName string `json:"tlsServerName,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TracingConfiguration 直接内嵌组件配置中的 tracing 结构，复用其字段与语义。
type TracingConfiguration struct {
	metav1.TypeMeta `json:""`

	// Embed the component config tracing configuration struct
	tracingapi.TracingConfiguration `json:""`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthenticationConfiguration 取代旧的 --oidc-* 命令行参数，是认证器的声明式配置。
type AuthenticationConfiguration struct {
	metav1.TypeMeta `json:""`

	// jwt 是一组使用符合 JWT 规范的 token 认证 Kubernetes 用户的认证器。
	// 认证器会尝试解析原始 ID token，并验证其由配置的 issuer 签名；
	// 验签公钥通过 OIDC discovery 从 issuer 的公开端点获取。
	// 对传入的 token，按本列表指定顺序依次尝试每个 JWT 认证器；
	// 但其他认证器可能排在 JWT 认证器之前或之后，其相对位置在各版本间
	// 既未定义也不稳定。由于每个 JWT 认证器必须拥有唯一的 issuer URL，
	// 最多只有一个 JWT 认证器会尝试对 token 进行密码学校验。
	//
	// 合法 JWT payload 至少需要包含以下 claims：
	// {
	//		"iss": "https://issuer.example.com",
	//		"aud": ["audience"],
	//		"exp": 1234567890,
	//		"<username claim>": "username"
	// }
	// issuer URL 唯一性保证了同一 token 只会被一个 JWT 认证器验签成功，因此
	// 列表顺序只影响尝试顺序，不影响最终身份来源。
	JWT []JWTAuthenticator `json:"jwt"`

	// 匿名认证与 --anonymous-auth 参数互斥，同时配置会因语义冲突在校验阶段报错。
	Anonymous *AnonymousAuthConfig `json:"anonymous,omitempty"`
}

// AnonymousAuthConfig 控制匿名请求是否被认证为 system:anonymous，以及可选的
// 路径白名单。
type AnonymousAuthConfig struct {
	Enabled bool `json:"enabled"`

	// 请求满足以下任一条件时才允许匿名认证；条件之间是 OR 关系；
	// 为空表示所有请求都可匿名（只要 Enabled 为 true）。
	Conditions []AnonymousAuthCondition `json:"conditions,omitempty"`
}

// AnonymousAuthCondition 描述允许匿名认证的启用条件；目前仅支持按请求路径匹配，路径需精确匹配。
type AnonymousAuthCondition struct {
	// Path for which anonymous auth is enabled.
	Path string `json:"path"`
}

// JWTAuthenticator 描述一个 OIDC issuer：发现端点、claim 映射与用户校验规则。
type JWTAuthenticator struct {
	// issuer contains the basic OIDC provider connection options.
	// +required
	Issuer Issuer `json:"issuer"`

	// claimValidationRules 是认证用户前应用于校验 token claims 的规则。
	// +optional
	// 规则之间按逻辑与组合，全部通过才认证成功；失败时整个 token 被拒绝。
	ClaimValidationRules []ClaimValidationRule `json:"claimValidationRules,omitempty"`

	// claimMappings points claims of a token to be treated as user attributes.
	// +required
	ClaimMappings ClaimMappings `json:"claimMappings"`

	// userValidationRules are rules that are applied to final user before completing authentication.
	// These allow invariants to be applied to incoming identities such as preventing the
	// use of the system: prefix that is commonly used by Kubernetes components.
	// The validation rules are logically ANDed together and must all return true for the validation to pass.
	// +optional
	UserValidationRules []UserValidationRule `json:"userValidationRules,omitempty"`
}

// Issuer 描述如何定位并信任外部 OIDC provider。
type Issuer struct {
	// url points to the issuer URL in a format https://url or https://url/path.
	// This must match the "iss" claim in the presented JWT, and the issuer returned from discovery.
	// Same value as the --oidc-issuer-url flag.
	// Discovery information is fetched from "{url}/.well-known/openid-configuration" unless overridden by discoveryURL.
	// Required to be unique across all JWT authenticators.
	// Note that egress selection configuration is not used for this network connection.
	// +required
	URL string `json:"url"`

	// discoveryURL 若指定，则覆盖用于获取 discovery 信息的地址，不再使用
	// "{url}/.well-known/openid-configuration"。
	// 指定值会被原样使用，因此必要时需在 discoveryURL 中包含
	// "/.well-known/openid-configuration"。
	//
	// 获取到的 discovery 信息中的 "issuer" 字段必须与 AuthenticationConfiguration
	// 中的 "issuer.url" 字段一致，并用于校验所提交 JWT 中的 "iss" claim。
	// 发现地址被覆盖后该约束依然成立，防止配置被用来把验签流量
	// 导到不受信任的端点。
	// 这适用于 well-known 与 jwks 端点与 issuer 不在同一位置（例如部署在集群内）的场景。
	//
	// 示例：
	// 通过命名空间 'oidc-namespace' 中的 kubernetes service 'oidc' 暴露 discovery 地址，
	// discovery 信息位于 '/.well-known/openid-configuration'。
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
	DiscoveryURL *string `json:"discoveryURL,omitempty"`

	// certificateAuthority contains PEM-encoded certificate authority certificates
	// used to validate the connection when fetching discovery information.
	// If unset, the system verifier is used.
	// Same value as the content of the file referenced by the --oidc-ca-file flag.
	// +optional
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// audiences is the set of acceptable audiences the JWT must be issued to.
	// At least one of the entries must match the "aud" claim in presented JWTs.
	// Same value as the --oidc-client-id flag (though this field supports an array).
	// Required to be non-empty.
	// +required
	Audiences []string `json:"audiences"`

	// audienceMatchPolicy defines how the "audiences" field is used to match the "aud" claim in the presented JWT.
	// Allowed values are:
	// 1. "MatchAny" when multiple audiences are specified and
	// 2. empty (or unset) or "MatchAny" when a single audience is specified.
	//
	// - MatchAny: the "aud" claim in the presented JWT must match at least one of the entries in the "audiences" field.
	// For example, if "audiences" is ["foo", "bar"], the "aud" claim in the presented JWT must contain either "foo" or "bar" (and may contain both).
	//
	// - "": The match policy can be empty (or unset) when a single audience is specified in the "audiences" field. The "aud" claim in the presented JWT must contain the single audience (and may contain others).
	//
	// For more nuanced audience validation, use claimValidationRules.
	//   example: claimValidationRule[].expression: 'sets.equivalent(claims.aud, ["bar", "foo", "baz"])' to require an exact match.
	// +optional
	AudienceMatchPolicy AudienceMatchPolicyType `json:"audienceMatchPolicy,omitempty"`

	// egressSelectorType is an indicator of which egress selection should be used for sending all traffic related
	// to this issuer (discovery, JWKS, distributed claims, etc).  If unspecified, no custom dialer is used.
	// When specified, the valid choices are "controlplane" and "cluster".  These correspond to the associated
	// values in the --egress-selector-config-file.
	//
	// - controlplane: for traffic intended to go to the control plane.
	//
	// - cluster: for traffic intended to go to the system being managed by Kubernetes.
	//
	// +optional
	// 注意：与上面 URL/DiscoveryURL 注释中“不使用 egress 选择”的旧说明相比，
	// 该字段显式指定后，发现、JWKS 与 distributed claims 流量都会走所选出口。
	EgressSelectorType EgressSelectorType `json:"egressSelectorType,omitempty"`
}

// AudienceMatchPolicyType 限制 audience 匹配策略的取值，未知值会被校验拒绝。
type AudienceMatchPolicyType string

// Valid types for AudienceMatchPolicyType
const (
	// MatchAny means the "aud" claim in the presented JWT must match at least one of the entries in the "audiences" field.
	AudienceMatchPolicyMatchAny AudienceMatchPolicyType = "MatchAny"
)

// EgressSelectorType 指示发送流量应使用哪个 egress 选择，与 egress 配置中的 selection name 一一对应，
// 便于复用同一定义而不在认证配置中重复连接参数。
type EgressSelectorType string

const (
	// EgressSelectorControlPlane is the EgressSelectorType for traffic intended to go to the control plane.
	EgressSelectorControlPlane EgressSelectorType = "controlplane"

	// EgressSelectorCluster is the EgressSelectorType for traffic intended to go to the system being managed by Kubernetes.
	EgressSelectorCluster EgressSelectorType = "cluster"
)

// ClaimValidationRule 支持两种互斥形式：claim/requiredValue 的等值校验，或 CEL
// expression 的任意校验。二者不可混用，避免语义不清。
type ClaimValidationRule struct {
	// claim is the name of a required claim.
	// Same as --oidc-required-claim flag.
	// Only string claim keys are supported.
	// Mutually exclusive with expression and message.
	// +optional
	Claim string `json:"claim,omitempty"`
	// requiredValue is the value of a required claim.
	// Same as --oidc-required-claim flag.
	// Only string claim values are supported.
	// If claim is set and requiredValue is not set, the claim must be present with a value set to the empty string.
	// Mutually exclusive with expression and message.
	// +optional
	RequiredValue string `json:"requiredValue,omitempty"`

	// expression represents the expression which will be evaluated by CEL.
	// Must produce a boolean.
	//
	// CEL expressions have access to the contents of the token claims, organized into CEL variable:
	// - 'claims' is a map of claim names to claim values.
	//   For example, a variable named 'sub' can be accessed as 'claims.sub'.
	//   Nested claims can be accessed using dot notation, e.g. 'claims.foo.bar'.
	// Must return true for the validation to pass.
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// Mutually exclusive with claim and requiredValue.
	// +optional
	Expression string `json:"expression,omitempty"`
	// message customizes the returned error message when expression returns false.
	// message is a literal string.
	// Mutually exclusive with claim and requiredValue.
	// +optional
	Message string `json:"message,omitempty"`
}

// ClaimMappings 定义如何把 token claims 映射成 username、groups、uid 与 extra。
type ClaimMappings struct {
	// username represents an option for the username attribute.
	// The claim's value must be a singular string.
	// Same as the --oidc-username-claim and --oidc-username-prefix flags.
	// If username.expression is set, the expression must produce a string value.
	// If username.expression uses 'claims.email', then 'claims.email_verified' must be used in
	// username.expression or extra[*].valueExpression or claimValidationRules[*].expression.
	// An example claim validation rule expression that matches the validation automatically
	// applied when username.claim is set to 'email' is 'claims.?email_verified.orValue(true) == true'. By explicitly comparing
	// the value to true, we let type-checking see the result will be a boolean, and to make sure a non-boolean email_verified
	// claim will be caught at runtime.
	//
	// In the flag based approach, the --oidc-username-claim and --oidc-username-prefix are optional. If --oidc-username-claim is not set,
	// the default value is "sub". For the authentication config, there is no defaulting for claim or prefix. The claim and prefix must be set explicitly.
	// For claim, if --oidc-username-claim was not set with legacy flag approach, configure username.claim="sub" in the authentication config.
	// For prefix:
	//     (1) --oidc-username-prefix="-", no prefix was added to the username. For the same behavior using authentication config,
	//         set username.prefix=""
	//     (2) --oidc-username-prefix="" and  --oidc-username-claim != "email", prefix was "<value of --oidc-issuer-url>#". For the same
	//         behavior using authentication config, set username.prefix="<value of issuer.url>#"
	//     (3) --oidc-username-prefix="<value>". For the same behavior using authentication config, set username.prefix="<value>"
	// +required
	Username PrefixedClaimOrExpression `json:"username"`
	// groups represents an option for the groups attribute.
	// The claim's value must be a string or string array claim.
	// If groups.claim is set, the prefix must be specified (and can be the empty string).
	// If groups.expression is set, the expression must produce a string or string array value.
	//  "", [], and null values are treated as the group mapping not being present.
	// +optional
	Groups PrefixedClaimOrExpression `json:"groups,omitempty"`

	// uid represents an option for the uid attribute.
	// Claim must be a singular string claim.
	// If uid.expression is set, the expression must produce a string value.
	// +optional
	UID ClaimOrExpression `json:"uid"`

	// extra represents an option for the extra attribute.
	// expression must produce a string or string array value.
	// If the value is empty, the extra mapping will not be present.
	//
	// hard-coded extra key/value
	// - key: "foo"
	//   valueExpression: "'bar'"
	// This will result in an extra attribute - foo: ["bar"]
	//
	// hard-coded key, value copying claim value
	// - key: "foo"
	//   valueExpression: "claims.some_claim"
	// This will result in an extra attribute - foo: [value of some_claim]
	//
	// hard-coded key, value derived from claim value
	// - key: "admin"
	//   valueExpression: '(has(claims.is_admin) && claims.is_admin) ? "true":""'
	// This will result in:
	//  - if is_admin claim is present and true, extra attribute - admin: ["true"]
	//  - if is_admin claim is present and false or is_admin claim is not present, no extra attribute will be added
	//
	// +optional
	Extra []ExtraMapping `json:"extra,omitempty"`
}

// PrefixedClaimOrExpression 用于 username/groups：claim 形式必须带 prefix（可为空串），
// expression 形式自行返回最终值，不再附加前缀。
type PrefixedClaimOrExpression struct {
	// claim is the JWT claim to use.
	// Mutually exclusive with expression.
	// +optional
	Claim string `json:"claim,omitempty"`
	// prefix is prepended to claim's value to prevent clashes with existing names.
	// prefix needs to be set if claim is set and can be the empty string.
	// Mutually exclusive with expression.
	// +optional
	Prefix *string `json:"prefix,omitempty"`

	// expression represents the expression which will be evaluated by CEL.
	//
	// CEL expressions have access to the contents of the token claims, organized into CEL variable:
	// - 'claims' is a map of claim names to claim values.
	//   For example, a variable named 'sub' can be accessed as 'claims.sub'.
	//   Nested claims can be accessed using dot notation, e.g. 'claims.foo.bar'.
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// Mutually exclusive with claim and prefix.
	// +optional
	Expression string `json:"expression,omitempty"`
}

// ClaimOrExpression 用于 uid 等不需要前缀的属性；claim 与 expression 二选一。
type ClaimOrExpression struct {
	// claim is the JWT claim to use.
	// Either claim or expression must be set.
	// Mutually exclusive with expression.
	// +optional
	Claim string `json:"claim,omitempty"`

	// expression represents the expression which will be evaluated by CEL.
	//
	// CEL expressions have access to the contents of the token claims, organized into CEL variable:
	// - 'claims' is a map of claim names to claim values.
	//   For example, a variable named 'sub' can be accessed as 'claims.sub'.
	//   Nested claims can be accessed using dot notation, e.g. 'claims.foo.bar'.
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// Mutually exclusive with claim.
	// +optional
	Expression string `json:"expression,omitempty"`
}

// ExtraMapping 把 CEL 求值结果附加到 user.Info.Extra，key 必须是带域前缀的
// 规范化名称，以免与内置属性冲突。
type ExtraMapping struct {
	// key is a string to use as the extra attribute key.
	// key must be a domain-prefix path (e.g. example.org/foo). All characters before the first "/" must be a valid
	// subdomain as defined by RFC 1123. All characters trailing the first "/" must
	// be valid HTTP Path characters as defined by RFC 3986.
	// key must be lowercase.
	// Required to be unique.
	// +required
	Key string `json:"key"`

	// valueExpression is a CEL expression to extract extra attribute value.
	// valueExpression must produce a string or string array value.
	// "", [], and null values are treated as the extra mapping not being present.
	// Empty string values contained within a string array are filtered out.
	//
	// CEL expressions have access to the contents of the token claims, organized into CEL variable:
	// - 'claims' is a map of claim names to claim values.
	//   For example, a variable named 'sub' can be accessed as 'claims.sub'.
	//   Nested claims can be accessed using dot notation, e.g. 'claims.foo.bar'.
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// +required
	ValueExpression string `json:"valueExpression"`
}

// UserValidationRule 在映射完成后对最终 UserInfo 施加不变量，例如阻止外部
// 身份冒用 system: 前缀。表达式只能读 user，不能修改映射结果。
type UserValidationRule struct {
	// expression represents the expression which will be evaluated by CEL.
	// Must return true for the validation to pass.
	//
	// CEL expressions have access to the contents of UserInfo, organized into CEL variable:
	// - 'user' - authentication.k8s.io/v1, Kind=UserInfo object
	//    Refer to https://github.com/kubernetes/api/blob/release-1.28/authentication/v1/types.go#L105-L122 for the definition.
	//    API documentation: https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#userinfo-v1-authentication-k8s-io
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// +required
	Expression string `json:"expression"`

	// message customizes the returned error message when rule returns false.
	// message is a literal string.
	// +optional
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthorizationConfiguration 是授权配置的外部版本，按顺序组合多个授权器；
// 后续授权器可对前者的弃权（NoOpinion）作出最终决定。
type AuthorizationConfiguration struct {
	metav1.TypeMeta `json:""`

	// Authorizers is an ordered list of authorizers to
	// authorize requests against.
	// This is similar to the --authorization-modes kube-apiserver flag
	// Must be at least one.
	Authorizers []AuthorizerConfiguration `json:"authorizers"`
}

const (
	// TypeWebhook 是目前 generic apiserver 唯一支持的授权器类型。
	TypeWebhook AuthorizerType = "Webhook"
	// FailurePolicyNoOpinion 表示 webhook 出错时弃权，交给后续授权器决定。
	FailurePolicyNoOpinion string = "NoOpinion"
	// FailurePolicyDeny 表示 webhook 出错时直接拒绝，不再咨询后续授权器。
	FailurePolicyDeny string = "Deny"
	// AuthorizationWebhookConnectionInfoTypeKubeConfigFile 使用 kubeconfig 文件
	// 定位 SubjectAccessReview 服务。
	AuthorizationWebhookConnectionInfoTypeKubeConfigFile string = "KubeConfigFile"
	// AuthorizationWebhookConnectionInfoTypeInCluster 使用集群内配置调用本机
	// kube-apiserver 的 SubjectAccessReview API（kube-apiserver 自身禁止此模式）。
	AuthorizationWebhookConnectionInfoTypeInCluster string = "InClusterConfig"
)

// AuthorizerType 授权器类型；不同 apiserver 实现可扩展自己的类型。
type AuthorizerType string

// AuthorizerConfiguration 描述单个授权器实例。
type AuthorizerConfiguration struct {
	// Type refers to the type of the authorizer
	// "Webhook" is supported in the generic API server
	// Other API servers may support additional authorizer
	// types like Node, RBAC, ABAC, etc.
	Type string `json:"type"`

	// Name 用于描述该 webhook 授权器，会显式用于监控机制中的指标。
	// 注意：名称必须是 DNS1123 标签（如 `myauthorizername`）或
	//		 子域名（如 `myauthorizer.example.domain`）。
	// 必填，无默认值，且在同一配置内必须唯一。
	Name string `json:"name"`

	// Webhook defines the configuration for a Webhook authorizer
	// Must be defined when Type=Webhook
	// Must not be defined when Type!=Webhook
	Webhook *WebhookConfiguration `json:"webhook,omitempty"`
}

// WebhookConfiguration 定义 webhook 授权器的连接、缓存与匹配条件。
type WebhookConfiguration struct {
	// The duration to cache 'authorized' responses from the webhook
	// authorizer.
	// Same as setting `--authorization-webhook-cache-authorized-ttl` flag
	// Default: 5m0s
	AuthorizedTTL metav1.Duration `json:"authorizedTTL"`
	// CacheAuthorizedRequests specifies whether authorized requests should be cached.
	// If set to true, the TTL for cached decisions can be configured via the
	// AuthorizedTTL field.
	// Default: true
	// +optional
	CacheAuthorizedRequests *bool `json:"cacheAuthorizedRequests,omitempty"`
	// The duration to cache 'unauthorized' responses from the webhook
	// authorizer.
	// Same as setting `--authorization-webhook-cache-unauthorized-ttl` flag
	// Default: 30s
	UnauthorizedTTL metav1.Duration `json:"unauthorizedTTL"`
	// CacheUnauthorizedRequests specifies whether unauthorized requests should be cached.
	// If set to true, the TTL for cached decisions can be configured via the
	// UnauthorizedTTL field.
	// Default: true
	// +optional
	CacheUnauthorizedRequests *bool `json:"cacheUnauthorizedRequests,omitempty"`
	// Timeout for the webhook request
	// Maximum allowed value is 30s.
	// Required, no default value.
	Timeout metav1.Duration `json:"timeout"`
	// The API version of the authorization.k8s.io SubjectAccessReview to
	// send to and expect from the webhook.
	// Same as setting `--authorization-webhook-version` flag
	// Valid values: v1beta1, v1
	// Required, no default value
	SubjectAccessReviewVersion string `json:"subjectAccessReviewVersion"`
	// MatchConditionSubjectAccessReviewVersion specifies the SubjectAccessReview
	// version the CEL expressions are evaluated against
	// Valid values: v1
	// Required, no default value
	MatchConditionSubjectAccessReviewVersion string `json:"matchConditionSubjectAccessReviewVersion"`
	// Controls the authorization decision when a webhook request fails to
	// complete or returns a malformed response or errors evaluating
	// matchConditions.
	// Valid values:
	//   - NoOpinion: continue to subsequent authorizers to see if one of
	//     them allows the request
	//   - Deny: reject the request without consulting subsequent authorizers
	// Required, with no default.
	FailurePolicy string `json:"failurePolicy"`

	// ConnectionInfo defines how we talk to the webhook
	ConnectionInfo WebhookConnectionInfo `json:"connectionInfo"`

	// matchConditions is a list of conditions that must be met for a request to be sent to this
	// webhook. An empty list of matchConditions matches all requests.
	// There are a maximum of 64 match conditions allowed.
	//
	// The exact matching logic is (in order):
	//   1. If at least one matchCondition evaluates to FALSE, then the webhook is skipped.
	//   2. If ALL matchConditions evaluate to TRUE, then the webhook is called.
	//   3. If at least one matchCondition evaluates to an error (but none are FALSE):
	//      - If failurePolicy=Deny, then the webhook rejects the request
	//      - If failurePolicy=NoOpinion, then the error is ignored and the webhook is skipped
	MatchConditions []WebhookMatchCondition `json:"matchConditions"`
}

// WebhookConnectionInfo 定义 webhook 授权器如何与服务器建立连接。
type WebhookConnectionInfo struct {
	// Controls how the webhook should communicate with the server.
	// Valid values:
	// - KubeConfigFile: use the file specified in kubeConfigFile to locate the
	//   server.
	// - InClusterConfig: use the in-cluster configuration to call the
	//   SubjectAccessReview API hosted by kube-apiserver. This mode is not
	//   allowed for kube-apiserver.
	Type string `json:"type"`

	// 指向 kubeconfig 文件的路径；仅在 Type=KubeConfigFile 时必填；InClusterConfig 会忽略该字段。
	KubeConfigFile *string `json:"kubeConfigFile"`
}

// WebhookMatchCondition 以 CEL 表达式决定是否把请求发给 webhook 授权器；
// 表达式始终针对 v1 版 SubjectAccessReview 求值，低版本会先转换再求值。
type WebhookMatchCondition struct {
	// expression represents the expression which will be evaluated by CEL. Must evaluate to bool.
	// CEL expressions have access to the contents of the SubjectAccessReview in v1 version.
	// If version specified by subjectAccessReviewVersion in the request variable is v1beta1,
	// the contents would be converted to the v1 version before evaluating the CEL expression.
	//
	// - 'resourceAttributes' describes information for a resource access request and is unset for non-resource requests. e.g. has(request.resourceAttributes) && request.resourceAttributes.namespace == 'default'
	// - 'nonResourceAttributes' describes information for a non-resource access request and is unset for resource requests. e.g. has(request.nonResourceAttributes) && request.nonResourceAttributes.path == '/healthz'.
	// - 'user' is the user to test for. e.g. request.user == 'alice'
	// - 'groups' is the groups to test for. e.g. ('group1' in request.groups)
	// - 'extra' corresponds to the user.Info.GetExtra() method from the authenticator.
	// - 'uid' is the information about the requesting user. e.g. request.uid == '1'
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	Expression string `json:"expression"`
}
