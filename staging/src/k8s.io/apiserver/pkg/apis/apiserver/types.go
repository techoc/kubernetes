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

// AdmissionConfiguration provides versioned configuration for admission controllers.
//
// 这是所有准入插件配置的统一入口：每个插件通过名字关联一段配置，
// 配置可以内嵌在 Configuration 字段中，也可以放在 Path 指向的单独文件里。
type AdmissionConfiguration struct {
	metav1.TypeMeta

	// Plugins allows specifying a configuration per admission control plugin.
	// +optional
	Plugins []AdmissionPluginConfiguration
}

// AdmissionPluginConfiguration provides the configuration for a single plug-in.
//
// 插件名必须与启动参数 --enable-admission-plugins 中使用的注册名一致；
// 找不到配置的插件按默认行为运行。
type AdmissionPluginConfiguration struct {
	// Name is the name of the admission controller.
	// It must match the registered admission plugin name.
	Name string

	// Path is the path to a configuration file that contains the plugin's
	// configuration
	// +optional
	Path string

	// Configuration is an embedded configuration object to be used as the plugin's
	// configuration. If present, it will be used instead of the path to the configuration file.
	// +optional
	Configuration *runtime.Unknown
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EgressSelectorConfiguration provides versioned configuration for egress selector clients.
//
// 出口选择器（egress selector）决定 apiserver 访问 controlplane、etcd、cluster
// 等不同目的地时如何建立连接（直连或经 konnectivity 代理）。
type EgressSelectorConfiguration struct {
	metav1.TypeMeta

	// EgressSelections contains a list of egress selection client configurations
	EgressSelections []EgressSelection
}

// EgressSelection provides the configuration for a single egress selection client.
//
// 每个 Name 对应一类目的地（controlplane/etcd/cluster），同一个 Name 只能出现一次。
type EgressSelection struct {
	// Name is the name of the egress selection.
	// Currently supported values are "controlplane", "etcd" and "cluster"
	Name string

	// Connection is the exact information used to configure the egress selection
	Connection Connection
}

// Connection provides the configuration for a single egress selection client.
//
// ProxyProtocol 决定是否需要 Transport：Direct 直接连接，HTTPConnect/GRPC
// 则需要通过 konnectivity 代理。
type Connection struct {
	// Protocol is the protocol used to connect from client to the konnectivity server.
	ProxyProtocol ProtocolType

	// Transport defines the transport configurations we use to dial to the konnectivity server.
	// This is required if ProxyProtocol is HTTPConnect or GRPC.
	// +optional
	Transport *Transport
}

// ProtocolType is a set of valid values for Connection.ProtocolType
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

// Transport defines the transport configurations we use to dial to the konnectivity server
//
// TCP 与 UDS 至少配置一个；具体用哪个取决于 konnectivity 服务的部署形态。
type Transport struct {
	// TCP is the TCP configuration for communicating with the konnectivity server via TCP
	// ProxyProtocol of GRPC is not supported with TCP transport at the moment
	// Requires at least one of TCP or UDS to be set
	// +optional
	TCP *TCPTransport

	// UDS is the UDS configuration for communicating with the konnectivity server via UDS
	// Requires at least one of TCP or UDS to be set
	// +optional
	UDS *UDSTransport
}

// TCPTransport provides the information to connect to konnectivity server via TCP
//
// URL 的 scheme 决定是否启用 TLS：http:// 时不允许配置 TLSConfig。
type TCPTransport struct {
	// URL is the location of the konnectivity server to connect to.
	// As an example it might be "https://127.0.0.1:8131"
	URL string

	// TLSConfig is the config needed to use TLS when connecting to konnectivity server
	// +optional
	TLSConfig *TLSConfig
}

// UDSTransport provides the information to connect to konnectivity server via UDS
//
// UDS 通常用于 node 本地，不经过网络，因此不需要 TLS 配置。
type UDSTransport struct {
	// UDSName is the name of the unix domain socket to connect to konnectivity server
	// This does not use a unix:// prefix. (Eg: /etc/srv/kubernetes/konnectivity-server/konnectivity-server.socket)
	UDSName string
}

// TLSConfig provides the authentication information to connect to konnectivity server
// Only used with TCPTransport
//
// 这些字段都是“文件路径”而不是内联内容；apiserver 启动时读取，变更需重启生效。
type TLSConfig struct {
	// caBundle is the file location of the CA to be used to determine trust with the konnectivity server.
	// Must be absent/empty if TCPTransport.URL is prefixed with http://
	// If absent while TCPTransport.URL is prefixed with https://, default to system trust roots.
	// +optional
	CABundle string

	// clientKey is the file location of the client key to authenticate with the konnectivity server
	// Must be absent/empty if TCPTransport.URL is prefixed with http://
	// Must be configured if TCPTransport.URL is prefixed with https://
	// +optional
	ClientKey string

	// clientCert is the file location of the client certificate to authenticate with the konnectivity server
	// Must be absent/empty if TCPTransport.URL is prefixed with http://
	// Must be configured if TCPTransport.URL is prefixed with https://
	// +optional
	ClientCert string

	// tlsServerName is used to check server certificate. If tlsServerName is empty, the hostname used to contact the server is used.
	// +optional
	TLSServerName string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TracingConfiguration provides versioned configuration for tracing clients.
//
// 直接内嵌 component-base 的 tracing 配置，便于各组件复用同一套 sampling/endpoint 结构。
type TracingConfiguration struct {
	metav1.TypeMeta

	// Embed the component config tracing configuration struct
	tracingapi.TracingConfiguration
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthenticationConfiguration provides versioned configuration for authentication.
//
// 该配置与 --anonymous-auth 互斥：一旦提供此对象，匿名认证只能通过 Anonymous 字段配置。
type AuthenticationConfiguration struct {
	metav1.TypeMeta

	// JWT 认证器按顺序尝试；每个 Issuer.URL 必须全局唯一。
	JWT []JWTAuthenticator

	// If present --anonymous-auth must not be set
	Anonymous *AnonymousAuthConfig
}

// AnonymousAuthConfig provides the configuration for the anonymous authenticator.
type AnonymousAuthConfig struct {
	Enabled bool

	// If set, anonymous auth is only allowed if the request meets one of the
	// conditions.
	//
	// Conditions 为空时对所有请求生效；非空时只对命中的路径生效。
	Conditions []AnonymousAuthCondition
}

// AnonymousAuthCondition describes the condition under which anonymous auth
// should be enabled.
type AnonymousAuthCondition struct {
	// Path for which anonymous auth is enabled.
	Path string
}

// JWTAuthenticator provides the configuration for a single JWT authenticator.
//
// 认证分两步：先按 ClaimValidationRules 校验 claim，再用 ClaimMappings 映射出
// user.Info，最后用 UserValidationRules 对拼装出的用户信息做最终校验。
type JWTAuthenticator struct {
	Issuer               Issuer
	ClaimValidationRules []ClaimValidationRule
	ClaimMappings        ClaimMappings
	UserValidationRules  []UserValidationRule
}

// Issuer provides the configuration for an external provider's specific settings.
//
// 一个 JWT 认证器绑定一个 issuer：iss claim、discovery 返回的 issuer 与这里的
// URL 必须三者一致，否则令牌会被拒绝。
type Issuer struct {
	// url points to the issuer URL in a format https://url or https://url/path.
	// This must match the "iss" claim in the presented JWT, and the issuer returned from discovery.
	// Same value as the --oidc-issuer-url flag.
	// Discovery information is fetched from "{url}/.well-known/openid-configuration" unless overridden by discoveryURL.
	// Required to be unique across all JWT authenticators.
	// Note that egress selection configuration is not used for this network connection.
	// +required
	URL string
	// discoveryURL, if specified, overrides the URL used to fetch discovery
	// information instead of using "{url}/.well-known/openid-configuration".
	// The exact value specified is used, so "/.well-known/openid-configuration"
	// must be included in discoveryURL if needed.
	//
	// The "issuer" field in the fetched discovery information must match the "issuer.url" field
	// in the AuthenticationConfiguration and will be used to validate the "iss" claim in the presented JWT.
	// This is for scenarios where the well-known and jwks endpoints are hosted at a different
	// location than the issuer (such as locally in the cluster).
	//
	// Example:
	// A discovery url that is exposed using kubernetes service 'oidc' in namespace 'oidc-namespace'
	// and discovery information is available at '/.well-known/openid-configuration'.
	// discoveryURL: "https://oidc.oidc-namespace/.well-known/openid-configuration"
	// certificateAuthority is used to verify the TLS connection and the hostname on the leaf certificate
	// must be set to 'oidc.oidc-namespace'.
	//
	// curl https://oidc.oidc-namespace/.well-known/openid-configuration (.discoveryURL field)
	// {
	//     issuer: "https://oidc.example.com" (.url field)
	// }
	//
	// discoveryURL must be different from url.
	// Required to be unique across all JWT authenticators.
	// Note that egress selection configuration is not used for this network connection.
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

// AudienceMatchPolicyType is a set of valid values for Issuer.AudienceMatchPolicy
type AudienceMatchPolicyType string

// Valid types for AudienceMatchPolicyType
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

// ClaimValidationRule provides the configuration for a single claim validation rule.
//
// 校验规则要么要求 claim 等于某个固定值（Claim + RequiredValue），要么使用
// CEL 表达式（Expression）进行更复杂的判断；二者必须择一。
type ClaimValidationRule struct {
	Claim         string
	RequiredValue string

	Expression string
	Message    string
}

// ClaimMappings provides the configuration for claim mapping
//
// 定义如何把 JWT 的 claim 映射为 user.Info 的 username、groups、uid 和 extra。
type ClaimMappings struct {
	Username PrefixedClaimOrExpression
	Groups   PrefixedClaimOrExpression
	UID      ClaimOrExpression
	Extra    []ExtraMapping
}

// PrefixedClaimOrExpression provides the configuration for a single prefixed claim or expression.
//
// Claim 与 Expression 二选一；Prefix 只对 claim 形式生效，用于拼接用户名前缀。
type PrefixedClaimOrExpression struct {
	Claim  string
	Prefix *string

	Expression string
}

// ClaimOrExpression provides the configuration for a single claim or expression.
type ClaimOrExpression struct {
	Claim      string
	Expression string
}

// ExtraMapping provides the configuration for a single extra mapping.
//
// Key 是写入 user.Info.Extra 的键名，ValueExpression 是求值得到字符串列表的 CEL 表达式。
type ExtraMapping struct {
	Key             string
	ValueExpression string
}

// UserValidationRule provides the configuration for a single user validation rule.
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

	// Authorizers is an ordered list of authorizers to
	// authorize requests against.
	// This is similar to the --authorization-modes kube-apiserver flag
	// Must be at least one.
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

// AuthorizerConfiguration describes one authorizer in the ordered chain.
//
// Authorizers 列表顺序决定决策顺序：前面的授权器返回允许或拒绝即终止，
// 返回 NoOpinion 时继续交给下一个。
type AuthorizerConfiguration struct {
	// Type refers to the type of the authorizer
	// "Webhook" is supported in the generic API server
	// Other API servers may support additional authorizer
	// types like Node, RBAC, ABAC, etc.
	Type AuthorizerType

	// Name used to describe the webhook
	// This is explicitly used in monitoring machinery for metrics
	// Note: Names must be DNS1123 labels like `myauthorizername` or
	//		 subdomains like `myauthorizer.example.domain`
	// Required, with no default
	Name string

	// Webhook defines the configuration for a Webhook authorizer
	// Must be defined when Type=Webhook
	Webhook *WebhookConfiguration
}

// WebhookConfiguration 配置一个 webhook 授权器，包含缓存 TTL、超时、
// SubjectAccessReview 版本、失败策略与 matchConditions。
type WebhookConfiguration struct {
	// The duration to cache 'authorized' responses from the webhook
	// authorizer.
	// Same as setting `--authorization-webhook-cache-authorized-ttl` flag
	// Default: 5m0s
	AuthorizedTTL metav1.Duration
	// CacheAuthorizedRequests specifies whether authorized requests should be cached.
	// If set to true, the TTL for cached decisions can be configured via the
	// AuthorizedTTL field.
	// Default: true
	CacheAuthorizedRequests bool
	// The duration to cache 'unauthorized' responses from the webhook
	// authorizer.
	// Same as setting `--authorization-webhook-cache-unauthorized-ttl` flag
	// Default: 30s
	UnauthorizedTTL metav1.Duration
	// CacheUnauthorizedRequests specifies whether unauthorized requests should be cached.
	// If set to true, the TTL for cached decisions can be configured via the
	// UnauthorizedTTL field.
	// Default: true
	CacheUnauthorizedRequests bool
	// Timeout for the webhook request
	// Maximum allowed value is 30s.
	// Required, no default value.
	Timeout metav1.Duration
	// The API version of the authorization.k8s.io SubjectAccessReview to
	// send to and expect from the webhook.
	// Same as setting `--authorization-webhook-version` flag
	// Valid values: v1beta1, v1
	// Required, no default value
	SubjectAccessReviewVersion string
	// MatchConditionSubjectAccessReviewVersion specifies the SubjectAccessReview
	// version the CEL expressions are evaluated against
	// Valid values: v1
	// Required, no default value
	MatchConditionSubjectAccessReviewVersion string
	// Controls the authorization decision when a webhook request fails to
	// complete or returns a malformed response or errors evaluating
	// matchConditions.
	// Valid values:
	//   - NoOpinion: continue to subsequent authorizers to see if one of
	//     them allows the request
	//   - Deny: reject the request without consulting subsequent authorizers
	// Required, with no default.
	FailurePolicy string

	// ConnectionInfo defines how we talk to the webhook
	ConnectionInfo WebhookConnectionInfo

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
	MatchConditions []WebhookMatchCondition
}

// WebhookConnectionInfo 描述如何连接到授权 webhook 服务。
type WebhookConnectionInfo struct {
	// Controls how the webhook should communicate with the server.
	// Valid values:
	// - KubeConfigFile: use the file specified in kubeConfigFile to locate the
	//   server.
	// - InClusterConfig: use the in-cluster configuration to call the
	//   SubjectAccessReview API hosted by kube-apiserver. This mode is not
	//   allowed for kube-apiserver.
	Type string

	// Path to KubeConfigFile for connection info
	// Required, if connectionInfo.Type is KubeConfig
	KubeConfigFile *string
}

// WebhookMatchCondition 是一条在调用 webhook 之前求值的 CEL 条件。
// 条件为 false 时跳过 webhook；条件求值出错时按 FailurePolicy 处理。
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
	Expression string
}
