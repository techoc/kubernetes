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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	tracingapi "k8s.io/component-base/tracing/api/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AdmissionConfiguration 提供准入控制器的版本化配置，是准入控制器的顶层配置
// 对象，由 --admission-control-config-file 指向的文件解码得到；每个插件只能出现一次。
type AdmissionConfiguration struct {
	metav1.TypeMeta `json:""`

	// Plugins 允许为每个准入控制插件分别指定配置。
	// +optional
	Plugins []AdmissionPluginConfiguration `json:"plugins"`
}

// AdmissionPluginConfiguration 提供单个准入插件的配置。
type AdmissionPluginConfiguration struct {
	// Name 是准入控制器的名称，必须与注册的准入插件名称一致。
	Name string `json:"name"`

	// Path 是指向配置文件的路径，该文件包含此插件的配置。
	// +optional
	Path string `json:"path"`

	// Configuration 是作为该插件配置使用的内嵌配置对象。
	// 如果设置了该字段，将使用它而不是 Path 指向的配置文件。
	// +optional
	Configuration *runtime.Unknown `json:"configuration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthenticationConfiguration 提供认证的版本化配置：JWT 与匿名认证的配置都在
// 这里声明，替代大量命令行参数。
type AuthenticationConfiguration struct {
	metav1.TypeMeta `json:""`

	// jwt 是一组认证器，使用符合 JWT 规范的令牌认证 Kubernetes 用户。
	// 认证器会尝试解析原始 ID 令牌，并验证其由配置的 issuer 签名。
	// 验签所用的公钥通过 OIDC discovery 从 issuer 的公开端点获取。
	// 对于传入的令牌，将按本列表中指定的顺序依次尝试每个 JWT 认证器。
	// 但请注意，其他认证器可能运行在 JWT 认证器之前或之后。
	// JWT 认证器与其他认证器的相对先后位置既未定义，也不跨版本保持稳定。
	// 由于每个 JWT 认证器必须拥有唯一的 issuer URL，
	// 最多只有一个 JWT 认证器会尝试对令牌进行密码学校验。
	//
	// 有效的最小 JWT payload 必须包含以下 claims：
	// {
	//		"iss": "https://issuer.example.com",
	//		"aud": ["audience"],
	//		"exp": 1234567890,
	//		"<username claim>": "username"
	// }
	JWT []JWTAuthenticator `json:"jwt"`

	// 若设置了该字段，则 --anonymous-auth 必须未被设置
	Anonymous *AnonymousAuthConfig `json:"anonymous,omitempty"`
}

// AnonymousAuthConfig 提供匿名认证器的配置。
type AnonymousAuthConfig struct {
	Enabled bool `json:"enabled"`

	// 若设置了本字段，则只有满足以下某个条件的请求才允许匿名认证。
	Conditions []AnonymousAuthCondition `json:"conditions,omitempty"`
}

// AnonymousAuthCondition 描述应当启用匿名认证的条件。
type AnonymousAuthCondition struct {
	// Path 是启用匿名认证所对应的请求路径。
	Path string `json:"path"`
}

// JWTAuthenticator 提供单个 JWT 认证器的配置。
type JWTAuthenticator struct {
	// issuer 包含连接外部 OIDC provider 的基础选项。
	// +required
	Issuer Issuer `json:"issuer"`

	// claimValidationRules 是在认证用户之前应用于校验令牌 claims 的规则。
	// +optional
	ClaimValidationRules []ClaimValidationRule `json:"claimValidationRules,omitempty"`

	// claimMappings 指定将令牌的哪些 claims 作为用户属性处理。
	// +required
	ClaimMappings ClaimMappings `json:"claimMappings"`

	// userValidationRules 是在完成认证之前应用于最终 user 的规则。
	// 这些规则允许对传入身份施加不变量约束，例如禁止使用
	// Kubernetes 组件常用的 system: 前缀。
	// 校验规则之间按逻辑与（AND）组合，必须全部返回 true 才能通过校验。
	// +optional
	UserValidationRules []UserValidationRule `json:"userValidationRules,omitempty"`
}

// Issuer 提供外部 provider 特定设置的配置。
type Issuer struct {
	// url 指向 issuer URL，格式为 https://url 或 https://url/path。
	// 该值必须与所提交 JWT 中的 "iss" claim 以及 discovery 返回的 issuer 一致。
	// 与 --oidc-issuer-url 参数的取值相同。
	// 除非被 discoveryURL 覆盖，discovery 信息将从 "{url}/.well-known/openid-configuration" 获取。
	// 在所有 JWT 认证器之间必须保持唯一。
	// 注意：该网络连接不使用 egress 选择配置。
	// +required
	URL string `json:"url"`

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
	DiscoveryURL *string `json:"discoveryURL,omitempty"`

	// certificateAuthority 包含 PEM 编码的证书机构证书，
	// 用于在获取 discovery 信息时验证连接。
	// 若未设置，则使用系统验证器。
	// 与 --oidc-ca-file 参数所引用文件的内容取值相同。
	// +optional
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// audiences 是 JWT 必须签发到的可接受 audience 集合。
	// 至少有一个条目必须与所提交 JWT 中的 "aud" claim 匹配。
	// 与 --oidc-client-id 参数的取值相同（不过本字段支持数组）。
	// 必须非空。
	// +required
	Audiences []string `json:"audiences"`

	// audienceMatchPolicy 定义如何使用 "audiences" 字段匹配所提交 JWT 中的 "aud" claim。
	// 允许的取值为：
	// 1. 指定多个 audience 时为 "MatchAny"，以及
	// 2. 只指定单个 audience 时为空（或未设置）或 "MatchAny"。
	//
	// - MatchAny: 所提交 JWT 中的 "aud" claim 必须匹配 "audiences" 字段中的至少一个条目。
	// 例如，若 "audiences" 为 ["foo", "bar"]，则所提交 JWT 中的 "aud" claim 必须包含 "foo" 或 "bar" 之一（也可以同时包含两者）。
	//
	// - "": 当 "audiences" 字段只指定单个 audience 时，匹配策略可以为空（或未设置）。所提交 JWT 中的 "aud" claim 必须包含该 audience（也可以包含其他值）。
	//
	// 若需要更精细的 audience 校验，请使用 claimValidationRules。
	//   example: claimValidationRule[].expression: 'sets.equivalent(claims.aud, ["bar", "foo", "baz"])' 用于要求完全匹配。
	// +optional
	AudienceMatchPolicy AudienceMatchPolicyType `json:"audienceMatchPolicy,omitempty"`

	// egressSelectorType 指示发送与此 issuer 相关的所有流量（discovery、JWKS、
	// 分布式 claims 等）时应使用哪个 egress 选择。未指定时，不使用自定义拨号器。
	// 指定时，合法取值为 "controlplane" 和 "cluster"。这些取值对应
	// --egress-selector-config-file 中的关联值。
	//
	// - controlplane: 用于发往控制面的流量。
	//
	// - cluster: 用于发往由 Kubernetes 管理的系统的流量。
	//
	// +optional
	EgressSelectorType EgressSelectorType `json:"egressSelectorType,omitempty"`
}

// AudienceMatchPolicyType 是 issuer.audienceMatchPolicy 的合法取值集合
type AudienceMatchPolicyType string

// AudienceMatchPolicyType 的合法类型
const (
	// MatchAny 表示所提交 JWT 中的 "aud" claim 必须匹配 "audiences" 字段中的至少一个条目。
	AudienceMatchPolicyMatchAny AudienceMatchPolicyType = "MatchAny"
)

// EgressSelectorType 指示发送流量时应使用哪个 egress 选择。
type EgressSelectorType string

const (
	// EgressSelectorControlPlane 是用于发往控制面流量的 EgressSelectorType。
	EgressSelectorControlPlane EgressSelectorType = "controlplane"

	// EgressSelectorCluster 是用于发往由 Kubernetes 管理的系统的流量的 EgressSelectorType。
	EgressSelectorCluster EgressSelectorType = "cluster"
)

// ClaimValidationRule 提供单条 claim 校验规则的配置。
//
// 校验规则有两种互斥的写法：静态的 claim/requiredValue 组合，或 CEL 表达式。
// 前者适合简单存在性检查，后者用于复杂约束。
type ClaimValidationRule struct {
	// claim 是必需 claim 的名称。
	// 与 --oidc-required-claim 参数相同。
	// 仅支持字符串类型的 claim 键。
	// 与 expression 和 message 互斥。
	// +optional
	Claim string `json:"claim,omitempty"`
	// requiredValue 是必需 claim 的取值。
	// 与 --oidc-required-claim 参数相同。
	// 仅支持字符串类型的 claim 值。
	// 如果设置了 claim 而未设置 requiredValue，则该 claim 必须存在且取值为空字符串。
	// 与 expression 和 message 互斥。
	// +optional
	RequiredValue string `json:"requiredValue,omitempty"`

	// expression 表示将由 CEL 求值的表达式。
	// 必须产生一个布尔值。
	//
	// CEL 表达式可以访问令牌 claims 的内容，组织为 CEL 变量：
	// - 'claims' 是从 claim 名称到 claim 值的映射。
	//   例如，名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claims 可使用点号表示法访问，例如 'claims.foo.bar'。
	// 必须返回 true 才能通过校验。
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 与 claim 和 requiredValue 互斥。
	// +optional
	Expression string `json:"expression,omitempty"`
	// message 自定义 expression 返回 false 时返回的错误消息。
	// message 是一个字面量字符串。
	// 与 claim 和 requiredValue 互斥。
	// +optional
	Message string `json:"message,omitempty"`
}

// ClaimMappings 提供 claim 映射的配置
//
// 把 JWT claims 映射为 user.Info 的 username/groups/uid/extra。claim 与
// expression 互斥，使用 expression 时可以做更灵活的值变换。
type ClaimMappings struct {
	// username 表示 username 属性的配置选项。
	// claim 的取值必须是单个字符串。
	// 与 --oidc-username-claim 和 --oidc-username-prefix 参数的取值相同。
	// 如果设置了 username.expression，则该表达式必须产生字符串值。
	// 如果 username.expression 使用了 'claims.email'，则 'claims.email_verified' 必须出现在
	// username.expression 或 extra[*].valueExpression 或 claimValidationRules[*].expression 中。
	// 与 username.claim 设置为 'email' 时自动应用的校验等价的一个 claim 校验规则表达式示例是
	// 'claims.?email_verified.orValue(true) == true'。通过与 true 显式比较，
	// 让类型检查得知结果为布尔值，从而确保非布尔的 email_verified
	// claim 会在运行时被捕获。
	//
	// 在使用参数的旧方案中，--oidc-username-claim 和 --oidc-username-prefix 是可选的。如果未设置 --oidc-username-claim，
	// 默认值为 "sub"。对于认证配置，claim 和 prefix 都没有默认值。claim 和 prefix 必须显式设置。
	// 关于 claim：如果旧参数方案未设置 --oidc-username-claim，则在认证配置中设置 username.claim="sub"。
	// 关于 prefix：
	//     (1) --oidc-username-prefix="-" 时，用户名不添加前缀。要在认证配置中获得相同行为，
	//         设置 username.prefix=""
	//     (2) --oidc-username-prefix="" 且 --oidc-username-claim != "email" 时，前缀为 "<value of --oidc-issuer-url>#"。要在认证配置中获得相同
	//         行为，设置 username.prefix="<value of issuer.url>#"
	//     (3) --oidc-username-prefix="<value>"。要在认证配置中获得相同行为，设置 username.prefix="<value>"
	// +required
	Username PrefixedClaimOrExpression `json:"username"`
	// groups 表示 groups 属性的配置选项。
	// claim 的取值必须是字符串或字符串数组类型的 claim。
	// 如果设置了 groups.claim，则必须指定 prefix（可以是空字符串）。
	// 如果设置了 groups.expression，则表达式必须产生字符串或字符串数组值。
	//  ""、[] 和 null 值会被视为该组映射不存在。
	// +optional
	Groups PrefixedClaimOrExpression `json:"groups,omitempty"`

	// uid 表示 uid 属性的配置选项。
	// claim 必须是单个字符串 claim。
	// 如果设置了 uid.expression，则表达式必须产生字符串值。
	// +optional
	UID ClaimOrExpression `json:"uid"`

	// extra 表示 extra 属性的配置选项。
	// expression 必须产生字符串或字符串数组值。
	// 如果值为空，则 extra 映射不存在。
	//
	// 硬编码的 extra 键/值
	// - key: "foo"
	//   valueExpression: "'bar'"
	// 这将产生一个 extra 属性 - foo: ["bar"]
	//
	// 硬编码键，值复制自 claim 值
	// - key: "foo"
	//   valueExpression: "claims.some_claim"
	// 这将产生一个 extra 属性 - foo: [value of some_claim]
	//
	// 硬编码键，值由 claim 值派生
	// - key: "admin"
	//   valueExpression: '(has(claims.is_admin) && claims.is_admin) ? "true":""'
	// 结果为：
	//  - 若 is_admin claim 存在且为 true，extra 属性 - admin: ["true"]
	//  - 若 is_admin claim 存在且为 false，或 is_admin claim 不存在，则不会添加 extra 属性
	//
	// +optional
	Extra []ExtraMapping `json:"extra,omitempty"`
}

// PrefixedClaimOrExpression 提供单个带前缀 claim 或表达式的配置。
//
// prefix 用于避免不同 IdP 的用户名/组名冲突，通常配置为 issuer URL 或固定前缀。
type PrefixedClaimOrExpression struct {
	// claim 是要使用的 JWT claim。
	// 与 expression 互斥。
	// +optional
	Claim string `json:"claim,omitempty"`
	// prefix 会添加在 claim 值之前，以避免与已有名称冲突。
	// 如果设置了 claim，则必须设置 prefix，且 prefix 可以是空字符串。
	// 与 expression 互斥。
	// +optional
	Prefix *string `json:"prefix,omitempty"`

	// expression 表示将由 CEL 求值的表达式。
	//
	// CEL 表达式可以访问令牌 claims 的内容，组织为 CEL 变量：
	// - 'claims' 是从 claim 名称到 claim 值的映射。
	//   例如，名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claims 可使用点号表示法访问，例如 'claims.foo.bar'。
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 与 claim 和 prefix 互斥。
	// +optional
	Expression string `json:"expression,omitempty"`
}

// ClaimOrExpression 提供单个 claim 或表达式的配置。
//
// 与 PrefixedClaimOrExpression 的区别是不支持 prefix（用于 uid 等无需前缀的字段）。
type ClaimOrExpression struct {
	// claim 是要使用的 JWT claim。
	// claim 和 expression 必须设置其中之一。
	// 与 expression 互斥。
	// +optional
	Claim string `json:"claim,omitempty"`

	// expression 表示将由 CEL 求值的表达式。
	//
	// CEL 表达式可以访问令牌 claims 的内容，组织为 CEL 变量：
	// - 'claims' 是从 claim 名称到 claim 值的映射。
	//   例如，名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claims 可使用点号表示法访问，例如 'claims.foo.bar'。
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 与 claim 互斥。
	// +optional
	Expression string `json:"expression,omitempty"`
}

// ExtraMapping 提供单条 extra 映射的配置。
//
// extra 键必须带域名前缀，以便与内置或第三方扩展属性区分。
type ExtraMapping struct {
	// key 是用作 extra 属性键的字符串。
	// key 必须是带域名前缀的路径（例如 example.org/foo）。第一个 "/" 之前的所有字符必须是
	// 符合 RFC 1123 定义的合法子域名。第一个 "/" 之后的所有字符必须是
	// 符合 RFC 3986 定义的合法 HTTP 路径字符。
	// key 必须是小写。
	// 必须唯一。
	// +required
	Key string `json:"key"`

	// valueExpression 是用于提取 extra 属性值的 CEL 表达式。
	// valueExpression 必须产生字符串或字符串数组值。
	// ""、[] 和 null 值会被视为该 extra 映射不存在。
	// 字符串数组中包含的空字符串值会被过滤掉。
	//
	// CEL 表达式可以访问令牌 claims 的内容，组织为 CEL 变量：
	// - 'claims' 是从 claim 名称到 claim 值的映射。
	//   例如，名为 'sub' 的变量可通过 'claims.sub' 访问。
	//   嵌套 claims 可使用点号表示法访问，例如 'claims.foo.bar'。
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// +required
	ValueExpression string `json:"valueExpression"`
}

// UserValidationRule 提供单条用户信息校验规则的配置。
//
// 认证完成的最后一道关卡，常用于阻止 IdP 用户冒用 system: 前缀的身份。
type UserValidationRule struct {
	// expression 表示将由 CEL 求值的表达式。
	// 必须返回 true 才能通过校验。
	//
	// CEL 表达式可以访问 UserInfo 的内容，组织为 CEL 变量：
	// - 'user' - authentication.k8s.io/v1, Kind=UserInfo 对象
	//    定义参见 https://github.com/kubernetes/api/blob/release-1.28/authentication/v1/types.go#L105-L122。
	//    API 文档：https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.28/#userinfo-v1-authentication-k8s-io
	//
	// Documentation on CEL: https://kubernetes.io/docs/reference/using-api/cel/
	//
	// +required
	Expression string `json:"expression"`

	// message 自定义规则返回 false 时返回的错误消息。
	// message 是一个字面量字符串。
	// +optional
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// AuthorizationConfiguration 是授权链的版本化配置，替代 --authorization-modes。
type AuthorizationConfiguration struct {
	metav1.TypeMeta `json:""`

	// Authorizers 是按顺序对请求执行授权的授权器列表。
	// 与 kube-apiserver 的 --authorization-modes 参数类似。
	// 至少必须包含一个。
	Authorizers []AuthorizerConfiguration `json:"authorizers"`
}

const (
	// 授权器类型与失败策略的合法取值；字符串会直接出现在配置文件与指标中，
	// 修改需要保持向后兼容。
	TypeWebhook                                          AuthorizerType = "Webhook"
	FailurePolicyNoOpinion                               string         = "NoOpinion"
	FailurePolicyDeny                                    string         = "Deny"
	AuthorizationWebhookConnectionInfoTypeKubeConfigFile string         = "KubeConfigFile"
	AuthorizationWebhookConnectionInfoTypeInCluster      string         = "InClusterConfig"
)

// AuthorizerType 标识授权器的类型。
type AuthorizerType string

// AuthorizerConfiguration 描述授权链中的单个授权器。
//
// Name 会出现在指标与日志中，因此必须是合法的 DNS1123 名称；Webhook 字段仅在
// Type=Webhook 时允许设置。
type AuthorizerConfiguration struct {
	// Type 指授权器的类型。
	// generic API server 支持 "Webhook"。
	// 其他 API server 可以支持额外的授权器类型，
	// 如 Node、RBAC、ABAC 等。
	Type string `json:"type"`

	// Name 用于描述该 webhook。
	// 该名称会显式用于监控机制中的指标。
	// 注意：名称必须是 DNS1123 标签（如 `myauthorizername`）或
	//		 子域名（如 `myauthorizer.example.domain`）。
	// 必填，无默认值。
	Name string `json:"name"`

	// Webhook 定义 Webhook 授权器的配置。
	// Type=Webhook 时必须定义。
	// Type!=Webhook 时必须不定义。
	Webhook *WebhookConfiguration `json:"webhook,omitempty"`
}

// WebhookConfiguration 描述 Webhook 授权器的连接、缓存与失败策略。
//
// 缓存 TTL 与是否缓存分别可配：授权结果可能在短期内变化，管理员需要在
// 一致性（短 TTL/关闭缓存）与 webhook 负载之间权衡。
type WebhookConfiguration struct {
	// 缓存 webhook 授权器返回的 'authorized' 响应的时长。
	// 等同于设置 `--authorization-webhook-cache-authorized-ttl` 参数
	// Default: 5m0s
	AuthorizedTTL metav1.Duration `json:"authorizedTTL"`
	// CacheAuthorizedRequests 指定是否应当缓存已授权（authorized）的请求。
	// 设置为 true 时，可以通过 AuthorizedTTL 字段配置
	// 缓存决策的 TTL。
	// Default: true
	// +optional
	CacheAuthorizedRequests *bool `json:"cacheAuthorizedRequests,omitempty"`
	// 缓存 webhook 授权器返回的 'unauthorized' 响应的时长。
	// 等同于设置 `--authorization-webhook-cache-unauthorized-ttl` 参数
	// Default: 30s
	UnauthorizedTTL metav1.Duration `json:"unauthorizedTTL"`
	// CacheUnauthorizedRequests 指定是否应当缓存未授权（unauthorized）的请求。
	// 设置为 true 时，可以通过 UnauthorizedTTL 字段配置
	// 缓存决策的 TTL。
	// Default: true
	// +optional
	CacheUnauthorizedRequests *bool `json:"cacheUnauthorizedRequests,omitempty"`
	// webhook 请求的超时时间
	// 允许的最大值为 30s。
	// 必填，无默认值。
	Timeout metav1.Duration `json:"timeout"`
	// 发送给 webhook 以及期望 webhook 返回的 authorization.k8s.io
	// SubjectAccessReview 的 API 版本。
	// 等同于设置 `--authorization-webhook-version` 参数
	// Valid values: v1beta1, v1
	// 必填，无默认值
	SubjectAccessReviewVersion string `json:"subjectAccessReviewVersion"`
	// MatchConditionSubjectAccessReviewVersion 指定 CEL 表达式求值所针对的
	// SubjectAccessReview 版本
	// Valid values: v1
	// 必填，无默认值
	MatchConditionSubjectAccessReviewVersion string `json:"matchConditionSubjectAccessReviewVersion"`
	// 控制 webhook 请求未能完成、返回格式错误的响应、或求值
	// matchConditions 出错时的授权决策。
	// Valid values:
	//   - NoOpinion: 继续交由后续授权器判断是否有授权器允许该请求
	//   - Deny: 拒绝该请求，不再咨询后续授权器
	// 必填，且无默认值。
	FailurePolicy string `json:"failurePolicy"`

	// ConnectionInfo 定义我们与 webhook 通信的方式
	ConnectionInfo WebhookConnectionInfo `json:"connectionInfo"`

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
	MatchConditions []WebhookMatchCondition `json:"matchConditions"`
}

// WebhookConnectionInfo 描述如何连接授权 webhook。
type WebhookConnectionInfo struct {
	// 控制 webhook 应如何与服务器通信。
	// Valid values:
	// - KubeConfigFile: 使用 kubeConfigFile 中指定的文件来定位
	//   服务器。
	// - InClusterConfig: 使用集群内配置来调用由 kube-apiserver 托管的
	//   SubjectAccessReview API。kube-apiserver 自身不允许使用此模式。
	Type string `json:"type"`

	// 连接信息中 KubeConfigFile 的文件路径
	// 必填，当 connectionInfo.Type 为 KubeConfig 时
	KubeConfigFile *string `json:"kubeConfigFile"`
}

// WebhookMatchCondition 是决定是否把请求发送给该 webhook 的 CEL 条件。
//
// 条件在 SubjectAccessReview 上求值，因此可以基于 user/group/资源属性做
// 细粒度过滤，减少不必要的远程调用。
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
	Expression string `json:"expression"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TracingConfiguration 提供 tracing 客户端的版本化配置，
// 直接嵌入 component-base 的 tracing 配置，保持与其他组件的一致。
type TracingConfiguration struct {
	metav1.TypeMeta `json:""`

	// 内嵌组件配置中的 tracing 配置结构体
	tracingapi.TracingConfiguration `json:""`
}
