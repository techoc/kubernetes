/*
Copyright 2016 The Kubernetes Authors.

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

package options

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/apis/apiserver/install"
	apiservervalidation "k8s.io/apiserver/pkg/apis/apiserver/validation"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	authenticationconfigmetrics "k8s.io/apiserver/pkg/server/options/authenticationconfig/metrics"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/filesystem"
	"k8s.io/apiserver/plugin/pkg/authenticator/token/oidc"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	v1listers "k8s.io/client-go/listers/core/v1"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/keyutil"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	serviceaccountcontroller "k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/pkg/features"
	kubeauthenticator "k8s.io/kubernetes/pkg/kubeapiserver/authenticator"
	authzmodes "k8s.io/kubernetes/pkg/kubeapiserver/authorizer/modes"
	"k8s.io/kubernetes/pkg/serviceaccount"
	"k8s.io/kubernetes/plugin/pkg/auth/authenticator/token/bootstrap"
	"k8s.io/utils/ptr"
)

const (
	oidcIssuerURLFlag      = "oidc-issuer-url"
	oidcClientIDFlag       = "oidc-client-id"
	oidcCAFileFlag         = "oidc-ca-file"
	oidcUsernameClaimFlag  = "oidc-username-claim"
	oidcUsernamePrefixFlag = "oidc-username-prefix"
	oidcGroupsClaimFlag    = "oidc-groups-claim"
	oidcGroupsPrefixFlag   = "oidc-groups-prefix"
	oidcSigningAlgsFlag    = "oidc-signing-algs"
	oidcRequiredClaimFlag  = "oidc-required-claim"
)

// UpdateAuthenticationConfigTimeout 控制等待 updateAuthenticationConfig 成功的时长。
// 导出为变量以便集成测试中覆盖。
var UpdateAuthenticationConfigTimeout = time.Minute

// BuiltInAuthenticationOptions 包含 API Server 所有内建的认证选项。
type BuiltInAuthenticationOptions struct {
	APIAudiences    []string
	Anonymous       *AnonymousAuthenticationOptions
	BootstrapToken  *BootstrapTokenAuthenticationOptions
	ClientCert      *genericoptions.ClientCertAuthenticationOptions
	OIDC            *OIDCAuthenticationOptions
	RequestHeader   *genericoptions.RequestHeaderAuthenticationOptions
	ServiceAccounts *ServiceAccountAuthenticationOptions
	TokenFile       *TokenFileAuthenticationOptions
	WebHook         *WebHookAuthenticationOptions

	AuthenticationConfigFile string

	TokenSuccessCacheTTL time.Duration
	TokenFailureCacheTTL time.Duration
}

// AnonymousAuthenticationOptions 包含 API Server 的匿名认证选项。
type AnonymousAuthenticationOptions struct {
	Allow bool
	// FlagsSet 记录该配置项是否由命令行标志显式设置，用于与配置文件互斥校验。
	FlagsSet bool
}

// BootstrapTokenAuthenticationOptions 包含 API Server 的 bootstrap token 认证选项。
type BootstrapTokenAuthenticationOptions struct {
	Enable bool
}

// OIDCAuthenticationOptions 包含 API Server 的 OIDC 认证选项。
type OIDCAuthenticationOptions struct {
	CAFile         string
	ClientID       string
	IssuerURL      string
	UsernameClaim  string
	UsernamePrefix string
	GroupsClaim    string
	GroupsPrefix   string
	SigningAlgs    []string
	RequiredClaims map[string]string

	// FlagsSet 记录任一配置是否由命令行标志显式设置，用于与配置文件互斥校验。
	FlagsSet bool
}

// ServiceAccountAuthenticationOptions 包含 API Server 的 ServiceAccount 认证选项。
type ServiceAccountAuthenticationOptions struct {
	KeyFiles              []string
	Lookup                bool
	Issuers               []string
	JWKSURI               string
	ExtendExpiration      bool
	MaxExpiration         time.Duration
	MaxExtendedExpiration time.Duration
	// OptionalTokenGetter 返回自定义的服务账号令牌获取器；未设置时使用默认值。
	OptionalTokenGetter func(factory informers.SharedInformerFactory) serviceaccount.ServiceAccountTokenGetter
	// 传入 `--service-account-signing-endpoint` 时设置 ExternalPublicKeysGetter。
	// 它与 KeyFiles 互斥。
	ExternalPublicKeysGetter serviceaccount.PublicKeysGetter
}

// TokenFileAuthenticationOptions 包含 API Server 的 token 文件认证选项。
type TokenFileAuthenticationOptions struct {
	TokenFile string
}

// WebHookAuthenticationOptions 包含 API Server 的 webhook 认证选项。
type WebHookAuthenticationOptions struct {
	ConfigFile string
	Version    string
	CacheTTL   time.Duration

	// RetryBackoff 指定认证 webhook 重试逻辑的退避参数。
	// 通过配置每次迭代的休眠时间与最大重试次数，可在系统降级时限制请求放大效应。
	RetryBackoff *wait.Backoff
}

// NewBuiltInAuthenticationOptions 创建 BuiltInAuthenticationOptions，仅设置 token 缓存 TTL 的默认值。
func NewBuiltInAuthenticationOptions() *BuiltInAuthenticationOptions {
	return &BuiltInAuthenticationOptions{
		TokenSuccessCacheTTL: 10 * time.Second,
		TokenFailureCacheTTL: 0 * time.Second,
	}
}

// WithAll 为每个内建认证选项设置默认值。
func (o *BuiltInAuthenticationOptions) WithAll() *BuiltInAuthenticationOptions {
	return o.
		WithAnonymous().
		WithBootstrapToken().
		WithClientCert().
		WithOIDC().
		WithRequestHeader().
		WithServiceAccounts().
		WithTokenFile().
		WithWebHook()
}

// WithAnonymous 为匿名认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithAnonymous() *BuiltInAuthenticationOptions {
	o.Anonymous = &AnonymousAuthenticationOptions{
		Allow: true,
	}
	return o
}

// WithBootstrapToken 为 bootstrap token 认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithBootstrapToken() *BuiltInAuthenticationOptions {
	o.BootstrapToken = &BootstrapTokenAuthenticationOptions{}
	return o
}

// WithClientCert 为客户端证书认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithClientCert() *BuiltInAuthenticationOptions {
	o.ClientCert = &genericoptions.ClientCertAuthenticationOptions{}
	return o
}

// WithOIDC 为 OIDC 认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithOIDC() *BuiltInAuthenticationOptions {
	o.OIDC = &OIDCAuthenticationOptions{
		UsernameClaim: "sub",
		SigningAlgs:   []string{"RS256"},
	}
	return o
}

// WithRequestHeader 为请求头认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithRequestHeader() *BuiltInAuthenticationOptions {
	o.RequestHeader = &genericoptions.RequestHeaderAuthenticationOptions{}
	return o
}

// WithServiceAccounts 为 ServiceAccount 认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithServiceAccounts() *BuiltInAuthenticationOptions {
	if o.ServiceAccounts == nil {
		o.ServiceAccounts = &ServiceAccountAuthenticationOptions{}
	}
	o.ServiceAccounts.Lookup = true
	o.ServiceAccounts.ExtendExpiration = true
	o.ServiceAccounts.MaxExtendedExpiration = serviceaccount.ExpirationExtensionSeconds * time.Second
	return o
}

// WithTokenFile 为 token 文件认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithTokenFile() *BuiltInAuthenticationOptions {
	o.TokenFile = &TokenFileAuthenticationOptions{}
	return o
}

// WithWebHook 为 webhook 认证设置默认值。
func (o *BuiltInAuthenticationOptions) WithWebHook() *BuiltInAuthenticationOptions {
	o.WebHook = &WebHookAuthenticationOptions{
		Version:      "v1beta1",
		CacheTTL:     2 * time.Minute,
		RetryBackoff: genericoptions.DefaultAuthWebhookRetryBackoff(),
	}
	return o
}

// Validate 检查非法的配置组合。
func (o *BuiltInAuthenticationOptions) Validate() []error {
	if o == nil {
		return nil
	}

	var allErrors []error

	allErrors = append(allErrors, o.validateOIDCOptions()...)

	if o.ServiceAccounts != nil && len(o.ServiceAccounts.Issuers) > 0 {
		seen := make(map[string]bool)
		for _, issuer := range o.ServiceAccounts.Issuers {
			if strings.Contains(issuer, ":") {
				if _, err := url.Parse(issuer); err != nil {
					allErrors = append(allErrors, fmt.Errorf("service-account-issuer %q contained a ':' but was not a valid URL: %v", issuer, err))
					continue
				}
			}
			if issuer == "" {
				allErrors = append(allErrors, fmt.Errorf("service-account-issuer should not be an empty string"))
				continue
			}
			if seen[issuer] {
				allErrors = append(allErrors, fmt.Errorf("service-account-issuer %q is already specified", issuer))
				continue
			}
			seen[issuer] = true
		}
	}

	if o.ServiceAccounts != nil {
		if len(o.ServiceAccounts.Issuers) == 0 {
			allErrors = append(allErrors, errors.New("service-account-issuer is a required flag"))
		}
		if len(o.ServiceAccounts.KeyFiles) == 0 && o.ServiceAccounts.ExternalPublicKeysGetter == nil {
			allErrors = append(allErrors, errors.New("either `--service-account-key-file` or `--service-account-signing-endpoint` must be set"))
		}

		// 仅在显式设置时校验 JWKS URI；未设置时后续会从 ExternalHost 推导。
		if o.ServiceAccounts.JWKSURI != "" {
			if u, err := url.Parse(o.ServiceAccounts.JWKSURI); err != nil {
				allErrors = append(allErrors, fmt.Errorf("service-account-jwks-uri must be a valid URL: %v", err))
			} else if u.Scheme != "https" {
				allErrors = append(allErrors, fmt.Errorf("service-account-jwks-uri requires https scheme, parsed as: %v", u.String()))
			}
		}
	}

	if o.WebHook != nil {
		retryBackoff := o.WebHook.RetryBackoff
		if retryBackoff != nil && retryBackoff.Steps <= 0 {
			allErrors = append(allErrors, fmt.Errorf("number of webhook retry attempts must be greater than 0, but is: %d", retryBackoff.Steps))
		}
	}

	if o.RequestHeader != nil {
		allErrors = append(allErrors, o.RequestHeader.Validate()...)

		if o.ClientCert != nil &&
			len(o.ClientCert.ClientCA) > 0 &&
			len(o.RequestHeader.ClientCAFile) > 0 &&
			len(o.RequestHeader.AllowedNames) == 0 {
			clientCACerts, err1 := certutil.CertsFromFile(o.ClientCert.ClientCA)
			requestHeaderCACerts, err2 := certutil.CertsFromFile(o.RequestHeader.ClientCAFile)
			if err1 == nil && err2 == nil {
				if certificatesOverlap(clientCACerts, requestHeaderCACerts) {
					allErrors = append(allErrors,
						fmt.Errorf("--requestheader-client-ca-file and --client-ca-file contain overlapping certificates; --requestheader-allowed-names must be specified to ensure regular client certificates cannot set authenticating proxy headers for arbitrary users"))
				}
			}
		}

	}

	return allErrors
}

// certificatesOverlap 判断两个 CA 证书包中是否存在至少一张相同的证书。
func certificatesOverlap(a, b []*x509.Certificate) bool {
	for _, ca := range a {
		for _, cb := range b {
			if ca.Equal(cb) {
				return true
			}
		}
	}
	return false
}

// AddFlags 将 API Server 认证相关的标志添加到指定的 FlagSet。
func (o *BuiltInAuthenticationOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.StringVar(&o.AuthenticationConfigFile, "authentication-config", o.AuthenticationConfigFile, ""+
		"File with Authentication Configuration to configure the JWT Token authenticator or the anonymous authenticator. "+
		"This flag is mutually exclusive with the --oidc-* flags if the file configures the JWT Token authenticator. "+
		"This flag is mutually exclusive with --anonymous-auth if the file configures the Anonymous authenticator.")

	fs.StringSliceVar(&o.APIAudiences, "api-audiences", o.APIAudiences, ""+
		"Identifiers of the API. The service account token authenticator will validate that "+
		"tokens used against the API are bound to at least one of these audiences. If the "+
		"--service-account-issuer flag is configured and this flag is not, this field "+
		"defaults to a single element list containing the issuer URL.")

	if o.Anonymous != nil {
		fs.BoolVar(&o.Anonymous.Allow, "anonymous-auth", o.Anonymous.Allow, ""+
			"Enables anonymous requests to the secure port of the API server. "+
			"Requests that are not rejected by another authentication method are treated as anonymous requests. "+
			"Anonymous requests have a username of system:anonymous, and a group name of system:unauthenticated.")
		trackProvidedFlag(fs, "anonymous-auth", &o.Anonymous.FlagsSet)
	}

	if o.BootstrapToken != nil {
		fs.BoolVar(&o.BootstrapToken.Enable, "enable-bootstrap-token-auth", o.BootstrapToken.Enable, ""+
			"Enable to allow secrets of type 'bootstrap.kubernetes.io/token' in the 'kube-system' "+
			"namespace to be used for TLS bootstrapping authentication.")
	}

	if o.ClientCert != nil {
		o.ClientCert.AddFlags(fs)
	}

	if o.OIDC != nil {
		fs.StringVar(&o.OIDC.IssuerURL, oidcIssuerURLFlag, o.OIDC.IssuerURL, ""+
			"The URL of the OpenID issuer, only HTTPS scheme will be accepted. "+
			"If set, it will be used to verify the OIDC JSON Web Token (JWT).")
		trackProvidedFlag(fs, oidcIssuerURLFlag, &o.OIDC.FlagsSet)

		fs.StringVar(&o.OIDC.ClientID, oidcClientIDFlag, o.OIDC.ClientID, ""+
			"The client ID for the OpenID Connect client, must be set if oidc-issuer-url is set.")
		trackProvidedFlag(fs, oidcClientIDFlag, &o.OIDC.FlagsSet)

		fs.StringVar(&o.OIDC.CAFile, oidcCAFileFlag, o.OIDC.CAFile, ""+
			"If set, the OpenID server's certificate will be verified by one of the authorities "+
			"in the oidc-ca-file, otherwise the host's root CA set will be used.")
		trackProvidedFlag(fs, oidcCAFileFlag, &o.OIDC.FlagsSet)

		fs.StringVar(&o.OIDC.UsernameClaim, oidcUsernameClaimFlag, o.OIDC.UsernameClaim, ""+
			"The OpenID claim to use as the user name. Note that claims other than the default ('sub') "+
			"is not guaranteed to be unique and immutable. This flag is experimental, please see "+
			"the authentication documentation for further details.")
		trackProvidedFlag(fs, oidcUsernameClaimFlag, &o.OIDC.FlagsSet)

		fs.StringVar(&o.OIDC.UsernamePrefix, oidcUsernamePrefixFlag, o.OIDC.UsernamePrefix, ""+
			"If provided, all usernames will be prefixed with this value. If not provided, "+
			"username claims other than 'email' are prefixed by the issuer URL to avoid "+
			"clashes. To skip any prefixing, provide the value '-'.")
		trackProvidedFlag(fs, oidcUsernamePrefixFlag, &o.OIDC.FlagsSet)

		fs.StringVar(&o.OIDC.GroupsClaim, oidcGroupsClaimFlag, o.OIDC.GroupsClaim, ""+
			"If provided, the name of a custom OpenID Connect claim for specifying user groups. "+
			"The claim value is expected to be a string or array of strings. This flag is experimental, "+
			"please see the authentication documentation for further details.")
		trackProvidedFlag(fs, oidcGroupsClaimFlag, &o.OIDC.FlagsSet)

		fs.StringVar(&o.OIDC.GroupsPrefix, oidcGroupsPrefixFlag, o.OIDC.GroupsPrefix, ""+
			"If provided, all groups will be prefixed with this value to prevent conflicts with "+
			"other authentication strategies.")
		trackProvidedFlag(fs, oidcGroupsPrefixFlag, &o.OIDC.FlagsSet)

		fs.StringSliceVar(&o.OIDC.SigningAlgs, oidcSigningAlgsFlag, o.OIDC.SigningAlgs, ""+
			"Comma-separated list of allowed JOSE asymmetric signing algorithms. JWTs with a "+
			"supported 'alg' header values are: RS256, RS384, RS512, ES256, ES384, ES512, PS256, PS384, PS512. "+
			"Values are defined by RFC 7518 https://tools.ietf.org/html/rfc7518#section-3.1.")
		trackProvidedFlag(fs, oidcSigningAlgsFlag, &o.OIDC.FlagsSet)

		fs.Var(cliflag.NewMapStringStringNoSplit(&o.OIDC.RequiredClaims), oidcRequiredClaimFlag, ""+
			"A key=value pair that describes a required claim in the ID Token. "+
			"If set, the claim is verified to be present in the ID Token with a matching value. "+
			"Repeat this flag to specify multiple claims.")
		trackProvidedFlag(fs, oidcRequiredClaimFlag, &o.OIDC.FlagsSet)
	}

	if o.RequestHeader != nil {
		o.RequestHeader.AddFlags(fs)
	}

	if o.ServiceAccounts != nil {
		fs.StringArrayVar(&o.ServiceAccounts.KeyFiles, "service-account-key-file", o.ServiceAccounts.KeyFiles, ""+
			"File containing PEM-encoded x509 RSA or ECDSA private or public keys, used to verify "+
			"ServiceAccount tokens. The specified file can contain multiple keys, and the flag can "+
			"be specified multiple times with different files. If unspecified, "+
			"--tls-private-key-file is used. Must be specified when "+
			"--service-account-signing-key-file is provided")

		fs.BoolVar(&o.ServiceAccounts.Lookup, "service-account-lookup", o.ServiceAccounts.Lookup,
			"If true, validate ServiceAccount tokens exist in etcd as part of authentication.")

		fs.StringArrayVar(&o.ServiceAccounts.Issuers, "service-account-issuer", o.ServiceAccounts.Issuers, ""+
			"Identifier of the service account token issuer. The issuer will assert this identifier "+
			"in \"iss\" claim of issued tokens. This value is a string or URI. If this option is not "+
			"a valid URI per the OpenID Discovery 1.0 spec, the ServiceAccountIssuerDiscovery feature "+
			"will remain disabled, even if the feature gate is set to true. It is highly recommended "+
			"that this value comply with the OpenID spec: https://openid.net/specs/openid-connect-discovery-1_0.html. "+
			"In practice, this means that service-account-issuer must be an https URL. It is also highly "+
			"recommended that this URL be capable of serving OpenID discovery documents at "+
			"{service-account-issuer}/.well-known/openid-configuration. "+
			"When this flag is specified multiple times, the first is used to generate tokens "+
			"and all are used to determine which issuers are accepted.")

		fs.StringVar(&o.ServiceAccounts.JWKSURI, "service-account-jwks-uri", o.ServiceAccounts.JWKSURI, ""+
			"Overrides the URI for the JSON Web Key Set in the discovery doc served at "+
			"/.well-known/openid-configuration. This flag is useful if the discovery doc "+
			"and key set are served to relying parties from a URL other than the "+
			"API server's external (as auto-detected or overridden with external-hostname).")

		fs.DurationVar(&o.ServiceAccounts.MaxExpiration, "service-account-max-token-expiration", o.ServiceAccounts.MaxExpiration, ""+
			"The maximum validity duration of a token created by the service account token issuer. If an otherwise valid "+
			"TokenRequest with a validity duration larger than this value is requested, a token will be issued with a validity duration of this value.")

		fs.BoolVar(&o.ServiceAccounts.ExtendExpiration, "service-account-extend-token-expiration", o.ServiceAccounts.ExtendExpiration, ""+
			"Turns on projected service account expiration extension during token generation, "+
			"which helps safe transition from legacy token to bound service account token feature. "+
			"If this flag is enabled, admission injected tokens would be extended up to 1 year to "+
			"prevent unexpected failure during transition, ignoring value of service-account-max-token-expiration.")
	}

	if o.TokenFile != nil {
		fs.StringVar(&o.TokenFile.TokenFile, "token-auth-file", o.TokenFile.TokenFile, ""+
			"If set, the file that will be used to secure the secure port of the API server "+
			"via token authentication.")
	}

	if o.WebHook != nil {
		fs.StringVar(&o.WebHook.ConfigFile, "authentication-token-webhook-config-file", o.WebHook.ConfigFile, ""+
			"File with webhook configuration for token authentication in kubeconfig format. "+
			"The API server will query the remote service to determine authentication for bearer tokens.")

		fs.StringVar(&o.WebHook.Version, "authentication-token-webhook-version", o.WebHook.Version, ""+
			"The API version of the authentication.k8s.io TokenReview to send to and expect from the webhook.")

		fs.DurationVar(&o.WebHook.CacheTTL, "authentication-token-webhook-cache-ttl", o.WebHook.CacheTTL,
			"The duration to cache responses from the webhook token authenticator.")
	}
}

// ToAuthenticationConfig 将 BuiltInAuthenticationOptions 转换为 kubeauthenticator.Config。
// o 为 nil 时返回空配置。
func (o *BuiltInAuthenticationOptions) ToAuthenticationConfig() (kubeauthenticator.Config, error) {
	if o == nil {
		return kubeauthenticator.Config{}, nil
	}

	ret := kubeauthenticator.Config{
		TokenSuccessCacheTTL: o.TokenSuccessCacheTTL,
		TokenFailureCacheTTL: o.TokenFailureCacheTTL,
	}

	if o.BootstrapToken != nil {
		ret.BootstrapToken = o.BootstrapToken.Enable
	}

	if o.ClientCert != nil {
		var err error
		ret.ClientCAContentProvider, err = o.ClientCert.GetClientCAContentProvider()
		if err != nil {
			return kubeauthenticator.Config{}, err
		}
	}

	// 提供配置文件时从中加载认证配置，否则使用空配置对象。
	if len(o.AuthenticationConfigFile) > 0 {
		var err error
		if ret.AuthenticationConfig, ret.AuthenticationConfigData, err = loadAuthenticationConfig(o.AuthenticationConfigFile); err != nil {
			return kubeauthenticator.Config{}, fmt.Errorf("failed to load authentication configuration from file %q: %w", o.AuthenticationConfigFile, err)
		}
	} else {
		ret.AuthenticationConfig = &apiserver.AuthenticationConfiguration{}
	}

	// JWT 认证器既可来自配置文件也可来自标志。
	if len(o.AuthenticationConfigFile) > 0 {
		// 使用配置文件时允许所有已知签名算法。
		// TODO: 真正想表达的是“只要匹配公钥，任何算法都可以”。
		ret.OIDCSigningAlgs = oidc.AllValidSigningAlgorithms()
	} else if o.OIDC != nil && len(o.OIDC.IssuerURL) > 0 && len(o.OIDC.ClientID) > 0 {
		usernamePrefix := o.OIDC.UsernamePrefix

		if o.OIDC.UsernamePrefix == "" && o.OIDC.UsernameClaim != "email" {
			// 兼容旧标志行为：未提供前缀时，除 email 外的所有声明都加上 issuerURL 前缀。
			// 见 https://github.com/kubernetes/kubernetes/issues/31380
			usernamePrefix = o.OIDC.IssuerURL + "#"
		}
		if o.OIDC.UsernamePrefix == "-" {
			// 特殊值，表示不添加任何前缀。
			usernamePrefix = ""
		}

		jwtAuthenticator := apiserver.JWTAuthenticator{
			Issuer: apiserver.Issuer{
				URL:       o.OIDC.IssuerURL,
				Audiences: []string{o.OIDC.ClientID},
			},
			ClaimMappings: apiserver.ClaimMappings{
				Username: apiserver.PrefixedClaimOrExpression{
					Prefix: ptr.To(usernamePrefix),
					Claim:  o.OIDC.UsernameClaim,
				},
			},
		}

		if len(o.OIDC.GroupsClaim) > 0 {
			jwtAuthenticator.ClaimMappings.Groups = apiserver.PrefixedClaimOrExpression{
				Prefix: ptr.To(o.OIDC.GroupsPrefix),
				Claim:  o.OIDC.GroupsClaim,
			}
		}

		if len(o.OIDC.CAFile) != 0 {
			caContent, err := os.ReadFile(o.OIDC.CAFile)
			if err != nil {
				return kubeauthenticator.Config{}, err
			}
			jwtAuthenticator.Issuer.CertificateAuthority = string(caContent)
		}

		if len(o.OIDC.RequiredClaims) > 0 {
			claimValidationRules := make([]apiserver.ClaimValidationRule, 0, len(o.OIDC.RequiredClaims))
			for claim, value := range o.OIDC.RequiredClaims {
				claimValidationRules = append(claimValidationRules, apiserver.ClaimValidationRule{
					Claim:         claim,
					RequiredValue: value,
				})
			}
			jwtAuthenticator.ClaimValidationRules = claimValidationRules
		}

		ret.AuthenticationConfig.JWT = []apiserver.JWTAuthenticator{jwtAuthenticator}

		ret.OIDCSigningAlgs = o.OIDC.SigningAlgs
	}

	// 匿名认证器同样可来自配置文件或标志，两者互斥。
	if o.Anonymous != nil {
		switch {
		case ret.AuthenticationConfig.Anonymous != nil && o.Anonymous.FlagsSet:
			// 标志与配置文件互斥。
			return kubeauthenticator.Config{}, field.Forbidden(field.NewPath("anonymous"), "--anonymous-auth flag cannot be set when anonymous field is configured in authentication configuration file")
		case ret.AuthenticationConfig.Anonymous != nil:
			// 优先采用配置文件中的值。
			ret.Anonymous = *ret.AuthenticationConfig.Anonymous
		default:
			// 否则采用标志指定的值。
			ret.Anonymous = apiserver.AnonymousAuthConfig{Enabled: o.Anonymous.Allow}
		}
	}

	if err := apiservervalidation.ValidateAuthenticationConfiguration(authenticationcel.NewDefaultCompiler(), ret.AuthenticationConfig, ret.ServiceAccountIssuers).ToAggregate(); err != nil {
		return kubeauthenticator.Config{}, fmt.Errorf("invalid authentication configuration: %w", err)
	}

	if o.RequestHeader != nil {
		var err error
		ret.RequestHeaderConfig, err = o.RequestHeader.ToAuthenticationRequestHeaderConfig()
		if err != nil {
			return kubeauthenticator.Config{}, err
		}
	}

	ret.APIAudiences = o.APIAudiences
	if o.ServiceAccounts != nil {
		if len(o.ServiceAccounts.Issuers) != 0 && len(o.APIAudiences) == 0 {
			ret.APIAudiences = authenticator.Audiences(o.ServiceAccounts.Issuers)
		}

		switch {
		case len(o.ServiceAccounts.KeyFiles) > 0 && o.ServiceAccounts.ExternalPublicKeysGetter != nil:
			return kubeauthenticator.Config{}, fmt.Errorf("cannot set mutually exclusive flags `--service-account-key-file` and `--service-account-signing-endpoint` at the same time")
		case len(o.ServiceAccounts.KeyFiles) > 0:
			allPublicKeys := []interface{}{}
			for _, keyfile := range o.ServiceAccounts.KeyFiles {
				publicKeys, err := keyutil.PublicKeysFromFile(keyfile)
				if err != nil {
					return kubeauthenticator.Config{}, err
				}
				allPublicKeys = append(allPublicKeys, publicKeys...)
			}
			keysGetter, err := serviceaccount.StaticPublicKeysGetter(allPublicKeys)
			if err != nil {
				return kubeauthenticator.Config{}, fmt.Errorf("failed to set up public service account keys: %w", err)
			}
			ret.ServiceAccountPublicKeysGetter = keysGetter
		case o.ServiceAccounts.ExternalPublicKeysGetter != nil:
			ret.ServiceAccountPublicKeysGetter = o.ServiceAccounts.ExternalPublicKeysGetter
		}

		ret.ServiceAccountIssuers = o.ServiceAccounts.Issuers
		ret.ServiceAccountLookup = o.ServiceAccounts.Lookup
	}

	if o.TokenFile != nil {
		ret.TokenAuthFile = o.TokenFile.TokenFile
	}

	if o.WebHook != nil {
		ret.WebhookTokenAuthnConfigFile = o.WebHook.ConfigFile
		ret.WebhookTokenAuthnVersion = o.WebHook.Version
		ret.WebhookTokenAuthnCacheTTL = o.WebHook.CacheTTL
		ret.WebhookRetryBackoff = o.WebHook.RetryBackoff

		if len(o.WebHook.ConfigFile) > 0 && o.WebHook.CacheTTL > 0 {
			if o.TokenSuccessCacheTTL > 0 && o.WebHook.CacheTTL < o.TokenSuccessCacheTTL {
				klog.Warningf("the webhook cache ttl of %s is shorter than the overall cache ttl of %s for successful token authentication attempts.", o.WebHook.CacheTTL, o.TokenSuccessCacheTTL)
			}
			if o.TokenFailureCacheTTL > 0 && o.WebHook.CacheTTL < o.TokenFailureCacheTTL {
				klog.Warningf("the webhook cache ttl of %s is shorter than the overall cache ttl of %s for failed token authentication attempts.", o.WebHook.CacheTTL, o.TokenFailureCacheTTL)
			}
		}
	}

	return ret, nil
}

// ApplyTo 要求已应用 OpenAPIConfig，并在存在时已应用 EgressSelector。
// 入参 ctx 控制用于重载认证配置文件的后台 goroutine 的生命周期。
func (o *BuiltInAuthenticationOptions) ApplyTo(
	ctx context.Context,
	authInfo *genericapiserver.AuthenticationInfo,
	secureServing *genericapiserver.SecureServingInfo,
	egressSelector *egressselector.EgressSelector,
	openAPIConfig *openapicommon.Config,
	openAPIV3Config *openapicommon.OpenAPIV3Config,
	extclient kubernetes.Interface,
	versionedInformer informers.SharedInformerFactory,
	apiServerID string) error {
	if o == nil {
		return nil
	}

	if openAPIConfig == nil {
		return errors.New("uninitialized OpenAPIConfig")
	}

	authenticatorConfig, err := o.ToAuthenticationConfig()
	if err != nil {
		return err
	}

	if authenticatorConfig.ClientCAContentProvider != nil {
		if err = authInfo.ApplyClientCert(authenticatorConfig.ClientCAContentProvider, secureServing); err != nil {
			return fmt.Errorf("unable to load client CA file: %v", err)
		}
	}
	if authenticatorConfig.RequestHeaderConfig != nil && authenticatorConfig.RequestHeaderConfig.CAContentProvider != nil {
		if err = authInfo.ApplyClientCert(authenticatorConfig.RequestHeaderConfig.CAContentProvider, secureServing); err != nil {
			return fmt.Errorf("unable to load client CA file: %v", err)
		}
	}

	authInfo.RequestHeaderConfig = authenticatorConfig.RequestHeaderConfig
	authInfo.APIAudiences = o.APIAudiences
	if o.ServiceAccounts != nil && len(o.ServiceAccounts.Issuers) != 0 && len(o.APIAudiences) == 0 {
		authInfo.APIAudiences = authenticator.Audiences(o.ServiceAccounts.Issuers)
	}

	// 优先使用注入的自定义 token getter，否则使用默认的基于 client 的实现。
	if o.ServiceAccounts != nil && o.ServiceAccounts.OptionalTokenGetter != nil {
		authenticatorConfig.ServiceAccountTokenGetter = o.ServiceAccounts.OptionalTokenGetter(versionedInformer)
	} else {
		var nodeLister v1listers.NodeLister
		if utilfeature.DefaultFeatureGate.Enabled(features.ServiceAccountTokenNodeBindingValidation) {
			nodeLister = versionedInformer.Core().V1().Nodes().Lister()
		}

		authenticatorConfig.ServiceAccountTokenGetter = serviceaccountcontroller.NewGetterFromClient(
			extclient,
			versionedInformer.Core().V1().Secrets().Lister(),
			versionedInformer.Core().V1().ServiceAccounts().Lister(),
			versionedInformer.Core().V1().Pods().Lister(),
			nodeLister,
			versionedInformer.Admissionregistration().V1().ValidatingWebhookConfigurations().Lister(),
			versionedInformer.Admissionregistration().V1().MutatingWebhookConfigurations().Lister(),
		)
	}
	authenticatorConfig.SecretsWriter = extclient.CoreV1()

	if authenticatorConfig.BootstrapToken {
		authenticatorConfig.BootstrapTokenAuthenticator = bootstrap.NewTokenAuthenticator(
			versionedInformer.Core().V1().Secrets().Lister().Secrets(metav1.NamespaceSystem),
		)
	}

	if egressSelector != nil {
		egressDialer, err := egressSelector.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return err
		}
		authenticatorConfig.CustomDial = egressDialer
		authenticatorConfig.EgressLookup = egressSelector.Lookup
	}

	authenticatorConfig.APIServerID = apiServerID
	// New 同时返回认证器、热更新函数及 OpenAPI 安全定义。
	authenticator, updateAuthenticationConfig, openAPIV2SecurityDefinitions, openAPIV3SecuritySchemes, err := authenticatorConfig.New(ctx)
	if err != nil {
		return err
	}
	authInfo.Authenticator = authenticator

	if len(o.AuthenticationConfigFile) > 0 {
		authenticationconfigmetrics.RegisterMetrics()
		authenticationconfigmetrics.RecordAuthenticationConfigLastConfigInfo(apiServerID, authenticatorConfig.AuthenticationConfigData)
		trackedAuthenticationConfigData := authenticatorConfig.AuthenticationConfigData
		var mu sync.Mutex

		// 匿名认证配置在热重载期间不允许变化，先保存初始快照用于比对。
		originalFileAnonymousConfig := authenticatorConfig.AuthenticationConfig.DeepCopy().Anonymous

		go filesystem.WatchUntil(
			ctx,
			time.Minute,
			o.AuthenticationConfigFile,
			func() {
				// TODO: 与 DynamicEncryptionConfigContent 控制器共用重载逻辑。

				// 串行处理文件变更，避免并发回调交错更新跟踪状态。
				mu.Lock()
				defer mu.Unlock()

				authConfigBytes, err := os.ReadFile(o.AuthenticationConfigFile)
				if err != nil {
					klog.ErrorS(err, "failed to read authentication config file")
					authenticationconfigmetrics.RecordAuthenticationConfigAutomaticReloadFailure(apiServerID)
					// 读取失败可能是暂时的，不更新跟踪值以便继续重试。
					return
				}

				authConfigData := string(authConfigBytes)

				if authConfigData == trackedAuthenticationConfigData {
					return
				}

				authConfig, err := loadAuthenticationConfigFromData(authConfigBytes)
				if err != nil {
					klog.ErrorS(err, "failed to load authentication config")
					authenticationconfigmetrics.RecordAuthenticationConfigAutomaticReloadFailure(apiServerID)
					// 结构非法永远不会变好，更新跟踪值以停止重试。
					trackedAuthenticationConfigData = authConfigData
					return
				}

				validationErrs := apiservervalidation.ValidateAuthenticationConfiguration(authenticationcel.NewDefaultCompiler(), authConfig, authenticatorConfig.ServiceAccountIssuers)
				if !reflect.DeepEqual(originalFileAnonymousConfig, authConfig.Anonymous) {
					validationErrs = append(validationErrs, field.Forbidden(field.NewPath("anonymous"), "changed from initial configuration file"))
				}
				if err := validationErrs.ToAggregate(); err != nil {
					klog.ErrorS(err, "failed to validate authentication config")
					authenticationconfigmetrics.RecordAuthenticationConfigAutomaticReloadFailure(apiServerID)
					// 语义非法永远不会变好，更新跟踪值以停止重试。
					trackedAuthenticationConfigData = authConfigData
					return
				}

				timeoutCtx, timeoutCancel := context.WithTimeout(ctx, UpdateAuthenticationConfigTimeout)
				defer timeoutCancel()
				if err := updateAuthenticationConfig(timeoutCtx, authConfig); err != nil {
					klog.ErrorS(err, "failed to update authentication config")
					authenticationconfigmetrics.RecordAuthenticationConfigAutomaticReloadFailure(apiServerID)
					// 更新失败可能是暂时的，不更新跟踪值以便继续重试。
					return
				}

				trackedAuthenticationConfigData = authConfigData
				klog.InfoS("reloaded authentication config")
				authenticationconfigmetrics.RecordAuthenticationConfigAutomaticReloadSuccess(apiServerID, authConfigData)
			},
			func(err error) { klog.ErrorS(err, "watching authentication config file") },
		)
	}

	openAPIConfig.SecurityDefinitions = openAPIV2SecurityDefinitions
	if openAPIV3Config != nil {
		openAPIV3Config.SecuritySchemes = openAPIV3SecuritySchemes
	}
	return nil
}

// ApplyAuthorization 根据授权选项有条件地调整认证选项。
func (o *BuiltInAuthenticationOptions) ApplyAuthorization(authorization *BuiltInAuthorizationOptions) {
	if o == nil || authorization == nil || o.Anonymous == nil {
		return
	}

	// AlwaysAllow 授权模式与匿名认证互斥，此时强制关闭匿名认证并记录告警。
	if o.Anonymous.Allow && sets.NewString(authorization.Modes...).Has(authzmodes.ModeAlwaysAllow) {
		klog.Warningf("AnonymousAuth is not allowed with the AlwaysAllow authorizer. Resetting AnonymousAuth to false. You should use a different authorizer")
		o.Anonymous.Allow = false
	}
}

// trackProvidedFlag 包装标志的值，记录它是否被显式设置过。
func trackProvidedFlag(fs *pflag.FlagSet, flagName string, provided *bool) {
	f := fs.Lookup(flagName)
	f.Value = cliflag.NewTracker(f.Value, provided)
}

// validateOIDCOptions 校验 OIDC 标志与认证配置文件的互斥关系及完整性。
func (o *BuiltInAuthenticationOptions) validateOIDCOptions() []error {
	var allErrors []error

	// 通过 oidc-* 标志配置 JWT 认证器时的既有校验。
	if len(o.AuthenticationConfigFile) == 0 {
		if o.OIDC != nil && o.OIDC.FlagsSet && (len(o.OIDC.IssuerURL) == 0 || len(o.OIDC.ClientID) == 0) {
			allErrors = append(allErrors, fmt.Errorf("oidc-issuer-url and oidc-client-id must be specified together when any oidc-* flags are set"))
		}

		return allErrors
	}

	// 提供认证配置文件时的新增校验：配置文件与 oidc-* 标志互斥。
	if o.OIDC != nil && o.OIDC.FlagsSet {
		allErrors = append(allErrors, fmt.Errorf("authentication-config file and oidc-* flags are mutually exclusive"))
	}

	return allErrors
}

var (
	cfgScheme = runtime.NewScheme()
	codecs    = serializer.NewCodecFactory(cfgScheme, serializer.EnableStrict)
)

// init 安装认证配置 API 到 scheme，供严格解码使用。
func init() {
	install.Install(cfgScheme)
}

// loadAuthenticationConfig 从给定文件读取认证配置，并返回配置及原始文件内容。
func loadAuthenticationConfig(configFilePath string) (*apiserver.AuthenticationConfiguration, string, error) {
	data, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, "", err
	}

	configuration, err := loadAuthenticationConfigFromData(data)
	if err != nil {
		return nil, "", err
	}

	return configuration, string(data), nil
}

// loadAuthenticationConfigFromData 从字节数据中严格解码认证配置。
func loadAuthenticationConfigFromData(data []byte) (*apiserver.AuthenticationConfiguration, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty config data")
	}

	decodedObj, err := runtime.Decode(codecs.UniversalDecoder(), data)
	if err != nil {
		return nil, err
	}
	configuration, ok := decodedObj.(*apiserver.AuthenticationConfiguration)
	if !ok {
		return nil, fmt.Errorf("expected AuthenticationConfiguration, got %T", decodedObj)
	}
	if configuration == nil { // 后续逻辑依赖非 nil 配置；正常情况下不会为空，仍作防御性检查
		return nil, fmt.Errorf("expected non-nil AuthenticationConfiguration")
	}

	return configuration, nil
}
