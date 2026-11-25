/*
Copyright 2014 The Kubernetes Authors.

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

package authenticator

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/authenticatorfactory"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/apiserver/pkg/authentication/request/anonymous"
	"k8s.io/apiserver/pkg/authentication/request/bearertoken"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	"k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/request/websocket"
	"k8s.io/apiserver/pkg/authentication/request/x509"
	tokencache "k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/authentication/token/tokenfile"
	tokenunion "k8s.io/apiserver/pkg/authentication/token/union"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/egressselector"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/apiserver/plugin/pkg/authenticator/token/oidc"
	"k8s.io/apiserver/plugin/pkg/authenticator/token/webhook"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kube-openapi/pkg/spec3"
	"k8s.io/kube-openapi/pkg/validation/spec"

	// 注册所有已知客户端认证插件，供 webhook kubeconfig 等场景使用。
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

// Config 包含如何对 kube-apiserver 的请求进行认证的数据。
type Config struct {
	// Anonymous 保存生效的匿名认证配置，可能来自配置文件
	//（从 AuthenticationConfig 中提取）或由标志值构造。
	Anonymous apiserver.AnonymousAuthConfig

	BootstrapToken bool

	TokenAuthFile               string
	AuthenticationConfig        *apiserver.AuthenticationConfiguration
	AuthenticationConfigData    string
	OIDCSigningAlgs             []string
	ServiceAccountLookup        bool
	ServiceAccountIssuers       []string
	APIAudiences                authenticator.Audiences
	WebhookTokenAuthnConfigFile string
	WebhookTokenAuthnVersion    string
	WebhookTokenAuthnCacheTTL   time.Duration
	// WebhookRetryBackoff 指定认证 webhook 重试逻辑的退避参数。
	// 通过配置每次迭代的休眠时间与最大重试次数，可在系统降级时限制请求放大效应。
	WebhookRetryBackoff *wait.Backoff

	TokenSuccessCacheTTL time.Duration
	TokenFailureCacheTTL time.Duration

	RequestHeaderConfig *authenticatorfactory.RequestHeaderConfig

	// ServiceAccountPublicKeysGetter 返回用于校验 ServiceAccount token 的公钥。
	ServiceAccountPublicKeysGetter serviceaccount.PublicKeysGetter
	// ServiceAccountTokenGetter 获取用于校验 ServiceAccount token 声明中绑定对象的 API 对象。
	ServiceAccountTokenGetter   serviceaccount.ServiceAccountTokenGetter
	SecretsWriter               typedv1core.SecretsGetter
	BootstrapTokenAuthenticator authenticator.Token
	// ClientCAContentProvider 提供 mTLS 校验连接并直接映射用户的选项，
	// 通常是用于认证客户端证书的 CA 包；为 nil 时禁用双向 TLS。
	ClientCAContentProvider dynamiccertificates.CAContentProvider

	// 可选字段，连接 webhook 时使用的自定义拨号函数。
	CustomDial utilnet.DialFunc

	EgressLookup egressselector.Lookup

	// APIServerID 是该 API server 的标识。
	APIServerID string
}

// New 返回一个支持 Kubernetes 标准认证机制的 authenticator.Request 或错误。
func (config Config) New(serverLifecycle context.Context) (authenticator.Request, func(context.Context, *apiserver.AuthenticationConfiguration) error, *spec.SecurityDefinitions, spec3.SecuritySchemes, error) {
	var authenticators []authenticator.Request
	var tokenAuthenticators []authenticator.Token
	securityDefinitionsV2 := spec.SecurityDefinitions{}
	securitySchemesV3 := spec3.SecuritySchemes{}

	// 认证器按“本地优先、远程其次”的顺序排列：
	// front-proxy、BasicAuth 等先于远程 webhook 执行。
	if config.RequestHeaderConfig != nil {
		requestHeaderAuthenticator := headerrequest.NewDynamicVerifyOptionsSecure(
			config.RequestHeaderConfig.CAContentProvider.VerifyOptions,
			config.RequestHeaderConfig.AllowedClientNames,
			config.RequestHeaderConfig.UsernameHeaders,
			config.RequestHeaderConfig.UIDHeaders,
			config.RequestHeaderConfig.GroupHeaders,
			config.RequestHeaderConfig.ExtraHeaderPrefixes,
		)
		authenticators = append(authenticators, authenticator.WrapAudienceAgnosticRequest(config.APIAudiences, requestHeaderAuthenticator))
	}

	// X509 客户端证书认证。
	if config.ClientCAContentProvider != nil {
		certAuth := x509.NewDynamic(config.ClientCAContentProvider.VerifyOptions, x509.CommonNameUserConversion)
		authenticators = append(authenticators, certAuth)
	}

	// Bearer token 类认证器同样是本地优先、远程其次。
	if len(config.TokenAuthFile) > 0 {
		tokenAuth, err := newAuthenticatorFromTokenFile(config.TokenAuthFile)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		tokenAuthenticators = append(tokenAuthenticators, authenticator.WrapAudienceAgnosticToken(config.APIAudiences, tokenAuth))
	}
	if config.ServiceAccountPublicKeysGetter != nil {
		serviceAccountAuth, err := newLegacyServiceAccountAuthenticator(config.ServiceAccountPublicKeysGetter, config.ServiceAccountLookup, config.APIAudiences, config.ServiceAccountTokenGetter, config.SecretsWriter)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		tokenAuthenticators = append(tokenAuthenticators, serviceAccountAuth)
	}
	if len(config.ServiceAccountIssuers) > 0 && config.ServiceAccountPublicKeysGetter != nil {
		serviceAccountAuth, err := newServiceAccountAuthenticator(config.ServiceAccountIssuers, config.ServiceAccountPublicKeysGetter, config.APIAudiences, config.ServiceAccountTokenGetter)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		tokenAuthenticators = append(tokenAuthenticators, serviceAccountAuth)
	}

	if config.BootstrapToken && config.BootstrapTokenAuthenticator != nil {
		tokenAuthenticators = append(tokenAuthenticators, authenticator.WrapAudienceAgnosticToken(config.APIAudiences, config.BootstrapTokenAuthenticator))
	}

	// NOTE(ericchiang): 必须让 OpenID Connect 排在 Service Account 之后。
	//
	// 两者都校验 JWT，先命中的插件会对使用另一类 token 的请求产生缓存未命中；
	// Service Account 插件仅返回错误，而 OIDC 插件可能查询 provider 更新密钥，造成性能损耗。
	var updateAuthenticationConfig func(context.Context, *apiserver.AuthenticationConfiguration) error
	if config.AuthenticationConfig != nil {
		initialJWTAuthenticator, err := newJWTAuthenticator(serverLifecycle, config.AuthenticationConfig, config.OIDCSigningAlgs, config.APIAudiences, config.ServiceAccountIssuers, config.EgressLookup, config.APIServerID)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		jwtAuthenticatorPtr := &atomic.Pointer[jwtAuthenticatorWithCancel]{}
		jwtAuthenticatorPtr.Store(initialJWTAuthenticator)

		initialIssuers := sets.New[string]()
		for _, jwt := range config.AuthenticationConfig.JWT {
			initialIssuers.Insert(jwt.Issuer.URL)
		}

		updateAuthenticationConfig = (&authenticationConfigUpdater{
			serverLifecycle:     serverLifecycle,
			config:              config,
			jwtAuthenticatorPtr: jwtAuthenticatorPtr,
			issuers:             initialIssuers,
		}).updateAuthenticationConfig

		tokenAuthenticators = append(tokenAuthenticators,
			authenticator.TokenFunc(func(ctx context.Context, token string) (*authenticator.Response, bool, error) {
				return jwtAuthenticatorPtr.Load().jwtAuthenticator.AuthenticateToken(ctx, token)
			}),
		)
	}

	if len(config.WebhookTokenAuthnConfigFile) > 0 {
		webhookTokenAuth, err := newWebhookTokenAuthenticator(config)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		tokenAuthenticators = append(tokenAuthenticators, webhookTokenAuth)
	}

	if len(tokenAuthenticators) > 0 {
		// 先合并所有 token 认证器，再按配置缓存认证结果以减少重复开销。
		tokenAuth := tokenunion.New(tokenAuthenticators...)
		if config.TokenSuccessCacheTTL > 0 || config.TokenFailureCacheTTL > 0 {
			tokenAuth = tokencache.New(tokenAuth, true, config.TokenSuccessCacheTTL, config.TokenFailureCacheTTL)
		}
		authenticators = append(authenticators, bearertoken.New(tokenAuth), websocket.NewProtocolAuthenticator(tokenAuth))

		securityDefinitionsV2["BearerToken"] = &spec.SecurityScheme{
			SecuritySchemeProps: spec.SecuritySchemeProps{
				Type:        "apiKey",
				Name:        "authorization",
				In:          "header",
				Description: "Bearer Token authentication",
			},
		}
		securitySchemesV3["BearerToken"] = &spec3.SecurityScheme{
			SecuritySchemeProps: spec3.SecuritySchemeProps{
				Type:        "apiKey",
				Name:        "authorization",
				In:          "header",
				Description: "Bearer Token authentication",
			},
		}
	}

	if len(authenticators) == 0 {
		if config.Anonymous.Enabled {
			return anonymous.NewAuthenticator(config.Anonymous.Conditions), nil, &securityDefinitionsV2, securitySchemesV3, nil
		}
		return nil, nil, &securityDefinitionsV2, securitySchemesV3, nil
	}

	authenticator := union.New(authenticators...)

	authenticator = group.NewAuthenticatedGroupAdder(authenticator)

	if config.Anonymous.Enabled {
		// 认证链报错时直接返回错误，避免把非法 token 或错误凭据当作匿名请求放行。
		authenticator = union.NewFailOnError(authenticator, anonymous.NewAuthenticator(config.Anonymous.Conditions))
	}

	return authenticator, updateAuthenticationConfig, &securityDefinitionsV2, securitySchemesV3, nil
}

// jwtAuthenticatorWithCancel 包装 JWT 认证器及其生命周期取消函数，便于配置热更新时替换。
type jwtAuthenticatorWithCancel struct {
	jwtAuthenticator authenticator.Token
	healthCheck      func() error
	cancel           func()
}

// newJWTAuthenticator 根据认证配置构造 JWT 认证器及健康检查与取消函数。
func newJWTAuthenticator(serverLifecycle context.Context, config *apiserver.AuthenticationConfiguration, oidcSigningAlgs []string, apiAudiences authenticator.Audiences, disallowedIssuers []string, egressLookup egressselector.Lookup, apiServerID string) (_ *jwtAuthenticatorWithCancel, buildErr error) {
	ctx, cancel := context.WithCancel(serverLifecycle)

	// 构造失败时立即取消派生的 context，避免泄漏 OIDC 后台 goroutine。
	defer func() {
		if buildErr != nil {
			cancel()
		}
	}()
	var jwtAuthenticators []authenticator.Token
	var healthChecks []func() error
	for _, jwtAuthenticator := range config.JWT {
		// TODO: 移除 CAContentProvider 这一间接层。
		var oidcCAContent oidc.CAContentProvider
		if len(jwtAuthenticator.Issuer.CertificateAuthority) > 0 {
			var oidcCAError error
			oidcCAContent, oidcCAError = dynamiccertificates.NewStaticCAContent("oidc-authenticator", []byte(jwtAuthenticator.Issuer.CertificateAuthority))
			if oidcCAError != nil {
				return nil, oidcCAError
			}
		}
		oidcAuth, err := oidc.New(ctx, oidc.Options{
			JWTAuthenticator:     jwtAuthenticator,
			CAContentProvider:    oidcCAContent,
			EgressLookup:         egressLookup,
			SupportedSigningAlgs: oidcSigningAlgs,
			DisallowedIssuers:    disallowedIssuers,
			APIServerID:          apiServerID,
		})
		if err != nil {
			return nil, err
		}
		jwtAuthenticators = append(jwtAuthenticators, oidcAuth)
		healthChecks = append(healthChecks, oidcAuth.HealthCheck)
	}
	return &jwtAuthenticatorWithCancel{
		// tokenunion.NewFailOnError 能正确处理空切片，避免无 JWT 配置时误放行。
		jwtAuthenticator: authenticator.WrapAudienceAgnosticToken(apiAudiences, tokenunion.NewFailOnError(jwtAuthenticators...)),
		healthCheck: func() error {
			var errs []error
			for _, check := range healthChecks {
				if err := check(); err != nil {
					errs = append(errs, err)
				}
			}
			return utilerrors.NewAggregate(errs)
		},
		cancel: cancel,
	}, nil
}

// authenticationConfigUpdater 负责在运行时热更新 OIDC/JWT 认证配置。
type authenticationConfigUpdater struct {
	serverLifecycle     context.Context
	config              Config
	jwtAuthenticatorPtr *atomic.Pointer[jwtAuthenticatorWithCancel]
	issuers             sets.Set[string]
}

// updateAuthenticationConfig 热更新 JWT 认证器配置。
// 入参 ctx 仅控制本次更新的超时时间，不决定新建认证器的生命周期。
func (c *authenticationConfigUpdater) updateAuthenticationConfig(ctx context.Context, authConfig *apiserver.AuthenticationConfiguration) error {
	updatedJWTAuthenticator, err := newJWTAuthenticator(c.serverLifecycle, authConfig, c.config.OIDCSigningAlgs, c.config.APIAudiences, c.config.ServiceAccountIssuers, c.config.EgressLookup, c.config.APIServerID)
	if err != nil {
		return err
	}

	// 新认证器上线前必须先通过健康检查，防止切换到无法工作的 OIDC provider。
	var lastErr error
	if waitErr := wait.PollUntilContextCancel(ctx, 10*time.Second, true, func(_ context.Context) (done bool, err error) {
		lastErr = updatedJWTAuthenticator.healthCheck()
		return lastErr == nil, nil
	}); lastErr != nil || waitErr != nil {
		updatedJWTAuthenticator.cancel()
		return utilerrors.NewAggregate([]error{lastErr, waitErr}) // 过滤 nil 错误
	}

	newIssuers := sets.New[string]()
	for _, jwt := range authConfig.JWT {
		newIssuers.Insert(jwt.Issuer.URL)
	}

	// 记录被移除的 issuer，稍后清理其指标。
	removedIssuers := c.issuers.Difference(newIssuers)

	oldJWTAuthenticator := c.jwtAuthenticatorPtr.Swap(updatedJWTAuthenticator)
	c.issuers = newIssuers

	// 延迟取消旧认证器：给仍在进行的请求留出完成时间，再清理过期指标。
	go func() {
		t := time.NewTimer(time.Minute)
		defer t.Stop()
		select {
		case <-c.serverLifecycle.Done():
		case <-t.C:
		}
		// TODO: 可跟踪在途请求，以确定何时可以安全取消旧认证器。
		oldJWTAuthenticator.cancel()

		// shouldRecordFunc 未加锁，额外等待让取消前已检查过它的在途请求
		// 有时间完成指标记录。
		time.Sleep(30 * time.Second)

		// 删除已移除 issuer 的指标。
		// 注意：若配置在宽限期加休眠期内快速回滚（issuer 被移除后又重新加入），
		// 可能误删仍在使用的指标。这属于配置抖动时的边缘情况，
		// 下次 JWKS 拉取会重建指标，配置稳定后系统即恢复一致。
		for _, issuer := range removedIssuers.UnsortedList() {
			oidc.DeleteJWKSFetchMetrics(issuer, c.config.APIServerID)
		}
	}()

	return nil
}

// newAuthenticatorFromTokenFile 从 CSV token 文件构造 token 认证器。
func newAuthenticatorFromTokenFile(tokenAuthFile string) (authenticator.Token, error) {
	tokenAuthenticator, err := tokenfile.NewCSV(tokenAuthFile)
	if err != nil {
		return nil, err
	}

	return tokenAuthenticator, nil
}

// newLegacyServiceAccountAuthenticator 构造兼容旧版 ServiceAccount token 的认证器。
func newLegacyServiceAccountAuthenticator(publicKeysGetter serviceaccount.PublicKeysGetter, lookup bool, apiAudiences authenticator.Audiences, serviceAccountGetter serviceaccount.ServiceAccountTokenGetter, secretsWriter typedv1core.SecretsGetter) (authenticator.Token, error) {
	if publicKeysGetter == nil {
		return nil, fmt.Errorf("no public key getter provided")
	}
	validator, err := serviceaccount.NewLegacyValidator(lookup, serviceAccountGetter, secretsWriter)
	if err != nil {
		return nil, fmt.Errorf("while creating legacy validator, err: %w", err)
	}

	tokenAuthenticator := serviceaccount.JWTTokenAuthenticator([]string{serviceaccount.LegacyIssuer}, publicKeysGetter, apiAudiences, validator)
	return tokenAuthenticator, nil
}

// newServiceAccountAuthenticator 构造新版 ServiceAccount token 认证器。
func newServiceAccountAuthenticator(issuers []string, publicKeysGetter serviceaccount.PublicKeysGetter, apiAudiences authenticator.Audiences, serviceAccountGetter serviceaccount.ServiceAccountTokenGetter) (authenticator.Token, error) {
	if publicKeysGetter == nil {
		return nil, fmt.Errorf("no public key getter provided")
	}
	tokenAuthenticator := serviceaccount.JWTTokenAuthenticator(issuers, publicKeysGetter, apiAudiences, serviceaccount.NewValidator(serviceAccountGetter))
	return tokenAuthenticator, nil
}

// newWebhookTokenAuthenticator 构造带缓存的远程 webhook token 认证器。
func newWebhookTokenAuthenticator(config Config) (authenticator.Token, error) {
	if config.WebhookRetryBackoff == nil {
		return nil, errors.New("retry backoff parameters for authentication webhook has not been specified")
	}

	clientConfig, err := webhookutil.LoadKubeconfig(config.WebhookTokenAuthnConfigFile, config.CustomDial)
	if err != nil {
		return nil, err
	}
	webhookTokenAuthenticator, err := webhook.New(clientConfig, config.WebhookTokenAuthnVersion, config.APIAudiences, *config.WebhookRetryBackoff)
	if err != nil {
		return nil, err
	}

	return tokencache.New(webhookTokenAuthenticator, false, config.WebhookTokenAuthnCacheTTL, config.WebhookTokenAuthnCacheTTL), nil
}
