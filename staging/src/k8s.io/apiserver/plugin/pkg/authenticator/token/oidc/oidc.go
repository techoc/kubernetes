/*
Copyright 2015 The Kubernetes Authors.

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

/*
oidc 使用 OpenID Connect 协议实现 authenticator.Token 接口。

	config := oidc.Options{
		IssuerURL:     "https://accounts.google.com",
		ClientID:      os.Getenv("GOOGLE_CLIENT_ID"),
		UsernameClaim: "email",
	}
	tokenAuthenticator, err := oidc.New(config)

认证器先用未经验证的 iss 声明做 issuer 匹配，再完整校验签名与
aud；issuer 匹配失败属于"不是我的 token"，返回 (nil, false, nil) 而非错误，
以便认证链继续尝试下一个认证器。
*/
package oidc

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coreos/go-oidc"
	celgo "github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	jose "gopkg.in/go-jose/go-jose.v2"

	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	apiservervalidation "k8s.io/apiserver/pkg/apis/apiserver/validation"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
	authenticationtokenjwt "k8s.io/apiserver/pkg/authentication/token/jwt"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/lazy"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/egressselector"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/klog/v2"
)

var (
	// synchronizeTokenIDVerifierForTest 设为 true 时会强制等待，直到 token ID verifier 准备就绪。
	// 异步初始化使生产环境启动不被外部 IdP 阻塞，但测试需要确定性，
	// 因此用该开关等待就绪。
	synchronizeTokenIDVerifierForTest = false
)

const (
	wellKnownEndpointPath = "/.well-known/openid-configuration"
)

// Options 是构造 OIDC 认证器所需的全部输入。
// KeySet 与 DiscoveryURL 互斥、Client 与 CAContentProvider/
// EgressLookup 互斥，这些约束在 New 中显式校验，避免产生歧义配置。
type Options struct {
	// JWTAuthenticator is the authenticator that will be used to verify the JWT.
	JWTAuthenticator apiserver.JWTAuthenticator

	// Optional KeySet to allow for synchronous initialization instead of fetching from the remote issuer.
	// Mutually exclusive with JWTAuthenticator.Issuer.DiscoveryURL.
	//
	// The following API server metrics for fetching JWKS and provider status will not be recorded if this is set.
	//  - apiserver_authentication_jwt_authenticator_jwks_fetch_last_timestamp_seconds
	//  - apiserver_authentication_jwt_authenticator_jwks_fetch_last_key_set_info
	KeySet oidc.KeySet

	// PEM encoded root certificate contents of the provider.  Mutually exclusive with Client.
	CAContentProvider CAContentProvider

	// EgressLookup allows for optional opt-in egress configuration via a custom dialer.  Mutually exclusive with Client.
	EgressLookup egressselector.Lookup

	// Optional http.Client used to make all requests to the remote issuer.  Mutually exclusive with CAContentProvider and EgressLookup.
	Client *http.Client

	// Optional CEL compiler used to compile the CEL expressions. This is useful to use a shared instance
	// of the compiler as these compilers holding a CEL environment are expensive to create. If not provided,
	// a default compiler will be created.
	// Note: the compiler construction depends on feature gates and the compatibility version to be initialized.
	Compiler authenticationcel.Compiler

	// SupportedSigningAlgs sets the accepted set of JOSE signing algorithms that
	// can be used by the provider to sign tokens.
	//
	// https://tools.ietf.org/html/rfc7518#section-3.1
	//
	// This value defaults to RS256, the value recommended by the OpenID Connect
	// spec:
	//
	// https://openid.net/specs/openid-connect-core-1_0.html#IDTokenValidation
	SupportedSigningAlgs []string

	DisallowedIssuers []string

	// APIServerID is the ID of the API server
	// This is used in metrics to identify the API server
	APIServerID string

	// now 用于测试，默认取 time.Now。注入时钟可精确测试 token 过期边界，避免依赖真实时间。
	now func() time.Time
}

// CAContentProvider 是 dynamiccertificates.CAContentProvider 的子集，可用于动态加载根 CA。
// 这里只暴露读取接口，使认证器不依赖证书轮转实现。
type CAContentProvider interface {
	CurrentCABundleContent() []byte
}

// initVerifier 针对给定配置和 issuer URL 创建新的 ID token verifier，成功后用得到的 verifier 调用 setVerifier。
// oidc.NewProvider 会拉取 discovery 文档与 JWKS，属于网络操作，
// 因此调用方通常在后台轮询中执行。
func initVerifier(ctx context.Context, config *oidc.Config, iss string, audiences sets.Set[string]) (*idTokenVerifier, error) {
	provider, err := oidc.NewProvider(ctx, iss)
	if err != nil {
		return nil, fmt.Errorf("init verifier failed: %v", err)
	}
	return &idTokenVerifier{provider.Verifier(config), audiences}, nil
}

// asyncIDTokenVerifier 是一个允许异步初始化 issuer 校验的 ID token verifier，
// 内部包装了 sync.Mutex，因此必须按引用传递。
// 分布式声明的 issuer 在首次遇到时才初始化，避免为每个潜在 issuer
// 预先建立网络连接。
type asyncIDTokenVerifier struct {
	m sync.Mutex

	// v is the ID token verifier initialized asynchronously.  It remains nil
	// up until it is eventually initialized.
	// Guarded by m
	v *idTokenVerifier
}

// newAsyncIDTokenVerifier 创建一个新的异步 token verifier：verifier 立即可用，
// 但创建之后可能在一段时间内仍未完成初始化。
// 初始化失败会无限重试（每 10 秒），因为 IdP 可能暂时不可达；
// ctx 取消后轮询终止。
func newAsyncIDTokenVerifier(ctx context.Context, c *oidc.Config, iss string, audiences sets.Set[string]) *asyncIDTokenVerifier {
	t := &asyncIDTokenVerifier{}

	sync := make(chan struct{})
	// Polls indefinitely in an attempt to initialize the distributed claims
	// verifier, or until context canceled.
	initFn := func(ctx context.Context) (done bool, err error) {
		klog.V(4).Infof("oidc authenticator: attempting init: iss=%v", iss)
		v, err := initVerifier(ctx, c, iss, audiences)
		if err != nil {
			klog.Errorf("oidc authenticator: async token verifier for issuer: %q: %v", iss, err)
			return false, nil
		}
		t.m.Lock()
		defer t.m.Unlock()
		t.v = v
		close(sync)
		return true, nil
	}

	go func() {
		_ = wait.PollUntilContextCancel(ctx, 10*time.Second, true, initFn)
	}()

	if synchronizeTokenIDVerifierForTest {
		select {
		case <-sync:
		case <-ctx.Done():
		}
	}

	return t
}

// verifier 返回底层的 ID token verifier，若尚未初始化则返回 nil。
// 加锁读取是因为初始化 goroutine 会并发写入 t.v。
func (a *asyncIDTokenVerifier) verifier() *idTokenVerifier {
	a.m.Lock()
	defer a.m.Unlock()
	return a.v
}

// jwtAuthenticator 是针对单个 issuer 的 OIDC 认证器。
// verifier 用 atomic.Value 存储，读路径无锁；异步初始化期间
// 值为 nil，AuthenticateToken 需显式处理未就绪状态。
type jwtAuthenticator struct {
	jwtAuthenticator apiserver.JWTAuthenticator

	// Contains an *oidc.IDTokenVerifier. Do not access directly use the
	// idTokenVerifier method.
	verifier atomic.Value

	// resolver is used to resolve distributed claims.
	resolver *claimResolver

	// celMapper contains the compiled CEL expressions for
	// username, groups, uid, extra, claimMapping and claimValidation
	celMapper authenticationcel.CELMapper

	// requiredClaims contains the list of claims that must be present in the token.
	requiredClaims map[string]string

	healthCheck atomic.Pointer[errorHolder]
}

// idTokenVerifier 包装 oidc.IDTokenVerifier：用它校验原始 ID token，之后在本地执行 audience 校验。
// go-oidc 只支持单 audience，本地补上多 audience 的"任一匹配"逻辑。
type idTokenVerifier struct {
	verifier  *oidc.IDTokenVerifier
	audiences sets.Set[string]
}

// setVerifier 原子地发布 verifier 并清除健康检查错误。
// 先存 verifier 再存健康状态，保证 HealthCheck 返回 nil 时认证
// 一定可用，避免"健康但未就绪"的窗口。
func (a *jwtAuthenticator) setVerifier(v *idTokenVerifier) {
	a.verifier.Store(v)
	if v != nil {
		// this must be done after the verifier has been stored so that a nil error
		// from HealthCheck always means that the authenticator is ready for use.
		a.healthCheck.Store(&errorHolder{})
	}
}

// idTokenVerifier 返回当前 verifier 及是否已初始化。
func (a *jwtAuthenticator) idTokenVerifier() (*idTokenVerifier, bool) {
	if v := a.verifier.Load(); v != nil {
		return v.(*idTokenVerifier), true
	}
	return nil, false
}

// AllValidSigningAlgorithms 返回允许配置的签名算法列表（排序后）。
func AllValidSigningAlgorithms() []string {
	return sets.List(sets.KeySet(allowedSigningAlgs))
}

// allowedSigningAlgs 是签名算法白名单，用于防止用户误传入不安全的算法。
// 只允许 RSA/ECDSA/PSS 系列，排除 none 与对称算法（HS*），
// 防止配置成无法安全验证的算法。
var allowedSigningAlgs = map[string]bool{
	oidc.RS256: true,
	oidc.RS384: true,
	oidc.RS512: true,
	oidc.ES256: true,
	oidc.ES384: true,
	oidc.ES512: true,
	oidc.PS256: true,
	oidc.PS384: true,
	oidc.PS512: true,
}

// AuthenticatorTokenWithHealthCheck 组合 token 认证与健康检查，供聚合认证器
// 在启动期判断 OIDC 是否就绪。
type AuthenticatorTokenWithHealthCheck interface {
	authenticator.Token
	HealthCheck() error
}

// New 返回一个认证器；当 opts.KeySet 未设置时，该认证器采用异步初始化。
// 传入的 lifecycleCtx 用于：
// - 终止异步初始化所需的后台 goroutine
// - 作为所发起请求（例如拉取密钥）的父上下文
// 因此 lifecycleCtx 一旦被取消，就不应再使用该认证器。
// 调用方可通过 HealthCheck 方法判断认证器是否健康。
// 提供 KeySet 时同步初始化，适合离线或已知公钥的场景；否则后台
// 轮询 discovery，使 apiserver 启动不依赖外部 IdP 可用性。
func New(lifecycleCtx context.Context, opts Options) (AuthenticatorTokenWithHealthCheck, error) {
	compiler := opts.Compiler
	if compiler == nil {
		compiler = authenticationcel.NewDefaultCompiler()
	}
	celMapper, fieldErr := apiservervalidation.CompileAndValidateJWTAuthenticator(compiler, opts.JWTAuthenticator, opts.DisallowedIssuers)
	if err := fieldErr.ToAggregate(); err != nil {
		return nil, err
	}

	RegisterMetrics()
	supportedSigningAlgs := opts.SupportedSigningAlgs
	if len(supportedSigningAlgs) == 0 {
		// RS256 is the default recommended by OpenID Connect and an 'alg' value
		// providers are required to implement.
		supportedSigningAlgs = []string{oidc.RS256}
	}
	for _, alg := range supportedSigningAlgs {
		if !allowedSigningAlgs[alg] {
			return nil, fmt.Errorf("oidc: unsupported signing alg: %q", alg)
		}
	}

	if opts.Client != nil && opts.CAContentProvider != nil {
		return nil, fmt.Errorf("oidc: Client and CAContentProvider are mutually exclusive")
	}

	if opts.Client != nil && opts.EgressLookup != nil {
		return nil, fmt.Errorf("oidc: Client and EgressLookup are mutually exclusive")
	}

	client := opts.Client

	if client == nil {
		var roots *x509.CertPool
		var err error
		if opts.CAContentProvider != nil {
			// TODO(enj): make this reload CA data dynamically
			roots, err = certutil.NewPoolFromBytes(opts.CAContentProvider.CurrentCABundleContent())
			if err != nil {
				return nil, fmt.Errorf("Failed to read the CA contents: %v", err)
			}
		} else {
			klog.Info("OIDC: No x509 certificates provided, will use host's root CA set")
		}

		var customDial net.DialFunc
		switch et := opts.JWTAuthenticator.Issuer.EgressSelectorType; et {
		case "":
			// valid but nothing to do
		case apiserver.EgressSelectorControlPlane:
			customDial, err = egressLookupForType(opts.EgressLookup, egressselector.ControlPlane)
		case apiserver.EgressSelectorCluster:
			customDial, err = egressLookupForType(opts.EgressLookup, egressselector.Cluster)
		default:
			// this should be impossible as validation should catch this at an earlier point
			return nil, fmt.Errorf("oidc: unknown egress selector type %q", et)
		}
		if err != nil {
			return nil, err
		}

		// Copied from http.DefaultTransport.
		tr := net.SetTransportDefaults(&http.Transport{
			DialContext: customDial,
			// According to golang's doc, if RootCAs is nil,
			// TLS uses the host's root CA set.
			TLSClientConfig: &tls.Config{RootCAs: roots},
		})

		client = &http.Client{Transport: tr, Timeout: 30 * time.Second}
	}

	// If the discovery URL is set in authentication configuration, we set up a
	// roundTripper to rewrite the {url}/.well-known/openid-configuration to
	// the discovery URL. This is useful for self-hosted providers, for example,
	// providers that run on top of Kubernetes itself.
	if len(opts.JWTAuthenticator.Issuer.DiscoveryURL) > 0 {
		if opts.KeySet != nil {
			return nil, fmt.Errorf("oidc: KeySet and DiscoveryURL are mutually exclusive")
		}

		discoveryURL, err := url.Parse(opts.JWTAuthenticator.Issuer.DiscoveryURL)
		if err != nil {
			return nil, fmt.Errorf("oidc: invalid discovery URL: %w", err)
		}

		clientWithDiscoveryURL := *client
		baseTransport := clientWithDiscoveryURL.Transport
		if baseTransport == nil {
			baseTransport = http.DefaultTransport
		}
		// This matches the url construction in oidc.NewProvider as of go-oidc v2.2.1.
		// xref: https://github.com/coreos/go-oidc/blob/40cd342c4a2076195294612a834d11df23c1b25a/oidc.go#L114
		urlToRewrite := strings.TrimSuffix(opts.JWTAuthenticator.Issuer.URL, "/") + wellKnownEndpointPath
		clientWithDiscoveryURL.Transport = &discoveryURLRoundTripper{baseTransport, discoveryURL, urlToRewrite}
		client = &clientWithDiscoveryURL
	}

	lifecycleCtx = oidc.ClientContext(lifecycleCtx, client)

	now := opts.now
	if now == nil {
		now = time.Now
	}

	audiences := sets.New[string](opts.JWTAuthenticator.Issuer.Audiences...)
	verifierConfig := &oidc.Config{
		ClientID:             opts.JWTAuthenticator.Issuer.Audiences[0],
		SupportedSigningAlgs: supportedSigningAlgs,
		Now:                  now,
	}
	if audiences.Len() > 1 {
		verifierConfig.ClientID = ""
		// SkipClientIDCheck is set to true because we want to support multiple audiences
		// in the authentication configuration.
		// The go oidc library does not support validating
		// multiple audiences, so we have to skip the client ID check and do it ourselves.
		// xref: https://github.com/coreos/go-oidc/issues/397
		verifierConfig.SkipClientIDCheck = true
	}

	var resolver *claimResolver
	groupsClaim := opts.JWTAuthenticator.ClaimMappings.Groups.Claim
	if groupsClaim != "" {
		resolver = newClaimResolver(lifecycleCtx, groupsClaim, client, verifierConfig, audiences)
	}

	requiredClaims := make(map[string]string)
	for _, claimValidationRule := range opts.JWTAuthenticator.ClaimValidationRules {
		if len(claimValidationRule.Claim) > 0 {
			requiredClaims[claimValidationRule.Claim] = claimValidationRule.RequiredValue
		}
	}

	authn := &jwtAuthenticator{
		jwtAuthenticator: opts.JWTAuthenticator,
		resolver:         resolver,
		celMapper:        celMapper,
		requiredClaims:   requiredClaims,
	}
	authn.healthCheck.Store(&errorHolder{
		err: fmt.Errorf("oidc: authenticator for issuer %q is not initialized", authn.jwtAuthenticator.Issuer.URL),
	})

	issuerURL := opts.JWTAuthenticator.Issuer.URL
	if opts.KeySet != nil {
		// We already have a key set, synchronously initialize the verifier.
		authn.setVerifier(&idTokenVerifier{
			oidc.NewVerifier(issuerURL, opts.KeySet, verifierConfig),
			audiences,
		})
	} else {
		// Asynchronously attempt to initialize the authenticator. This enables
		// self-hosted providers, providers that run on top of Kubernetes itself.
		go func() {
			// we ignore any errors from polling because they can only come from the context being canceled
			_ = wait.PollUntilContextCancel(lifecycleCtx, 10*time.Second, true, func(_ context.Context) (done bool, err error) {
				// this must always use lifecycleCtx because NewProvider uses that context for future key set fetching.
				// this also means that there is no correct way to control the timeout of the discovery request made by NewProvider.
				// the global timeout of the http.Client is still honored.
				provider, err := oidc.NewProvider(lifecycleCtx, issuerURL)
				if err != nil {
					klog.Errorf("oidc authenticator: initializing plugin: %v", err)
					authn.healthCheck.Store(&errorHolder{err: err})
					return false, nil
				}

				if utilfeature.DefaultFeatureGate.Enabled(features.StructuredAuthenticationConfigurationJWKSMetrics) {
					providerJSON := &struct {
						JWKSURL string `json:"jwks_uri"`
					}{}

					if err := provider.Claims(providerJSON); err != nil {
						klog.Errorf("oidc authenticator: error getting JWKS URL: %v", err)
						authn.healthCheck.Store(&errorHolder{err: err})
						return false, nil
					}
					if len(providerJSON.JWKSURL) == 0 {
						klog.Errorf("provider did not return JWKS URL")
						authn.healthCheck.Store(&errorHolder{err: fmt.Errorf("provider did not return JWKS URL")})
						return false, nil
					}

					clientWithJWKSMetrics := *client
					clientWithJWKSMetrics.Transport = withMetricsRoundTripper(client.Transport, providerJSON.JWKSURL, issuerURL, opts.APIServerID, lifecycleCtx)
					client = &clientWithJWKSMetrics

					remoteKeySet := oidc.NewRemoteKeySet(oidc.ClientContext(lifecycleCtx, client), providerJSON.JWKSURL)
					authn.setVerifier(&idTokenVerifier{oidc.NewVerifier(issuerURL, remoteKeySet, verifierConfig), audiences})
					return true, nil
				}

				verifier := provider.Verifier(verifierConfig)
				authn.setVerifier(&idTokenVerifier{verifier, audiences})
				return true, nil
			})
		}()
	}

	return newInstrumentedAuthenticator(issuerURL, authn), nil
}

// egressLookupForType 解析指定 egress 类型的拨号器。
//
// 与其它 egress 使用方不同，这里要求必须显式配置拨号器，因为
// JWT 认证器的 egress 选择是可选特性，静默回退会掩盖配置错误。
func egressLookupForType(egressLookup egressselector.Lookup, egressSelector egressselector.EgressType) (net.DialFunc, error) {
	if egressLookup == nil {
		return nil, fmt.Errorf("oidc: egress lookup required with egress selector type %q", egressSelector)
	}
	customDial, err := egressLookup(egressSelector.AsNetworkContext())
	if err != nil {
		return nil, fmt.Errorf("oidc: egress lookup for %q failed: %w", egressSelector, err)
	}
	// we are stricter than other egress lookups because this is opt-in config
	// we expect the user who is configuring the JWT authenticator to keep it in sync with the egress configuration
	if customDial == nil {
		return nil, fmt.Errorf("oidc: egress lookup for %q is not configured", egressSelector)
	}
	return customDial, nil
}

// errorHolder 包装 error，以便存入 atomic.Pointer。
type errorHolder struct {
	err error
}

// metricsRoundTripper 是一个记录 JWKS 拉取指标的 http.RoundTripper。
// 它在传输层拦截 JWKS 响应，既记录成功/失败时间戳，也能校验
// key set 内容，避免把无效响应误记为成功。
type metricsRoundTripper struct {
	base    http.RoundTripper
	jwksURL string

	jwtIssuer        string
	apiServerID      string
	shouldRecordFunc func() bool
}

// shouldRecordMetrics returns true if metrics should be recorded.
// This is used to avoid recording metrics after the lifecycle context is done.
func (m *metricsRoundTripper) shouldRecordMetrics() bool {
	return m.shouldRecordFunc()
}

// RoundTrip 仅对 JWKS URL 的请求记录指标，其余请求直接透传。
func (m *metricsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.String() != m.jwksURL || !m.shouldRecordMetrics() {
		return m.base.RoundTrip(req)
	}

	resp, err := m.base.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusOK || resp.Body == nil {
		if m.shouldRecordMetrics() {
			recordJWKSFetchKeySetFailure(m.jwtIssuer, m.apiServerID)
		}
		return resp, err
	}

	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if m.shouldRecordMetrics() {
			recordJWKSFetchKeySetFailure(m.jwtIssuer, m.apiServerID)
		}
		return resp, fmt.Errorf("error reading JWKS response: %w", err)
	}

	// This check has been copied from https://github.com/coreos/go-oidc/blob/0fe98873951208147e6d412602432038c91cda54/oidc/jwks.go#L263-L267
	// to validate the key set before setting the metrics to avoid recording invalid key sets.
	var keySet jose.JSONWebKeySet
	if m.shouldRecordMetrics() {
		if unmarshalErr := unmarshalResp(resp, body, &keySet); unmarshalErr != nil {
			recordJWKSFetchKeySetFailure(m.jwtIssuer, m.apiServerID)
		} else {
			recordJWKSFetchKeySetSuccess(m.jwtIssuer, m.apiServerID, string(body))
		}
	}

	newResp := &http.Response{}
	*newResp = *resp
	newResp.Body = io.NopCloser(bytes.NewBuffer(body))

	return newResp, nil
}

// 本函数复制自 https://github.com/coreos/go-oidc/blob/0fe98873951208147e6d412602432038c91cda54/oidc/oidc.go#L543-L554，
// 在写入指标之前先校验 key set，避免把无效的 key set 记录成成功。
// 错误信息会区分"Content-Type 正确但 JSON 无效"与"Content-Type
// 不符"，便于定位 IdP 行为异常。
func unmarshalResp(r *http.Response, body []byte, v interface{}) error {
	err := json.Unmarshal(body, &v)
	if err == nil {
		return nil
	}
	ct := r.Header.Get("Content-Type")
	mediaType, _, parseErr := mime.ParseMediaType(ct)
	if parseErr == nil && mediaType == "application/json" {
		return fmt.Errorf("got Content-Type = application/json, but could not unmarshal as JSON: %w", err)
	}
	return fmt.Errorf("expected Content-Type = application/json, got %q: %w", ct, err)
}

// withMetricsRoundTripper 返回一个新的、用于记录 JWKS 拉取指标的 http.RoundTripper。
// 用生命周期上下文判断是否记录，进程关闭后不再产生指标，
// 避免与指标注册表清理竞态。
func withMetricsRoundTripper(base http.RoundTripper, jwksURL, jwtIssuer, apiServerID string, lifecycleCtx context.Context) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &metricsRoundTripper{
		base:        base,
		jwksURL:     jwksURL,
		jwtIssuer:   jwtIssuer,
		apiServerID: apiServerID,
		shouldRecordFunc: func() bool {
			select {
			case <-lifecycleCtx.Done():
				return false
			default:
				return true
			}
		},
	}
}

// discoveryURLRoundTripper 是一个 http.RoundTripper，把 {url}/.well-known/openid-configuration
// 改写为 discovery URL。
// 它用于 issuer URL 与 discovery 地址不同的自托管 IdP；只改写精确
// 匹配的 GET 请求，其余流量原样透传。
type discoveryURLRoundTripper struct {
	base http.RoundTripper
	// discoveryURL is the URL to use to fetch the openid configuration
	discoveryURL *url.URL
	// urlToRewrite is the URL to rewrite to the discovery URL
	urlToRewrite string
}

// RoundTrip 在命中 discovery 路径时改写请求 URL。
//
// 克隆请求并清空 Host，避免旧 Host 头与新 URL 不匹配。
func (t *discoveryURLRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet && req.URL.String() == t.urlToRewrite {
		clone := req.Clone(req.Context())
		clone.Host = ""
		clone.URL = t.discoveryURL
		return t.base.RoundTrip(clone)
	}
	return t.base.RoundTrip(req)
}

// untrustedIssuer 从给定 JWT 中取出一个不可信的 "iss" 声明；token 无法解析时返回错误。
// 由于此时 JWT 尚未完成校验，返回的 issuer 不可信任。
// 它只用于选择认证器与分布式声明解析；任何安全决策都必须基于
// 已验证的 token。
func untrustedIssuer(token string) (string, error) {
	if strings.HasPrefix(strings.TrimSpace(token), "{") {
		return "", fmt.Errorf("token is not compact JWT")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("malformed token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("error decoding token: %v", err)
	}
	claims := struct {
		// WARNING: this JWT is not verified. Do not trust these claims.
		Issuer string `json:"iss"`
	}{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", fmt.Errorf("while unmarshaling token: %v", err)
	}
	// Coalesce the legacy GoogleIss with the new one.
	//
	// http://openid.net/specs/openid-connect-core-1_0.html#GoogleIss
	if claims.Issuer == "accounts.google.com" {
		return "https://accounts.google.com", nil
	}
	return claims.Issuer, nil
}

// hasCorrectIssuer 判断 token 的未验证 iss 是否与配置一致。
func hasCorrectIssuer(iss, tokenData string) bool {
	uiss, err := untrustedIssuer(tokenData)
	if err != nil {
		return false
	}
	if uiss != iss {
		return false
	}
	return true
}

// endpoint 表示 OIDC 分布式声明的取值端点。
// JWT 字段对应聚合声明，当前不支持，保留以便解析不报错。
type endpoint struct {
	// URL to use to request the distributed claim.  This URL is expected to be
	// prefixed by one of the known issuer URLs.
	URL string `json:"endpoint,omitempty"`
	// AccessToken is the bearer token to use for access.  If empty, it is
	// not used.  Access token is optional per the OIDC distributed claims
	// specification.
	// See: http://openid.net/specs/openid-connect-core-1_0.html#DistributedExample
	AccessToken string `json:"access_token,omitempty"`
	// JWT is the container for aggregated claims.  Not supported at the moment.
	// See: http://openid.net/specs/openid-connect-core-1_0.html#AggregatedExample
	JWT string `json:"JWT,omitempty"`
}

// claimResolver 通过调用相应的声明来源端点来展开分布式声明。
// 每个 issuer 的 verifier 惰性创建并缓存；m 同时保护 map 与
// 惰性初始化，避免并发创建重复连接。
type claimResolver struct {
	ctx context.Context

	// claim is the distributed claim that may be resolved.
	claim string

	// audiences is the set of acceptable audiences the JWT must be issued to.
	// At least one of the entries must match the "aud" claim in presented JWTs.
	audiences sets.Set[string]

	// client is the to use for resolving distributed claims
	client *http.Client

	// config is the OIDC configuration used for resolving distributed claims.
	config *oidc.Config

	// verifierPerIssuer contains, for each issuer, the appropriate verifier to use
	// for this claim.  It is assumed that there will be very few entries in
	// this map.
	// Guarded by m.
	verifierPerIssuer map[string]*asyncIDTokenVerifier

	m sync.Mutex
}

// newClaimResolver creates a new resolver for distributed claims.
// the input ctx is retained and is used as the base context for background requests such as key fetching.
func newClaimResolver(ctx context.Context, claim string, client *http.Client, config *oidc.Config, audiences sets.Set[string]) *claimResolver {
	return &claimResolver{
		ctx:               ctx,
		claim:             claim,
		audiences:         audiences,
		client:            client,
		config:            config,
		verifierPerIssuer: map[string]*asyncIDTokenVerifier{},
	}
}

// Verifier 返回指定 issuer 对应的 verifier，否则返回错误。
// 首次遇到某 issuer 时才创建异步 verifier；创建后立即返回，
// 若尚未就绪则返回错误，由调用方决定重试或失败。
func (r *claimResolver) Verifier(iss string) (*idTokenVerifier, error) {
	r.m.Lock()
	av := r.verifierPerIssuer[iss]
	if av == nil {
		// This lazy init should normally be very quick.
		ctx := oidc.ClientContext(r.ctx, r.client)
		av = newAsyncIDTokenVerifier(ctx, r.config, iss, r.audiences)
		r.verifierPerIssuer[iss] = av
	}
	r.m.Unlock()

	v := av.verifier()
	if v == nil {
		return nil, fmt.Errorf("verifier not initialized for issuer: %q", iss)
	}
	return v, nil
}

// expand 从 claim names 与 claim sources 中提取分布式声明，
// 提取到的声明值会被回填进传入的 claims。
//
// 分布式声明的形式如下所示，定义见 OIDC Connect Core 1.0 第 5.6.2 节。
// See: https://openid.net/specs/openid-connect-core-1_0.html#AggregatedDistributedClaims
//
//	{
//	  ... (other normal claims)...
//	  "_claim_names": {
//	    "groups": "src1"
//	  },
//	  "_claim_sources": {
//	    "src1": {
//	      "endpoint": "https://www.example.com",
//	      "access_token": "f005ba11"
//	    },
//	  },
//	}
//
// 只有普通 claim 缺失且存在 _claim_names 时才解析分布式声明；
// 若已存在同名普通 claim 则直接跳过，避免远端覆盖本地值。
func (r *claimResolver) expand(ctx context.Context, c claims) error {
	const (
		// The claim containing a map of endpoint references per claim.
		// OIDC Connect Core 1.0, section 5.6.2.
		claimNamesKey = "_claim_names"
		// The claim containing endpoint specifications.
		// OIDC Connect Core 1.0, section 5.6.2.
		claimSourcesKey = "_claim_sources"
	)

	_, ok := c[r.claim]
	if ok {
		// There already is a normal claim, skip resolving.
		return nil
	}
	names, ok := c[claimNamesKey]
	if !ok {
		// No _claim_names, no keys to look up.
		return nil
	}

	claimToSource := map[string]string{}
	if err := json.Unmarshal([]byte(names), &claimToSource); err != nil {
		return fmt.Errorf("oidc: error parsing distributed claim names: %v", err)
	}

	rawSources, ok := c[claimSourcesKey]
	if !ok {
		// Having _claim_names claim,  but no _claim_sources is not an expected
		// state.
		return fmt.Errorf("oidc: no claim sources")
	}

	var sources map[string]endpoint
	if err := json.Unmarshal([]byte(rawSources), &sources); err != nil {
		// The claims sources claim is malformed, this is not an expected state.
		return fmt.Errorf("oidc: could not parse claim sources: %v", err)
	}

	src, ok := claimToSource[r.claim]
	if !ok {
		// No distributed claim present.
		return nil
	}
	ep, ok := sources[src]
	if !ok {
		return fmt.Errorf("id token _claim_names contained a source %s missing in _claims_sources", src)
	}
	if ep.URL == "" {
		// This is maybe an aggregated claim (ep.JWT != "").
		return nil
	}
	return r.resolve(ctx, ep, c)
}

// resolve 向传入的所有端点请求分布式声明，并把查询结果写入 allClaims。
// 从 endpoint 拉取的 JWT 必须再次完整校验（issuer 选择 + 签名 +
// audience），不能信任其中的声明，否则分布式声明会成为伪造身份的入口。
func (r *claimResolver) resolve(ctx context.Context, endpoint endpoint, allClaims claims) error {
	// TODO: cache resolved claims.
	jwt, err := getClaimJWT(ctx, r.client, endpoint.URL, endpoint.AccessToken)
	if err != nil {
		return fmt.Errorf("while getting distributed claim %q: %v", r.claim, err)
	}
	untrustedIss, err := untrustedIssuer(jwt)
	if err != nil {
		return fmt.Errorf("getting untrusted issuer from endpoint %v failed for claim %q: %v", endpoint.URL, r.claim, err)
	}
	v, err := r.Verifier(untrustedIss)
	if err != nil {
		return fmt.Errorf("verifying untrusted issuer %v failed: %v", untrustedIss, err)
	}
	t, err := v.Verify(ctx, jwt)
	if err != nil {
		return fmt.Errorf("verify distributed claim token: %v", err)
	}
	var distClaims claims
	if err := t.Claims(&distClaims); err != nil {
		return fmt.Errorf("could not parse distributed claims for claim %v: %v", r.claim, err)
	}
	value, ok := distClaims[r.claim]
	if !ok {
		return fmt.Errorf("jwt returned by distributed claim endpoint %q did not contain claim: %v", endpoint.URL, r.claim)
	}
	allClaims[r.claim] = value
	return nil
}

// Verify 校验原始 ID token 并做本地 audience 校验。
func (v *idTokenVerifier) Verify(ctx context.Context, rawIDToken string) (*oidc.IDToken, error) {
	t, err := v.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return nil, err
	}
	if err := v.verifyAudience(t); err != nil {
		return nil, err
	}
	return t, nil
}

// verifyAudience 校验 ID token 中的 audience 字段是否符合预期 audience。
// 该实现参考 https://github.com/coreos/go-oidc/blob/b203e58c24394ddf5e816706a7645f01280245c7/oidc/verify.go#L275-L281，
// 区别在于我们允许多个 audience。
//
// AuthenticationConfiguration 有 audienceMatchPolicy 字段，但目前唯一支持的取值是 "MatchAny"。
// 因此默认的匹配行为是：只要命中 ID token 中 audience 的任意一个即算匹配。
// 空 audience 配置直接报错而非放行，防止配置遗漏导致任意 aud
// 都被接受。
func (v *idTokenVerifier) verifyAudience(t *oidc.IDToken) error {
	// We validate audience field is not empty in the authentication configuration.
	// This check ensures callers of "Verify" using idTokenVerifier are not passing
	// an empty audience.
	if v.audiences.Len() == 0 {
		return fmt.Errorf("oidc: invalid configuration, audiences cannot be empty")
	}
	if v.audiences.HasAny(t.Audience...) {
		return nil
	}

	return fmt.Errorf("oidc: expected audience in %q got %q", sets.List(v.audiences), t.Audience)
}

// AuthenticateToken 实现 authenticator.Token。
//
// 先用未验证的 iss 快速排除不属于本 issuer 的 token，避免对每个
// token 都做昂贵的签名校验；随后依次完成签名、声明解析、分布式声明展开、
// 身份映射、必需声明检查与 CEL 校验。
func (a *jwtAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	if !hasCorrectIssuer(a.jwtAuthenticator.Issuer.URL, token) {
		return nil, false, nil
	}

	verifier, ok := a.idTokenVerifier()
	if !ok {
		return nil, false, fmt.Errorf("oidc: authenticator not initialized")
	}

	idToken, err := verifier.Verify(ctx, token)
	if err != nil {
		return nil, false, fmt.Errorf("oidc: verify token: %v", err)
	}

	var c claims
	if err := idToken.Claims(&c); err != nil {
		return nil, false, fmt.Errorf("oidc: parse claims: %v", err)
	}
	if a.resolver != nil {
		if err := a.resolver.expand(ctx, c); err != nil {
			return nil, false, fmt.Errorf("oidc: could not expand distributed claims: %v", err)
		}
	}

	var claimsValue *lazy.MapValue
	// Convert the claims to traits.Mapper so that we can evaluate the CEL expressions
	// against the claims. This is done once here so that we don't have to convert
	// the claims to traits.Mapper multiple times in the CEL mapper for each mapping.
	// Only perform this conversion if any of the mapping or validation rules contain
	// CEL expressions.  The traits.Mapper is lazily evaluated against the expressions.
	if a.celMapper.Username != nil || a.celMapper.Groups != nil || a.celMapper.UID != nil || a.celMapper.Extra != nil || a.celMapper.ClaimValidationRules != nil {
		claimsValue = newClaimsValue(c)
	}

	var username string
	if username, err = a.getUsername(ctx, c, claimsValue); err != nil {
		return nil, false, err
	}

	info := &user.DefaultInfo{Name: username}
	if info.Groups, err = a.getGroups(ctx, c, claimsValue); err != nil {
		return nil, false, err
	}

	if info.UID, err = a.getUID(ctx, c, claimsValue); err != nil {
		return nil, false, err
	}

	extra, err := a.getExtra(ctx, c, claimsValue)
	if err != nil {
		return nil, false, err
	}
	if len(extra) > 0 {
		info.Extra = extra
	}

	// check to ensure all required claims are present in the ID token and have matching values.
	for claim, value := range a.requiredClaims {
		if !c.hasClaim(claim) {
			return nil, false, fmt.Errorf("oidc: required claim %s not present in ID token", claim)
		}

		// NOTE: Only string values are supported as valid required claim values.
		var claimValue string
		if err := c.unmarshalClaim(claim, &claimValue); err != nil {
			return nil, false, fmt.Errorf("oidc: parse claim %s: %w", claim, err)
		}
		if claimValue != value {
			return nil, false, fmt.Errorf("oidc: required claim %s value does not match. Got = %s, want = %s", claim, claimValue, value)
		}
	}

	if a.celMapper.ClaimValidationRules != nil {
		evalResult, err := a.celMapper.ClaimValidationRules.EvalClaimMappings(ctx, claimsValue)
		if err != nil {
			return nil, false, fmt.Errorf("oidc: error evaluating claim validation expression: %w", err)
		}
		if err := checkValidationRulesEvaluation(evalResult, func(a authenticationcel.ExpressionAccessor) (string, error) {
			claimValidationCondition, ok := a.(*authenticationcel.ClaimValidationCondition)
			if !ok {
				return "", fmt.Errorf("invalid type conversion, expected ClaimValidationCondition")
			}
			return claimValidationCondition.Message, nil
		}); err != nil {
			return nil, false, fmt.Errorf("oidc: error evaluating claim validation expression: %w", err)
		}
	}

	if a.celMapper.UserValidationRules != nil {
		// Convert the user info to traits.Mapper so that we can evaluate the CEL expressions
		// against the user info. This is done once here so that we don't have to convert
		// the user info to traits.Mapper multiple times in the CEL mapper for each mapping.
		// The traits.Mapper is lazily evaluated against the expressions.
		userInfoVal := newUserInfoValue(info)

		evalResult, err := a.celMapper.UserValidationRules.EvalUser(ctx, userInfoVal)
		if err != nil {
			return nil, false, fmt.Errorf("oidc: error evaluating user info validation rule: %w", err)
		}
		if err := checkValidationRulesEvaluation(evalResult, func(a authenticationcel.ExpressionAccessor) (string, error) {
			userValidationCondition, ok := a.(*authenticationcel.UserValidationCondition)
			if !ok {
				return "", fmt.Errorf("invalid type conversion, expected UserValidationCondition")
			}
			return userValidationCondition.Message, nil
		}); err != nil {
			return nil, false, fmt.Errorf("oidc: error evaluating user info validation rule: %w", err)
		}
	}

	return &authenticator.Response{User: info}, true, nil
}

// HealthCheck 返回认证器初始化/刷新过程中的最近一次错误。
func (a *jwtAuthenticator) HealthCheck() error {
	if holder := *a.healthCheck.Load(); holder.err != nil {
		return fmt.Errorf("oidc: authenticator for issuer %q is not healthy: %w", a.jwtAuthenticator.Issuer.URL, holder.err)
	}

	return nil
}

// getUsername 解析用户名：优先 CEL 映射，否则使用静态 claim 配置。
//
// 使用 email 作为用户名且 token 带 email_verified 时，必须为 true，
// 否则未验证邮箱可冒充他人。
func (a *jwtAuthenticator) getUsername(ctx context.Context, c claims, claimsValue *lazy.MapValue) (string, error) {
	if a.celMapper.Username != nil {
		evalResult, err := a.celMapper.Username.EvalClaimMapping(ctx, claimsValue)
		if err != nil {
			return "", fmt.Errorf("oidc: error evaluating username claim expression: %w", err)
		}
		if evalResult.EvalResult.Type() != celgo.StringType {
			return "", fmt.Errorf("oidc: error evaluating username claim expression: %w", fmt.Errorf("username claim expression must return a string"))
		}

		username := evalResult.EvalResult.Value().(string)

		if len(username) == 0 {
			return "", fmt.Errorf("oidc: empty username via CEL expression is not allowed")
		}

		return username, nil
	}

	var username string
	usernameClaim := a.jwtAuthenticator.ClaimMappings.Username.Claim
	if err := c.unmarshalClaim(usernameClaim, &username); err != nil {
		return "", fmt.Errorf("oidc: parse username claims %q: %v", usernameClaim, err)
	}

	if usernameClaim == "email" {
		// If the email_verified claim is present, ensure the email is valid.
		// https://openid.net/specs/openid-connect-core-1_0.html#StandardClaims
		if hasEmailVerified := c.hasClaim("email_verified"); hasEmailVerified {
			var emailVerified bool
			if err := c.unmarshalClaim("email_verified", &emailVerified); err != nil {
				return "", fmt.Errorf("oidc: parse 'email_verified' claim: %v", err)
			}

			// If the email_verified claim is present we have to verify it is set to `true`.
			if !emailVerified {
				return "", fmt.Errorf("oidc: email not verified")
			}
		}
	}

	userNamePrefix := a.jwtAuthenticator.ClaimMappings.Username.Prefix
	if userNamePrefix != nil && *userNamePrefix != "" {
		return *userNamePrefix + username, nil
	}
	return username, nil
}

// getGroups 解析用户组：静态 claim 优先，其次 CEL 映射。
//
// 允许 claim 是单个字符串而非数组，兼容把 "role" 当组名的常见配置。
func (a *jwtAuthenticator) getGroups(ctx context.Context, c claims, claimsValue *lazy.MapValue) ([]string, error) {
	groupsClaim := a.jwtAuthenticator.ClaimMappings.Groups.Claim
	if len(groupsClaim) > 0 {
		if _, ok := c[groupsClaim]; ok {
			// Some admins want to use string claims like "role" as the group value.
			// Allow the group claim to be a single string instead of an array.
			//
			// See: https://github.com/kubernetes/kubernetes/issues/33290
			var groups stringOrArray
			if err := c.unmarshalClaim(groupsClaim, &groups); err != nil {
				return nil, fmt.Errorf("oidc: parse groups claim %q: %w", groupsClaim, err)
			}

			prefix := a.jwtAuthenticator.ClaimMappings.Groups.Prefix
			if prefix != nil && *prefix != "" {
				for i, group := range groups {
					groups[i] = *prefix + group
				}
			}

			return []string(groups), nil
		}
	}

	if a.celMapper.Groups == nil {
		return nil, nil
	}

	evalResult, err := a.celMapper.Groups.EvalClaimMapping(ctx, claimsValue)
	if err != nil {
		return nil, fmt.Errorf("oidc: error evaluating group claim expression: %w", err)
	}

	groups, err := convertCELValueToStringList(evalResult.EvalResult)
	if err != nil {
		return nil, fmt.Errorf("oidc: error evaluating group claim expression: %w", err)
	}
	return groups, nil
}

// getUID 解析用户 UID；CEL 表达式必须返回字符串。
func (a *jwtAuthenticator) getUID(ctx context.Context, c claims, claimsValue *lazy.MapValue) (string, error) {
	uidClaim := a.jwtAuthenticator.ClaimMappings.UID.Claim
	if len(uidClaim) > 0 {
		var uid string
		if err := c.unmarshalClaim(uidClaim, &uid); err != nil {
			return "", fmt.Errorf("oidc: parse uid claim %q: %w", uidClaim, err)
		}
		return uid, nil
	}

	if a.celMapper.UID == nil {
		return "", nil
	}

	evalResult, err := a.celMapper.UID.EvalClaimMapping(ctx, claimsValue)
	if err != nil {
		return "", fmt.Errorf("oidc: error evaluating uid claim expression: %w", err)
	}
	if evalResult.EvalResult.Type() != celgo.StringType {
		return "", fmt.Errorf("oidc: error evaluating uid claim expression: %w", fmt.Errorf("uid claim expression must return a string"))
	}

	return evalResult.EvalResult.Value().(string), nil
}

// getExtra 解析额外用户属性，并总是附加 credential ID（若 token 含 jti）。
func (a *jwtAuthenticator) getExtra(ctx context.Context, c claims, claimsValue *lazy.MapValue) (map[string][]string, error) {
	extra := make(map[string][]string)

	if credentialID := getCredentialID(c); len(credentialID) > 0 {
		extra[user.CredentialIDKey] = []string{credentialID}
	}

	if a.celMapper.Extra == nil {
		return extra, nil
	}

	evalResult, err := a.celMapper.Extra.EvalClaimMappings(ctx, claimsValue)
	if err != nil {
		return nil, err
	}
	for _, result := range evalResult {
		extraMapping, ok := result.ExpressionAccessor.(*authenticationcel.ExtraMappingExpression)
		if !ok {
			return nil, fmt.Errorf("oidc: error evaluating extra claim expression: %w", fmt.Errorf("invalid type conversion, expected ExtraMappingCondition"))
		}

		extraValues, err := convertCELValueToStringList(result.EvalResult)
		if err != nil {
			return nil, fmt.Errorf("oidc: error evaluating extra claim expression: %s: %w", extraMapping.Expression, err)
		}

		if len(extraValues) > 0 {
			extra[extraMapping.Key] = extraValues
		}
	}

	return extra, nil
}

// getCredentialID 从 jti 声明派生 credential ID，用于审计与令牌撤销追踪。
// 解析失败时返回空串而不是错误——credential ID 是附加信息，不应阻断认证。
func getCredentialID(c claims) string {
	if _, ok := c["jti"]; ok {
		var jti string
		if err := c.unmarshalClaim("jti", &jti); err == nil {
			return authenticationtokenjwt.CredentialIDForJTI(jti)
		}
	}

	return ""
}

// getClaimJWT gets a distributed claim JWT from url, using the supplied access
// token as bearer token.  If the access token is "", the authorization header
// will not be set.
// TODO: Allow passing in JSON hints to the IDP.
func getClaimJWT(ctx context.Context, client *http.Client, url, accessToken string) (string, error) {
	// TODO: Allow passing request body with configurable information.
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("while calling %v: %v", url, err)
	}
	if accessToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %v", accessToken))
	}
	req = req.WithContext(ctx)
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	// Report non-OK status code as an error.
	if response.StatusCode < http.StatusOK || response.StatusCode > http.StatusIMUsed {
		return "", fmt.Errorf("error while getting distributed claim JWT: %v", response.Status)
	}
	responseBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return "", fmt.Errorf("could not decode distributed claim response")
	}
	return string(responseBytes), nil
}

// stringOrArray 兼容 claim 值是字符串或字符串数组两种编码。
type stringOrArray []string

// UnmarshalJSON 先尝试数组，失败再尝试单字符串。
//
// 不能先试字符串，因为 JSON 数组无法解码到 string；顺序不可交换。
func (s *stringOrArray) UnmarshalJSON(b []byte) error {
	var a []string
	if err := json.Unmarshal(b, &a); err == nil {
		*s = a
		return nil
	}
	var str string
	if err := json.Unmarshal(b, &str); err != nil {
		return err
	}
	*s = []string{str}
	return nil
}

// claims 是延迟解码的 claim 集合，保留原始 JSON 以便按需解析。
type claims map[string]json.RawMessage

// unmarshalClaim 解码指定 claim；不存在时返回明确的 "claim not present"。
func (c claims) unmarshalClaim(name string, v interface{}) error {
	val, ok := c[name]
	if !ok {
		return fmt.Errorf("claim not present")
	}
	return json.Unmarshal(val, v)
}

// hasClaim 判断 claim 是否存在（与值是否为空无关）。
func (c claims) hasClaim(name string) bool {
	if _, ok := c[name]; !ok {
		return false
	}
	return true
}

// newClaimsValue 把 claims 包装为惰性 CEL map。
//
// 每个字段在首次访问时才反序列化，未被表达式引用的 claim 完全不
// 付出解析成本，这对包含大量声明的 token 很关键。
func newClaimsValue(c claims) *lazy.MapValue {
	lazyMap := lazy.NewMapValue(types.NewObjectType("kubernetes.claims"))
	for name, msg := range c { // TODO add distributed claims support
		lazyMap.Append(name, func(_ *lazy.MapValue) ref.Val {
			data, err := msg.MarshalJSON()
			if err != nil {
				return types.WrapErr(err) // impossible since RawMessage never errors
			}

			var value any // TODO how do we do multiple levels of lazy decoding?
			if err := json.Unmarshal(data, &value); err != nil {
				return types.NewErr("claim %q failed to unmarshal: %w", name, err)
			}
			return nativeToValueWithUnescape(value)
		})
	}
	return lazyMap
}

// convertCELValueToStringList 把 CEL 值转换为字符串列表。CEL 值必须是字符串或字符串列表。
// ""、[] 视为不存在并返回 nil；字符串列表中的空字符串同样视为不存在并被过滤掉。
// 空值被规范化为 nil，使 "未设置" 与 "设置为空" 在认证语义上一致，
// 避免产生名为空的用户或组。
func convertCELValueToStringList(val ref.Val) ([]string, error) {
	switch val.Type().TypeName() {
	case celgo.StringType.TypeName():
		out := val.Value().(string)
		if len(out) == 0 {
			return nil, nil
		}
		return []string{out}, nil

	case celgo.ListType(nil).TypeName():
		var result []string
		switch val.Value().(type) {
		case []interface{}:
			for _, v := range val.Value().([]interface{}) {
				out, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("expression must return a string or a list of strings")
				}
				if len(out) == 0 {
					continue
				}
				result = append(result, out)
			}
		case []ref.Val:
			for _, v := range val.Value().([]ref.Val) {
				out, ok := v.Value().(string)
				if !ok {
					return nil, fmt.Errorf("expression must return a string or a list of strings")
				}
				if len(out) == 0 {
					continue
				}
				result = append(result, out)
			}
		default:
			return nil, fmt.Errorf("expression must return a string or a list of strings")
		}

		if len(result) == 0 {
			return nil, nil
		}

		return result, nil
	case celgo.NullType.TypeName():
		return nil, nil
	default:
		return nil, fmt.Errorf("expression must return a string or a list of strings")
	}
}

// messageFunc is a function that returns a message for a validation rule.
type messageFunc func(authenticationcel.ExpressionAccessor) (string, error)

// checkValidationRulesEvaluation 检查校验规则的求值结果是否合法；不合法时返回错误，
// 错误中可携带校验规则里设置的 message。
// 任一条规则返回非 bool 即视为配置错误；返回 false 时立即失败，
// 并附带规则作者提供的 message。
func checkValidationRulesEvaluation(results []authenticationcel.EvaluationResult, messageFn messageFunc) error {
	for _, result := range results {
		if result.EvalResult.Type() != celgo.BoolType {
			return fmt.Errorf("validation expression must return a boolean")
		}
		if !result.EvalResult.Value().(bool) {
			expression := result.ExpressionAccessor.GetExpression()

			message, err := messageFn(result.ExpressionAccessor)
			if err != nil {
				return err
			}

			return fmt.Errorf("validation expression '%s' failed: %s", expression, message)
		}
	}

	return nil
}

// newUserInfoValue 把已解析的 user.Info 包装为惰性 CEL map。
func newUserInfoValue(info user.Info) *lazy.MapValue {
	lazyMap := lazy.NewMapValue(types.NewObjectType("kubernetes.UserInfo"))
	field := func(name string, get func() any) {
		lazyMap.Append(name, func(_ *lazy.MapValue) ref.Val {
			value := get()
			return nativeToValueWithUnescape(value)
		})
	}
	field("username", func() any { return info.GetName() })
	field("uid", func() any { return info.GetUID() })
	field("groups", func() any { return info.GetGroups() })
	field("extra", func() any { return info.GetExtra() })
	return lazyMap
}

// nativeToValueWithUnescape 把原生 Go 值转成 CEL 值，并为 map/list 加上
// __dot__ 转义支持。
func nativeToValueWithUnescape(value any) ref.Val {
	return unescapeWrapper(types.DefaultTypeAdapter.NativeToValue(value))
}

// unescapeMapper 为 CEL map 的键访问增加反转义。
type unescapeMapper struct {
	traits.Mapper
}

// Find 先尝试反转义后的键，未命中再按原键查找。
func (m *unescapeMapper) Find(key ref.Val) (ref.Val, bool) {
	name, ok := unescapedName(key)
	if ok {
		key = name
	}
	value, ok := m.Mapper.Find(key)
	return unescapeWrapper(value), ok
}

// unescapeLister 为 CEL list 的元素访问递归包装嵌套结构。
type unescapeLister struct {
	traits.Lister
}

// Get 返回包装后的元素，保证深层嵌套的 map 同样支持转义键。
func (l *unescapeLister) Get(index ref.Val) ref.Val {
	return unescapeWrapper(l.Lister.Get(index))
}

// unescapeWrapper 为被转换成 CEL 值的原生类型提供基于 __dot__ 的字段访问。
// 也就是说需要为这些原生类型（claims JSON 与 user info 数据）处理 map 查找：
// user info 很简单，只有一个需要 __dot__ 支持的 map 字段；claims JSON 更复杂，
// 因为 map 可以出现在深度嵌套的字段里。因此凡是需要返回 CEL 值的地方，
// 都必须同时考虑嵌套的 JSON 对象和嵌套的 JSON 数组。
// 传入任何 CEL 值都是安全的，包括 nil（即调用方无需先做错误检查）。
// 只包装容器类型，标量原样返回，避免为每个叶子节点分配包装对象。
func unescapeWrapper(value ref.Val) ref.Val {
	switch v := value.(type) {
	case traits.Mapper:
		return &unescapeMapper{Mapper: v} // handle nested JSON objects
	case traits.Lister:
		return &unescapeLister{Lister: v} // handle nested JSON arrays
	default:
		return value
	}
}

// unescapedName 尝试把 CEL 转义标识符还原为原始 JSON 名。
//
// 还原结果与输入相同表示无需还原，返回 false 以避免无谓的二次查找。
func unescapedName(key ref.Val) (types.String, bool) {
	n, ok := key.(types.String)
	if !ok {
		return "", false
	}
	ns := string(n)
	name, ok := cel.Unescape(ns)
	if !ok || name == ns {
		return "", false
	}
	return types.String(name), true
}
