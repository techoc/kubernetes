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

// Package oidc implements the "oidc" kubeconfig auth provider.
//
// Package oidc 实现 kubeconfig 中的 oidc 认证提供者：用 id-token 发起请求，
// 过期时用 refresh-token 走 OAuth2 刷新流程，并把新 token 写回 kubeconfig。
// 注意该 in-tree provider 已属遗留实现，新代码更推荐 exec 凭证插件。
package oidc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"k8s.io/apimachinery/pkg/util/net"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

const (
	// 以下是 kubeconfig 中 auth-provider.config 支持的全部键名。
	cfgIssuerURL                = "idp-issuer-url"
	cfgClientID                 = "client-id"
	cfgClientSecret             = "client-secret"
	cfgCertificateAuthority     = "idp-certificate-authority"
	cfgCertificateAuthorityData = "idp-certificate-authority-data"
	cfgIDToken                  = "id-token"
	cfgRefreshToken             = "refresh-token"

	// Unused. Scopes aren't sent during refreshing.
	cfgExtraScopes = "extra-scopes"
)

func init() {
	// 同名 provider 重复注册属于编程错误，直接终止。
	if err := restclient.RegisterAuthProviderPlugin("oidc", newOIDCAuthProvider); err != nil {
		klog.Fatalf("Failed to register oidc auth plugin: %v", err)
	}
}

// expiryDelta determines how earlier a token should be considered
// expired than its actual expiration time. It is used to avoid late
// expirations due to client-server time mismatches.
//
// NOTE(ericchiang): this is take from golang.org/x/oauth2
const expiryDelta = 10 * time.Second

var cache = newClientCache()

// Like TLS transports, keep a cache of OIDC clients indexed by issuer URL. This ensures
// current requests from different clients don't concurrently attempt to refresh the same
// set of credentials.
//
// clientCache 按 (集群地址, issuer, clientID) 缓存 provider；同一组凭证只应有一个
// provider 实例，否则并发刷新会互相覆盖 kubeconfig。
type clientCache struct {
	mu sync.RWMutex

	cache map[cacheKey]*oidcAuthProvider
}

func newClientCache() *clientCache {
	return &clientCache{cache: make(map[cacheKey]*oidcAuthProvider)}
}

// cacheKey 定位一组互相独立的 OIDC 凭证。clusterAddress 参与键计算，
// 使同一 issuer 但不同集群的配置可以共存。
type cacheKey struct {
	clusterAddress string
	// Canonical issuer URL string of the provider.
	issuerURL string
	clientID  string
}

func (c *clientCache) getClient(clusterAddress, issuer, clientID string) (*oidcAuthProvider, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	client, ok := c.cache[cacheKey{clusterAddress: clusterAddress, issuerURL: issuer, clientID: clientID}]
	return client, ok
}

// setClient attempts to put the client in the cache but may return any clients
// with the same keys set before. This is so there's only ever one client for a provider.
//
// 若键已存在则返回旧实例：调用方必须使用返回值，否则会绕过共享互斥锁，
// 导致对 kubeconfig 的并发写入。
func (c *clientCache) setClient(clusterAddress, issuer, clientID string, client *oidcAuthProvider) *oidcAuthProvider {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := cacheKey{clusterAddress: clusterAddress, issuerURL: issuer, clientID: clientID}

	// If another client has already initialized a client for the given provider we want
	// to use that client instead of the one we're trying to set. This is so all transports
	// share a client and can coordinate around the same mutex when refreshing and writing
	// to the kubeconfig.
	if oldClient, ok := c.cache[key]; ok {
		return oldClient
	}

	c.cache[key] = client
	return client
}

// newOIDCAuthProvider 校验必需配置并构造 provider。CA 数据支持内联 base64，
// 与 TLS 客户端配置的优先级保持一致（内联数据优先于文件路径）。
func newOIDCAuthProvider(clusterAddress string, cfg map[string]string, persister restclient.AuthProviderConfigPersister) (restclient.AuthProvider, error) {
	issuer := cfg[cfgIssuerURL]
	if issuer == "" {
		return nil, fmt.Errorf("Must provide %s", cfgIssuerURL)
	}

	clientID := cfg[cfgClientID]
	if clientID == "" {
		return nil, fmt.Errorf("Must provide %s", cfgClientID)
	}

	// Check cache for existing provider.
	if provider, ok := cache.getClient(clusterAddress, issuer, clientID); ok {
		return provider, nil
	}

	if len(cfg[cfgExtraScopes]) > 0 {
		// 保留该字段仅为兼容旧配置；刷新请求不再发送 scopes。
		klog.V(2).Infof("%s auth provider field depricated, refresh request don't send scopes",
			cfgExtraScopes)
	}

	var certAuthData []byte
	var err error
	if cfg[cfgCertificateAuthorityData] != "" {
		certAuthData, err = base64.StdEncoding.DecodeString(cfg[cfgCertificateAuthorityData])
		if err != nil {
			return nil, err
		}
	}

	// 仅使用 CA 相关字段；该 transport 只用于访问 issuer 的发现文档和 token 端点。
	clientConfig := restclient.Config{
		TLSClientConfig: restclient.TLSClientConfig{
			CAFile: cfg[cfgCertificateAuthority],
			CAData: certAuthData,
		},
	}

	trans, err := restclient.TransportFor(&clientConfig)
	if err != nil {
		return nil, err
	}
	hc := &http.Client{Transport: trans}

	provider := &oidcAuthProvider{
		client:    hc,
		now:       time.Now,
		cfg:       cfg,
		persister: persister,
	}

	return cache.setClient(clusterAddress, issuer, clientID, provider), nil
}

// oidcAuthProvider 保存可变配置与持久化回调；cfg 由 mu 保护。
type oidcAuthProvider struct {
	client *http.Client

	// now 用于确定当前时间；提取为字段便于测试中伪造时间，避免依赖真实时钟。
	now func() time.Time

	// Mutex guards persisting to the kubeconfig file and allows synchronized
	// updates to the in-memory config. It also ensures concurrent calls to
	// the RoundTripper only trigger a single refresh request.
	mu        sync.Mutex
	cfg       map[string]string
	persister restclient.AuthProviderConfigPersister
}

// WrapTransport 在原始 transport 外包裹一层，为缺少 Authorization 头的请求注入 Bearer token。
func (p *oidcAuthProvider) WrapTransport(rt http.RoundTripper) http.RoundTripper {
	return &roundTripper{
		wrapped:  rt,
		provider: p,
	}
}

// Login 尚未实现；OIDC provider 只依赖 kubeconfig 中已有的 token。
func (p *oidcAuthProvider) Login() error {
	return errors.New("not yet implemented")
}

// roundTripper 在发送前补充 Authorization 头，并实现 RoundTripperWrapper 以支持
// 取消包装（例如连接池/超时配置需要访问底层 transport）。
type roundTripper struct {
	provider *oidcAuthProvider
	wrapped  http.RoundTripper
}

var _ net.RoundTripperWrapper = &roundTripper{}

func (r *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// 已有认证头（如显式 Bearer）则不覆盖，避免破坏调用方意图。
	if len(req.Header.Get("Authorization")) != 0 {
		return r.wrapped.RoundTrip(req)
	}
	token, err := r.provider.idToken()
	if err != nil {
		return nil, err
	}

	// shallow copy of the struct
	r2 := new(http.Request)
	*r2 = *req
	// 对 Header 做深拷贝，避免修改原请求的 Header（这是 RoundTripper 契约的要求）：
	// Header 是 map，浅拷贝仍共享底层数据，必须逐项复制。
	// Header 是 map，浅拷贝仍共享底层数据，必须逐项复制以遵守 RoundTripper 不得修改入参的约定。
	r2.Header = make(http.Header)
	for k, s := range req.Header {
		r2.Header[k] = s
	}
	r2.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	return r.wrapped.RoundTrip(r2)
}

func (r *roundTripper) WrappedRoundTripper() http.RoundTripper { return r.wrapped }

// idToken 返回可用的 id-token：未过期则直接复用，否则用 refresh-token 换取新 token。
// mu 串行化整个刷新与持久化过程，防止并发请求同时刷新、重复写 kubeconfig。
func (p *oidcAuthProvider) idToken() (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if idToken, ok := p.cfg[cfgIDToken]; ok && len(idToken) > 0 {
		valid, err := idTokenExpired(p.now, idToken)
		if err != nil {
			return "", err
		}
		if valid {
			// If the cached id token is still valid use it.
			return idToken, nil
		}
	}

	// Try to request a new token using the refresh token.
	rt, ok := p.cfg[cfgRefreshToken]
	if !ok || len(rt) == 0 {
		return "", errors.New("No valid id-token, and cannot refresh without refresh-token")
	}

	// Determine provider's OAuth2 token endpoint.
	tokenURL, err := tokenEndpoint(p.client, p.cfg[cfgIssuerURL])
	if err != nil {
		return "", err
	}

	config := oauth2.Config{
		ClientID:     p.cfg[cfgClientID],
		ClientSecret: p.cfg[cfgClientSecret],
		Endpoint:     oauth2.Endpoint{TokenURL: tokenURL},
	}

	// 通过 context 注入自定义 http.Client，使 oauth2 使用配置好的 CA/transport。
	ctx := context.WithValue(context.Background(), oauth2.HTTPClient, p.client)
	token, err := config.TokenSource(ctx, &oauth2.Token{RefreshToken: rt}).Token()
	if err != nil {
		return "", fmt.Errorf("failed to refresh token: %v", err)
	}

	idToken, ok := token.Extra("id_token").(string)
	if !ok {
		// id_token isn't a required part of a refresh token response, so some
		// providers (Okta) don't return this value.
		//
		// See https://github.com/kubernetes/kubernetes/issues/36847
		return "", fmt.Errorf("token response did not contain an id_token, either the scope \"openid\" wasn't requested upon login, or the provider doesn't support id_tokens as part of the refresh response")
	}

	// 新建一份待持久化的配置：复制一份再改，避免在持久化失败时污染内存中的旧配置。
	newCfg := make(map[string]string)
	for key, val := range p.cfg {
		newCfg[key] = val
	}

	// 服务端返回了新的 refresh token 时更新：部分 IdP 会轮换 refresh token；未返回新值则保留旧值。
	if token.RefreshToken != "" && token.RefreshToken != rt {
		newCfg[cfgRefreshToken] = token.RefreshToken
	}
	newCfg[cfgIDToken] = idToken

	// 先持久化新配置、成功后再更新内存中的配置，保证进程内与 kubeconfig 一致。
	if err = p.persister.Persist(newCfg); err != nil {
		return "", fmt.Errorf("could not persist new tokens: %v", err)
	}
	p.cfg = newCfg

	return idToken, nil
}

// tokenEndpoint uses OpenID Connect discovery to determine the OAuth2 token
// endpoint for the provider, the endpoint the client will use the refresh
// token against.
func tokenEndpoint(client *http.Client, issuer string) (string, error) {
	// Well known URL for getting OpenID Connect metadata.
	//
	// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderConfig
	// issuer 末尾的 "/" 需去掉，避免拼出 "//.well-known"。
	wellKnown := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"
	resp, err := client.Get(wellKnown)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		// 不要生成过大的错误信息（例如不知为何返回了 HTML 页面）：截断响应体，避免把整页 HTML 塞进错误信息。
		const n = 80
		if len(body) > n {
			body = append(body[:n], []byte("...")...)
		}
		return "", fmt.Errorf("oidc: failed to query metadata endpoint %s: %q", resp.Status, body)
	}

	// Metadata object. We only care about the token_endpoint, the thing endpoint
	// we'll be refreshing against.
	//
	// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
	var metadata struct {
		TokenURL string `json:"token_endpoint"`
	}
	if err := json.Unmarshal(body, &metadata); err != nil {
		return "", fmt.Errorf("oidc: failed to decode provider discovery object: %v", err)
	}
	if metadata.TokenURL == "" {
		return "", fmt.Errorf("oidc: discovery object doesn't contain a token_endpoint")
	}
	return metadata.TokenURL, nil
}

// idTokenExpired 解析 JWT 载荷的 exp 声明，并用 expiryDelta 做提前量判断。
// 注意返回值为“仍有效”，命名沿用历史实现，与函数名语义相反。
func idTokenExpired(now func() time.Time, idToken string) (bool, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return false, fmt.Errorf("ID Token is not a valid JWT")
	}

	// 载荷使用 RawURLEncoding（无填充），base64.StdEncoding 会解析失败。
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false, err
	}
	var claims struct {
		Expiry jsonTime `json:"exp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return false, fmt.Errorf("parsing claims: %v", err)
	}

	return now().Add(expiryDelta).Before(time.Time(claims.Expiry)), nil
}

// jsonTime is a json.Unmarshaler that parses a unix timestamp.
// Because JSON numbers don't differentiate between ints and floats,
// we want to ensure we can parse either.
type jsonTime time.Time

func (j *jsonTime) UnmarshalJSON(b []byte) error {
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	var unix int64

	// 先按整数解析；失败再按浮点解析并截断小数部分。
	if t, err := n.Int64(); err == nil {
		unix = t
	} else {
		f, err := n.Float64()
		if err != nil {
			return err
		}
		unix = int64(f)
	}
	*j = jsonTime(time.Unix(unix, 0))
	return nil
}

func (j jsonTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(j).Unix())
}
