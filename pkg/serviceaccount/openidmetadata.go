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

package serviceaccount

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/url"
	"sync/atomic"

	jose "gopkg.in/go-jose/go-jose.v2"

	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	// OpenIDConfigPath 是 API Server 提供 OIDC 发现文档的路径。
	// 遵循 OIDC Discovery 1.0 规范，外部依赖方通过此路径获取 ServiceAccount Token 的验证信息。
	// https://openid.net/specs/openid-connect-discovery-1_0.html
	OpenIDConfigPath = "/.well-known/openid-configuration"

	// JWKSPath 是 API 服务器提供 JWKS（JSON Web Key Set）的路径。
	// 包含用于验证 ServiceAccount Token 签名的所有公钥，支持密钥轮换。
	JWKSPath = "/openid/v1/jwks"
)

// OpenIDMetadataProvider 提供 OIDC 发现端点的预渲染响应。
// GetConfigJSON 返回 OIDC Discovery 文档，GetKeysetJSON 返回 JWKS。
// maxAge 指示响应可以缓存的秒数，用于 HTTP Cache-Control 头。
type OpenIDMetadataProvider interface {
	GetConfigJSON() (json []byte, maxAge int)
	GetKeysetJSON() (json []byte, maxAge int)
}

// openidConfigProvider 实现 OpenIDMetadataProvider 接口。
// 当公钥变化时（通过 Listener 通知），原子更新预渲染的 JSON 内容。
type openidConfigProvider struct {
	issuerURL, jwksURI string
	pubKeyGetter       PublicKeysGetter
	config             atomic.Pointer[openidConfig]
}

// openidConfig 存储预渲染的 OIDC 配置和 JWKS JSON 内容。
// 使用 atomic.Pointer 确保并发读取时不会看到部分更新的数据。
type openidConfig struct {
	configJSON []byte
	keysetJSON []byte
}

func (p *openidConfigProvider) GetConfigJSON() ([]byte, int) {
	return p.config.Load().configJSON, p.pubKeyGetter.GetCacheAgeMaxSeconds()
}
func (p *openidConfigProvider) GetKeysetJSON() ([]byte, int) {
	return p.config.Load().keysetJSON, p.pubKeyGetter.GetCacheAgeMaxSeconds()
}

// Enqueue 实现 Listener 接口，在公钥变化时重新生成 OIDC 元数据。
// 由 PublicKeysGetter 在密钥更新时调用。
func (p *openidConfigProvider) Enqueue() {
	err := p.Update()
	if err != nil {
		klog.ErrorS(err, "failed to update openid config metadata")
	}
}

// Update 重新生成 OIDC 配置和 JWKS JSON 内容。
// 过滤掉 ExcludeFromOIDCDiscovery 的密钥，只保留可用于新 Token 验证的公钥。
// 使用 atomic.Store 确保并发读取时不会看到部分更新的数据。
func (p *openidConfigProvider) Update() error {
	pubKeys := []PublicKey{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	unfilteredPubKeys := p.pubKeyGetter.GetPublicKeys(ctx, "")
	for _, key := range unfilteredPubKeys {
		if !key.ExcludeFromOIDCDiscovery {
			pubKeys = append(pubKeys, key)
		}
	}

	if len(pubKeys) == 0 {
		return fmt.Errorf("no keys provided for validating keyset")
	}
	configJSON, err := openIDConfigJSON(p.issuerURL, p.jwksURI, pubKeys)
	if err != nil {
		return fmt.Errorf("could not marshal issuer discovery JSON, error: %w", err)
	}
	keysetJSON, err := openIDKeysetJSON(pubKeys)
	if err != nil {
		return fmt.Errorf("could not marshal issuer keys JSON, error: %w", err)
	}
	p.config.Store(&openidConfig{
		configJSON: configJSON,
		keysetJSON: keysetJSON,
	})
	return nil
}

// NewOpenIDMetadataProvider 创建 OIDC 发现端点提供方，创建失败时返回错误。
// 此函数可能执行命令行校验之外且不向后兼容的额外输入校验；调用方应记录错误并跳过安装发现端点。
func NewOpenIDMetadataProvider(issuerURL, jwksURI, defaultExternalAddress string, pubKeyGetter PublicKeysGetter) (OpenIDMetadataProvider, error) {
	if issuerURL == "" {
		return nil, fmt.Errorf("empty issuer URL")
	}
	if jwksURI == "" && defaultExternalAddress == "" {
		return nil, fmt.Errorf("either the JWKS URI or the default external address, or both, must be set")
	}
	if pubKeyGetter == nil {
		return nil, fmt.Errorf("no public key getter provided")
	}

	// 确保 issuer URL 满足 OIDC 规范，即文档注释所述的额外校验。
	// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
	iss, err := url.Parse(issuerURL)
	if err != nil {
		return nil, err
	}
	if iss.Scheme != "https" {
		return nil, fmt.Errorf("issuer URL must use https scheme, got: %s", issuerURL)
	}
	if iss.RawQuery != "" {
		return nil, fmt.Errorf("issuer URL may not include a query, got: %s", issuerURL)
	}
	if iss.Fragment != "" {
		return nil, fmt.Errorf("issuer URL may not include a fragment, got: %s", issuerURL)
	}

	// 使用提供的 JWKS URI；未提供时由 ExternalAddress 加 JWKS 路径推导。
	if jwksURI == "" {
		const msg = "attempted to build jwks_uri from external " +
			"address %s, but could not construct a valid URL. Error: %v"

		if defaultExternalAddress == "" {
			return nil, fmt.Errorf(msg, defaultExternalAddress,
				fmt.Errorf("empty address"))
		}

		u := &url.URL{
			Scheme: "https",
			Host:   defaultExternalAddress,
			Path:   JWKSPath,
		}
		jwksURI = u.String()

		// TODO(mtaufen): ExternalAddress 可能只需 host + port，但确认前仍保留此检查。

		// 检查推导出的 jwksURI 是否是预期 URL，防止 ExternalAddress 误含 scheme + host + port。
		parsed, err := url.Parse(jwksURI)
		if err != nil {
			return nil, fmt.Errorf(msg, defaultExternalAddress, err)
		} else if u.Scheme != parsed.Scheme ||
			u.Host != parsed.Host ||
			u.Path != parsed.Path {
			return nil, fmt.Errorf(msg, defaultExternalAddress,
				fmt.Errorf("got %v, expected %v", parsed, u))
		}
	} else {
		// 再次确认显式提供的 jwksURI 是 HTTPS URL。
		if u, err := url.Parse(jwksURI); err != nil {
			return nil, err
		} else if u.Scheme != "https" {
			return nil, fmt.Errorf("jwksURI requires https scheme, parsed as: %v", u.String())
		}
	}

	provider := &openidConfigProvider{
		issuerURL:    issuerURL,
		jwksURI:      jwksURI,
		pubKeyGetter: pubKeyGetter,
	}
	// 注册公钥变化通知。
	pubKeyGetter.AddListener(provider)
	// 启动时同步生成配置和密钥集 JSON，确保初始端点可用。
	if err := provider.Update(); err != nil {
		return nil, err
	}
	return provider, nil
}

// openIDMetadata 提供 OIDC 提供者元数据的最小子集：
// https://openid.net/specs/openid-connect-discovery-1_0.html#ProviderMetadata
type openIDMetadata struct {
	Issuer string `json:"issuer"` // OIDC 必填；依赖方据此识别签发者。
	// TODO(mtaufen): 依赖方只需验证 ID Token，无需发起登录流程；
	// authorization_endpoint 的取值尚未确定，在有人反馈前保持省略。
	// AuthzEndpoint string   `json:"authorization_endpoint"`                // OIDC 必填；但依赖方无需使用。
	JWKSURI       string   `json:"jwks_uri"`                              // OIDC 必填；依赖方据此获取验证密钥。
	ResponseTypes []string `json:"response_types_supported"`              // OIDC 必填。
	SubjectTypes  []string `json:"subject_types_supported"`               // OIDC 必填。
	SigningAlgs   []string `json:"id_token_signing_alg_values_supported"` // OIDC 必填。
}

// openIDConfigJSON 返回服务账号签发者的 OIDC Discovery JSON 文档。
func openIDConfigJSON(iss, jwksURI string, keys []PublicKey) ([]byte, error) {
	keyset, errs := publicJWKSFromKeys(keys)
	if errs != nil {
		return nil, errs
	}

	metadata := openIDMetadata{
		Issuer:        iss,
		JWKSURI:       jwksURI,
		ResponseTypes: []string{"id_token"}, // Kubernetes 只签发 ID Token。
		SubjectTypes:  []string{"public"},   // https://openid.net/specs/openid-connect-core-1_0.html#SubjectIDTypes
		SigningAlgs:   getAlgs(keyset),      // OIDC 必填。
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal service account issuer metadata: %v", err)
	}

	return metadataJSON, nil
}

// openIDKeysetJSON 返回 ServiceAccount issuer 所用密钥的 JSON Web Key Set。
func openIDKeysetJSON(keys []PublicKey) ([]byte, error) {
	keyset, errs := publicJWKSFromKeys(keys)
	if errs != nil {
		return nil, errs
	}

	keysetJSON, err := json.Marshal(keyset)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal service account issuer JWKS: %v", err)
	}

	return keysetJSON, nil
}

func getAlgs(keys *jose.JSONWebKeySet) []string {
	algs := sets.NewString()
	for _, k := range keys.Keys {
		algs.Insert(k.Algorithm)
	}
	// List 返回排序后的切片。
	return algs.List()
}

type publicKeyGetter interface {
	Public() crypto.PublicKey
}

// publicJWKSFromKeys 从输入密钥构造 JSONWebKeySet，且只包含对应的公钥。
func publicJWKSFromKeys(in []PublicKey) (*jose.JSONWebKeySet, errors.Aggregate) {
	// 将密钥解码到 JWKS。
	var keys jose.JSONWebKeySet
	var errs []error
	for i, key := range in {
		pubkey, err := jwkFromPublicKey(key)
		if err != nil {
			errs = append(errs, fmt.Errorf("error constructing JWK for key #%d: %v", i, err))
			continue
		}

		if !pubkey.Valid() {
			errs = append(errs, fmt.Errorf("key #%d not valid", i))
			continue
		}
		keys.Keys = append(keys.Keys, *pubkey)
	}
	if len(errs) != 0 {
		return nil, errors.NewAggregate(errs)
	}
	return &keys, nil
}

func jwkFromPublicKey(publicKey PublicKey) (*jose.JSONWebKey, error) {
	alg, err := algorithmFromPublicKey(publicKey.PublicKey)
	if err != nil {
		return nil, err
	}

	jwk := &jose.JSONWebKey{
		Algorithm: string(alg),
		Key:       publicKey.PublicKey,
		KeyID:     publicKey.KeyID,
		Use:       "sig",
	}

	if !jwk.IsPublic() {
		return nil, fmt.Errorf("JWK was not a public key! JWK: %v", jwk)
	}

	return jwk, nil
}

func algorithmFromPublicKey(publicKey crypto.PublicKey) (jose.SignatureAlgorithm, error) {
	switch pk := publicKey.(type) {
	case *rsa.PublicKey:
		// 重要：若支持更多密钥长度，必须同步更新 serviceaccount/jwt.go 的 signerFromRSAPrivateKey；目前仅支持 RS256。
		return jose.RS256, nil
	case *ecdsa.PublicKey:
		switch pk.Curve {
		case elliptic.P256():
			return jose.ES256, nil
		case elliptic.P384():
			return jose.ES384, nil
		case elliptic.P521():
			return jose.ES512, nil
		default:
			return "", fmt.Errorf("unknown private key curve, must be 256, 384, or 521")
		}
	case jose.OpaqueSigner:
		return jose.SignatureAlgorithm(pk.Public().Algorithm), nil
	default:
		return "", fmt.Errorf("unknown public key type, must be *rsa.PublicKey, *ecdsa.PublicKey, or jose.OpaqueSigner")
	}
}
