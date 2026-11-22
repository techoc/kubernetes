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

package serviceaccount

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	jose "gopkg.in/go-jose/go-jose.v2"
	"gopkg.in/go-jose/go-jose.v2/jwt"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	apiserverserviceaccount "k8s.io/apiserver/pkg/authentication/serviceaccount"
)

// ServiceAccountTokenGetter 定义获取 ServiceAccount、Pod、Secret、Node 和 Webhook 配置的方法。
// 用于 Token 验证时检查绑定对象的存在性和 UID 匹配。
type ServiceAccountTokenGetter interface {
	GetServiceAccount(ctx context.Context, namespace, name string) (*v1.ServiceAccount, error)
	GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error)
	GetSecret(ctx context.Context, namespace, name string) (*v1.Secret, error)
	GetNode(ctx context.Context, name string) (*v1.Node, error)
	GetValidatingWebhookConfiguration(ctx context.Context, name string) (*admissionregistrationv1.ValidatingWebhookConfiguration, error)
	GetMutatingWebhookConfiguration(ctx context.Context, name string) (*admissionregistrationv1.MutatingWebhookConfiguration, error)
}

// TokenGenerator 是生成 ServiceAccount Token 的接口。
// GenerateToken 将公共和私有 claim 合并为 JWT payload，并用私钥签名。
// 公共 claim 优先级高于私有 claim，如果两者有同名字段，使用公共 claim 的值。
type TokenGenerator interface {
	// GenerateToken generates a token which will identify the given
	// ServiceAccount. privateClaims is an interface that will be
	// serialized into the JWT payload JSON encoding at the root level of
	// the payload object. Public claims take precedent over private
	// claims i.e. if both claims and privateClaims have an "exp" field,
	// the value in claims will be used.
	GenerateToken(ctx context.Context, claims *jwt.Claims, privateClaims interface{}) (string, error)
}

// JWTTokenGenerator 创建使用给定私钥生成签名 JWT Token 的生成器。
// 支持 RSA、ECDSA 和 OpaqueSigner（外部签名器）三种私钥类型。
// iss 是 Token 的 issuer，会嵌入每个生成的 Token 中。
func JWTTokenGenerator(iss string, privateKey interface{}) (TokenGenerator, error) {
	var signer jose.Signer
	var err error
	switch pk := privateKey.(type) {
	case *rsa.PrivateKey:
		signer, err = signerFromRSAPrivateKey(pk)
		if err != nil {
			return nil, fmt.Errorf("could not generate signer for RSA keypair: %v", err)
		}
	case *ecdsa.PrivateKey:
		signer, err = signerFromECDSAPrivateKey(pk)
		if err != nil {
			return nil, fmt.Errorf("could not generate signer for ECDSA keypair: %v", err)
		}
	case jose.OpaqueSigner:
		signer, err = signerFromOpaqueSigner(pk)
		if err != nil {
			return nil, fmt.Errorf("could not generate signer for OpaqueSigner: %v", err)
		}
	default:
		return nil, fmt.Errorf("unknown private key type %T, must be *rsa.PrivateKey, *ecdsa.PrivateKey, or jose.OpaqueSigner", privateKey)
	}

	return &jwtTokenGenerator{
		iss:    iss,
		signer: signer,
	}, nil
}

// keyIDFromPublicKey derives a key ID non-reversibly from a public key.
//
// The Key ID is field on a given on JWTs and JWKs that help relying parties
// pick the correct key for verification when the identity party advertises
// multiple keys.
//
// Making the derivation non-reversible makes it impossible for someone to
// accidentally obtain the real key from the key ID and use it for token
// validation.
func keyIDFromPublicKey(publicKey interface{}) (string, error) {
	publicKeyDERBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("failed to serialize public key to DER format: %v", err)
	}

	hasher := crypto.SHA256.New()
	hasher.Write(publicKeyDERBytes)
	publicKeyDERHash := hasher.Sum(nil)

	keyID := base64.RawURLEncoding.EncodeToString(publicKeyDERHash)

	return keyID, nil
}

// signerFromRSAPrivateKey 从 RSA 私钥创建 JOSE Signer。
// 使用 RS256 算法，并从公钥派生 keyID（SHA256 hash）。
// keyID 用于 JWT header 的 kid 字段，让验证方选择正确的公钥。
func signerFromRSAPrivateKey(keyPair *rsa.PrivateKey) (jose.Signer, error) {
	keyID, err := keyIDFromPublicKey(&keyPair.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to derive keyID: %v", err)
	}

	// IMPORTANT: The RSA algorithms listed below must be kept in sync with:
	// - pkg/serviceaccount/externaljwt/plugin/plugin.go validateJWTHeader
	// - pkg/serviceaccount/jwt.go signerFromRSAPrivateKey
	// - pkg/serviceaccount/jwt.go signerFromECDSAPrivateKey
	// - test/images/agnhost/openidmetadata/openidmetadata.go validate SupportedSigningAlgs

	// Wrap the RSA keypair in a JOSE JWK with the designated key ID.
	privateJWK := &jose.JSONWebKey{
		Algorithm: string(jose.RS256),
		Key:       keyPair,
		KeyID:     keyID,
		Use:       "sig",
	}

	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: jose.RS256,
			Key:       privateJWK,
		},
		nil,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create signer: %v", err)
	}

	return signer, nil
}

// signerFromECDSAPrivateKey 从 ECDSA 私钥创建 JOSE Signer。
// 根据椭圆曲线类型选择算法：P-256→ES256, P-384→ES384, P-521→ES512。
func signerFromECDSAPrivateKey(keyPair *ecdsa.PrivateKey) (jose.Signer, error) {
	var alg jose.SignatureAlgorithm

	// IMPORTANT: The EC algorithms listed below must be kept in sync with:
	// - pkg/serviceaccount/externaljwt/plugin/plugin.go validateJWTHeader
	// - pkg/serviceaccount/jwt.go signerFromRSAPrivateKey
	// - pkg/serviceaccount/jwt.go signerFromECDSAPrivateKey
	// - test/images/agnhost/openidmetadata/openidmetadata.go validate SupportedSigningAlgs

	switch keyPair.Curve {
	case elliptic.P256():
		alg = jose.ES256
	case elliptic.P384():
		alg = jose.ES384
	case elliptic.P521():
		alg = jose.ES512
	default:
		return nil, fmt.Errorf("unknown private key curve, must be 256, 384, or 521")
	}

	keyID, err := keyIDFromPublicKey(&keyPair.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to derive keyID: %v", err)
	}

	// Wrap the ECDSA keypair in a JOSE JWK with the designated key ID.
	privateJWK := &jose.JSONWebKey{
		Algorithm: string(alg),
		Key:       keyPair,
		KeyID:     keyID,
		Use:       "sig",
	}

	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: alg,
			Key:       privateJWK,
		},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create signer: %v", err)
	}

	return signer, nil
}

// signerFromOpaqueSigner 从 OpaqueSigner（如 HSM 或外部签名服务）创建 JOSE Signer。
// 支持硬件安全模块等场景，私钥不直接暴露给 API Server。
func signerFromOpaqueSigner(opaqueSigner jose.OpaqueSigner) (jose.Signer, error) {
	alg := jose.SignatureAlgorithm(opaqueSigner.Public().Algorithm)

	signer, err := jose.NewSigner(
		jose.SigningKey{
			Algorithm: alg,
			Key: &jose.JSONWebKey{
				Algorithm: string(alg),
				Key:       opaqueSigner,
				KeyID:     opaqueSigner.Public().KeyID,
				Use:       "sig",
			},
		},
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create signer: %v", err)
	}

	return signer, nil
}

// jwtTokenGenerator 使用 JOSE Signer 生成签名的 JWT Token。
type jwtTokenGenerator struct {
	iss    string
	signer jose.Signer
}

func (j *jwtTokenGenerator) GenerateToken(ctx context.Context, claims *jwt.Claims, privateClaims interface{}) (string, error) {
	return GenerateToken(j.signer, j.iss, claims, privateClaims)
}

// JWTTokenAuthenticator 创建 JWT Token 认证器。
// 使用提供的公钥验证 Token 签名，支持密钥轮换（多个公钥依次尝试）。
// 通过泛型 PrivateClaims 支持不同领域的 claim 验证逻辑（如 ServiceAccount 或外部 JWT）。
// issuers 是受信任的签发者列表，implicitAuds 用于没有 audience claim 的旧版 Token。
func JWTTokenAuthenticator[PrivateClaims any](issuers []string, publicKeysGetter PublicKeysGetter, implicitAuds authenticator.Audiences, validator Validator[PrivateClaims]) authenticator.Token {
	issuersMap := make(map[string]bool)
	for _, issuer := range issuers {
		issuersMap[issuer] = true
	}
	return &jwtTokenAuthenticator[PrivateClaims]{
		issuers:      issuersMap,
		keysGetter:   publicKeysGetter,
		implicitAuds: implicitAuds,
		validator:    validator,
	}
}

// Listener 用于通知感兴趣的组件输入可能已变化。
// 当公钥更新时，PublicKeysGetter 调用所有注册的 Listener 的 Enqueue 方法，
// 触发依赖方（如 OIDC 元数据提供者）重新生成缓存数据。
type Listener interface {
	// Enqueue should be called when an input may have changed
	Enqueue()
}

// PublicKeysGetter 根据 key ID 获取公钥列表。
// 支持静态公钥（StaticPublicKeysGetter）和动态公钥（如 external-jwt-signer 的 keyCache）。
// 通过 Listener 机制通知公钥变化，支持密钥轮换。
type PublicKeysGetter interface {
	// AddListener is adds a listener to be notified of potential input changes.
	// This is a noop on static providers.
	AddListener(listener Listener)

	// GetCacheAgeMaxSeconds returns the seconds a call to GetPublicKeys() can be cached for.
	// If the results of GetPublicKeys() can be dynamic, this means a new key must be included in the results
	// for at least this long before it is used to sign new tokens.
	GetCacheAgeMaxSeconds() int

	// GetPublicKeys returns public keys to use for verifying a token with the given key id.
	// keyIDHint may be empty if the token did not have a kid header, or if all public keys are desired.
	GetPublicKeys(ctx context.Context, keyIDHint string) []PublicKey
}

// PublicKey 表示用于验证 JWT 签名的公钥及其元数据。
// ExcludeFromOIDCDiscovery 用于标记仅用于验证历史 Token 的密钥，不暴露在 OIDC 发现端点。
type PublicKey struct {
	KeyID                    string
	PublicKey                interface{}
	ExcludeFromOIDCDiscovery bool
}

// staticPublicKeysGetter 实现静态公钥的 PublicKeysGetter。
// 启动时初始化，之后不会变化，适用于内置签名密钥场景。
type staticPublicKeysGetter struct {
	allPublicKeys  []PublicKey
	publicKeysByID map[string][]PublicKey
}

// StaticPublicKeysGetter 创建静态公钥获取器。
// 支持传入私钥（自动提取公钥）或公钥。通过 keyIDFromPublicKey 派生 keyID。
// 适用于 API Server 内置签名密钥场景，不支持运行时密钥轮换。
func StaticPublicKeysGetter(keys []interface{}) (PublicKeysGetter, error) {
	allPublicKeys := []PublicKey{}
	publicKeysByID := map[string][]PublicKey{}
	for _, key := range keys {
		if privateKey, isPrivateKey := key.(publicKeyGetter); isPrivateKey {
			// This is a private key. Extract its public key.
			key = privateKey.Public()
		}

		keyID, err := keyIDFromPublicKey(key)
		if err != nil {
			return nil, err
		}
		pk := PublicKey{PublicKey: key, KeyID: keyID}
		publicKeysByID[keyID] = append(publicKeysByID[keyID], pk)
		allPublicKeys = append(allPublicKeys, pk)
	}
	return &staticPublicKeysGetter{
		allPublicKeys:  allPublicKeys,
		publicKeysByID: publicKeysByID,
	}, nil
}

func (s staticPublicKeysGetter) AddListener(listener Listener) {
	// no-op, static key content never changes
}

func (s staticPublicKeysGetter) GetCacheAgeMaxSeconds() int {
	// hard-coded to match cache max-age set in OIDC discovery
	return 3600
}

func (s staticPublicKeysGetter) GetPublicKeys(ctx context.Context, keyID string) []PublicKey {
	if len(keyID) == 0 {
		return s.allPublicKeys
	}
	return s.publicKeysByID[keyID]
}

// jwtTokenAuthenticator 实现 authenticator.Token 接口，验证 JWT Token。
// 使用泛型 PrivateClaims 支持不同领域的 claim 验证逻辑。
type jwtTokenAuthenticator[PrivateClaims any] struct {
	issuers      map[string]bool
	keysGetter   PublicKeysGetter
	validator    Validator[PrivateClaims]
	implicitAuds authenticator.Audiences
}

// Validator 由 JWT Token 认证器调用，对 Token 进行领域特定的验证并提取用户信息。
// PrivateClaims 是认证器反序列化 JWT payload 的目标结构体。
// 实现者可以假设调用 Validate 时，Token 的 issuer 和签名已经验证通过。
type Validator[PrivateClaims any] interface {
	// Validate validates a token and returns user information or an error.
	// Validator can assume that the issuer and signature of a token are already
	// verified when this function is called.
	Validate(ctx context.Context, tokenData string, public *jwt.Claims, private *PrivateClaims) (*apiserverserviceaccount.ServiceAccountInfo, error)
}

// AuthenticateToken 验证 JWT Token 并返回认证结果。
// 流程：检查 issuer → 解析 JWT → 按 keyID 选择公钥 → 验证签名 → 验证 audience → 调用 Validator。
// 返回 (response, true, nil) 表示认证成功；(nil, false, nil) 表示不是此认证器处理的 Token。
func (j *jwtTokenAuthenticator[PrivateClaims]) AuthenticateToken(ctx context.Context, tokenData string) (*authenticator.Response, bool, error) {
	if !j.hasCorrectIssuer(tokenData) {
		return nil, false, nil
	}

	tok, err := jwt.ParseSigned(tokenData)
	if err != nil {
		return nil, false, nil
	}

	public := &jwt.Claims{}
	private := new(PrivateClaims)

	// Pick the key that has the same key ID as `tok`, if one exists.
	var kid string
	for _, header := range tok.Headers {
		if header.KeyID != "" {
			kid = header.KeyID
			break
		}
	}

	var (
		found   bool
		errlist []error
	)
	keys := j.keysGetter.GetPublicKeys(ctx, kid)
	if len(keys) == 0 {
		return nil, false, fmt.Errorf("invalid signature, no keys found")
	}
	for _, key := range keys {
		if err := tok.Claims(key.PublicKey, public, private); err != nil {
			errlist = append(errlist, err)
			continue
		}
		found = true
		break
	}

	if !found {
		return nil, false, utilerrors.NewAggregate(errlist)
	}

	// sanity check issuer since we parsed it out before signature validation
	if !j.issuers[public.Issuer] {
		return nil, false, fmt.Errorf("token issuer %q is invalid", public.Issuer)
	}

	tokenAudiences := authenticator.Audiences(public.Audience)
	if len(tokenAudiences) == 0 {
		// only apiserver audiences are allowed for legacy tokens
		audit.AddAuditAnnotation(ctx, "authentication.k8s.io/legacy-token", public.Subject)
		legacyTokensTotal.WithContext(ctx).Inc()
		tokenAudiences = j.implicitAuds
	}

	requestedAudiences, ok := authenticator.AudiencesFrom(ctx)
	if !ok {
		// default to apiserver audiences
		requestedAudiences = j.implicitAuds
	}

	auds := authenticator.Audiences(tokenAudiences).Intersect(requestedAudiences)
	if len(auds) == 0 && len(j.implicitAuds) != 0 {
		return nil, false, fmt.Errorf("token audiences %q is invalid for the target audiences %q", tokenAudiences, requestedAudiences)
	}

	// If we get here, we have a token with a recognized signature and
	// issuer string.
	sa, err := j.validator.Validate(ctx, tokenData, public, private)
	if err != nil {
		return nil, false, err
	}

	return &authenticator.Response{
		User:      sa.UserInfo(),
		Audiences: auds,
	}, true, nil
}

// hasCorrectIssuer returns true if tokenData is a valid JWT in compact
// serialization format and the "iss" claim matches the iss field of this token
// authenticator, and otherwise returns false.
//
// Note: go-jose currently does not allow access to unverified JWS payloads.
// See https://github.com/square/go-jose/issues/169
func (j *jwtTokenAuthenticator[PrivateClaims]) hasCorrectIssuer(tokenData string) bool {
	if strings.HasPrefix(strings.TrimSpace(tokenData), "{") {
		return false
	}
	parts := strings.Split(tokenData, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	claims := struct {
		// WARNING: this JWT is not verified. Do not trust these claims.
		Issuer string `json:"iss"`
	}{}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return false
	}
	return j.issuers[claims.Issuer]
}

// GenerateToken is shared between internal and external signer code to ensure that claim merging logic remains consistent between them.
func GenerateToken(signer jose.Signer, iss string, claims *jwt.Claims, privateClaims interface{}) (string, error) {
	// claims are applied in reverse precedence
	return jwt.Signed(signer).
		Claims(privateClaims).
		Claims(claims).
		Claims(&jwt.Claims{
			Issuer: iss,
		}).
		CompactSerialize()
}
