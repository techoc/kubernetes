/*
Copyright 2024 The Kubernetes Authors.

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

package plugin

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	jose "gopkg.in/go-jose/go-jose.v2"
	"gopkg.in/go-jose/go-jose.v2/jwt"

	externaljwtv1 "k8s.io/externaljwt/apis/v1"
	"k8s.io/kubernetes/pkg/serviceaccount"
	externaljwtmetrics "k8s.io/kubernetes/pkg/serviceaccount/externaljwt/metrics"
)

// init 在包加载时注册 external-jwt-signer 相关的指标。
func init() {
	externaljwtmetrics.RegisterMetrics()
}

// VerificationKeys 存储从外部签名器获取的公钥集及其元数据。
type VerificationKeys struct {
	Keys            []serviceaccount.PublicKey
	DataTimestamp   time.Time
	NextRefreshHint time.Time
}

// New 创建外部 JWT 签名器插件，建立与外部签名器的 gRPC 连接。
// 通过 Unix domain socket 通信，使用 insecure 凭证（因为 Unix socket 本身提供 OS 级别的访问控制）。
// 启动时同步获取初始密钥集，然后在后台 goroutine 中定期同步。
// ctx 控制整个插件的生命周期，取消时关闭 gRPC 连接。
func New(ctx context.Context, issuer, socketPath string, keySyncTimeout time.Duration, allowSigningWithNonOIDCKeys bool) (*Plugin, *keyCache, error) {
	conn, err := grpc.Dial(
		socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithAuthority("localhost"),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithContextDialer(func(ctx context.Context, path string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", path)
		}),
		grpc.WithChainUnaryInterceptor(externaljwtmetrics.OuboundRequestMetricsInterceptor),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("while dialing grpc socket at %q: %w", socketPath, err)
	}

	plugin := newPlugin(issuer, conn, allowSigningWithNonOIDCKeys)

	initialFillCtx, cancel := context.WithTimeout(ctx, keySyncTimeout)
	defer cancel()

	if err := plugin.keyCache.initialFill(initialFillCtx); err != nil {
		return nil, nil, fmt.Errorf("while initially filling key cache: %w", err)
	}
	go plugin.keyCache.scheduleSync(ctx, keySyncTimeout)

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	return plugin, plugin.keyCache, nil
}

// Plugin 是外部 JWT 签名器插件，实现 serviceaccount.TokenGenerator 接口。
// 通过 gRPC 调用外部签名器生成签名的 JWT Token，支持 HSM 等硬件安全模块。
type Plugin struct {
	iss                         string
	client                      externaljwtv1.ExternalJWTSignerClient
	keyCache                    *keyCache
	allowSigningWithNonOIDCKeys bool
}

// newPlugin 创建外部 JWT 签名器插件的实例。
func newPlugin(iss string, conn *grpc.ClientConn, allowSigningWithNonOIDCKeys bool) *Plugin {
	client := externaljwtv1.NewExternalJWTSignerClient(conn)
	plugin := &Plugin{
		iss:                         iss,
		client:                      client,
		allowSigningWithNonOIDCKeys: allowSigningWithNonOIDCKeys,
		keyCache:                    newKeyCache(client),
	}
	return plugin
}

// GenerateToken 通过外部签名器生成签名的 ServiceAccount Token。
// 将 claims 序列化为 JWT payload，发送给外部签名器签名，然后组装为完整的 JWT。
func (p *Plugin) GenerateToken(ctx context.Context, claims *jwt.Claims, privateClaims interface{}) (string, error) {
	jwt, err := p.signAndAssembleJWT(ctx, claims, privateClaims)
	externaljwtmetrics.RecordTokenGenAttempt(err)
	return jwt, err
}

// signAndAssembleJWT 将 claims 合并为 payload，发送给外部签名器签名，然后组装为 JWT 字符串。
// 验证返回的 JWT header 的算法、keyID 和类型，确保外部签名器返回合法的签名。
func (p *Plugin) signAndAssembleJWT(ctx context.Context, claims *jwt.Claims, privateClaims interface{}) (string, error) {
	payload, err := mergeClaims(p.iss, claims, privateClaims)
	if err != nil {
		return "", fmt.Errorf("while merging claims: %w", err)
	}

	payloadBase64 := base64.RawURLEncoding.EncodeToString(payload)

	request := &externaljwtv1.SignJWTRequest{
		Claims: payloadBase64,
	}

	response, err := p.client.Sign(ctx, request)
	if err != nil {
		return "", fmt.Errorf("while signing jwt: %w", err)
	}

	if err := p.validateJWTHeader(ctx, response); err != nil {
		return "", fmt.Errorf("while validating header: %w", err)
	}

	if len(response.Signature) == 0 {
		return "", fmt.Errorf("empty signature returned")
	}

	return response.Header + "." + payloadBase64 + "." + response.Signature, nil
}

// GetServiceMetadata 获取外部签名器的元数据，如支持的最大 Token 生命周期等。
func (p *Plugin) GetServiceMetadata(ctx context.Context) (*externaljwtv1.MetadataResponse, error) {
	req := &externaljwtv1.MetadataRequest{}
	return p.client.Metadata(ctx, req)
}

// validateJWTHeader 验证外部签名器返回的 JWT header。
// 检查 typ 必须是 "JWT"，kid 不能为空且不超过 1KB，算法必须是 RS256/ES256/ES384/ES512。
// 如果 allowSigningWithNonOIDCKeys 为 false，还检查使用的密钥没有被排除在 OIDC 发现之外。
func (p *Plugin) validateJWTHeader(ctx context.Context, response *externaljwtv1.SignJWTResponse) error {
	jsonBytes, err := base64.RawURLEncoding.DecodeString(response.Header)
	if err != nil {
		return fmt.Errorf("while unwrapping header: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewBuffer(jsonBytes))
	decoder.DisallowUnknownFields()

	header := &struct {
		Algorithm string `json:"alg,omitempty"`
		KeyID     string `json:"kid,omitempty"`
		Type      string `json:"typ,omitempty"`
	}{}

	if err := decoder.Decode(header); err != nil {
		return fmt.Errorf("while parsing header JSON: %w", err)
	}

	if header.Type != "JWT" {
		return fmt.Errorf("bad type")
	}
	if len(header.KeyID) == 0 {
		return fmt.Errorf("key id missing")
	}
	if len(header.KeyID) > 1024 {
		return fmt.Errorf("key id longer than 1 kb")
	}
	switch header.Algorithm {
	// IMPORTANT: The algorithms listed below must be kept in sync with:
	// - pkg/serviceaccount/externaljwt/plugin/plugin.go validateJWTHeader
	// - pkg/serviceaccount/jwt.go signerFromRSAPrivateKey
	// - pkg/serviceaccount/jwt.go signerFromECDSAPrivateKey
	// - test/images/agnhost/openidmetadata/openidmetadata.go validate SupportedSigningAlgs
	case "RS256", "ES256", "ES384", "ES512":
		// OK
	default:
		return fmt.Errorf("bad signing algorithm %q", header.Algorithm)
	}

	if !p.allowSigningWithNonOIDCKeys {
		publicKeys := p.keyCache.GetPublicKeys(ctx, header.KeyID)
		for _, key := range publicKeys {
			// Such keys shall only be used for validating formerly issued tokens.
			if key.ExcludeFromOIDCDiscovery {
				return fmt.Errorf("key used for signing JWT (kid: %s) is excluded from OIDC discovery docs", header.KeyID)
			}
		}
	}

	return nil
}

func mergeClaims(iss string, claims *jwt.Claims, privateClaims interface{}) ([]byte, error) {
	var out []byte
	signer := payloadGrabber(func(payload []byte) { out = payload })
	_, err := serviceaccount.GenerateToken(signer, iss, claims, privateClaims)
	if len(out) == 0 {
		return nil, fmt.Errorf("failed to marshal: %w", err)
	}
	return out, nil // error is safe to ignore as long as we have the payload bytes
}

var _ jose.Signer = payloadGrabber(nil)

type payloadGrabber func(payload []byte)

func (p payloadGrabber) Sign(payload []byte) (*jose.JSONWebSignature, error) {
	p(payload)
	return nil, jose.ErrUnprotectedNonce // return some error to stop after we have the payload
}

func (p payloadGrabber) Options() jose.SignerOptions { return jose.SignerOptions{} }
