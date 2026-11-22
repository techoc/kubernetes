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

package routes

import (
	"fmt"
	"net/http"

	restful "github.com/emicklei/go-restful/v3"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

// 此代码放在 routes 包中，而非 serviceaccount 包，是因为许多控制器依赖 pkg/serviceaccount，
// 但 import-boss 不允许它们直接依赖 go-restful。因此将所有与 HTTP 路由相关的代码集中在此包，
// serviceaccount 包只负责密钥逻辑，返回渲染好的 JSON 字节，由 routes 包负责 HTTP 响应。

const (
	// headerCacheControl 是 Cache-Control 响应头的键名。
	// API Server 全局默认设置是 "private, no-cache"，但 OpenID 元数据和 JWKS 需要客户端缓存，
	// 因为密钥集不频繁变化，频繁请求会增加不必要的开销。
	headerCacheControl = "Cache-Control"

	// cacheControlTemplate 是 Cache-Control 的模板，设置为 public 并指定 max-age，
	// 允许中间代理和客户端缓存响应，max-age 由 provider 动态提供。
	cacheControlTemplate = "public, max-age=%d"

	// mimeJWKS 是 JWKS（JSON Web Key Set）响应的 Content-Type。
	// 遵循 RFC7517 规范：https://tools.ietf.org/html/rfc7517#section-8.5.1
	mimeJWKS = "application/jwk-set+json"
)

// OpenIDMetadataServer 提供 KSA（Kubernetes Service Account）Token 签发者的 OpenID Connect 元数据服务。
// 它暴露两个标准 OIDC 端点：
//
//   - /.well-known/openid-configuration：OpenID 发现文档，描述签发者的元数据（如 issuer、jwks_uri 等）
//   - /openid/v1/jwks：JWKS 端点，返回用于验证 KSA Token 签名的公钥集合
//
// 客户端（如外部 OIDC 提供者或其他 Kubernetes 集群）通过这些端点验证 KSA Token 的合法性。
type OpenIDMetadataServer struct {
	// provider 提供 OpenID 元数据和 JWKS 的 JSON 内容，实际密钥管理逻辑在 pkg/serviceaccount 中。
	provider serviceaccount.OpenIDMetadataProvider
}

// NewOpenIDMetadataServer 创建一个新的 OpenIDMetadataServer。
// provider 负责生成 OpenID 配置 JSON 和 JWKS JSON，通常由 API Server 的 serviceaccount 控制器提供。
func NewOpenIDMetadataServer(provider serviceaccount.OpenIDMetadataProvider) *OpenIDMetadataServer {
	return &OpenIDMetadataServer{provider: provider}
}

// Install 将 OpenID 元数据服务的路由注册到 restful.Container 中。
// 注册两个独立的 WebService，分别对应 OpenID 发现文档和 JWKS 端点。
func (s *OpenIDMetadataServer) Install(c *restful.Container) {
	// OpenID 发现文档 WebService
	// restful.Container.Add 会检测重复的根路径并退出，因此需要为 /.well-known/openid-configuration
	// 设置独立的路径，避免与其他路由冲突。
	cfg := new(restful.WebService).
		Produces(restful.MIME_JSON)

	cfg.Path(serviceaccount.OpenIDConfigPath).Route(
		cfg.GET("").
			To(fromStandard(s.serveConfiguration)).
			Doc("get service account issuer OpenID configuration, also known as the 'OIDC discovery doc'").
			Operation("getServiceAccountIssuerOpenIDConfiguration").
			// OpenAPI 规范中通常只声明成功响应，不声明 Internal Error。
			Returns(http.StatusOK, "OK", ""))
	c.Add(cfg)

	// JWKS WebService，返回用于验证 KSA Token 的公钥集合
	jwks := new(restful.WebService).
		Produces(mimeJWKS)

	jwks.Path(serviceaccount.JWKSPath).Route(
		jwks.GET("").
			To(fromStandard(s.serveKeys)).
			Doc("get service account issuer OpenID JSON Web Key Set (contains public token verification keys)").
			Operation("getServiceAccountIssuerOpenIDKeyset").
			Returns(http.StatusOK, "OK", ""))
	c.Add(jwks)
}

// fromStandard 将标准 net/http 的 HandlerFunc 签名适配为 restful 的 RouteFunction 签名。
// 因为 serveConfiguration 和 serveKeys 使用标准 http.ResponseWriter 更简洁，
// 而 restful 框架需要 *restful.Request 和 *restful.Response 参数。
func fromStandard(h http.HandlerFunc) restful.RouteFunction {
	return func(req *restful.Request, resp *restful.Response) {
		h(resp, req.Request)
	}
}

// serveConfiguration 处理 OpenID 发现文档请求（/.well-known/openid-configuration）。
// 返回包含 issuer、jwks_uri、token_endpoint 等字段的 JSON，遵循 OpenID Connect Discovery 规范。
func (s *OpenIDMetadataServer) serveConfiguration(w http.ResponseWriter, req *http.Request) {
	// 从 provider 获取配置 JSON 和缓存时间（max-age）
	configJSON, maxAge := s.provider.GetConfigJSON()
	w.Header().Set(restful.HEADER_ContentType, restful.MIME_JSON)
	// 设置缓存头，覆盖 API Server 全局的 "private, no-cache" 默认值
	w.Header().Set(headerCacheControl, fmt.Sprintf(cacheControlTemplate, maxAge))
	if _, err := w.Write(configJSON); err != nil {
		klog.Errorf("failed to write service account issuer metadata response: %v", err)
		return
	}
}

// serveKeys 处理 JWKS 请求（/openid/v1/jwks），返回当前用于签发 KSA Token 的公钥集合。
// 客户端使用这些公钥验证 KSA Token 的签名，实现跨集群或外部系统的 Token 验证。
func (s *OpenIDMetadataServer) serveKeys(w http.ResponseWriter, req *http.Request) {
	// 从 provider 获取 JWKS JSON 和缓存时间
	keysetJSON, maxAge := s.provider.GetKeysetJSON()
	// 按 RFC7517 规范设置 Content-Type 为 application/jwk-set+json
	w.Header().Set(restful.HEADER_ContentType, mimeJWKS)
	w.Header().Set(headerCacheControl, fmt.Sprintf(cacheControlTemplate, maxAge))
	if _, err := w.Write(keysetJSON); err != nil {
		klog.Errorf("failed to write service account issuer JWKS response: %v", err)
		return
	}
}
