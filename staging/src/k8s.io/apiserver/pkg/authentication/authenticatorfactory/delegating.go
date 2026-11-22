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

package authenticatorfactory

import (
	"errors"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/group"
	"k8s.io/apiserver/pkg/authentication/request/anonymous"
	"k8s.io/apiserver/pkg/authentication/request/bearertoken"
	"k8s.io/apiserver/pkg/authentication/request/headerrequest"
	unionauth "k8s.io/apiserver/pkg/authentication/request/union"
	"k8s.io/apiserver/pkg/authentication/request/websocket"
	"k8s.io/apiserver/pkg/authentication/request/x509"
	"k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	webhooktoken "k8s.io/apiserver/plugin/pkg/authenticator/token/webhook"
	authenticationclient "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// DelegatingAuthenticatorConfig is the minimal configuration needed to create an authenticator
// built to delegate authentication to a kube API server
// DelegatingAuthenticatorConfig 汇总可选的认证方式；New 按固定优先级把它们
// 组合成单一 authenticator.Request。
type DelegatingAuthenticatorConfig struct {
	Anonymous *apiserver.AnonymousAuthConfig

	// TokenAccessReviewClient is a client to do token review. It can be nil. Then every token is ignored.
	TokenAccessReviewClient authenticationclient.AuthenticationV1Interface

	// TokenAccessReviewTimeout specifies a time limit for requests made by the authorization webhook client.
	TokenAccessReviewTimeout time.Duration

	// WebhookRetryBackoff specifies the backoff parameters for the authentication webhook retry logic.
	// This allows us to configure the sleep time at each iteration and the maximum number of retries allowed
	// before we fail the webhook call in order to limit the fan out that ensues when the system is degraded.
	WebhookRetryBackoff *wait.Backoff

	// CacheTTL is the length of time that a token authentication answer will be cached.
	CacheTTL time.Duration

	// CAContentProvider are the options for verifying incoming connections using mTLS and directly assigning to users.
	// Generally this is the CA bundle file used to authenticate client certificates
	// If this is nil, then mTLS will not be used.
	ClientCertificateCAContentProvider dynamiccertificates.CAContentProvider

	APIAudiences authenticator.Audiences

	RequestHeaderConfig *RequestHeaderConfig
}

// New 按“front-proxy -> 客户端证书 -> bearer token”的顺序组装认证链，
// 并返回供 OpenAPI 描述的 securityDefinitions。任一步骤配置错误都会整体失败，
// 避免部分认证方式静默缺席。
func (c DelegatingAuthenticatorConfig) New() (authenticator.Request, *spec.SecurityDefinitions, error) {
	authenticators := []authenticator.Request{}
	securityDefinitions := spec.SecurityDefinitions{}

	// front-proxy first, then remote
	// Add the front proxy authenticator if requested
	// 请求头认证必须放在最前：front-proxy 已在 TLS 层验证过客户端，后续认证器
	// 不应再覆盖其身份判断。
	if c.RequestHeaderConfig != nil {
		requestHeaderAuthenticator := headerrequest.NewDynamicVerifyOptionsSecure(
			c.RequestHeaderConfig.CAContentProvider.VerifyOptions,
			c.RequestHeaderConfig.AllowedClientNames,
			c.RequestHeaderConfig.UsernameHeaders,
			c.RequestHeaderConfig.UIDHeaders,
			c.RequestHeaderConfig.GroupHeaders,
			c.RequestHeaderConfig.ExtraHeaderPrefixes,
		)
		authenticators = append(authenticators, requestHeaderAuthenticator)
	}

	// x509 client cert auth
	if c.ClientCertificateCAContentProvider != nil {
		authenticators = append(authenticators, x509.NewDynamic(c.ClientCertificateCAContentProvider.VerifyOptions, x509.CommonNameUserConversion))
	}

	if c.TokenAccessReviewClient != nil {
		RegisterMetrics()
		// 未配置重试退避时直接报错，而不是使用隐式默认，防止生产环境
		// 在 webhook 故障时产生不可控重试风暴。
		if c.WebhookRetryBackoff == nil {
			return nil, nil, errors.New("retry backoff parameters for delegating authentication webhook has not been specified")
		}
		tokenAuth, err := webhooktoken.NewFromInterface(c.TokenAccessReviewClient, c.APIAudiences, *c.WebhookRetryBackoff, c.TokenAccessReviewTimeout, webhooktoken.AuthenticatorMetrics{
			RecordRequestTotal:   RecordRequestTotal,
			RecordRequestLatency: RecordRequestLatency,
		})
		if err != nil {
			return nil, nil, err
		}
		// 正向与负向结果同 TTL 缓存，限制对 TokenReview webhook 的调用频率。
		cachingTokenAuth := cache.New(tokenAuth, false, c.CacheTTL, c.CacheTTL)
		// Bearer 头与 WebSocket 子协议中的 token 共用同一认证器；后者服务于
		// kubectl exec 等基于 WebSocket 的场景。
		authenticators = append(authenticators, bearertoken.New(cachingTokenAuth), websocket.NewProtocolAuthenticator(cachingTokenAuth))

		securityDefinitions["BearerToken"] = &spec.SecurityScheme{
			SecuritySchemeProps: spec.SecuritySchemeProps{
				Type:        "apiKey",
				Name:        "authorization",
				In:          "header",
				Description: "Bearer Token authentication",
			},
		}
	}

	// 没有任何显式认证器时才允许纯匿名；否则匿名只能作为兜底追加。
	if len(authenticators) == 0 {
		if c.Anonymous != nil && c.Anonymous.Enabled {
			return anonymous.NewAuthenticator(c.Anonymous.Conditions), &securityDefinitions, nil
		}
		return nil, nil, errors.New("no authentication method configured")
	}

	// 为认证成功的用户补上 system:authenticated 组，这是 RBAC 等授权策略的
	// 基础假设。
	authenticator := group.NewAuthenticatedGroupAdder(unionauth.New(authenticators...))
	if c.Anonymous != nil && c.Anonymous.Enabled {
		// FailOnError 保证前面认证器的后端错误不会被匿名认证“兜底”成匿名用户。
		authenticator = unionauth.NewFailOnError(authenticator, anonymous.NewAuthenticator(c.Anonymous.Conditions))
	}
	return authenticator, &securityDefinitions, nil
}
