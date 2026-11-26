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

// Package webhook 用 HTTP webhook 实现 authenticator.Token 接口。
package webhook

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	authenticationv1beta1 "k8s.io/api/authentication/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/kubernetes/scheme"
	authenticationv1client "k8s.io/client-go/kubernetes/typed/authentication/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// DefaultRetryBackoff 返回 webhook 重试的默认退避参数。
//
// 认证是每个请求的必经路径，重试窗口因此较短：只在短暂抖动时补救，
// 不让不可用的 webhook 长时间拖住请求。
func DefaultRetryBackoff() *wait.Backoff {
	backoff := webhook.DefaultRetryBackoffWithInitialDelay(500 * time.Millisecond)
	return &backoff
}

// 确保 WebhookTokenAuthenticator 实现 authenticator.Token 接口。
var _ authenticator.Token = (*WebhookTokenAuthenticator)(nil)

// tokenReviewer 抽象 TokenReview 的创建，同时返回 HTTP 状态码用于指标。
// 之所以要状态码，是因为错误本身不足以区分"服务端 5xx"与"网络失败"。
type tokenReviewer interface {
	Create(ctx context.Context, review *authenticationv1.TokenReview, _ metav1.CreateOptions) (*authenticationv1.TokenReview, int, error)
}

// WebhookTokenAuthenticator 通过向外部 webhook 发送 TokenReview 来校验 bearer token。
// 无状态且并发安全，可在多个请求间共享；建议外层再套 token cache。
type WebhookTokenAuthenticator struct {
	tokenReview    tokenReviewer
	retryBackoff   wait.Backoff
	implicitAuds   authenticator.Audiences
	requestTimeout time.Duration
	metrics        AuthenticatorMetrics
}

// NewFromInterface 用给定 tokenReview client 创建 webhook 认证器。
//
// k8s.io/apiserver/pkg/authentication/token/cache.
func NewFromInterface(tokenReview authenticationv1client.AuthenticationV1Interface, implicitAuds authenticator.Audiences, retryBackoff wait.Backoff, requestTimeout time.Duration, metrics AuthenticatorMetrics) (*WebhookTokenAuthenticator, error) {
	tokenReviewClient := &tokenReviewV1Client{tokenReview.RESTClient()}
	return newWithBackoff(tokenReviewClient, retryBackoff, implicitAuds, requestTimeout, metrics)
}

// k8s.io/apiserver/pkg/authentication/token/cache.
func New(config *rest.Config, version string, implicitAuds authenticator.Audiences, retryBackoff wait.Backoff) (*WebhookTokenAuthenticator, error) {
	tokenReview, err := tokenReviewInterfaceFromConfig(config, version, retryBackoff)
	if err != nil {
		return nil, err
	}
	return newWithBackoff(tokenReview, retryBackoff, implicitAuds, time.Duration(0), AuthenticatorMetrics{
		RecordRequestTotal:   noopMetrics{}.RequestTotal,
		RecordRequestLatency: noopMetrics{}.RequestLatency,
	})
}

// newWithBackoff 让测试可以跳过 sleep。
//
// 直接构造结构体而不做校验：调用方已保证参数合法，测试则可注入极小的 backoff。
func newWithBackoff(tokenReview tokenReviewer, retryBackoff wait.Backoff, implicitAuds authenticator.Audiences, requestTimeout time.Duration, metrics AuthenticatorMetrics) (*WebhookTokenAuthenticator, error) {
	return &WebhookTokenAuthenticator{
		tokenReview,
		retryBackoff,
		implicitAuds,
		requestTimeout,
		metrics,
	}, nil
}

// AuthenticateToken 实现 authenticator.Token 接口。
func (w *WebhookTokenAuthenticator) AuthenticateToken(ctx context.Context, token string) (*authenticator.Response, bool, error) {
	// 在构造 WebhookTokenAuthenticator 时记录 apiserver 的隐式受众。
	//
	wantAuds, checkAuds := authenticator.AudiencesFrom(ctx)
	r := &authenticationv1.TokenReview{
		Spec: authenticationv1.TokenReviewSpec{
			Token:     token,
			Audiences: wantAuds,
		},
	}
	var (
		result *authenticationv1.TokenReview
		auds   authenticator.Audiences
		cancel context.CancelFunc
	)

	// 定义了硬超时就设置它
	// 子 context 期限更短则先到期，
	// 否则父 context 先到期并传播给子 context
	if w.requestTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, w.requestTimeout)
		defer cancel()
	}

	// WithExponentialBackoff 会返回 tokenreview 创建错误（若有）。
	if err := webhook.WithExponentialBackoff(ctx, w.retryBackoff, func() error {
		var tokenReviewErr error
		var statusCode int

		start := time.Now()
		result, statusCode, tokenReviewErr = w.tokenReview.Create(ctx, r, metav1.CreateOptions{})
		latency := time.Since(start)

		// 有状态码时用状态码作标签；连 HTTP 响应都没拿到（如网络错误）
		// 则记为 "<error>"，两者语义不同，不能混用。
		if statusCode != 0 {
			w.metrics.RecordRequestTotal(ctx, strconv.Itoa(statusCode))
			w.metrics.RecordRequestLatency(ctx, strconv.Itoa(statusCode), latency.Seconds())
			return tokenReviewErr
		}

		if tokenReviewErr != nil {
			w.metrics.RecordRequestTotal(ctx, "<error>")
			w.metrics.RecordRequestLatency(ctx, "<error>", latency.Seconds())
		}
		return tokenReviewErr
	}, webhook.DefaultShouldRetry); err != nil {
		// 此处出错意味着配置错误或服务不可用，记录日志以便排查。
		klog.Errorf("Failed to make webhook authenticator request: %v", err)
		return nil, false, err
	}

	// 只有在调用方限制了 audience 时才做交集校验；未限制时信任 webhook 的结果。
	if checkAuds {
		gotAuds := w.implicitAuds
		if len(result.Status.Audiences) > 0 {
			gotAuds = result.Status.Audiences
		}
		auds = wantAuds.Intersect(gotAuds)
		// 交集为空表示 token 不属于任何被请求的 audience，按未认证处理。
		if len(auds) == 0 {
			return nil, false, nil
		}
	}

	r.Status = result.Status
	if !r.Status.Authenticated {
		// webhook 明确拒绝时，把它的 Error 透出给调用方（通常是 401 的原因）。
		var err error
		if len(r.Status.Error) != 0 {
			err = errors.New(r.Status.Error)
		}
		return nil, false, err
	}

	// 复制 Extra，避免把 webhook 响应中的切片直接暴露给下游修改。
	var extra map[string][]string
	if r.Status.User.Extra != nil {
		extra = map[string][]string{}
		for k, v := range r.Status.User.Extra {
			extra[k] = v
		}
	}

	return &authenticator.Response{
		User: &user.DefaultInfo{
			Name:   r.Status.User.Username,
			UID:    r.Status.User.UID,
			Groups: r.Status.User.Groups,
			Extra:  extra,
		},
		Audiences: auds,
	}, true, nil
}

// tokenReviewInterfaceFromConfig 从指定 kubeconfig 构建 client，
// 并返回使用该 client 的 TokenReviewInterface。注意它按 kubeconfig 中的路径
//
// 通过版本切换选择具体的客户端实现：v1 直接使用 client-go 客户端，v1beta1
// 则走通用 webhook 并做字段转换，以兼容尚未升级的旧 webhook 服务端。
func tokenReviewInterfaceFromConfig(config *rest.Config, version string, retryBackoff wait.Backoff) (tokenReviewer, error) {
	localScheme := runtime.NewScheme()
	if err := scheme.AddToScheme(localScheme); err != nil {
		return nil, err
	}

	switch version {
	case authenticationv1.SchemeGroupVersion.Version:
		// 单一版本的 scheme 让通用 webhook 只按该版本序列化请求。
		groupVersions := []schema.GroupVersion{authenticationv1.SchemeGroupVersion}
		if err := localScheme.SetVersionPriority(groupVersions...); err != nil {
			return nil, err
		}
		gw, err := webhook.NewGenericWebhook(localScheme, scheme.Codecs, config, groupVersions, retryBackoff)
		if err != nil {
			return nil, err
		}
		return &tokenReviewV1ClientGW{gw.RestClient}, nil

	case authenticationv1beta1.SchemeGroupVersion.Version:
		groupVersions := []schema.GroupVersion{authenticationv1beta1.SchemeGroupVersion}
		if err := localScheme.SetVersionPriority(groupVersions...); err != nil {
			return nil, err
		}
		gw, err := webhook.NewGenericWebhook(localScheme, scheme.Codecs, config, groupVersions, retryBackoff)
		if err != nil {
			return nil, err
		}
		return &tokenReviewV1beta1ClientGW{gw.RestClient}, nil

	default:
		return nil, fmt.Errorf(
			"unsupported authentication webhook version %q, supported versions are %q, %q",
			version,
			authenticationv1.SchemeGroupVersion.Version,
			authenticationv1beta1.SchemeGroupVersion.Version,
		)
	}

}

// tokenReviewV1Client 使用 client-go 的类型化客户端访问 tokenreviews 资源。
type tokenReviewV1Client struct {
	client rest.Interface
}

// Create 接收 tokenReview 表示并创建它，返回 server 的 tokenReview 表示、HTTP 状态码与错误（若有）。
func (c *tokenReviewV1Client) Create(ctx context.Context, tokenReview *authenticationv1.TokenReview, opts metav1.CreateOptions) (result *authenticationv1.TokenReview, statusCode int, err error) {
	result = &authenticationv1.TokenReview{}

	restResult := c.client.Post().
		Resource("tokenreviews").
		VersionedParams(&opts, scheme.ParameterCodec).
		Body(tokenReview).
		Do(ctx)

	restResult.StatusCode(&statusCode)
	err = restResult.Into(result)
	return
}

// 通用 webhook 使用的 tokenReviewV1ClientGW 不指定 GVR。
type tokenReviewV1ClientGW struct {
	client rest.Interface
}

// Create 接收 tokenReview 表示并创建它，返回 server 的 tokenReview 表示、HTTP 状态码与错误（若有）。
func (c *tokenReviewV1ClientGW) Create(ctx context.Context, tokenReview *authenticationv1.TokenReview, opts metav1.CreateOptions) (result *authenticationv1.TokenReview, statusCode int, err error) {
	result = &authenticationv1.TokenReview{}

	restResult := c.client.Post().
		Body(tokenReview).
		Do(ctx)

	restResult.StatusCode(&statusCode)
	err = restResult.Into(result)
	return
}

// 通用 webhook 使用的 tokenReviewV1beta1ClientGW 不指定 GVR。
//
// 对上层仍暴露 v1 类型，内部转换为 v1beta1 收发，从而让认证器代码无需感知版本差异。
type tokenReviewV1beta1ClientGW struct {
	client rest.Interface
}

// Create 把 v1 请求转换为 v1beta1 发送，再把响应状态转回 v1。
// 注意返回的 review 就是传入对象（就地写入 Status），与 v1 客户端新建对象的行为不同。
func (t *tokenReviewV1beta1ClientGW) Create(ctx context.Context, review *authenticationv1.TokenReview, _ metav1.CreateOptions) (*authenticationv1.TokenReview, int, error) {
	var statusCode int
	v1beta1Review := &authenticationv1beta1.TokenReview{Spec: v1SpecToV1beta1Spec(&review.Spec)}
	v1beta1Result := &authenticationv1beta1.TokenReview{}

	restResult := t.client.Post().Body(v1beta1Review).Do(ctx)
	restResult.StatusCode(&statusCode)
	err := restResult.Into(v1beta1Result)
	if err != nil {
		return nil, statusCode, err
	}
	review.Status = v1beta1StatusToV1Status(&v1beta1Result.Status)
	return review, statusCode, nil
}

// v1SpecToV1beta1Spec 只复制两个版本共有的字段。
func v1SpecToV1beta1Spec(in *authenticationv1.TokenReviewSpec) authenticationv1beta1.TokenReviewSpec {
	return authenticationv1beta1.TokenReviewSpec{
		Token:     in.Token,
		Audiences: in.Audiences,
	}
}

// v1beta1StatusToV1Status 把 v1beta1 的认证结果升级为 v1 结构。
func v1beta1StatusToV1Status(in *authenticationv1beta1.TokenReviewStatus) authenticationv1.TokenReviewStatus {
	return authenticationv1.TokenReviewStatus{
		Authenticated: in.Authenticated,
		User:          v1beta1UserToV1User(in.User),
		Audiences:     in.Audiences,
		Error:         in.Error,
	}
}

// v1beta1UserToV1User 转换用户信息，并为 Extra 分配新 map 以免共享底层数据。
func v1beta1UserToV1User(u authenticationv1beta1.UserInfo) authenticationv1.UserInfo {
	var extra map[string]authenticationv1.ExtraValue
	if u.Extra != nil {
		extra = make(map[string]authenticationv1.ExtraValue, len(u.Extra))
		for k, v := range u.Extra {
			extra[k] = authenticationv1.ExtraValue(v)
		}
	}
	return authenticationv1.UserInfo{
		Username: u.Username,
		UID:      u.UID,
		Groups:   u.Groups,
		Extra:    extra,
	}
}
