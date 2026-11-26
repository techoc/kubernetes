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

// Package webhook implements a generic HTTP webhook plugin.
package webhook

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/x509metrics"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// 所有 webhook 请求的绝对超时，包含读取响应体的时间。
const defaultRequestTimeout = 30 * time.Second

// DefaultRetryBackoffWithInitialDelay 返回基于给定初始延迟的 webhook 重试默认退避参数；仅定制初始延迟的客户端可直接使用。重试策略与认证/授权 webhook 保持一致：5 步、1.5 倍增长、20% 抖动，抑制同步重试风暴。
func DefaultRetryBackoffWithInitialDelay(initialBackoffDelay time.Duration) wait.Backoff {
	return wait.Backoff{
		Duration: initialBackoffDelay,
		Factor:   1.5,
		Jitter:   0.2,
		Steps:    5,
	}
}

// GenericWebhook 定义了带有常用能力（如请求重试）的通用 webhook 客户端。
// 字段可被调用方覆盖以定制重试策略；零值 ShouldRetry 会回退到 DefaultShouldRetry。
type GenericWebhook struct {
	RestClient   *rest.RESTClient
	RetryBackoff wait.Backoff
	ShouldRetry  func(error) bool
}

// DefaultShouldRetry 是 GenericWebhook 的 ShouldRetry 属性函数的默认实现：若错误原因为网络问题（连接重置）或 HTTP 问题（InternalServerError (500)、GatewayTimeout (504)、TooManyRequests (429)），或 apierrors.SuggestsClientDelay() 返回 true，则建议重试；否则返回 false，立即失败。
// 只重试明确可恢复的错误：4xx（除 429）等确定性失败重试无益，反而放大负载。
func DefaultShouldRetry(err error) bool {
	// 这些错误属于瞬时故障，值得重试。
	if utilnet.IsConnectionReset(err) || utilnet.IsHTTP2ConnectionLost(err) || apierrors.IsInternalError(err) || apierrors.IsTimeout(err) || apierrors.IsTooManyRequests(err) {
		return true
	}
	// 错误带有 Retry-After 头时，视为服务端明确要求重试。
	if _, shouldRetry := apierrors.SuggestsClientDelay(err); shouldRetry {
		return true
	}
	return false
}

// 启动时校验 scheme 是否已注册所需 groupVersion，尽早暴露装配错误，
// 否则首个 webhook 调用才会失败且错误信息不够直接。
func NewGenericWebhook(scheme *runtime.Scheme, codecFactory serializer.CodecFactory, config *rest.Config, groupVersions []schema.GroupVersion, retryBackoff wait.Backoff) (*GenericWebhook, error) {
	for _, groupVersion := range groupVersions {
		if !scheme.IsVersionRegistered(groupVersion) {
			return nil, fmt.Errorf("webhook plugin requires enabling extension resource: %s", groupVersion)
		}
	}

	clientConfig := rest.CopyConfig(config)

	codec := codecFactory.LegacyCodec(groupVersions...)
	clientConfig.ContentType = runtime.ContentTypeJSON
	clientConfig.ContentConfig.NegotiatedSerializer = serializer.NegotiatedSerializerWrapper(runtime.SerializerInfo{Serializer: codec})

	// 包一层证书指标：webhook 后端使用缺失 SAN 或 SHA-1 证书时给出可观测的告警。
	clientConfig.Wrap(x509metrics.NewDeprecatedCertificateRoundTripperWrapperConstructor(
		x509MissingSANCounter,
		x509InsecureSHA1Counter,
	))

	restClient, err := rest.UnversionedRESTClientFor(clientConfig)
	if err != nil {
		return nil, err
	}

	return &GenericWebhook{restClient, retryBackoff, DefaultShouldRetry}, nil
}

// 按给定退避参数重试 webhookFn：仅当 ShouldRetry 判定错误可重试时才重试，
// 未配置时采用 DefaultShouldRetry。
func (g *GenericWebhook) WithExponentialBackoff(ctx context.Context, webhookFn func() rest.Result) rest.Result {
	var result rest.Result
	shouldRetry := g.ShouldRetry
	if shouldRetry == nil {
		shouldRetry = DefaultShouldRetry
	}
	WithExponentialBackoff(ctx, g.RetryBackoff, func() error {
		result = webhookFn()
		return result.Error()
	}, shouldRetry)
	return result
}

// 当 shouldRetry 判定错误可重试时，最多重试 webhookFn 5 次，退避时间指数增长。
func WithExponentialBackoff(ctx context.Context, retryBackoff wait.Backoff, webhookFn func() error, shouldRetry func(error) bool) error {
	// 保存最近一次真实的 webhook 错误，便于请求随后被取消或超时时仍可报告原因。
	var webhookErr error
	err := wait.ExponentialBackoffWithContext(ctx, retryBackoff, func(_ context.Context) (bool, error) {
		webhookErr = webhookFn()
		if shouldRetry(webhookErr) {
			return false, nil
		}
		if webhookErr != nil {
			return false, webhookErr
		}
		return true, nil
	})

	switch {
	// 优先检查 webhookErr——若其存在，它才是最需要返回的错误：上下文取消/超时时 wait 返回的错误信息较笼统，优先返回真实的 webhook 错误更有诊断价值。
	case webhookErr != nil:
		return webhookErr
	case err != nil:
		return fmt.Errorf("webhook call failed: %s", err.Error())
	default:
		return nil
	}
}

// LoadKubeconfig 从 kubeconfig 文件构造 webhook 客户端配置。
// 与委托认证的客户端不同，这里禁用客户端限流（QPS=-1），因为限流应在请求入口处统一实施，
// 在 webhook 客户端再次限流会造成重复排队并放大延迟。
func LoadKubeconfig(kubeConfigFile string, customDial utilnet.DialFunc) (*rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeConfigFile
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})

	clientConfig, err := loader.ClientConfig()
	if err != nil {
		return nil, err
	}

	clientConfig.Dial = customDial

	// kubeconfig 无法设置超时，只能通过命令行 flag 指定。
	//
	// https://github.com/kubernetes/client-go/blob/master/tools/clientcmd/overrides.go
	//
	// 将其设为一个合理的值，避免对 webhook 的请求永远挂起：这是包含读取响应体在内的绝对超时，防止异常后端无限占用连接。
	clientConfig.Timeout = defaultRequestTimeout

	// 客户端不限制调用 webhook 后端的速率；限流应在决定接受多少请求时完成。
	clientConfig.QPS = -1

	return clientConfig, nil
}
