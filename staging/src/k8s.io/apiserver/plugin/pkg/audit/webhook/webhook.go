/*
Copyright 2017 The Kubernetes Authors.

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

// Package webhook implements the audit.Backend interface using HTTP webhooks.
package webhook

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/apis/audit/install"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/util/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/tracing"
)

const (
	// PluginName is the name of this plugin, to be used in help and logs.
	PluginName = "webhook"

	// DefaultInitialBackoffDelay is the default amount of time to wait before
	// retrying sending audit events through a webhook.
	DefaultInitialBackoffDelay = 10 * time.Second
)

func init() {
	install.Install(audit.Scheme)
}

// retryOnError enforces the webhook client to retry requests
// on error regardless of its nature.
// The default implementation considers a very limited set of
// 'retriable' errors, assuming correct use of HTTP codes by
// external webhooks.
// That may easily lead to dropped audit events. In fact, there is
// hardly any error that could be a justified reason NOT to retry
// sending audit events if there is even a slight chance that the
// receiving service gets back to normal at some point.
// 中文说明：审计事件丢失通常不可恢复，因此这里采取「只要出错就重试」的策略，
// 与默认只重试少量状态码的行为相反。代价是持续失败时会占用重试预算，
// 但审计完整性的优先级高于资源节省。
func retryOnError(err error) bool {
	if err != nil {
		return true
	}
	return false
}

// loadWebhook 从 kubeconfig 构造通用 webhook 客户端，并覆写重试策略。
// 中文说明：customDial 允许注入自定义拨号（例如代理或测试中的本地监听），
// 保持与其它 webhook 客户端一致的连接语义。
func loadWebhook(configFile string, groupVersion schema.GroupVersion, retryBackoff wait.Backoff, customDial utilnet.DialFunc) (*webhook.GenericWebhook, error) {
	clientConfig, err := webhook.LoadKubeconfig(configFile, customDial)
	if err != nil {
		return nil, err
	}
	w, err := webhook.NewGenericWebhook(audit.Scheme, audit.Codecs, clientConfig,
		[]schema.GroupVersion{groupVersion}, retryBackoff)
	if err != nil {
		return nil, err
	}

	w.ShouldRetry = retryOnError
	return w, nil
}

// backend 是 audit.Backend 的 HTTP 实现；name 用于日志与错误归属。
type backend struct {
	w    *webhook.GenericWebhook
	name string
}

// NewDynamicBackend returns an audit backend configured from a REST client that
// sends events over HTTP to an external service.
//
// 中文说明：动态后端由动态审计配置构造，REST client 已由上层完成认证/限流配置，
// 此处只需注入重试策略。名称加 dynamic_ 前缀以便在日志中区分静态配置的后端。
func NewDynamicBackend(rc *rest.RESTClient, retryBackoff wait.Backoff) audit.Backend {
	return &backend{
		w: &webhook.GenericWebhook{
			RestClient:   rc,
			RetryBackoff: retryBackoff,
			ShouldRetry:  retryOnError,
		},
		name: fmt.Sprintf("dynamic_%s", PluginName),
	}
}

// NewBackend returns an audit backend that sends events over HTTP to an external service.
func NewBackend(kubeConfigFile string, groupVersion schema.GroupVersion, retryBackoff wait.Backoff, customDial utilnet.DialFunc) (audit.Backend, error) {
	w, err := loadWebhook(kubeConfigFile, groupVersion, retryBackoff, customDial)
	if err != nil {
		return nil, err
	}
	return &backend{w: w, name: PluginName}, nil
}

// Run 无需后台协程：事件在 ProcessEvents 中同步发送，故立即返回 nil。
func (b *backend) Run(stopCh <-chan struct{}) error {
	return nil
}

// Shutdown 无事可做：没有需要关闭的本地资源，连接由 REST client 自行管理。
func (b *backend) Shutdown() {
	// nothing to do here
}

// ProcessEvents 批量发送事件；失败时通过 audit.HandlePluginError 记录并返回 false，
// 由审计框架决定是否丢弃该批事件。
func (b *backend) ProcessEvents(ev ...*auditinternal.Event) bool {
	if err := b.processEvents(ev...); err != nil {
		audit.HandlePluginError(b.String(), err, ev...)
		return false
	}
	return true
}

// processEvents 把事件打包成 EventList 一次性 POST，利用指数退避重试。
// 中文说明：批量发送减少往返次数；trace 的结束阈值按事件数线性放宽，
// 因为单个事件包含嵌套的请求/响应对象，序列化开销随事件数增长。
// 阈值定得较宽是为了避免把正常的大批量发送误报为慢调用。
func (b *backend) processEvents(ev ...*auditinternal.Event) error {
	var list auditinternal.EventList
	for _, e := range ev {
		list.Items = append(list.Items, *e)
	}
	return b.w.WithExponentialBackoff(context.Background(), func() rest.Result {
		ctx, span := tracing.Start(context.Background(), "Call Audit Events webhook",
			attribute.String("name", b.name),
			attribute.Int("event-count", len(list.Items)),
		)
		// Only log audit webhook traces that exceed a 25ms per object limit plus a 50ms
		// request overhead allowance. The high per object limit used here is primarily to
		// allow enough time for the serialization/deserialization of audit events, which
		// contain nested request and response objects plus additional event fields.
		defer span.End(time.Duration(50+25*len(list.Items)) * time.Millisecond)
		return b.w.RestClient.Post().Body(&list).Do(ctx)
	}).Error()
}

// String 返回后端名，用于审计错误信息与日志标识。
func (b *backend) String() string {
	return b.name
}
