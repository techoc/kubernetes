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

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/capabilities"
)

// ProxyREST 实现 services/{name}/proxy：通过 Service Redirector 解析 EndpointSlice 后，将请求代理到实际后端 Pod。
type ProxyREST struct {
	Redirector     rest.Redirector
	ProxyTransport http.RoundTripper
}

// 实现 Connecter 接口。
var _ = rest.Connecter(&ProxyREST{})

var proxyMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}

// New 返回空的 Service 代理资源。
func (r *ProxyREST) New() runtime.Object {
	return &api.ServiceProxyOptions{}
}

// Destroy 在关闭时清理资源；当前没有底层 Store 需要显式销毁。
func (r *ProxyREST) Destroy() {
	// 当前没有底层 Store，无需在此显式销毁资源。
}

// ConnectMethods 返回允许代理的 HTTP 方法列表。
func (r *ProxyREST) ConnectMethods() []string {
	return proxyMethods
}

// NewConnectOptions 返回表示代理参数的资源，并指定从请求路径解析参数。
func (r *ProxyREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.ServiceProxyOptions{}, true, "path"
}

// Connect 返回 Service 代理 Handler。
// Connect 校验代理选项，解析 Service 对应的可用后端地址并拼接路径，最终返回支持 HTTP Upgrade 的限速代理 Handler。
func (r *ProxyREST) Connect(ctx context.Context, id string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	proxyOpts, ok := opts.(*api.ServiceProxyOptions)
	if !ok {
		return nil, fmt.Errorf("Invalid options object: %#v", opts)
	}
	location, transport, err := r.Redirector.ResourceLocation(ctx, id)
	if err != nil {
		return nil, err
	}
	location.Path = net.JoinPreservingTrailingSlash(location.Path, proxyOpts.Path)
	// 使用指定的 Transport，并叠加 URL 重写、X-Forwarded-* 请求头等代理处理。
	return newThrottledUpgradeAwareProxyHandler(location, transport, true, false, responder), nil
}

// newThrottledUpgradeAwareProxyHandler 统一设置错误响应器和单连接带宽限制，避免 Service/Pod 代理各自实现限速逻辑。
func newThrottledUpgradeAwareProxyHandler(location *url.URL, transport http.RoundTripper, wrapTransport, upgradeRequired bool, responder rest.Responder) *proxy.UpgradeAwareHandler {
	handler := proxy.NewUpgradeAwareHandler(location, transport, wrapTransport, upgradeRequired, proxy.NewErrorResponder(responder))
	handler.MaxBytesPerSec = capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
	return handler
}
