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

package node

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/proxy"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/registry/core/node"
)

// ProxyREST 实现 nodes/{name}/proxy：把 API 请求转发到指定 Node 的 Kubelet HTTP 端点。
type ProxyREST struct {
	Store          *genericregistry.Store
	Connection     client.ConnectionInfoGetter
	ProxyTransport http.RoundTripper
}

// 实现 Connecter
var _ = rest.Connecter(&ProxyREST{})

var proxyMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}

// New 返回一个空的 nodeProxyOptions 对象。
func (r *ProxyREST) New() runtime.Object {
	return &api.NodeProxyOptions{}
}

// Destroy 在关闭时清理资源。
func (r *ProxyREST) Destroy() {
	// 底层 store 与 REST 共享，因此这里不显式销毁它。
}

// ConnectMethods 返回可以被代理的 HTTP 方法列表。
func (r *ProxyREST) ConnectMethods() []string {
	return proxyMethods
}

// NewConnectOptions 返回表示代理参数的版本化资源。
func (r *ProxyREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.NodeProxyOptions{}, true, "path"
}

// Connect 校验代理选项，解析 Node 的 Kubelet 地址并拼接相对路径，返回带带宽限制的升级感知代理。
func (r *ProxyREST) Connect(ctx context.Context, id string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	proxyOpts, ok := opts.(*api.NodeProxyOptions)
	if !ok {
		return nil, fmt.Errorf("Invalid options object: %#v", opts)
	}
	// ResourceLocation 同时决定目标地址和 Transport：默认 Kubelet 端口使用节点专用 TLS 配置。
	location, transport, err := node.ResourceLocation(r.Store, r.Connection, r.ProxyTransport, ctx, id)
	if err != nil {
		return nil, err
	}
	// 保留末尾斜杠语义拼接用户指定路径，避免代理到 Kubelet 时改变相对路径解析结果。
	location.Path = net.JoinPreservingTrailingSlash(location.Path, proxyOpts.Path)
	// 返回使用指定 transport 的代理处理函数，外面再包一层代理处理逻辑（用于 URL 重写、X-Forwarded-* 头部等）
	return newThrottledUpgradeAwareProxyHandler(location, transport, true, false, responder), nil
}

// newThrottledUpgradeAwareProxyHandler 统一创建支持 HTTP Upgrade 的代理，并施加每连接带宽上限。
func newThrottledUpgradeAwareProxyHandler(location *url.URL, transport http.RoundTripper, wrapTransport, upgradeRequired bool, responder rest.Responder) *proxy.UpgradeAwareHandler {
	handler := proxy.NewUpgradeAwareHandler(location, transport, wrapTransport, upgradeRequired, proxy.NewErrorResponder(responder))
	handler.MaxBytesPerSec = capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
	return handler
}
