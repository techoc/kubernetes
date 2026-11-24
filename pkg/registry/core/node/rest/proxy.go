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

// Implement Connecter
var _ = rest.Connecter(&ProxyREST{})

var proxyMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}

// New returns an empty nodeProxyOptions object.
func (r *ProxyREST) New() runtime.Object {
	return &api.NodeProxyOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *ProxyREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// ConnectMethods returns the list of HTTP methods that can be proxied
func (r *ProxyREST) ConnectMethods() []string {
	return proxyMethods
}

// NewConnectOptions returns versioned resource that represents proxy parameters
func (r *ProxyREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.NodeProxyOptions{}, true, "path"
}

// Connect returns a handler for the node proxy
// Connect 校验代理选项，解析 Node 的 Kubelet 地址并拼接相对路径，最后返回带带宽限制的升级感知代理。
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
	// Return a proxy handler that uses the desired transport, wrapped with additional proxy handling (to get URL rewriting, X-Forwarded-* headers, etc)
	return newThrottledUpgradeAwareProxyHandler(location, transport, true, false, responder), nil
}

// newThrottledUpgradeAwareProxyHandler 统一创建支持 HTTP Upgrade 的代理，并施加每连接带宽上限。
func newThrottledUpgradeAwareProxyHandler(location *url.URL, transport http.RoundTripper, wrapTransport, upgradeRequired bool, responder rest.Responder) *proxy.UpgradeAwareHandler {
	handler := proxy.NewUpgradeAwareHandler(location, transport, wrapTransport, upgradeRequired, proxy.NewErrorResponder(responder))
	handler.MaxBytesPerSec = capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
	return handler
}
