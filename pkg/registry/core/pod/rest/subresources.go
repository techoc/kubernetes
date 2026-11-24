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

package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	translator "k8s.io/apiserver/pkg/util/proxy"
	proxymetrics "k8s.io/apiserver/pkg/util/proxy/metrics"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/capabilities"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/registry/core/pod"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// ProxyREST 实现 pods/{name}/proxy：解析 Pod IP/端口和相对路径，再构造限速的升级感知反向代理。
type ProxyREST struct {
	Store          *genericregistry.Store
	ProxyTransport http.RoundTripper
}

// Implement Connecter
var _ = rest.Connecter(&ProxyREST{})

var proxyMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}

// New returns an empty podProxyOptions object.
func (r *ProxyREST) New() runtime.Object {
	return &api.PodProxyOptions{}
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
	return &api.PodProxyOptions{}, true, "path"
}

// Connect returns a handler for the pod proxy
// Connect 校验代理参数、解析 Pod 网络位置、拼接请求路径，并返回支持 WebSocket/HTTP Upgrade 的代理 Handler。
func (r *ProxyREST) Connect(ctx context.Context, id string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	proxyOpts, ok := opts.(*api.PodProxyOptions)
	if !ok {
		return nil, fmt.Errorf("Invalid options object: %#v", opts)
	}
	location, transport, err := pod.ResourceLocation(ctx, r.Store, r.ProxyTransport, id)
	if err != nil {
		return nil, err
	}
	location.Path = net.JoinPreservingTrailingSlash(location.Path, proxyOpts.Path)
	// Return a proxy handler that uses the desired transport, wrapped with additional proxy handling (to get URL rewriting, X-Forwarded-* headers, etc)
	return newThrottledUpgradeAwareProxyHandler(location, transport, true, false, responder), nil
}

// Support both GET and POST methods. We must support GET for browsers that want to use WebSockets.
var upgradeableMethods = []string{"GET", "POST"}

// AttachREST 实现 pods/{name}/attach，将标准输入/输出连接到指定容器已有进程的流式会话。
type AttachREST struct {
	Store       pod.ResourceGetter
	KubeletConn client.ConnectionInfoGetter
	Authorizer  authorizer.UnconditionalAuthorizer
}

// Implement Connecter
var _ = rest.Connecter(&AttachREST{})

// New creates a new podAttachOptions object.
func (r *AttachREST) New() runtime.Object {
	return &api.PodAttachOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *AttachREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Connect returns a handler for the pod exec proxy
// Connect 先按 Feature Gate 检查兼容的 create 权限，再定位节点并根据 Kubelet WebSocket 能力决定是否在 API Server 转换协议。
func (r *AttachREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	// Forces a authz check for "create", if feature gate enabled.
	// See: https://github.com/kubernetes/kubernetes/issues/133515
	if utilfeature.DefaultFeatureGate.Enabled(features.AuthorizePodWebsocketUpgradeCreatePermission) {
		if err := ensureAuthorizedForVerb(ctx, r.Authorizer, "create"); err != nil {
			return nil, err
		}
	}

	attachOpts, ok := opts.(*api.PodAttachOptions)
	if !ok {
		return nil, fmt.Errorf("Invalid options object: %#v", opts)
	}

	location, connInfo, err := pod.AttachLocation(ctx, r.Store, r.KubeletConn, name, attachOpts)
	if err != nil {
		return nil, err
	}
	transport := connInfo.Transport
	nodeSupportsWebsockets := checkNodeSupportsWebsockets(connInfo.NodeFeatures)
	handler := newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder)
	if utilfeature.DefaultFeatureGate.Enabled(features.TranslateStreamCloseWebsocketRequests) {
		// If the node supports websocket translation, revert to legacy proxy, and let kubelet handle it.
		if utilfeature.DefaultFeatureGate.Enabled(features.ExtendWebSocketsToKubelet) && nodeSupportsWebsockets {
			proxymetrics.IncWebSocketStreamingRequest(ctx, "attach", "proxied_to_kubelet")
			return handler, nil
		}
		proxymetrics.IncWebSocketStreamingRequest(ctx, "attach", "translated_at_apiserver")
		// Wrap the upgrade aware handler to implement stream translation
		// for WebSocket/V5 upgrade requests.
		streamOptions := translator.Options{
			Stdin:  attachOpts.Stdin,
			Stdout: attachOpts.Stdout,
			Stderr: attachOpts.Stderr,
			Tty:    attachOpts.TTY,
		}
		maxBytesPerSec := capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
		streamtranslator := translator.NewStreamTranslatorHandler(location, transport, maxBytesPerSec, streamOptions)
		handler = translator.NewTranslatingHandler(handler, streamtranslator, wsstream.IsWebSocketRequestWithStreamCloseProtocol)
	}
	return handler, nil
}

// NewConnectOptions returns the versioned object that represents exec parameters
func (r *AttachREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodAttachOptions{}, false, ""
}

// ConnectMethods returns the methods supported by exec
func (r *AttachREST) ConnectMethods() []string {
	return upgradeableMethods
}

// ExecREST 实现 pods/{name}/exec：创建到容器内新进程的交互式流，并转发 stdin/stdout/stderr/TTY 参数。
type ExecREST struct {
	Store       pod.ResourceGetter
	KubeletConn client.ConnectionInfoGetter
	Authorizer  authorizer.UnconditionalAuthorizer
}

// Implement Connecter
var _ = rest.Connecter(&ExecREST{})

// New creates a new podExecOptions object.
func (r *ExecREST) New() runtime.Object {
	return &api.PodExecOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *ExecREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Connect returns a handler for the pod exec proxy
// Connect 校验 ExecOptions、授权和节点连接信息；必要时将 WebSocket stream-close 协议转换为 Kubelet 支持的内部流。
func (r *ExecREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	// Forces a authz check for "create", if feature gate enabled.
	// See: https://github.com/kubernetes/kubernetes/issues/133515
	if utilfeature.DefaultFeatureGate.Enabled(features.AuthorizePodWebsocketUpgradeCreatePermission) {
		if err := ensureAuthorizedForVerb(ctx, r.Authorizer, "create"); err != nil {
			return nil, err
		}
	}

	execOpts, ok := opts.(*api.PodExecOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}

	location, connInfo, err := pod.ExecLocation(ctx, r.Store, r.KubeletConn, name, execOpts)
	if err != nil {
		return nil, err
	}
	transport := connInfo.Transport
	nodeSupportsWebsockets := checkNodeSupportsWebsockets(connInfo.NodeFeatures)
	handler := newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder)
	if utilfeature.DefaultFeatureGate.Enabled(features.TranslateStreamCloseWebsocketRequests) {
		// If the node supports websocket translation, revert to legacy proxy, and let kubelet handle it.
		if utilfeature.DefaultFeatureGate.Enabled(features.ExtendWebSocketsToKubelet) && nodeSupportsWebsockets {
			proxymetrics.IncWebSocketStreamingRequest(ctx, "exec", "proxied_to_kubelet")
			return handler, nil
		}
		proxymetrics.IncWebSocketStreamingRequest(ctx, "exec", "translated_at_apiserver")
		// Wrap the upgrade aware handler to implement stream translation
		// for WebSocket/V5 upgrade requests.
		streamOptions := translator.Options{
			Stdin:  execOpts.Stdin,
			Stdout: execOpts.Stdout,
			Stderr: execOpts.Stderr,
			Tty:    execOpts.TTY,
		}
		maxBytesPerSec := capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
		streamtranslator := translator.NewStreamTranslatorHandler(location, transport, maxBytesPerSec, streamOptions)
		handler = translator.NewTranslatingHandler(handler, streamtranslator, wsstream.IsWebSocketRequestWithStreamCloseProtocol)
	}
	return handler, nil
}

// NewConnectOptions returns the versioned object that represents exec parameters
func (r *ExecREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodExecOptions{}, false, ""
}

// ConnectMethods returns the methods supported by exec
func (r *ExecREST) ConnectMethods() []string {
	return upgradeableMethods
}

// PortForwardREST 实现 pods/{name}/portforward，将客户端指定的 TCP 端口流转发到 Pod 所在节点。
type PortForwardREST struct {
	Store       pod.ResourceGetter
	KubeletConn client.ConnectionInfoGetter
	Authorizer  authorizer.UnconditionalAuthorizer
}

// Implement Connecter
var _ = rest.Connecter(&PortForwardREST{})

// New returns an empty podPortForwardOptions object
func (r *PortForwardREST) New() runtime.Object {
	return &api.PodPortForwardOptions{}
}

// Destroy cleans up resources on shutdown.
func (r *PortForwardREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// NewConnectOptions returns the versioned object that represents the
// portforward parameters
func (r *PortForwardREST) NewConnectOptions() (runtime.Object, bool, string) {
	return &api.PodPortForwardOptions{}, false, ""
}

// ConnectMethods returns the methods supported by portforward
func (r *PortForwardREST) ConnectMethods() []string {
	return upgradeableMethods
}

// Connect returns a handler for the pod portforward proxy
// Connect 根据 Node 能力选择旧式 Kubelet 代理或 API Server WebSocket 隧道转换，并记录对应请求指标。
func (r *PortForwardREST) Connect(ctx context.Context, name string, opts runtime.Object, responder rest.Responder) (http.Handler, error) {
	// Forces a authz check for "create", if feature gate enabled.
	// See: https://github.com/kubernetes/kubernetes/issues/133515
	if utilfeature.DefaultFeatureGate.Enabled(features.AuthorizePodWebsocketUpgradeCreatePermission) {
		if err := ensureAuthorizedForVerb(ctx, r.Authorizer, "create"); err != nil {
			return nil, err
		}
	}

	portForwardOpts, ok := opts.(*api.PodPortForwardOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}
	location, connInfo, err := pod.PortForwardLocation(ctx, r.Store, r.KubeletConn, name, portForwardOpts)
	if err != nil {
		return nil, err
	}
	transport := connInfo.Transport
	nodeSupportsWebsockets := checkNodeSupportsWebsockets(connInfo.NodeFeatures)
	handler := newThrottledUpgradeAwareProxyHandler(location, transport, false, true, responder)
	if utilfeature.DefaultFeatureGate.Enabled(features.PortForwardWebsockets) {
		// If the node supports websocket tunneling, revert to legacy proxy, and let kubelet handle it.
		if utilfeature.DefaultFeatureGate.Enabled(features.ExtendWebSocketsToKubelet) && nodeSupportsWebsockets {
			proxymetrics.IncWebSocketStreamingRequest(ctx, "portforward", "proxied_to_kubelet")
			return handler, nil
		}
		proxymetrics.IncWebSocketStreamingRequest(ctx, "portforward", "translated_at_apiserver")
		tunnelingHandler := translator.NewTunnelingHandler(handler)
		handler = translator.NewTranslatingHandler(handler, tunnelingHandler, wsstream.IsWebSocketRequestWithTunnelingProtocol)
	}
	return handler, nil
}

// newThrottledUpgradeAwareProxyHandler 统一创建带错误响应器和单连接带宽上限的升级感知代理。
func newThrottledUpgradeAwareProxyHandler(location *url.URL, transport http.RoundTripper, wrapTransport, upgradeRequired bool, responder rest.Responder) http.Handler {
	handler := proxy.NewUpgradeAwareHandler(location, transport, wrapTransport, upgradeRequired, proxy.NewErrorResponder(responder))
	handler.MaxBytesPerSec = capabilities.Get().PerConnectionBandwidthLimitBytesPerSec
	return handler
}

// 根据 NodeFeatures 判断目标 Kubelet 是否声明支持 WebSocket 扩展；这决定流式请求由 Kubelet 还是 API Server 做协议转换。
// translation/tunneling, false otherwise.
func checkNodeSupportsWebsockets(nodeFeatures []string) bool {
	nodeSupportsWebsockets := slices.Contains(nodeFeatures, string(features.ExtendWebSocketsToKubelet))
	return nodeSupportsWebsockets
}
