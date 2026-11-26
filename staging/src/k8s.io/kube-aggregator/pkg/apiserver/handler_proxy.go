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

package apiserver

import (
	"net/http"
	"net/url"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	endpointmetrics "k8s.io/apiserver/pkg/endpoints/metrics"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	apiserverproxyutil "k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/apiserver/pkg/util/x509metrics"
	"k8s.io/client-go/transport"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	apiregistrationv1api "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1apihelper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
	"k8s.io/kube-aggregator/pkg/controllers/status/remote"
	"k8s.io/streaming/pkg/httpstream"
)

const (
	aggregatorComponent string = "aggregator"
)

// 证书函数而不是固定字节：证书轮换时只需换掉函数返回的字节，
// 不必重建代理 handler，正在处理的请求也不会读到半新半旧的凭证。
type certKeyFunc func() ([]byte, []byte)

// proxyHandler 提供 http.Handler，把流量代理到 Redirector 指定的位置。
// 每个 handler 只对应一个 GroupVersion；路由在更上层完成，这里只负责
// 按缓存的 handlingInfo 决定本地处理还是转发到后端服务。
type proxyHandler struct {
	// localDelegate 用于处理本地 APIService。
	localDelegate http.Handler

	// proxyCurrentCertKeyContent 保存标识本代理的客户端证书，后端 APIService 用它确认代理身份。
	proxyCurrentCertKeyContent certKeyFunc
	proxyTransportDial         *transport.DialHolder

	// 基于 Endpoints 的路由，把 cluster IP 映射到可路由 IP。
	serviceResolver ServiceResolver

	// 整个处理信息作为一个值原子替换：请求要么看到旧配置要么看到新配置，
	// 不会在 APIService 更新过程中读到互相矛盾的 service、证书和可用状态。
	handlingInfo atomic.Value

	// 是否拒绝转发重定向响应。
	rejectForwardingRedirects bool

	// tracerProvider 用于给代理传输与 handler 加上链路追踪。
	tracerProvider tracing.TracerProvider
}

// 代理所需的全部不可变状态被打包成一个快照，由上面的 atomic.Value 整体换出。
// 这样请求路径上不需要加锁，控制循环重建配置时也不会让请求看到中间态。
type proxyHandlingInfo struct {
	// local 表示该 APIService 由本地处理。
	local bool

	// name 是 APIService 的名字。
	name string
	// transportConfig 保存构造 roundtripper 所需的信息。
	transportConfig *transport.Config
	// transportBuildingError 是构造 transport 时的错误；非空时会上报给客户端。
	transportBuildingError error
	// proxyRoundTripper 是 transport 中可复用的部分，不随请求变化。
	proxyRoundTripper http.RoundTripper
	// serviceName 是本 handler 代理到的 Service 名字。
	serviceName string
	// namespace 是该 Service 所在的命名空间。
	serviceNamespace string
	// serviceAvailable 表示该 APIService 是否可用。
	// 状态检查在请求路径上只是快速开关：真正的健康检查由状态控制器异步做，
	// 这里只按结果决定快速返回 503，避免请求洪峰时再去探测后端。
	serviceAvailable bool
	// servicePort 是本 handler 代理到的 Service 端口。
	servicePort int32
}

func proxyError(w http.ResponseWriter, req *http.Request, error string, code int) {
	http.Error(w, error, code)

	ctx := req.Context()
	info, ok := genericapirequest.RequestInfoFrom(ctx)
	if !ok {
		klog.Warning("no RequestInfo found in the context")
		return
	}
	// TODO: 是否需要换一种方式记录长请求？这里的判定函数未必与被聚合 apiserver 一致。
	endpointmetrics.RecordRequestTermination(req, info, aggregatorComponent, code)
}

func (r *proxyHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	value := r.handlingInfo.Load()
	if value == nil {
		r.localDelegate.ServeHTTP(w, req)
		return
	}
	handlingInfo := value.(proxyHandlingInfo)
	if handlingInfo.local {
		if r.localDelegate == nil {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		r.localDelegate.ServeHTTP(w, req)
		return
	}

	if !handlingInfo.serviceAvailable {
		proxyError(w, req, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	if handlingInfo.transportBuildingError != nil {
		proxyError(w, req, handlingInfo.transportBuildingError.Error(), http.StatusInternalServerError)
		return
	}

	user, ok := genericapirequest.UserFrom(req.Context())
	if !ok {
		proxyError(w, req, "missing user", http.StatusInternalServerError)
		return
	}

	// 基于现有请求写出指向目标 Service 的 location。
	location := &url.URL{}
	location.Scheme = "https"
	rloc, err := r.serviceResolver.ResolveEndpoint(handlingInfo.serviceNamespace, handlingInfo.serviceName, handlingInfo.servicePort)
	if err != nil {
		klog.Errorf("error resolving %s/%s: %v", handlingInfo.serviceNamespace, handlingInfo.serviceName, err)
		proxyError(w, req, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	location.Host = rloc.Host
	location.Path = req.URL.Path
	location.RawQuery = req.URL.Query().Encode()

	newReq, cancelFn := apiserverproxyutil.NewRequestForProxy(location, req)
	defer cancelFn()

	if handlingInfo.proxyRoundTripper == nil {
		proxyError(w, req, "", http.StatusNotFound)
		return
	}

	proxyRoundTripper := handlingInfo.proxyRoundTripper
	upgrade := httpstream.IsUpgradeRequest(req)

	var userUID string
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.RemoteRequestHeaderUID) {
		userUID = user.GetUID()
	}

	proxyRoundTripper = transport.NewAuthProxyRoundTripper(user.GetName(), userUID, user.GetGroups(), user.GetExtra(), proxyRoundTripper)

	if !upgrade {
		tracingWrapper := tracing.WrapperFor(r.tracerProvider)
		proxyRoundTripper = tracingWrapper(proxyRoundTripper)
	}

	// 升级协议时 upgrade 路径会使用这里给出的 TLS 配置，却绕过 proxyRoundTripper 直接拨号，
	// 因此必须提前把正确的用户头写到请求上。
	if upgrade {
		transport.SetAuthProxyHeaders(newReq, user.GetName(), userUID, user.GetGroups(), user.GetExtra())
	}

	handler := proxy.NewUpgradeAwareHandler(location, proxyRoundTripper, true, upgrade, &responder{w: w})
	if r.rejectForwardingRedirects {
		handler.RejectForwardingRedirects = true
	}
	utilflowcontrol.RequestDelegated(req.Context())
	handler.ServeHTTP(w, newReq)
}

// responder 实现 rest.Responder，帮助 connector 写回对象或错误。
type responder struct {
	w http.ResponseWriter
}

// TODO: 这里应正确处理 content type 协商；调用方要 protobuf 却返回 JSON 会出问题。
func (r *responder) Object(statusCode int, obj runtime.Object) {
	responsewriters.WriteRawJSON(statusCode, obj, r.w)
}

func (r *responder) Error(_ http.ResponseWriter, _ *http.Request, err error) {
	http.Error(r.w, err.Error(), http.StatusServiceUnavailable)
}

// 以下方法提供对字段的加锁访问。

// setServiceAvailable 设置 serviceAvailable，非线程安全。
func (r *proxyHandler) setServiceAvailable() {
	info := r.handlingInfo.Load().(proxyHandlingInfo)
	info.serviceAvailable = true
	r.handlingInfo.Store(info)
}

func (r *proxyHandler) updateAPIService(apiService *apiregistrationv1api.APIService) {
	if apiService.Spec.Service == nil {
		r.handlingInfo.Store(proxyHandlingInfo{local: true})
		return
	}

	proxyClientCert, proxyClientKey := r.proxyCurrentCertKeyContent()

	transportConfig := remote.BuildTransportConfig(r.proxyTransportDial, proxyClientCert, proxyClientKey, apiService)
	transportConfig.Wrap(x509metrics.NewDeprecatedCertificateRoundTripperWrapperConstructor(
		x509MissingSANCounter,
		x509InsecureSHA1Counter,
	))

	newInfo := proxyHandlingInfo{
		name:             apiService.Name,
		transportConfig:  transportConfig,
		serviceName:      apiService.Spec.Service.Name,
		serviceNamespace: apiService.Spec.Service.Namespace,
		servicePort:      *apiService.Spec.Service.Port,
		serviceAvailable: apiregistrationv1apihelper.IsAPIServiceConditionTrue(apiService, apiregistrationv1api.Available),
	}
	newInfo.proxyRoundTripper, newInfo.transportBuildingError = transport.New(newInfo.transportConfig)
	if newInfo.transportBuildingError != nil {
		klog.Warning(newInfo.transportBuildingError.Error())
	}
	r.handlingInfo.Store(newInfo)
}
