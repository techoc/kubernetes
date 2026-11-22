/*
Copyright 2022 The Kubernetes Authors.

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

package aggregated

import (
	"net/http"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apidiscoveryv2beta1 "k8s.io/api/apidiscovery/v2beta1"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/cbor"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"

	"github.com/emicklei/go-restful/v3"
	"k8s.io/apimachinery/pkg/runtime"

	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// WrappedHandler 在同一个路径上按内容协商结果分发到「传统发现」或「聚合发现」处理器。
//
// 中文说明：聚合发现（APIGroupDiscoveryList）是 v2/v2beta1 的新格式，
// 但老客户端仍按 Accept 头请求 v1 的 APIVersions/APIResourceList。
// 用包装器而非两个独立端点，保证 URL 稳定、客户端按 Accept 头自行选择，
// 从而可以在同一 apiserver 上渐进启用新格式。
type WrappedHandler struct {
	s                     runtime.NegotiatedSerializer
	handler               http.Handler
	aggHandler            http.Handler
	peerAggregatedHandler http.Handler
}

// ServeHTTP 先协商媒体类型，再按「是否聚合发现」分流。
//
// 中文说明：协商失败时 mediaType 为零值，Convert 为空，自然走传统处理器，
// 从而保证异常 Accept 头不会导致 500。对 v2 且开启 UnknownVersionInteroperabilityProxy 时，
// 默认走 peerAggregatedHandler（聚合对等 apiserver 的信息）；客户端显式加
// profile=nopeer 才退回本地聚合，这一分支会计数以便观察显式退避的使用量。
func (wrapped *WrappedHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	// mediaType.Convert looks at the request accept headers and is used to control whether the discovery document will be aggregated.
	mediaType, _ := negotiation.NegotiateMediaTypeOptions(req.Header.Get("Accept"), wrapped.s.SupportedMediaTypes(), DiscoveryEndpointRestrictions)
	if IsAggregatedDiscoveryGVK(mediaType.Convert) {
		if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.UnknownVersionInteroperabilityProxy) && mediaType.Convert.Version == "v2" {
			if mediaType.Profile == "nopeer" {
				NoPeerDiscoveryRequestCounter.Inc()
				wrapped.aggHandler.ServeHTTP(resp, req)
				return
			}
			if wrapped.peerAggregatedHandler != nil {
				// Serve peer-aggregated discovery by default if provided.
				wrapped.peerAggregatedHandler.ServeHTTP(resp, req)
				return
			}
		}
		wrapped.aggHandler.ServeHTTP(resp, req)
		return
	}
	wrapped.handler.ServeHTTP(resp, req)
}

// restfulHandle 是 go-restful 与 net/http 之间的适配：把 restful 的响应包装器
// 展开回原始 http.ResponseWriter 与 *http.Request，避免中间层丢失 Flusher/Hijacker 能力。
func (wrapped *WrappedHandler) restfulHandle(req *restful.Request, resp *restful.Response) {
	wrapped.ServeHTTP(resp.ResponseWriter, req.Request)
}

// GenerateWebService 构造注册到 go-restful 的路由。
// 中文说明：Produces/Consumes 使用 serializer 支持的全部媒体类型，
// 使内容协商在 ServeHTTP 内完成而不是由 restful 提前拒绝；
// Writes 声明返回类型仅用于生成 OpenAPI 文档。
func (wrapped *WrappedHandler) GenerateWebService(prefix string, returnType interface{}) *restful.WebService {
	mediaTypes, _ := negotiation.MediaTypesForSerializer(wrapped.s)
	ws := new(restful.WebService)
	ws.Path(prefix)
	ws.Doc("get available API versions")
	ws.Route(ws.GET("/").To(wrapped.restfulHandle).
		Doc("get available API versions").
		Operation("getAPIVersions").
		Produces(mediaTypes...).
		Consumes(mediaTypes...).
		Writes(returnType))
	return ws
}

// WrapAggregatedDiscoveryToHandler wraps a handler with an option to
// emit the aggregated discovery by passing in the aggregated
// discovery type in content negotiation headers: eg: (Accept:
// application/json;v=v2;g=apidiscovery.k8s.io;as=APIGroupDiscoveryList)
//
// If peerAggregatedHandler is non-nil, it will be used to serve peer-aggregated
// discovery requests which aggregate discovery information from peer API servers.
// To request a no-peer aggregation, the client must include the "profile=nopeer" parameter in the
// Accept header eg: (Accept: application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=nopeer).
//
// 中文说明：此处自建 scheme 与 codec，而不是复用全局 scheme，是因为聚合发现
// 只涉及 apidiscovery 类型，独立 codec 可避免与主 API 的类型注册互相影响。
// CBOR 序列化器按特性门控附加，保证在未启用时行为与之前完全一致。
func WrapAggregatedDiscoveryToHandler(handler, aggHandler, peerAggregatedHandler http.Handler) *WrappedHandler {
	scheme := runtime.NewScheme()
	utilruntime.Must(apidiscoveryv2.AddToScheme(scheme))
	// v2beta1 仍需注册：老客户端可能请求 beta 版本，codec 必须能编码它。
	utilruntime.Must(apidiscoveryv2beta1.AddToScheme(scheme))
	var opts []serializer.CodecFactoryOptionsMutator
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.CBORServingAndStorage) {
		opts = append(opts, serializer.WithSerializer(cbor.NewSerializerInfo))
	}
	codecs := serializer.NewCodecFactory(scheme, opts...)
	return &WrappedHandler{codecs, handler, aggHandler, peerAggregatedHandler}
}
