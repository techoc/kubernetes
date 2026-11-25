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

package discovery

import (
	"net/http"

	"github.com/emicklei/go-restful/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// legacyRootAPIHandler 创建一个 WebService，服务于 API 组发现。
// 这里服务的是旧版 /api 端点，只暴露核心组的版本列表，与新式 /apis 分开维护。
type legacyRootAPIHandler struct {
	// addresses 用于在发现响应中构造集群 IP
	addresses  Addresses
	apiPrefix  string
	serializer runtime.NegotiatedSerializer
}

// NewLegacyRootAPIHandler 构造旧版 /api 发现处理器。
func NewLegacyRootAPIHandler(addresses Addresses, serializer runtime.NegotiatedSerializer, apiPrefix string) *legacyRootAPIHandler {
	// 1.1 版本中 /apis 返回的响应里 APIVersion 为空，这里用
	// stripVersionNegotiatedSerializer 保持响应向后兼容。
	// 该兼容行为已固化进 API 契约，修改会破坏老客户端，因此保留。
	serializer = stripVersionNegotiatedSerializer{serializer}

	return &legacyRootAPIHandler{
		addresses:  addresses,
		apiPrefix:  apiPrefix,
		serializer: serializer,
	}
}

// WebService 创建一个服务，在旧版 /api 端点返回受支持的 API 版本列表。
// 注意方法名沿用历史拼写 WebService，实际返回 restful.WebService。
func (s *legacyRootAPIHandler) WebService() *restful.WebService {
	mediaTypes, _ := negotiation.MediaTypesForSerializer(s.serializer)
	ws := new(restful.WebService)
	ws.Path(s.apiPrefix)
	ws.Doc("get available API versions")
	ws.Route(ws.GET("/").To(s.restfulHandle).
		Doc("get available API versions").
		Operation("getAPIVersions").
		Produces(mediaTypes...).
		Consumes(mediaTypes...).
		Writes(metav1.APIVersions{}))
	return ws
}

// restfulHandle 把 go-restful 请求桥接回标准 net/http 处理逻辑。
func (s *legacyRootAPIHandler) restfulHandle(req *restful.Request, resp *restful.Response) {
	s.ServeHTTP(resp.ResponseWriter, req.Request)
}

// ServeHTTP 返回核心组支持的版本（目前固定为 v1）以及按客户端 CIDR 计算的 server address。
func (s *legacyRootAPIHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	clientIP := utilnet.GetClientIP(req)
	apiVersions := &metav1.APIVersions{
		ServerAddressByClientCIDRs: s.addresses.ServerAddressByClientCIDRs(clientIP),
		Versions:                   []string{"v1"},
	}

	responsewriters.WriteObjectNegotiated(s.serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, resp, req, http.StatusOK, apiVersions, false)
}
