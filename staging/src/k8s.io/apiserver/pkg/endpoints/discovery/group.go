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
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// APIGroupHandler 创建一个 WebService，返回某个 API 组的受支持版本、首选版本和名称。
// 例如，该 WebService 会注册在 /apis/extensions。
type APIGroupHandler struct {
	serializer runtime.NegotiatedSerializer
	group      metav1.APIGroup
}

// NewAPIGroupHandler 构造某 API 组的发现处理器。group 中的版本列表与 preferredVersion
// 由调用方预先计算好，这里只负责序列化，不再做任何取舍。
func NewAPIGroupHandler(serializer runtime.NegotiatedSerializer, group metav1.APIGroup) *APIGroupHandler {
	if keepUnversioned(group.Name) {
		// 1.1 版本中 /apis/extensions 返回的响应里 APIVersion 为空，这里用
		// stripVersionNegotiatedSerializer 保持响应向后兼容。
		// 这是历史兼容性包袱：老客户端依赖 /apis/extensions 响应中 apiVersion 为空，
		// 不能因为“清理”而移除。
		serializer = stripVersionNegotiatedSerializer{serializer}
	}

	return &APIGroupHandler{
		serializer: serializer,
		group:      group,
	}
}

// WebService 注册 GET /apis/<group> 路由，返回该组的版本与首选版本信息。
func (s *APIGroupHandler) WebService() *restful.WebService {
	mediaTypes, _ := negotiation.MediaTypesForSerializer(s.serializer)
	ws := new(restful.WebService)
	ws.Path(APIGroupPrefix + "/" + s.group.Name)
	ws.Doc("get information of a group")
	ws.Route(ws.GET("/").To(s.handle).
		Doc("get information of a group").
		Operation("getAPIGroup").
		Produces(mediaTypes...).
		Consumes(mediaTypes...).
		Writes(metav1.APIGroup{}))
	return ws
}

// handle 返回处理该组的 APIGroup 信息的处理函数。
func (s *APIGroupHandler) handle(req *restful.Request, resp *restful.Response) {
	s.ServeHTTP(resp.ResponseWriter, req.Request)
}

// ServeHTTP 以内容协商方式写出 APIGroup 对象；传入空的 GroupVersion 表示响应体
// 不属于某个具体版本（发现信息本身是跨版本的）。
func (s *APIGroupHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	responsewriters.WriteObjectNegotiated(s.serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, w, req, http.StatusOK, &s.group, false)
}
