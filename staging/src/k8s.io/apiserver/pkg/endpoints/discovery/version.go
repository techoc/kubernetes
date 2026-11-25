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

// Package discovery 实现 API 发现端点：/api、/apis、/apis/<group>/<version> 等
// 路径返回服务端支持的组、版本与资源列表，客户端依赖这些信息进行服务发现。
package discovery

import (
	"net/http"

	restful "github.com/emicklei/go-restful/v3"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// APIResourceLister 提供某个组版本下的资源列表，由各 API 安装器实现。
type APIResourceLister interface {
	ListAPIResources() []metav1.APIResource
}

// APIResourceListerFunc 把普通函数适配成 APIResourceLister。
type APIResourceListerFunc func() []metav1.APIResource

func (f APIResourceListerFunc) ListAPIResources() []metav1.APIResource {
	return f()
}

// APIVersionHandler 创建一个 WebService，返回该版本支持的资源列表。
// 例如，该 WebService 会注册在 /apis/extensions/v1beta1。
type APIVersionHandler struct {
	serializer runtime.NegotiatedSerializer

	groupVersion      schema.GroupVersion
	apiResourceLister APIResourceLister
}

// NewAPIVersionHandler 构造单个组版本的发现处理器。
//
// 对部分历史遗留的组（见 keepUnversioned），响应体需要去掉 apiVersion 字段以
// 兼容 1.1 时代的客户端行为，因此在这里替换序列化器。
func NewAPIVersionHandler(serializer runtime.NegotiatedSerializer, groupVersion schema.GroupVersion, apiResourceLister APIResourceLister) *APIVersionHandler {
	if keepUnversioned(groupVersion.Group) {
		// 1.1 版本中 /apis/extensions 返回的响应里 APIVersion 为空，这里用
		// stripVersionNegotiatedSerializer 保持响应向后兼容。
		serializer = stripVersionNegotiatedSerializer{serializer}
	}

	return &APIVersionHandler{
		serializer:        serializer,
		groupVersion:      groupVersion,
		apiResourceLister: apiResourceLister,
	}
}

// AddToWebService 把 GET / 路由注册到给定的 WebService 上。
func (s *APIVersionHandler) AddToWebService(ws *restful.WebService) {
	mediaTypes, _ := negotiation.MediaTypesForSerializer(s.serializer)
	ws.Route(ws.GET("/").To(s.handle).
		Doc("get available resources").
		Operation("getAPIResources").
		Produces(mediaTypes...).
		Consumes(mediaTypes...).
		Writes(metav1.APIResourceList{}))
}

// handle 返回处理该组版本信息的处理函数。
func (s *APIVersionHandler) handle(req *restful.Request, resp *restful.Response) {
	s.ServeHTTP(resp.ResponseWriter, req.Request)
}

// ServeHTTP 以协商后的格式输出该组版本的 APIResourceList。
//
// 这里传入空的 GroupVersion 作为错误响应上下文，因为响应内容本身已带有
// 组版本信息，错误编码不需要额外的 GV。
func (s *APIVersionHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	responsewriters.WriteObjectNegotiated(s.serializer, negotiation.DefaultEndpointRestrictions, schema.GroupVersion{}, w, req, http.StatusOK,
		&metav1.APIResourceList{GroupVersion: s.groupVersion.String(), APIResources: s.apiResourceLister.ListAPIResources()}, false)
}
