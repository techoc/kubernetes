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

package aggregator

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	openapinamer "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/util/responsewriter"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	openapicommon "k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/handler3"
	kubeopenapispec "k8s.io/kube-openapi/pkg/validation/spec"
)

// testV3APIService 模拟一个只暴露 V3 OpenAPI 的聚合服务。
// 同时处理根文档（给出 group/version 的链接）和具体版本的 spec 请求，
// 并支持 ETag/304，用来验证聚合器是否正确透传缓存语义。
type testV3APIService struct {
	etag string
	data []byte
}

var _ http.Handler = testV3APIService{}

func (h testV3APIService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 创建一个只处理某个 group/version 的 APIService 及 handler。
	if r.URL.Path == "/openapi/v3" {
		group := &handler3.OpenAPIV3Discovery{
			Paths: map[string]handler3.OpenAPIV3DiscoveryGroupVersion{
				"apis/group.example.com/v1": {
					ServerRelativeURL: "/openapi/v3/apis/group.example.com/v1?hash=" + h.etag,
				},
			},
		}

		j, _ := json.Marshal(group)
		w.Write(j)
		return
	}

	if r.URL.Path == "/openapi/v3/apis/group.example.com/v1" {
		if len(h.etag) > 0 {
			w.Header().Add("Etag", h.etag)
		}
		ifNoneMatches := r.Header["If-None-Match"]
		for _, match := range ifNoneMatches {
			if match == h.etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		w.Write(h.data)
	}
}

// testV2APIService 模拟一个只提供 legacy V2 spec 的聚合服务，
// 用来验证聚合器会把 V2 转换成 V3 后对外暴露。
type testV2APIService struct{}

var _ http.Handler = testV2APIService{}

func (h testV2APIService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 创建一个只处理某个 group/version 的 APIService 及 handler。
	if r.URL.Path == "/openapi/v2" {
		w.Write([]byte(`{"swagger":"2.0","info":{"title":"Kubernetes","version":"unversioned"}}`))
		return
	}
	w.WriteHeader(404)
}

// TestV2APIService 验证只提供 V2 spec 的 APIService 也能出现在 V3 发现文档中，
// 且取回的文档已转换成 V3；最后再确认移除该 APIService 后对应路径消失。
func TestV2APIService(t *testing.T) {
	downloader := Downloader{}
	pathHandler := mux.NewPathRecorderMux("aggregator_test")
	var serveHandler http.Handler = pathHandler
	specProxier, err := BuildAndRegisterAggregator(downloader, genericapiserver.NewEmptyDelegate(), nil, nil, pathHandler)
	if err != nil {
		t.Error(err)
	}
	handler := testV2APIService{}
	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "group.example.com",
			Version: "v1",
		},
	}
	apiService.Name = "v1.group.example.com"
	specProxier.AddUpdateAPIService(handler, apiService)
	specProxier.UpdateAPIServiceSpec("v1.group.example.com")

	data := sendReq(t, serveHandler, "/openapi/v3")
	groupVersionList := handler3.OpenAPIV3Discovery{}
	if err := json.Unmarshal(data, &groupVersionList); err != nil {
		t.Fatal(err)
	}

	// legacy APIService 不发布 OpenAPI v3，需确保其 v2 规范仍能被聚合并转换为 v3。
	path, ok := groupVersionList.Paths["apis/group.example.com/v1"]
	if !ok {
		t.Error("Expected group.example.com/v1 to be in group version list")
	}
	gotSpecJSON := sendReq(t, serveHandler, path.ServerRelativeURL)

	expectedV3Bytes := []byte(`{"openapi":"3.0.0","info":{"title":"Kubernetes","version":"unversioned"},"components":{}}`)

	if bytes.Compare(gotSpecJSON, expectedV3Bytes) != 0 {
		t.Errorf("Spec mismatch, expected %s, got %s", expectedV3Bytes, gotSpecJSON)
	}

	apiServiceNames := specProxier.GetAPIServiceNames()
	assert.ElementsMatch(t, []string{openAPIV2Converter, apiService.Name}, apiServiceNames)

	// 验证 legacy APIService 的 OpenAPI v3 已移除。
	specProxier.RemoveAPIServiceSpec(apiService.Name)
	data = sendReq(t, serveHandler, "/openapi/v3")
	groupVersionList = handler3.OpenAPIV3Discovery{}
	if err := json.Unmarshal(data, &groupVersionList); err != nil {
		t.Fatal(err)
	}

	path, ok = groupVersionList.Paths["apis/group.example.com/v1"]
	if ok {
		t.Error("Expected group.example.com/v1 not to be in group version list")
	}
}

// TestV3APIService 验证原生 V3 spec 能被原样聚合：
// 发现文档里有对应路径，且取回的内容与 APIService 提供的字节完全一致。
func TestV3APIService(t *testing.T) {
	downloader := Downloader{}

	pathHandler := mux.NewPathRecorderMux("aggregator_test")
	var serveHandler http.Handler = pathHandler
	specProxier, err := BuildAndRegisterAggregator(downloader, genericapiserver.NewEmptyDelegate(), nil, nil, pathHandler)
	if err != nil {
		t.Error(err)
	}
	specJSON := []byte(`{"openapi":"3.0.0","info":{"title":"Kubernetes","version":"unversioned"}}`)
	handler := testV3APIService{
		etag: "6E8F849B434D4B98A569B9D7718876E9-356ECAB19D7FBE1336BABB1E70F8F3025050DE218BE78256BE81620681CFC9A268508E542B8B55974E17B2184BBFC8FFFAA577E51BE195D32B3CA2547818ABE4",
		data: specJSON,
	}
	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "group.example.com",
			Version: "v1",
		},
	}
	apiService.Name = "v1.group.example.com"
	specProxier.AddUpdateAPIService(handler, apiService)
	specProxier.UpdateAPIServiceSpec("v1.group.example.com")

	data := sendReq(t, serveHandler, "/openapi/v3")
	groupVersionList := handler3.OpenAPIV3Discovery{}
	if err := json.Unmarshal(data, &groupVersionList); err != nil {
		t.Fatal(err)
	}
	path, ok := groupVersionList.Paths["apis/group.example.com/v1"]
	if !ok {
		t.Error("Expected group.example.com/v1 to be in group version list")
	}
	gotSpecJSON := sendReq(t, serveHandler, path.ServerRelativeURL)
	if bytes.Compare(gotSpecJSON, specJSON) != 0 {
		t.Errorf("Spec mismatch, expected %s, got %s", specJSON, gotSpecJSON)
	}

	apiServiceNames := specProxier.GetAPIServiceNames()
	assert.ElementsMatch(t, []string{openAPIV2Converter, apiService.Name}, apiServiceNames)
}

// TestV3RootAPIService 验证聚合层自身（local/root）的 API 类型也能出现在 V3 文档中。
// 用一个 go-restful WebService 提供 APIService 的 OpenAPI 定义，
// 检查最终文档的 operationId、$ref 等细节，确保本地类型与远端类型走的是同一套 V3 聚合流程。
func TestV3RootAPIService(t *testing.T) {
	ws := new(restful.WebService)
	{
		ws.Path("/apis/apiregistration.k8s.io/v1")
		ws.Doc("API at/apis/apiregistration.k8s.io/v1 ")
		ws.Consumes("*/*")
		ws.Produces("application/json")
		ws.ApiVersion("apiregistration.k8s.io/v1")
		routeBuilder := ws.GET("apiservices").
			To(func(request *restful.Request, response *restful.Response) {}).
			Doc("list or watch objects of kind APIService").
			Operation("listAPIService").
			Produces("application/json").
			Returns(http.StatusOK, "OK", v1.APIService{}).
			Writes(v1.APIService{})
		ws.Route(routeBuilder)
	}
	openapiConfig := genericapiserver.DefaultOpenAPIV3Config(getTestAPIServiceOpenAPIDefinitions, openapinamer.NewDefinitionNamer(runtime.NewScheme()))

	downloader := Downloader{}
	goRestfulContainer := restful.NewContainer()
	goRestfulContainer.Add(ws)
	pathHandler := mux.NewPathRecorderMux("aggregator_test")
	var serveHandler http.Handler = pathHandler
	specProxier, err := BuildAndRegisterAggregator(downloader, genericapiserver.NewEmptyDelegate(), goRestfulContainer, openapiConfig, pathHandler)
	if err != nil {
		t.Error(err)
	}
	expectedSpecJSON := []byte(`{"openapi":"3.0.0","info":{"title":"Generic API Server"},"paths":{"/apis/apiregistration.k8s.io/v1/apiservices":{"get":{"tags":["apiregistration_v1"],"description":"list or watch objects of kind APIService","operationId":"listApiregistrationV1APIService","responses":{"200":{"description":"OK","content":{"application/json":{"schema":{"$ref":"#/components/schemas/io.k8s.kube-aggregator.pkg.apis.apiregistration.v1.APIService"}}}}}}}},"components":{"schemas":{"io.k8s.kube-aggregator.pkg.apis.apiregistration.v1.APIService":{"description":"APIService represents a server for a particular GroupVersion. Name must be \"version.group\".","type":"object"}}}}`)

	data := sendReq(t, serveHandler, "/openapi/v3")
	groupVersionList := handler3.OpenAPIV3Discovery{}
	if err := json.Unmarshal(data, &groupVersionList); err != nil {
		t.Fatal(err)
	}
	path, ok := groupVersionList.Paths["apis/apiregistration.k8s.io/v1"]
	if !ok {
		t.Error("Expected apiregistration.k8s.io/v1 to be in group version list")
	}
	gotSpecJSON := sendReq(t, serveHandler, path.ServerRelativeURL)
	if bytes.Compare(gotSpecJSON, expectedSpecJSON) != 0 {
		t.Errorf("Spec mismatch, expected %s, got %s", expectedSpecJSON, gotSpecJSON)
	}

	apiServiceNames := specProxier.GetAPIServiceNames()
	assert.ElementsMatch(t, []string{"k8s_internal_local_kube_aggregator_types", openAPIV2Converter}, apiServiceNames)
}

// TestOpenAPIRequestMetrics 验证两种请求都会被计入 apiserver_request_total：
// 访问 V3 根文档、以及访问某个 group/version 的 spec，分别对应不同的 subresource 标签。
// 先 Reset 再用 GatherAndCompare 校验累计值，避免测试间指标相互干扰。
func TestOpenAPIRequestMetrics(t *testing.T) {
	metrics.Register()
	metrics.Reset()

	downloader := Downloader{}

	pathHandler := mux.NewPathRecorderMux("aggregator_metrics_test")
	var serveHandler http.Handler = pathHandler
	specProxier, err := BuildAndRegisterAggregator(downloader, genericapiserver.NewEmptyDelegate(), nil, nil, pathHandler)
	if err != nil {
		t.Error(err)
	}
	specJSON := []byte(`{"openapi":"3.0.0","info":{"title":"Kubernetes","version":"unversioned"}}`)
	handler := testV3APIService{
		etag: "6E8F849B434D4B98A569B9D7718876E9-356ECAB19D7FBE1336BABB1E70F8F3025050DE218BE78256BE81620681CFC9A268508E542B8B55974E17B2184BBFC8FFFAA577E51BE195D32B3CA2547818ABE4",
		data: specJSON,
	}
	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "group.example.com",
			Version: "v1",
		},
	}
	apiService.Name = "v1.group.example.com"
	specProxier.AddUpdateAPIService(handler, apiService)
	specProxier.UpdateAPIServiceSpec("v1.group.example.com")

	data := sendReq(t, serveHandler, "/openapi/v3")
	groupVersionList := handler3.OpenAPIV3Discovery{}
	if err := json.Unmarshal(data, &groupVersionList); err != nil {
		t.Fatal(err)
	}
	_, ok := groupVersionList.Paths["apis/group.example.com/v1"]
	if !ok {
		t.Error("Expected group.example.com/v1 to be in group version list")
	}

	// 请求根文档后指标应更新。
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(`
# HELP apiserver_request_total [STABLE] Counter of apiserver requests broken out for each verb, dry run value, group, version, resource, scope, component, and HTTP response code.
# TYPE apiserver_request_total counter
apiserver_request_total{code="200",component="",dry_run="",group="",resource="",scope="",subresource="openapi/v3",verb="GET",version=""} 1
`), "apiserver_request_total"); err != nil {
		t.Fatal(err)
	}

	_ = sendReq(t, serveHandler, "/openapi/v3/apis/group.example.com/v1")

	// 请求某个 group/version 的 OpenAPI 后指标应更新。
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(`
# HELP apiserver_request_total [STABLE] Counter of apiserver requests broken out for each verb, dry run value, group, version, resource, scope, component, and HTTP response code.
# TYPE apiserver_request_total counter
apiserver_request_total{code="200",component="",dry_run="",group="",resource="",scope="",subresource="openapi/v3",verb="GET",version=""} 1
apiserver_request_total{code="200",component="",dry_run="",group="",resource="",scope="",subresource="openapi/v3/",verb="GET",version=""} 1
`), "apiserver_request_total"); err != nil {
		t.Fatal(err)
	}

}

// sendReq 用内存 ResponseWriter 直接打 handler，省去起真实 HTTP 服务器；
// 测试只关心聚合器返回的字节内容，不关心网络传输。
func sendReq(t *testing.T, handler http.Handler, path string) []byte {
	req, err := http.NewRequest("GET", path, nil)
	if err != nil {
		t.Fatal(err)
	}
	writer := responsewriter.NewInMemoryResponseWriter()
	handler.ServeHTTP(writer, req)
	return writer.Data()
}

// getTestAPIServiceOpenAPIDefinitions 为测试构造最小的 OpenAPI 定义表，
// 只声明 APIService 这一种类型；忽略 ReferenceCallback 是因为测试定义不需要递归解析引用。
func getTestAPIServiceOpenAPIDefinitions(_ openapicommon.ReferenceCallback) map[string]openapicommon.OpenAPIDefinition {
	return map[string]openapicommon.OpenAPIDefinition{
		"io.k8s.kube-aggregator.pkg.apis.apiregistration.v1.APIService": buildTestAPIServiceOpenAPIDefinition(),
	}
}

// buildTestAPIServiceOpenAPIDefinition 返回一个“够用即可”的类型定义：
// 只描述 Kind 和 object 类型，因为测试断言只比对文档结构，不关心完整字段。
func buildTestAPIServiceOpenAPIDefinition() openapicommon.OpenAPIDefinition {
	return openapicommon.OpenAPIDefinition{
		Schema: kubeopenapispec.Schema{
			SchemaProps: kubeopenapispec.SchemaProps{
				Description: "APIService represents a server for a particular GroupVersion. Name must be \"version.group\".",
				Type:        []string{"object"},
			},
		},
	}
}
