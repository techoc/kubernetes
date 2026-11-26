/*
Copyright 2023 The Kubernetes Authors.

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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bytes"

	v1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// 聚合文档至少要保证两件事：被委托方已有的路径还在，聚合器自身的
// apiregistration 路径也被并进来。这条是最小的冒烟测试，
// 后面的用例都在这个基线上叠加增删改。
func TestBasicPathsMerged(t *testing.T) {
	mux := http.NewServeMux()
	delegationHandlers := []http.Handler{
		&openAPIHandler{
			openapi: &spec.Swagger{
				SwaggerProps: spec.SwaggerProps{
					Paths: &spec.Paths{
						Paths: map[string]spec.PathItem{
							"/apis/foo/v1/": {},
						},
					},
				},
			},
		},
	}
	buildAndRegisterSpecAggregator(delegationHandlers, mux)

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/foo/v1/")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")
}

// 这个用例把 ADD 和 UPDATE 串起来：先注册一个 APIService 的路径，
// 再替换它的 spec 并重新触发同步，最后确认旧路径消失、新路径出现。
// 关键是验证更新走的是"替换"而不是"追加"，否则过期路径会一直挂在聚合文档里。
func TestAddUpdateAPIService(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)
	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "apiservicegroup",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiService.Name = "apiservice"

	handler := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/apiservicegroup/v1/path1": {},
				},
			},
		},
	}}

	if err := s.AddUpdateAPIService(apiService, handler); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err != nil {
		t.Error(err)
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}

	expectPath(t, swagger, "/apis/apiservicegroup/v1/path1")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")

	t.Log("Update APIService OpenAPI")
	handler.openapi = &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/apiservicegroup/v1/path2": {},
				},
			},
		},
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err != nil {
		t.Error(err)
	}

	swagger, err = fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	// 验证 APIService 的 OpenAPI 更新后，聚合结果也随之更新。
	expectPath(t, swagger, "/apis/apiservicegroup/v1/path2")
	expectNoPath(t, swagger, "/apis/apiservicegroup/v1/path1")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")
}

// 验证注册 OpenAPI 的 APIService 只提供其自身 GroupVersion 的 OpenAPI。
// 背景见 https://github.com/kubernetes/kubernetes/pull/123570。
// 后端服务可能顺带暴露多个 group/version 的文档，但一个 APIService 只对应其中一个。
// 这里刻意让两个 APIService 的 handler 各自夹带不属于自己的路径，
// 验证聚合器按 APIService 声明的 GroupVersion 做裁剪，而不是照单全收。
// 这也是上面那个 PR 修复的问题：放任不相关路径进入聚合文档会造成路由冲突。
func TestAPIServiceOpenAPIServiceMismatch(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1/": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)

	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "apiservicegroup",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiService.Name = "apiservice"

	apiService2 := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "apiservicegroup",
			Version: "v2",
			Service: &v1.ServiceReference{Name: "dummy2"},
		},
	}
	apiService2.Name = "apiservice2"

	handler := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/apiservicegroup/v1/":      {},
					"/apis/apiservicegroup/v1beta1/": {},
				},
			},
		},
	}}

	handler2 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/a/":                  {},
					"/apis/apiservicegroup/v1/": {},
					"/apis/apiservicegroup/v2/": {},
				},
			},
		},
	}}

	if err := s.AddUpdateAPIService(apiService, handler); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err != nil {
		t.Error(err)
	}

	if err := s.AddUpdateAPIService(apiService2, handler2); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService2.Name); err != nil {
		t.Error(err)
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/apiservicegroup/v1/")
	expectPath(t, swagger, "/apis/apiservicegroup/v2/")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")
	expectNoPath(t, swagger, "/apis/a/")
	expectNoPath(t, swagger, "/apis/apiservicegroup/v1beta1/")

	t.Logf("Remove APIService %s", apiService.Name)
	s.RemoveAPIService(apiService.Name)

	swagger, err = fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	// 验证 APIService 加入后再移除时，其 OpenAPI 也从聚合结果中消失。
	expectNoPath(t, swagger, "/apis/apiservicegroup/v1")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")
	expectNoPath(t, swagger, "/apis/a")
}

// 删除 APIService 时，它贡献的路径必须从聚合文档里同步消失，
// 而聚合器自身的路径要保留。否则后端下线后，文档里还留着访问不到的接口。
func TestAddRemoveAPIService(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1/": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)

	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "apiservicegroup",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiService.Name = "apiservice"

	handler := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/apiservicegroup/v1/": {},
				},
			},
		},
	}}

	if err := s.AddUpdateAPIService(apiService, handler); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err != nil {
		t.Error(err)
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/apiservicegroup/v1/")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")

	t.Logf("Remove APIService %s", apiService.Name)
	s.RemoveAPIService(apiService.Name)

	swagger, err = fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	// 验证 APIService 加入后再移除时，其 OpenAPI 也从聚合结果中消失。
	expectNoPath(t, swagger, "/apis/apiservicegroup/v1/")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")
}

// 和 TestAddUpdateAPIService 不同，这里让同一个 APIService 换一个 handler，
// 模拟后端 Pod 被替换、但 APIService 对象本身没变的场景。
// 聚合器必须认出"名字没变、来源变了"，用新 handler 的文档覆盖旧的。
func TestUpdateAPIService(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1/": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)

	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "apiservicegroup",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiService.Name = "apiservice"

	handler := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/apiservicegroup/v1/": {},
				},
			},
		},
	}}

	handler2 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{},
			},
		},
	}}

	if err := s.AddUpdateAPIService(apiService, handler); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err != nil {
		t.Error(err)
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/apiservicegroup/v1/")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")

	t.Logf("Updating APIService %s", apiService.Name)
	if err := s.AddUpdateAPIService(apiService, handler2); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err != nil {
		t.Error(err)
	}

	swagger, err = fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	// 验证 APIService 加入后 handler 内容变化时，聚合结果反映新数据。
	expectNoPath(t, swagger, "/apis/apiservicegroup/v1/")
	expectPath(t, swagger, "/apis/apiregistration.k8s.io/v1/")
}

// 某个后端拉取失败时，不能让它拖垮整个聚合文档：
// 失败的那个跳过，其余正常后端的路径照常出现。这里同时断言同步该 APIService
// 会返回错误，方便调用方决定是否重试，而不是静默吞掉失败。
func TestFailingAPIServiceSkippedAggregation(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1/": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)

	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiServiceFailed := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "failed",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiServiceFailed.Name = "apiserviceFailed"

	handlerFailed := &openAPIHandler{
		returnErr: true,
		openapi: &spec.Swagger{
			SwaggerProps: spec.SwaggerProps{
				Paths: &spec.Paths{
					Paths: map[string]spec.PathItem{
						"/apis/failed/v1/": {},
					},
				},
			},
		},
	}

	apiServiceSuccess := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "success",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy2"},
		},
	}
	apiServiceSuccess.Name = "apiserviceSuccess"

	handlerSuccess := &openAPIHandler{
		openapi: &spec.Swagger{
			SwaggerProps: spec.SwaggerProps{
				Paths: &spec.Paths{
					Paths: map[string]spec.PathItem{
						"/apis/success/v1/": {},
					},
				},
			},
		},
	}

	if err := s.AddUpdateAPIService(apiServiceSuccess, handlerSuccess); err != nil {
		t.Error(err)
	}
	if err := s.AddUpdateAPIService(apiServiceFailed, handlerFailed); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiServiceSuccess.Name); err != nil {
		t.Error(err)
	}
	err := s.UpdateAPIServiceSpec(apiServiceFailed.Name)
	if err == nil {
		t.Errorf("Expected updating failing apiService %s to return error", apiServiceFailed.Name)
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/foo/v1/")
	expectNoPath(t, swagger, "/apis/failed/v1/")
	expectPath(t, swagger, "/apis/success/v1/")
}

// 后端从失败恢复到正常之后，它贡献的路径要重新出现在聚合文档里。
// 这个用例盯的是状态翻转：失败时的"跳过"不能被缓存成永久决定。
func TestAPIServiceFailSuccessTransition(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1/": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)

	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiService := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "apiservicegroup",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiService.Name = "apiservice"

	handler := &openAPIHandler{
		returnErr: true,
		openapi: &spec.Swagger{
			SwaggerProps: spec.SwaggerProps{
				Paths: &spec.Paths{
					Paths: map[string]spec.PathItem{
						"/apis/apiservicegroup/v1/": {},
					},
				},
			},
		},
	}

	if err := s.AddUpdateAPIService(apiService, handler); err != nil {
		t.Error(err)
	}
	if err := s.UpdateAPIServiceSpec(apiService.Name); err == nil {
		t.Errorf("Expected error for when updating spec for failing apiservice")
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/foo/v1/")
	expectNoPath(t, swagger, "/apis/apiservicegroup/v1/")

	t.Log("Transition APIService to not return error")
	handler.returnErr = false
	err = s.UpdateAPIServiceSpec(apiService.Name)
	if err != nil {
		t.Error(err)
	}
	swagger, err = fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/foo/v1/")
	expectPath(t, swagger, "/apis/apiservicegroup/v1/")
}

// 注册 APIService 是元数据操作，不该同步等待后端返回文档：
// 后端可能很慢甚至挂掉，同步等待会把整个聚合器的控制循环卡住。
// 这里用 5 秒延迟的 handler 和 2 秒超时把这条约束钉死。
func TestFailingAPIServiceDoesNotBlockAdd(t *testing.T) {
	mux := http.NewServeMux()
	var delegationHandlers []http.Handler
	delegate1 := &openAPIHandler{openapi: &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/foo/v1/": {},
				},
			},
		},
	}}
	delegationHandlers = append(delegationHandlers, delegate1)

	s := buildAndRegisterSpecAggregator(delegationHandlers, mux)

	apiServiceFailed := &v1.APIService{
		Spec: v1.APIServiceSpec{
			Group:   "failed",
			Version: "v1",
			Service: &v1.ServiceReference{Name: "dummy"},
		},
	}
	apiServiceFailed.Name = "apiserviceFailed"

	// 构造一个响应很慢的 handler，验证添加 APIService 不会被阻塞。
	handlerFailed := &openAPIHandler{
		delaySeconds: 5,
		returnErr:    true,
		openapi: &spec.Swagger{
			SwaggerProps: spec.SwaggerProps{
				Paths: &spec.Paths{
					Paths: map[string]spec.PathItem{
						"/apis/failed/v1/": {},
					},
				},
			},
		},
	}

	updateDone := make(chan bool)
	go func() {
		if err := s.AddUpdateAPIService(apiServiceFailed, handlerFailed); err != nil {
			t.Error(err)
		}
		close(updateDone)
	}()

	select {
	case <-updateDone:
	case <-time.After(2 * time.Second):
		t.Errorf("AddUpdateAPIService affected by APIService response time")
	}

	swagger, err := fetchOpenAPI(mux)
	if err != nil {
		t.Error(err)
	}
	expectPath(t, swagger, "/apis/foo/v1/")
	expectNoPath(t, swagger, "/apis/failed/v1/")
}

// 这个假后端把"慢响应""返回错误""正常返回"三种行为放在一个结构里，
// 测试只要改字段就能组合出需要的场景，不必为每个场景写一个新的 handler。
type openAPIHandler struct {
	delaySeconds int
	openapi      *spec.Swagger
	returnErr    bool
}

func (o *openAPIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(o.delaySeconds) * time.Second)
	if o.returnErr {
		w.WriteHeader(500)
		return
	}
	data, err := json.Marshal(o.openapi)
	if err != nil {
		panic(err)
	}
	http.ServeContent(w, r, "/openapi/v2", time.Now(), bytes.NewReader(data))
	return
}

// 每次请求都建一个临时 server，是为了让测试直接走真实的 HTTP 路径，
// 而不是绕过 handler 去读内部结构——聚合文档最终就是经这条路径交给客户端的。
func fetchOpenAPI(mux *http.ServeMux) (*spec.Swagger, error) {
	server := httptest.NewServer(mux)
	defer server.Close()
	client := server.Client()

	req, err := http.NewRequest("GET", server.URL+"/openapi/v2", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(resp.Body)

	swagger := &spec.Swagger{}
	if err := swagger.UnmarshalJSON(body); err != nil {
		return nil, err
	}
	return swagger, err
}

// 把"构造聚合器 + 注册路由"收成一个函数，保证每个用例测到的都是
// 客户端真正能访问到的那套 handler，避免有人忘了调 register 而测了个空壳。
func buildAndRegisterSpecAggregator(delegationHandlers []http.Handler, mux common.PathHandler) *specAggregator {
	downloader := NewDownloader()
	aggregatorSpec := &spec.Swagger{
		SwaggerProps: spec.SwaggerProps{
			Paths: &spec.Paths{
				Paths: map[string]spec.PathItem{
					"/apis/apiregistration.k8s.io/v1/": {},
				},
			},
		},
	}
	s := buildAndRegisterSpecAggregatorForLocalServices(&downloader, aggregatorSpec, delegationHandlers, mux)
	return s
}

func expectPath(t *testing.T, swagger *spec.Swagger, path string) {
	if _, ok := swagger.Paths.Paths[path]; !ok {
		t.Errorf("Expected path %s to exist in aggregated paths", path)
	}
}

func expectNoPath(t *testing.T, swagger *spec.Swagger, path string) {
	if _, ok := swagger.Paths.Paths[path]; ok {
		t.Errorf("Expected path %s to be omitted in aggregated paths", path)
	}
}
