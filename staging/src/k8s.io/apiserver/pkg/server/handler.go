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

package server

import (
	"bytes"
	"fmt"
	"net/http"
	rt "runtime"
	"sort"
	"strings"

	"github.com/emicklei/go-restful/v3"
	"k8s.io/klog/v2"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/server/mux"
)

// APIServerHandlers 持有 API server 使用的各种 http.Handler。
// 包括完整的 handler 链、director（在 gorestful 与非 gorestful 之间选择）、
// gorestful handler（用于 API，未注册路径会落到非 gorestful handler），
// 以及非 gorestful handler（自身也可能继续向下传递）。
// FullHandlerChain -> Director -> {GoRestfulContainer,NonGoRestfulMux}，由已注册的 web service 决定分发。
//
// 分层原因：gorestful 无法在不破坏 swagger 的前提下做通配路由，所以用一个 director
// 在其前面做路由判定，未匹配的请求交给普通 mux 处理。
type APIServerHandler struct {
	// FullHandlerChain 是最终处理请求的 handler，应包含完整过滤器链并调用 Director。
	// 然后调用 Director。
	FullHandlerChain http.Handler
	// 已注册的 API。InstallAPIs 使用此字段；其他 server 一般不应直接访问。
	GoRestfulContainer *restful.Container
	// NonGoRestfulMux 是链中的最后一个 HTTP handler。
	// 位于所有过滤器与 API 处理之后。
	// 其它 server 可在此把 handler 挂到链的不同位置。
	NonGoRestfulMux *mux.PathRecorderMux

	// Director 用于正确处理请求下传与代理的情况。
	// 看起来有点离谱，但背景是这样的：为兼容 swagger 生成，必须在 gorestful 中注册 /apis。
	// 把 `/apis` 注册为 webservice 后，它会对所有非 /apis 与 /apis/ 的请求强制返回 404；
	// 这些请求需要穿透 gorestful 才能正确委托给其它 handler。
	// 注册一个覆盖所有子路径的 pattern 也行不通，因为 gorestful 会协商动词与内容类型，
	// 而它真正需要做的只是透传；此外 openapi 强制动词唯一，我们并不符合这一约束，
	// 这还会把 swagger 搞乱。把 webservice 换成 route 也不行，因为
	// 包含 webservice 会面临上述所有问题。
	// 于是有了这里的“疯狂”做法：我们的 mux 能做所需的事，因此把它放在 gorestful 前面。
	// 它先判断路由是否可能由 gorestful 处理，是则转过去，否则交给 NonGoRestfulMux
	// 处理“普通”路径与委托。希望 API 消费者永远不必了解这一层细节。
	// 或许我们该考虑彻底移除 gorestful。
	// 其它 server 只应把它当作不透明的委托入口。
	Director http.Handler
}

// HandlerChainBuilderFn 用给定的 handler 链包装 GoRestfulContainer handler。
// 通常用于套上认证与授权等过滤。
type HandlerChainBuilderFn func(apiHandler http.Handler) http.Handler

// NewAPIServerHandler 组装 director、gorestful 容器与非 gorestful mux。
// handlerChainBuilder 用于在最外层套上鉴权/审计等过滤器，保证所有路径（包括 404）都经过它们。
func NewAPIServerHandler(name string, s runtime.NegotiatedSerializer, handlerChainBuilder HandlerChainBuilderFn, notFoundHandler http.Handler) *APIServerHandler {
	nonGoRestfulMux := mux.NewPathRecorderMux(name)
	if notFoundHandler != nil {
		nonGoRestfulMux.NotFoundHandler(notFoundHandler)
	}

	gorestfulContainer := restful.NewContainer()
	gorestfulContainer.Router(restful.CurlyRouter{}) // 例如 proxy/{kind}/{name}/{*}。
	gorestfulContainer.RecoverHandler(func(panicReason interface{}, httpWriter http.ResponseWriter) {
		logStackOnRecover(s, panicReason, httpWriter)
	})
	gorestfulContainer.ServiceErrorHandler(func(serviceErr restful.ServiceError, request *restful.Request, response *restful.Response) {
		serviceErrorHandler(s, serviceErr, request, response)
	})

	director := director{
		name:               name,
		goRestfulContainer: gorestfulContainer,
		nonGoRestfulMux:    nonGoRestfulMux,
	}

	return &APIServerHandler{
		FullHandlerChain:   handlerChainBuilder(director),
		GoRestfulContainer: gorestfulContainer,
		NonGoRestfulMux:    nonGoRestfulMux,
		Director:           director,
	}
}

// ListedPaths 返回应在 / 下展示的路径。
func (a *APIServerHandler) ListedPaths() []string {
	var handledPaths []string
	// 提取通过 restful.WebService 处理的路径。
	for _, ws := range a.GoRestfulContainer.RegisteredWebServices() {
		handledPaths = append(handledPaths, ws.RootPath())
	}
	handledPaths = append(handledPaths, a.NonGoRestfulMux.ListedPaths()...)
	// 排序保证 / 索引输出稳定，便于用户与测试比较。
	sort.Strings(handledPaths)

	return handledPaths
}

// director 按注册的 web service 根路径决定请求交给 gorestful 还是普通 mux。
type director struct {
	name               string
	goRestfulContainer *restful.Container
	nonGoRestfulMux    *mux.PathRecorderMux
}

// ServeHTTP 实现路由分发。注意 /apis 需要特殊处理，见上方 Director 字段的长注释。
func (d director) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path

	// 检查我们的 webservice 是否要认领该路径。
	for _, ws := range d.goRestfulContainer.RegisteredWebServices() {
		switch {
		case ws.RootPath() == "/apis":
			// 若恰好是 /apis 或 /apis/，需要特殊处理。
			// 通常这些请求交给 nonGoRestfulMux，但启用发现时会直接处理。
			// 不能依赖前缀匹配，因为 /apis 匹配一切（见上方 Director 的长注释）。
			if path == "/apis" || path == "/apis/" {
				klog.V(5).Infof("%v: %v %q satisfied by gorestful with webservice %v", d.name, req.Method, path, ws.RootPath())
				// 这里不用 servemux：移除 webservice 时 gorestful 的 servemux 会被搞乱。
				// TODO 修复 gorestful、移除 TPR，或停止使用 gorestful。
				d.goRestfulContainer.Dispatch(w, req)
				return
			}

		case strings.HasPrefix(path, ws.RootPath()):
			// 要求精确匹配或路径边界匹配。
			//
			// 仅前缀匹配不够：/apis/appsfoo 不应被 /apis/apps 认领，
			// 因此要求下一字符是 '/' 或路径已结束。
			if len(path) == len(ws.RootPath()) || path[len(ws.RootPath())] == '/' {
				klog.V(5).Infof("%v: %v %q satisfied by gorestful with webservice %v", d.name, req.Method, path, ws.RootPath())
				// 这里不用 servemux：移除 webservice 时 gorestful 的 servemux 会被搞乱。
				// TODO 修复 gorestful、移除 TPR，或停止使用 gorestful。
				d.goRestfulContainer.Dispatch(w, req)
				return
			}
		}
	}

	// 若未找到匹配，就完全跳过 gorestful。
	klog.V(5).Infof("%v: %v %q satisfied by nonGoRestful", d.name, req.Method, path)
	d.nonGoRestfulMux.ServeHTTP(w, req)
}

// logStackOnRecover 是 gorestful 的 panic 恢复钩子：记录完整调用栈并返回 500。
// 用独立的 buffer 一次性输出，避免 panic 路径上的多行日志交错。
// TODO: 与 RecoverPanics 统一？
func logStackOnRecover(s runtime.NegotiatedSerializer, panicReason interface{}, w http.ResponseWriter) {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("recover from panic situation: - %v\r\n", panicReason))
	for i := 2; ; i++ {
		_, file, line, ok := rt.Caller(i)
		if !ok {
			break
		}
		buffer.WriteString(fmt.Sprintf("    %s:%d\r\n", file, line))
	}
	klog.Errorln(buffer.String())

	headers := http.Header{}
	if ct := w.Header().Get("Content-Type"); len(ct) > 0 {
		headers.Set("Accept", ct)
	}
	responsewriters.ErrorNegotiated(apierrors.NewGenericServerResponse(http.StatusInternalServerError, "", schema.GroupResource{}, "", "", 0, false), s, schema.GroupVersion{}, w, &http.Request{Header: headers})
}

// serviceErrorHandler 把 gorestful 内部错误转换为标准 Status 响应，
// 保证错误格式与 API 其他部分一致（客户端只需处理一种错误结构）。
func serviceErrorHandler(s runtime.NegotiatedSerializer, serviceErr restful.ServiceError, request *restful.Request, resp *restful.Response) {
	responsewriters.ErrorNegotiated(
		apierrors.NewGenericServerResponse(serviceErr.Code, "", schema.GroupResource{}, "", serviceErr.Message, 0, false),
		s,
		schema.GroupVersion{},
		resp,
		request.Request,
	)
}

// ServeHTTP 使其成为 http.Handler。
func (a *APIServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.FullHandlerChain.ServeHTTP(w, r)
}
