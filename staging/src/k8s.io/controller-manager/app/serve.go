/*
Copyright 2018 The Kubernetes Authors.

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

package app

import (
	"net/http"
	goruntime "runtime"

	genericapifilters "k8s.io/apiserver/pkg/endpoints/filters"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	apiserver "k8s.io/apiserver/pkg/server"
	genericfilters "k8s.io/apiserver/pkg/server/filters"
	"k8s.io/apiserver/pkg/server/mux"
	"k8s.io/apiserver/pkg/server/routes"
	"k8s.io/client-go/kubernetes/scheme"
	componentbaseconfig "k8s.io/component-base/config"
	"k8s.io/component-base/configz"
	"k8s.io/component-base/logs"
	"k8s.io/component-base/metrics/legacyregistry"
	_ "k8s.io/component-base/metrics/prometheus/workqueue" // for workqueue metric registration
)

// BuildHandlerChain builds a handler chain with a base handler and CompletedConfig.
// BuildHandlerChain 把核心 API handler 包上一层通用的 HTTP 处理链。
// 调用方给出的授权/认证信息可选；最终从内到外的顺序为：
// 业务 handler -> 授权 -> 认证 -> 请求信息解析 -> Cache-Control -> 访问日志 -> panic 恢复。
// 注意 handler 是层层包裹的：后包裹的先看到请求，因此 panic 恢复位于最外层。
func BuildHandlerChain(apiHandler http.Handler, authorizationInfo *apiserver.AuthorizationInfo, authenticationInfo *apiserver.AuthenticationInfo) http.Handler {
	requestInfoResolver := &apirequest.RequestInfoFactory{}
	// 负责从 URL 中解析出 API 请求信息（组、版本、资源、动作等）。
	failedHandler := genericapifilters.Unauthorized(scheme.Codecs)
	// 认证失败时使用的处理器：返回 401 并写入统一的错误响应。

	handler := apiHandler
	// 从最内层的业务 handler 开始逐层向外包装。
	if authorizationInfo != nil {
		// 只有提供了授权器时才插入授权过滤器，避免误拒绝请求。
		handler = genericapifilters.WithAuthorization(apiHandler, authorizationInfo.Authorizer, scheme.Codecs)
	}
	if authenticationInfo != nil {
		// 只有提供了认证器时才插入认证过滤器；认证失败交给 failedHandler。
		handler = genericapifilters.WithAuthentication(handler, authenticationInfo.Authenticator, failedHandler, nil, nil)
	}
	// 把解析出的 RequestInfo 注入请求上下文，后续过滤器和审计都依赖它。
	handler = genericapifilters.WithRequestInfo(handler, requestInfoResolver)
	// 为响应加上合适的 Cache-Control 头，避免中间代理缓存 API 响应。
	handler = genericapifilters.WithCacheControl(handler)
	// 记录 HTTP 访问日志。
	handler = genericfilters.WithHTTPLogging(handler)
	// 最外层兜底：捕获 panic 并返回 500，同时用 RequestInfo 丰富日志上下文。
	handler = genericfilters.WithPanicRecovery(handler, requestInfoResolver)

	return handler
}

// NewBaseHandler takes in CompletedConfig and returns a handler.
// NewBaseHandler 构造 controller-manager 的基础 HTTP 路由（不含业务 API handler）。
// 路由包括：/healthz、/configz、/metrics，以及可选的采集/调试端点。
func NewBaseHandler(c *componentbaseconfig.DebuggingConfiguration, healthzHandler http.Handler) *mux.PathRecorderMux {
	mux := mux.NewPathRecorderMux("controller-manager")
	// 使用会记录已注册路径的 mux，便于重复注册时给出清晰错误。
	mux.Handle("/healthz", healthzHandler)
	// 健康检查由调用方传入，便于控制器在运行期继续追加检查项。
	if c.EnableProfiling {
		// 只有显式开启时才暴露 pprof 等调试端点。
		routes.Profiling{}.Install(mux)
		// 挂载 /debug/pprof/ 系列端点。
		if c.EnableContentionProfiling {
			// 锁竞争采集开销较大，必须单独开启。
			goruntime.SetBlockProfileRate(1)
			// 采样所有阻塞事件（采样率 1），否则 pprof 的 block  profile 没有数据。
		}
		// 允许通过 HTTP PUT /debug/flags/v 动态修改日志详细级别。
		routes.DebugFlags{}.Install(mux, "v", routes.StringFlagPutHandler(logs.GlogSetter))
	}
	// 挂载 /configz，用于查看当前生效配置（便于问题排查）。
	configz.InstallHandler(mux)
	// 暴露 Prometheus 指标端点；workqueue 指标通过本文件的空导入完成注册。
	mux.Handle("/metrics", legacyregistry.Handler())

	return mux
}
