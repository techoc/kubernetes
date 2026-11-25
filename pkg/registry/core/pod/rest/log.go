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

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	genericrest "k8s.io/apiserver/pkg/registry/generic/rest"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/registry/core/pod"
	"k8s.io/utils/ptr"

	// 确保相关 API 类型已安装。
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

// LogREST 实现 pods/{name}/log 子资源：将请求参数转换为 Kubelet 日志 URL，并通过流式响应返回日志内容。
type LogREST struct {
	KubeletConn client.ConnectionInfoGetter
	Store       *genericregistry.Store
}

// LogREST 实现 rest.GetterWithOptions 接口。
var _ = rest.GetterWithOptions(&LogREST{})

// New 返回用于承载日志子资源响应的空 Pod 对象。
func (r *LogREST) New() runtime.Object {
	// TODO：返回能够表示日志的资源对象。
	return &api.Pod{}
}

// Destroy 在关闭时清理资源。
func (r *LogREST) Destroy() {
	// 底层 Store 与其他 REST 实现共享，因此这里不显式销毁它。
}

// ProducesMIMETypes 返回指定 HTTP 动词（GET、POST、DELETE、PATCH）可以响应的 MIME 类型。
func (r *LogREST) ProducesMIMETypes(verb string) []string {
	// 默认列表不含 "text/plain"，需要显式覆盖 ProducesMIMETypes，将其加入 pods/{name}/log 的 "produces" 声明。
	return []string{
		"text/plain",
	}
}

// ProducesObject 返回指定 HTTP 动词的响应对象类型；非 nil 时会覆盖存储对象，但只有返回类型生效，值会被忽略。
func (r *LogREST) ProducesObject(verb string) interface{} {
	return ""
}

// Get 校验 PodLogOptions、记录 TLS 参数用法指标，最后构造 LocationStreamer 代理 Kubelet 日志响应。
func (r *LogREST) Get(ctx context.Context, name string, opts runtime.Object) (runtime.Object, error) {
	// 首次使用时注册指标；sync.Once 应足够快，否则可改为 init 阶段注册。
	registerMetrics()

	// API Handler 应传入 PodLogOptions；类型不匹配表示端点参数装配错误。
	logOpts, ok := opts.(*api.PodLogOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}

	countSkipTLSMetric(logOpts.InsecureSkipTLSVerifyBackend)

	// 新分流能力启用时，即使旧客户端未传 stream，也默认读取 All，保持历史接口语义。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams) {
		// 即使经过默认值处理，完全未传参数时 logOpts.Stream 仍可能为 nil。
		if logOpts.Stream == nil {
			// 默认使用 "All"，保持向后兼容。
			logOpts.Stream = ptr.To(api.LogStreamAll)
		}
	}
	// 先校验 since/tail/follow/stream 等组合，避免把不合法查询转发到节点后才失败。
	if errs := validation.ValidatePodLogOptions(logOpts, utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams)); len(errs) > 0 {
		return nil, errors.NewInvalid(api.Kind("PodLogOptions"), name, errs)
	}
	// 根据 Pod 的 NodeName、容器名和查询参数生成 Kubelet URL，并选择是否跳过后端 TLS 校验的 Transport。
	location, transport, err := pod.LogLocation(ctx, r.Store, r.KubeletConn, name, logOpts)
	if err != nil {
		return nil, err
	}
	// LocationStreamer 负责转发响应体；Follow 场景启用 Flush，使日志增量尽快到达客户端。
	return &genericrest.LocationStreamer{
		Location:                    location,
		Transport:                   transport,
		ContentType:                 "text/plain",
		Flush:                       logOpts.Follow,
		ResponseChecker:             genericrest.NewGenericHttpResponseChecker(api.Resource("pods/log"), name),
		RedirectChecker:             genericrest.PreventRedirects,
		TLSVerificationErrorCounter: podLogsTLSFailure,
	}, nil
}

// countSkipTLSMetric 按强制 TLS 或允许跳过 TLS 两类统计后端安全配置的使用量。
func countSkipTLSMetric(insecureSkipTLSVerifyBackend bool) {
	usageType := usageEnforce
	if insecureSkipTLSVerifyBackend {
		usageType = usageSkipAllowed
	}

	counter, err := podLogsUsage.GetMetricWithLabelValues(usageType)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	counter.Inc()
}

// NewGetOptions 返回新的 Pod 日志查询选项对象。
func (r *LogREST) NewGetOptions() (runtime.Object, bool, string) {
	return &api.PodLogOptions{}, false, ""
}

// OverrideMetricsVerb 将 Pod 日志资源的 GET 动词替换为 CONNECT，用于指标统计。
func (r *LogREST) OverrideMetricsVerb(oldVerb string) (newVerb string) {
	newVerb = oldVerb

	if oldVerb == "GET" {
		newVerb = "CONNECT"
	}

	return
}
