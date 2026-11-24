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

	// ensure types are installed
	_ "k8s.io/kubernetes/pkg/apis/core/install"
)

// LogREST 实现 pods/{name}/log 子资源：将请求参数转换为 Kubelet 日志 URL，并通过流式响应返回日志内容。
type LogREST struct {
	KubeletConn client.ConnectionInfoGetter
	Store       *genericregistry.Store
}

// LogREST implements GetterWithOptions
var _ = rest.GetterWithOptions(&LogREST{})

// New creates a new Pod log options object
func (r *LogREST) New() runtime.Object {
	// TODO - return a resource that represents a log
	return &api.Pod{}
}

// Destroy cleans up resources on shutdown.
func (r *LogREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// ProducesMIMETypes returns a list of the MIME types the specified HTTP verb (GET, POST, DELETE,
// PATCH) can respond with.
func (r *LogREST) ProducesMIMETypes(verb string) []string {
	// Since the default list does not include "plain/text", we need to
	// explicitly override ProducesMIMETypes, so that it gets added to
	// the "produces" section for pods/{name}/log
	return []string{
		"text/plain",
	}
}

// ProducesObject returns an object the specified HTTP verb respond with. It will overwrite storage object if
// it is not nil. Only the type of the return object matters, the value will be ignored.
func (r *LogREST) ProducesObject(verb string) interface{} {
	return ""
}

// Get retrieves a runtime.Object that will stream the contents of the pod log
// Get 校验 PodLogOptions、记录 TLS 参数用法指标，最后构造 LocationStreamer 代理 Kubelet 日志响应。
func (r *LogREST) Get(ctx context.Context, name string, opts runtime.Object) (runtime.Object, error) {
	// register the metrics if the context is used.  This assumes sync.Once is fast.  If it's not, it could be an init block.
	registerMetrics()

	// API Handler 应传入 PodLogOptions；类型不匹配表示端点参数装配错误。
	logOpts, ok := opts.(*api.PodLogOptions)
	if !ok {
		return nil, fmt.Errorf("invalid options object: %#v", opts)
	}

	countSkipTLSMetric(logOpts.InsecureSkipTLSVerifyBackend)

	// 新分流能力启用时，即使旧客户端未传 stream，也默认读取 All，保持历史接口语义。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodLogsQuerySplitStreams) {
		// Even with defaulters, logOpts.Stream can be nil if no arguments are provided at all.
		if logOpts.Stream == nil {
			// Default to "All" to maintain backward compatibility.
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

// NewGetOptions creates a new options object
func (r *LogREST) NewGetOptions() (runtime.Object, bool, string) {
	return &api.PodLogOptions{}, false, ""
}

// OverrideMetricsVerb override the GET verb to CONNECT for pod log resource
func (r *LogREST) OverrideMetricsVerb(oldVerb string) (newVerb string) {
	newVerb = oldVerb

	if oldVerb == "GET" {
		newVerb = "CONNECT"
	}

	return
}
