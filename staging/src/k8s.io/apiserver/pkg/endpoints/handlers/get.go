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

package handlers

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metainternalversionvalidation "k8s.io/apimachinery/pkg/apis/meta/internalversion/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/server/routine"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
)

// getterFunc 使用给定的 context 与对象名执行一次 get 请求；
// request 可用于反序列化要传给 getter 的 options 对象。
type getterFunc func(ctx context.Context, name string, req *http.Request) (runtime.Object, error)

// getResourceHandler 是处理 get 请求的 HTTP handler，具体的 get 操作委托给
// 传入的 getterFunc 执行。
// 公共的 GET 骨架：解析命名、协商输出格式、调用 getter、统一写响应；
// 单资源与子资源的差异全部封装在 getterFunc 内。
func getResourceHandler(scope *RequestScope, getter getterFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		ctx, span := tracing.Start(ctx, "Get", traceFields(req)...)
		req = req.WithContext(ctx)
		defer span.End(500 * time.Millisecond)

		namespace, name, err := scope.Namer.Name(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		ctx = request.WithNamespace(ctx, namespace)

		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		result, err := getter(ctx, name, req)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		span.AddEvent("About to write a response")
		defer span.AddEvent("Writing http response done")
		transformResponseObject(ctx, scope, req, w, http.StatusOK, outputMediaType, result)
	}
}

// GetResource 返回从 rest.Storage 对象获取单个资源的函数。
func GetResource(r rest.Getter, scope *RequestScope) http.HandlerFunc {
	return getResourceHandler(scope,
		func(ctx context.Context, name string, req *http.Request) (runtime.Object, error) {
			// 检查 export 参数。
			// export 参数自 v1.14 起被移除；显式传 true 必须报错而不是静默忽略，
			// 以免客户端误以为拿到了脱敏/可移植的对象。
			options := metav1.GetOptions{}
			if values := req.URL.Query(); len(values) > 0 {
				if len(values["export"]) > 0 {
					exportBool := true
					exportStrings := values["export"]
					err := runtime.Convert_Slice_string_To_bool(&exportStrings, &exportBool, nil)
					if err != nil {
						return nil, errors.NewBadRequest(fmt.Sprintf("the export parameter cannot be parsed: %v", err))
					}
					if exportBool {
						return nil, errors.NewBadRequest("the export parameter, deprecated since v1.14, is no longer supported")
					}
				}
				if err := metainternalversionscheme.ParameterCodec.DecodeParameters(values, scope.MetaGroupVersion, &options); err != nil {
					err = errors.NewBadRequest(err.Error())
					return nil, err
				}
			}
			tracing.SpanFromContext(ctx).AddEvent("About to Get from storage")
			return r.Get(ctx, name, &options)
		})
}

// GetResourceWithOptions 返回从 rest.Storage 对象获取单个资源的函数。
//
// 与 GetResource 的区别在于资源自带 options 类型（如 pods/log），且子资源
// 需要把 URL 尾部路径（subpath）作为参数传给存储层。
func GetResourceWithOptions(r rest.GetterWithOptions, scope *RequestScope, isSubresource bool) http.HandlerFunc {
	return getResourceHandler(scope,
		func(ctx context.Context, name string, req *http.Request) (runtime.Object, error) {
			opts, subpath, subpathKey := r.NewGetOptions()
			span := tracing.SpanFromContext(ctx)
			span.AddEvent("About to process Get options")
			if err := getRequestOptions(req, scope, opts, subpath, subpathKey, isSubresource); err != nil {
				err = errors.NewBadRequest(err.Error())
				return nil, err
			}
			span.AddEvent("About to Get from storage")
			return r.Get(ctx, name, opts)
		})
}

// getRequestOptions 解析出 options，其中可能包含路径信息；路径信息不应包含子资源部分。
//
// startingIndex 跳过 /api|apis/<group>/<version> 与 namespaces/<ns> 等固定段，
// 剩余部分才是真正的子路径；因此这里的魔法数字与请求信息解析规则强耦合。
func getRequestOptions(req *http.Request, scope *RequestScope, into runtime.Object, subpath bool, subpathKey string, isSubresource bool) error {
	if into == nil {
		return nil
	}

	query := req.URL.Query()
	if subpath {
		newQuery := make(url.Values)
		for k, v := range query {
			newQuery[k] = v
		}

		ctx := req.Context()
		requestInfo, _ := request.RequestInfoFrom(ctx)
		startingIndex := 2
		if isSubresource {
			startingIndex = 3
		}

		p := strings.Join(requestInfo.Parts[startingIndex:], "/")

		// 保证非空子路径正确反映开头的斜杠。
		if len(p) > 0 && !strings.HasPrefix(p, "/") {
			p = "/" + p
		}

		// 保证子路径正确反映原始请求末尾是否有斜杠。
		if strings.HasSuffix(requestInfo.Path, "/") && !strings.HasSuffix(p, "/") {
			p += "/"
		}

		newQuery[subpathKey] = []string{p}
		query = newQuery
	}
	return scope.ParameterCodec.DecodeParameters(query, scope.Kind.GroupVersion(), into)
}

// ListResource 返回同时服务 list 与 watch 请求的 handler。
// forceWatch 由路由决定（/watch 前缀），用于在缺少 watch 参数时仍走 watch 路径。
func ListResource(r rest.Lister, rw rest.Watcher, scope *RequestScope, forceWatch bool, minRequestTimeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		namespace, err := scope.Namer.Namespace(req)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		ctx = request.WithNamespace(ctx, namespace)

		outputMediaType, _, err := negotiation.NegotiateOutputMediaType(req, scope.Serializer, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}
		opts, err := listOpts(req, scope)
		if err != nil {
			scope.err(err, w, req)
			return
		}

		switch {
		case opts.Watch || forceWatch:
			err = handleWatch(ctx, rw, scope, req, w, opts, outputMediaType, minRequestTimeout)
		default:
			err = handleList(ctx, r, scope, req, w, opts, outputMediaType)
		}
		if err != nil {
			scope.err(err, w, req)
			return
		}
	}
}

// listOpts 解码并校验 list/watch 的查询参数。
func listOpts(req *http.Request, scope *RequestScope) (metainternalversion.ListOptions, error) {
	// 单对象的 watch 也会被路由到这里。
	// name 参数的处理方式与字段选择器条目相同。
	// 单对象 watch 没有独立的 code path，而是通过 metadata.name 字段选择器
	// 让存储层把它优化为精确查询。
	hasName := true
	_, name, err := scope.Namer.Name(req)
	if err != nil {
		hasName = false
	}

	opts := metainternalversion.ListOptions{}
	if err := metainternalversionscheme.ParameterCodec.DecodeParameters(req.URL.Query(), scope.MetaGroupVersion, &opts); err != nil {
		return opts, errors.NewBadRequest(err.Error())
	}

	metainternalversion.SetListOptionsDefaults(&opts, utilfeature.DefaultFeatureGate.Enabled(features.WatchList))
	if errs := metainternalversionvalidation.ValidateListOptions(&opts, utilfeature.DefaultFeatureGate.Enabled(features.WatchList)); len(errs) > 0 {
		err := errors.NewInvalid(schema.GroupKind{Group: metav1.GroupName, Kind: "ListOptions"}, "", errs)
		return opts, err
	}

	// 转换字段。
	// TODO: 这件事本该由 DecodeParametersInto 来完成。
	// 不同资源可自定义字段名（例如把外部字段映射到内部字段），
	// 必须在交给存储层之前完成转换。
	if opts.FieldSelector != nil {
		fn := func(label, value string) (newLabel, newValue string, err error) {
			return scope.Convertor.ConvertFieldLabel(scope.Kind, label, value)
		}
		if opts.FieldSelector, err = opts.FieldSelector.Transform(fn); err != nil {
			// TODO: 让 bad request 能够根据查询参数给出具体原因。
			return opts, errors.NewBadRequest(err.Error())
		}
	}

	if hasName {
		// metadata.name 是规范的内部字段名。SelectionPredicate 会识别出这是
		// 单对象请求，并相应优化存储层查询。
		nameSelector := fields.OneTermEqualSelector("metadata.name", name)

		// 注意：显式设置 metadata.name 的 fieldSelector 也会走到这个分支
		// （该字段的值会作为 name 参数传递到 requestInfo）。
		// 该分支允许的 field selector 只有：nil、fields.Everything，
		// 以及与我们的 name 匹配的 metadata.name 选择器。
		if opts.FieldSelector != nil && !opts.FieldSelector.Empty() {
			selectedName, ok := opts.FieldSelector.RequiresExactMatch("metadata.name")
			if !ok || name != selectedName {
				return opts, errors.NewBadRequest("fieldSelector metadata.name doesn't match requested name")
			}
		} else {
			opts.FieldSelector = nameSelector
		}
	}
	return opts, nil
}

// handleWatch 建立并服务一个 watch（或 watch-list）请求。
// 超时策略：优先使用客户端指定的 timeoutSeconds；否则在
// [minRequestTimeout, 2*minRequestTimeout) 间随机取值，避免大量 watch
// 在同一时刻集体断开重连（惊群）。
func handleWatch(ctx context.Context, rw rest.Watcher, scope *RequestScope, req *http.Request, w http.ResponseWriter, opts metainternalversion.ListOptions, outputMediaType negotiation.MediaTypeOptions, minRequestTimeout time.Duration) error {
	isWatchList := isListWatchRequest(opts)
	var span *tracing.Span
	var onWatchListComplete WatchListCompleteHook
	if isWatchList {
		ctx, span = tracing.Start(ctx, "WatchList", traceFields(req)...)
		onWatchListComplete = func() { span.End(500 * time.Millisecond) }
		req = req.WithContext(ctx)
	}

	if rw == nil {
		return errors.NewMethodNotSupported(scope.Resource.GroupResource(), "watch")
	}
	// TODO: 目前我们显式忽略 ?timeout=，只使用 ?timeoutSeconds=。
	timeout := time.Duration(0)
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	if timeout == 0 && minRequestTimeout > 0 {
		// 随机化超时，分散客户端重连时间。
		timeout = time.Duration(float64(minRequestTimeout) * (rand.Float64() + 1.0))
	}

	klog.V(3).InfoS("Starting watch", "path", req.URL.Path, "resourceVersion", opts.ResourceVersion, "labels", opts.LabelSelector, "fields", opts.FieldSelector, "sendInitialEvents", opts.SendInitialEvents, "timeout", timeout, "audit-ID", audit.GetAuditIDTruncated(ctx))
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer func() { cancel() }()
	watcher, err := rw.Watch(ctx, &opts)
	if err != nil {
		return err
	}
	handler, err := serveWatchHandler(watcher, scope, outputMediaType, req, w, timeout, metrics.CleanListScope(ctx, &opts), isWatchList, onWatchListComplete)
	if err != nil {
		return err
	}
	// 使 cancel() 失效，直到 serve() 完成才取消。
	// 超时上下文必须覆盖整个 watch 服务过程，而不是在 handleWatch 返回时取消；
	// 否则 watch 刚建立就会被立刻中止。
	deferredCancel := cancel
	cancel = func() {}

	serve := func() {
		defer deferredCancel()
		requestInfo, _ := request.RequestInfoFrom(ctx)
		metrics.RecordLongRunning(req, requestInfo, metrics.APIServerComponent, func() {
			defer watcher.Stop()
			handler.ServeHTTP(w, req)
		})
	}

	// 在独立 goroutine 中运行 watch 服务，以便释放当前栈内存。
	// 有 Task 时交给 routine 中间件在独立 goroutine 执行，否则就地执行。
	t := routine.TaskFrom(req.Context())
	if t != nil {
		t.Func = serve
	} else {
		serve()
	}
	return nil
}

// handleList 处理非 watch 的 list 请求。
func handleList(ctx context.Context, r rest.Lister, scope *RequestScope, req *http.Request, w http.ResponseWriter, opts metainternalversion.ListOptions, outputMediaType negotiation.MediaTypeOptions) error {
	// 用于性能追踪。
	ctx, span := tracing.Start(ctx, "List", traceFields(req)...)
	defer span.End(500 * time.Millisecond)
	req = req.WithContext(ctx)

	result, err := r.List(ctx, &opts)
	if err != nil {
		return err
	}
	transformResponseObject(ctx, scope, req, w, http.StatusOK, outputMediaType, result)
	return nil
}

// isListWatchRequest 的实现与 staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go 中保持一致。
//
// 判定条件与 cacher 中必须保持一致，否则追踪 span 的开启/结束会与实际
// 的 watch-list 语义错位。修改此处需同步修改 cacher。
func isListWatchRequest(opts metainternalversion.ListOptions) bool {
	return opts.SendInitialEvents != nil && *opts.SendInitialEvents && opts.AllowWatchBookmarks
}
