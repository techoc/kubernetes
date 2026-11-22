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

// getterFunc performs a get request with the given context and object name. The request
// may be used to deserialize an options object to pass to the getter.
type getterFunc func(ctx context.Context, name string, req *http.Request) (runtime.Object, error)

// getResourceHandler is an HTTP handler function for get requests. It delegates to the
// passed-in getterFunc to perform the actual get.
//
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

// GetResource returns a function that handles retrieving a single resource from a rest.Storage object.
func GetResource(r rest.Getter, scope *RequestScope) http.HandlerFunc {
	return getResourceHandler(scope,
		func(ctx context.Context, name string, req *http.Request) (runtime.Object, error) {
			// check for export
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

// GetResourceWithOptions returns a function that handles retrieving a single resource from a rest.Storage object.
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

// getRequestOptions parses out options and can include path information.  The path information shouldn't include the subresource.
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

		// ensure non-empty subpaths correctly reflect a leading slash
		if len(p) > 0 && !strings.HasPrefix(p, "/") {
			p = "/" + p
		}

		// ensure subpaths correctly reflect the presence of a trailing slash on the original request
		if strings.HasSuffix(requestInfo.Path, "/") && !strings.HasSuffix(p, "/") {
			p += "/"
		}

		newQuery[subpathKey] = []string{p}
		query = newQuery
	}
	return scope.ParameterCodec.DecodeParameters(query, scope.Kind.GroupVersion(), into)
}

// ListResource returns a handler serving both list and watch requests.
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

// listOpts decodes and validates list/watch query parameters.
func listOpts(req *http.Request, scope *RequestScope) (metainternalversion.ListOptions, error) {
	// Watches for single objects are routed to this function.
	// Treat a name parameter the same as a field selector entry.
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

	// transform fields
	// TODO: DecodeParametersInto should do this.
	// transform fields
	// 不同资源可自定义字段名（例如把外部字段映射到内部字段），
	// 必须在交给存储层之前完成转换。
	if opts.FieldSelector != nil {
		fn := func(label, value string) (newLabel, newValue string, err error) {
			return scope.Convertor.ConvertFieldLabel(scope.Kind, label, value)
		}
		if opts.FieldSelector, err = opts.FieldSelector.Transform(fn); err != nil {
			// TODO: allow bad request to set field causes based on query parameters
			return opts, errors.NewBadRequest(err.Error())
		}
	}

	if hasName {
		// metadata.name is the canonical internal name.
		// SelectionPredicate will notice that this is a request for
		// a single object and optimize the storage query accordingly.
		nameSelector := fields.OneTermEqualSelector("metadata.name", name)

		// Note that fieldSelector setting explicitly the "metadata.name"
		// will result in reaching this branch (as the value of that field
		// is propagated to requestInfo as the name parameter.
		// That said, the allowed field selectors in this branch are:
		// nil, fields.Everything and field selector matching metadata.name
		// for our name.
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

// handleWatch sets up and serves a watch (or watch-list) request.
//
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
	// TODO: Currently we explicitly ignore ?timeout= and use only ?timeoutSeconds=.
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
	// Invalidate cancel() to defer until serve() is complete.
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

	// Run watch serving in a separate goroutine to allow freeing current stack memory
	// 有 Task 时交给 routine 中间件在独立 goroutine 执行，否则就地执行。
	t := routine.TaskFrom(req.Context())
	if t != nil {
		t.Func = serve
	} else {
		serve()
	}
	return nil
}

// handleList serves a non-watch list request.
func handleList(ctx context.Context, r rest.Lister, scope *RequestScope, req *http.Request, w http.ResponseWriter, opts metainternalversion.ListOptions, outputMediaType negotiation.MediaTypeOptions) error {
	// For performance tracking purposes.
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

// isListWatchRequest is mirrored in staging/src/k8s.io/apiserver/pkg/storage/cacher/cacher.go
//
// 判定条件与 cacher 中必须保持一致，否则追踪 span 的开启/结束会与实际
// 的 watch-list 语义错位。修改此处需同步修改 cacher。
func isListWatchRequest(opts metainternalversion.ListOptions) bool {
	return opts.SendInitialEvents != nil && *opts.SendInitialEvents && opts.AllowWatchBookmarks
}
