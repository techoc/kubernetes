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
	"bytes"
	"context"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/watch"
	restclientwatch "k8s.io/client-go/rest/watch"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// longThrottleLatency 是记录限流日志的阈值：被限流超过该时长的请求会被记日志。
	longThrottleLatency = 50 * time.Millisecond

	// extraLongThrottleLatency 是在日志级别 2 记录请求的阈值。
	extraLongThrottleLatency = 1 * time.Second
)

// 抽象出 http.Client 的唯一方法，便于在测试中替换传输层。
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// ResponseWrapper 用于获取响应，既可以一次性读入内存，也可以流式读取。
type ResponseWrapper interface {
	DoRaw(context.Context) ([]byte, error)
	Stream(context.Context) (io.ReadCloser, error)
}

// RequestConstructionError 表示组装请求时出错。
type RequestConstructionError struct {
	Err error
}

// Error 返回 r 的文本描述。
func (r *RequestConstructionError) Error() string {
	return fmt.Sprintf("request construction error: '%v'", r.Err)
}

var noBackoff = &NoBackoff{}

// requestRetryFunc 允许测试替换重试策略；生产代码使用默认的指数退避实现。
type requestRetryFunc func(maxRetries int) WithRetry

func defaultRequestRetryFn(maxRetries int) WithRetry {
	return &withRetry{maxRetries: maxRetries}
}

// Request 以链式调用方式逐步构造对服务端的请求。
// 任何错误都会被暂存直到调用结束才返回，因此只需检查一次。
//
// 链式 setter 在遇到第一个错误后变为 no-op，后续调用不会覆盖原始错误，
// 这样调用方只需在最后检查一次 Error()。Request 本身不是并发安全的：
// 构造完成后应只读使用。
type Request struct {
	c *RESTClient

	contentConfig     ClientContentConfig
	contentTypeNotSet bool

	warningHandler WarningHandlerWithContext

	rateLimiter flowcontrol.RateLimiter
	backoff     BackoffManagerWithContext
	timeout     time.Duration
	maxRetries  int

	// 通过 setter 设置的通用组件。
	verb       string
	pathPrefix string
	subpath    string
	params     url.Values
	headers    http.Header

	// 请求的结构部分，遵循 Kubernetes API 约定。
	namespace    string
	namespaceSet bool
	resource     string
	resourceName string
	subresource  string

	// 输出。
	err error

	// body 与 bodyBytes 只能设置其一。body 是流式 reader，只能消费一次，
	// 因此带 body 的请求无法安全重试；bodyBytes 可以每次重建 reader，是重试路径的要求。
	body      io.Reader
	bodyBytes []byte

	retryFn requestRetryFunc
}

// NewRequest 创建请求辅助对象，用于访问服务端上的 runtime.Object。
func NewRequest(c *RESTClient) *Request {
	// backoff 由 client 注入；未配置时使用 no-op，避免调用方必须显式初始化。
	var backoff BackoffManagerWithContext
	if c.createBackoffMgr != nil {
		backoff = c.createBackoffMgr()
	}
	if backoff == nil {
		backoff = noBackoff
	}

	var pathPrefix string
	if c.base != nil {
		pathPrefix = path.Join("/", c.base.Path, c.versionedAPIPath)
	} else {
		pathPrefix = path.Join("/", c.versionedAPIPath)
	}

	var timeout time.Duration
	if c.Client != nil {
		timeout = c.Client.Timeout
	}

	// 请求需要知道内容类型是显式配置还是默认选择的，以支持 --prefers-protobuf 生成的客户端
	// 在单个请求上覆盖为 Protobuf。
	contentConfig, contentTypeDefaulted := c.content.GetClientContentConfig()

	r := &Request{
		c:              c,
		rateLimiter:    c.rateLimiter,
		backoff:        backoff,
		timeout:        timeout,
		pathPrefix:     pathPrefix,
		maxRetries:     10,
		retryFn:        defaultRequestRetryFn,
		warningHandler: c.warningHandler,

		contentConfig:     contentConfig,
		contentTypeNotSet: contentTypeDefaulted,
	}

	r.setAcceptHeader()
	return r
}

// NewRequestWithClient 创建内嵌 RESTClient 的 Request，供测试场景使用。
func NewRequestWithClient(base *url.URL, versionedAPIPath string, content ClientContentConfig, client *http.Client) *Request {
	return NewRequest(&RESTClient{
		base:             base,
		versionedAPIPath: versionedAPIPath,
		content:          requestClientContentConfigProvider{base: content},
		Client:           client,
	})
}

// UseProtobufAsDefaultIfPreferred 仅在 prefersProtobuf 为 true 时切换到 protobuf，
// 是生成代码中 --prefers-protobuf client 的调用入口。
func (r *Request) UseProtobufAsDefaultIfPreferred(prefersProtobuf bool) *Request {
	if prefersProtobuf {
		return r.UseProtobufAsDefault()
	}
	return r
}

// UseProtobufAsDefault 把默认的 Accept/Content-Type 切换为 protobuf。
// 只有在内容类型未被显式配置时才生效，否则会覆盖调用方的选择。
func (r *Request) UseProtobufAsDefault() *Request {
	if r.contentTypeNotSet && len(r.contentConfig.AcceptContentTypes) == 0 {
		r.contentConfig.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
		r.contentConfig.ContentType = "application/vnd.kubernetes.protobuf"
		r.setAcceptHeader()
	}
	return r
}

// setAcceptHeader 依据内容配置生成 Accept 头。
// 未配置 AcceptContentTypes 时退化为 ContentType + ", */*"，保持向后兼容。
func (r *Request) setAcceptHeader() {
	switch {
	case len(r.contentConfig.AcceptContentTypes) > 0:
		r.SetHeader("Accept", r.contentConfig.AcceptContentTypes)
	case len(r.contentConfig.ContentType) > 0:
		r.SetHeader("Accept", r.contentConfig.ContentType+", */*")
	}
}

// Verb 设置该请求使用的动词。
func (r *Request) Verb(verb string) *Request {
	r.verb = verb
	return r
}

// Prefix 在相对路径开头追加路径段，位于可选的 Namespace、Resource、Name 之前。
// 调用 AbsPath 会清除此前设置的 Prefix。
func (r *Request) Prefix(segments ...string) *Request {
	if r.err != nil {
		return r
	}
	r.pathPrefix = path.Join(r.pathPrefix, path.Join(segments...))
	return r
}

// Suffix 在路径末尾追加路径段，位于 prefix 及可选的 Namespace、Resource、Name 之后。
func (r *Request) Suffix(segments ...string) *Request {
	if r.err != nil {
		return r
	}
	r.subpath = path.Join(r.subpath, path.Join(segments...))
	return r
}

// Resource 设置要访问的资源，路径形态为 <resource>/[ns/<namespace>/]<name>。
//
// 重复设置或路径段非法都记录为 sticky error，而不是覆盖前值，
// 以便尽早暴露调用方的拼接错误。
func (r *Request) Resource(resource string) *Request {
	if r.err != nil {
		return r
	}
	if len(r.resource) != 0 {
		r.err = fmt.Errorf("resource already set to %q, cannot change to %q", r.resource, resource)
		return r
	}
	if msgs := IsValidPathSegmentName(resource); len(msgs) != 0 {
		r.err = fmt.Errorf("invalid resource %q: %v", resource, msgs)
		return r
	}
	r.resource = resource
	return r
}

// BackOff 把请求的 backoff 管理器设置为指定对象；传 nil 时使用默认的空实现。
//
// Deprecated: BackoffManager.Sleep ignores the caller's context. Use BackOffWithContext and BackoffManagerWithContext instead.
//
// 已弃用：BackoffManager.Sleep 忽略调用方 context。请改用 BackOffWithContext
// 与 BackoffManagerWithContext。此处为了兼容旧实现，用适配器丢弃 context。
func (r *Request) BackOff(manager BackoffManager) *Request {
	if manager == nil {
		r.backoff = &NoBackoff{}
		return r
	}

	r.backoff = &backoffManagerNopContext{BackoffManager: manager}
	return r
}

// BackOffWithContext 设置请求的退避管理器；传入 nil 时使用空实现。
func (r *Request) BackOffWithContext(manager BackoffManagerWithContext) *Request {
	if manager == nil {
		r.backoff = &NoBackoff{}
		return r
	}

	r.backoff = manager
	return r
}

// WarningHandler 设置客户端遇到 warning 头时使用的处理器；设为 nil 则使用默认处理器（见 [SetDefaultWarningHandler]）。
//
//logcheck:context // WarningHandlerWithContext should be used instead of WarningHandler in code which supports contextual logging.
func (r *Request) WarningHandler(handler WarningHandler) *Request {
	if handler == nil {
		r.warningHandler = nil
		return r
	}
	r.warningHandler = warningLoggerNopContext{l: handler}
	return r
}

// WarningHandlerWithContext 设置客户端遇到 warning 头时使用的处理器；设为 nil 则使用默认处理器（见 [SetDefaultWarningHandlerWithContext]）。
func (r *Request) WarningHandlerWithContext(handler WarningHandlerWithContext) *Request {
	r.warningHandler = handler
	return r
}

// Throttle 设置或替换请求的限速器。
func (r *Request) Throttle(limiter flowcontrol.RateLimiter) *Request {
	r.rateLimiter = limiter
	return r
}

// SubResource 设置子资源路径，可包含多段，位于资源名之后、suffix 之前。
func (r *Request) SubResource(subresources ...string) *Request {
	if r.err != nil {
		return r
	}
	// 多个子资源段合并为一段路径，例如 "proxy"/"http" 变为 "proxy/http"。
	subresource := path.Join(subresources...)
	if len(r.subresource) != 0 {
		r.err = fmt.Errorf("subresource already set to %q, cannot change to %q", r.subresource, subresource)
		return r
	}
	for _, s := range subresources {
		if msgs := IsValidPathSegmentName(s); len(msgs) != 0 {
			r.err = fmt.Errorf("invalid subresource %q: %v", s, msgs)
			return r
		}
	}
	r.subresource = subresource
	return r
}

// Name 设置要访问的资源名，路径形态为 <resource>/[ns/<namespace>/]<name>。
func (r *Request) Name(resourceName string) *Request {
	if r.err != nil {
		return r
	}
	if len(resourceName) == 0 {
		r.err = fmt.Errorf("resource name may not be empty")
		return r
	}
	if len(r.resourceName) != 0 {
		r.err = fmt.Errorf("resource name already set to %q, cannot change to %q", r.resourceName, resourceName)
		return r
	}
	if msgs := IsValidPathSegmentName(resourceName); len(msgs) != 0 {
		r.err = fmt.Errorf("invalid resource name %q: %v", resourceName, msgs)
		return r
	}
	r.resourceName = resourceName
	return r
}

// Namespace 为请求设置命名空间范围，路径形态为 <resource>/[ns/<namespace>/]<name>。
func (r *Request) Namespace(namespace string) *Request {
	if r.err != nil {
		return r
	}
	if r.namespaceSet {
		r.err = fmt.Errorf("namespace already set to %q, cannot change to %q", r.namespace, namespace)
		return r
	}
	if msgs := IsValidPathSegmentName(namespace); len(msgs) != 0 {
		r.err = fmt.Errorf("invalid namespace %q: %v", namespace, msgs)
		return r
	}
	r.namespaceSet = true
	r.namespace = namespace
	return r
}

// NamespaceIfScoped 在 scoped 为 true 时设置命名空间，是便捷封装。
func (r *Request) NamespaceIfScoped(namespace string, scoped bool) *Request {
	if scoped {
		return r.Namespace(namespace)
	}
	return r
}

// AbsPath 用给定路径段覆盖已有路径。传入单段时会保留尾部斜杠；RESTClient base URL 中的路径始终作为前缀保留。
func (r *Request) AbsPath(segments ...string) *Request {
	if r.err != nil {
		return r
	}
	// 直接替换 pathPrefix，因此会清除之前 Prefix/VersionedAPIPath 设置的路径。
	r.pathPrefix = path.Join(r.c.base.Path, path.Join(segments...))
	if len(segments) == 1 && (len(r.c.base.Path) > 1 || len(segments[0]) > 1) && strings.HasSuffix(segments[0], "/") {
		// 保留尾部斜杠以兼容旧行为。
		r.pathPrefix += "/"
	}
	return r
}

// RequestURI 用给定的服务端相对 URI 覆盖已有路径与参数，等价于清空参数后依次调用 AbsPath() 和 Param()。
func (r *Request) RequestURI(uri string) *Request {
	if r.err != nil {
		return r
	}
	locator, err := url.Parse(uri)
	if err != nil {
		r.err = err
		return r
	}
	// 若设置了 r.c.base.Path，AbsPath 会自动加上该前缀。
	r.AbsPath(locator.Path)
	if len(locator.Query()) > 0 {
		// 清空已有参数。
		r.params = make(url.Values)
		for k, v := range locator.Query() {
			r.params[k] = v
		}
	} else {
		// 清空已有参数。
		r.params = nil
	}
	return r
}

// Param 用给定字符串值创建查询参数。
func (r *Request) Param(paramName, s string) *Request {
	if r.err != nil {
		return r
	}
	return r.setParam(paramName, s)
}

// VersionedParams 用 RESTClient 的隐式 API 版本与默认参数编解码器把对象序列化为 map[string][]string，
// 再作为参数加入请求，供客户端库提供带版本的查询参数。带 omitempty 且为空的字段不会写出；
// 已存在的参数会被追加而非覆盖（Params 与 VersionedParams 是累加的）。
func (r *Request) VersionedParams(obj runtime.Object, codec runtime.ParameterCodec) *Request {
	return r.SpecificallyVersionedParams(obj, codec, r.contentConfig.GroupVersion)
}

// SpecificallyVersionedParams 与 VersionedParams 相同，但显式指定用于编码参数的版本，
// 供需要跨版本查询参数（如多个 group/version）的场景使用。
func (r *Request) SpecificallyVersionedParams(obj runtime.Object, codec runtime.ParameterCodec, version schema.GroupVersion) *Request {
	if r.err != nil {
		return r
	}
	params, err := codec.EncodeParameters(obj, version)
	if err != nil {
		r.err = err
		return r
	}
	for k, v := range params {
		if r.params == nil {
			r.params = make(url.Values)
		}
		r.params[k] = append(r.params[k], v...)
	}
	return r
}

// setParam 追加而非覆盖参数值，允许同名参数出现多次（如 fieldSelector 组合）。
func (r *Request) setParam(paramName, value string) *Request {
	if r.params == nil {
		r.params = make(url.Values)
	}
	r.params[paramName] = append(r.params[paramName], value)
	return r
}

// SetHeader 覆盖（而非追加）指定头部的全部值，语义与 http.Header.Set 不同，
// 便于一次设置多个值。
func (r *Request) SetHeader(key string, values ...string) *Request {
	if r.headers == nil {
		r.headers = http.Header{}
	}
	r.headers.Del(key)
	for _, value := range values {
		r.headers.Add(key, value)
	}
	return r
}

// Timeout 设置整个请求的超时时间，同时把该值作为 "timeout" 查询参数传给服务端。
func (r *Request) Timeout(d time.Duration) *Request {
	if r.err != nil {
		return r
	}
	r.timeout = d
	return r
}

// MaxRetries 设置收到 "Retry-After" 或 429 时的最大重试次数，默认 10。设为 0 则不重试，立即返回错误。
func (r *Request) MaxRetries(maxRetries int) *Request {
	if maxRetries < 0 {
		maxRetries = 0
	}
	r.maxRetries = maxRetries
	return r
}

// Body 设置请求体，可选。string 按文件名读取内容，[]byte 直接发送，io.Reader 直接使用，
// runtime.Object 会被正确序列化并设置 Content-Type；nil 的 runtime.Object 不做处理，其他类型记录错误。
func (r *Request) Body(obj interface{}) *Request {
	if r.err != nil {
		return r
	}
	switch t := obj.(type) {
	case string:
		// 字符串按“文件名”解释，读取其内容作为请求体。
		data, err := os.ReadFile(t)
		if err != nil {
			r.err = err
			return r
		}
		r.body = nil
		r.bodyBytes = data
	case []byte:
		r.body = nil
		r.bodyBytes = t
	case io.Reader:
		r.body = t
		r.bodyBytes = nil
	case runtime.Object:
		// 调用方可能传入类型化接口指针，因此必须用反射判空。
		if reflect.ValueOf(t).IsNil() {
			return r
		}
		encoder, err := r.contentConfig.Negotiator.Encoder(r.contentConfig.ContentType, nil)
		if err != nil {
			r.err = err
			return r
		}
		data, err := runtime.Encode(encoder, t)
		if err != nil {
			r.err = err
			return r
		}
		r.body = nil
		r.bodyBytes = data
		r.SetHeader("Content-Type", r.contentConfig.ContentType)
	default:
		r.err = fmt.Errorf("unknown type used for body: %+v", obj)
	}
	return r
}

// Error 返回构造请求过程中遇到的错误。
func (r *Request) Error() error {
	return r.err
}

// URL 返回当前工作 URL。应先检查 Error() 以确保 URL 有效。
func (r *Request) URL() *url.URL {
	p := r.pathPrefix
	// 路径顺序固定为 <prefix>/namespaces/<ns>/<resource>/<name>/<subresource>/<suffix>，
	// 与 Kubernetes API 约定一致。
	if r.namespaceSet && len(r.namespace) > 0 {
		p = path.Join(p, "namespaces", r.namespace)
	}
	if len(r.resource) != 0 {
		p = path.Join(p, strings.ToLower(r.resource))
	}
	// Join 会裁掉尾部斜杠，若路径未改动则保留 r.pathPrefix 的尾部斜杠以兼容旧行为。
	if len(r.resourceName) != 0 || len(r.subpath) != 0 || len(r.subresource) != 0 {
		p = path.Join(p, r.resourceName, r.subresource, r.subpath)
	}

	finalURL := &url.URL{}
	if r.c.base != nil {
		*finalURL = *r.c.base
	}
	finalURL.Path = p

	query := url.Values{}
	for key, values := range r.params {
		for _, value := range values {
			query.Add(key, value)
		}
	}

	// timeout 在这里被特殊处理：
	// 显式设置 timeout 参数，让服务端也能感知客户端的超时预算。
	if r.timeout != 0 {
		query.Set("timeout", r.timeout.String())
	}
	finalURL.RawQuery = query.Encode()
	return finalURL
}

// finalURLTemplate 与 URL() 类似，但把所有具体参数值抹平：name 与 namespace 分别替换为
// "{name}" 与 "{namespace}"，所有查询参数被重置。返回 URL 副本，不修改底层对象。
//
// 该模板刻意抹掉具体对象名与 namespace，避免指标标签基数爆炸；
// 因此这里的 URL 只能用于指标，不能用于实际请求。
func (r Request) finalURLTemplate() url.URL {
	// 值接收者会复制 Request，修改 r.params 不会影响原对象。
	newParams := url.Values{}
	v := []string{"{value}"}
	for k := range r.params {
		newParams[k] = v
	}
	r.params = newParams
	u := r.URL()
	if u == nil {
		return url.URL{}
	}

	segments := strings.Split(u.Path, "/")
	groupIndex := 0
	index := 0
	trimmedBasePath := ""
	if r.c.base != nil && strings.Contains(u.Path, r.c.base.Path) {
		p := strings.TrimPrefix(u.Path, r.c.base.Path)
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		// 保存裁掉的基础路径，返回 URL 前再拼回去。
		trimmedBasePath = r.c.base.Path
		segments = strings.Split(p, "/")
		groupIndex = 1
	}
	if len(segments) <= 2 {
		return *u
	}

	const CoreGroupPrefix = "api"
	const NamedGroupPrefix = "apis"
	isCoreGroup := segments[groupIndex] == CoreGroupPrefix
	isNamedGroup := segments[groupIndex] == NamedGroupPrefix
	if isCoreGroup {
		// 处理核心组的 /api/v1/... 形式。
		index = groupIndex + 2
	} else if isNamedGroup {
		// 处理带组名的 /apis/apps/v1/... 形式。
		index = groupIndex + 3
	} else {
		// 目前只应有 /api... 与 /apis... 两种形式，这里留个兜底分支，以防将来新增其他形式。
		// https://kubernetes.io/docs/concepts/overview/kubernetes-api/#api-groups
		// 遇到非法的 API group 名时，url.Path 返回 {prefix}。
		u.Path = "/{prefix}"
		u.RawQuery = ""
		return *u
	}
	// switch segLength := len(segments) - index; segLength {
	// 归一化规则：只保留 group/version 之后的资源路径结构，
	// 把其中类似名字的段替换为 {name}/{namespace} 占位符。
	switch {
	// case len(segments) - index == 1:
	// 只有资源名，无需处理。
	case len(segments)-index == 2:
		// /$RESOURCE/$NAME：把 $NAME 替换为 {name}。
		segments[index+1] = "{name}"
	case len(segments)-index == 3:
		if segments[index+2] == "finalize" || segments[index+2] == "status" {
			// /$RESOURCE/$NAME/$SUBRESOURCE：把 $NAME 替换为 {name}。
			segments[index+1] = "{name}"
		} else {
			// /namespace/$NAMESPACE/$RESOURCE：把 $NAMESPACE 替换为 {namespace}。
			segments[index+1] = "{namespace}"
		}
	case len(segments)-index >= 4:
		segments[index+1] = "{namespace}"
		// /namespace/$NAMESPACE/$RESOURCE/$NAME：$NAMESPACE 换成 {namespace}，$NAME 换成 {name}。
		if segments[index+3] != "finalize" && segments[index+3] != "status" {
			// /$RESOURCE/$NAME/$SUBRESOURCE：把 $NAME 替换为 {name}。
			segments[index+3] = "{name}"
		}
	}
	u.Path = path.Join(trimmedBasePath, path.Join(segments...))
	return *u
}

// tryThrottleWithInfo 在发送请求前等待客户端限速器，并记录等待延迟。
// retryInfo 用于在日志中区分首次请求与重试。
func (r *Request) tryThrottleWithInfo(ctx context.Context, retryInfo string) error {
	// 未配置限速器时直接放行，避免无谓的时间采样。
	if r.rateLimiter == nil {
		return nil
	}

	now := time.Now()

	err := r.rateLimiter.Wait(ctx)
	// 调用方主动取消 context 不属于限速错误，不要包装 context 错误。
	if err != nil && !stderrors.Is(err, context.Canceled) && !stderrors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("client rate limiter Wait returned an error: %w", err)
	}
	latency := time.Since(now)

	if latency > longThrottleLatency {
		if retryInfo == "" {
			retryInfo = "client-side throttling, not priority and fairness"
		}
		klog.FromContext(ctx).V(3).Info("Waited before sending request", "delay", latency, "reason", retryInfo, "verb", r.verb, "URL", r.URL())

		if latency > extraLongThrottleLatency {
			// 限速等待极长时，日志应在更详细的级别打印，但为避免刷屏使用限速日志器：
			// 高并发下每次等待都可能超过阈值，不限流会淹没日志。
			globalThrottledLogger.info(klog.FromContext(ctx), "Waited before sending request", "delay", latency, "reason", retryInfo, "verb", r.verb, "URL", r.URL())
		}
	}
	metrics.RateLimiterLatency.Observe(ctx, r.verb, r.finalURLTemplate(), latency)

	return err
}

func (r *Request) tryThrottle(ctx context.Context) error {
	return r.tryThrottleWithInfo(ctx, "")
}

// throttleSettings 描述某一日志级别下的最小输出间隔。
type throttleSettings struct {
	logLevel       int
	minLogInterval time.Duration

	lastLogTime time.Time
	lock        sync.RWMutex
}

// throttledLogger 按级别限制日志输出频率；级别越高（越详细）间隔越短。
type throttledLogger struct {
	clock    clock.PassiveClock
	settings []*throttleSettings
}

// 全局实例：V(2) 最多每秒一条，V(0) 最多每 10 秒一条。
var globalThrottledLogger = &throttledLogger{
	clock: clock.RealClock{},
	settings: []*throttleSettings{
		{
			logLevel:       2,
			minLogInterval: 1 * time.Second,
		}, {
			logLevel:       0,
			minLogInterval: 10 * time.Second,
		},
	},
}

// attemptToLog 返回允许使用的日志级别；需要写日志时同时推进 lastLogTime。
// 先读锁检查、后写锁复核，既减少写锁竞争又保证同一间隔内只输出一次。
func (b *throttledLogger) attemptToLog(logger klog.Logger) (int, bool) {
	for _, setting := range b.settings {
		if bool(logger.V(setting.logLevel).Enabled()) {
			// 尽量不走写锁提前返回。
			if func() bool {
				setting.lock.RLock()
				defer setting.lock.RUnlock()
				return b.clock.Since(setting.lastLogTime) >= setting.minLogInterval
			}() {
				setting.lock.Lock()
				defer setting.lock.Unlock()
				if b.clock.Since(setting.lastLogTime) >= setting.minLogInterval {
					setting.lastLogTime = b.clock.Now()
					return setting.logLevel, true
				}
			}
			return -1, false
		}
	}
	return -1, false
}

// Infof 按接收者的 throttleSettings 指定的日志级别输出日志，前提是距上次输出已超过 minLogInterval。
func (b *throttledLogger) info(logger klog.Logger, message string, kv ...any) {
	if logLevel, ok := b.attemptToLog(logger); ok {
		logger.V(logLevel).Info(message, kv...)
	}
}

// Watch 尝试监听目标位置，返回 watch.Interface 或错误。
func (r *Request) Watch(ctx context.Context) (watch.Interface, error) {
	if r.body == nil {
		logBody(klog.FromContext(ctx), 2, "Request Body", r.bodyBytes)
	}

	// 刻意不对 watch 限速，因此这里不使用 r.rateLimiter。
	if r.err != nil {
		return nil, r.err
	}

	client := r.c.Client
	if client == nil {
		client = http.DefaultClient
	}

	isErrRetryableFunc := func(request *http.Request, err error) bool {
		// watch 流机制能处理许多常见的部分数据错误，因此连接被关闭时多数情况下可以安全重试。
		// 只对连接级错误重试；HTTP 状态错误由下方 transformResponse 统一处理。
		if net.IsProbableEOF(err) || net.IsTimeout(err) {
			return true
		}
		return false
	}
	retry := r.retryFn(r.maxRetries)
	url := r.URL().String()
	for {
		if err := retry.Before(ctx, r); err != nil {
			return nil, retry.WrapPreviousError(err)
		}

		req, err := r.newHTTPRequest(ctx)
		if err != nil {
			return nil, err
		}

		resp, err := client.Do(req)
		retry.After(ctx, r, resp, err)
		if err == nil && resp.StatusCode == http.StatusOK {
			return r.newStreamWatcher(ctx, resp)
		}

		done, transformErr := func() (bool, error) {
			// 无论走哪条路径都关闭响应体，防止连接泄漏。
			defer readAndCloseResponseBody(resp)

			if retry.IsNextRetry(ctx, r, req, resp, err, isErrRetryableFunc) {
				return false, nil
			}

			if resp == nil {
				// 服务端必然已通过 err 返回了错误。
				return true, nil
			}
			result := r.transformResponse(ctx, resp, req)
			if err := result.Error(); err != nil {
				return true, err
			}
			return true, fmt.Errorf("for request %s, got status: %v", url, resp.StatusCode)
		}()
		if done {
			if isErrRetryableFunc(req, err) {
				return watch.NewEmptyWatch(), nil
			}
			if err == nil {
				// 若服务端返回了 HTTP Response 对象，需要从中提取错误对象返回。
				err = transformErr
			}
			return nil, retry.WrapPreviousError(err)
		}
	}
}

// newStreamWatcher 根据响应的 Content-Type 协商流式解码器，并构造 watch.Interface。
func (r *Request) newStreamWatcher(ctx context.Context, resp *http.Response) (watch.Interface, error) {
	contentType := resp.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		klog.FromContext(ctx).V(4).Info("Unexpected content type from the server", "contentType", contentType, "err", err)
	}
	objectDecoder, streamingSerializer, framer, err := r.contentConfig.Negotiator.StreamDecoder(mediaType, params)
	if err != nil {
		return nil, err
	}

	handleWarnings(ctx, resp.Header, r.warningHandler)

	frameReader := framer.NewFrameReader(resp.Body)
	watchEventDecoder := streaming.NewDecoder(frameReader, streamingSerializer)

	return watch.NewStreamWatcherWithLogger(
		klog.FromContext(ctx),
		restclientwatch.NewDecoder(watchEventDecoder, objectDecoder),
		// 原因不明时用 500；其他错误码更贴近具体的 HTTP 交互场景，并附带 reason。
		errors.NewClientErrorReporter(http.StatusInternalServerError, r.verb, "ClientWatchDecoding"),
	), nil
}

// updateRequestResultMetric 累加 RequestResult 计数，应传入服务端最终响应的 (response, err)。
func updateRequestResultMetric(ctx context.Context, req *Request, resp *http.Response, err error) {
	code, host := sanitize(req, resp, err)
	metrics.RequestResult.Increment(ctx, code, req.verb, host)
}

// updateRequestRetryMetric 累加 RequestRetry 计数，每次重试（不含最后一次）都应传入对应的 (response, err)。
func updateRequestRetryMetric(ctx context.Context, req *Request, resp *http.Response, err error) {
	code, host := sanitize(req, resp, err)
	metrics.RequestRetry.IncrementRetry(ctx, code, req.verb, host)
}

// sanitize 把 (响应, 错误) 归一为指标标签。
// 错误文本可能任意变化，直接作为标签会导致基数失控，因此统一记为 "<error>"。
func sanitize(req *Request, resp *http.Response, err error) (string, string) {
	host := "none"
	if req.c.base != nil {
		host = req.c.base.Host
	}

	// 错误文本可能是任意字符串，直接作为标签会导致基数失控，统一记为 `<error>`。
	code := "<error>"
	if resp != nil {
		code = strconv.Itoa(resp.StatusCode)
	}

	return code, host
}

// Stream 构造并发送请求，以流的形式返回响应。非 2xx 状态码一律视为错误：先尝试把响应体解析为 APIStatus 并返回，
// 否则返回包含 HTTP 状态码与响应内容的错误。
func (r *Request) Stream(ctx context.Context) (io.ReadCloser, error) {
	if r.body == nil {
		logBody(klog.FromContext(ctx), 2, "Request Body", r.bodyBytes)
	}

	if r.err != nil {
		return nil, r.err
	}

	if err := r.tryThrottle(ctx); err != nil {
		return nil, err
	}

	client := r.c.Client
	if client == nil {
		client = http.DefaultClient
	}

	retry := r.retryFn(r.maxRetries)
	url := r.URL().String()
	for {
		if err := retry.Before(ctx, r); err != nil {
			return nil, err
		}

		req, err := r.newHTTPRequest(ctx)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		retry.After(ctx, r, resp, err)
		if err != nil {
			// 只在响应带 'Retry-After' 头时重试。
			return nil, err
		}

		switch {
		case (resp.StatusCode >= 200) && (resp.StatusCode < 300):
			handleWarnings(ctx, resp.Header, r.warningHandler)
			return resp.Body, nil

		default:
			done, transformErr := func() (bool, error) {
				// 非 2xx 时响应体不会返回给调用方，必须在这里关闭。
				defer resp.Body.Close()

				if retry.IsNextRetry(ctx, r, req, resp, err, neverRetryError) {
					return false, nil
				}
				result := r.transformResponse(ctx, resp, req)
				if err := result.Error(); err != nil {
					return true, err
				}
				return true, fmt.Errorf("%d while accessing %v: %s", result.statusCode, url, string(result.body))
			}()
			if done {
				return nil, transformErr
			}
		}
	}
}

// requestPreflightCheck 检查 Request 中常见的调用方编码错误。
//
// 这里处理两类错误：第一，在 namespaceSet 为 true 时试图以空字符串作为
// namespace 创建资源（POST）——namespaceSet 为 true 时 namespace 也必须已定义；
// 第二，在相同情况下试图 GET、PUT 或 DELETE 一个具名资源（resourceName != ""），
// 同样，namespaceSet 为 true 时 namespace 不得为空。
//
// 这两类错误无法从服务端得到有意义的响应，提前在客户端拦截并给出明确原因。
func (r *Request) requestPreflightCheck() error {
	if !r.namespaceSet {
		return nil
	}
	if len(r.namespace) > 0 {
		return nil
	}

	switch r.verb {
	case "POST":
		return fmt.Errorf("an empty namespace may not be set during creation")
	case "GET", "PUT", "DELETE":
		if len(r.resourceName) > 0 {
			return fmt.Errorf("an empty namespace may not be set when a resource name is provided")
		}
	}
	return nil
}

// newHTTPRequest 为每次（重试）尝试构造独立的 *http.Request。
func (r *Request) newHTTPRequest(ctx context.Context) (*http.Request, error) {
	var body io.Reader
	switch {
	case r.body != nil && r.bodyBytes != nil:
		return nil, fmt.Errorf("cannot set both body and bodyBytes")
	case r.body != nil:
		body = r.body
	case r.bodyBytes != nil:
		// 为每次（重试）请求创建独立的 reader，避免重置请求体时发生竞争。
		body = bytes.NewReader(r.bodyBytes)
	}

	url := r.URL().String()
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, newDNSMetricsTrace(ctx)), r.verb, url, body)
	if err != nil {
		return nil, err
	}
	req.Header = r.headers
	return req, nil
}

// newDNSMetricsTrace 返回按主机统计 DNS 查询耗时的 HTTP trace，对应指标 "rest_client_dns_resolution_duration_seconds"。
func newDNSMetricsTrace(ctx context.Context) *httptrace.ClientTrace {
	// DNS 回调可能并发触发，用互斥量保护共享的 start/host。
	type dnsMetric struct {
		start time.Time
		host  string
		sync.Mutex
	}
	dns := &dnsMetric{}
	return &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dns.Lock()
			defer dns.Unlock()
			dns.start = time.Now()
			dns.host = info.Host
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			dns.Lock()
			defer dns.Unlock()
			metrics.ResolverLatency.Observe(ctx, dns.host, time.Since(dns.start))
		},
	}
}

// request 连接服务端并在收到响应时调用 fn，负责重试与前置校验。fn 最多调用一次；
// 连接前发生的错误由本函数返回，服务端响应层面的错误由 fn 处理。
func (r *Request) request(ctx context.Context, fn func(*http.Request, *http.Response)) error { // Metrics for total request latency
	start := time.Now()
	defer func() {
		metrics.RequestLatency.Observe(ctx, r.verb, r.finalURLTemplate(), time.Since(start))
	}()

	if r.err != nil {
		klog.FromContext(ctx).V(4).Info("Error in request", "err", r.err)
		return r.err
	}

	if err := r.requestPreflightCheck(); err != nil {
		return err
	}

	client := r.c.Client
	if client == nil {
		client = http.DefaultClient
	}

	// 首次尝试先等待限速，再设置客户端超时，避免请求尚未发出就向调用方报超时。
	if err := r.tryThrottle(ctx); err != nil {
		return err
	}

	if r.timeout > 0 {
		// 超时只覆盖实际请求阶段，限速等待不计入，避免被限速时误报超时。
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	isErrRetryableFunc := func(req *http.Request, err error) bool {
		// "Connection reset by peer" 与 "apiserver is shutting down" 通常是瞬时错误，因此 GET 可直接重试。
		// 写操作不自动重试，因为它们不具备幂等性。
		if req.Method != "GET" {
			return false
		}
		// 连接错误与 apiserver 关闭错误应重试。
		if net.IsConnectionReset(err) || net.IsProbableEOF(err) || net.IsHTTP2ConnectionLost(err) {
			return true
		}
		return false
	}

	// 目前收到 Retry-After 响应时大约重试十次。
	retry := r.retryFn(r.maxRetries)
	for {
		if err := retry.Before(ctx, r); err != nil {
			return retry.WrapPreviousError(err)
		}
		req, err := r.newHTTPRequest(ctx)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		// -1，或 Body 非空时为 0，都表示长度未知。
		// https://pkg.go.dev/net/http#Request
		if req.ContentLength >= 0 && !(req.Body != nil && req.ContentLength == 0) {
			metrics.RequestSize.Observe(ctx, r.verb, r.URL().Host, float64(req.ContentLength))
		}
		if resp != nil && resp.StatusCode == http.StatusUnsupportedMediaType {
			// 让内容协商层记住服务端不支持的媒体类型，后续请求降级到其他格式。
			r.c.content.UnsupportedMediaType(resp.Request.Header.Get("Content-Type"))
		}
		retry.After(ctx, r, resp, err)

		done := func() bool {
			// 每次尝试都关闭响应体；若返回 false 表示还要重试，否则已完成。
			defer readAndCloseResponseBody(resp)

			// 若服务端通过 err 返回错误，response 会是 nil。
			f := func(req *http.Request, resp *http.Response) {
				if resp == nil {
					return
				}
				fn(req, resp)
			}

			if retry.IsNextRetry(ctx, r, req, resp, err, isErrRetryableFunc) {
				return false
			}

			f(req, resp)
			return true
		}()
		if done {
			return retry.WrapPreviousError(err)
		}
	}
}

// Do 构造并执行请求，返回便于处理响应的 Result。
//
// 错误类型：
//   - 服务端返回 status 时：*errors.StatusError 或 *errors.UnexpectedObjectError
//   - http.Client.Do 的错误直接返回。
func (r *Request) Do(ctx context.Context) Result {
	logger := klog.FromContext(ctx)
	if r.body == nil {
		logBody(logger, 2, "Request Body", r.bodyBytes)
	}

	var result Result
	err := r.request(ctx, func(req *http.Request, resp *http.Response) {
		result = r.transformResponse(ctx, resp, req)
	})
	if err != nil {
		return Result{err: err, logger: logger}
	}
	if result.err == nil || len(result.body) > 0 {
		metrics.ResponseSize.Observe(ctx, r.verb, r.URL().Host, float64(len(result.body)))
	}
	return result
}

// DoRaw 执行请求但不解析响应体。
//
// 非 2xx 且响应体无法解析为 Status 时，仍返回原始 body 并附带通用错误，
// 便于调用方自行展示代理或网关返回的内容。
func (r *Request) DoRaw(ctx context.Context) ([]byte, error) {
	logger := klog.FromContext(ctx)
	if r.body == nil {
		logBody(logger, 2, "Request Body", r.bodyBytes)
	}

	var result Result
	err := r.request(ctx, func(req *http.Request, resp *http.Response) {
		result.body, result.err = io.ReadAll(resp.Body)
		logBody(logger, 2, "Response Body", result.body)
		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent {
			result.err = r.transformUnstructuredResponseError(resp, req, result.body)
		}
	})
	if err != nil {
		return nil, err
	}
	if result.err == nil || len(result.body) > 0 {
		metrics.ResponseSize.Observe(ctx, r.verb, r.URL().Host, float64(len(result.body)))
	}
	return result.body, result.err
}

// transformResponse 把 API 响应转换为结构化的 API 对象。
//
// 该函数只在响应体已被完整读取后调用；它不返回错误，而是把状态码、解码器
// 与可能的错误一起放进 Result，由调用方决定如何处理。
func (r *Request) transformResponse(ctx context.Context, resp *http.Response, req *http.Request) Result {
	logger := klog.FromContext(ctx)
	var body []byte
	if resp.Body != nil {
		data, err := io.ReadAll(resp.Body)
		switch err.(type) {
		case nil:
			body = data
		case http2.StreamError:
			// 用于捕获服务端在发送响应体过程中关闭连接的情况，可能由慢网络导致的服务端超时引起。
			// TODO: 补测试：1) client-go（或 kubectl）发 GET；2) apiserver 返回头及部分 body；
			// 3) apiserver 关闭连接；4) client-go 应捕获并返回错误。
			logger.V(2).Info("Stream error when reading response body, may be caused by closed connection", "err", err)
			streamErr := fmt.Errorf("stream error when reading response body, may be caused by closed connection. Please retry. Original error: %w", err)
			return Result{
				err:    streamErr,
				logger: logger,
			}
		default:
			logger.Error(err, "Unexpected error when reading response body")
			unexpectedErr := fmt.Errorf("unexpected error when reading response body. Please retry. Original error: %w", err)
			return Result{
				err:    unexpectedErr,
				logger: logger,
			}
		}
	}

	// 调用层数容易出错，该值适用于 Do 与 DoRaw。
	logBody(logger, 7, "Response Body", body)

	// 校验 Content-Type 是否准确。
	var decoder runtime.Decoder
	contentType := resp.Header.Get("Content-Type")
	if len(contentType) == 0 {
		contentType = r.contentConfig.ContentType
	}
	if len(contentType) > 0 {
		var err error
		mediaType, params, err := mime.ParseMediaType(contentType)
		if err != nil {
			return Result{err: errors.NewInternalError(err), logger: logger}
		}
		decoder, err = r.contentConfig.Negotiator.Decoder(mediaType, params)
		if err != nil {
			// 协商解码器失败时，按非结构化错误处理：
			// 优先把非 2xx 转成通用错误；协议升级（101）则视为正常，交由调用方处理。
			switch {
			case resp.StatusCode == http.StatusSwitchingProtocols:
				// 协议已升级，无需处理。
			case resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent:
				return Result{err: r.transformUnstructuredResponseError(resp, req, body), logger: logger}
			}
			return Result{
				body:        body,
				contentType: contentType,
				statusCode:  resp.StatusCode,
				warnings:    handleWarnings(ctx, resp.Header, r.warningHandler),
				logger:      logger,
			}
		}
	}

	switch {
	case resp.StatusCode == http.StatusSwitchingProtocols:
		// 协议已升级，无需处理。
	case resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusPartialContent:
		// 从响应中构造非结构化错误，供调用方未返回结构化错误时的 Result 使用。
		retryAfter, _ := retryAfterSeconds(resp)
		err := r.newUnstructuredResponseError(body, isTextResponse(resp), resp.StatusCode, req.Method, retryAfter)
		return Result{
			body:        body,
			contentType: contentType,
			statusCode:  resp.StatusCode,
			decoder:     decoder,
			err:         err,
			warnings:    handleWarnings(ctx, resp.Header, r.warningHandler),
			logger:      logger,
		}
	}

	return Result{
		body:        body,
		contentType: contentType,
		statusCode:  resp.StatusCode,
		decoder:     decoder,
		warnings:    handleWarnings(ctx, resp.Header, r.warningHandler),
		logger:      logger,
	}
}

// truncateBody 根据 glog 的日志级别（Verbosity）决定是否截断 body。
// 日志级别越高保留越多内容：V(10) 全量、V(9) 10KB、V(8) 1KB。
func truncateBody(logger klog.Logger, body string) string {
	max := 0
	switch {
	case bool(logger.V(10).Enabled()):
		return body
	case bool(logger.V(9).Enabled()):
		max = 10240
	case bool(logger.V(8).Enabled()):
		max = 1024
	}

	if len(body) <= max {
		return body
	}

	return body[:max] + fmt.Sprintf(" [truncated %d chars]", len(body)-max)
}

// logBody 记录可能是 JSON 或 protobuf 的响应体，尽量避免不必要的字符串分配，并用简单启发式判断
// body 是否可打印。所有发送或接收数据的函数都应调用它。
func logBody(logger klog.Logger, callDepth int, prefix string, body []byte) {
	// 只有 V(8) 才需要做任何工作；其余情况直接返回，避免字符串分配。
	if loggerV := logger.V(8); loggerV.Enabled() {
		loggerV := loggerV.WithCallDepth(callDepth)
		// 含控制字符（如 protobuf 二进制）时用 hex dump，避免日志输出乱码。
		if bytes.IndexFunc(body, func(r rune) bool {
			return r < 0x0a
		}) != -1 {
			loggerV.Info(prefix, "body", truncateBody(logger, hex.Dump(body)))
		} else {
			loggerV.Info(prefix, "body", truncateBody(logger, string(body)))
		}
	}
}

// maxUnstructuredResponseTextBytes is an upper bound on how much output to include in the unstructured error.
const maxUnstructuredResponseTextBytes = 2048

// transformUnstructuredResponseError handles an error from the server that is not in a structured form.
// It is expected to transform any response that is not recognizable as a clear server sent error from the
// K8S API using the information provided with the request. In practice, HTTP proxies and client libraries
// introduce a level of uncertainty to the responses returned by servers that in common use result in
// unexpected responses. The rough structure is:
//
// 1. Assume the server sends you something sane - JSON + well defined error objects + proper codes
//   - this is the happy path
//   - when you get this output, trust what the server sends
//     2. Guard against empty fields / bodies in received JSON and attempt to cull sufficient info from them to
//     generate a reasonable facsimile of the original failure.
//   - Be sure to use a distinct error type or flag that allows a client to distinguish between this and error 1 above
//     3. Handle true disconnect failures / completely malformed data by moving up to a more generic client error
//     4. Distinguish between various connection failures like SSL certificates, timeouts, proxy errors, unexpected
//     initial contact, the presence of mismatched body contents from posted content types
//   - Give these a separate distinct error type and capture as much as possible of the original message
//
// TODO: introduce transformation of generic http.Client.Do() errors that separates 4.
func (r *Request) transformUnstructuredResponseError(resp *http.Response, req *http.Request, body []byte) error {
	// 非结构化调用方可能还没读 body，这里限量读取，避免把超大响应当作错误信息。
	if body == nil && resp.Body != nil {
		if data, err := io.ReadAll(&io.LimitedReader{R: resp.Body, N: maxUnstructuredResponseTextBytes}); err == nil {
			body = data
		}
	}
	retryAfter, _ := retryAfterSeconds(resp)
	return r.newUnstructuredResponseError(body, isTextResponse(resp), resp.StatusCode, req.Method, retryAfter)
}

// newUnstructuredResponseError instantiates the appropriate generic error for the provided input. It also logs the body.
func (r *Request) newUnstructuredResponseError(body []byte, isTextResponse bool, statusCode int, method string, retryAfter int) error {
	// 限制输出长度。
	if len(body) > maxUnstructuredResponseTextBytes {
		body = body[:maxUnstructuredResponseTextBytes]
	}

	message := "unknown"
	if isTextResponse {
		// 只对文本响应回填 body；二进制内容对错误信息没有价值。
		message = strings.TrimSpace(string(body))
	}
	var groupResource schema.GroupResource
	if len(r.resource) > 0 {
		groupResource.Group = r.contentConfig.GroupVersion.Group
		groupResource.Resource = r.resource
	}
	return errors.NewGenericServerResponse(
		statusCode,
		method,
		groupResource,
		r.resourceName,
		message,
		retryAfter,
		true,
	)
}

// isTextResponse 判断响应是否为文本媒体类型。
func isTextResponse(resp *http.Response) bool {
	contentType := resp.Header.Get("Content-Type")
	if len(contentType) == 0 {
		// 缺少 Content-Type 时按文本处理，尽力而为。
		return true
	}
	media, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false
	}
	return strings.HasPrefix(media, "text/")
}

// retryAfterSeconds 返回 Retry-After 头的值及 true；头缺失或不是合法数字时返回 0 和 false。
func retryAfterSeconds(resp *http.Response) (int, bool) {
	if h := resp.Header.Get("Retry-After"); len(h) > 0 {
		if i, err := strconv.Atoi(h); err == nil {
			return i, true
		}
	}
	return 0, false
}

// Result 包含调用 Request.Do() 的结果。
//
// Result 是不可变的响应快照：body 已被完整读入内存，因此可以安全地多次
// 调用 Get/Into/Raw，而不依赖底层连接。
type Result struct {
	body        []byte
	warnings    []net.WarningHeader
	contentType string
	err         error
	statusCode  int
	logger      klog.Logger

	decoder runtime.Decoder
}

// Raw 返回原始结果。
func (r Result) Raw() ([]byte, error) {
	return r.body, r.err
}

// Get 返回经解码后的对象。若结果是 Status 且 .Status != StatusSuccess，会用其中的信息丰富错误。
func (r Result) Get() (runtime.Object, error) {
	if r.err != nil {
		// 优先使用 body 中的 Status 对象。
		return nil, r.Error()
	}
	if r.decoder == nil {
		return nil, fmt.Errorf("serializer for %s doesn't exist", r.contentType)
	}

	// 解码；若结果是 Status，则把它作为 error 返回：
	// 即使 HTTP 状态是 2xx，body 中的 StatusFailure 也视为错误。
	out, _, err := r.decoder.Decode(r.body, nil, nil)
	if err != nil {
		return nil, err
	}
	switch t := out.(type) {
	case *metav1.Status:
		// 除 StatusSuccess 外的状态一律视为错误。
		if t.Status != metav1.StatusSuccess {
			return nil, errors.FromObject(t)
		}
	}
	return out, nil
}

// StatusCode 返回请求的 HTTP 状态码，仅在未返回错误时有效。
func (r Result) StatusCode(statusCode *int) Result {
	*statusCode = r.statusCode
	return r
}

// ContentType 把 "Content-Type" 响应头写入传入的字符串并返回 Result 以支持链式调用，仅在未返回错误时有效。
func (r Result) ContentType(contentType *string) Result {
	*contentType = r.contentType
	return r
}

// Into 尽可能把结果存入 obj；obj 为 nil 时忽略。若结果是 Status 且 .Status != StatusSuccess，
// 会用其中的信息丰富错误。
func (r Result) Into(obj runtime.Object) error {
	if r.err != nil {
		// 优先使用 body 中的 Status 对象。
		return r.Error()
	}
	if r.decoder == nil {
		return fmt.Errorf("serializer for %s doesn't exist", r.contentType)
	}
	if len(r.body) == 0 {
		return fmt.Errorf("0-length response with status code: %d and content type: %s",
			r.statusCode, r.contentType)
	}

	out, _, err := r.decoder.Decode(r.body, nil, obj)
	if err != nil || out == obj {
		// 解码器已把结果就地写入目标对象时，无需再检查 Status。
		return err
	}
	// 若返回的是另一个对象，检查它是否为 Status，避免重复解码。
	switch t := out.(type) {
	case *metav1.Status:
		// 除 StatusSuccess 外的状态一律视为错误。
		if t.Status != metav1.StatusSuccess {
			return errors.FromObject(t)
		}
	}
	return nil
}

// WasCreated 根据服务端是否返回 201 更新传入的 bool 指针。
func (r Result) WasCreated(wasCreated *bool) Result {
	*wasCreated = r.statusCode == http.StatusCreated
	return r
}

// Error 返回执行请求时的错误，无错误时返回 nil。若结果是 Status 且 Status != StatusSuccess，
// 会用其中的信息丰富错误。
// See the Request.Do() comment for what errors you might get.
func (r Result) Error() error {
	// 若收到的是意外的服务端错误，且有 body 和 decoder，就尝试从中提取 Status 对象。
	// 仅对“非结构化服务器错误”做二次解析；其他错误（含连接错误）原样返回。
	if r.err == nil || !errors.IsUnexpectedServerError(r.err) || len(r.body) == 0 || r.decoder == nil {
		return r.err
	}

	// 尝试把 body 转成 Status 对象。为兼容不返回版本的旧服务端，默认使用 "v1"。
	out, _, err := r.decoder.Decode(r.body, &schema.GroupVersionKind{Version: "v1"}, nil)
	if err != nil {
		r.logger.V(5).Info("Body was not decodable (unable to check for Status)", "err", err)
		return r.err
	}
	switch t := out.(type) {
	case *metav1.Status:
		// 由于 kind 是默认填充的，必须检查 StatusFailure。
		if t.Status == metav1.StatusFailure {
			return errors.FromObject(t)
		}
	}
	return r.err
}

// Warnings 返回响应中收到的所有 warning 头。
func (r Result) Warnings() []net.WarningHeader {
	return r.warnings
}

// NameMayNotBe 指定不能用作路径段名称（如 REST API 或 etcd 存储）的字符串。
var NameMayNotBe = []string{".", ".."}

// NameMayNotContain 指定不能出现在路径段名称中的子串。
var NameMayNotContain = []string{"/", "%"}

// IsValidPathSegmentName 校验名字能否被安全地编码为路径段，
// 返回人类可读的错误描述列表；空列表表示合法。
func IsValidPathSegmentName(name string) []string {
	for _, illegalName := range NameMayNotBe {
		if name == illegalName {
			return []string{fmt.Sprintf(`may not be '%s'`, illegalName)}
		}
	}

	var errors []string
	for _, illegalContent := range NameMayNotContain {
		if strings.Contains(name, illegalContent) {
			errors = append(errors, fmt.Sprintf(`may not contain '%s'`, illegalContent))
		}
	}

	return errors
}

// IsValidPathSegmentPrefix 校验名字能否作为「将被编码为路径段的名字」的前缀。
// 它不检查与被禁名字的精确匹配，因为任意后缀可能使名字变得合法。
// 用于 generateName 场景：前缀后面还会拼接随机后缀，因此 "." 或 ".." 可能变合法。
func IsValidPathSegmentPrefix(name string) []string {
	var errors []string
	for _, illegalContent := range NameMayNotContain {
		if strings.Contains(name, illegalContent) {
			errors = append(errors, fmt.Sprintf(`may not contain '%s'`, illegalContent))
		}
	}

	return errors
}

// ValidatePathSegmentName 校验名称能否安全地编码为路径段。
func ValidatePathSegmentName(name string, prefix bool) []string {
	// prefix 为 true 时放宽对 "."/".." 的检查。
	if prefix {
		return IsValidPathSegmentPrefix(name)
	}
	return IsValidPathSegmentName(name)
}
