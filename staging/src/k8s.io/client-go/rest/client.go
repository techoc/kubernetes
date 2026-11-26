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
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/munnerz/goautoneg"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/util/flowcontrol"
)

const (
	// 环境变量：退避持续时间应足够长（例如 120 秒），base 通常取 "1"。
	envBackoffBase     = "KUBE_CLIENT_BACKOFF_BASE"
	envBackoffDuration = "KUBE_CLIENT_BACKOFF_DURATION"
)

// Interface 定义与 Kubernetes REST API 通用交互所需的操作集合。
//
// 它是 RESTClient 的最小抽象：生成的客户端只依赖该接口，
// 使测试可以注入 fake transport，也便于在中间层替换实现。
type Interface interface {
	GetRateLimiter() flowcontrol.RateLimiter
	Verb(verb string) *Request
	Post() *Request
	Put() *Request
	Patch(pt types.PatchType) *Request
	Get() *Request
	Delete() *Request
	APIVersion() schema.GroupVersion
}

// ClientContentConfig 控制 RESTClient 与服务端通信的内容协商方式。
//
// TODO：ContentConfig 将改为接受 Negotiator 而非 NegotiatedSerializer，后者会被移除。
type ClientContentConfig struct {
	// AcceptContentTypes 指定客户端可接受的类型，可选；未设置时用 ContentType 填充 Accept 头。
	AcceptContentTypes string
	// ContentType 指定与服务端通信的线上格式：未设置 AcceptContentTypes 时作为 Accept 头，
	// 同时作为发送给服务端的对象的默认 Content-Type。未设置时取 "application/json"。
	ContentType string
	// GroupVersion 是通信所用的 API 版本。直接初始化 RESTClient 时必须提供；通过 Client
	// 初始化时取默认代码版本。它作为 VersionedParams 的默认 group version。
	//
	// 该版本号用于给请求参数套上正确的类型（VersionedParams），
	// 因此直接构造 RESTClient 时必须显式设置，不能留空。
	GroupVersion schema.GroupVersion
	// Negotiator 用于为多种媒体类型获取编解码器。
	Negotiator runtime.ClientNegotiator
}

// RESTClient 在一组资源路径上施加 Kubernetes API 的通用约定：baseURL 应指向一个或多个
// 资源的父级 HTTP(S) 路径，服务端应返回可解码的 API 资源对象，或说明失败原因的 api.Status。
//
// 大多数调用方应使用 client.New() 获取 Kubernetes API 客户端。
type RESTClient struct {
	// base 是客户端所有调用的根 URL。
	base *url.URL
	// versionedAPIPath 连接 base URL 与资源根路径。
	versionedAPIPath string

	// content 描述 RESTClient 如何编解码响应。
	//
	// 除固定的序列化配置外，它还记录“服务端不支持 CBOR”的负缓存状态，
	// 因此被所有请求共享。
	content requestClientContentConfigProvider

	// 创建传给各请求的 BackoffManager。
	//
	// 用函数而不是实例，是为了让每个请求拿到独立的退避状态。
	createBackoffMgr func() BackoffManagerWithContext

	// 除非单独覆盖，rateLimiter 由该客户端创建的所有请求共享。
	rateLimiter flowcontrol.RateLimiter

	// warningHandler 由所有请求共享，未设置时使用 defaultWarningHandler。
	warningHandler WarningHandlerWithContext

	// 设置客户端的特定行为；未设置时使用 http.DefaultClient。
	Client *http.Client
}

// NewRESTClient 创建 RESTClient，在指定路径上执行 Get/Put/Post/Delete 等通用 REST 操作。
func NewRESTClient(baseURL *url.URL, versionedAPIPath string, config ClientContentConfig, rateLimiter flowcontrol.RateLimiter, client *http.Client) (*RESTClient, error) {
	metrics.EnsureRegistered()

	// 复制 URL 再规范化：调用方传入的 *url.URL 不应被修改，
	// 同时清空 query/fragment，避免它们意外出现在每个请求上。
	base := *baseURL
	if !strings.HasSuffix(base.Path, "/") {
		base.Path += "/"
	}
	base.RawQuery = ""
	base.Fragment = ""

	return &RESTClient{
		base:             &base,
		versionedAPIPath: versionedAPIPath,
		content:          requestClientContentConfigProvider{base: scrubCBORContentConfigIfDisabled(config)},
		createBackoffMgr: readExpBackoffConfig,
		rateLimiter:      rateLimiter,
		Client:           client,
	}, nil
}

// scrubCBORContentConfigIfDisabled 在 CBOR 门控关闭时，把配置中的 CBOR 一律降级为 JSON。
//
// 这样即使用户 kubeconfig 或调用方硬编码了 application/cbor，旧服务端也不会因
// 不认识的媒体类型而失败。
func scrubCBORContentConfigIfDisabled(content ClientContentConfig) ClientContentConfig {
	if clientfeatures.FeatureGates().Enabled(clientfeatures.ClientsAllowCBOR) {
		// 门控打开时额外包一层，把 "application/cbor-seq" 当作流的别名。
		content.Negotiator = clientNegotiatorWithCBORSequenceStreamDecoder{content.Negotiator}
		return content
	}

	// 显式指定的 CBOR ContentType 降级为 JSON。
	if mediatype, _, err := mime.ParseMediaType(content.ContentType); err == nil && mediatype == "application/cbor" {
		content.ContentType = "application/json"
	}

	clauses := goautoneg.ParseAccept(content.AcceptContentTypes)
	scrubbed := false
	for i, clause := range clauses {
		// Accept 头里可能同时列了 cbor 与 json，只把 cbor 子类型替换为 json，
		// 保留其 q 值，从而尽量维持原有的偏好顺序。
		if clause.Type == "application" && clause.SubType == "cbor" {
			scrubbed = true
			clauses[i].SubType = "json"
		}
	}
	if !scrubbed {
		// AcceptContentTypes 中不含 application/cbor，无需处理。
		return content
	}

	parts := make([]string, 0, len(clauses))
	for _, clause := range clauses {
		// ParseAccept 不会把参数 "q" 存进 Params。
		params := clause.Params
		if clause.Q < 1 { // omit q=1, it's the default
			if params == nil {
				params = make(map[string]string, 1)
			}
			params["q"] = strconv.FormatFloat(clause.Q, 'g', 3, 32)
		}
		parts = append(parts, mime.FormatMediaType(fmt.Sprintf("%s/%s", clause.Type, clause.SubType), params))
	}
	content.AcceptContentTypes = strings.Join(parts, ",")

	return content
}

// GetRateLimiter 返回客户端的限流器；在 nil 客户端上调用时返回 nil。
func (c *RESTClient) GetRateLimiter() flowcontrol.RateLimiter {
	if c == nil {
		return nil
	}
	return c.rateLimiter
}

// readExpBackoffConfig 决定退避策略；无可用信息时默认 NoBackoff。
// TODO：泛化该逻辑，见 #17727。
//
// 退避参数通过环境变量注入，环境变量缺失或格式错误时明确退回 NoBackoff，
// 避免客户端在配置损坏时无限重试。
func readExpBackoffConfig() BackoffManagerWithContext {
	backoffBase := os.Getenv(envBackoffBase)
	backoffDuration := os.Getenv(envBackoffDuration)

	backoffBaseInt, errBase := strconv.ParseInt(backoffBase, 10, 64)
	backoffDurationInt, errDuration := strconv.ParseInt(backoffDuration, 10, 64)
	if errBase != nil || errDuration != nil {
		return &NoBackoff{}
	}
	// 两个环境变量都以秒为单位。
	return &URLBackoff{
		Backoff: flowcontrol.NewBackOff(
			time.Duration(backoffBaseInt)*time.Second,
			time.Duration(backoffDurationInt)*time.Second)}
}

// Verb 以指定动词（GET、POST、PUT、DELETE 等）开始构造请求。
//
// RESTClient 请求构造接口的使用示例：
// c, err := NewRESTClient(...)
// if err != nil { ... }
// resp, err := c.Verb("GET").
//
//	Path("pods").
//	SelectorParam("labels", "area=staging").
//	Timeout(10*time.Second).
//	Do()
//
// if err != nil { ... }
// list, ok := resp.(*api.PodList)
//
// 每次调用都会创建新的 Request（含独立的退避与限流包装），
// 因此 Request 不可跨 goroutine 复用。
func (c *RESTClient) Verb(verb string) *Request {
	return NewRequest(c).Verb(verb)
}

// Post 开始一个 POST 请求，等价于 c.Verb("POST")。
func (c *RESTClient) Post() *Request {
	return c.Verb("POST")
}

// Put 开始一个 PUT 请求，等价于 c.Verb("PUT")。
func (c *RESTClient) Put() *Request {
	return c.Verb("PUT")
}

// Patch 开始一个 PATCH 请求，等价于 c.Verb("Patch")。
func (c *RESTClient) Patch(pt types.PatchType) *Request {
	return c.Verb("PATCH").SetHeader("Content-Type", string(pt))
}

// Get 开始一个 GET 请求，等价于 c.Verb("GET")。
func (c *RESTClient) Get() *Request {
	return c.Verb("GET")
}

// Delete 开始一个 DELETE 请求，等价于 c.Verb("DELETE")。
func (c *RESTClient) Delete() *Request {
	return c.Verb("DELETE")
}

// APIVersion 返回该 RESTClient 应使用的 API 版本。
func (c *RESTClient) APIVersion() schema.GroupVersion {
	config, _ := c.content.GetClientContentConfig()
	return config.GroupVersion
}

// requestClientContentConfigProvider 通过 HTTP 415（Unsupported Media Type）判断服务端是否
// 不认识 CBOR，一旦出现就强制后续请求改用 JSON，以免持续失败。这让偏好 CBOR 的客户端
// 也能兼容旧服务端。在所有受支持的 kube-apiserver 都锁定启用 CBOR 之前，客户端不会默认
// 选 CBOR，因此该路径很少走到；访问内置资源的生成客户端会被强制用 protobuf，也不会降级。
type requestClientContentConfigProvider struct {
	base ClientContentConfig

	// 服务端对带 CBOR Content-Type 的请求返回 415 后，该标志永久置为 true。
	sawUnsupportedMediaTypeForCBOR atomic.Bool
}

// GetClientContentConfig 返回新请求应使用的 ClientContentConfig，以及 ContentType 是否为默认值。
//
// 返回值中的 bool 表示 ContentType 是否来自默认值；调用方据此区分
// “用户显式要求 CBOR”和“客户端自行偏好 CBOR”，决定是否允许降级。
func (p *requestClientContentConfigProvider) GetClientContentConfig() (ClientContentConfig, bool) {
	config := p.base

	defaulted := config.ContentType == ""
	if defaulted {
		config.ContentType = "application/json"
	}

	if !clientfeatures.FeatureGates().Enabled(clientfeatures.ClientsAllowCBOR) {
		return config, defaulted
	}

	// 只有在用户未指定 ContentType 时才主动偏好 CBOR；
	// 显式指定了其他格式时尊重用户选择。
	if defaulted && clientfeatures.FeatureGates().Enabled(clientfeatures.ClientsPreferCBOR) {
		config.ContentType = "application/cbor"
	}

	if sawUnsupportedMediaTypeForCBOR := p.sawUnsupportedMediaTypeForCBOR.Load(); !sawUnsupportedMediaTypeForCBOR {
		return config, defaulted
	}

	if mediaType, _, _ := mime.ParseMediaType(config.ContentType); mediaType != runtime.ContentTypeCBOR {
		return config, defaulted
	}

	// 实际 ContentType 为 CBOR 且此前 CBOR 请求收到过 415，此时降级为 JSON。
	config.ContentType = runtime.ContentTypeJSON
	return config, defaulted
}

// UnsupportedMediaType 表示服务端对请求返回了 HTTP 415 Unsupported Media Type。
func (p *requestClientContentConfigProvider) UnsupportedMediaType(requestContentType string) {
	if !clientfeatures.FeatureGates().Enabled(clientfeatures.ClientsAllowCBOR) {
		return
	}

	// 这里可扩展为同时考虑 Content-Encoding 请求头、Accept/Accept-Encoding 响应头、
	// 请求方法与 URI（如
	// https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.16). The request Content-Type
	// 仅凭该请求头已足以实现全局的 CBOR 回退机制。
	//
	// 只根据请求的 Content-Type 判断：415 若是针对 CBOR 或其 patch 类型，
	// 就永久关闭该客户端后续的 CBOR 使用（atomic.Bool 保证并发安全）。
	requestContentType, _, _ = mime.ParseMediaType(requestContentType)
	switch requestContentType {
	case runtime.ContentTypeCBOR, string(types.ApplyCBORPatchType):
		p.sawUnsupportedMediaTypeForCBOR.Store(true)
	}
}

// clientNegotiatorWithCBORSequenceStreamDecoder 委托另一个 ClientNegotiator 按媒体类型选择编解码器。
// 特例是选择流解码器时，把 "application/cbor-seq"（零个或多个 CBOR 数据项的序列）
// 当作 "application/cbor"（恰好一个数据项）的别名。
type clientNegotiatorWithCBORSequenceStreamDecoder struct {
	negotiator runtime.ClientNegotiator
}

func (n clientNegotiatorWithCBORSequenceStreamDecoder) Encoder(contentType string, params map[string]string) (runtime.Encoder, error) {
	return n.negotiator.Encoder(contentType, params)
}

func (n clientNegotiatorWithCBORSequenceStreamDecoder) Decoder(contentType string, params map[string]string) (runtime.Decoder, error) {
	return n.negotiator.Decoder(contentType, params)
}

func (n clientNegotiatorWithCBORSequenceStreamDecoder) StreamDecoder(contentType string, params map[string]string) (runtime.Decoder, runtime.Serializer, runtime.Framer, error) {
	if !clientfeatures.FeatureGates().Enabled(clientfeatures.ClientsAllowCBOR) {
		return n.negotiator.StreamDecoder(contentType, params)
	}

	switch contentType {
	case runtime.ContentTypeCBORSequence:
		// CBOR Sequence 允许零个或多个数据项，正好对应 watch 流的多个事件，
		// 因此复用单个 CBOR 解码器即可。
		return n.negotiator.StreamDecoder(runtime.ContentTypeCBOR, params)
	case runtime.ContentTypeCBOR:
		// 该媒体类型只适合恰好一个数据项，不适合 watch 流的零到多个事件。
		return nil, nil, nil, runtime.NegotiateError{ContentType: contentType, Stream: true}
	default:
		return n.negotiator.StreamDecoder(contentType, params)
	}

}
