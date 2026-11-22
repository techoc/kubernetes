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
	// Environment variables: Note that the duration should be long enough that the backoff
	// persists for some reasonable time (i.e. 120 seconds).  The typical base might be "1".
	envBackoffBase     = "KUBE_CLIENT_BACKOFF_BASE"
	envBackoffDuration = "KUBE_CLIENT_BACKOFF_DURATION"
)

// Interface captures the set of operations for generically interacting with Kubernetes REST apis.
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

// ClientContentConfig controls how RESTClient communicates with the server.
//
// TODO: ContentConfig will be updated to accept a Negotiator instead of a
// NegotiatedSerializer and NegotiatedSerializer will be removed.
type ClientContentConfig struct {
	// AcceptContentTypes specifies the types the client will accept and is optional.
	// If not set, ContentType will be used to define the Accept header
	AcceptContentTypes string
	// ContentType specifies the wire format used to communicate with the server.
	// This value will be set as the Accept header on requests made to the server if
	// AcceptContentTypes is not set, and as the default content type on any object
	// sent to the server. If not set, "application/json" is used.
	ContentType string
	// GroupVersion is the API version to talk to. Must be provided when initializing
	// a RESTClient directly. When initializing a Client, will be set with the default
	// code version. This is used as the default group version for VersionedParams.
	//
	// 该版本号用于给请求参数套上正确的类型（VersionedParams），
	// 因此直接构造 RESTClient 时必须显式设置，不能留空。
	GroupVersion schema.GroupVersion
	// Negotiator is used for obtaining encoders and decoders for multiple
	// supported media types.
	Negotiator runtime.ClientNegotiator
}

// RESTClient imposes common Kubernetes API conventions on a set of resource paths.
// The baseURL is expected to point to an HTTP or HTTPS path that is the parent
// of one or more resources.  The server should return a decodable API resource
// object, or an api.Status object which contains information about the reason for
// any failure.
//
// Most consumers should use client.New() to get a Kubernetes API client.
type RESTClient struct {
	// base is the root URL for all invocations of the client
	base *url.URL
	// versionedAPIPath is a path segment connecting the base URL to the resource root
	versionedAPIPath string

	// content describes how a RESTClient encodes and decodes responses.
	//
	// 除固定的序列化配置外，它还记录“服务端不支持 CBOR”的负缓存状态，
	// 因此被所有请求共享。
	content requestClientContentConfigProvider

	// creates BackoffManager that is passed to requests.
	//
	// 用函数而不是实例，是为了让每个请求拿到独立的退避状态。
	createBackoffMgr func() BackoffManagerWithContext

	// rateLimiter is shared among all requests created by this client unless specifically
	// overridden.
	rateLimiter flowcontrol.RateLimiter

	// warningHandler is shared among all requests created by this client.
	// If not set, defaultWarningHandler is used.
	warningHandler WarningHandlerWithContext

	// Set specific behavior of the client.  If not set http.DefaultClient will be used.
	Client *http.Client
}

// NewRESTClient creates a new RESTClient. This client performs generic REST functions
// such as Get, Put, Post, and Delete on specified paths.
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
		// No application/cbor in AcceptContentTypes, nothing more to do.
		return content
	}

	parts := make([]string, 0, len(clauses))
	for _, clause := range clauses {
		// ParseAccept does not store the parameter "q" in Params.
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

// GetRateLimiter returns rate limiter for a given client, or nil if it's called on a nil client
func (c *RESTClient) GetRateLimiter() flowcontrol.RateLimiter {
	if c == nil {
		return nil
	}
	return c.rateLimiter
}

// readExpBackoffConfig handles the internal logic of determining what the
// backoff policy is.  By default if no information is available, NoBackoff.
// TODO Generalize this see #17727 .
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

// Verb begins a request with a verb (GET, POST, PUT, DELETE).
//
// Example usage of RESTClient's request building interface:
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

// Post begins a POST request. Short for c.Verb("POST").
func (c *RESTClient) Post() *Request {
	return c.Verb("POST")
}

// Put begins a PUT request. Short for c.Verb("PUT").
func (c *RESTClient) Put() *Request {
	return c.Verb("PUT")
}

// Patch begins a PATCH request. Short for c.Verb("Patch").
func (c *RESTClient) Patch(pt types.PatchType) *Request {
	return c.Verb("PATCH").SetHeader("Content-Type", string(pt))
}

// Get begins a GET request. Short for c.Verb("GET").
func (c *RESTClient) Get() *Request {
	return c.Verb("GET")
}

// Delete begins a DELETE request. Short for c.Verb("DELETE").
func (c *RESTClient) Delete() *Request {
	return c.Verb("DELETE")
}

// APIVersion returns the APIVersion this RESTClient is expected to use.
func (c *RESTClient) APIVersion() schema.GroupVersion {
	config, _ := c.content.GetClientContentConfig()
	return config.GroupVersion
}

// requestClientContentConfigProvider observes HTTP 415 (Unsupported Media Type) responses to detect
// that the server does not understand CBOR. Once this has happened, future requests are forced to
// use JSON so they can succeed. This is convenient for client users that want to prefer CBOR, but
// also need to interoperate with older servers so requests do not permanently fail. The clients
// will not default to using CBOR until at least all supported kube-apiservers have enable-CBOR
// locked to true, so this path will be rarely taken. Additionally, all generated clients accessing
// built-in kube resources are forced to protobuf, so those will not degrade to JSON.
type requestClientContentConfigProvider struct {
	base ClientContentConfig

	// Becomes permanently true if a server responds with HTTP 415 (Unsupported Media Type) to a
	// request with "Content-Type" header containing the CBOR media type.
	sawUnsupportedMediaTypeForCBOR atomic.Bool
}

// GetClientContentConfig returns the ClientContentConfig that should be used for new requests by
// this client and true if the request ContentType was selected by default.
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

	// The effective ContentType is CBOR and the client has previously received an HTTP 415 in
	// response to a CBOR request. Override ContentType to JSON.
	config.ContentType = runtime.ContentTypeJSON
	return config, defaulted
}

// UnsupportedMediaType reports that the server has responded to a request with HTTP 415 Unsupported
// Media Type.
func (p *requestClientContentConfigProvider) UnsupportedMediaType(requestContentType string) {
	if !clientfeatures.FeatureGates().Enabled(clientfeatures.ClientsAllowCBOR) {
		return
	}

	// This could be extended to consider the Content-Encoding request header, the Accept and
	// Accept-Encoding response headers, the request method, and URI (as mentioned in
	// https://www.rfc-editor.org/rfc/rfc9110.html#section-15.5.16). The request Content-Type
	// header is sufficient to implement a blanket CBOR fallback mechanism.
	//
	// 只根据请求的 Content-Type 判断：415 若是针对 CBOR 或其 patch 类型，
	// 就永久关闭该客户端后续的 CBOR 使用（atomic.Bool 保证并发安全）。
	requestContentType, _, _ = mime.ParseMediaType(requestContentType)
	switch requestContentType {
	case runtime.ContentTypeCBOR, string(types.ApplyCBORPatchType):
		p.sawUnsupportedMediaTypeForCBOR.Store(true)
	}
}

// clientNegotiatorWithCBORSequenceStreamDecoder is a ClientNegotiator that delegates to another
// ClientNegotiator to select the appropriate Encoder or Decoder for a given media type. As a
// special case, it will resolve "application/cbor-seq" (a CBOR Sequence, the concatenation of zero
// or more CBOR data items) as an alias for "application/cbor" (exactly one CBOR data item) when
// selecting a stream decoder.
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
		// This media type is only appropriate for exactly one data item, not the zero or
		// more events of a watch stream.
		return nil, nil, nil, runtime.NegotiateError{ContentType: contentType, Stream: true}
	default:
		return n.negotiator.StreamDecoder(contentType, params)
	}

}
