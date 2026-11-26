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

// 本包单独成包且只应由测试导入，因为它依赖 testapi。
//
// Package fake 提供可在测试中替换 rest.Interface 的假实现，
// 让生成的 typed client 在不启动 apiserver 的情况下被调用和断言。
package fake

import (
	"net/http"
	"net/url"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
)

// CreateHTTPClient 创建 http.Client，它会调用给定的 roundTripper 函数。
// 构造请求时返回。
//
// 返回的 client 把 Transport 替换为函数适配器；测试可通过它捕获请求或返回固定响应。
func CreateHTTPClient(roundTripper func(*http.Request) (*http.Response, error)) *http.Client {
	return &http.Client{
		Transport: roundTripperFunc(roundTripper),
	}
}

// roundTripperFunc 让普通函数满足 http.RoundTripper 接口。
type roundTripperFunc func(*http.Request) (*http.Response, error)

// RoundTrip 调用被包装的函数，不做任何额外处理。
func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// RESTClient 提供假的 RESTClient 接口，用于 mock 通过 rest.Request 发起的
// 网络交互，或把请求交给给定 Client 发送到指定服务端。
//
// 字段优先级约定：Err 最高；其次 Client；最后才返回 Resp。
type RESTClient struct {
	NegotiatedSerializer runtime.NegotiatedSerializer
	GroupVersion         schema.GroupVersion
	VersionedAPIPath     string

	// Err 非空时任何请求都会返回该错误：不记录 Req、不返回 Resp，也不会调用 Client。
	Err error
	// Req 是最后一次被执行（调用过 Do/DoRaw）的请求。
	// 注意只保留最后一次请求，便于断言最终发出的 URL/方法/请求体。
	Req *http.Request
	// 若指定了 Client 且 Err 未设置，则调用 Client 而不是返回 Resp。
	Client *http.Client
	// 在记录 Req 之后返回 Resp；若设置了 Err 或 Client 则不返回。
	Resp *http.Response
}

// Get 构造 GET 请求。
func (c *RESTClient) Get() *restclient.Request {
	return c.Verb("GET")
}

// Put 构造 PUT 请求，常用于 Update 或子资源更新。
func (c *RESTClient) Put() *restclient.Request {
	return c.Verb("PUT")
}

// Patch 构造 PATCH 请求，并按补丁类型设置 Content-Type。
func (c *RESTClient) Patch(pt types.PatchType) *restclient.Request {
	return c.Verb("PATCH").SetHeader("Content-Type", string(pt))
}

// Post 构造 POST 请求。
func (c *RESTClient) Post() *restclient.Request {
	return c.Verb("POST")
}

// Delete 构造 DELETE 请求。
func (c *RESTClient) Delete() *restclient.Request {
	return c.Verb("DELETE")
}

// Verb 构造指定 HTTP 方法的请求。
func (c *RESTClient) Verb(verb string) *restclient.Request {
	return c.Request().Verb(verb)
}

// APIVersion 返回该客户端使用的 GroupVersion；生成的 typed client 依赖它。
func (c *RESTClient) APIVersion() schema.GroupVersion {
	return c.GroupVersion
}

// GetRateLimiter 返回 nil，表示假客户端不做客户端限流。
func (c *RESTClient) GetRateLimiter() flowcontrol.RateLimiter {
	return nil
}

// Request 创建一个 rest.Request。URL 中的 host 是占位值，
// 只有真正执行 Do 时才会通过 do 回调（或注入的 Client）处理请求。
func (c *RESTClient) Request() *restclient.Request {
	config := restclient.ClientContentConfig{
		ContentType:  runtime.ContentTypeJSON,
		GroupVersion: c.GroupVersion,
		Negotiator:   runtime.NewClientNegotiator(c.NegotiatedSerializer, c.GroupVersion),
	}
	return restclient.NewRequestWithClient(&url.URL{Scheme: "https", Host: "localhost"}, c.VersionedAPIPath, config, CreateHTTPClient(c.do))
}

// do 在本客户端创建的 Request() 被执行时调用。
// 它实现了请求拦截：优先返回 Err，其次委托给 Client，最后直接返回预设的 Resp。
func (c *RESTClient) do(req *http.Request) (*http.Response, error) {
	if c.Err != nil {
		return nil, c.Err
	}
	c.Req = req
	if c.Client != nil {
		return c.Client.Do(req)
	}
	return c.Resp, nil
}
