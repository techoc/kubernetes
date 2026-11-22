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

// This is made a separate package and should only be imported by tests, because
// it imports testapi
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

// CreateHTTPClient creates an http.Client that will invoke the provided roundTripper func
// when a request is made.
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

// RESTClient provides a fake RESTClient interface. It is used to mock network
// interactions via a rest.Request, or to make them via the provided Client to
// a specific server.
//
// 字段优先级约定：Err 最高；其次 Client；最后才返回 Resp。
type RESTClient struct {
	NegotiatedSerializer runtime.NegotiatedSerializer
	GroupVersion         schema.GroupVersion
	VersionedAPIPath     string

	// Err is returned when any request would be made to the server. If Err is set,
	// Req will not be recorded, Resp will not be returned, and Client will not be
	// invoked.
	Err error
	// Req is set to the last request that was executed (had the methods Do/DoRaw) invoked.
	// 注意只保留最后一次请求，便于断言最终发出的 URL/方法/请求体。
	Req *http.Request
	// If Client is specified, the client will be invoked instead of returning Resp if
	// Err is not set.
	Client *http.Client
	// Resp is returned to the caller after Req is recorded, unless Err or Client are set.
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

// do is invoked when a Request() created by this client is executed.
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
