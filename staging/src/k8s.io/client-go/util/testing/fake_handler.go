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

package testing

import (
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sync"
)

// TestInterface 是只提供 Errorf 的轻量接口，便于在测试中注入 *testing.T。
type TestInterface interface {
	Errorf(format string, args ...interface{})
	Logf(format string, args ...interface{})
}

// LogInterface 是允许注入 Logf 以报告服务错误的轻量接口。
type LogInterface interface {
	Logf(format string, args ...interface{})
}

// FakeHandler 辅助测试 HTTP 请求。注意它不是线程安全的，除待测请求外不应把
// 其他流量导向它。
// 可把它藏在 http.ServeMux 后面，只暴露待测的那部分。
//
// 只能记录一次请求：记录后若再有请求进来会 panic，用于发现意外的重试/额外调用。
type FakeHandler struct {
	RequestReceived *http.Request
	RequestBody     string
	StatusCode      int
	ResponseBody    string
	// 用于记录日志，可传入 *testing.T，使日志与测试关联。
	T LogInterface

	// 强制“只用一次”的约束，避免复用导致计数误判。
	lock           sync.Mutex
	requestCount   int
	hasBeenChecked bool

	// SkipRequestFn 返回 true 时直接返回预设响应，不记录请求（用于探活等噪声请求）。
	SkipRequestFn func(verb string, url url.URL) bool
}

// SetResponseBody 加锁更新响应体，便于测试中途改变行为。
func (f *FakeHandler) SetResponseBody(responseBody string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.ResponseBody = responseBody
}

// ServeHTTP 记录请求并返回预设状态码/响应体；响应先写出，再读取请求体，
// 使调用方能及时得到响应。
func (f *FakeHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.SkipRequestFn != nil && f.SkipRequestFn(request.Method, *request.URL) {
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(f.StatusCode)
		response.Write([]byte(f.ResponseBody))
		return
	}

	f.requestCount++
	if f.hasBeenChecked {
		// 校验之后仍有请求，说明被测代码发生了预期外的调用。
		panic("got request after having been validated")
	}

	f.RequestReceived = request
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(f.StatusCode)
	response.Write([]byte(f.ResponseBody))

	bodyReceived, err := io.ReadAll(request.Body)
	if err != nil && f.T != nil {
		f.T.Logf("Received read error: %v", err)
	}
	f.RequestBody = string(bodyReceived)
	if f.T != nil {
		f.T.Logf("request body: %s", f.RequestBody)
	}
}

// ValidateRequestCount 断言收到的请求次数，并标记 handler 已校验（后续请求会 panic）。
func (f *FakeHandler) ValidateRequestCount(t TestInterface, count int) bool {
	ok := true
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.requestCount != count {
		ok = false
		t.Errorf("Expected %d call, but got %d. Only the last call is recorded and checked.", count, f.requestCount)
	}
	f.hasBeenChecked = true
	return ok
}

// ValidateRequest 校验 FakeHandler 收到的请求路径、方法与请求体是否符合预期。
func (f *FakeHandler) ValidateRequest(t TestInterface, expectedPath, expectedMethod string, body *string) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if f.requestCount != 1 {
		t.Logf("Expected 1 call, but got %v. Only the last call is recorded and checked.", f.requestCount)
	}
	f.hasBeenChecked = true

	expectURL, err := url.Parse(expectedPath)
	if err != nil {
		t.Errorf("Couldn't parse %v as a URL.", expectedPath)
	}
	if f.RequestReceived == nil {
		t.Errorf("Unexpected nil request received for %s", expectedPath)
		return
	}
	// 分别比较 path/query/method/body，失败时错误信息能直接指出差异字段。
	if f.RequestReceived.URL.Path != expectURL.Path {
		t.Errorf("Unexpected request path for request %#v, received: %q, expected: %q", f.RequestReceived, f.RequestReceived.URL.Path, expectURL.Path)
	}
	if e, a := expectURL.Query(), f.RequestReceived.URL.Query(); !reflect.DeepEqual(e, a) {
		t.Errorf("Unexpected query for request %#v, received: %q, expected: %q", f.RequestReceived, a, e)
	}
	if f.RequestReceived.Method != expectedMethod {
		t.Errorf("Unexpected method: %q, expected: %q", f.RequestReceived.Method, expectedMethod)
	}
	if body != nil {
		if *body != f.RequestBody {
			t.Errorf("Received body:\n%s\n Doesn't match expected body:\n%s", f.RequestBody, *body)
		}
	}
}
