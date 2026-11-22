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
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	goruntime "runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/streaming"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	restclientwatch "k8s.io/client-go/rest/watch"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/client-go/util/flowcontrol"
	utiltesting "k8s.io/client-go/util/testing"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	testingclock "k8s.io/utils/clock/testing"
)

// TestNewRequestSetsAccept 验证 Accept 头的默认构造。
//
// 中文说明：未配置 ContentType 时默认接受 JSON；配置后把它放在首位，再追加 */* 作为
// 兜底，使服务端在无法满足首选类型时仍可返回其它表示。
func TestNewRequestSetsAccept(t *testing.T) {
	r := NewRequestWithClient(&url.URL{Path: "/path/"}, "", ClientContentConfig{}, nil).Verb("get")
	if r.headers.Get("Accept") != "application/json, */*" {
		t.Errorf("unexpected headers: %#v", r.headers)
	}
	r = NewRequestWithClient(&url.URL{Path: "/path/"}, "", ClientContentConfig{ContentType: "application/other"}, nil).Verb("get")
	if r.headers.Get("Accept") != "application/other, */*" {
		t.Errorf("unexpected headers: %#v", r.headers)
	}
}

// clientForFunc 把函数包装成 *http.Client，便于测试逐次控制响应。
func clientForFunc(fn clientFunc) *http.Client {
	return &http.Client{
		Transport: fn,
	}
}

// clientFunc 是 http.RoundTripper 的函数式适配器，方便在测试中内联响应逻辑。
type clientFunc func(req *http.Request) (*http.Response, error)

// RoundTrip 实现 http.RoundTripper。
func (f clientFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// TestRequestSetsHeaders 确保 Do/Watch/Stream 三条执行路径都会发送 Accept 头。
//
// 中文：服务端对三条路径都返回 403，断言只关心请求头是否正确，不关心响应结果。
func TestRequestSetsHeaders(t *testing.T) {
	server := clientForFunc(func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Accept") != "application/other, */*" {
			t.Errorf("unexpected headers: %#v", req.Header)
		}
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Body:       io.NopCloser(bytes.NewReader([]byte{})),
		}, nil
	})
	config := defaultContentConfig()
	config.ContentType = "application/other"
	r := NewRequestWithClient(&url.URL{Path: "/path"}, "", config, nil).Verb("get")
	r.c.Client = server

	// Check if all "issue" methods are setting headers.
	_ = r.Do(context.Background())
	_, _ = r.Watch(context.Background())
	_, _ = r.Stream(context.Background())
}

// TestRequestWithErrorWontChange 验证已记录错误的 Request 在后续链式调用中保持原状。
//
// 中文说明：Request 是不可变风格的 builder；一旦出错，后续方法应短路并返回同一对象，
// 避免把“已失败”的请求悄悄修复成可发送状态。
func TestRequestWithErrorWontChange(t *testing.T) {
	gvCopy := v1.SchemeGroupVersion
	original := Request{
		err: errors.New("test"),
		c:   &RESTClient{},

		contentConfig: ClientContentConfig{GroupVersion: gvCopy},
	}
	r := original
	changed := r.Param("foo", "bar").
		AbsPath("/abs").
		Prefix("test").
		Suffix("testing").
		Namespace("new").
		Resource("foos").
		Name("bars").
		Body("foo").
		Timeout(time.Millisecond)
	if changed != &r {
		t.Errorf("returned request should point to the same object")
	}
	if !reflect.DeepEqual(changed, &original) {
		t.Errorf("expected %#v, got %#v", &original, changed)
	}
}

// TestRequestPreservesBaseTrailingSlash 确保 base/prefix 的尾斜杠不会被规范化掉。
func TestRequestPreservesBaseTrailingSlash(t *testing.T) {
	r := &Request{c: &RESTClient{base: &url.URL{}}, pathPrefix: "/path/"}
	if s := r.URL().String(); s != "/path/" {
		t.Errorf("trailing slash should be preserved: %s", s)
	}
}

// TestRequestAbsPathPreservesTrailingSlash 确保 AbsPath 不会丢失尾斜杠。
func TestRequestAbsPathPreservesTrailingSlash(t *testing.T) {
	r := (&Request{c: &RESTClient{base: &url.URL{}}}).AbsPath("/foo/")
	if s := r.URL().String(); s != "/foo/" {
		t.Errorf("trailing slash should be preserved: %s", s)
	}
}

// TestRequestAbsPathJoins 验证 AbsPath 的多个段以 / 连接。
func TestRequestAbsPathJoins(t *testing.T) {
	r := (&Request{c: &RESTClient{base: &url.URL{}}}).AbsPath("foo/bar", "baz")
	if s := r.URL().String(); s != "foo/bar/baz" {
		t.Errorf("trailing slash should be preserved: %s", s)
	}
}

// TestRequestSetsNamespace 验证 Namespace 会被写入 URL 路径。
func TestRequestSetsNamespace(t *testing.T) {
	r := (&Request{
		c: &RESTClient{base: &url.URL{Path: "/"}},
	}).Namespace("foo")
	if r.namespace == "" {
		t.Errorf("namespace should be set: %#v", r)
	}

	if s := r.URL().String(); s != "namespaces/foo" {
		t.Errorf("namespace should be in path: %s", s)
	}
}

// TestRequestOrdersNamespaceInPath 验证路径段顺序固定为
// namespaces/<namespace>/<resource>/<name>，与调用顺序无关。
func TestRequestOrdersNamespaceInPath(t *testing.T) {
	r := (&Request{
		c:          &RESTClient{base: &url.URL{}},
		pathPrefix: "/test/",
	}).Name("bar").Resource("baz").Namespace("foo")
	if s := r.URL().String(); s != "/test/namespaces/foo/baz/bar" {
		t.Errorf("namespace should be in order in path: %s", s)
	}
}

// TestRequestOrdersSubResource 验证子资源位于 name 之后、suffix 之前。
func TestRequestOrdersSubResource(t *testing.T) {
	r := (&Request{
		c:          &RESTClient{base: &url.URL{}},
		pathPrefix: "/test/",
	}).Name("bar").Resource("baz").Namespace("foo").Suffix("test").SubResource("a", "b")
	if s := r.URL().String(); s != "/test/namespaces/foo/baz/bar/a/b/test" {
		t.Errorf("namespace should be in order in path: %s", s)
	}
}

// TestRequestSetTwiceError 验证 name/namespace/resource/subresource 重复设置会报错。
//
// 中文：同一语义字段只允许设置一次，重复设置往往意味着调用方逻辑错误，早失败更安全。
func TestRequestSetTwiceError(t *testing.T) {
	if (&Request{}).Name("bar").Name("baz").err == nil {
		t.Errorf("setting name twice should result in error")
	}
	if (&Request{}).Namespace("bar").Namespace("baz").err == nil {
		t.Errorf("setting namespace twice should result in error")
	}
	if (&Request{}).Resource("bar").Resource("baz").err == nil {
		t.Errorf("setting resource twice should result in error")
	}
	if (&Request{}).SubResource("bar").SubResource("baz").err == nil {
		t.Errorf("setting subresource twice should result in error")
	}
}

// TestInvalidSegments 验证路径段校验会拒绝 "."、".."、含 "/" 或 URL 编码斜杠的值。
//
// 中文：这些输入可能造成路径穿越或改变资源层级，必须在构造请求时拒绝。
func TestInvalidSegments(t *testing.T) {
	invalidSegments := []string{".", "..", "test/segment", "test%2bsegment"}
	setters := map[string]func(string, *Request){
		"namespace":   func(s string, r *Request) { r.Namespace(s) },
		"resource":    func(s string, r *Request) { r.Resource(s) },
		"name":        func(s string, r *Request) { r.Name(s) },
		"subresource": func(s string, r *Request) { r.SubResource(s) },
	}
	for _, invalidSegment := range invalidSegments {
		for setterName, setter := range setters {
			r := &Request{}
			setter(invalidSegment, r)
			if r.err == nil {
				t.Errorf("%s: %s: expected error, got none", setterName, invalidSegment)
			}
		}
	}
}

// TestRequestParam 验证同名查询参数会被追加而非覆盖。
func TestRequestParam(t *testing.T) {
	r := (&Request{}).Param("foo", "a")
	if !reflect.DeepEqual(r.params, url.Values{"foo": []string{"a"}}) {
		t.Errorf("should have set a param: %#v", r)
	}

	r.Param("bar", "1")
	r.Param("bar", "2")
	if !reflect.DeepEqual(r.params, url.Values{"foo": []string{"a"}, "bar": []string{"1", "2"}}) {
		t.Errorf("should have set a param: %#v", r)
	}
}

// TestRequestVersionedParams 验证结构化 options 经 ParameterCodec 转为查询参数，
// 并与已有参数合并。
func TestRequestVersionedParams(t *testing.T) {
	r := (&Request{c: &RESTClient{}, contentConfig: ClientContentConfig{GroupVersion: v1.SchemeGroupVersion}}).Param("foo", "a")
	if !reflect.DeepEqual(r.params, url.Values{"foo": []string{"a"}}) {
		t.Errorf("should have set a param: %#v", r)
	}
	r.VersionedParams(&v1.PodLogOptions{Follow: true, Container: "bar"}, scheme.ParameterCodec)

	if !reflect.DeepEqual(r.params, url.Values{
		"foo":       []string{"a"},
		"container": []string{"bar"},
		"follow":    []string{"true"},
	}) {
		t.Errorf("should have set a param: %#v", r)
	}
}

// TestRequestVersionedParamsFromListOptions 验证 ListOptions 只输出非零字段，
// 且多次调用会累积同名参数（如 resourceVersion）。
func TestRequestVersionedParamsFromListOptions(t *testing.T) {
	r := &Request{c: &RESTClient{}, contentConfig: ClientContentConfig{GroupVersion: v1.SchemeGroupVersion}}
	r.VersionedParams(&metav1.ListOptions{ResourceVersion: "1"}, scheme.ParameterCodec)
	if !reflect.DeepEqual(r.params, url.Values{
		"resourceVersion": []string{"1"},
	}) {
		t.Errorf("should have set a param: %#v", r)
	}

	var timeout int64 = 10
	r.VersionedParams(&metav1.ListOptions{ResourceVersion: "2", TimeoutSeconds: &timeout}, scheme.ParameterCodec)
	if !reflect.DeepEqual(r.params, url.Values{
		"resourceVersion": []string{"1", "2"},
		"timeoutSeconds":  []string{"10"},
	}) {
		t.Errorf("should have set a param: %#v %v", r.params, r.err)
	}
}

// TestRequestVersionedParamsWithInvalidScheme 验证 scheme 未注册该类型时只记录错误，
// 不会 panic。
func TestRequestVersionedParamsWithInvalidScheme(t *testing.T) {
	parameterCodec := runtime.NewParameterCodec(runtime.NewScheme())
	r := &Request{c: &RESTClient{}, contentConfig: ClientContentConfig{GroupVersion: v1.SchemeGroupVersion}}
	r.VersionedParams(&v1.PodExecOptions{Stdin: false, Stdout: true},
		parameterCodec)

	if r.Error() == nil {
		t.Errorf("should have recorded an error: %#v", r.params)
	}
}

// TestRequestError 验证 Error() 返回的是同一个错误引用，而非重新包装。
func TestRequestError(t *testing.T) {
	// Invalid body, see TestRequestBody()
	r := (&Request{}).Body([]string{"test"})

	if r.Error() != r.err {
		t.Errorf("getter should be identical to reference: %#v %#v", r.Error(), r.err)
	}
}

// TestRequestURI 验证 RequestURI 会设置路径前缀并解析其中的查询参数。
func TestRequestURI(t *testing.T) {
	r := (&Request{c: &RESTClient{base: &url.URL{Path: "/"}}}).Param("foo", "a").Param("bar", "b")
	r.Prefix("other")
	r.RequestURI("/test?foo=b&a=b&c=1&c=2")
	if r.pathPrefix != "/test" {
		t.Errorf("path is wrong: %#v", r)
	}
	if !reflect.DeepEqual(r.params, url.Values{"a": []string{"b"}, "foo": []string{"b"}, "c": []string{"1", "2"}}) {
		t.Errorf("should have set a param, got: %#v", r.params)
	}
}

// TestRequestURIContext 验证 RequestURI 相对于客户端 base 解析，且支持 "../" 上溯。
func TestRequestURIContext(t *testing.T) {
	r := (&Request{c: &RESTClient{base: &url.URL{Path: "/context"}}}).Param("foo", "a").Param("bar", "b")
	r.Prefix("other")
	r.RequestURI("/test?foo=b&a=b&c=1&c=2")
	if r.pathPrefix != "/context/test" {
		t.Errorf("path is wrong: %#v", r)
	}
	if !reflect.DeepEqual(r.params, url.Values{"a": []string{"b"}, "foo": []string{"b"}, "c": []string{"1", "2"}}) {
		t.Errorf("should have set a param, got: %#v", r.params)
	}

	r = (&Request{c: &RESTClient{base: &url.URL{Path: "/context"}}}).Param("foo", "a").Param("bar", "b")
	r.Prefix("other")
	r.RequestURI("../test?foo=b&a=b&c=1&c=2")
	if r.pathPrefix != "/test" {
		t.Errorf("path is wrong: %#v", r)
	}
	if !reflect.DeepEqual(r.params, url.Values{"a": []string{"b"}, "foo": []string{"b"}, "c": []string{"1", "2"}}) {
		t.Errorf("should have set a param, got: %#v", r.params)
	}
}

// NotAnAPIObject 实现了部分 TypeMeta 方法但未注册进 scheme，用于测试编码失败路径。
type NotAnAPIObject struct{}

// GroupVersionKind 返回 nil：该类型没有可用的类型信息。
func (obj NotAnAPIObject) GroupVersionKind() *schema.GroupVersionKind { return nil }

// SetGroupVersionKind 是空实现。
func (obj NotAnAPIObject) SetGroupVersionKind(gvk *schema.GroupVersionKind) {}

// defaultContentConfig 返回测试通用的 JSON 内容配置（含协商器）。
func defaultContentConfig() ClientContentConfig {
	gvCopy := v1.SchemeGroupVersion
	return ClientContentConfig{
		ContentType:  "application/json",
		GroupVersion: gvCopy,
		Negotiator:   runtime.NewClientNegotiator(scheme.Codecs.WithoutConversion(), gvCopy),
	}
}

// TestRequestBody 验证 Body 对各类输入的编码行为。
//
// 中文说明：支持 []byte、文件路径、io.Reader 与已注册的 API 对象；未知类型、无法读取
// 的文件、未注册对象都应只设置 err 并保持 body 为 nil。类型化的 nil 对象被视为
// “无 body”，用于 DELETE 等可选 body 的请求。
func TestRequestBody(t *testing.T) {
	// test unknown type
	r := (&Request{}).Body([]string{"test"})
	if r.err == nil || r.body != nil {
		t.Errorf("should have set err and left body nil: %#v", r)
	}

	// test error set when failing to read file
	f, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("unable to create temp file")
	}
	defer f.Close()
	os.Remove(f.Name())
	r = (&Request{}).Body(f.Name())
	if r.err == nil || r.body != nil {
		t.Errorf("should have set err and left body nil: %#v", r)
	}

	// test unencodable api object
	r = (&Request{c: &RESTClient{}, contentConfig: defaultContentConfig()}).Body(&NotAnAPIObject{})
	if r.err == nil || r.body != nil {
		t.Errorf("should have set err and left body nil: %#v", r)
	}
}

// TestResultIntoWithErrReturnsErr 验证 Result 已有错误时 Into 直接返回该错误。
func TestResultIntoWithErrReturnsErr(t *testing.T) {
	res := Result{err: errors.New("test")}
	if err := res.Into(&v1.Pod{}); err != res.err {
		t.Errorf("should have returned exact error from result")
	}
}

// TestResultIntoWithNoBodyReturnsErr 验证 0 长度 body 无法解码时返回明确错误，
// 而不是静默产出零值对象。
func TestResultIntoWithNoBodyReturnsErr(t *testing.T) {
	res := Result{
		body:    []byte{},
		decoder: scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),
	}
	if err := res.Into(&v1.Pod{}); err == nil || !strings.Contains(err.Error(), "0-length") {
		t.Errorf("should have complained about 0 length body")
	}
}

// TestURLTemplate 验证真实请求 URL 与“模板 URL”的生成规则。
//
// 中文说明：模板 URL 把 namespace/name/param 值替换为 {placeholder}，用于日志脱敏与
// 指标聚合；它必须与真实 URL 的路径结构一致，且生成模板时不得改动原 Request。
// 用例覆盖核心组、命名组、有无名称、子资源、以及前缀不匹配时退化为 {prefix}。
func TestURLTemplate(t *testing.T) {
	uri, _ := url.Parse("http://localhost/some/base/url/path")
	uriSingleSlash, _ := url.Parse("http://localhost/")
	testCases := []struct {
		Request          *Request
		ExpectedFullURL  string
		ExpectedFinalURL string
	}{
		{
			// non dynamic client
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("POST").
				Prefix("api", "v1").Resource("r1").Namespace("ns").Name("nm").Param("p0", "v0"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/api/v1/namespaces/ns/r1/nm?p0=v0",
			ExpectedFinalURL: "http://localhost/some/base/url/path/api/v1/namespaces/%7Bnamespace%7D/r1/%7Bname%7D?p0=%7Bvalue%7D",
		},
		{
			// non dynamic client with wrong api group
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("POST").
				Prefix("pre1", "v1").Resource("r1").Namespace("ns").Name("nm").Param("p0", "v0"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/pre1/v1/namespaces/ns/r1/nm?p0=v0",
			ExpectedFinalURL: "http://localhost/%7Bprefix%7D",
		},
		{
			// dynamic client with core group + namespace + resourceResource (with name)
			// /api/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/api/v1/namespaces/ns/r1/name1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/api/v1/namespaces/ns/r1/name1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/api/v1/namespaces/%7Bnamespace%7D/r1/%7Bname%7D",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/g1/v1/namespaces/ns/r1/name1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/g1/v1/namespaces/ns/r1/name1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/g1/v1/namespaces/%7Bnamespace%7D/r1/%7Bname%7D",
		},
		{
			// dynamic client with core group + namespace + resourceResource (with NO name)
			// /api/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/api/v1/namespaces/ns/r1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/api/v1/namespaces/ns/r1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/api/v1/namespaces/%7Bnamespace%7D/r1",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with NO name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/g1/v1/namespaces/ns/r1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/g1/v1/namespaces/ns/r1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/g1/v1/namespaces/%7Bnamespace%7D/r1",
		},
		{
			// dynamic client with core group + resourceResource (with name)
			// /api/$RESOURCEVERSION/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/api/v1/r1/name1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/api/v1/r1/name1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/api/v1/r1/%7Bname%7D",
		},
		{
			// dynamic client with named group + resourceResource (with name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/g1/v1/r1/name1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/g1/v1/r1/name1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/g1/v1/r1/%7Bname%7D",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with name) + subresource
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME/$SUBRESOURCE
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/namespaces/namespaces/finalize"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/namespaces/namespaces/finalize",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bnamespace%7D/namespaces/%7Bname%7D/finalize",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/namespaces/namespaces"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/namespaces/namespaces",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bnamespace%7D/namespaces/%7Bname%7D",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with NO name) + subresource
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%SUBRESOURCE
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/namespaces/finalize"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/namespaces/finalize",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bnamespace%7D/namespaces/finalize",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with NO name) + subresource
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%SUBRESOURCE
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/namespaces/status"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/namespaces/status",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bnamespace%7D/namespaces/status",
		},
		{
			// dynamic client with named group + namespace + resourceResource (with no name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/namespaces"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/namespaces",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bnamespace%7D/namespaces",
		},
		{
			// dynamic client with named group + resourceResource (with name) + subresource
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/finalize"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/finalize",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bname%7D/finalize",
		},
		{
			// dynamic client with named group + resourceResource (with name) + subresource
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces/status"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces/status",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bname%7D/status",
		},
		{
			// dynamic client with named group + resourceResource (with name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces/namespaces"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/namespaces",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces/%7Bname%7D",
		},
		{
			// dynamic client with named group + resourceResource (with no name)
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/apis/namespaces/namespaces/namespaces"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces",
			ExpectedFinalURL: "http://localhost/some/base/url/path/apis/namespaces/namespaces/namespaces",
		},
		{
			// dynamic client with wrong api group + namespace + resourceResource (with name) + subresource
			// /apis/$NAMEDGROUPNAME/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME/$SUBRESOURCE
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/pre1/namespaces/namespaces/namespaces/namespaces/namespaces/namespaces/finalize"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/pre1/namespaces/namespaces/namespaces/namespaces/namespaces/namespaces/finalize",
			ExpectedFinalURL: "http://localhost/%7Bprefix%7D",
		},
		{
			// dynamic client with core group + namespace + resourceResource (with name) where baseURL is a single /
			// /api/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uriSingleSlash, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/api/v1/namespaces/ns/r2/name1"),
			ExpectedFullURL:  "http://localhost/api/v1/namespaces/ns/r2/name1",
			ExpectedFinalURL: "http://localhost/api/v1/namespaces/%7Bnamespace%7D/r2/%7Bname%7D",
		},
		{
			// dynamic client with core group + namespace + resourceResource (with name) where baseURL is 'some/base/url/path'
			// /api/$RESOURCEVERSION/namespaces/$NAMESPACE/$RESOURCE/%NAME
			Request: NewRequestWithClient(uri, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/api/v1/namespaces/ns/r3/name1"),
			ExpectedFullURL:  "http://localhost/some/base/url/path/api/v1/namespaces/ns/r3/name1",
			ExpectedFinalURL: "http://localhost/some/base/url/path/api/v1/namespaces/%7Bnamespace%7D/r3/%7Bname%7D",
		},
		{
			// dynamic client where baseURL is a single /
			// /
			Request: NewRequestWithClient(uriSingleSlash, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/"),
			ExpectedFullURL:  "http://localhost/",
			ExpectedFinalURL: "http://localhost/",
		},
		{
			// dynamic client where baseURL is a single /
			// /version
			Request: NewRequestWithClient(uriSingleSlash, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("DELETE").
				Prefix("/version"),
			ExpectedFullURL:  "http://localhost/version",
			ExpectedFinalURL: "http://localhost/version",
		},
	}
	for i, testCase := range testCases {
		r := testCase.Request
		full := r.URL()
		if full.String() != testCase.ExpectedFullURL {
			t.Errorf("%d: unexpected initial URL: %s %s", i, full, testCase.ExpectedFullURL)
		}
		actualURL := r.finalURLTemplate()
		actual := actualURL.String()
		if actual != testCase.ExpectedFinalURL {
			t.Errorf("%d: unexpected URL template: %s %s", i, actual, testCase.ExpectedFinalURL)
		}
		if r.URL().String() != full.String() {
			t.Errorf("%d, creating URL template changed request: %s -> %s", i, full.String(), r.URL().String())
		}
	}
}

// TestTransformResponse 验证 HTTP 响应到 Result 的转换。
//
// 中文说明：2xx 返回原始 body，201 额外标记 created；4xx/5xx 应转为实现 APIStatus
// 的错误，且错误中的 Code 与 HTTP 状态码一致。对 Unauthorized，正文为 JSON 时尝试
// 解码 Status，正文为其它类型时给出可读的认证提示。
func TestTransformResponse(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	invalid := []byte("aaaaa")
	uri, _ := url.Parse("http://localhost")
	testCases := []struct {
		Response *http.Response
		Data     []byte
		Created  bool
		Error    bool
		ErrFn    func(err error) bool
	}{
		{Response: &http.Response{StatusCode: http.StatusOK}, Data: []byte{}},
		{Response: &http.Response{StatusCode: http.StatusCreated}, Data: []byte{}, Created: true},
		{Response: &http.Response{StatusCode: 199}, Error: true},
		{Response: &http.Response{StatusCode: http.StatusInternalServerError}, Error: true},
		{Response: &http.Response{StatusCode: http.StatusUnprocessableEntity}, Error: true},
		{Response: &http.Response{StatusCode: http.StatusConflict}, Error: true},
		{Response: &http.Response{StatusCode: http.StatusNotFound}, Error: true},
		{Response: &http.Response{StatusCode: http.StatusUnauthorized}, Error: true},
		{
			Response: &http.Response{
				StatusCode: http.StatusUnauthorized,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Error: true,
			ErrFn: func(err error) bool {
				return err.Error() != "aaaaa" && apierrors.IsUnauthorized(err)
			},
		},
		{
			Response: &http.Response{
				StatusCode: http.StatusUnauthorized,
				Header:     http.Header{"Content-Type": []string{"text/any"}},
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Error: true,
			ErrFn: func(err error) bool {
				return strings.Contains(err.Error(), "server has asked for the client to provide") && apierrors.IsUnauthorized(err)
			},
		},
		{Response: &http.Response{StatusCode: http.StatusForbidden}, Error: true},
		{Response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(invalid))}, Data: invalid},
		{Response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(invalid))}, Data: invalid},
	}
	for i, test := range testCases {
		r := NewRequestWithClient(uri, "", defaultContentConfig(), nil)
		if test.Response.Body == nil {
			test.Response.Body = io.NopCloser(bytes.NewReader([]byte{}))
		}
		result := r.transformResponse(ctx, test.Response, &http.Request{})
		response, created, err := result.body, result.statusCode == http.StatusCreated, result.err
		hasErr := err != nil
		if hasErr != test.Error {
			t.Errorf("%d: unexpected error: %t %v", i, test.Error, err)
		} else if hasErr && test.Response.StatusCode > 399 {
			status, ok := err.(apierrors.APIStatus)
			if !ok {
				t.Errorf("%d: response should have been transformable into APIStatus: %v", i, err)
				continue
			}
			if int(status.Status().Code) != test.Response.StatusCode {
				t.Errorf("%d: status code did not match response: %#v", i, status.Status())
			}
		}
		if test.ErrFn != nil && !test.ErrFn(err) {
			t.Errorf("%d: error function did not match: %v", i, err)
		}
		if !(test.Data == nil && response == nil) && !apiequality.Semantic.DeepDerivative(test.Data, response) {
			t.Errorf("%d: unexpected response: %#v %#v", i, test.Data, response)
		}
		if test.Created != created {
			t.Errorf("%d: expected created %t, got %t", i, test.Created, created)
		}
	}
}

// renegotiator 记录 Decoder 调用参数，用于断言响应内容类型协商的行为。
type renegotiator struct {
	called      bool
	contentType string
	params      map[string]string
	decoder     runtime.Decoder
	err         error
}

// Decoder 记录协商的 content type，并按预设返回 decoder 或错误。
func (r *renegotiator) Decoder(contentType string, params map[string]string) (runtime.Decoder, error) {
	r.called = true
	r.contentType = contentType
	r.params = params
	return r.decoder, r.err
}

// Encoder 未实现，仅满足接口。
func (r *renegotiator) Encoder(contentType string, params map[string]string) (runtime.Encoder, error) {
	return nil, fmt.Errorf("UNIMPLEMENTED")
}

// StreamDecoder 未实现，仅满足接口。
func (r *renegotiator) StreamDecoder(contentType string, params map[string]string) (runtime.Decoder, runtime.Serializer, runtime.Framer, error) {
	return nil, nil, nil, fmt.Errorf("UNIMPLEMENTED")
}

// TestTransformResponseNegotiate 验证响应解码器按响应 Content-Type 协商。
//
// 中文说明：响应未带 Content-Type 时回退到请求的 ContentType；未知类型交给协商器，
// 协商失败仍应保留 HTTP 状态码对应的 APIStatus。即使解码失败，错误也不应是原始
// 响应正文。
func TestTransformResponseNegotiate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	invalid := []byte("aaaaa")
	uri, _ := url.Parse("http://localhost")
	testCases := []struct {
		Response *http.Response
		Data     []byte
		Created  bool
		Error    bool
		ErrFn    func(err error) bool

		ContentType       string
		Called            bool
		ExpectContentType string
		Decoder           runtime.Decoder
		NegotiateErr      error
	}{
		{
			ContentType: "application/json",
			Response: &http.Response{
				StatusCode: http.StatusUnauthorized,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Called:            true,
			ExpectContentType: "application/json",
			Error:             true,
			ErrFn: func(err error) bool {
				return err.Error() != "aaaaa" && apierrors.IsUnauthorized(err)
			},
		},
		{
			ContentType: "application/json",
			Response: &http.Response{
				StatusCode: http.StatusUnauthorized,
				Header:     http.Header{"Content-Type": []string{"application/protobuf"}},
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Decoder: scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),

			Called:            true,
			ExpectContentType: "application/protobuf",

			Error: true,
			ErrFn: func(err error) bool {
				return err.Error() != "aaaaa" && apierrors.IsUnauthorized(err)
			},
		},
		{
			ContentType: "application/json",
			Response: &http.Response{
				StatusCode: http.StatusInternalServerError,
				Header:     http.Header{"Content-Type": []string{"application/,others"}},
			},
			Decoder: scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),

			Error: true,
			ErrFn: func(err error) bool {
				return err.Error() == "Internal error occurred: mime: expected token after slash" && err.(apierrors.APIStatus).Status().Code == 500
			},
		},
		{
			// negotiate when no content type specified
			Response: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/any"}},
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Decoder:           scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),
			Called:            true,
			ExpectContentType: "text/any",
		},
		{
			// negotiate when no response content type specified
			ContentType: "text/any",
			Response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Decoder:           scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),
			Called:            true,
			ExpectContentType: "text/any",
		},
		{
			// unrecognized content type is not handled
			ContentType: "application/json",
			Response: &http.Response{
				StatusCode: http.StatusNotFound,
				Header:     http.Header{"Content-Type": []string{"application/unrecognized"}},
				Body:       io.NopCloser(bytes.NewReader(invalid)),
			},
			Decoder: scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion),

			NegotiateErr:      fmt.Errorf("aaaa"),
			Called:            true,
			ExpectContentType: "application/unrecognized",

			Error: true,
			ErrFn: func(err error) bool {
				return err.Error() != "aaaaa" && apierrors.IsNotFound(err)
			},
		},
	}
	for i, test := range testCases {
		contentConfig := defaultContentConfig()
		contentConfig.ContentType = test.ContentType
		negotiator := &renegotiator{
			decoder: test.Decoder,
			err:     test.NegotiateErr,
		}
		contentConfig.Negotiator = negotiator
		r := NewRequestWithClient(uri, "", contentConfig, nil)
		if test.Response.Body == nil {
			test.Response.Body = io.NopCloser(bytes.NewReader([]byte{}))
		}
		result := r.transformResponse(ctx, test.Response, &http.Request{})
		_, err := result.body, result.err
		hasErr := err != nil
		if hasErr != test.Error {
			t.Errorf("%d: unexpected error: %t %v", i, test.Error, err)
			continue
		} else if hasErr && test.Response.StatusCode > 399 {
			status, ok := err.(apierrors.APIStatus)
			if !ok {
				t.Errorf("%d: response should have been transformable into APIStatus: %v", i, err)
				continue
			}
			if int(status.Status().Code) != test.Response.StatusCode {
				t.Errorf("%d: status code did not match response: %#v", i, status.Status())
			}
		}
		if test.ErrFn != nil && !test.ErrFn(err) {
			t.Errorf("%d: error function did not match: %v", i, err)
		}
		if negotiator.called != test.Called {
			t.Errorf("%d: negotiator called %t != %t", i, negotiator.called, test.Called)
		}
		if !test.Called {
			continue
		}
		if negotiator.contentType != test.ExpectContentType {
			t.Errorf("%d: unexpected content type: %s", i, negotiator.contentType)
		}
	}
}

// TestTransformUnstructuredError 验证非结构化错误响应的状态推断与 Result 各方法的一致性。
//
// 中文说明：响应体若是合法 Status，则其 code 覆盖按 HTTP 状态推断的结果；Success
// 状态、空对象、非法 JSON 都回退到 HTTP 状态。POST+409 视为 AlreadyExists，其它
// 方法为 Conflict。Raw() 必须保留未转换的原始错误，Error/Get/Into 则使用转换后的。
func TestTransformUnstructuredError(t *testing.T) {
	testCases := []struct {
		Req *http.Request
		Res *http.Response

		Resource string
		Name     string

		ErrFn       func(error) bool
		Transformed error
	}{
		{
			Resource: "foo",
			Name:     "bar",
			Req: &http.Request{
				Method: "POST",
			},
			Res: &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			},
			ErrFn: apierrors.IsAlreadyExists,
		},
		{
			Resource: "foo",
			Name:     "bar",
			Req: &http.Request{
				Method: "PUT",
			},
			Res: &http.Response{
				StatusCode: http.StatusConflict,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			},
			ErrFn: apierrors.IsConflict,
		},
		{
			Resource: "foo",
			Name:     "bar",
			Req:      &http.Request{},
			Res: &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			},
			ErrFn: apierrors.IsNotFound,
		},
		{
			Req: &http.Request{},
			Res: &http.Response{
				StatusCode: http.StatusBadRequest,
				Body:       io.NopCloser(bytes.NewReader(nil)),
			},
			ErrFn: apierrors.IsBadRequest,
		},
		{
			// status in response overrides transformed result
			Req:   &http.Request{},
			Res:   &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewReader([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`)))},
			ErrFn: apierrors.IsBadRequest,
			Transformed: &apierrors.StatusError{
				ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: http.StatusNotFound},
			},
		},
		{
			// successful status is ignored
			Req:   &http.Request{},
			Res:   &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewReader([]byte(`{"kind":"Status","apiVersion":"v1","status":"Success","code":404}`)))},
			ErrFn: apierrors.IsBadRequest,
		},
		{
			// empty object does not change result
			Req:   &http.Request{},
			Res:   &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewReader([]byte(`{}`)))},
			ErrFn: apierrors.IsBadRequest,
		},
		{
			// we default apiVersion for backwards compatibility with old clients
			// TODO: potentially remove in 1.7
			Req:   &http.Request{},
			Res:   &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewReader([]byte(`{"kind":"Status","status":"Failure","code":404}`)))},
			ErrFn: apierrors.IsBadRequest,
			Transformed: &apierrors.StatusError{
				ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: http.StatusNotFound},
			},
		},
		{
			// we do not default kind
			Req:   &http.Request{},
			Res:   &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(bytes.NewReader([]byte(`{"status":"Failure","code":404}`)))},
			ErrFn: apierrors.IsBadRequest,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			r := &Request{
				contentConfig: defaultContentConfig(),
				c:             &RESTClient{},
				resourceName:  testCase.Name,
				resource:      testCase.Resource,
			}
			result := r.transformResponse(ctx, testCase.Res, testCase.Req)
			err := result.err
			if !testCase.ErrFn(err) {
				t.Fatalf("unexpected error: %v", err)
			}
			if !apierrors.IsUnexpectedServerError(err) {
				t.Errorf("unexpected error type: %v", err)
			}
			if len(testCase.Name) != 0 && !strings.Contains(err.Error(), testCase.Name) {
				t.Errorf("unexpected error string: %s", err)
			}
			if len(testCase.Resource) != 0 && !strings.Contains(err.Error(), testCase.Resource) {
				t.Errorf("unexpected error string: %s", err)
			}

			// verify Error() properly transforms the error
			transformed := result.Error()
			expect := testCase.Transformed
			if expect == nil {
				expect = err
			}
			if !reflect.DeepEqual(expect, transformed) {
				t.Errorf("unexpected Error(): %s", cmp.Diff(expect, transformed))
			}

			// verify result.Get properly transforms the error
			if _, err := result.Get(); !reflect.DeepEqual(expect, err) {
				t.Errorf("unexpected error on Get(): %s", cmp.Diff(expect, err))
			}

			// verify result.Into properly handles the error
			if err := result.Into(&v1.Pod{}); !reflect.DeepEqual(expect, err) {
				t.Errorf("unexpected error on Into(): %s", cmp.Diff(expect, err))
			}

			// verify result.Raw leaves the error in the untransformed state
			if _, err := result.Raw(); !reflect.DeepEqual(result.err, err) {
				t.Errorf("unexpected error on Raw(): %s", cmp.Diff(expect, err))
			}
		})
	}
}

// TestRequestWatch 验证 Watch 的错误分类与重试次数。
//
// 中文说明：请求已带错误时零次调用；不可重试错误只尝试一次；带 Retry-After 的响应
// 会重试到 maxRetries；EOF、连接中断等被视为正常结束，返回空 watcher 而非错误；
// 403/401 转为对应 StatusError。
func TestRequestWatch(t *testing.T) {
	testCases := []struct {
		name             string
		Request          *Request
		maxRetries       int
		serverReturns    []responseErr
		Expect           []watch.Event
		attemptsExpected int
		Err              bool
		ErrFn            func(error) bool
		Empty            bool
	}{
		{
			name:             "Request has error",
			Request:          &Request{err: errors.New("bail")},
			attemptsExpected: 0,
			Err:              true,
		},
		{
			name:    "Client is nil, should use http.DefaultClient",
			Request: &Request{c: &RESTClient{base: &url.URL{}}, pathPrefix: "%"},
			Err:     true,
		},
		{
			name: "error is not retryable",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: nil, err: errors.New("err")},
			},
			attemptsExpected: 1,
			Err:              true,
		},
		{
			name: "server returns forbidden with json content",
			Request: &Request{
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: &http.Response{
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					StatusCode: http.StatusForbidden,
					Body: io.NopCloser(bytes.NewReader([]byte(runtime.EncodeOrDie(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &metav1.Status{
						Status:  metav1.StatusFailure,
						Message: "secrets is forbidden",
						Reason:  metav1.StatusReasonForbidden,
						Code:    http.StatusForbidden,
					})))),
				}, err: nil},
			},
			attemptsExpected: 1,
			Err:              true,
			ErrFn: func(err error) bool {
				if err.Error() != "secrets is forbidden" {
					return false
				}
				return apierrors.IsForbidden(err)
			},
		},
		{
			name: "server returns forbidden without content",
			Request: &Request{
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: &http.Response{
					StatusCode: http.StatusForbidden,
					Body:       io.NopCloser(bytes.NewReader([]byte{})),
				}, err: nil},
			},
			attemptsExpected: 1,
			Err:              true,
			ErrFn: func(err error) bool {
				return apierrors.IsForbidden(err)
			},
		},
		{
			name: "server returns unauthorized",
			Request: &Request{
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: &http.Response{
					StatusCode: http.StatusUnauthorized,
					Body:       io.NopCloser(bytes.NewReader([]byte{})),
				}, err: nil},
			},
			attemptsExpected: 1,
			Err:              true,
			ErrFn: func(err error) bool {
				return apierrors.IsUnauthorized(err)
			},
		},
		{
			name: "server returns unauthorized",
			Request: &Request{
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: &http.Response{
					StatusCode: http.StatusUnauthorized,
					Body: io.NopCloser(bytes.NewReader([]byte(runtime.EncodeOrDie(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &metav1.Status{
						Status: metav1.StatusFailure,
						Reason: metav1.StatusReasonUnauthorized,
					})))),
				}, err: nil},
			},
			attemptsExpected: 1,
			Err:              true,
			ErrFn: func(err error) bool {
				return apierrors.IsUnauthorized(err)
			},
		},
		{
			name: "server returns EOF error",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: nil, err: io.EOF},
			},
			attemptsExpected: 1,
			Empty:            true,
		},
		{
			name: "server returns can't write HTTP request on broken connection error",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: nil, err: errors.New("http: can't write HTTP request on broken connection")},
			},
			attemptsExpected: 1,
			Empty:            true,
		},
		{
			name: "server returns connection reset by peer",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: nil, err: errors.New("foo: connection reset by peer")},
			},
			attemptsExpected: 1,
			Empty:            true,
		},
		{
			name: "max retries 2, server always returns EOF error",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			maxRetries:       2,
			attemptsExpected: 3,
			serverReturns: []responseErr{
				{response: nil, err: io.EOF},
				{response: nil, err: io.EOF},
				{response: nil, err: io.EOF},
			},
			Empty: true,
		},
		{
			name: "max retries 2, server always returns a response with Retry-After header",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			maxRetries:       2,
			attemptsExpected: 3,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
			},
			Err: true,
			ErrFn: func(err error) bool {
				return apierrors.IsInternalError(err)
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var attemptsGot int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				defer func() {
					attemptsGot++
				}()

				if attemptsGot >= len(testCase.serverReturns) {
					t.Fatalf("Wrong test setup, the server does not know what to return")
				}
				re := testCase.serverReturns[attemptsGot]
				return re.response, re.err
			})
			if c := testCase.Request.c; c != nil && len(testCase.serverReturns) > 0 {
				c.Client = client
			}
			testCase.Request.backoff = &noSleepBackOff{}
			testCase.Request.maxRetries = testCase.maxRetries
			testCase.Request.retryFn = defaultRequestRetryFn

			watch, err := testCase.Request.Watch(context.Background())

			if watch == nil && err == nil {
				t.Fatal("Both watch.Interface and err returned by Watch are nil")
			}
			if testCase.attemptsExpected != attemptsGot {
				t.Errorf("Expected RoundTrip to be invoked %d times, but got: %d", testCase.attemptsExpected, attemptsGot)
			}
			hasErr := err != nil
			if hasErr != testCase.Err {
				t.Fatalf("expected %t, got %t: %v", testCase.Err, hasErr, err)
			}
			if testCase.ErrFn != nil && !testCase.ErrFn(err) {
				t.Errorf("error not valid: %v", err)
			}
			if hasErr && watch != nil {
				t.Fatalf("watch should be nil when error is returned")
			}
			if hasErr {
				return
			}
			defer watch.Stop()
			if testCase.Empty {
				evt, ok := <-watch.ResultChan()
				if ok {
					t.Errorf("expected the watch to be empty: %#v", evt)
				}
			}
			if testCase.Expect != nil {
				for i, evt := range testCase.Expect {
					out, ok := <-watch.ResultChan()
					if !ok {
						t.Fatalf("Watch closed early, %d/%d read", i, len(testCase.Expect))
					}
					if !reflect.DeepEqual(evt, out) {
						t.Fatalf("Event %d does not match: %s", i, cmp.Diff(evt, out))
					}
				}
			}
		})
	}
}

// TestRequestStream 验证 Stream 的握手期错误与重试。
//
// 中文说明：与 Do 类似，Retry-After 可触发重试；但请求体不可重放（如 io.EOF 的
// readSeeker）时只尝试一次。网络层错误在 Stream 路径通常不重试，最终错误保持可
// 通过 errors 链还原。
func TestRequestStream(t *testing.T) {
	testCases := []struct {
		name             string
		Request          *Request
		maxRetries       int
		serverReturns    []responseErr
		attemptsExpected int
		Err              bool
		ErrFn            func(error) bool
	}{
		{
			name:             "request has error",
			Request:          &Request{err: errors.New("bail")},
			attemptsExpected: 0,
			Err:              true,
		},
		{
			name:    "Client is nil, should use http.DefaultClient",
			Request: &Request{c: &RESTClient{base: &url.URL{}}, pathPrefix: "%"},
			Err:     true,
		},
		{
			name: "server returns an error",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: nil, err: errors.New("err")},
			},
			attemptsExpected: 1,
			Err:              true,
		},
		{
			Request: &Request{
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: &http.Response{
					StatusCode: http.StatusUnauthorized,
					Body: io.NopCloser(bytes.NewReader([]byte(runtime.EncodeOrDie(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &metav1.Status{
						Status: metav1.StatusFailure,
						Reason: metav1.StatusReasonUnauthorized,
					})))),
				}, err: nil},
			},
			attemptsExpected: 1,
			Err:              true,
		},
		{
			Request: &Request{
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			serverReturns: []responseErr{
				{response: &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewReader([]byte(`{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"a container name must be specified for pod kube-dns-v20-mz5cv, choose one of: [kubedns dnsmasq healthz]","reason":"BadRequest","code":400}`))),
				}, err: nil},
			},
			attemptsExpected: 1,
			Err:              true,
			ErrFn: func(err error) bool {
				if err.Error() == "a container name must be specified for pod kube-dns-v20-mz5cv, choose one of: [kubedns dnsmasq healthz]" {
					return true
				}
				return false
			},
		},
		{
			name: "max retries 1, server returns a retry-after response, non-bytes request, no retry",
			Request: &Request{
				body: &readSeeker{err: io.EOF},
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			maxRetries:       1,
			attemptsExpected: 1,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
			},
			Err: true,
		},
		{
			name: "max retries 2, server always returns a response with Retry-After header",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			maxRetries:       2,
			attemptsExpected: 3,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
			},
			Err: true,
			ErrFn: func(err error) bool {
				return apierrors.IsInternalError(err)
			},
		},
		{
			name: "server returns EOF after attempt 1, retry aborted",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			maxRetries:       2,
			attemptsExpected: 2,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: io.EOF},
			},
			Err: true,
			ErrFn: func(err error) bool {
				return unWrap(err) == io.EOF
			},
		},
		{
			name: "max retries 2, server returns success on the final attempt",
			Request: &Request{
				c: &RESTClient{
					base: &url.URL{},
				},
			},
			maxRetries:       2,
			attemptsExpected: 3,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewReader([]byte{})),
				}, err: nil},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			var attemptsGot int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				defer func() {
					attemptsGot++
				}()

				if attemptsGot >= len(testCase.serverReturns) {
					t.Fatalf("Wrong test setup, the server does not know what to return")
				}
				re := testCase.serverReturns[attemptsGot]
				return re.response, re.err
			})
			if c := testCase.Request.c; c != nil && len(testCase.serverReturns) > 0 {
				c.Client = client
			}
			testCase.Request.backoff = &noSleepBackOff{}
			testCase.Request.maxRetries = testCase.maxRetries
			testCase.Request.retryFn = defaultRequestRetryFn

			body, err := testCase.Request.Stream(context.Background())

			if body == nil && err == nil {
				t.Fatal("Both body and err returned by Stream are nil")
			}
			if testCase.attemptsExpected != attemptsGot {
				t.Errorf("Expected RoundTrip to be invoked %d times, but got: %d", testCase.attemptsExpected, attemptsGot)
			}

			hasErr := err != nil
			if hasErr != testCase.Err {
				t.Errorf("expected %t, got %t: %v", testCase.Err, hasErr, err)
			}
			if hasErr && body != nil {
				t.Error("body should be nil when error is returned")
			}

			if hasErr {
				if testCase.ErrFn != nil && !testCase.ErrFn(err) {
					t.Errorf("unexpected error: %#v", err)
				}
			}
		})
	}
}

// TestRequestDo 验证 Do 的基础错误路径：已带错误、非法 URL、传输层错误都应返回错误
// 且不返回 body。
func TestRequestDo(t *testing.T) {
	testCases := []struct {
		Request *Request
		Err     bool
	}{
		{
			Request: &Request{c: &RESTClient{}, err: errors.New("bail")},
			Err:     true,
		},
		{
			Request: &Request{c: &RESTClient{base: &url.URL{}}, pathPrefix: "%"},
			Err:     true,
		},
		{
			Request: &Request{
				c: &RESTClient{
					Client: clientForFunc(func(req *http.Request) (*http.Response, error) {
						return nil, errors.New("err")
					}),
					base: &url.URL{},
				},
			},
			Err: true,
		},
	}
	for i, testCase := range testCases {
		testCase.Request.backoff = &NoBackoff{}
		testCase.Request.retryFn = defaultRequestRetryFn
		body, err := testCase.Request.Do(context.Background()).Raw()
		hasErr := err != nil
		if hasErr != testCase.Err {
			t.Errorf("%d: expected %t, got %t: %v", i, testCase.Err, hasErr, err)
		}
		if hasErr && body != nil {
			t.Errorf("%d: body should be nil when error is returned", i)
		}
	}
}

// TestDoRequestNewWay 验证字节切片 body 的完整请求构造：
// prefix/suffix、timeout 查询参数与响应解码。
func TestDoRequestNewWay(t *testing.T) {
	reqBody := "request body"
	expectedObj := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{
		Protocol:   "TCP",
		Port:       12345,
		TargetPort: intstr.FromInt32(12345),
	}}}}
	expectedBody, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), expectedObj)
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:   200,
		ResponseBody: string(expectedBody),
		T:            t,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	c := testRESTClient(t, testServer)
	obj, err := c.Verb("POST").
		Prefix("foo", "bar").
		Suffix("baz").
		Timeout(time.Second).
		Body([]byte(reqBody)).
		Do(context.Background()).Get()
	if err != nil {
		t.Errorf("Unexpected error: %v %#v", err, err)
		return
	}
	if obj == nil {
		t.Error("nil obj")
	} else if !apiequality.Semantic.DeepDerivative(expectedObj, obj) {
		t.Errorf("Expected: %#v, got %#v", expectedObj, obj)
	}
	requestURL := defaultResourcePathWithPrefix("foo/bar", "", "", "baz")
	requestURL += "?timeout=1s"
	fakeHandler.ValidateRequest(t, requestURL, "POST", &reqBody)
}

// This test assumes that the client implementation backs off exponentially, for an individual request.
// This test assumes that the client implementation backs off exponentially, for an individual request.
//
// 中文说明：服务端在第 5、9 次返回成功，其余返回 504。期望每次请求的退避序列为
// 0,1,2,4,8 后因成功而重置，再次 0,1,2,4 后重置，以此验证成功的请求会清空退避状态。
// 使用假时钟精确断言 Sleep 时长，避免真实等待。
func TestBackoffLifecycle(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	count := 0
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		count++
		t.Logf("Attempt %d", count)
		if count == 5 || count == 9 {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusGatewayTimeout)
		return
	}))
	defer testServer.Close()
	c := testRESTClient(t, testServer)

	// Test backoff recovery and increase.  This correlates to the constants
	// which are used in the server implementation returning StatusOK above.
	seconds := []int{0, 1, 2, 4, 8, 0, 1, 2, 4, 0}
	request := c.Verb("POST").Prefix("backofftest").Suffix("abc")
	clock := testingclock.FakeClock{}
	request.backoff = stepClockDuringSleep{
		BackoffManagerWithContext: &URLBackoff{
			// Use a fake backoff here to avoid flakes and speed the test up.
			Backoff: flowcontrol.NewFakeBackOff(
				time.Duration(1)*time.Second,
				time.Duration(200)*time.Second,
				&clock,
			),
		},
		clock: &clock,
	}

	for _, sec := range seconds {
		thisBackoff := request.backoff.CalculateBackoffWithContext(ctx, request.URL())
		t.Logf("Current backoff %v", thisBackoff)
		if thisBackoff != time.Duration(sec)*time.Second {
			t.Errorf("Backoff is %v instead of %v", thisBackoff, sec)
		}

		// This relies on advancing the fake clock by exactly the duration
		// that SleepWithContext is being called for while DoRaw is executing.
		// stepClockDuringSleep.SleepWithContext ensures that this happens.
		now := clock.Now()
		request.DoRaw(ctx)
		elapsed := clock.Since(now)
		if clock.Since(now) != thisBackoff {
			t.Errorf("CalculatedBackoff not honored by clock: Expected time of %v, but got %v ", thisBackoff, elapsed)
		}
	}
}

// stepClockDuringSleep 包装 BackoffManager，使 fake clock 在 sleep 时同步推进。
type stepClockDuringSleep struct {
	BackoffManagerWithContext
	clock *testingclock.FakeClock
}

// SleepWithContext wraps the underlying SleepWithContext and ensures that once
// that is sleeping, the fake clock advances by exactly the duration that
// it is sleeping for.
//
// 中文说明：仅当 d != 0 时才启动推进逻辑；轮询 HasWaiters 直到调用方真正进入
// timer 等待，再 Step(d)，从而让“睡眠时长”与“时钟推进”严格对应。
func (s stepClockDuringSleep) SleepWithContext(ctx context.Context, d time.Duration) {
	// This code is sensitive to both the implementation of
	// URLBackoff.SleepWithContext and of FakeClock.NewTimer:
	// - SleepWithContext must be a no-op when the duration is zero
	//   => no need to step the fake clock
	// - SleepWithContext must use FakeClock.NewTimer, not FakeClock.Sleep
	//   because the latter would advance time itself
	if d != 0 {
		go func() {
			// Poll until the caller is sleeping.
			for {
				if s.clock.HasWaiters() {
					s.clock.Step(d)
					return
				}
				if ctx.Err() != nil {
					return
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	s.BackoffManagerWithContext.SleepWithContext(ctx, d)
}

// testBackoffManager 记录每次 Sleep 的时长，CalculateBackoff 恒为 0，便于断言
// Retry-After 与退避取 max 后的实际睡眠序列。
type testBackoffManager struct {
	sleeps []time.Duration
}

// UpdateBackoffWithContext 空实现。
func (b *testBackoffManager) UpdateBackoffWithContext(ctx context.Context, actualURL *url.URL, err error, responseCode int) {
}

// CalculateBackoffWithContext 恒返回 0。
func (b *testBackoffManager) CalculateBackoffWithContext(ctx context.Context, actualURL *url.URL) time.Duration {
	return time.Duration(0)
}

// SleepWithContext 记录时长而不真正睡眠。
func (b *testBackoffManager) SleepWithContext(ctx context.Context, d time.Duration) {
	b.sleeps = append(b.sleeps, d)
}

// TestCheckRetryClosesBody 验证 429 重试期间请求体被重放、响应体被关闭。
//
// 中文说明：前四次返回 429 + Retry-After: 1，第五次成功；请求体是 []byte，可重放。
// 期望 5 次尝试，睡眠序列为初始 0 加上四次 1s。
func TestCheckRetryClosesBody(t *testing.T) {
	count := 0
	ch := make(chan struct{})
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		count++
		t.Logf("attempt %d", count)
		if count >= 5 {
			w.WriteHeader(http.StatusOK)
			close(ch)
			return
		}
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Too many requests, please try again later.", http.StatusTooManyRequests)
	}))
	defer testServer.Close()

	backoff := &testBackoffManager{}

	// testBackoffManager.CalculateBackoff always returns 0
	expectedSleeps := []time.Duration{0, time.Second, time.Second, time.Second, time.Second}

	c := testRESTClient(t, testServer)
	c.createBackoffMgr = func() BackoffManagerWithContext { return backoff }
	_, err := c.Verb("POST").
		Prefix("foo", "bar").
		Suffix("baz").
		Timeout(time.Second).
		Body([]byte(strings.Repeat("abcd", 1000))).
		DoRaw(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v %#v", err, err)
	}
	<-ch
	if count != 5 {
		t.Errorf("unexpected retries: %d", count)
	}
	if !reflect.DeepEqual(backoff.sleeps, expectedSleeps) {
		t.Errorf("unexpected sleeps, expected: %v, got: %v", expectedSleeps, backoff.sleeps)
	}
}

// TestConnectionResetByPeerIsRetried 验证 ECONNRESET 被识别为可重试错误。
//
// 中文：前两次返回 net.OpError(ECONNRESET)，第三次成功；每次尝试前都有一次 sleep
// （包括首次），所以共 3 次。
func TestConnectionResetByPeerIsRetried(t *testing.T) {
	count := 0
	backoff := &testBackoffManager{}
	req := &Request{
		verb: "GET",
		c: &RESTClient{
			Client: clientForFunc(func(req *http.Request) (*http.Response, error) {
				count++
				if count >= 3 {
					return &http.Response{
						StatusCode: http.StatusOK,
						Body:       io.NopCloser(bytes.NewReader([]byte{})),
					}, nil
				}
				return nil, &net.OpError{Err: syscall.ECONNRESET}
			}),
		},
		backoff:    backoff,
		maxRetries: 10,
		retryFn:    defaultRequestRetryFn,
	}
	// We expect two retries of "connection reset by peer" and the success.
	_, err := req.Do(context.Background()).Raw()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3 attempts, got: %d", count)
	}
	// We have a sleep before each retry (including the initial one) thus 3 together.
	if len(backoff.sleeps) != 3 {
		t.Errorf("Expected 3 backoff.Sleep, got: %d", len(backoff.sleeps))
	}
}

// TestCheckRetryHandles429And5xx 验证 429 与 500/501/504 都会重试，并且每次重试都
// 重新发送完整请求体（不会因 body 已被消费而截断）。
func TestCheckRetryHandles429And5xx(t *testing.T) {
	count := 0
	ch := make(chan struct{})
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("unable to read request body: %v", err)
		}
		if !bytes.Equal(data, []byte(strings.Repeat("abcd", 1000))) {
			t.Fatalf("retry did not send a complete body: %s", data)
		}
		t.Logf("attempt %d", count)
		if count >= 4 {
			w.WriteHeader(http.StatusOK)
			close(ch)
			return
		}
		w.Header().Set("Retry-After", "0")
		w.WriteHeader([]int{http.StatusTooManyRequests, 500, 501, 504}[count])
		count++
	}))
	defer testServer.Close()

	c := testRESTClient(t, testServer)
	_, err := c.Verb("POST").
		Prefix("foo", "bar").
		Suffix("baz").
		Timeout(time.Second).
		Body([]byte(strings.Repeat("abcd", 1000))).
		DoRaw(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error: %v %#v", err, err)
	}
	<-ch
	if count != 4 {
		t.Errorf("unexpected retries: %d", count)
	}
}

// BenchmarkCheckRetryClosesBody 基准测试重试时的 body 重放与响应体关闭成本。
func BenchmarkCheckRetryClosesBody(b *testing.B) {
	count := 0
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		count++
		if count%3 == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer testServer.Close()

	c := testRESTClient(b, testServer)

	requests := make([]*Request, 0, b.N)
	for i := 0; i < b.N; i++ {
		requests = append(requests, c.Verb("POST").
			Prefix("foo", "bar").
			Suffix("baz").
			Timeout(time.Second).
			Body([]byte(strings.Repeat("abcd", 1000))))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := requests[i].DoRaw(context.Background()); err != nil {
			b.Fatalf("Unexpected error (%d/%d): %v", i, b.N, err)
		}
	}
}

// TestDoRequestNewWayReader 验证 io.Reader body 会被完整发送。
func TestDoRequestNewWayReader(t *testing.T) {
	reqObj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	reqBodyExpected, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), reqObj)
	expectedObj := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{
		Protocol:   "TCP",
		Port:       12345,
		TargetPort: intstr.FromInt32(12345),
	}}}}
	expectedBody, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), expectedObj)
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:   200,
		ResponseBody: string(expectedBody),
		T:            t,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	c := testRESTClient(t, testServer)
	obj, err := c.Verb("POST").
		Resource("bar").
		Name("baz").
		Prefix("foo").
		Timeout(time.Second).
		Body(bytes.NewBuffer(reqBodyExpected)).
		Do(context.Background()).Get()
	if err != nil {
		t.Errorf("Unexpected error: %v %#v", err, err)
		return
	}
	if obj == nil {
		t.Error("nil obj")
	} else if !apiequality.Semantic.DeepDerivative(expectedObj, obj) {
		t.Errorf("Expected: %#v, got %#v", expectedObj, obj)
	}
	tmpStr := string(reqBodyExpected)
	requestURL := defaultResourcePathWithPrefix("foo", "bar", "", "baz")
	requestURL += "?timeout=1s"
	fakeHandler.ValidateRequest(t, requestURL, "POST", &tmpStr)
}

// TestDoRequestNewWayObj 验证 API 对象 body 会被编码并自动设置 JSON Content-Type。
func TestDoRequestNewWayObj(t *testing.T) {
	reqObj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	reqBodyExpected, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), reqObj)
	expectedObj := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{
		Protocol:   "TCP",
		Port:       12345,
		TargetPort: intstr.FromInt32(12345),
	}}}}
	expectedBody, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), expectedObj)
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:   200,
		ResponseBody: string(expectedBody),
		T:            t,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	c := testRESTClient(t, testServer)
	obj, err := c.Verb("POST").
		Suffix("baz").
		Name("bar").
		Resource("foo").
		Timeout(time.Second).
		Body(reqObj).
		Do(context.Background()).Get()
	if err != nil {
		t.Errorf("Unexpected error: %v %#v", err, err)
		return
	}
	if obj == nil {
		t.Error("nil obj")
	} else if !apiequality.Semantic.DeepDerivative(expectedObj, obj) {
		t.Errorf("Expected: %#v, got %#v", expectedObj, obj)
	}
	tmpStr := string(reqBodyExpected)
	requestURL := defaultResourcePathWithPrefix("", "foo", "", "bar/baz")
	requestURL += "?timeout=1s"
	fakeHandler.ValidateRequest(t, requestURL, "POST", &tmpStr)
}

// TestDoRequestNewWayFile 验证以文件路径作为 body 时内容被正确发送；
// 响应为 200 而非 201，因此 WasCreated 应为 false。
func TestDoRequestNewWayFile(t *testing.T) {
	reqObj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	reqBodyExpected, err := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), reqObj)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	file, err := os.CreateTemp("", "foo")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer file.Close()
	defer os.Remove(file.Name())

	_, err = file.Write(reqBodyExpected)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expectedObj := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{
		Protocol:   "TCP",
		Port:       12345,
		TargetPort: intstr.FromInt32(12345),
	}}}}
	expectedBody, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), expectedObj)
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:   200,
		ResponseBody: string(expectedBody),
		T:            t,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	c := testRESTClient(t, testServer)
	wasCreated := true
	obj, err := c.Verb("POST").
		Prefix("foo/bar", "baz").
		Timeout(time.Second).
		Body(file.Name()).
		Do(context.Background()).WasCreated(&wasCreated).Get()
	if err != nil {
		t.Errorf("Unexpected error: %v %#v", err, err)
		return
	}
	if obj == nil {
		t.Error("nil obj")
	} else if !apiequality.Semantic.DeepDerivative(expectedObj, obj) {
		t.Errorf("Expected: %#v, got %#v", expectedObj, obj)
	}
	if wasCreated {
		t.Errorf("expected object was created")
	}
	tmpStr := string(reqBodyExpected)
	requestURL := defaultResourcePathWithPrefix("foo/bar/baz", "", "", "")
	requestURL += "?timeout=1s"
	fakeHandler.ValidateRequest(t, requestURL, "POST", &tmpStr)
}

// TestWasCreated 验证响应 201 时 WasCreated 被置为 true，供调用方区分创建与更新。
func TestWasCreated(t *testing.T) {
	reqObj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	reqBodyExpected, err := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), reqObj)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	expectedObj := &v1.Service{Spec: v1.ServiceSpec{Ports: []v1.ServicePort{{
		Protocol:   "TCP",
		Port:       12345,
		TargetPort: intstr.FromInt32(12345),
	}}}}
	expectedBody, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), expectedObj)
	fakeHandler := utiltesting.FakeHandler{
		StatusCode:   201,
		ResponseBody: string(expectedBody),
		T:            t,
	}
	testServer := httptest.NewServer(&fakeHandler)
	defer testServer.Close()
	c := testRESTClient(t, testServer)
	wasCreated := false
	obj, err := c.Verb("PUT").
		Prefix("foo/bar", "baz").
		Timeout(time.Second).
		Body(reqBodyExpected).
		Do(context.Background()).WasCreated(&wasCreated).Get()
	if err != nil {
		t.Errorf("Unexpected error: %v %#v", err, err)
		return
	}
	if obj == nil {
		t.Error("nil obj")
	} else if !apiequality.Semantic.DeepDerivative(expectedObj, obj) {
		t.Errorf("Expected: %#v, got %#v", expectedObj, obj)
	}
	if !wasCreated {
		t.Errorf("Expected object was created")
	}

	tmpStr := string(reqBodyExpected)
	requestURL := defaultResourcePathWithPrefix("foo/bar/baz", "", "", "")
	requestURL += "?timeout=1s"
	fakeHandler.ValidateRequest(t, requestURL, "PUT", &tmpStr)
}

// TestVerbs 验证 Post/Put/Get/Delete 便捷方法设置的 HTTP 动词。
func TestVerbs(t *testing.T) {
	c := testRESTClient(t, nil)
	if r := c.Post(); r.verb != "POST" {
		t.Errorf("Post verb is wrong")
	}
	if r := c.Put(); r.verb != "PUT" {
		t.Errorf("Put verb is wrong")
	}
	if r := c.Get(); r.verb != "GET" {
		t.Errorf("Get verb is wrong")
	}
	if r := c.Delete(); r.verb != "DELETE" {
		t.Errorf("Delete verb is wrong")
	}
}

// TestAbsPath 验证 AbsPath 与客户端 base 前缀、资源前缀的合并规则：
// AbsPath 以绝对路径语义覆盖资源前缀，但保留客户端 base 上的部署前缀。
func TestAbsPath(t *testing.T) {
	for i, tc := range []struct {
		configPrefix   string
		resourcePrefix string
		absPath        string
		wantsAbsPath   string
	}{
		{"/", "", "", "/"},
		{"", "", "/", "/"},
		{"", "", "/api", "/api"},
		{"", "", "/api/", "/api/"},
		{"", "", "/apis", "/apis"},
		{"", "/foo", "/bar/foo", "/bar/foo"},
		{"", "/api/foo/123", "/bar/foo", "/bar/foo"},
		{"/p1", "", "", "/p1"},
		{"/p1", "", "/", "/p1/"},
		{"/p1", "", "/api", "/p1/api"},
		{"/p1", "", "/apis", "/p1/apis"},
		{"/p1", "/r1", "/apis", "/p1/apis"},
		{"/p1", "/api/r1", "/apis", "/p1/apis"},
		{"/p1/api/p2", "", "", "/p1/api/p2"},
		{"/p1/api/p2", "", "/", "/p1/api/p2/"},
		{"/p1/api/p2", "", "/api", "/p1/api/p2/api"},
		{"/p1/api/p2", "", "/api/", "/p1/api/p2/api/"},
		{"/p1/api/p2", "/r1", "/api/", "/p1/api/p2/api/"},
		{"/p1/api/p2", "/api/r1", "/api/", "/p1/api/p2/api/"},
	} {
		u, _ := url.Parse("http://localhost:123" + tc.configPrefix)
		r := NewRequestWithClient(u, "", ClientContentConfig{GroupVersion: schema.GroupVersion{Group: "test"}}, nil).Verb("POST").Prefix(tc.resourcePrefix).AbsPath(tc.absPath)
		if r.pathPrefix != tc.wantsAbsPath {
			t.Errorf("test case %d failed, unexpected path: %q, expected %q", i, r.pathPrefix, tc.wantsAbsPath)
		}
	}
}

// TestUnacceptableParamNames 验证 timeout 已不再是受保护参数，可以显式设置。
func TestUnacceptableParamNames(t *testing.T) {
	table := []struct {
		name          string
		testVal       string
		expectSuccess bool
	}{
		// timeout is no longer "protected"
		{"timeout", "42", true},
	}

	for _, item := range table {
		c := testRESTClient(t, nil)
		r := c.Get().setParam(item.name, item.testVal)
		if e, a := item.expectSuccess, r.err == nil; e != a {
			t.Errorf("expected %v, got %v (%v)", e, a, r.err)
		}
	}
}

// TestBody 验证 Body 对各种输入的处理：字节、文件、Reader、API 对象与类型化 nil。
//
// 中文：类型化 nil（如 nil *DeleteOptions）应产生空 body 而非零值对象序列化结果。
func TestBody(t *testing.T) {
	const data = "test payload"

	obj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	bodyExpected, _ := runtime.Encode(scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), obj)

	f, err := os.CreateTemp("", "test_body")
	if err != nil {
		t.Fatalf("TempFile error: %v", err)
	}
	if _, err := f.WriteString(data); err != nil {
		t.Fatalf("TempFile.WriteString error: %v", err)
	}
	f.Close()
	defer os.Remove(f.Name())

	var nilObject *metav1.DeleteOptions
	typedObject := interface{}(nilObject)
	c := testRESTClient(t, nil)
	tests := []struct {
		input    interface{}
		expected string
		headers  map[string]string
	}{
		{[]byte(data), data, nil},
		{f.Name(), data, nil},
		{strings.NewReader(data), data, nil},
		{obj, string(bodyExpected), map[string]string{"Content-Type": "application/json"}},
		{typedObject, "", nil},
	}
	for i, tt := range tests {
		r := c.Post().Body(tt.input)
		if r.err != nil {
			t.Errorf("%d: r.Body(%#v) error: %v", i, tt, r.err)
			continue
		}
		if tt.headers != nil {
			for k, v := range tt.headers {
				if r.headers.Get(k) != v {
					t.Errorf("%d: r.headers[%q] = %q; want %q", i, k, v, v)
				}
			}
		}

		req, err := r.newHTTPRequest(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if req.Body == nil {
			if len(tt.expected) != 0 {
				t.Errorf("%d: req.Body = %q; want %q", i, req.Body, tt.expected)
			}
			continue
		}
		buf := make([]byte, len(tt.expected))
		if _, err := req.Body.Read(buf); err != nil {
			t.Errorf("%d: req.Body.Read error: %v", i, err)
			continue
		}
		body := string(buf)
		if body != tt.expected {
			t.Errorf("%d: req.Body = %q; want %q", i, body, tt.expected)
		}
	}
}

// TestWatch 验证 watch 流的事件解码与重试恢复。
//
// 中文说明：分别覆盖不重试与重试 3 次两种配置。服务端在重试耗尽前返回 429，随后
// 以 chunked 流发送 Added/Modified/Deleted 三个事件并结束；客户端应按序收到事件，
// 并在流结束后关闭 ResultChan。
func TestWatch(t *testing.T) {
	tests := []struct {
		name       string
		maxRetries int
	}{
		{
			name:       "no retry",
			maxRetries: 0,
		},
		{
			name:       "with retries",
			maxRetries: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var table = []struct {
				t   watch.EventType
				obj runtime.Object
			}{
				{watch.Added, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "first"}}},
				{watch.Modified, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "second"}}},
				{watch.Deleted, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "last"}}},
			}

			var attempts int
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer func() {
					attempts++
				}()

				flusher, ok := w.(http.Flusher)
				if !ok {
					panic("need flusher!")
				}

				if attempts < test.maxRetries {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}

				w.Header().Set("Transfer-Encoding", "chunked")
				w.WriteHeader(http.StatusOK)
				flusher.Flush()

				encoder := restclientwatch.NewEncoder(streaming.NewEncoder(w, scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)), scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion))
				for _, item := range table {
					if err := encoder.Encode(&watch.Event{Type: item.t, Object: item.obj}); err != nil {
						panic(err)
					}
					flusher.Flush()
				}
			}))
			defer testServer.Close()

			s := testRESTClient(t, testServer)
			watching, err := s.Get().Prefix("path/to/watch/thing").
				MaxRetries(test.maxRetries).Watch(context.Background())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for _, item := range table {
				got, ok := <-watching.ResultChan()
				if !ok {
					t.Fatalf("Unexpected early close")
				}
				if e, a := item.t, got.Type; e != a {
					t.Errorf("Expected %v, got %v", e, a)
				}
				if e, a := item.obj, got.Object; !apiequality.Semantic.DeepDerivative(e, a) {
					t.Errorf("Expected %v, got %v", e, a)
				}
			}

			_, ok := <-watching.ResultChan()
			if ok {
				t.Fatal("Unexpected non-close")
			}
		})
	}
}

// TestWatchNonDefaultContentType 验证客户端偏好 protobuf、服务端实际返回 JSON 时，
// 能通过响应 Content-Type 重新协商并使用 JSON 流解码。
func TestWatchNonDefaultContentType(t *testing.T) {
	var table = []struct {
		t   watch.EventType
		obj runtime.Object
	}{
		{watch.Added, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "first"}}},
		{watch.Modified, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "second"}}},
		{watch.Deleted, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "last"}}},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			panic("need flusher!")
		}

		w.Header().Set("Transfer-Encoding", "chunked")
		// manually set the content type here so we get the renegotiation behavior
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		encoder := restclientwatch.NewEncoder(streaming.NewEncoder(w, scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)), scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion))
		for _, item := range table {
			if err := encoder.Encode(&watch.Event{Type: item.t, Object: item.obj}); err != nil {
				panic(err)
			}
			flusher.Flush()
		}
	}))
	defer testServer.Close()

	// set the default content type to protobuf so that we test falling back to JSON serialization
	contentConfig := defaultContentConfig()
	contentConfig.ContentType = "application/vnd.kubernetes.protobuf"
	s := testRESTClientWithConfig(t, testServer, contentConfig)
	watching, err := s.Get().Prefix("path/to/watch/thing").Watch(context.Background())
	if err != nil {
		t.Fatalf("Unexpected error")
	}

	for _, item := range table {
		got, ok := <-watching.ResultChan()
		if !ok {
			t.Fatalf("Unexpected early close")
		}
		if e, a := item.t, got.Type; e != a {
			t.Errorf("Expected %v, got %v", e, a)
		}
		if e, a := item.obj, got.Object; !apiequality.Semantic.DeepDerivative(e, a) {
			t.Errorf("Expected %v, got %v", e, a)
		}
	}

	_, ok := <-watching.ResultChan()
	if ok {
		t.Fatal("Unexpected non-close")
	}
}

// TestWatchUnknownContentType 验证服务端返回未知流类型时 Watch 直接失败，
// 而不是静默使用错误的解码器。
func TestWatchUnknownContentType(t *testing.T) {
	var table = []struct {
		t   watch.EventType
		obj runtime.Object
	}{
		{watch.Added, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "first"}}},
		{watch.Modified, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "second"}}},
		{watch.Deleted, &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "last"}}},
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			panic("need flusher!")
		}

		w.Header().Set("Transfer-Encoding", "chunked")
		// manually set the content type here so we get the renegotiation behavior
		w.Header().Set("Content-Type", "foobar")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		encoder := restclientwatch.NewEncoder(streaming.NewEncoder(w, scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion)), scheme.Codecs.LegacyCodec(v1.SchemeGroupVersion))
		for _, item := range table {
			if err := encoder.Encode(&watch.Event{Type: item.t, Object: item.obj}); err != nil {
				panic(err)
			}
			flusher.Flush()
		}
	}))
	defer testServer.Close()

	s := testRESTClient(t, testServer)
	_, err := s.Get().Prefix("path/to/watch/thing").Watch(context.Background())
	if err == nil {
		t.Fatalf("Expected to fail due to lack of known stream serialization for content type")
	}
}

// TestStream 验证 Stream 原样返回响应字节流（不做 schema 解码）。
func TestStream(t *testing.T) {
	expectedBody := "expected body"

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			panic("need flusher!")
		}
		w.Header().Set("Transfer-Encoding", "chunked")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(expectedBody))
		flusher.Flush()
	}))
	defer testServer.Close()

	s := testRESTClient(t, testServer)
	readCloser, err := s.Get().Prefix("path/to/stream/thing").Stream(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer readCloser.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(readCloser)
	resultBody := buf.String()

	if expectedBody != resultBody {
		t.Errorf("Expected %s, got %s", expectedBody, resultBody)
	}
}

// testRESTClientWithConfig 构造指向测试服务器的 RESTClient；
// srv 为 nil 时使用 http://localhost，仅用于不需要真实网络的用例。
func testRESTClientWithConfig(t testing.TB, srv *httptest.Server, contentConfig ClientContentConfig) *RESTClient {
	base, _ := url.Parse("http://localhost")
	var c *http.Client
	if srv != nil {
		var err error
		base, err = url.Parse(srv.URL)
		if err != nil {
			t.Fatalf("failed to parse test URL: %v", err)
		}
		c = srv.Client()
	}
	versionedAPIPath := defaultResourcePathWithPrefix("", "", "", "")
	client, err := NewRESTClient(base, versionedAPIPath, contentConfig, nil, c)
	if err != nil {
		t.Fatalf("failed to create a client: %v", err)
	}
	return client

}

// testRESTClient 使用默认 JSON 内容配置构造测试客户端。
func testRESTClient(t testing.TB, srv *httptest.Server) *RESTClient {
	contentConfig := defaultContentConfig()
	return testRESTClientWithConfig(t, srv, contentConfig)
}

// TestDoContext 验证 context 取消会中止进行中的请求并返回错误。
//
// 中文：服务端阻塞直到测试取消 ctx；客户端应尽快返回 cancellation 错误。
func TestDoContext(t *testing.T) {
	receivedCh := make(chan struct{})
	block := make(chan struct{})
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(receivedCh)
		<-block
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()
	defer close(block)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-receivedCh
		cancel()
	}()

	c := testRESTClient(t, testServer)
	_, err := c.Verb("GET").
		Prefix("foo").
		DoRaw(ctx)
	if err == nil {
		t.Fatal("Expected context cancellation error")
	}
}

// buildString 生成长度为 length、全部由 'a' 组成的字符串。
func buildString(length int) string {
	s := make([]byte, length)
	for i := range s {
		s[i] = 'a'
	}
	return string(s)
}

// init 注册 klog 的 flag，使测试可以通过 -v 控制日志级别。
func init() {
	klog.InitFlags(nil)
}

// TestTruncateBody 验证日志正文按 klog 详细级别截断。
//
// 中文说明：级别 <8 时正文完全不记录；级别 8 截断到 1024 字符；级别 9 截断到 10240；
// 级别 ≥10 不截断。截断后缀会注明丢弃的字符数。
func TestTruncateBody(t *testing.T) {
	tests := []struct {
		body  string
		want  string
		level string
	}{
		// Anything below 8 is completely truncated
		{
			body:  "Completely truncated below 8",
			want:  " [truncated 28 chars]",
			level: "0",
		},
		// Small strings are not truncated by high levels
		{
			body:  "Small body never gets truncated",
			want:  "Small body never gets truncated",
			level: "10",
		},
		{
			body:  "Small body never gets truncated",
			want:  "Small body never gets truncated",
			level: "8",
		},
		// Strings are truncated to 1024 if level is less than 9.
		{
			body:  buildString(2000),
			level: "8",
			want:  fmt.Sprintf("%s [truncated 976 chars]", buildString(1024)),
		},
		// Strings are truncated to 10240 if level is 9.
		{
			body:  buildString(20000),
			level: "9",
			want:  fmt.Sprintf("%s [truncated 9760 chars]", buildString(10240)),
		},
		// Strings are not truncated if level is 10 or higher
		{
			body:  buildString(20000),
			level: "10",
			want:  buildString(20000),
		},
		// Strings are not truncated if level is 10 or higher
		{
			body:  buildString(20000),
			level: "11",
			want:  buildString(20000),
		},
	}

	l := flag.Lookup("v").Value.(flag.Getter).Get().(klog.Level)
	for _, test := range tests {
		flag.Set("v", test.level)
		got := truncateBody(klog.Background(), test.body)
		if got != test.want {
			t.Errorf("truncateBody(%v) = %v, want %v", test.body, got, test.want)
		}
	}
	flag.Set("v", l.String())
}

// defaultResourcePathWithPrefix 按核心组的 URL 规则拼接期望路径，供断言使用。
func defaultResourcePathWithPrefix(prefix, resource, namespace, name string) string {
	var path string
	path = "/api/" + v1.SchemeGroupVersion.Version

	if prefix != "" {
		path = path + "/" + prefix
	}
	if namespace != "" {
		path = path + "/namespaces/" + namespace
	}
	// Resource names are lower case.
	resource = strings.ToLower(resource)
	if resource != "" {
		path = path + "/" + resource
	}
	if name != "" {
		path = path + "/" + name
	}
	return path
}

// TestRequestPreflightCheck 验证发送前的路径完整性校验。
//
// 中文说明：显式设置了 namespace 时，GET/PUT/DELETE 若带 name 则必须也有 namespace；
// POST 即使无 name，也要求指定 namespace（集群级 POST 应走不同的构造函数）。
func TestRequestPreflightCheck(t *testing.T) {
	for _, tt := range []struct {
		name         string
		verbs        []string
		namespace    string
		resourceName string
		namespaceSet bool
		expectsErr   bool
	}{
		{
			name:         "no namespace set",
			verbs:        []string{"GET", "PUT", "DELETE", "POST"},
			namespaceSet: false,
			expectsErr:   false,
		},
		{
			name:         "empty resource name and namespace",
			verbs:        []string{"GET", "PUT", "DELETE"},
			namespaceSet: true,
			expectsErr:   false,
		},
		{
			name:         "resource name with empty namespace",
			verbs:        []string{"GET", "PUT", "DELETE"},
			namespaceSet: true,
			resourceName: "ResourceName",
			expectsErr:   true,
		},
		{
			name:         "post empty resource name and namespace",
			verbs:        []string{"POST"},
			namespaceSet: true,
			expectsErr:   true,
		},
		{
			name:         "working requests",
			verbs:        []string{"GET", "PUT", "DELETE", "POST"},
			namespaceSet: true,
			resourceName: "ResourceName",
			namespace:    "Namespace",
			expectsErr:   false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, verb := range tt.verbs {
				r := &Request{
					verb:         verb,
					namespace:    tt.namespace,
					resourceName: tt.resourceName,
					namespaceSet: tt.namespaceSet,
				}

				err := r.requestPreflightCheck()
				hasErr := err != nil
				if hasErr == tt.expectsErr {
					return
				}
				t.Errorf("%s: expects error: %v, has error: %v", verb, tt.expectsErr, hasErr)
			}
		})
	}
}

// TestThrottledLogger 验证全局节流日志在并发下每秒最多输出一条。
//
// 中文说明：1000 秒 × 每秒 10 个并发调用，期望恰好 100 条（每秒 1 条），且不因并发
// 竞争而多记。测试直接替换全局 clock，结束后恢复，避免污染其它用例。
func TestThrottledLogger(t *testing.T) {
	logger := klog.Background()
	now := time.Now()
	oldClock := globalThrottledLogger.clock
	defer func() {
		globalThrottledLogger.clock = oldClock
	}()
	clock := testingclock.NewFakeClock(now)
	globalThrottledLogger.clock = clock

	logMessages := 0
	for i := 0; i < 1000; i++ {
		var wg sync.WaitGroup
		wg.Add(10)
		for j := 0; j < 10; j++ {
			go func() {
				if _, ok := globalThrottledLogger.attemptToLog(logger); ok {
					logMessages++
				}
				wg.Done()
			}()
		}
		wg.Wait()
		now = now.Add(1 * time.Second)
		clock.SetTime(now)
	}

	if a, e := logMessages, 100; a != e {
		t.Fatalf("expected %v log messages, but got %v", e, a)
	}
}

// TestRequestMaxRetries 验证 maxRetries 是精确的上限：0 次不重试，1 次恰好覆盖一次
// 429，5 次也正常成功（多余额度不会被使用）。
func TestRequestMaxRetries(t *testing.T) {
	successAtNthCalls := 1
	actualCalls := 0
	retryOneTimeHandler := func(w http.ResponseWriter, req *http.Request) {
		defer func() { actualCalls++ }()
		if actualCalls >= successAtNthCalls {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		actualCalls++
	}
	testServer := httptest.NewServer(http.HandlerFunc(retryOneTimeHandler))
	defer testServer.Close()

	u, err := url.Parse(testServer.URL)
	if err != nil {
		t.Error(err)
	}

	testCases := []struct {
		name        string
		maxRetries  int
		expectError bool
	}{
		{
			name:        "no retrying should fail",
			maxRetries:  0,
			expectError: true,
		},
		{
			name:        "1 max-retry should exactly work",
			maxRetries:  1,
			expectError: false,
		},
		{
			name:        "5 max-retry should work",
			maxRetries:  5,
			expectError: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			defer func() { actualCalls = 0 }()
			_, err := NewRequestWithClient(u, "", defaultContentConfig(), testServer.Client()).
				Verb("get").
				MaxRetries(testCase.maxRetries).
				AbsPath("/foo").
				DoRaw(context.TODO())
			hasError := err != nil
			if testCase.expectError != hasError {
				t.Error(" failed checking error")
			}
		})
	}
}

// responseErr 表示一次模拟的服务端返回：响应与错误二选一（或同时为 nil）。
type responseErr struct {
	response *http.Response
	err      error
}

// seek 记录一次 Seek 调用的参数，用于断言请求体是否被回绕重放。
type seek struct {
	offset int64
	whence int
}

// count 记录 Seek 调用序列与 Close 次数；closes 加锁以支持并发场景。
type count struct {
	// keeps track of the number of Seek(offset, whence) calls.
	seeks []seek

	// how many times {Request|Response}.Body.Close() has been invoked
	lock   sync.Mutex
	closes int
}

// close 增加关闭计数。
func (c *count) close() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.closes++
}

// getCloseCount 返回已记录的关闭次数。
func (c *count) getCloseCount() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.closes
}

// used to track {Request|Response}.Body
//
// 中文说明：包装底层 body，记录 Seek/Close 调用；delegated 可在每次响应时替换，
// 从而对每次尝试使用新的空 body。
type readTracker struct {
	delegated io.Reader
	count     *count
}

// Seek 委托给底层 Seeker 并记录参数；底层不可 Seek 时返回 EOF。
func (r *readTracker) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := r.delegated.(io.Seeker); ok {
		r.count.seeks = append(r.count.seeks, seek{offset: offset, whence: whence})
		return seeker.Seek(offset, whence)
	}
	return 0, io.EOF
}

// Read 直接委托底层 Reader。
func (r *readTracker) Read(p []byte) (n int, err error) {
	return r.delegated.Read(p)
}

// Close 记录一次关闭后委托底层 Closer。
func (r *readTracker) Close() error {
	if closer, ok := r.delegated.(io.Closer); ok {
		r.count.close()
		return closer.Close()
	}
	return nil
}

// newReadTracker 创建绑定给定计数器的 reader 包装器。
func newReadTracker(count *count) *readTracker {
	return &readTracker{
		count: count,
	}
}

// newCount 创建零值的计数器。
func newCount() *count {
	return &count{
		closes: 0,
		seeks:  make([]seek, 0),
	}
}

// readSeeker 是一个永远失败的 ReadSeeker，用于模拟不可重放的请求体。
type readSeeker struct{ err error }

// Read 恒返回预设错误。
func (rs readSeeker) Read([]byte) (int, error) { return 0, rs.err }

// Seek 恒返回预设错误。
func (rs readSeeker) Seek(int64, int) (int64, error) { return 0, rs.err }

// unWrap 取出 *url.Error 内部的实际错误，便于直接比较错误值。
func unWrap(err error) error {
	if uerr, ok := err.(*url.Error); ok {
		return uerr.Err
	}
	return err
}

// noSleepBackOff is a NoBackoff except it does not sleep,
// used for faster execution of the unit tests.
//
// 中文：行为等价于 NoBackoff，但跳过实际等待，使重试测试无需真实延时。
type noSleepBackOff struct {
	*NoBackoff
}

// Sleep 空实现。
func (n *noSleepBackOff) Sleep(d time.Duration) {}

// SleepWithContext 空实现。
func (n *noSleepBackOff) SleepWithContext(ctx context.Context, d time.Duration) {}

// TestRequestWithRetry 验证 request() 内部循环的重试判定与 body 可重放性。
//
// 中文说明：无 body 或 []byte body 时可重试，opaque（不可 Seek）body 时只能尝试一次；
// Retry-After 响应会触发转换函数，而传输层错误不会走到响应转换。
func TestRequestWithRetry(t *testing.T) {
	tests := []struct {
		name                         string
		body                         io.Reader
		bodyBytes                    []byte
		serverReturns                responseErr
		errExpected                  error
		errContains                  string
		transformFuncInvokedExpected int
		roundTripInvokedExpected     int
	}{
		{
			name:                         "server returns retry-after response, no request body, retry goes ahead",
			bodyBytes:                    nil,
			serverReturns:                responseErr{response: retryAfterResponse(), err: nil},
			errExpected:                  nil,
			transformFuncInvokedExpected: 1,
			roundTripInvokedExpected:     2,
		},
		{
			name:                         "server returns retry-after response, bytes request body, retry goes ahead",
			bodyBytes:                    []byte{},
			serverReturns:                responseErr{response: retryAfterResponse(), err: nil},
			errExpected:                  nil,
			transformFuncInvokedExpected: 1,
			roundTripInvokedExpected:     2,
		},
		{
			name:                         "server returns retry-after response, opaque request body, retry aborted",
			body:                         &readSeeker{},
			serverReturns:                responseErr{response: retryAfterResponse(), err: nil},
			errExpected:                  nil,
			transformFuncInvokedExpected: 1,
			roundTripInvokedExpected:     1,
		},
		{
			name:                         "server returns retryable err, no request body, retry goes ahead",
			bodyBytes:                    nil,
			serverReturns:                responseErr{response: nil, err: io.ErrUnexpectedEOF},
			errExpected:                  io.ErrUnexpectedEOF,
			transformFuncInvokedExpected: 0,
			roundTripInvokedExpected:     2,
		},
		{
			name:                         "server returns retryable err, bytes request body, retry goes ahead",
			bodyBytes:                    []byte{},
			serverReturns:                responseErr{response: nil, err: io.ErrUnexpectedEOF},
			errExpected:                  io.ErrUnexpectedEOF,
			transformFuncInvokedExpected: 0,
			roundTripInvokedExpected:     2,
		},
		{
			name:                         "server returns retryable err, opaque request body, retry aborted",
			body:                         &readSeeker{},
			serverReturns:                responseErr{response: nil, err: io.ErrUnexpectedEOF},
			errExpected:                  io.ErrUnexpectedEOF,
			transformFuncInvokedExpected: 0,
			roundTripInvokedExpected:     1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var roundTripInvoked int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				roundTripInvoked++
				return test.serverReturns.response, test.serverReturns.err
			})

			req := &Request{
				verb: "GET",
				body: test.body,
				c: &RESTClient{
					Client: client,
				},
				backoff:    &noSleepBackOff{},
				maxRetries: 1,
				retryFn:    defaultRequestRetryFn,
			}

			var transformFuncInvoked int
			err := req.request(context.Background(), func(request *http.Request, response *http.Response) {
				transformFuncInvoked++
			})

			if test.roundTripInvokedExpected != roundTripInvoked {
				t.Errorf("Expected RoundTrip to be invoked %d times, but got: %d", test.roundTripInvokedExpected, roundTripInvoked)
			}
			if test.transformFuncInvokedExpected != transformFuncInvoked {
				t.Errorf("Expected transform func to be invoked %d times, but got: %d", test.transformFuncInvokedExpected, transformFuncInvoked)
			}
			switch {
			case test.errExpected != nil:
				if test.errExpected != unWrap(err) {
					t.Errorf("Expected error: %v, but got: %v", test.errExpected, unWrap(err))
				}
			case len(test.errContains) > 0:
				if !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("Expected error message to caontain: %q, but got: %q", test.errContains, err.Error())
				}
			}
		})
	}
}

// TestRequestDoWithRetry 以 Do 路径运行通用的重试测试。
func TestRequestDoWithRetry(t *testing.T) {
	testRequestWithRetry(t, "Do", func(ctx context.Context, r *Request) {
		r.Do(ctx)
	})
}

// TestRequestDoRawWithRetry 以 DoRaw 路径运行通用的重试测试。
func TestRequestDoRawWithRetry(t *testing.T) {
	// both request.Do and request.DoRaw have the same behavior and expectations
	testRequestWithRetry(t, "Do", func(ctx context.Context, r *Request) {
		r.DoRaw(ctx)
	})
}

// TestRequestStreamWithRetry 以 Stream 路径运行通用的重试测试。
func TestRequestStreamWithRetry(t *testing.T) {
	testRequestWithRetry(t, "Stream", func(ctx context.Context, r *Request) {
		r.Stream(ctx)
	})
}

// TestRequestWatchWithRetry 以 Watch 路径运行通用的重试测试。
func TestRequestWatchWithRetry(t *testing.T) {
	testRequestWithRetry(t, "Watch", func(ctx context.Context, r *Request) {
		w, err := r.Watch(ctx)
		if err == nil {
			// in this test the response body returned by the server is always empty,
			// this will cause StreamWatcher.receive() to:
			// - return an io.EOF to indicate that the watch closed normally and
			// - then close the io.Reader
			// since we assert on the number of times 'Close' has been called on the
			// body of the response object, we need to wait here to avoid race condition.
			<-w.ResultChan()
		}
	})
}

// TestRequestDoRetryWithRateLimiterBackoffAndMetrics 以 DoRaw 路径验证速率限制、
// 退避与指标的协同调用。
func TestRequestDoRetryWithRateLimiterBackoffAndMetrics(t *testing.T) {
	// both request.Do and request.DoRaw have the same behavior and expectations
	testRetryWithRateLimiterBackoffAndMetrics(t, "Do", func(ctx context.Context, r *Request) {
		r.DoRaw(ctx)
	})
}

// TestRequestStreamRetryWithRateLimiterBackoffAndMetrics 以 Stream 路径运行上组验证。
func TestRequestStreamRetryWithRateLimiterBackoffAndMetrics(t *testing.T) {
	testRetryWithRateLimiterBackoffAndMetrics(t, "Stream", func(ctx context.Context, r *Request) {
		r.Stream(ctx)
	})
}

// TestRequestWatchRetryWithRateLimiterBackoffAndMetrics 以 Watch 路径运行上组验证。
func TestRequestWatchRetryWithRateLimiterBackoffAndMetrics(t *testing.T) {
	testRetryWithRateLimiterBackoffAndMetrics(t, "Watch", func(ctx context.Context, r *Request) {
		w, err := r.Watch(ctx)
		if err == nil {
			// in this test the response body returned by the server is always empty,
			// this will cause StreamWatcher.receive() to:
			// - return an io.EOF to indicate that the watch closed normally and
			// - then close the io.Reader
			// since we assert on the number of times 'Close' has been called on the
			// body of the response object, we need to wait here to avoid race condition.
			<-w.ResultChan()
		}
	})
}

// TestRequestDoWithRetryInvokeOrder 以 DoRaw 路径验证 WithRetry 回调的调用顺序。
func TestRequestDoWithRetryInvokeOrder(t *testing.T) {
	// both request.Do and request.DoRaw have the same behavior and expectations
	testWithRetryInvokeOrder(t, "Do", func(ctx context.Context, r *Request) {
		r.DoRaw(ctx)
	})
}

// TestRequestStreamWithRetryInvokeOrder 以 Stream 路径验证 WithRetry 回调的调用顺序。
func TestRequestStreamWithRetryInvokeOrder(t *testing.T) {
	testWithRetryInvokeOrder(t, "Stream", func(ctx context.Context, r *Request) {
		r.Stream(ctx)
	})
}

// TestRequestWatchWithRetryInvokeOrder 以 Watch 路径验证 WithRetry 回调的调用顺序。
func TestRequestWatchWithRetryInvokeOrder(t *testing.T) {
	testWithRetryInvokeOrder(t, "Watch", func(ctx context.Context, r *Request) {
		w, err := r.Watch(ctx)
		if err == nil {
			// in this test the response body returned by the server is always empty,
			// this will cause StreamWatcher.receive() to:
			// - return an io.EOF to indicate that the watch closed normally and
			// - then close the io.Reader
			// since we assert on the number of times 'Close' has been called on the
			// body of the response object, we need to wait here to avoid race condition.
			<-w.ResultChan()
		}
	})
}

// TestRequestWatchWithWrapPreviousError 验证 Watch 在重试耗尽时包装最近一次网络错误。
func TestRequestWatchWithWrapPreviousError(t *testing.T) {
	testWithWrapPreviousError(t, func(ctx context.Context, r *Request) error {
		w, err := r.Watch(ctx)
		if err == nil {
			// in this test the response body returned by the server is always empty,
			// this will cause StreamWatcher.receive() to:
			// - return an io.EOF to indicate that the watch closed normally and
			// - then close the io.Reader
			// since we assert on the number of times 'Close' has been called on the
			// body of the response object, we need to wait here to avoid race condition.
			<-w.ResultChan()
		}
		return err
	})
}

// TestRequestDoWithWrapPreviousError 验证 Do 在重试耗尽时包装最近一次网络错误。
func TestRequestDoWithWrapPreviousError(t *testing.T) {
	// both request.Do and request.DoRaw have the same behavior and expectations
	testWithWrapPreviousError(t, func(ctx context.Context, r *Request) error {
		result := r.Do(ctx)
		return result.err
	})
}

// testRequestWithRetry 是 Do/DoRaw/Stream/Watch 共享的重试行为测试。
//
// 中文说明：对每种路径断言尝试次数，以及请求体 Seek 次数与响应体 Close 次数。
// Stream 在网络错误上不重试，是三种路径中唯一不同的行为。
func testRequestWithRetry(t *testing.T, key string, doFunc func(ctx context.Context, r *Request)) {
	type expected struct {
		attempts  int
		reqCount  *count
		respCount *count
	}

	tests := []struct {
		name          string
		verb          string
		body          io.Reader
		bodyBytes     []byte
		maxRetries    int
		serverReturns []responseErr

		// expectations differ based on whether it is 'Watch', 'Stream' or 'Do'
		expectations map[string]expected
	}{
		{
			name:       "server always returns retry-after response",
			verb:       "GET",
			bodyBytes:  []byte{},
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
			},
			expectations: map[string]expected{
				"Do": {
					attempts:  3,
					reqCount:  &count{closes: 0, seeks: make([]seek, 2)},
					respCount: &count{closes: 3, seeks: []seek{}},
				},
				"Watch": {
					attempts:  3,
					reqCount:  &count{closes: 0, seeks: make([]seek, 2)},
					respCount: &count{closes: 3, seeks: []seek{}},
				},
				"Stream": {
					attempts:  3,
					reqCount:  &count{closes: 0, seeks: make([]seek, 2)},
					respCount: &count{closes: 3, seeks: []seek{}},
				},
			},
		},
		{
			name:       "server always returns retryable error",
			verb:       "GET",
			bodyBytes:  []byte{},
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: nil, err: io.EOF},
				{response: nil, err: io.EOF},
				{response: nil, err: io.EOF},
			},
			expectations: map[string]expected{
				"Do": {
					attempts:  3,
					reqCount:  &count{closes: 0, seeks: make([]seek, 2)},
					respCount: &count{closes: 0, seeks: []seek{}},
				},
				"Watch": {
					attempts:  3,
					reqCount:  &count{closes: 0, seeks: make([]seek, 2)},
					respCount: &count{closes: 0, seeks: []seek{}},
				},
				// for Stream, we never retry on any error
				"Stream": {
					attempts:  1, // only the first attempt is expected
					reqCount:  &count{closes: 0, seeks: []seek{}},
					respCount: &count{closes: 0, seeks: []seek{}},
				},
			},
		},
		{
			name:       "server returns success on the final retry",
			verb:       "GET",
			bodyBytes:  []byte{},
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: io.EOF},
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			expectations: map[string]expected{
				"Do": {
					attempts:  3,
					reqCount:  &count{closes: 0, seeks: make([]seek, 2)},
					respCount: &count{closes: 2, seeks: []seek{}},
				},
				"Watch": {
					attempts: 3,
					reqCount: &count{closes: 0, seeks: make([]seek, 2)},
					// the Body of the successful response object will get closed by
					// StreamWatcher, so we need to take that into account.
					respCount: &count{closes: 2, seeks: []seek{}},
				},
				"Stream": {
					attempts:  2,
					reqCount:  &count{closes: 0, seeks: make([]seek, 1)},
					respCount: &count{closes: 1, seeks: []seek{}},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			respCountGot := newCount()
			responseRecorder := newReadTracker(respCountGot)
			var attempts int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				defer func() {
					attempts++
				}()

				resp := test.serverReturns[attempts].response
				if resp != nil {
					responseRecorder.delegated = io.NopCloser(bytes.NewReader([]byte{}))
					resp.Body = responseRecorder
				}
				return resp, test.serverReturns[attempts].err
			})

			req := &Request{
				verb:          test.verb,
				body:          test.body,
				bodyBytes:     test.bodyBytes,
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					Client: client,
				},
				backoff:    &noSleepBackOff{},
				maxRetries: test.maxRetries,
				retryFn:    defaultRequestRetryFn,
			}

			doFunc(context.Background(), req)

			expected, ok := test.expectations[key]
			if !ok {
				t.Fatalf("Wrong test setup - did not find expected for: %s", key)
			}
			if expected.attempts != attempts {
				t.Errorf("Expected retries: %d, but got: %d", expected.attempts, attempts)
			}

			if expected.respCount.closes != respCountGot.getCloseCount() {
				t.Errorf("Expected response body Close to be invoked %d times, but got: %d", expected.respCount.closes, respCountGot.getCloseCount())
			}
		})
	}
}

// retryTestKeyType 是测试专用 context key 的类型，避免与库内其它 key 冲突。
type retryTestKeyType int

// retryTestKey 用于标记“属于本测试的”指标调用；零值即有效值。
const retryTestKey retryTestKeyType = iota

// fake flowcontrol.RateLimiter so we can tap into the Wait method of the rate limiter.
// fake BackoffManager so we can tap into backoff calls
// fake metrics.ResultMetric to tap into the metric calls
// we use it to verify that RateLimiter, BackoffManager, and
// metric calls are invoked appropriately in right order.
//
// 中文说明：同时嵌入 RateLimiter、ResultMetric、RetryMetric 并实现 BackoffManager，
// 把各组件调用按发生顺序记入 invokeOrderGot，供断言精确的调用序列。
type withRateLimiterBackoffManagerAndMetrics struct {
	flowcontrol.RateLimiter
	metrics.ResultMetric
	calculateBackoffSeq int64
	calculateBackoffFn  func(i int64) time.Duration
	metrics.RetryMetric

	invokeOrderGot []string
	sleepsGot      []string
	statusCodesGot []string
}

// Wait 记录速率限制等待，立即返回。
func (lb *withRateLimiterBackoffManagerAndMetrics) Wait(ctx context.Context) error {
	lb.invokeOrderGot = append(lb.invokeOrderGot, "RateLimiter.Wait")
	return nil
}

// CalculateBackoffWithContext 按预设函数返回第 n 次退避时长，并记录调用。
func (lb *withRateLimiterBackoffManagerAndMetrics) CalculateBackoffWithContext(ctx context.Context, actualURL *url.URL) time.Duration {
	lb.invokeOrderGot = append(lb.invokeOrderGot, "BackoffManager.CalculateBackoff")

	waitFor := lb.calculateBackoffFn(lb.calculateBackoffSeq)
	lb.calculateBackoffSeq++
	return waitFor
}

// UpdateBackoffWithContext 记录调用，状态由测试外部控制。
func (lb *withRateLimiterBackoffManagerAndMetrics) UpdateBackoffWithContext(ctx context.Context, actualURL *url.URL, err error, responseCode int) {
	lb.invokeOrderGot = append(lb.invokeOrderGot, "BackoffManager.UpdateBackoff")
}

// SleepWithContext 记录睡眠时长而不真正等待。
func (lb *withRateLimiterBackoffManagerAndMetrics) SleepWithContext(ctx context.Context, d time.Duration) {
	lb.invokeOrderGot = append(lb.invokeOrderGot, "BackoffManager.Sleep")
	lb.sleepsGot = append(lb.sleepsGot, d.String())
}

// Increment 记录结果指标；仅统计带 retryTestKey 标记的 context，避免其它测试干扰。
func (lb *withRateLimiterBackoffManagerAndMetrics) Increment(ctx context.Context, code, _, _ string) {
	// we are interested in the request context that is marked by this test
	if marked, ok := ctx.Value(retryTestKey).(bool); ok && marked {
		lb.invokeOrderGot = append(lb.invokeOrderGot, "RequestResult.Increment")
		lb.statusCodesGot = append(lb.statusCodesGot, code)
	}
}

// IncrementRetry 记录重试指标；同样只统计带标记的 context。
func (lb *withRateLimiterBackoffManagerAndMetrics) IncrementRetry(ctx context.Context, code, _, _ string) {
	// we are interested in the request context that is marked by this test
	if marked, ok := ctx.Value(retryTestKey).(bool); ok && marked {
		lb.invokeOrderGot = append(lb.invokeOrderGot, "RequestRetry.IncrementRetry")
		lb.statusCodesGot = append(lb.statusCodesGot, code)
	}
}

// Do 记录一次“实际发送请求”事件，用于还原调用顺序。
func (lb *withRateLimiterBackoffManagerAndMetrics) Do() {
	lb.invokeOrderGot = append(lb.invokeOrderGot, "Client.Do")
}

// testRetryWithRateLimiterBackoffAndMetrics 断言一次 500+Retry-After 后成功的过程中，
// RateLimiter、Backoff、指标调用的顺序、睡眠时长与状态码。
//
// 中文说明：重试睡眠取 Retry-After 与 CalculateBackoff 的较大值，两个场景分别覆盖
// Retry-After 更大与退避更大。Watch 在首次发送前不调用 RateLimiter.Wait，因此期望
// 顺序少一项。测试临时替换全局 metrics 接口，依据 context key 过滤本测试的调用。
func testRetryWithRateLimiterBackoffAndMetrics(t *testing.T, key string, doFunc func(ctx context.Context, r *Request)) {
	type expected struct {
		attempts    int
		order       []string
		sleeps      []string
		statusCodes []string
	}

	// we define the expected order of how the client invokes the
	// rate limiter, backoff, and metrics methods.
	// scenario:
	//  - A: original request fails with a retryable response: (500, 'Retry-After: N')
	//  - B: retry 1: successful with a status code 200
	// so we have a total of 2 attempts
	invokeOrderWant := []string{
		// before we send the request to the server:
		// - we wait as dictated by the client rate lmiter
		// - we wait, as dictated by the backoff manager
		"RateLimiter.Wait",
		"BackoffManager.CalculateBackoff",
		"BackoffManager.Sleep",

		// A: first attempt for which the server sends a retryable response
		// status code: 500, Retry-Afer: N
		"Client.Do",

		// we got a response object, status code: 500, Retry-Afer: N
		//  - call metrics method with appropriate status code
		//  - update backoff parameters with the status code returned
		"RequestResult.Increment",
		"BackoffManager.UpdateBackoff",
		"BackoffManager.CalculateBackoff",
		// sleep for delay=max(BackoffManager.CalculateBackoff, Retry-After: N)
		"BackoffManager.Sleep",
		// wait as dictated by the client rate lmiter
		"RateLimiter.Wait",

		// B: 2nd attempt: retry, and this should return a status code=200
		"Client.Do",

		// it's a success, so do the following:
		// count the result metric, and since it's a retry,
		// count the retry metric, and then update backoff parameters.
		"RequestResult.Increment",
		"RequestRetry.IncrementRetry",
		"BackoffManager.UpdateBackoff",
	}
	statusCodesWant := []string{
		// first attempt (A): we count the result metric only
		"500",
		// final attempt (B): we count the result metric, and the retry metric
		"200", "200",
	}

	tests := []struct {
		name               string
		maxRetries         int
		serverReturns      []responseErr
		calculateBackoffFn func(i int64) time.Duration
		// expectations differ based on whether it is 'Watch', 'Stream' or 'Do'
		expectations map[string]expected
	}{
		{
			name:       "success after one retry, Retry-After: N > BackoffManager.CalculateBackoff",
			maxRetries: 1,
			serverReturns: []responseErr{
				{response: retryAfterResponseWithDelay("5"), err: nil},
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			// we simulate a sleep sequence of 0s, 1s, 2s, 3s, ...
			calculateBackoffFn: func(i int64) time.Duration { return time.Duration(i * int64(time.Second)) },
			expectations: map[string]expected{
				"Do": {
					attempts:    2,
					order:       invokeOrderWant,
					statusCodes: statusCodesWant,
					sleeps: []string{
						// initial backoff.Sleep before we send the request to the server for the first time
						"0s",
						// maximum of:
						//  - 'Retry-After: 5' response header from (A)
						//  - BackoffManager.CalculateBackoff (will return 1s)
						(5 * time.Second).String(),
					},
				},
				"Watch": {
					attempts: 2,
					// Watch does not do 'RateLimiter.Wait' before initially sending the request to the server
					order:       invokeOrderWant[1:],
					statusCodes: statusCodesWant,
					sleeps: []string{
						"0s",
						(5 * time.Second).String(),
					},
				},
				"Stream": {
					attempts:    2,
					order:       invokeOrderWant,
					statusCodes: statusCodesWant,
					sleeps: []string{
						"0s",
						(5 * time.Second).String(),
					},
				},
			},
		},
		{
			name:       "success after one retry, Retry-After: N < BackoffManager.CalculateBackoff",
			maxRetries: 1,
			serverReturns: []responseErr{
				{response: retryAfterResponseWithDelay("2"), err: nil},
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			// we simulate a sleep sequence of 0s, 4s, 8s, 16s, ...
			calculateBackoffFn: func(i int64) time.Duration { return time.Duration(i * int64(4*time.Second)) },
			expectations: map[string]expected{
				"Do": {
					attempts:    2,
					order:       invokeOrderWant,
					statusCodes: statusCodesWant,
					sleeps: []string{
						// initial backoff.Sleep before we send the request to the server for the first time
						"0s",
						// maximum of:
						//  - 'Retry-After: 2' response header from (A)
						//  - BackoffManager.CalculateBackoff (will return 4s)
						(4 * time.Second).String(),
					},
				},
				"Watch": {
					attempts: 2,
					// Watch does not do 'RateLimiter.Wait' before initially sending the request to the server
					order:       invokeOrderWant[1:],
					statusCodes: statusCodesWant,
					sleeps: []string{
						"0s",
						(4 * time.Second).String(),
					},
				},
				"Stream": {
					attempts:    2,
					order:       invokeOrderWant,
					statusCodes: statusCodesWant,
					sleeps: []string{
						"0s",
						(4 * time.Second).String(),
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			interceptor := &withRateLimiterBackoffManagerAndMetrics{
				RateLimiter:        flowcontrol.NewFakeAlwaysRateLimiter(),
				calculateBackoffFn: test.calculateBackoffFn,
			}

			// TODO: today this is the only site where a test overrides the
			//  default metric interfaces, in future if we other tests want
			//  to override as well, and we want tests to be able to run in
			//  parallel then we will need to provide a way for tests to
			//  register/deregister their own metric inerfaces.
			oldRequestResult := metrics.RequestResult
			oldRequestRetry := metrics.RequestRetry
			metrics.RequestResult = interceptor
			metrics.RequestRetry = interceptor
			defer func() {
				metrics.RequestResult = oldRequestResult
				metrics.RequestRetry = oldRequestRetry
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// we are changing metrics.RequestResult (a global state) in
			// this test, to avoid interference from other tests running in
			// parallel we need to associate a key to the context so we
			// can identify the metric calls associated with this test.
			ctx = context.WithValue(ctx, retryTestKey, true)

			var attempts int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				defer func() {
					attempts++
				}()

				interceptor.Do()
				resp := test.serverReturns[attempts].response
				if resp != nil {
					resp.Body = io.NopCloser(bytes.NewReader([]byte{}))
				}
				return resp, test.serverReturns[attempts].err
			})

			base, err := url.Parse("http://foo.bar")
			if err != nil {
				t.Fatalf("Wrong test setup - did not find expected for: %s", key)
			}
			req := &Request{
				verb:          "GET",
				bodyBytes:     []byte{},
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base:        base,
					Client:      client,
					rateLimiter: interceptor,
				},
				pathPrefix:  "/api/v1",
				rateLimiter: interceptor,
				backoff:     interceptor,
				maxRetries:  test.maxRetries,
				retryFn:     defaultRequestRetryFn,
			}

			doFunc(ctx, req)

			want, ok := test.expectations[key]
			if !ok {
				t.Fatalf("Wrong test setup - did not find expected for: %s", key)
			}
			if want.attempts != attempts {
				t.Errorf("%s: Expected retries: %d, but got: %d", key, want.attempts, attempts)
			}
			if !cmp.Equal(want.order, interceptor.invokeOrderGot) {
				t.Errorf("%s: Expected invoke order to match, diff: %s", key, cmp.Diff(want.order, interceptor.invokeOrderGot))
			}
			if !cmp.Equal(want.sleeps, interceptor.sleepsGot) {
				t.Errorf("%s: Expected sleep sequence to match, diff: %s", key, cmp.Diff(want.sleeps, interceptor.sleepsGot))
			}
			if !cmp.Equal(want.statusCodes, interceptor.statusCodesGot) {
				t.Errorf("%s: Expected status codes to match, diff: %s", key, cmp.Diff(want.statusCodes, interceptor.statusCodesGot))
			}
		})
	}
}

// retryInterceptor 包装 WithRetry，记录回调顺序后委托给真实实现。
type retryInterceptor struct {
	WithRetry
	invokeOrderGot []string
}

// IsNextRetry 记录调用并委托。
func (ri *retryInterceptor) IsNextRetry(ctx context.Context, restReq *Request, httpReq *http.Request, resp *http.Response, err error, f IsRetryableErrorFunc) bool {
	ri.invokeOrderGot = append(ri.invokeOrderGot, "WithRetry.IsNextRetry")
	return ri.WithRetry.IsNextRetry(ctx, restReq, httpReq, resp, err, f)
}

// Before 记录调用并委托。
func (ri *retryInterceptor) Before(ctx context.Context, request *Request) error {
	ri.invokeOrderGot = append(ri.invokeOrderGot, "WithRetry.Before")
	return ri.WithRetry.Before(ctx, request)
}

// After 记录调用并委托。
func (ri *retryInterceptor) After(ctx context.Context, request *Request, resp *http.Response, err error) {
	ri.invokeOrderGot = append(ri.invokeOrderGot, "WithRetry.After")
	ri.WithRetry.After(ctx, request, resp, err)
}

// Do 记录一次实际发送。
func (ri *retryInterceptor) Do() {
	ri.invokeOrderGot = append(ri.invokeOrderGot, "Client.Do")
}

// testWithRetryInvokeOrder 断言 WithRetry 的 Before/After/IsNextRetry 调用顺序。
//
// 中文说明：一次重试后成功应产生 Before→Do→After→IsNextRetry 的两轮序列；但 Watch 和
// Stream 在收到成功响应后立即返回，不再调用最后一次 IsNextRetry。
func testWithRetryInvokeOrder(t *testing.T, key string, doFunc func(ctx context.Context, r *Request)) {
	// we define the expected order of how the client
	// should invoke the retry interface
	// scenario:
	//  - A: original request fails with a retryable response: (500, 'Retry-After: 1')
	//  - B: retry 1: successful with a status code 200
	// so we have a total of 2 attempts
	defaultInvokeOrderWant := []string{
		// first attempt (A)
		"WithRetry.Before",
		"Client.Do",
		"WithRetry.After",
		// server returns a retryable response: (500, 'Retry-After: 1')
		// IsNextRetry is expected to return true
		"WithRetry.IsNextRetry",

		// second attempt (B) - retry 1: successful with a status code 200
		"WithRetry.Before",
		"Client.Do",
		"WithRetry.After",
		// success: IsNextRetry is expected to return false
		// Watch and Stream are an exception, they return as soon as the
		// server sends a status code of success.
		"WithRetry.IsNextRetry",
	}

	tests := []struct {
		name          string
		maxRetries    int
		serverReturns []responseErr
		// expectations differ based on whether it is 'Watch', 'Stream' or 'Do'
		expectations map[string][]string
	}{
		{
			name:       "success after one retry",
			maxRetries: 1,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			expectations: map[string][]string{
				"Do": defaultInvokeOrderWant,
				// Watch and Stream skip the final 'IsNextRetry' by returning
				// as soon as they see a success from the server.
				"Watch":  defaultInvokeOrderWant[0 : len(defaultInvokeOrderWant)-1],
				"Stream": defaultInvokeOrderWant[0 : len(defaultInvokeOrderWant)-1],
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			interceptor := &retryInterceptor{
				WithRetry: &withRetry{maxRetries: test.maxRetries},
			}

			var attempts int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				defer func() {
					attempts++
				}()

				interceptor.Do()
				resp := test.serverReturns[attempts].response
				if resp != nil {
					resp.Body = io.NopCloser(bytes.NewReader([]byte{}))
				}
				return resp, test.serverReturns[attempts].err
			})

			base, err := url.Parse("http://foo.bar")
			if err != nil {
				t.Fatalf("Wrong test setup - did not find expected for: %s", key)
			}
			req := &Request{
				verb:          "GET",
				bodyBytes:     []byte{},
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base:   base,
					Client: client,
				},
				pathPrefix:  "/api/v1",
				rateLimiter: flowcontrol.NewFakeAlwaysRateLimiter(),
				backoff:     &NoBackoff{},
				retryFn:     func(_ int) WithRetry { return interceptor },
			}

			doFunc(context.Background(), req)

			if attempts != 2 {
				t.Errorf("%s: Expected attempts: %d, but got: %d", key, 2, attempts)
			}
			invokeOrderWant, ok := test.expectations[key]
			if !ok {
				t.Fatalf("Wrong test setup - did not find expected for: %s", key)
			}
			if !cmp.Equal(invokeOrderWant, interceptor.invokeOrderGot) {
				t.Errorf("%s: Expected invoke order to match, diff: %s", key, cmp.Diff(invokeOrderWant, interceptor.invokeOrderGot))
			}
		})
	}
}

// testWithWrapPreviousError 验证重试耗尽后错误链的构造。
//
// 中文说明：最终错误仍是最后一次尝试的错误（可经 url.Error 还原），并额外附加
// “- error from a previous attempt: ...” 说明最近一次网络层错误。非重试错误直接返回，
// 不添加包装；成功后不产生错误。
func testWithWrapPreviousError(t *testing.T, doFunc func(ctx context.Context, r *Request) error) {
	var (
		containsFormatExpected = "- error from a previous attempt: %s"
		nonRetryableErr        = errors.New("non retryable error")
	)

	tests := []struct {
		name             string
		maxRetries       int
		serverReturns    []responseErr
		expectedErr      error
		wrapped          bool
		attemptsExpected int
		contains         string
	}{
		{
			name:       "success at first attempt",
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			attemptsExpected: 1,
			expectedErr:      nil,
		},
		{
			name:       "success after a series of retry-after from the server",
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			attemptsExpected: 3,
			expectedErr:      nil,
		},
		{
			name:       "success after a series of retryable errors",
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: nil, err: io.EOF},
				{response: nil, err: io.EOF},
				{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			},
			attemptsExpected: 3,
			expectedErr:      nil,
		},
		{
			name:       "request errors out with a non retryable error",
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: nil, err: nonRetryableErr},
			},
			attemptsExpected: 1,
			expectedErr:      nonRetryableErr,
		},
		{
			name:       "request times out after retries, but no previous error",
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: context.Canceled},
			},
			attemptsExpected: 3,
			expectedErr:      context.Canceled,
		},
		{
			name:       "request times out after retries, and we have a relevant previous error",
			maxRetries: 3,
			serverReturns: []responseErr{
				{response: nil, err: io.EOF},
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: context.Canceled},
			},
			attemptsExpected: 4,
			wrapped:          true,
			expectedErr:      context.Canceled,
			contains:         fmt.Sprintf(containsFormatExpected, io.EOF),
		},
		{
			name:       "interleaved retry-after responses with retryable errors",
			maxRetries: 8,
			serverReturns: []responseErr{
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: io.ErrUnexpectedEOF},
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: io.EOF},
				{response: retryAfterResponse(), err: nil},
				{response: retryAfterResponse(), err: nil},
				{response: nil, err: context.Canceled},
			},
			attemptsExpected: 9,
			wrapped:          true,
			expectedErr:      context.Canceled,
			contains:         fmt.Sprintf(containsFormatExpected, io.EOF),
		},
		{
			name:       "request errors out with a retryable error, followed by a non-retryable one",
			maxRetries: 3,
			serverReturns: []responseErr{
				{response: nil, err: io.EOF},
				{response: nil, err: nonRetryableErr},
			},
			attemptsExpected: 2,
			wrapped:          true,
			expectedErr:      nonRetryableErr,
			contains:         fmt.Sprintf(containsFormatExpected, io.EOF),
		},
		{
			name:       "use the most recent error",
			maxRetries: 2,
			serverReturns: []responseErr{
				{response: nil, err: io.ErrUnexpectedEOF},
				{response: nil, err: io.EOF},
				{response: nil, err: context.Canceled},
			},
			attemptsExpected: 3,
			wrapped:          true,
			expectedErr:      context.Canceled,
			contains:         fmt.Sprintf(containsFormatExpected, io.EOF),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var attempts int
			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				defer func() {
					attempts++
				}()

				resp := test.serverReturns[attempts].response
				if resp != nil {
					resp.Body = io.NopCloser(bytes.NewReader([]byte{}))
				}
				return resp, test.serverReturns[attempts].err
			})

			base, err := url.Parse("http://foo.bar")
			if err != nil {
				t.Fatalf("Failed to create new HTTP request - %v", err)
			}
			req := &Request{
				verb:          "GET",
				bodyBytes:     []byte{},
				contentConfig: defaultContentConfig(),
				c: &RESTClient{
					base:   base,
					Client: client,
				},
				pathPrefix:  "/api/v1",
				rateLimiter: flowcontrol.NewFakeAlwaysRateLimiter(),
				backoff:     &noSleepBackOff{},
				maxRetries:  test.maxRetries,
				retryFn:     defaultRequestRetryFn,
			}

			err = doFunc(context.Background(), req)
			if test.attemptsExpected != attempts {
				t.Errorf("Expected attempts: %d, but got: %d", test.attemptsExpected, attempts)
			}

			switch {
			case test.expectedErr == nil:
				if err != nil {
					t.Errorf("Expected a nil error, but got: %v", err)
					return
				}
			case test.expectedErr != nil:
				if !strings.Contains(err.Error(), test.contains) {
					t.Errorf("Expected error message to contain %q, but got: %v", test.contains, err)
				}

				urlErrGot, _ := err.(*url.Error)
				if test.wrapped {
					// we expect the url.Error from net/http to be wrapped by WrapPreviousError
					unwrapper, ok := err.(interface {
						Unwrap() error
					})
					if !ok {
						t.Errorf("Expected error to implement Unwrap method, but got: %v", err)
						return
					}
					urlErrGot, _ = unwrapper.Unwrap().(*url.Error)
				}
				// we always get a url.Error from net/http
				if urlErrGot == nil {
					t.Errorf("Expected error to be url.Error, but got: %v", err)
					return
				}

				errGot := urlErrGot.Unwrap()
				if test.expectedErr != errGot {
					t.Errorf("Expected error %v, but got: %v", test.expectedErr, errGot)
				}
			}
		})
	}
}

// TestReuseRequest 验证同一 RESTClient 的连续请求可以复用底层连接。
//
// 中文说明：服务端返回 RemoteAddr，两次请求拿到相同地址即表示复用了同一条连接；
// HTTP/1.1 与 HTTP/2 都应如此。
func TestReuseRequest(t *testing.T) {
	var tests = []struct {
		name        string
		enableHTTP2 bool
	}{
		{"HTTP1", false},
		{"HTTP2", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(r.RemoteAddr))
			}))
			ts.EnableHTTP2 = tt.enableHTTP2
			ts.StartTLS()
			defer ts.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			c := testRESTClient(t, ts)

			req1, err := c.Verb("GET").
				Prefix("foo").
				DoRaw(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			req2, err := c.Verb("GET").
				Prefix("foo").
				DoRaw(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if string(req1) != string(req2) {
				t.Fatalf("Expected %v to be equal to %v", string(req1), string(req2))
			}

		})
	}
}

// TestHTTP1DoNotReuseRequestAfterTimeout 验证超时请求对连接复用的影响。
//
// 中文说明：HTTP/1.1 在请求超时后不会复用旧连接（地址变化），HTTP/2 则继续复用同一
// 连接（地址不变）。服务端在 /hang 上阻塞直到测试结束。
func TestHTTP1DoNotReuseRequestAfterTimeout(t *testing.T) {
	var tests = []struct {
		name        string
		enableHTTP2 bool
	}{
		{"HTTP1", false},
		{"HTTP2", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			done := make(chan struct{})
			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("TEST Connected from %v on %v\n", r.RemoteAddr, r.URL.Path)
				if r.URL.Path == "/hang" {
					t.Logf("TEST hanging %v\n", r.RemoteAddr)
					<-done
				}
				w.Write([]byte(r.RemoteAddr))
			}))
			ts.EnableHTTP2 = tt.enableHTTP2
			ts.StartTLS()
			defer ts.Close()
			// close hanging connection before shutting down the http server
			defer close(done)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			transport, ok := ts.Client().Transport.(*http.Transport)
			if !ok {
				t.Fatalf("failed to assert *http.Transport")
			}

			config := &Config{
				Host:      ts.URL,
				Transport: utilnet.SetTransportDefaults(transport),
				Timeout:   1 * time.Second,
				// These fields are required to create a REST client.
				ContentConfig: ContentConfig{
					GroupVersion:         &schema.GroupVersion{},
					NegotiatedSerializer: &serializer.CodecFactory{},
				},
			}
			if !tt.enableHTTP2 {
				config.TLSClientConfig.NextProtos = []string{"http/1.1"}
			}
			c, err := RESTClientFor(config)
			if err != nil {
				t.Fatalf("failed to create REST client: %v", err)
			}
			req1, err := c.Verb("GET").
				Prefix("foo").
				DoRaw(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			_, err = c.Verb("GET").
				Prefix("/hang").
				DoRaw(ctx)
			if err == nil {
				t.Fatalf("Expected error")
			}

			req2, err := c.Verb("GET").
				Prefix("foo").
				DoRaw(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// http1 doesn't reuse the connection after it times
			if tt.enableHTTP2 != (string(req1) == string(req2)) {
				if tt.enableHTTP2 {
					t.Fatalf("Expected %v to be the same as %v", string(req1), string(req2))
				} else {
					t.Fatalf("Expected %v to be different to %v", string(req1), string(req2))
				}
			}
		})
	}
}

// TestTransportConcurrency 验证并发请求下响应与请求正确配对，且传输层无数据竞争。
//
// 中文：4 个 worker 消费 10 个请求，服务端把 echo 参数原样写回，逐个比对。
func TestTransportConcurrency(t *testing.T) {
	const numReqs = 10
	var tests = []struct {
		name        string
		enableHTTP2 bool
	}{
		{"HTTP1", false},
		{"HTTP2", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Logf("Connected from %v %v", r.RemoteAddr, r.URL)
				fmt.Fprintf(w, "%v", r.FormValue("echo"))
			}))
			ts.EnableHTTP2 = tt.enableHTTP2
			ts.StartTLS()
			defer ts.Close()
			var wg sync.WaitGroup

			wg.Add(numReqs)
			c := testRESTClient(t, ts)
			reqs := make(chan string)
			defer close(reqs)

			for i := 0; i < 4; i++ {
				go func() {
					for req := range reqs {
						res, err := c.Get().Param("echo", req).DoRaw(context.Background())
						if err != nil {
							t.Errorf("error on req %s: %v", req, err)
							wg.Done()
							continue
						}

						if string(res) != req {
							t.Errorf("body of req %s = %q; want %q", req, res, req)
						}

						wg.Done()
					}
				}()
			}
			for i := 0; i < numReqs; i++ {
				reqs <- fmt.Sprintf("request-%d", i)
			}
			wg.Wait()
		})
	}
}

// TestRetryableConditions 以“方法 × 动词 × 服务端返回”矩阵验证可重试判定。
//
// 中文说明：429/5xx 且带 Retry-After 时都重试；缺 Retry-After 则不重试；连接重置、
// EOF、GOAWAY 等在非 Stream 路径重试（Stream 不重试）；连接拒绝、超时、3xx、2xx
// 均不重试。POST 上的 EOF 不重试，因为请求体可能已部分发送。
func TestRetryableConditions(t *testing.T) {
	var (
		methods = map[string]func(ctx context.Context, r *Request){
			"Do": func(ctx context.Context, r *Request) {
				r.Do(ctx)
			},
			"DoRaw": func(ctx context.Context, r *Request) {
				r.DoRaw(ctx)
			},
			"Stream": func(ctx context.Context, r *Request) {
				r.Stream(ctx)
			},
			"Watch": func(ctx context.Context, r *Request) {
				w, err := r.Watch(ctx)
				if err == nil {
					// we need to wait here to avoid race condition.
					<-w.ResultChan()
				}
			},
		}

		alwaysRetry = map[string]bool{
			"Do":     true,
			"DoRaw":  true,
			"Watch":  true,
			"Stream": true,
		}

		neverRetry = map[string]bool{
			"Do":     false,
			"DoRaw":  false,
			"Watch":  false,
			"Stream": false,
		}

		alwaysRetryExceptStream = map[string]bool{
			"Do":     true,
			"DoRaw":  true,
			"Watch":  true,
			"Stream": false,
		}
	)

	tests := []struct {
		name             string
		verbs            []string
		serverReturns    responseErr
		retryExpectation map[string]bool
	}{
		// {429, Retry-After: N} - we expect retry
		{
			name:             "server returns {429, Retry-After}",
			verbs:            []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			serverReturns:    responseErr{response: retryAfterResponseWithCodeAndDelay(http.StatusTooManyRequests, "0"), err: nil},
			retryExpectation: alwaysRetry,
		},
		// {5xx, Retry-After: N} - we expect retry
		{
			name:             "server returns {503, Retry-After}",
			verbs:            []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			serverReturns:    responseErr{response: retryAfterResponseWithCodeAndDelay(http.StatusServiceUnavailable, "0"), err: nil},
			retryExpectation: alwaysRetry,
		},
		// 5xx, but Retry-After: N is missing - no retry is expected
		{
			name:             "server returns 5xx, but no Retry-After",
			verbs:            []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			serverReturns:    responseErr{response: &http.Response{StatusCode: http.StatusInternalServerError}, err: nil},
			retryExpectation: neverRetry,
		},
		// 429, but Retry-After: N is missing - no retry is expected
		{
			name:             "server returns 429 but no Retry-After",
			verbs:            []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			serverReturns:    responseErr{response: &http.Response{StatusCode: http.StatusTooManyRequests}, err: nil},
			retryExpectation: neverRetry,
		},
		// response is nil, but error is set
		{
			name:             "server returns connection reset error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: syscall.ECONNRESET},
			retryExpectation: alwaysRetryExceptStream,
		},
		{
			name:             "server returns EOF error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: io.EOF},
			retryExpectation: alwaysRetryExceptStream,
		},
		{
			name:             "server returns unexpected EOF error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: io.ErrUnexpectedEOF},
			retryExpectation: alwaysRetryExceptStream,
		},
		{
			name:             "server returns broken connection error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: errors.New("http: can't write HTTP request on broken connection")},
			retryExpectation: alwaysRetryExceptStream,
		},
		{
			name:             "server returns GOAWAY error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: errors.New("http2: server sent GOAWAY and closed the connection")},
			retryExpectation: alwaysRetryExceptStream,
		},
		{
			name:             "server returns connection reset by peer error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: errors.New("connection reset by peer")},
			retryExpectation: alwaysRetryExceptStream,
		},
		{
			name:             "server returns use of closed network connection error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: errors.New("use of closed network connection")},
			retryExpectation: alwaysRetryExceptStream,
		},
		// connection refused error never gets retried
		{
			name:             "server returns connection refused error",
			verbs:            []string{"GET"},
			serverReturns:    responseErr{response: nil, err: syscall.ECONNREFUSED},
			retryExpectation: neverRetry,
		},
		{
			name:             "server returns connection refused error",
			verbs:            []string{"POST"},
			serverReturns:    responseErr{response: nil, err: syscall.ECONNREFUSED},
			retryExpectation: neverRetry,
		},
		{
			name:          "server returns EOF error",
			verbs:         []string{"POST"},
			serverReturns: responseErr{response: nil, err: io.EOF},
			retryExpectation: map[string]bool{
				"Do":     false,
				"DoRaw":  false,
				"Watch":  true, // not applicable, Watch should always be GET only
				"Stream": false,
			},
		},
		// Timeout error gets retries by watch only
		{
			name:          "server returns net.Timeout() == true error",
			verbs:         []string{"GET"},
			serverReturns: responseErr{response: nil, err: &net.DNSError{IsTimeout: true}},
			retryExpectation: map[string]bool{
				"Do":     false,
				"DoRaw":  false,
				"Watch":  true,
				"Stream": false,
			},
		},
		{
			name:             "server returns OK, never retry",
			verbs:            []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			serverReturns:    responseErr{response: &http.Response{StatusCode: http.StatusOK}, err: nil},
			retryExpectation: neverRetry,
		},
		{
			name:             "server returns {3xx, Retry-After}",
			verbs:            []string{"GET", "POST", "PUT", "DELETE", "PATCH"},
			serverReturns:    responseErr{response: &http.Response{StatusCode: http.StatusMovedPermanently, Header: http.Header{"Retry-After": []string{"0"}}}, err: nil},
			retryExpectation: neverRetry,
		},
	}

	for _, test := range tests {
		for method, retryExpected := range test.retryExpectation {
			fn, ok := methods[method]
			if !ok {
				t.Fatalf("Wrong test setup, unknown method: %s", method)
			}

			for _, verb := range test.verbs {
				t.Run(fmt.Sprintf("%s/%s/%s", test.name, method, verb), func(t *testing.T) {
					var attemptsGot int
					client := clientForFunc(func(req *http.Request) (*http.Response, error) {
						attemptsGot++
						return test.serverReturns.response, test.serverReturns.err
					})

					u, _ := url.Parse("http://localhost:123" + "/apis")
					req := &Request{
						verb:          verb,
						contentConfig: defaultContentConfig(),
						c: &RESTClient{
							base:   u,
							Client: client,
						},
						backoff:    &noSleepBackOff{},
						maxRetries: 2,
						retryFn:    defaultRequestRetryFn,
					}

					fn(context.Background(), req)

					if retryExpected {
						if attemptsGot != 3 {
							t.Errorf("Expected attempt count: %d, but got: %d", 3, attemptsGot)
						}
						return
					}
					// we don't expect retry, so we should see the first attempt only.
					if attemptsGot > 1 {
						t.Errorf("Expected no retry, but got %d attempts", attemptsGot)
					}
				})
			}
		}
	}
}

// TestRequestConcurrencyWithRetry 验证同一 Request 被并发调用时，每次调用独立执行
// 完整重试，不共享重试计数或 body 状态。
//
// 中文：20 个并发、每次最多 10 次尝试，期望总尝试数为 20×10。
func TestRequestConcurrencyWithRetry(t *testing.T) {
	var attempts int32
	client := clientForFunc(func(req *http.Request) (*http.Response, error) {
		defer func() {
			atomic.AddInt32(&attempts, 1)
		}()

		// always send a retry-after response
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Retry-After": []string{"1"}},
		}, nil
	})

	req := &Request{
		verb:          "POST",
		contentConfig: defaultContentConfig(),
		c: &RESTClient{
			Client: client,
		},
		backoff:    &noSleepBackOff{},
		maxRetries: 9, // 10 attempts in total, including the first
		retryFn:    defaultRequestRetryFn,
	}

	concurrency := 20
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	startCh := make(chan struct{})
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-startCh
			req.Do(context.Background())
		}()
	}

	close(startCh)
	wg.Wait()

	// we expect (concurrency*req.maxRetries+1) attempts to be recorded
	expected := concurrency * (req.maxRetries + 1)
	if atomic.LoadInt32(&attempts) != int32(expected) {
		t.Errorf("Expected attempts: %d, but got: %d", expected, attempts)
	}
}

// TestRequestLogging 验证按 verbosity 记录请求/响应正文及 warning。
//
// 中文说明：v<8 不记录正文；v>=8 记录请求与响应 body，超过 1024 字符截断；io.Reader
// 类型的 body 无法重读，因此只记录响应。code 299 的 Warning 通过 klog 输出，其它
// code 忽略。比较前剥离每行固定长度的时间戳前缀与 warnings.go 行号。
func TestRequestLogging(t *testing.T) {
	testcases := map[string]struct {
		v              int
		body           any
		response       *http.Response
		expectedOutput string
	}{
		"no-output": {
			v:    7,
			body: []byte("ping"),
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("pong")),
			},
		},
		"output": {
			v:    8,
			body: []byte("ping"),
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("pong")),
			},
			expectedOutput: `<location>] "Request Body" logger="TestLogger" body="ping"
<location>] "Response Body" logger="TestLogger" body="pong"
`,
		},
		"io-reader": {
			v:    8,
			body: strings.NewReader("ping"),
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("pong")),
			},
			// Cannot log the request body!
			expectedOutput: `<location>] "Response Body" logger="TestLogger" body="pong"
`,
		},
		"truncate": {
			v:    8,
			body: []byte(strings.Repeat("a", 2000)),
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("pong")),
			},
			expectedOutput: fmt.Sprintf(`<location>] "Request Body" logger="TestLogger" body="%s [truncated 976 chars]"
<location>] "Response Body" logger="TestLogger" body="pong"
`, strings.Repeat("a", 1024)),
		},
		"warnings": {
			v:    8,
			body: []byte("ping"),
			response: &http.Response{
				StatusCode: http.StatusOK,
				Header: http.Header{
					"Warning": []string{
						`299 request-test "warning 1"`,
						`299 request-test-2 "warning 2"`,
						`300 request-test-3 "ignore code 300"`,
					},
				},
				Body: io.NopCloser(strings.NewReader("pong")),
			},
			expectedOutput: `<location>] "Request Body" logger="TestLogger" body="ping"
<location>] "Response Body" logger="TestLogger" body="pong"
warnings.go] "Warning: warning 1" logger="TestLogger"
warnings.go] "Warning: warning 2" logger="TestLogger"
`,
		},
	}

	for name, tc := range testcases {
		//nolint:logcheck // Intentionally testing with plain klog here.
		t.Run(name, func(t *testing.T) {
			state := klog.CaptureState()
			defer state.Restore()

			var buffer bytes.Buffer
			klog.SetOutput(&buffer)
			klog.LogToStderr(false)
			var fs flag.FlagSet
			klog.InitFlags(&fs)
			require.NoError(t, fs.Set("v", fmt.Sprintf("%d", tc.v)), "set verbosity")
			require.NoError(t, fs.Set("one_output", "true"), "set one_output")

			client := clientForFunc(func(req *http.Request) (*http.Response, error) {
				return tc.response, nil
			})

			req := NewRequestWithClient(nil, "", defaultContentConfig(), client).
				Body(tc.body)

			logger := klog.Background()
			logger = klog.LoggerWithName(logger, "TestLogger")
			ctx := klog.NewContext(context.Background(), logger)

			_, file, line, _ := goruntime.Caller(0)
			result := req.Do(ctx)
			require.NoError(t, result.Error(), "request.Do")

			// Compare log output:
			// - strip date/time/pid from each line (fixed length header)
			// - replace <location> with the actual call location
			// - strip line number from warnings.go (might change)
			state.Restore()
			expectedOutput := strings.ReplaceAll(tc.expectedOutput, "<location>", fmt.Sprintf("%s:%d", path.Base(file), line+1))
			actualOutput := buffer.String()
			actualOutput = regexp.MustCompile(`(?m)^.{30}`).ReplaceAllString(actualOutput, "")
			actualOutput = regexp.MustCompile(`(?m)^warnings\.go:\d+`).ReplaceAllString(actualOutput, "warnings.go")
			assert.Equal(t, expectedOutput, actualOutput)
		})
	}
}

// TestRequestWarningHandler 验证 WarningHandler 与 WarningHandlerWithContext 的设置语义。
//
// 中文说明：旧接口会被适配为带 context 的实现；带 context 的接口直接保存；传入 nil
// 会清除已设置的处理器。所有设置方法都返回自身以支持链式调用。
func TestRequestWarningHandler(t *testing.T) {
	t.Run("no-context", func(t *testing.T) {
		request := &Request{}
		handler := &fakeWarningHandlerWithLogging{}
		//nolint:logcheck
		assert.Equal(t, request, request.WarningHandler(handler))
		assert.NotNil(t, request.warningHandler)
		request.warningHandler.HandleWarningHeaderWithContext(context.Background(), 0, "", "message")
		assert.Equal(t, []string{"message"}, handler.messages)
	})

	t.Run("with-context", func(t *testing.T) {
		request := &Request{}
		handler := &fakeWarningHandlerWithContext{}
		assert.Equal(t, request, request.WarningHandlerWithContext(handler))
		assert.Equal(t, request.warningHandler, handler)
	})

	t.Run("nil-no-context", func(t *testing.T) {
		request := &Request{
			warningHandler: &fakeWarningHandlerWithContext{},
		}
		//nolint:logcheck
		assert.Equal(t, request, request.WarningHandler(nil))
		assert.Nil(t, request.warningHandler)
	})

	t.Run("nil-with-context", func(t *testing.T) {
		request := &Request{
			warningHandler: &fakeWarningHandlerWithContext{},
		}
		assert.Equal(t, request, request.WarningHandlerWithContext(nil))
		assert.Nil(t, request.warningHandler)
	})
}

// TestTryThrottleWithInfoContextDeadline 验证 context 已取消时节流立即返回
// context.Canceled，且错误信息不额外包含“client rate limiter”之类的噪声。
func TestTryThrottleWithInfoContextDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := &Request{
		rateLimiter: flowcontrol.NewTokenBucketRateLimiter(1, 1),
		c:           &RESTClient{base: &url.URL{Path: "/"}},
	}
	err := r.tryThrottleWithInfo(ctx, "")
	require.ErrorIs(t, err, context.Canceled)
	assert.NotContains(t, err.Error(), "client rate limiter")
}
