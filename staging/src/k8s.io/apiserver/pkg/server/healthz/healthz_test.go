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

package healthz

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestInstallHandler 验证默认 /healthz 端点的契约：200、text/plain、正文 "ok"。
// 这是探针与监控依赖的稳定接口，字段变化会影响外部系统。
func TestInstallHandler(t *testing.T) {
	mux := http.NewServeMux()
	InstallHandler(mux)
	req, err := http.NewRequest("GET", "http://example.com/healthz", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected %v, got %v", http.StatusOK, w.Code)
	}
	c := w.Header().Get("Content-Type")
	if c != "text/plain; charset=utf-8" {
		t.Errorf("expected %v, got %v", "text/plain", c)
	}
	if w.Body.String() != "ok" {
		t.Errorf("expected %v, got %v", "ok", w.Body.String())
	}
}

// TestInstallPathHandler 验证同一 mux 上可挂载多个自定义路径（如 /healthz/test 与
// /healthz/ready），互不干扰。
func TestInstallPathHandler(t *testing.T) {
	mux := http.NewServeMux()
	InstallPathHandler(mux, "/healthz/test")
	InstallPathHandler(mux, "/healthz/ready")
	req, err := http.NewRequest("GET", "http://example.com/healthz/test", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected %v, got %v", http.StatusOK, w.Code)
	}
	c := w.Header().Get("Content-Type")
	if c != "text/plain; charset=utf-8" {
		t.Errorf("expected %v, got %v", "text/plain", c)
	}
	if w.Body.String() != "ok" {
		t.Errorf("expected %v, got %v", "ok", w.Body.String())
	}

	req, err = http.NewRequest("GET", "http://example.com/healthz/ready", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected %v, got %v", http.StatusOK, w.Code)
	}
	c = w.Header().Get("Content-Type")
	if c != "text/plain; charset=utf-8" {
		t.Errorf("expected %v, got %v", "text/plain", c)
	}
	if w.Body.String() != "ok" {
		t.Errorf("expected %v, got %v", "ok", w.Body.String())
	}

}

// testMultipleChecks 用同一套用例分别覆盖 /healthz 与自定义路径。
//
// 用例覆盖 verbose、exclude、子路径 /ping 与 /bad 的组合，锁定了响应正文格式：
// "[+]name ok"、"[+]name excluded: ok"、"[-]name failed: reason withheld"。
// 其中 "reason withheld" 是安全约束——失败原因不能回显给未授权调用方。
func testMultipleChecks(path, name string, t *testing.T) {
	tests := []struct {
		path             string
		expectedResponse string
		expectedStatus   int
		addBadCheck      bool
	}{
		{"?verbose", fmt.Sprintf("[+]ping ok\n%s check passed\n", name), http.StatusOK, false},
		{"?exclude=dontexist", "ok", http.StatusOK, false},
		{"?exclude=bad", "ok", http.StatusOK, true},
		{"?verbose=true&exclude=bad", fmt.Sprintf("[+]ping ok\n[+]bad excluded: ok\n%s check passed\n", name), http.StatusOK, true},
		{"?verbose=true&exclude=dontexist", fmt.Sprintf("[+]ping ok\nwarn: some health checks cannot be excluded: no matches for \"dontexist\"\n%s check passed\n", name), http.StatusOK, false},
		{"/ping", "ok", http.StatusOK, false},
		{"", "ok", http.StatusOK, false},
		{"?verbose", fmt.Sprintf("[+]ping ok\n[-]bad failed: reason withheld\n%s check failed\n", name), http.StatusInternalServerError, true},
		{"/ping", "ok", http.StatusOK, true},
		{"/bad", "internal server error: this will fail\n", http.StatusInternalServerError, true},
		{"", fmt.Sprintf("[+]ping ok\n[-]bad failed: reason withheld\n%s check failed\n", name), http.StatusInternalServerError, true},
	}

	for i, test := range tests {
		mux := http.NewServeMux()
		checks := []HealthChecker{PingHealthz}
		if test.addBadCheck {
			checks = append(checks, NamedCheck("bad", func(_ *http.Request) error {
				return errors.New("this will fail")
			}))
		}
		if path == "" {
			InstallHandler(mux, checks...)
			path = "/healthz"
		} else {
			InstallPathHandler(mux, path, checks...)
		}
		req, err := http.NewRequest("GET", fmt.Sprintf("http://example.com%s%v", path, test.path), nil)
		if err != nil {
			t.Fatalf("case[%d] Unexpected error: %v", i, err)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != test.expectedStatus {
			t.Errorf("case[%d] Expected: %v, got: %v", i, test.expectedStatus, w.Code)
		}
		c := w.Header().Get("Content-Type")
		if c != "text/plain; charset=utf-8" {
			t.Errorf("case[%d] Expected: %v, got: %v", i, "text/plain", c)
		}
		if w.Body.String() != test.expectedResponse {
			t.Errorf("case[%d] Expected:\n%v\ngot:\n%v\n", i, test.expectedResponse, w.Body.String())
		}
	}
}

// TestMultipleChecks 以默认 /healthz 路径运行通用检查用例。
func TestMultipleChecks(t *testing.T) {
	testMultipleChecks("", "healthz", t)
}

// TestMultiplePathChecks 以自定义 /ready 路径运行同一套用例，确认路径不影响语义。
func TestMultiplePathChecks(t *testing.T) {
	testMultipleChecks("/ready", "ready", t)
}

// TestCheckerNames 验证 checkerNames 保持注册顺序：
// verbose 输出依赖顺序与注册顺序一致，便于对照配置排查。
func TestCheckerNames(t *testing.T) {
	n1 := "n1"
	n2 := "n2"
	c1 := &healthzCheck{name: n1}
	c2 := &healthzCheck{name: n2}

	testCases := []struct {
		desc string
		have []HealthChecker
		want []string
	}{
		{"no checker", []HealthChecker{}, []string{}},
		{"one checker", []HealthChecker{c1}, []string{n1}},
		{"other checker", []HealthChecker{c2}, []string{n2}},
		{"checker order", []HealthChecker{c1, c2}, []string{n1, n2}},
		{"different checker order", []HealthChecker{c2, c1}, []string{n2, n1}},
	}

	for _, tc := range testCases {
		result := checkerNames(tc.have...)
		t.Run(tc.desc, func(t *testing.T) {
			if !reflect.DeepEqual(tc.want, result) {
				t.Errorf("want %#v, got %#v", tc.want, result)
			}
		})
	}
}

// TestFormatQuoted 验证名称列表被格式化为逗号分隔的带引号字符串（用于错误提示）。
func TestFormatQuoted(t *testing.T) {
	n1 := "n1"
	n2 := "n2"
	testCases := []struct {
		desc     string
		names    []string
		expected string
	}{
		{"empty", []string{}, ""},
		{"single name", []string{n1}, "\"n1\""},
		{"two names", []string{n1, n2}, "\"n1\",\"n2\""},
		{"two names, reverse order", []string{n2, n1}, "\"n2\",\"n1\""},
	}
	for _, tc := range testCases {
		result := formatQuoted(tc.names...)
		t.Run(tc.desc, func(t *testing.T) {
			if result != tc.expected {
				t.Errorf("expected %#v, got %#v", tc.expected, result)
			}
		})
	}
}

// TestGetExcludedChecks 验证从查询串解析 exclude 参数：无参数为空集，
// 重复出现时累加，而不是取最后一个。
func TestGetExcludedChecks(t *testing.T) {
	tests := []struct {
		name string
		r    *http.Request
		want sets.String
	}{
		{"Should have no excluded health checks",
			createGetRequestWithUrl("/healthz?verbose=true"),
			sets.NewString(),
		},
		{"Should extract out the ping health check",
			createGetRequestWithUrl("/healthz?exclude=ping"),
			sets.NewString("ping"),
		},
		{"Should extract out ping and log health check",
			createGetRequestWithUrl("/healthz?exclude=ping&exclude=log"),
			sets.NewString("ping", "log"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getExcludedChecks(tt.r); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getExcludedChecks() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMetrics 验证健康检查端点也会被指标中间件计数，且子资源标签使用路径本身。
// 先 Reset 再断言，避免其他测试残留的计数干扰精确匹配。
func TestMetrics(t *testing.T) {
	mux := http.NewServeMux()
	InstallHandler(mux)
	InstallLivezHandler(mux)
	InstallReadyzHandler(mux)
	metrics.Register()
	metrics.Reset()

	paths := []string{"/healthz", "/livez", "/readyz"}
	for _, path := range paths {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://example.com%s", path), nil)
		if err != nil {
			t.Errorf("%v", err)
		}
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}

	expected := strings.NewReader(`
        # HELP apiserver_request_total [STABLE] Counter of apiserver requests broken out for each verb, dry run value, group, version, resource, scope, component, and HTTP response code.
        # TYPE apiserver_request_total counter
        apiserver_request_total{code="200",component="",dry_run="",group="",resource="",scope="",subresource="/healthz",verb="GET",version=""} 1
        apiserver_request_total{code="200",component="",dry_run="",group="",resource="",scope="",subresource="/livez",verb="GET",version=""} 1
        apiserver_request_total{code="200",component="",dry_run="",group="",resource="",scope="",subresource="/readyz",verb="GET",version=""} 1
`)
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, expected, "apiserver_request_total"); err != nil {
		t.Error(err)
	}
}

// createGetRequestWithUrl 构造仅含 URL 的 GET 请求，用于单元测试查询串解析。
func createGetRequestWithUrl(rawUrlString string) *http.Request {
	url, _ := url.Parse(rawUrlString)
	return &http.Request{
		Method: http.MethodGet,
		Proto:  "HTTP/1.1",
		URL:    url,
	}
}

// TestInformerSyncHealthChecker 验证 informer 同步检查器：全部启动则健康，
// 任一未启动则报错（用于阻止流量在缓存未热时进入）。
func TestInformerSyncHealthChecker(t *testing.T) {
	t.Run("test that check returns nil when all informers are started", func(t *testing.T) {
		healthChecker := NewInformerSyncHealthz(cacheSyncWaiterStub{
			startedByInformerType: map[reflect.Type]bool{
				reflect.TypeOf(corev1.Pod{}): true,
			},
		})

		err := healthChecker.Check(nil)
		if err != nil {
			t.Errorf("Got %v, expected no error", err)
		}
	})

	t.Run("test that check returns err when there is not started informer", func(t *testing.T) {
		healthChecker := NewInformerSyncHealthz(cacheSyncWaiterStub{
			startedByInformerType: map[reflect.Type]bool{
				reflect.TypeOf(corev1.Pod{}):     true,
				reflect.TypeOf(corev1.Service{}): false,
				reflect.TypeOf(corev1.Node{}):    true,
			},
		})

		err := healthChecker.Check(nil)
		if err == nil {
			t.Errorf("expected error, got: %v", err)
		}
	})
}

// cacheSyncWaiterStub 用固定的 map 冒充 cache.WaitForCacheSync，便于构造同步状态。
type cacheSyncWaiterStub struct {
	startedByInformerType map[reflect.Type]bool
}

// WaitForCacheSync is a stub implementation of the corresponding func
// that simply returns the value passed during stub initialization.
func (s cacheSyncWaiterStub) WaitForCacheSync(_ <-chan struct{}) map[reflect.Type]bool {
	return s.startedByInformerType
}

// TestInstallPathHandlerWithHealthyFunc 验证"曾经健康"回调只触发一次。
//
// 场景：通道未关闭时检查失败；关闭后成功并调用回调；再次请求成功但回调不重复调用。
// 这保证了上层只在该路径首次转为健康时执行一次性初始化逻辑。
func TestInstallPathHandlerWithHealthyFunc(t *testing.T) {
	mux := http.NewServeMux()
	readyzCh := make(chan struct{})

	hasBeenReadyCounter := 0
	hasBeenReadyFn := func() {
		hasBeenReadyCounter++
	}
	InstallPathHandlerWithHealthyFunc(mux, "/readyz", hasBeenReadyFn, readyOnChanClose{readyzCh})

	// scenario 1: expect the check to fail since the channel hasn't been closed
	req, err := http.NewRequest("GET", fmt.Sprintf("http://example.com%s", "/readyz"), nil)
	if err != nil {
		t.Errorf("%v", err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusInternalServerError {
		t.Errorf("scenario 1: unexpected status code returned, expected %d, got %d", http.StatusInternalServerError, rr.Code)
	}

	// scenario 2: close the channel that will cause the readyz checker to report success,
	//             verify that hasBeenReadyFn was called
	close(readyzCh)
	rr = httptest.NewRecorder()
	req = req.Clone(context.TODO())
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("scenario 2: unexpected status code returned, expected %d, got %d", http.StatusOK, rr.Code)
	}
	if hasBeenReadyCounter != 1 {
		t.Errorf("scenario 2: unexpected value of hasBeenReadyCounter, expected 1, got %d", hasBeenReadyCounter)
	}

	// scenario 3: checks if hasBeenReadyFn hasn't been called again.
	rr = httptest.NewRecorder()
	req = req.Clone(context.TODO())
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("scenario 3: unexpected status code returned, expected %d, got %d", http.StatusOK, rr.Code)
	}
	if hasBeenReadyCounter != 1 {
		t.Errorf("scenario 3: unexpected value of hasBeenReadyCounter, expected 1, got %d", hasBeenReadyCounter)
	}
}

// readyOnChanClose 是"通道关闭即健康"的检查器，用于模拟启动完成的信号。
type readyOnChanClose struct {
	ch <-chan struct{}
}

func (readyOnChanClose) Name() string {
	return "readyOnChanClose"
}

func (c readyOnChanClose) Check(_ *http.Request) error {
	select {
	case <-c.ch:
		return nil
	default:
	}
	return fmt.Errorf("the provided channel hasn't been closed")
}
