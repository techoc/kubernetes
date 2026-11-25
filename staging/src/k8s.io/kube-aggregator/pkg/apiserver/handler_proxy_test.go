/*
Copyright 2016 The Kubernetes Authors.

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

package apiserver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-cmp/cmp"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/net/websocket"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/proxy"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/filters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/egressselector"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	apiserverproxyutil "k8s.io/apiserver/pkg/util/proxy"
	"k8s.io/client-go/transport"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	apiregistration "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/utils/ptr"
)

// 记录被代理请求的关键信息，让测试能断言“请求确实到达后端且头被正确改写”。
type targetHTTPHandler struct {
	called  bool
	headers map[string][]string
	path    string
	host    string
}

func (d *targetHTTPHandler) Reset() {
	d.path = ""
	d.called = false
	d.headers = nil
	d.host = ""
}

func (d *targetHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.path = r.URL.Path
	d.called = true
	d.headers = r.Header
	d.host = r.Host
	w.WriteHeader(http.StatusOK)
}

// 在请求上下文里注入 user 和 RequestInfo，模拟真实 apiserver 前置过滤器已经做过的工作，
// 这样 handler 里的鉴权与路由逻辑才能被走通。
func contextHandler(handler http.Handler, user user.Info) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		if user != nil {
			ctx = genericapirequest.WithUser(ctx, user)
		}
		resolver := &genericapirequest.RequestInfoFactory{
			APIPrefixes:          sets.NewString("api", "apis"),
			GrouplessAPIPrefixes: sets.NewString("api"),
		}
		info, err := resolver.NewRequestInfo(req)
		if err == nil {
			ctx = genericapirequest.WithRequestInfo(ctx, info)
		}
		req = req.WithContext(ctx)
		handler.ServeHTTP(w, req)
	})
}

// 绕过真实的 service 解析，直接返回指定的目标地址或错误，
// 让代理测试无需 Kubernetes 服务环境就能覆盖解析失败分支。
type mockedRouter struct {
	destinationHost string
	err             error
}

func (r *mockedRouter) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	return &url.URL{Scheme: "https", Host: r.destinationHost}, r.err
}

func emptyCert() []byte {
	return []byte{}
}

// 表驱动覆盖代理的主路径与异常路径：无目标、无用户、TLS 校验开关两种组合、
// 服务不可用、解析失败、证书不匹配，以及特性开关对 X-Remote-Uid 头的影响。
func TestProxyHandler(t *testing.T) {
	tests := map[string]struct {
		user       user.Info
		path       string
		apiService *apiregistration.APIService

		serviceResolver        ServiceResolver
		serviceCertOverride    []byte
		increaseSANWarnCounter bool

		expectedStatusCode int
		expectedBody       string
		expectedCalled     bool
		expectedHeaders    map[string][]string

		remoteRequestHeaderUIDFeature bool
	}{
		"no target": {
			expectedStatusCode:            http.StatusNotFound,
			remoteRequestHeaderUIDFeature: true,
		},
		"no user": {
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service: &apiregistration.ServiceReference{Port: ptr.To[int32](443)},
					Group:   "foo",
					Version: "v1",
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			expectedStatusCode:            http.StatusInternalServerError,
			expectedBody:                  "missing user",
			remoteRequestHeaderUIDFeature: true,
		},
		"[-RemoteRequestHeaderUID] proxy with user, insecure": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:               &apiregistration.ServiceReference{Port: ptr.To[int32](443)},
					Group:                 "foo",
					Version:               "v1",
					InsecureSkipTLSVerify: true,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			remoteRequestHeaderUIDFeature: false,
			expectedStatusCode:            http.StatusOK,
			expectedCalled:                true,
			expectedHeaders: map[string][]string{
				"X-Forwarded-Proto": {"https"},
				"X-Forwarded-Uri":   {"/request/path"},
				"X-Forwarded-For":   {"127.0.0.1"},
				"X-Remote-User":     {"username"},
				"User-Agent":        {"Go-http-client/1.1"},
				"Accept-Encoding":   {"gzip"},
				"X-Remote-Group":    {"one", "two"},
			},
		},
		"proxy with user, insecure": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:               &apiregistration.ServiceReference{Port: ptr.To[int32](443)},
					Group:                 "foo",
					Version:               "v1",
					InsecureSkipTLSVerify: true,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			remoteRequestHeaderUIDFeature: true,
			expectedStatusCode:            http.StatusOK,
			expectedCalled:                true,
			expectedHeaders: map[string][]string{
				"X-Forwarded-Proto": {"https"},
				"X-Forwarded-Uri":   {"/request/path"},
				"X-Forwarded-For":   {"127.0.0.1"},
				"X-Remote-User":     {"username"},
				"X-Remote-Uid":      {"6b60d791-1af9-4513-92e5-e4252a1e0a78"},
				"User-Agent":        {"Go-http-client/1.1"},
				"Accept-Encoding":   {"gzip"},
				"X-Remote-Group":    {"one", "two"},
			},
		},
		"[-RemoteRequestHeaderUID] proxy with user, cabundle": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
					Group:    "foo",
					Version:  "v1",
					CABundle: testCACrt,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			remoteRequestHeaderUIDFeature: false,
			expectedStatusCode:            http.StatusOK,
			expectedCalled:                true,
			expectedHeaders: map[string][]string{
				"X-Forwarded-Proto": {"https"},
				"X-Forwarded-Uri":   {"/request/path"},
				"X-Forwarded-For":   {"127.0.0.1"},
				"X-Remote-User":     {"username"},
				"User-Agent":        {"Go-http-client/1.1"},
				"Accept-Encoding":   {"gzip"},
				"X-Remote-Group":    {"one", "two"},
			},
		},
		"proxy with user, cabundle": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
					Group:    "foo",
					Version:  "v1",
					CABundle: testCACrt,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			remoteRequestHeaderUIDFeature: true,
			expectedStatusCode:            http.StatusOK,
			expectedCalled:                true,
			expectedHeaders: map[string][]string{
				"X-Forwarded-Proto": {"https"},
				"X-Forwarded-Uri":   {"/request/path"},
				"X-Forwarded-For":   {"127.0.0.1"},
				"X-Remote-User":     {"username"},
				"X-Remote-Uid":      {"6b60d791-1af9-4513-92e5-e4252a1e0a78"},
				"User-Agent":        {"Go-http-client/1.1"},
				"Accept-Encoding":   {"gzip"},
				"X-Remote-Group":    {"one", "two"},
			},
		},
		"service unavailable": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
					Group:    "foo",
					Version:  "v1",
					CABundle: testCACrt,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionFalse},
					},
				},
			},
			remoteRequestHeaderUIDFeature: true,
			expectedStatusCode:            http.StatusServiceUnavailable,
		},
		"service unresolveable": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path:            "/request/path",
			serviceResolver: &mockedRouter{err: fmt.Errorf("unresolveable")},
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:  &apiregistration.ServiceReference{Name: "bad-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
					Group:    "foo",
					Version:  "v1",
					CABundle: testCACrt,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			remoteRequestHeaderUIDFeature: true,
			expectedStatusCode:            http.StatusServiceUnavailable,
		},
		"fail on bad serving cert": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service: &apiregistration.ServiceReference{Port: ptr.To[int32](443)},
					Group:   "foo",
					Version: "v1",
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			remoteRequestHeaderUIDFeature: true,
			expectedStatusCode:            http.StatusServiceUnavailable,
		},
		"fail on bad serving cert w/o SAN and increase SAN error counter metrics": {
			user: &user.DefaultInfo{
				Name:   "username",
				UID:    "6b60d791-1af9-4513-92e5-e4252a1e0a78",
				Groups: []string{"one", "two"},
			},
			path: "/request/path",
			apiService: &apiregistration.APIService{
				ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
				Spec: apiregistration.APIServiceSpec{
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
					Group:    "foo",
					Version:  "v1",
					CABundle: testCACrt,
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			serviceCertOverride:           svcCrtNoSAN,
			increaseSANWarnCounter:        true,
			remoteRequestHeaderUIDFeature: true,
			expectedStatusCode:            http.StatusServiceUnavailable,
		},
	}

	target := &targetHTTPHandler{}
	for name, tc := range tests {
		target.Reset()
		// 每个用例都重置默认 registry，避免上一个用例的计数器影响下一个用例的断言。
		legacyregistry.Reset()

		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RemoteRequestHeaderUID, tc.remoteRequestHeaderUIDFeature)

			targetServer := httptest.NewUnstartedServer(target)
			serviceCert := tc.serviceCertOverride
			if serviceCert == nil {
				serviceCert = svcCrt
			}
			if cert, err := tls.X509KeyPair(serviceCert, svcKey); err != nil {
				t.Fatal(err)
			} else {
				targetServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
			}
			targetServer.StartTLS()
			defer targetServer.Close()

			serviceResolver := tc.serviceResolver
			if serviceResolver == nil {
				serviceResolver = &mockedRouter{destinationHost: targetServer.Listener.Addr().String()}
			}
			handler := &proxyHandler{
				localDelegate:              http.NewServeMux(),
				serviceResolver:            serviceResolver,
				proxyCurrentCertKeyContent: func() ([]byte, []byte) { return emptyCert(), emptyCert() },
			}
			server := httptest.NewServer(contextHandler(handler, tc.user))
			defer server.Close()

			if tc.apiService != nil {
				handler.updateAPIService(tc.apiService)
				// 把刚写入的 handlingInfo 读出再存回一次；
				// 这里不改动内容，只是沿用测试里惯用的同步写法。
				curr := handler.handlingInfo.Load().(proxyHandlingInfo)
				handler.handlingInfo.Store(curr)
			}

			resp, err := http.Get(server.URL + tc.path)
			if err != nil {
				t.Errorf("%v", err)
				return
			}
			if e, a := tc.expectedStatusCode, resp.StatusCode; e != a {
				body, _ := httputil.DumpResponse(resp, true)
				t.Logf("%v", string(body))
				t.Errorf("expected %v, got %v", e, a)
				return
			}
			bytes, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("%v", err)
				return
			}
			if !strings.Contains(string(bytes), tc.expectedBody) {
				t.Errorf("expected %q, got %q", tc.expectedBody, string(bytes))
				return
			}

			if e, a := tc.expectedCalled, target.called; e != a {
				t.Errorf("expected %v, got %v", e, a)
				return
			}
			// 这个头每次测试都在变。
			// 该头包含本次测试服务器的地址，每次都不同，无法写进固定期望值。
			delete(target.headers, "X-Forwarded-Host")
			if e, a := tc.expectedHeaders, target.headers; !reflect.DeepEqual(e, a) {
				t.Errorf("expected != got %v", cmp.Diff(e, a))
				return
			}
			if e, a := targetServer.Listener.Addr().String(), target.host; tc.expectedCalled && !reflect.DeepEqual(e, a) {
				t.Errorf("expected %v, got %v", e, a)
				return
			}

			if tc.increaseSANWarnCounter {
				errorCounter := getSingleCounterValueFromRegistry(t, legacyregistry.DefaultGatherer, "apiserver_kube_aggregator_x509_missing_san_total")
				if errorCounter == -1 {
					t.Errorf("failed to get the x509_missing_san_total metrics: %v", err)
				}
				if int(errorCounter) != 1 {
					t.Errorf("expected the x509_missing_san_total to be 1, but it's %d", errorCounter)
				}
			}
		})
	}
}

// 记录拨号次数，用来断言 egress selector 配置的拨号器确实被用到。
type mockEgressDialer struct {
	called int
}

func (m *mockEgressDialer) dial(ctx context.Context, net, addr string) (net.Conn, error) {
	m.called++
	return http.DefaultTransport.(*http.Transport).DialContext(ctx, net, addr)
}

func (m *mockEgressDialer) dialBroken(ctx context.Context, net, addr string) (net.Conn, error) {
	m.called++
	return nil, fmt.Errorf("Broken dialer")
}

func newDialerAndSelector() (*mockEgressDialer, *egressselector.EgressSelector) {
	dialer := &mockEgressDialer{}
	m := make(map[egressselector.EgressType]utilnet.DialFunc)
	m[egressselector.Cluster] = dialer.dial
	es := egressselector.NewEgressSelectorWithMap(m)
	return dialer, es
}

func newBrokenDialerAndSelector() (*mockEgressDialer, *egressselector.EgressSelector) {
	dialer := &mockEgressDialer{}
	m := make(map[egressselector.EgressType]utilnet.DialFunc)
	m[egressselector.Cluster] = dialer.dialBroken
	es := egressselector.NewEgressSelectorWithMap(m)
	return dialer, es
}

// 通过 websocket 升级请求验证代理在 Upgrade 场景下的行为：
// 证书校验是否生效、egress selector 是否被使用、用户身份头是否透传。
func TestProxyUpgrade(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RemoteRequestHeaderUID, true)

	upgradeUser := "upgradeUser"
	upgradeUID := "upgradeUser-UID"
	testcases := map[string]struct {
		APIService        *apiregistration.APIService
		NewEgressSelector func() (*mockEgressDialer, *egressselector.EgressSelector)
		ExpectError       bool
		ExpectCalled      bool
	}{
		"valid hostname + CABundle": {
			APIService: &apiregistration.APIService{
				Spec: apiregistration.APIServiceSpec{
					CABundle: testCACrt,
					Group:    "mygroup",
					Version:  "v1",
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			ExpectError:  false,
			ExpectCalled: true,
		},
		"invalid hostname + insecure": {
			APIService: &apiregistration.APIService{
				Spec: apiregistration.APIServiceSpec{
					InsecureSkipTLSVerify: true,
					Group:                 "mygroup",
					Version:               "v1",
					Service:               &apiregistration.ServiceReference{Name: "invalid-service", Namespace: "invalid-ns", Port: ptr.To[int32](443)},
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			ExpectError:  false,
			ExpectCalled: true,
		},
		"invalid hostname + CABundle": {
			APIService: &apiregistration.APIService{
				Spec: apiregistration.APIServiceSpec{
					CABundle: testCACrt,
					Group:    "mygroup",
					Version:  "v1",
					Service:  &apiregistration.ServiceReference{Name: "invalid-service", Namespace: "invalid-ns", Port: ptr.To[int32](443)},
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			ExpectError:  true,
			ExpectCalled: false,
		},
		"valid hostname + CABundle + egress selector": {
			APIService: &apiregistration.APIService{
				Spec: apiregistration.APIServiceSpec{
					CABundle: testCACrt,
					Group:    "mygroup",
					Version:  "v1",
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			NewEgressSelector: newDialerAndSelector,
			ExpectError:       false,
			ExpectCalled:      true,
		},
		"valid hostname + CABundle + egress selector non working": {
			APIService: &apiregistration.APIService{
				Spec: apiregistration.APIServiceSpec{
					CABundle: testCACrt,
					Group:    "mygroup",
					Version:  "v1",
					Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To[int32](443)},
				},
				Status: apiregistration.APIServiceStatus{
					Conditions: []apiregistration.APIServiceCondition{
						{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
					},
				},
			},
			NewEgressSelector: newBrokenDialerAndSelector,
			ExpectError:       true,
			ExpectCalled:      false,
		},
	}

	for k, tc := range testcases {
		tcName := k
		t.Run(tcName, func(t *testing.T) {
			path := "/apis/" + tc.APIService.Spec.Group + "/" + tc.APIService.Spec.Version + "/foo"
			timesCalled := int32(0)
			backendHandler := http.NewServeMux()
			backendHandler.Handle(path, websocket.Handler(func(ws *websocket.Conn) {
				atomic.AddInt32(&timesCalled, 1)
				defer ws.Close()
				req := ws.Request()
				user := req.Header.Get("X-Remote-User")
				if user != upgradeUser {
					t.Errorf("expected user %q, got %q", upgradeUser, user)
				}
				uid := req.Header.Get("X-Remote-Uid")
				if uid != upgradeUID {
					t.Errorf("expected UID %q, got %q", upgradeUID, uid)
				}
				body := make([]byte, 5)
				ws.Read(body)
				ws.Write([]byte("hello " + string(body)))
			}))

			backendServer := httptest.NewUnstartedServer(backendHandler)
			cert, err := tls.X509KeyPair(svcCrt, svcKey)
			if err != nil {
				t.Errorf("https (valid hostname): %v", err)
				return
			}
			backendServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
			backendServer.StartTLS()
			defer backendServer.Close()

			defer func() {
				if called := atomic.LoadInt32(&timesCalled) > 0; called != tc.ExpectCalled {
					t.Errorf("%s: expected called=%v, got %v", tcName, tc.ExpectCalled, called)
				}
			}()

			serverURL, _ := url.Parse(backendServer.URL)
			proxyHandler := &proxyHandler{
				serviceResolver:            &mockedRouter{destinationHost: serverURL.Host},
				proxyCurrentCertKeyContent: func() ([]byte, []byte) { return emptyCert(), emptyCert() },
			}

			var dialer *mockEgressDialer
			var selector *egressselector.EgressSelector
			if tc.NewEgressSelector != nil {
				dialer, selector = tc.NewEgressSelector()

				egressDialer, err := selector.Lookup(egressselector.Cluster.AsNetworkContext())
				if err != nil {
					t.Fatal(err)
				}
				if egressDialer != nil {
					proxyHandler.proxyTransportDial = &transport.DialHolder{Dial: egressDialer}
				}
			}

			proxyHandler.updateAPIService(tc.APIService)
			aggregator := httptest.NewServer(contextHandler(proxyHandler, &user.DefaultInfo{Name: upgradeUser, UID: upgradeUID}))
			defer aggregator.Close()

			ws, err := websocket.Dial("ws://"+aggregator.Listener.Addr().String()+path, "", "http://127.0.0.1/")
			if err != nil {
				if !tc.ExpectError {
					t.Errorf("%s: websocket dial err: %s", tcName, err)
				}
				return
			}
			defer ws.Close()

			// 如果配置了 egressselector，就假定它一定被调用。
			// 配置了 egress selector 就必须走它的拨号器，否则代理绕过了网络策略约束。
			if dialer != nil && dialer.called != 1 {
				t.Errorf("expect egress dialer gets called %d times, got %d", 1, dialer.called)
			}

			if tc.ExpectError {
				t.Errorf("%s: expected websocket error, got none", tcName)
				return
			}

			if _, err := ws.Write([]byte("world")); err != nil {
				t.Errorf("%s: write err: %s", tcName, err)
				return
			}

			response := make([]byte, 20)
			n, err := ws.Read(response)
			if err != nil {
				t.Errorf("%s: read err: %s", tcName, err)
				return
			}
			if e, a := "hello world", string(response[0:n]); e != a {
				t.Errorf("%s: expected '%#v', got '%#v'", tcName, e, a)
				return
			}
		})
	}
}

var testCACrt = []byte(`-----BEGIN CERTIFICATE-----
MIIDGTCCAgGgAwIBAgIUAlOGbZ9MSBRFDMq483nGW7h4YNIwDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQd2ViaG9va190ZXN0c19jYTAgFw0yMDEwMDcxNDI4MDVa
GA8yMjk0MDcyMzE0MjgwNVowGzEZMBcGA1UEAwwQd2ViaG9va190ZXN0c19jYTCC
ASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANQF9aox1wJlB7wrFeEDYlRk
2AIfC28PZYjW3LsW7/gas2ImmRpzdZYq3nNFQwF67sUudeuuNNAvEngb8Q1wojG7
Uftt52c9e0Hi5LDxElWV3Tw1XyZFJsk5uwVNb377r7CDfTX3WUsX1WlUeUF6xmwE
M4jYQJ9pMPNUOEWpe7G8daTYineTVvrHvGpxVMMSpOWTWy4+oqWaz5tfFSbyvNZT
+eOLNkDo441KfXvb66zWV4AEfB2QDyGGMuPUT/FgsZHNuj/WNjt3bWvyey9ZGlDm
LPnJgbzEP1FnfIdtuSpHhbWox2Jnuht4hCwhTW1lcAi68MSQEs8KqptEhIJoIxkC
AwEAAaNTMFEwHQYDVR0OBBYEFJnGJQd3VkQP5cZLB1n9/FRKyBLPMB8GA1UdIwQY
MBaAFJnGJQd3VkQP5cZLB1n9/FRKyBLPMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZI
hvcNAQELBQADggEBALwqR2oo3v5Ghs9hS1YQIqegQ/IGZqQwiRz2HFTUGzf5+nUY
BpZHQPapLJ6Ki687rY4nkdEAMCeZjefBFc6uawo6rY4O8IiJAQbDprNNK8oerwiM
BWSDDDjoNxMZMCegSAv39YSonecKZsg7+l1K/nmuQNehgHNem71ZroaRCFvJJ59E
WSd3QP+Gh9iKabsDnkBrTk5KFa7X24c43DJ23kPE49NOwBhiM6Fs8q+tdzWzaVSb
56uXONZxYmFH5yDFvnBIqk2Fys5Klsn6IsM1BCgH2snbA6kwh9Kph4pLdAVGyR9i
MxfBxx4eUypOzIBGqa3OmvMcuNElBe8fcUtpqO0=
-----END CERTIFICATE-----`)

/* testCAKey
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA1AX1qjHXAmUHvCsV4QNiVGTYAh8Lbw9liNbcuxbv+BqzYiaZ
GnN1lirec0VDAXruxS5166400C8SeBvxDXCiMbtR+23nZz17QeLksPESVZXdPDVf
JkUmyTm7BU1vfvuvsIN9NfdZSxfVaVR5QXrGbAQziNhAn2kw81Q4Ral7sbx1pNiK
d5NW+se8anFUwxKk5ZNbLj6ipZrPm18VJvK81lP544s2QOjjjUp9e9vrrNZXgAR8
HZAPIYYy49RP8WCxkc26P9Y2O3dta/J7L1kaUOYs+cmBvMQ/UWd8h225KkeFtajH
Yme6G3iELCFNbWVwCLrwxJASzwqqm0SEgmgjGQIDAQABAoIBAGRWua8kzRMWCvYT
EdSeDF/SJaPDW17g03VR8b4cmc45nKEbkSNCduhtOz8kDRTbP7pTRX0WwWmwjTYI
SyjIIAoXEzJBDdz+7KD+pqnSPJICTWPcAj6TRUq/pnFY9yYKKFgJsizi9QAjtFyX
nJbPaq3dwyHE7bhDSOYu+j6FecNfhqrvj1JbRvIhllKaZJC6He3mNCkHeHtW6ZFk
qJJzWQtPFwqT7tsYCJikwUcQs6QqhD+pPnTYlAkBf24z8ByR8ET7vvOBpH53ufG/
+gv1K1H+JXQhyO8p4ga4/DdWl+qZoQyTDzm0wy/q+lo/w/pzdIVtgUmguVk/qXad
Bgb6ie0CgYEA/StHVneYIIBmHNdz2fPMMbMzv99/ngDN+ed6Swj+aZKeRgf+QljU
QScwRHUlGKvrFE7Dq/TXVEYO9ksC7tEpIQaSKUlHLBJhhCo8YvfLvH6zoF3F1W6d
7a0ZyXhCWEp5NaNhKdRUMNVXt5H5jf5IGcGXgsErDAStJhiBW/+D0mcCgYEA1mTl
qjclhUr1Ef+wu2N2kNhi8NnScapC1fKjqCzlGcT74lB0BZpizY/hsxSlrtXZE0jI
DhrpiYaxKx4G/Ktr6cu4u0V6sYLH36+wbxmSV2XokHhUfPXZYZNO6+s7mHa8P10N
byTvocQzDhRwN3aD0d32/f8FFvPCZrg2MKAB7n8CgYEArLvZqZJhtlNE2IrcHaos
+QAG3/QzE3ADGW4pT4bsZsXFvYx4m3YWI/oEAcFXtTSfaTSwZuPgAzzlun/FmYIW
KNVd5lN7/wLvjAhxOSlO1eYw0ssITy5xDJhdjsvBoJH3j3RQuASKCOOXPMWZWptT
QFeI84quvz11kheIM2fr3iMCgYApKPnGsgusCXX/XJ1rfG744/Iq10bFt7BZLtoo
oWXiiqTpEBUWNkudt2/XV7FvXXLtdt2hh50qYAeHhZ5FyAtRuWDf4zjo93i0AyDW
U4x65v+9LLzbuL9hMkzGkkTAwprld1Hq8qZm4ioDG/1nSIOKORkALoOlomrCGb+d
mjqEtQKBgQCwtZ7yWxDn/dHeO32VBZOR2YZutOc61BtQHdMqoYsk7qR2ixxVG2bb
1jTedAqac+x0HnJ6au5jdbv0Z95cyyX22MMWaW/H/LNMLxL85OaZfiqjVnntzHcK
jHXdYlJHC8Eslr3iUvRUodgRwOB8c4wWF7s5b6mxGqoXgsNsLrOUPw==
-----END RSA PRIVATE KEY-----
*/

// valid for hostname test-service.test-ns.svc
// signed by testCACrt
var svcCrt = []byte(`-----BEGIN CERTIFICATE-----
MIIDMjCCAhqgAwIBAgIUEBND1EVKxjU7UaJ1ZBw1glkXuaowDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQd2ViaG9va190ZXN0c19jYTAgFw0yMDEwMDcxNDI4MDVa
GA8yMjk0MDcyMzE0MjgwNVowIzEhMB8GA1UEAwwYdGVzdC1zZXJ2aWNlLnRlc3Qt
bnMuc3ZjMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvDXYxvaUdbX/
MA3+3SdYY4o8Jl2s1PW9MX4Mr/nCNltyOKDgfSABCN4XVsrd+/A+/zQt+EyJEJxM
rd1syhzd/TJAnGzexmZg/dIi0jC3oBe/qyERWimZhqbu0O+0EpFx5qLzQ5eLabLU
9CtBwRSyYQjqsDmPoqplsKxaFF9NIFQrh1zmxBay9vTY7P7sLkfZ8LifP6jgQ5NH
QkjaY9XCMzYbcrzbc2r9vxTm//IR1cWxaifTNE9qo2NL1iiPGTpot65z83BWeu/q
WOU+aGUhY/xcZH0w/rUJ7ffviyd94EY4IN7FUJv53EJgmEp4UOaY1fAFtAFQQbVz
tGjYGpZ22wIDAQABo2QwYjAJBgNVHRMEAjAAMAsGA1UdDwQEAwIF4DAdBgNVHSUE
FjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwKQYDVR0RBCIwIIcEfwAAAYIYdGVzdC1z
ZXJ2aWNlLnRlc3QtbnMuc3ZjMA0GCSqGSIb3DQEBCwUAA4IBAQCw/EoFXFahLC4g
4iq9VWhnCmAqUv6IuJqOMC+qEH7fSB3UDAjL4A2iuNJaBAxhI2bccoP2wtqZCkHH
0YLyoKOPjgl6VZtByco8Su7T9yOaef6aX1OP4Snm/aeYdVbjSBKVwMywmmb34XFa
azChi6sq4TFPNesUUoEGkKErU+XG/ecp9Obc0DK/3AAVx/Fk8W5104m1i9PWlUZ2
KlyxQ5F2alBRv9csIpl2syWQ90DMSQ1Y/R8b+kfsBG7RwDbmwGpZLQTwhE8Uga9T
ZDnmwjUmWn7SD3ouyBSnbWkLE1KcbB32mz5jrwfKCPIa5ka+GIFrme1HxRoQziGo
w+KU2RWu
-----END CERTIFICATE-----`)

var svcCrtNoSAN = []byte(`-----BEGIN CERTIFICATE-----
MIIDBzCCAe+gAwIBAgIUEBND1EVKxjU7UaJ1ZBw1glkXuaswDQYJKoZIhvcNAQEL
BQAwGzEZMBcGA1UEAwwQd2ViaG9va190ZXN0c19jYTAgFw0yMDEwMDcxNDI4MDVa
GA8yMjk0MDcyMzE0MjgwNVowIzEhMB8GA1UEAwwYdGVzdC1zZXJ2aWNlLnRlc3Qt
bnMuc3ZjMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvDXYxvaUdbX/
MA3+3SdYY4o8Jl2s1PW9MX4Mr/nCNltyOKDgfSABCN4XVsrd+/A+/zQt+EyJEJxM
rd1syhzd/TJAnGzexmZg/dIi0jC3oBe/qyERWimZhqbu0O+0EpFx5qLzQ5eLabLU
9CtBwRSyYQjqsDmPoqplsKxaFF9NIFQrh1zmxBay9vTY7P7sLkfZ8LifP6jgQ5NH
QkjaY9XCMzYbcrzbc2r9vxTm//IR1cWxaifTNE9qo2NL1iiPGTpot65z83BWeu/q
WOU+aGUhY/xcZH0w/rUJ7ffviyd94EY4IN7FUJv53EJgmEp4UOaY1fAFtAFQQbVz
tGjYGpZ22wIDAQABozkwNzAJBgNVHRMEAjAAMAsGA1UdDwQEAwIF4DAdBgNVHSUE
FjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwDQYJKoZIhvcNAQELBQADggEBAMPhbecq
wJtlKnSe27xQIM1bNkI/+r1aVmuJqYYbtzCaVZFnFRD6ZbCLfEo7QT17gs7ulryI
yfeITEMAWG6Bq8cOhNQfXRIf2YMFHbDsFbfAEREy/jfYGw8G4b6RBVQzcuglCCB/
Y0++skz8kYIR1KuZnCtC6A0kaM2XrTWCXAc5KB0Q/WO0wqqWbH/xmEYQVZmDqWOH
k+qVFD+I1oT5NOzFpzaUe4T7grzoLs24IE0c+0clcc9pxTDXTfPyoLG9n3zxG0Ma
hPtkUeeEK8p73Zf/F4JHQ4tJv5XY1ytWkTROE79P6qT0BY/XZSpsGmB7TIS7wFCW
RfKAqN95Uso3IBI=
-----END CERTIFICATE-----`)

var svcKey = []byte(`-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAvDXYxvaUdbX/MA3+3SdYY4o8Jl2s1PW9MX4Mr/nCNltyOKDg
fSABCN4XVsrd+/A+/zQt+EyJEJxMrd1syhzd/TJAnGzexmZg/dIi0jC3oBe/qyER
WimZhqbu0O+0EpFx5qLzQ5eLabLU9CtBwRSyYQjqsDmPoqplsKxaFF9NIFQrh1zm
xBay9vTY7P7sLkfZ8LifP6jgQ5NHQkjaY9XCMzYbcrzbc2r9vxTm//IR1cWxaifT
NE9qo2NL1iiPGTpot65z83BWeu/qWOU+aGUhY/xcZH0w/rUJ7ffviyd94EY4IN7F
UJv53EJgmEp4UOaY1fAFtAFQQbVztGjYGpZ22wIDAQABAoIBAD7Wl5buUujuJ9Jq
idJaxZcOW0DP+9lqZo10sVW7xM0TQRKJHAqKue21AQPYXb81GkNor4R8QTMLjEps
aFsewjs8IPhZHRQOsIluNHQLEfPgmfzP4JRC2WBsscWOkoe0idvgQeoqWcCjlZgk
LSMC/v+I05qczUkZLTSMhtLQcta80OxU99kNU8Kfi6NFiAioqVQl4KlczjLJiUbK
3RGOqThtjS0IzXXFr+T+bgxQkmkyAPGmx06OqqM8hdA+6WsRb8LS1XfK7qGWbU0T
7mIehkcMFDRgxlDh4JfCQzWuLTax3Ds8BApJwZCBEQz8T+FbVWJpBwezyhaKBOis
nQmtw8ECgYEA3E+mANY6YNVfFztMpjfh57dY2DLZY9h1yHRK13FM7EK0Z8GgMji6
kDIubUBta19g3+YI4qIJgvS527ipVEHW0lYUIQ3q+JnafTC7mMxT+2J/j+lrZhrw
aIPxZML29iEm64Wr3mCmUU98iy5z7EUqqKTNwr03f2eSBeO/xn6VtrsCgYEA2rL4
tOJMoMDfQzAe7KIqEUn2Ob0nYP/MJZ1I8wrrdGMDhp4xofr+m99++uFPqm5u5uI5
cJ6+xZQ1A6CJSKWtzOALsKN1xx+JJh9Wo2vUliDomKtarFiQO+ONLpnjuSraDMWY
cKx6eXqqgit5hlQeCva2cbUP1De++3RhEpC6DmECgYA8kCiyUjH6LK3XVRXdG7+e
U2i5BkF8kSTP1ig80Yiz6iJt42yGYdHnkePxZKSvv6iB5FrM8n5q4Zu2Ky1hXDgR
2lfuPkU50hGeGKd5ebIciRdIGILNrton4R2a9X2ua66nUDfPCgKul4tFN5/mc50m
fyeRQTLgczhRJiqyBlphwQKBgQDTnjBIH12Ug2zF/688vGHGXvIRxrVvB7XLg9lN
y/gvo4uK3FIccdmijG27Zv+GY9uOL8Ly9biVSKbPvqx4jlCRmQ3WuyTBLAOyzsov
0axgJLHM4KoZcI0IVlSLjj8rMorRpvWtuUe9enO5B0ZNM+HqK/Y4KsKJT/POLzur
Ej3moQKBgQC+RWcly9opx0We4LG0lcdG3V0cawDRP2MmLbxHA/kSuGf5aBMJoCdf
f0vRPPCK7dpPGOX9x8Oz7K7QiOEvFL3Mv1sWBEnl5lSkK8gdBhi6St9RRBGimt2H
S+8g5OWupiWGF6qN+XX5WgYyuipW8mVRaROj8Vyl7JSiwu6KHfZ8RQ==
-----END RSA PRIVATE KEY-----`)

// 后端故意不返回，验证代理为新请求设置的超时上下文确实会生效，
// 客户端最终收到 503 且错误里包含 deadline exceeded。
func TestGetContextForNewRequest(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		// 阻塞到测试结束才返回，确保代理只能因超时而结束这次请求。
		<-done // never return so that we're certain to return base on timeout
	}))
	defer server.Close()
	defer close(done)

	proxyServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		location, err := url.Parse(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		location.Path = req.URL.Path

		nestedReq := req.WithContext(genericapirequest.WithRequestInfo(req.Context(), &genericapirequest.RequestInfo{Path: req.URL.Path}))
		newReq, cancelFn := apiserverproxyutil.NewRequestForProxy(location, nestedReq)
		defer cancelFn()

		theproxy := proxy.NewUpgradeAwareHandler(location, server.Client().Transport, true, false, &responder{w: w})
		theproxy.ServeHTTP(w, newReq)
	}))
	defer proxyServer.Close()

	// 正常客户端不会设置 timeout，这里也不要设置；我们的代理逻辑应当替我们构造它。
	// 故意不设置客户端超时，用来证明超时完全由代理自己构造的上下文提供。
	resp, err := proxyServer.Client().Get(proxyServer.URL + "/apis/group/version")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Error(err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "context deadline exceeded") {
		t.Error(string(body))
	}

}

// 验证请求经过代理后，后端收到的 traceparent 头能被还原出有效的远端 span，
// 且 span ID / trace ID 与本地记录的一致，说明代理正确透传了追踪上下文。
func TestTracerProvider(t *testing.T) {
	fakeRecorder := tracetest.NewSpanRecorder()
	otelTracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(fakeRecorder))
	target := &targetHTTPHandler{}
	user := &user.DefaultInfo{
		Name:   "username",
		Groups: []string{"one", "two"},
	}
	path := "/request/path"
	apiService := &apiregistration.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
		Spec: apiregistration.APIServiceSpec{
			Service:  &apiregistration.ServiceReference{Name: "test-service", Namespace: "test-ns", Port: ptr.To(int32(443))},
			Group:    "foo",
			Version:  "v1",
			CABundle: testCACrt,
		},
		Status: apiregistration.APIServiceStatus{
			Conditions: []apiregistration.APIServiceCondition{
				{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
			},
		},
	}
	targetServer := httptest.NewUnstartedServer(target)
	serviceCert := svcCrt
	if cert, err := tls.X509KeyPair(serviceCert, svcKey); err != nil {
		t.Fatalf("TestTracerProvider: failed to parse key pair: %v", err)
	} else {
		targetServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	targetServer.StartTLS()
	defer targetServer.Close()

	serviceResolver := &mockedRouter{destinationHost: targetServer.Listener.Addr().String()}
	handler := &proxyHandler{
		localDelegate:              http.NewServeMux(),
		serviceResolver:            serviceResolver,
		proxyCurrentCertKeyContent: func() ([]byte, []byte) { return emptyCert(), emptyCert() },
		tracerProvider:             otelTracer,
	}

	server := httptest.NewServer(contextHandler(filters.WithTracing(handler, otelTracer), user))
	defer server.Close()

	handler.updateAPIService(apiService)
	curr := handler.handlingInfo.Load().(proxyHandlingInfo)
	handler.handlingInfo.Store(curr)
	var propagator propagation.TraceContext
	req, err := http.NewRequest(http.MethodGet, server.URL+path, nil)
	if err != nil {
		t.Errorf("expected new request: %v", err)
		return
	}

	t.Logf("Sending request: %v", req)
	_, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Errorf("http request failed: %v", err)
		return
	}

	t.Log("Ensure the target server received the traceparent header")
	// 后端只有拿到 traceparent 才能还原出远端 span，因此先确认这个头确实传到了。
	id, ok := target.headers["Traceparent"]
	if !ok {
		t.Error("expected traceparent header")
		return
	}

	t.Log("Get the span context from the traceparent header")
	h := http.Header{
		"Traceparent": id,
	}
	ctx := propagator.Extract(context.Background(), propagation.HeaderCarrier(h))
	span := trace.SpanFromContext(ctx)

	t.Log("Ensure that the span context is valid and remote")
	// 必须是远端 span，才能证明这个上下文是从 traceparent 头还原的，
	// 而不是测试进程里自己新起的 span。
	if !span.SpanContext().IsValid() {
		t.Error("expected valid span context")
		return
	}

	if !span.SpanContext().IsRemote() {
		t.Error("expected remote span context")
		return
	}

	t.Log("Ensure that the span ID and trace ID match the expected values")
	expectedSpanCtx := fakeRecorder.Ended()[0].SpanContext()
	if expectedSpanCtx.TraceID() != span.SpanContext().TraceID() {
		t.Errorf("expected trace id to match. expected: %v, but got %v", expectedSpanCtx.TraceID(), span.SpanContext().TraceID())
		return
	}

	if expectedSpanCtx.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("expected span id to match. expected: %v, but got: %v", expectedSpanCtx.SpanID(), span.SpanContext().SpanID())
		return
	}

	t.Log("Ensure that the expected spans were recorded when sending a request through the proxy")
	// 期望两条 span：过滤层记录的客户端 GET 和代理内部的 GET，
	// 顺序固定，所以用 DeepEqual 一次性比较。
	expectedSpanNames := []string{"HTTP GET", "GET"}
	spanNames := []string{}
	for _, span := range fakeRecorder.Ended() {
		spanNames = append(spanNames, span.Name())
	}
	if e, a := expectedSpanNames, spanNames; !reflect.DeepEqual(e, a) {
		t.Errorf("expected span names %v, got %v", e, a)
		return
	}
}

// 验证审计 ID 能随代理请求透传：原请求带则新请求也要带，原请求没有则不应凭空生成，
// 避免审计日志中出现不一致的关联 ID。
func TestNewRequestForProxyWithAuditID(t *testing.T) {
	tests := []struct {
		name    string
		auditID string
	}{
		{
			name:    "original request has Audit-ID",
			auditID: "foo-bar",
		},
		{
			name:    "original request does not have Audit-ID",
			auditID: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "/api/group/version/foos/namespace/foo", nil)
			if err != nil {
				t.Fatalf("failed to create new http request - %v", err)
			}

			req = req.WithContext(genericapirequest.WithRequestInfo(req.Context(), &genericapirequest.RequestInfo{Path: req.URL.Path}))
			if len(test.auditID) > 0 {
				ctx := audit.WithAuditContext(req.Context())
				audit.WithAuditID(ctx, types.UID(test.auditID))
				req = req.WithContext(ctx)
			}

			newReq, _ := apiserverproxyutil.NewRequestForProxy(req.URL, req)
			if newReq == nil {
				t.Fatal("expected a non nil Request object")
			}

			auditIDGot := newReq.Header.Get("Audit-ID")
			if test.auditID != auditIDGot {
				t.Errorf("expected an Audit-ID value: %q, but got: %q", test.auditID, auditIDGot)
			}
		})
	}
}

// TestProxyCertReload verifies that the proxy reloading of certificates work
// to be able to test the reloading it starts a server with client auth enabled
// it first uses certs that does not match the client CA so the verification fails - expecting HTTP 503
// then we write correct client certs to the disk, expecting the proxy to reload the cert and use it for the next request
//
// Note: this test doesn't use apiserviceRegistrationController nor it doesn't start DynamicServingContentFromFiles controller
// instead it manually calls to updateAPIService and RunOnce to reload the certificate
func TestProxyCertReload(t *testing.T) {
	// STEP 1: 搭建一个要求客户端证书的后端 server；
	//         该 server 使用 clientCaCrt() 校验客户端证书。
	// 后端强制校验客户端证书，这样代理换证书前后的行为差异才能被观察到。
	backendHandler := &targetHTTPHandler{}
	backendServer := httptest.NewUnstartedServer(backendHandler)
	if cert, err := tls.X509KeyPair(backendCertificate(), backendKey()); err != nil {
		t.Fatal(err)
	} else {
		caCertPool := x509.NewCertPool()
		// we're testing this while enabling MTLS
		caCertPool.AppendCertsFromPEM(clientCaCrt())
		backendServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: caCertPool}
	}
	backendServer.StartTLS()
	defer backendServer.Close()

	// STEP 2: 搭建聚合层，它使用一张无效证书（不会被 clientCA 接受）向后端 server 认证。
	// 先把后端证书当作客户端证书使用，它无法通过 clientCA 校验，预期第一次请求得到 503。
	aggregatorHandler := &proxyHandler{
		localDelegate:   http.NewServeMux(),
		serviceResolver: &mockedRouter{destinationHost: backendServer.Listener.Addr().String()},
	}
	certFile, keyFile, dir := getCertAndKeyPaths(t)
	writeCerts(certFile, keyFile, backendCertificate(), backendKey(), t)

	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("Unable to clean up test directory %q: %v", dir, err)
		}
	}()

	certProvider, err := dynamiccertificates.NewDynamicServingContentFromFiles("test", certFile, keyFile)
	if err != nil {
		t.Fatalf("Unable to create dynamic certificates: %v", err)
	}
	ctx := context.TODO()
	err = certProvider.RunOnce(ctx)
	if err != nil {
		t.Fatalf("Unable to load dynamic certificates: %v", err)
	}
	aggregatorHandler.proxyCurrentCertKeyContent = certProvider.CurrentCertKeyContent

	apiService := &apiregistration.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: "v1.foo"},
		Spec: apiregistration.APIServiceSpec{
			Service:  &apiregistration.ServiceReference{Name: "test-service2", Namespace: "test-ns", Port: ptr.To[int32](443)},
			Group:    "foo",
			Version:  "v1",
			CABundle: backendCaCertificate(), // used to validate backendCertificate()
		},
		Status: apiregistration.APIServiceStatus{
			Conditions: []apiregistration.APIServiceCondition{
				{Type: apiregistration.Available, Status: apiregistration.ConditionTrue},
			},
		},
	}
	aggregatorHandler.updateAPIService(apiService)

	server := httptest.NewServer(contextHandler(aggregatorHandler, &user.DefaultInfo{
		Name:   "username",
		Groups: []string{"one", "two"},
	}))
	defer server.Close()

	resp, err := http.Get(server.URL + "/request/path")
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("Expected status code 503 but got %d", resp.StatusCode)
	}

	// STEP 3: 更换聚合层向后端 server 认证所用的证书，并验证请求可以通过；
	//         注意这一步使用的证书能被后端 server 的 clientCaCrt() 校验通过。
	// 直接改写磁盘上的证书文件并重新 RunOnce，模拟外部证书轮转后代理自动加载新证书。
	writeCerts(certFile, keyFile, clientCert(), clientKey(), t)
	err = certProvider.RunOnce(ctx)
	if err != nil {
		t.Fatalf("Expected no error when refreshing dynamic certs, got %v", err)
	}
	aggregatorHandler.updateAPIService(apiService)

	resp, err = http.Get(server.URL + "/request/path")
	if err != nil {
		t.Errorf("%v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected status code 200 but got %d", resp.StatusCode)
	}
}

// 实现流控初始化信号接口，SignalCount 用原子交换读取并清零，
// 便于断言在两次读取之间信号到底触发了几次。
type fcInitSignal struct {
	nSignals int32
}

func (s *fcInitSignal) SignalCount() int {
	return int(atomic.SwapInt32(&s.nSignals, 0))
}

func (s *fcInitSignal) Signal() {
	atomic.AddInt32(&s.nSignals, 1)
}

func (s *fcInitSignal) Wait() {
}

// 包装 net.Listener，在后端接受连接的瞬间记录流控信号是否已触发；
// 因为连接建立发生在真正转发请求时，所以这个时点能反映代理是否发出了初始化信号。
type hookedListener struct {
	l        net.Listener
	onAccept func()
}

func (wl *hookedListener) Accept() (net.Conn, error) {
	conn, err := wl.l.Accept()
	if err == nil {
		wl.onAccept()
	}
	return conn, err
}

func (wl *hookedListener) Close() error {
	return wl.l.Close()
}

func (wl *hookedListener) Addr() net.Addr {
	return wl.l.Addr()
}

// 验证只有“非本地且服务可用”的代理请求才会触发流控初始化信号；
// 本地请求和服务不可用的请求都不应触发，升级请求仍要触发。
func TestFlowControlSignal(t *testing.T) {
	for _, tc := range []struct {
		Name           string
		Local          bool
		Available      bool
		Request        http.Request
		SignalExpected bool
	}{
		{
			Name:           "local",
			Local:          true,
			SignalExpected: false,
		},
		{
			Name:           "unavailable",
			Local:          false,
			Available:      false,
			SignalExpected: false,
		},
		{
			Name:           "request performed",
			Local:          false,
			Available:      true,
			SignalExpected: true,
		},
		{
			Name:      "upgrade request performed",
			Local:     false,
			Available: true,
			Request: http.Request{
				Header: http.Header{"Connection": []string{"Upgrade"}},
			},
			SignalExpected: true,
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			okh := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})

			var sig fcInitSignal

			var signalCountOnAccept int32
			backend := httptest.NewUnstartedServer(okh)
			backend.Listener = &hookedListener{
				l: backend.Listener,
				onAccept: func() {
					atomic.StoreInt32(&signalCountOnAccept, int32(sig.SignalCount()))
				},
			}
			backend.Start()
			defer backend.Close()

			p := proxyHandler{
				localDelegate:   okh,
				serviceResolver: &mockedRouter{destinationHost: backend.Listener.Addr().String()},
			}

			server := httptest.NewServer(contextHandler(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					p.ServeHTTP(w, r.WithContext(utilflowcontrol.WithInitializationSignal(r.Context(), &sig)))
				}),
				&user.DefaultInfo{
					Name:   "username",
					Groups: []string{"one", "two"},
				},
			))
			defer server.Close()

			p.handlingInfo.Store(proxyHandlingInfo{
				local:             tc.Local,
				serviceAvailable:  tc.Available,
				proxyRoundTripper: backend.Client().Transport,
			})

			surl, err := url.Parse(server.URL)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			req := tc.Request
			req.URL = surl
			res, err := server.Client().Do(&req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if err := res.Body.Close(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if fired := (atomic.LoadInt32(&signalCountOnAccept) > 0); tc.SignalExpected && !fired {
				t.Errorf("flow control signal expected but not fired")
			} else if fired && !tc.SignalExpected {
				t.Errorf("flow control signal fired but not expected")
			}
		})
	}
}

// 证书轮转测试需要临时目录存放可写证书文件，目录名带前缀便于失败后定位残留。
func getCertAndKeyPaths(t *testing.T) (string, string, string) {
	dir, err := os.MkdirTemp(os.TempDir(), "k8s-test-handler-proxy-cert")
	if err != nil {
		t.Fatalf("Unable to create the test directory %q: %v", dir, err)
	}
	certFile := filepath.Join(dir, "certfile.pem")
	keyFile := filepath.Join(dir, "keytfile.pem")
	return certFile, keyFile, dir
}

func writeCerts(certFile, keyFile string, certContent, keyContent []byte, t *testing.T) {
	if err := os.WriteFile(certFile, certContent, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", certFile, err)
	}
	if err := os.WriteFile(keyFile, keyContent, 0600); err != nil {
		t.Fatalf("Unable to create the file %q: %v", keyFile, err)
	}
}

// 从 registry 中按名字取出唯一的 counter 值；找不到时返回 -1，
// 让调用方用一个哨兵值区分“指标未注册”与“指标值为 0”。
func getSingleCounterValueFromRegistry(t *testing.T, r metrics.Gatherer, name string) int {
	mfs, err := r.Gather()
	if err != nil {
		t.Logf("failed to gather local registry metrics: %v", err)
		return -1
	}

	for _, mf := range mfs {
		if mf.Name != nil && *mf.Name == name {
			mfMetric := mf.GetMetric()
			for _, m := range mfMetric {
				if m.GetCounter() != nil {
					return int(m.GetCounter().GetValue())
				}
			}
		}
	}

	return -1
}

// 统一从 testdata 目录读取证书/密钥文件，避免每个辅助函数重复拼接路径。
func readTestFile(filename string) []byte {
	data, err := os.ReadFile("testdata/" + filename)
	if err != nil {
		panic(err)
	}
	return data
}

// 用于客户端认证的证书与 CA。
// 这一组辅助函数把双向 TLS 测试所需的证书集中命名，
// 用例里只出现语义化函数名，不直接写死文件名。
func clientCert() []byte { return readTestFile("client.pem") }

func clientKey() []byte { return readTestFile("client-key.pem") }

func backendCertificate() []byte { return readTestFile("server.pem") }

func backendKey() []byte { return readTestFile("server-key.pem") }

func backendCaCertificate() []byte { return readTestFile("server-ca.pem") }

func clientCaCrt() []byte { return readTestFile("client-ca.pem") }
