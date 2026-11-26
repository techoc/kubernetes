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

package webhook

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/authentication/user"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	v1 "k8s.io/client-go/tools/clientcmd/api/v1"
)

// testRetryBackoff 是测试用的短重试策略：步数固定为 5，便于断言调用次数。
var testRetryBackoff = wait.Backoff{
	Duration: 5 * time.Millisecond,
	Factor:   1.5,
	Jitter:   0.2,
	Steps:    5,
}

// V1Service 模拟一个远端认证服务。
// 测试服务可控制 allow 与 HTTP 状态码，用于覆盖成功、拒绝、
// 可重试错误（429/5xx）与不可重试错误（404）等路径。
type V1Service interface {
	// 查看 TokenReviewSpec，并把认证结果写入 TokenReviewStatus。
	Review(*authenticationv1.TokenReview)
	HTTPStatusCode() int
}

// NewV1TestServer 把 V1Service 包装成一个 httptest.Server。
// caCert 非空时要求并校验客户端证书，cert/key 非空时启用服务端
// TLS；两者组合可模拟 mTLS、单向 TLS 与不安全连接三种场景。
func NewV1TestServer(s V1Service, cert, key, caCert []byte) (*httptest.Server, error) {
	const webhookPath = "/testserver"
	var tlsConfig *tls.Config
	if cert != nil {
		cert, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}

	if caCert != nil {
		rootCAs := x509.NewCertPool()
		rootCAs.AppendCertsFromPEM(caCert)
		if tlsConfig == nil {
			tlsConfig = &tls.Config{}
		}
		tlsConfig.ClientCAs = rootCAs
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	serveHTTP := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, fmt.Sprintf("unexpected method: %v", r.Method), http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path != webhookPath {
			http.Error(w, fmt.Sprintf("unexpected path: %v", r.URL.Path), http.StatusNotFound)
			return
		}

		var review authenticationv1.TokenReview
		bodyData, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(bodyData, &review); err != nil {
			http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)
			return
		}
		// 确保收到的正是预期的、已序列化的 TokenReview。
		// 校验 apiVersion 可发现编码错误（如误用 internal 版本），
		// 这类错误在真实环境中会表现为 webhook 端解析失败。
		if review.APIVersion != "authentication.k8s.io/v1" {
			http.Error(w, fmt.Sprintf("wrong api version: %s", string(bodyData)), http.StatusBadRequest)
			return
		}
		// 一旦有请求成功，就总是调用 review，以记录它确实被调用过。
		s.Review(&review)
		if s.HTTPStatusCode() < 200 || s.HTTPStatusCode() >= 300 {
			http.Error(w, "HTTP Error", s.HTTPStatusCode())
			return
		}
		type userInfo struct {
			Username string              `json:"username"`
			UID      string              `json:"uid"`
			Groups   []string            `json:"groups"`
			Extra    map[string][]string `json:"extra"`
		}
		type status struct {
			Authenticated bool     `json:"authenticated"`
			User          userInfo `json:"user"`
			Audiences     []string `json:"audiences"`
		}

		var extra map[string][]string
		if review.Status.User.Extra != nil {
			extra = map[string][]string{}
			for k, v := range review.Status.User.Extra {
				extra[k] = v
			}
		}

		resp := struct {
			Kind       string `json:"kind"`
			APIVersion string `json:"apiVersion"`
			Status     status `json:"status"`
		}{
			Kind:       "TokenReview",
			APIVersion: authenticationv1.SchemeGroupVersion.String(),
			Status: status{
				review.Status.Authenticated,
				userInfo{
					Username: review.Status.User.Username,
					UID:      review.Status.User.UID,
					Groups:   review.Status.User.Groups,
					Extra:    extra,
				},
				review.Status.Audiences,
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}

	server := httptest.NewUnstartedServer(http.HandlerFunc(serveHTTP))
	server.TLS = tlsConfig
	server.StartTLS()

	// 把路径改为我们自定义的路径。
	serverURL, _ := url.Parse(server.URL)
	serverURL.Path = webhookPath
	server.URL = serverURL.String()

	return server, nil
}

// mockV1Service 是一个可以被设置为对认证请求回答「同意」或「拒绝」的服务。
// called 计数器用于验证缓存命中与重试次数。
type mockV1Service struct {
	allow      bool
	statusCode int
	called     int
}

// Review 记录调用次数并按 allow 填充响应。
func (m *mockV1Service) Review(r *authenticationv1.TokenReview) {
	m.called++
	r.Status.Authenticated = m.allow
	if m.allow {
		r.Status.User.Username = "realHooman@email.com"
	}
}
func (m *mockV1Service) Allow()              { m.allow = true }
func (m *mockV1Service) Deny()               { m.allow = false }
func (m *mockV1Service) HTTPStatusCode() int { return m.statusCode }

// newV1TokenAuthenticator 用传入的参数生成一个临时 kubeconfig 文件，
// 并尝试从中加载一个新的 WebhookTokenAuthenticator。
// 走真实 kubeconfig 解析路径，确保客户端证书/CA 的接线方式与
// 生产一致；cacheTime 为 0 表示不缓存，便于验证每次调用都打到服务端。
func newV1TokenAuthenticator(serverURL string, clientCert, clientKey, ca []byte, cacheTime time.Duration, implicitAuds authenticator.Audiences, metrics AuthenticatorMetrics) (authenticator.Token, error) {
	tempfile, err := os.CreateTemp("", "")
	if err != nil {
		return nil, err
	}
	p := tempfile.Name()
	defer os.Remove(p)
	config := v1.Config{
		Clusters: []v1.NamedCluster{
			{
				Cluster: v1.Cluster{Server: serverURL, CertificateAuthorityData: ca},
			},
		},
		AuthInfos: []v1.NamedAuthInfo{
			{
				AuthInfo: v1.AuthInfo{ClientCertificateData: clientCert, ClientKeyData: clientKey},
			},
		},
	}
	if err := json.NewEncoder(tempfile).Encode(config); err != nil {
		return nil, err
	}

	clientConfig, err := webhookutil.LoadKubeconfig(p, nil)
	if err != nil {
		return nil, err
	}

	c, err := tokenReviewInterfaceFromConfig(clientConfig, "v1", testRetryBackoff)
	if err != nil {
		return nil, err
	}

	authn, err := newWithBackoff(c, testRetryBackoff, implicitAuds, 10*time.Second, metrics)
	if err != nil {
		return nil, err
	}

	return cache.New(authn, false, cacheTime, cacheTime), nil
}

// TestV1TLSConfig 验证客户端与服务端在各种证书组合下的 TLS 行为。
//
// webhook 认证器不支持不安全连接；信任关系不对称时必须报错，
// 而不是降级为明文，否则会静默失去传输安全。
func TestV1TLSConfig(t *testing.T) {
	tests := []struct {
		test                            string
		clientCert, clientKey, clientCA []byte
		serverCert, serverKey, serverCA []byte
		wantErr                         bool
	}{
		{
			test:       "TLS setup between client and server",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: caCert,
		},
		{
			test:       "Server does not require client auth",
			clientCA:   caCert,
			serverCert: serverCert, serverKey: serverKey,
		},
		{
			test:       "Server does not require client auth, client provides it",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey,
		},
		{
			test:       "Client does not trust server",
			clientCert: clientCert, clientKey: clientKey,
			serverCert: serverCert, serverKey: serverKey,
			wantErr: true,
		},
		{
			test:       "Server does not trust client",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: badCACert,
			wantErr: true,
		},
		{
			// 插件不支持不安全配置。
			test:    "Server is using insecure connection",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		// 使用闭包，让 defer 语句在每次循环迭代之间就生效。
		// 在闭包内 defer，保证每个用例结束即关闭 server，避免句柄泄漏。
		func() {
			service := new(mockV1Service)
			service.statusCode = 200

			server, err := NewV1TestServer(service, tt.serverCert, tt.serverKey, tt.serverCA)
			if err != nil {
				t.Errorf("%s: failed to create server: %v", tt.test, err)
				return
			}
			defer server.Close()

			wh, err := newV1TokenAuthenticator(server.URL, tt.clientCert, tt.clientKey, tt.clientCA, 0, nil, noopAuthenticatorMetrics())
			if err != nil {
				t.Errorf("%s: failed to create client: %v", tt.test, err)
				return
			}

			// 全部放行，观察是否会报错。
			service.Allow()
			_, authenticated, err := wh.AuthenticateToken(context.Background(), "t0k3n")
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error making authorization request: %v", err)
				}
				return
			}
			if !authenticated {
				t.Errorf("%s: failed to authenticate token", tt.test)
				return
			}

			service.Deny()
			_, authenticated, err = wh.AuthenticateToken(context.Background(), "t0k3n")
			if err != nil {
				t.Errorf("%s: unexpectedly failed AuthenticateToken", tt.test)
			}
			if authenticated {
				t.Errorf("%s: incorrectly authenticated token", tt.test)
			}
		}()
	}
}

// recorderV1Service 记录所有 token review 请求，并用给定的 TokenReviewStatus 作为响应。
// 记录完整请求，便于断言 token、TypeMeta 与 audience 是否正确传递。
type recorderV1Service struct {
	lastRequest authenticationv1.TokenReview
	response    authenticationv1.TokenReviewStatus
}

// Review 保存请求并回填预设响应。
func (rec *recorderV1Service) Review(r *authenticationv1.TokenReview) {
	rec.lastRequest = *r
	r.Status = rec.response
}

func (rec *recorderV1Service) HTTPStatusCode() int { return 200 }

// TestV1WebhookTokenAuthenticator 验证响应映射、未认证处理与 audience 语义。
//
// 未认证时即使服务端返回用户信息也必须丢弃；audience 需取请求
// 与隐式配置的交集，服务端返回的 Audiences 必须与请求期望一致。
func TestV1WebhookTokenAuthenticator(t *testing.T) {
	serv := &recorderV1Service{}

	s, err := NewV1TestServer(serv, serverCert, serverKey, caCert)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	expTypeMeta := metav1.TypeMeta{
		APIVersion: "authentication.k8s.io/v1",
		Kind:       "TokenReview",
	}

	tests := []struct {
		description           string
		implicitAuds, reqAuds authenticator.Audiences
		serverResponse        authenticationv1.TokenReviewStatus
		expectedAuthenticated bool
		expectedUser          *user.DefaultInfo
		expectedAuds          authenticator.Audiences
	}{
		{
			description: "successful response should pass through all user info.",
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
			},
			expectedAuthenticated: true,
			expectedUser: &user.DefaultInfo{
				Name: "somebody",
			},
		},
		{
			description: "successful response should pass through all user info.",
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "person@place.com",
					UID:      "abcd-1234",
					Groups:   []string{"stuff-dev", "main-eng"},
					Extra:    map[string]authenticationv1.ExtraValue{"foo": {"bar", "baz"}},
				},
			},
			expectedAuthenticated: true,
			expectedUser: &user.DefaultInfo{
				Name:   "person@place.com",
				UID:    "abcd-1234",
				Groups: []string{"stuff-dev", "main-eng"},
				Extra:  map[string][]string{"foo": {"bar", "baz"}},
			},
		},
		{
			description: "unauthenticated shouldn't even include extra provided info.",
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: false,
				User: authenticationv1.UserInfo{
					Username: "garbage",
					UID:      "abcd-1234",
					Groups:   []string{"not-actually-used"},
				},
			},
			expectedAuthenticated: false,
			expectedUser:          nil,
		},
		{
			description: "unauthenticated shouldn't even include extra provided info.",
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: false,
			},
			expectedAuthenticated: false,
			expectedUser:          nil,
		},
		{
			description:  "good audience",
			implicitAuds: apiAuds,
			reqAuds:      apiAuds,
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
			},
			expectedAuthenticated: true,
			expectedUser: &user.DefaultInfo{
				Name: "somebody",
			},
			expectedAuds: apiAuds,
		},
		{
			description:  "good audience",
			implicitAuds: append(apiAuds, "other"),
			reqAuds:      apiAuds,
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
			},
			expectedAuthenticated: true,
			expectedUser: &user.DefaultInfo{
				Name: "somebody",
			},
			expectedAuds: apiAuds,
		},
		{
			description:  "bad audiences",
			implicitAuds: apiAuds,
			reqAuds:      authenticator.Audiences{"other"},
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: false,
			},
			expectedAuthenticated: false,
		},
		{
			description:  "bad audiences",
			implicitAuds: apiAuds,
			reqAuds:      authenticator.Audiences{"other"},
			// webhook 认证器尚未升级到支持 audience。
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
			},
			expectedAuthenticated: false,
		},
		{
			description:  "audience aware backend",
			implicitAuds: apiAuds,
			reqAuds:      apiAuds,
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
				Audiences: []string(apiAuds),
			},
			expectedAuthenticated: true,
			expectedUser: &user.DefaultInfo{
				Name: "somebody",
			},
			expectedAuds: apiAuds,
		},
		{
			description: "audience aware backend",
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
				Audiences: []string(apiAuds),
			},
			expectedAuthenticated: true,
			expectedUser: &user.DefaultInfo{
				Name: "somebody",
			},
		},
		{
			description:  "audience aware backend",
			implicitAuds: apiAuds,
			reqAuds:      apiAuds,
			serverResponse: authenticationv1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1.UserInfo{
					Username: "somebody",
				},
				Audiences: []string{"other"},
			},
			expectedAuthenticated: false,
		},
	}
	token := "my-s3cr3t-t0ken" // Fake token for testing.
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			wh, err := newV1TokenAuthenticator(s.URL, clientCert, clientKey, caCert, 0, tt.implicitAuds, noopAuthenticatorMetrics())
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.Background()
			if tt.reqAuds != nil {
				ctx = authenticator.WithAudiences(ctx, tt.reqAuds)
			}

			serv.response = tt.serverResponse
			resp, authenticated, err := wh.AuthenticateToken(ctx, token)
			if err != nil {
				t.Fatalf("authentication failed: %v", err)
			}
			if serv.lastRequest.Spec.Token != token {
				t.Errorf("Server did not see correct token. Got %q, expected %q.",
					serv.lastRequest.Spec.Token, token)
			}
			if !reflect.DeepEqual(serv.lastRequest.TypeMeta, expTypeMeta) {
				t.Errorf("Server did not see correct TypeMeta. Got %v, expected %v",
					serv.lastRequest.TypeMeta, expTypeMeta)
			}
			if authenticated != tt.expectedAuthenticated {
				t.Errorf("Plugin returned incorrect authentication response. Got %t, expected %t.",
					authenticated, tt.expectedAuthenticated)
			}
			if resp != nil && tt.expectedUser != nil && !reflect.DeepEqual(resp.User, tt.expectedUser) {
				t.Errorf("Plugin returned incorrect user. Got %#v, expected %#v",
					resp.User, tt.expectedUser)
			}
			if resp != nil && tt.expectedAuds != nil && !reflect.DeepEqual(resp.Audiences, tt.expectedAuds) {
				t.Errorf("Plugin returned incorrect audiences. Got %#v, expected %#v",
					resp.Audiences, tt.expectedAuds)
			}
		})
	}
}

// authenticationV1UserInfo 让 TokenReview 的 UserInfo 满足 user.Info 接口，
// 用于校验字段映射是否完整。
type authenticationV1UserInfo authenticationv1.UserInfo

func (a *authenticationV1UserInfo) GetName() string     { return a.Username }
func (a *authenticationV1UserInfo) GetUID() string      { return a.UID }
func (a *authenticationV1UserInfo) GetGroups() []string { return a.Groups }

// GetExtra 把 ExtraValue 转换为普通字符串切片，nil 时保持 nil。
func (a *authenticationV1UserInfo) GetExtra() map[string][]string {
	if a.Extra == nil {
		return nil
	}
	ret := map[string][]string{}
	for k, v := range a.Extra {
		ret[k] = []string(v)
	}

	return ret
}

// 确保 authenticationv1.UserInfo 含有实现所需的字段。
// user.Info interface.
var _ user.Info = (*authenticationV1UserInfo)(nil)

// TestV1WebhookCacheAndRetry 验证服务端的错误响应不会被缓存、成功响应会被缓存，
// 同时确保 webhook 调用在遇到 429 与 500+ 错误时会重试。
// 缓存 "forever" 让"未再次调用"成为缓存命中的证据；用例顺序
// 依赖前一个 200 响应写入缓存，因此不能单独打乱执行。
func TestV1WebhookCacheAndRetry(t *testing.T) {
	serv := new(mockV1Service)
	s, err := NewV1TestServer(serv, serverCert, serverKey, caCert)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 构造一个把成功响应缓存 "forever"（100 天）的认证器。
	// 成功响应长期缓存，使后续"期望 0 次调用"的用例可判定缓存生效。
	wh, err := newV1TokenAuthenticator(s.URL, clientCert, clientKey, caCert, 2400*time.Hour, nil, noopAuthenticatorMetrics())
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		description string

		token string
		allow bool
		code  int

		expectError bool
		expectOk    bool
		expectCalls int
	}{
		{
			description: "t0k3n, 500 error, retries and fails",

			token: "t0k3n",
			allow: false,
			code:  500,

			expectError: true,
			expectOk:    false,
			expectCalls: 5,
		},
		{
			description: "t0k3n, 404 error, fails (but no retry)",

			token: "t0k3n",
			allow: false,
			code:  404,

			expectError: true,
			expectOk:    false,
			expectCalls: 1,
		},
		{
			description: "t0k3n, 200 response, allowed, succeeds with a single call",

			token: "t0k3n",
			allow: true,
			code:  200,

			expectError: false,
			expectOk:    true,
			expectCalls: 1,
		},
		{
			description: "t0k3n, 500 response, disallowed, but never called because previous 200 response was cached",

			token: "t0k3n",
			allow: false,
			code:  500,

			expectError: false,
			expectOk:    true,
			expectCalls: 0,
		},

		{
			description: "an0th3r_t0k3n, 500 response, disallowed, should be called again with retries",

			token: "an0th3r_t0k3n",
			allow: false,
			code:  500,

			expectError: true,
			expectOk:    false,
			expectCalls: 5,
		},
		{
			description: "an0th3r_t0k3n, 429 response, disallowed, should be called again with retries",

			token: "an0th3r_t0k3n",
			allow: false,
			code:  429,

			expectError: true,
			expectOk:    false,
			expectCalls: 5,
		},
		{
			description: "an0th3r_t0k3n, 200 response, allowed, succeeds with a single call",

			token: "an0th3r_t0k3n",
			allow: true,
			code:  200,

			expectError: false,
			expectOk:    true,
			expectCalls: 1,
		},
		{
			description: "an0th3r_t0k3n, 500 response, disallowed, but never called because previous 200 response was cached",

			token: "an0th3r_t0k3n",
			allow: false,
			code:  500,

			expectError: false,
			expectOk:    true,
			expectCalls: 0,
		},
	}

	for _, testcase := range testcases {
		t.Run(testcase.description, func(t *testing.T) {
			serv.allow = testcase.allow
			serv.statusCode = testcase.code
			serv.called = 0

			_, ok, err := wh.AuthenticateToken(context.Background(), testcase.token)
			hasError := err != nil
			if hasError != testcase.expectError {
				t.Errorf("Webhook returned HTTP %d, expected error=%v, but got error %v", testcase.code, testcase.expectError, err)
			}
			if serv.called != testcase.expectCalls {
				t.Errorf("Expected %d calls, got %d", testcase.expectCalls, serv.called)
			}
			if ok != testcase.expectOk {
				t.Errorf("Expected ok=%v, got %v", testcase.expectOk, ok)
			}
		})
	}
}

// noopAuthenticatorMetrics 返回空实现的指标回调，避免测试触碰全局 registry。
func noopAuthenticatorMetrics() AuthenticatorMetrics {
	return AuthenticatorMetrics{
		RecordRequestTotal:   noopMetrics{}.RequestTotal,
		RecordRequestLatency: noopMetrics{}.RequestLatency,
	}
}
