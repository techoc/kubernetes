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

	authenticationv1beta1 "k8s.io/api/authentication/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/token/cache"
	"k8s.io/apiserver/pkg/authentication/user"
	webhookutil "k8s.io/apiserver/pkg/util/webhook"
	v1 "k8s.io/client-go/tools/clientcmd/api/v1"
)

// apiAuds 是测试中 apiserver 的隐式 audience 集合。
var apiAuds = authenticator.Audiences{"api"}

// V1beta1Service 模拟远端认证服务。
type V1beta1Service interface {
	// Review 检查 TokenReviewSpec，并在 TokenReviewStatus 中给出认证响应。
	//
	Review(*authenticationv1beta1.TokenReview)
	HTTPStatusCode() int
}

// NewV1beta1TestServer 把 V1beta1Service 包装为 httptest.Server。
//
// 服务端强制以 v1beta1 收发，并可选地要求客户端证书，
// 用来验证认证器在旧版本协议与双向 TLS 下的行为。
func NewV1beta1TestServer(s V1beta1Service, cert, key, caCert []byte) (*httptest.Server, error) {
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
		// 配置了 CA 即要求并校验客户端证书，覆盖双向 TLS 场景。
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

		var review authenticationv1beta1.TokenReview
		bodyData, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(bodyData, &review); err != nil {
			http.Error(w, fmt.Sprintf("failed to decode body: %v", err), http.StatusBadRequest)
			return
		}
		// 确认按预期收到了序列化后的 tokenreview。
		if review.APIVersion != "authentication.k8s.io/v1beta1" {
			http.Error(w, fmt.Sprintf("wrong api version: %s", string(bodyData)), http.StatusBadRequest)
			return
		}
		// 一旦请求成功，始终调用 review，以记录它被调用过。
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
			APIVersion: authenticationv1beta1.SchemeGroupVersion.String(),
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

	// 调整路径，指向我们自定义的路径。
	serverURL, _ := url.Parse(server.URL)
	serverURL.Path = webhookPath
	server.URL = serverURL.String()

	return server, nil
}

// 一个可设置为对认证请求回答 yes 或 no 的服务。
type mockV1beta1Service struct {
	allow      bool
	statusCode int
	called     int
}

// Review 按 allow 设置认证结果，并记录调用次数。
func (m *mockV1beta1Service) Review(r *authenticationv1beta1.TokenReview) {
	m.called++
	r.Status.Authenticated = m.allow
	if m.allow {
		r.Status.User.Username = "realHooman@email.com"
	}
}

// Allow / Deny / HTTPStatusCode 分别控制认证结果与响应状态码。
func (m *mockV1beta1Service) Allow()              { m.allow = true }
func (m *mockV1beta1Service) Deny()               { m.allow = false }
func (m *mockV1beta1Service) HTTPStatusCode() int { return m.statusCode }

// newV1beta1TokenAuthenticator 根据给定参数创建临时 kubeconfig 文件，
// 并尝试从中加载新的 WebhookTokenAuthenticator。
//
// 刻意走真实 kubeconfig 加载路径，以覆盖证书/CA 的解析与校验逻辑，
// 而不是直接构造 rest.Config 绕过它们。
func newV1beta1TokenAuthenticator(serverURL string, clientCert, clientKey, ca []byte, cacheTime time.Duration, implicitAuds authenticator.Audiences) (authenticator.Token, error) {
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

	c, err := tokenReviewInterfaceFromConfig(clientConfig, "v1beta1", testRetryBackoff)
	if err != nil {
		return nil, err
	}

	authn, err := newWithBackoff(c, testRetryBackoff, implicitAuds, 10*time.Second, AuthenticatorMetrics{
		RecordRequestTotal:   noopMetrics{}.RequestTotal,
		RecordRequestLatency: noopMetrics{}.RequestLatency,
	})
	if err != nil {
		return nil, err
	}

	return cache.New(authn, false, cacheTime, cacheTime), nil
}

// TestV1beta1TLSConfig 覆盖客户端与服务端之间各种 TLS 组合：
// 正常双向认证、单/双向可选、CA 不匹配（双向）以及不安全连接必须被拒绝。
func TestV1beta1TLSConfig(t *testing.T) {
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
		// 用闭包让 defer 在每次循环迭代之间触发。
		func() {
			service := new(mockV1beta1Service)
			service.statusCode = 200

			server, err := NewV1beta1TestServer(service, tt.serverCert, tt.serverKey, tt.serverCA)
			if err != nil {
				t.Errorf("%s: failed to create server: %v", tt.test, err)
				return
			}
			defer server.Close()

			wh, err := newV1beta1TokenAuthenticator(server.URL, tt.clientCert, tt.clientKey, tt.clientCA, 0, nil)
			if err != nil {
				t.Errorf("%s: failed to create client: %v", tt.test, err)
				return
			}

			// 全部放行，看看是否会报错。
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

// recorderV1beta1Service 记录所有 token review 请求，并给出响应。
// provided TokenReviewStatus.
type recorderV1beta1Service struct {
	lastRequest authenticationv1beta1.TokenReview
	response    authenticationv1beta1.TokenReviewStatus
}

// Review 记录收到的请求并回填预设状态。
func (rec *recorderV1beta1Service) Review(r *authenticationv1beta1.TokenReview) {
	rec.lastRequest = *r
	r.Status = rec.response
}

// HTTPStatusCode 固定返回 200。
func (rec *recorderV1beta1Service) HTTPStatusCode() int { return 200 }

// TestV1beta1WebhookTokenAuthenticator 验证用户信息透传与 audience 交集语义：
// 请求未限制 audience 时不校验；限制时只接受与 webhook/implicit 集合有交集的 token。
func TestV1beta1WebhookTokenAuthenticator(t *testing.T) {
	serv := &recorderV1beta1Service{}

	s, err := NewV1beta1TestServer(serv, serverCert, serverKey, caCert)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	expTypeMeta := metav1.TypeMeta{
		APIVersion: "authentication.k8s.io/v1beta1",
		Kind:       "TokenReview",
	}

	tests := []struct {
		description           string
		implicitAuds, reqAuds authenticator.Audiences
		serverResponse        authenticationv1beta1.TokenReviewStatus
		expectedAuthenticated bool
		expectedUser          *user.DefaultInfo
		expectedAuds          authenticator.Audiences
	}{
		{
			description: "successful response should pass through all user info.",
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
					Username: "person@place.com",
					UID:      "abcd-1234",
					Groups:   []string{"stuff-dev", "main-eng"},
					Extra:    map[string]authenticationv1beta1.ExtraValue{"foo": {"bar", "baz"}},
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: false,
				User: authenticationv1beta1.UserInfo{
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: false,
			},
			expectedAuthenticated: false,
			expectedUser:          nil,
		},
		{
			description:  "good audience",
			implicitAuds: apiAuds,
			reqAuds:      apiAuds,
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: false,
			},
			expectedAuthenticated: false,
		},
		{
			description:  "bad audiences",
			implicitAuds: apiAuds,
			reqAuds:      authenticator.Audiences{"other"},
			// webhook 认证器尚未升级以支持 audience。
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
					Username: "somebody",
				},
			},
			expectedAuthenticated: false,
		},
		{
			description:  "audience aware backend",
			implicitAuds: apiAuds,
			reqAuds:      apiAuds,
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
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
			serverResponse: authenticationv1beta1.TokenReviewStatus{
				Authenticated: true,
				User: authenticationv1beta1.UserInfo{
					Username: "somebody",
				},
				Audiences: []string{"other"},
			},
			expectedAuthenticated: false,
		},
	}
	token := "my-s3cr3t-t0ken" // 用于测试的假 token。
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			wh, err := newV1beta1TokenAuthenticator(s.URL, clientCert, clientKey, caCert, 0, tt.implicitAuds)
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

type authenticationV1beta1UserInfo authenticationv1beta1.UserInfo

// GetName / GetUID / GetGroups 代理到底层字段，使该类型满足 user.Info。
func (a *authenticationV1beta1UserInfo) GetName() string     { return a.Username }
func (a *authenticationV1beta1UserInfo) GetUID() string      { return a.UID }
func (a *authenticationV1beta1UserInfo) GetGroups() []string { return a.Groups }

// GetExtra 返回额外属性，并复制为 map[string][]string 以避免共享底层数据。
func (a *authenticationV1beta1UserInfo) GetExtra() map[string][]string {
	if a.Extra == nil {
		return nil
	}
	ret := map[string][]string{}
	for k, v := range a.Extra {
		ret[k] = []string(v)
	}

	return ret
}

// 确认 authenticationv1beta1.UserInfo 包含实现该接口所需的字段。
// user.Info interface.
var _ user.Info = (*authenticationV1beta1UserInfo)(nil)

// TestWebhookCache 验证服务端错误响应不会被缓存，成功响应会被缓存；
// 同时验证遇到 429 与 500+ 错误时会重试 webhook 调用。
// 遇到 429 与 500+ 错误时重试调用。
//
// 成功结果缓存后，后续相同 token 的请求不再打到 webhook（expectCalls=0）；
// 失败不缓存，因此 5xx/429 会重试并在下一次请求时再次尝试。
func TestV1beta1WebhookCacheAndRetry(t *testing.T) {
	serv := new(mockV1beta1Service)
	s, err := NewV1beta1TestServer(serv, serverCert, serverKey, caCert)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 创建一个“永久”（100 天）缓存成功响应的认证器。
	wh, err := newV1beta1TokenAuthenticator(s.URL, clientCert, clientKey, caCert, 2400*time.Hour, nil)
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
