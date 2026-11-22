/*
Copyright 2018 The Kubernetes Authors.

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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"k8s.io/api/admission/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/testcerts"
	testingclock "k8s.io/utils/clock/testing"
)

// NewTestServerWithHandler returns a webhook test HTTPS server
// which uses given handler function to handle requests
//
// 强制要求客户端证书（RequireAndVerifyClientCert），以便同时覆盖
// apiserver 作为客户端时双向 TLS 的路径。
func NewTestServerWithHandler(t testing.TB, handler func(http.ResponseWriter, *http.Request)) *httptest.Server {
	// Create the test webhook server
	sCert, err := tls.X509KeyPair(testcerts.ServerCert, testcerts.ServerKey)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM(testcerts.CACert)
	testServer := httptest.NewUnstartedServer(http.HandlerFunc(handler))
	testServer.TLS = &tls.Config{
		Certificates: []tls.Certificate{sCert},
		ClientCAs:    rootCAs,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	return testServer
}

// NewTestServer returns a webhook test HTTPS server with fixed webhook test certs.
func NewTestServer(t testing.TB) *httptest.Server {
	return NewTestServerWithHandler(t, webhookHandler)
}

// webhookHandler 按路径模拟 webhook 的各种响应形态：正常放行/拒绝、
// 非法响应体、非法 patch、审计注解校验以及底层连接异常。
func webhookHandler(w http.ResponseWriter, r *http.Request) {
	// fmt.Printf("got req: %v\n", r.URL.Path)
	switch r.URL.Path {
	case "/internalErr":
		http.Error(w, "webhook internal server error", http.StatusInternalServerError)
		return
	case "/invalidReq":
		// 101 Switching Protocols 不是合法的 AdmissionReview 响应，
		// 用于验证 apiserver 对意外状态码的容错。
		w.WriteHeader(http.StatusSwitchingProtocols)
		w.Write([]byte("webhook invalid request"))
		return
	case "/invalidResp":
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("webhook invalid response"))
	case "/disallow":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Code: http.StatusForbidden,
				},
			},
		})
	case "/disallowReason":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "you shall not pass",
					Code:    http.StatusForbidden,
				},
			},
		})
	case "/shouldNotBeCalled":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Message: "doesn't expect labels to match object selector",
					Code:    http.StatusForbidden,
				},
			},
		})
	case "/allow":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: true,
				AuditAnnotations: map[string]string{
					"key1": "value1",
				},
			},
		})
	case "/removeLabel":
		w.Header().Set("Content-Type", "application/json")
		pt := v1beta1.PatchTypeJSONPatch
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed:   true,
				PatchType: &pt,
				Patch:     []byte(`[{"op": "remove", "path": "/metadata/labels/remove"}]`),
				AuditAnnotations: map[string]string{
					"key1": "value1",
				},
			},
		})
	case "/addLabel":
		w.Header().Set("Content-Type", "application/json")
		pt := v1beta1.PatchTypeJSONPatch
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed:   true,
				PatchType: &pt,
				Patch:     []byte(`[{"op": "add", "path": "/metadata/labels/added", "value": "test"}]`),
			},
		})
	case "/invalidPatch":
		w.Header().Set("Content-Type", "application/json")
		pt := v1beta1.PatchTypeJSONPatch
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed:   true,
				PatchType: &pt,
				Patch:     []byte(`[{`),
			},
		})
	case "/invalidMutation":
		w.Header().Set("Content-Type", "application/json")
		pt := v1beta1.PatchTypeJSONPatch
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed:   true,
				PatchType: &pt,
				Patch:     []byte(`[{"op": "add", "CORRUPTED_KEY":}]`),
			},
		})
	case "/nonStatusError":
		// 直接 hijack 连接并写入非 HTTP 内容，模拟网络层故障而非合法
		// 状态码，用于覆盖“无法解析响应”的错误分支。
		hj, _ := w.(http.Hijacker)
		conn, _, _ := hj.Hijack()
		defer conn.Close()             //nolint:errcheck
		conn.Write([]byte("bad-http")) //nolint:errcheck
	case "/nilResponse":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{})
	case "/invalidAnnotation":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: true,
				AuditAnnotations: map[string]string{
					"invalid*key": "value1",
				},
			},
		})
	case "/noop":
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: true,
			},
		})
	default:
		http.NotFound(w, r)
	}
}

// ClockSteppingWebhookHandler given a fakeClock returns a request handler
// that moves time in given clock by an amount specified in the webhook request
//
// 路径形如 /allow/300 或 /disallow/300，末尾数字是希望推进的（纳秒级）
// 时长；这样无需真实等待即可测试超时与重试逻辑。
func ClockSteppingWebhookHandler(t testing.TB, fakeClock *testingclock.FakeClock) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		validPath := regexp.MustCompile(`^/(?:allow|disallow)/(\d{1,10})$`)

		// 路径写错属于测试代码缺陷，直接失败以免产生难以诊断的假阳性。
		if !validPath.MatchString(path) {
			t.Errorf("error in test case, wrong webhook url path: '%q' expected to match: '%q'", path, validPath.String())
			t.FailNow()
		}

		delay, _ := strconv.ParseInt(validPath.FindStringSubmatch(path)[1], 0, 64)
		fakeClock.Step(time.Duration(delay))
		w.Header().Set("Content-Type", "application/json")

		if strings.HasPrefix(path, "/allow/") {
			json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
				Response: &v1beta1.AdmissionResponse{
					Allowed: true,
					AuditAnnotations: map[string]string{
						"key1": "value1",
					},
				},
			})
			return
		}
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: false,
				Result: &metav1.Status{
					Code: http.StatusForbidden,
				},
			},
		})
	}
}
