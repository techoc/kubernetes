/*
Copyright 2015 The Kubernetes Authors.

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

package httpstream

import (
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"testing"
)

// 最小 ResponseWriter 桩：只记录头与状态码，便于断言 Handshake 写出的内容。
// 用指针保存状态码，以区分“未写入”和“状态码为 0”。
type responseWriter struct {
	header     http.Header
	statusCode *int
}

func newResponseWriter() *responseWriter {
	return &responseWriter{
		header: make(http.Header),
	}
}

func (r *responseWriter) Header() http.Header {
	return r.header
}

func (r *responseWriter) WriteHeader(code int) {
	// 取局部变量地址，避免复用循环变量导致记录的状态码串味。
	r.statusCode = &code
}

func (r *responseWriter) Write([]byte) (int, error) {
	return 0, nil
}

// 用表驱动覆盖协议协商的成功与失败分支，并核对失败时写回的 HTTP 状态码与响应头。
func TestHandshake(t *testing.T) {
	tests := map[string]struct {
		clientProtocols    []string
		serverProtocols    []string
		expectedProtocol   string
		expectError        bool
		expectedStatusCode int
	}{
		"no protocol": {
			clientProtocols:    []string{},
			serverProtocols:    []string{"a", "b"},
			expectedProtocol:   "",
			expectError:        true,
			expectedStatusCode: http.StatusBadRequest,
		},
		"empty client protocol header": {
			clientProtocols:    []string{",, "},
			serverProtocols:    []string{"a", "b"},
			expectedProtocol:   "",
			expectError:        true,
			expectedStatusCode: http.StatusBadRequest,
		},
		"no common protocol": {
			clientProtocols:    []string{"c"},
			serverProtocols:    []string{"a", "b"},
			expectedProtocol:   "",
			expectError:        true,
			expectedStatusCode: http.StatusForbidden,
		},
		"no common protocol with comma separated list": {
			// 客户端把多个协议塞进同一个头，服务端必须拆开逐个比较。
			clientProtocols:    []string{"c, d"},
			serverProtocols:    []string{"a", "b"},
			expectedProtocol:   "",
			expectError:        true,
			expectedStatusCode: http.StatusForbidden,
		},
		"common protocol": {
			clientProtocols:  []string{"b"},
			serverProtocols:  []string{"a", "b"},
			expectedProtocol: "b",
		},
		"common protocol with comma separated list": {
			// 即使 b 不在首个位置，也要从逗号列表里挑出与服务端匹配的协议。
			clientProtocols:  []string{"b, c"},
			serverProtocols:  []string{"a", "b"},
			expectedProtocol: "b",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://www.example.com/", nil)
			if err != nil {
				t.Fatalf("%s: error creating request: %v", name, err)
			}

			for _, p := range test.clientProtocols {
				req.Header.Add(HeaderProtocolVersion, p)
			}

			w := newResponseWriter()
			negotiated, err := Handshake(req, w, test.serverProtocols)

			// 核对协商出的协议是否符合预期。
			if e, a := test.expectedProtocol, negotiated; e != a {
				t.Fatalf("%s: protocol: expected %q, got %q", name, e, a)
			}

			if test.expectError {
				if err == nil {
					t.Fatalf("%s: expected error but did not get one", name)
				}
				if w.statusCode == nil {
					t.Fatalf("%s: expected w.statusCode to be set", name)
				} else if e, a := test.expectedStatusCode, *w.statusCode; e != a {
					t.Fatalf("%s: w.statusCode: expected %d, got %d", name, e, a)
				}
				if test.expectedStatusCode == http.StatusForbidden {
					// 协商失败时把服务端可接受的协议列表回给客户端，便于其重试。
					if e, a := test.serverProtocols, w.Header()[HeaderAcceptedProtocolVersions]; !reflect.DeepEqual(e, a) {
						t.Fatalf("%s: accepted server protocols: expected %v, got %v", name, e, a)
					}
				}
				return
			}
			if !test.expectError && err != nil {
				t.Fatalf("%s: unexpected error: %v", name, err)
			}
			if w.statusCode != nil {
				t.Fatalf("%s: unexpected non-nil w.statusCode: %d", name, *w.statusCode)
			}

			if len(test.expectedProtocol) == 0 {
				if len(w.Header()[HeaderProtocolVersion]) > 0 {
					t.Fatalf("%s: unexpected protocol version response header: %s", name, w.Header()[HeaderProtocolVersion])
				}
				return
			}

			// 核对响应头是否回写了协商出的协议。
			if e, a := []string{test.expectedProtocol}, w.Header()[HeaderProtocolVersion]; !reflect.DeepEqual(e, a) {
				t.Fatalf("%s: protocol response header: expected %v, got %v", name, e, a)
			}
		})
	}

	// 服务端协议列表为空属于配置错误，应该 panic 而不是静默拒绝所有客户端。
	t.Run("empty server protocols should panic", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic")
			}
		}()
		req, _ := http.NewRequest(http.MethodGet, "http://www.example.com/", nil)
		req.Header.Add(HeaderProtocolVersion, "a")
		w := newResponseWriter()
		_, _ = Handshake(req, w, []string{})
	})
}

// 覆盖 errors.As 的穿透行为：被 fmt.Errorf 包裹的 UpgradeFailureError 也应识别为升级失败。
func TestIsUpgradeFailureError(t *testing.T) {
	testCases := map[string]struct {
		err      error
		expected bool
	}{
		"nil error should return false": {
			err:      nil,
			expected: false,
		},
		"Non-upgrade error should return false": {
			err:      fmt.Errorf("this is not an upgrade error"),
			expected: false,
		},
		"UpgradeFailure error should return true": {
			err:      &UpgradeFailureError{},
			expected: true,
		},
		"Wrapped Non-UpgradeFailure error should return false": {
			err:      fmt.Errorf("%s: %w", "first error", errors.New("Non-upgrade error")),
			expected: false,
		},
		"Wrapped UpgradeFailure error should return true": {
			err:      fmt.Errorf("%s: %w", "first error", &UpgradeFailureError{}),
			expected: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := IsUpgradeFailure(test.err)
			if test.expected != actual {
				t.Errorf("expected upgrade failure %t, got %t", test.expected, actual)
			}
		})
	}
}

// 按错误文本匹配已知的 HTTPS 代理报错，确保误判不会掩盖真正的升级失败原因。
func TestIsHTTPSProxyError(t *testing.T) {
	testCases := map[string]struct {
		err      error
		expected bool
	}{
		"nil error should return false": {
			err:      nil,
			expected: false,
		},
		"Not HTTPS proxy error should return false": {
			err:      errors.New("this is not an upgrade error"),
			expected: false,
		},
		"HTTPS proxy error should return true": {
			err:      errors.New("proxy: unknown scheme: https"),
			expected: true,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := IsHTTPSProxyError(test.err)
			if test.expected != actual {
				t.Errorf("expected HTTPS proxy error %t, got %t", test.expected, actual)
			}
		})
	}
}
