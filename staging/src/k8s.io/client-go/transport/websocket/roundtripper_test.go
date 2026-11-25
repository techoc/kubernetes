/*
Copyright 2023 The Kubernetes Authors.

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

package websocket

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/remotecommand"
	restclient "k8s.io/client-go/rest"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// TestWebSocketRoundTripper_RoundTripperSucceeds 验证成功升级后，协商出的子协议与请求
// 指定的 V5 流协议一致。
// RoundTripperFor 返回两层——外层是普通 http.RoundTripper，内层持有已建立的
// WebSocket 连接（wsRt）。通过服务端配合完成 upgrade 后，可从 wsRt.Connection()
// 读到协商结果。
func TestWebSocketRoundTripper_RoundTripperSucceeds(t *testing.T) {
	// Create fake WebSocket server.
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w)
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
	}))
	defer websocketServer.Close()

	// Create the wrapped roundtripper and websocket upgrade roundtripper and call "RoundTrip()".
	websocketLocation, err := url.Parse(websocketServer.URL)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), "GET", websocketServer.URL, nil)
	require.NoError(t, err)
	rt, wsRt, err := RoundTripperFor(&restclient.Config{Host: websocketLocation.Host})
	require.NoError(t, err)
	requestedProtocol := remotecommand.StreamProtocolV5Name
	// 中文：通过 Sec-WebSocket-Protocol 头声明客户端支持的子协议。
	req.Header[wsstream.WebSocketProtocolHeader] = []string{requestedProtocol}
	_, err = rt.RoundTrip(req)
	require.NoError(t, err)
	// WebSocket Connection is stored in websocket RoundTripper.
	// Compare the expected negotiated subprotocol with the actual subprotocol.
	actualProtocol := wsRt.Connection().Subprotocol()
	assert.Equal(t, requestedProtocol, actualProtocol)

}

// TestWebSocketRoundTripper_RoundTripperFails 验证握手失败时错误的分类与透传。
// 所有失败都应包装为 httpstream.UpgradeFailureError（可用
// IsUpgradeFailure 判断）；若响应体是合法的 metav1.Status，错误里应能取出原始
// StatusError。statusCode 为 -1 表示不显式写状态码，从而模拟“非升级响应”的
// 基础 bad handshake。
func TestWebSocketRoundTripper_RoundTripperFails(t *testing.T) {
	testCases := map[string]struct {
		statusCode    int
		body          string
		status        *metav1.Status
		expectedError string
	}{
		"Empty response status still returns basic websocket error": {
			// 中文：不写状态码也不带升级头，得到底层 websocket 库的握手错误。
			statusCode:    -1,
			body:          "",
			expectedError: "websocket: bad handshake",
		},
		"Empty response body still returns status": {
			// 中文：仅状态码，无正文，错误信息里应包含状态码文本。
			statusCode:    http.StatusForbidden,
			body:          "",
			expectedError: "(403 Forbidden)",
		},
		"Error response body returned as string when can not be cast as metav1.Status": {
			// 中文：正文不是合法 Status，应原样拼接进错误信息。
			statusCode:    http.StatusForbidden,
			body:          "RBAC violated",
			expectedError: "(403 Forbidden): RBAC violated",
		},
		"Error returned as metav1.Status within response body": {
			// 中文：正文是合法 Status，应解码为 StatusError 保留结构化字段。
			statusCode: http.StatusBadRequest,
			body:       "",
			status: &metav1.Status{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "meta.k8s.io/v1",
					Kind:       "Status",
				},
				Status: "Failure",
				Reason: "Unable to negotiate sub-protocol",
				Code:   http.StatusBadRequest,
			},
		},
	}
	encoder := statusCodecs.LegacyCodec(metav1.SchemeGroupVersion)
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			// Create fake WebSocket server.
			websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				// 中文：按用例构造失败响应，不执行升级。
				if testCase.statusCode > 0 {
					w.WriteHeader(testCase.statusCode)
				}
				if testCase.status != nil {
					statusBytes, err := runtime.Encode(encoder, testCase.status)
					if err != nil {
						t.Errorf("unexpected error %v", err)
						return
					}
					_, err = w.Write(statusBytes)
					if err != nil {
						t.Errorf("unexpected error %v", err)
						return
					}
				} else if len(testCase.body) > 0 {
					_, err := w.Write([]byte(testCase.body))
					if err != nil {
						t.Errorf("unexpected error %v", err)
						return
					}
				}
			}))
			defer websocketServer.Close()

			// Create the wrapped roundtripper and websocket upgrade roundtripper and call "RoundTrip()".
			websocketLocation, err := url.Parse(websocketServer.URL)
			require.NoError(t, err)
			req, err := http.NewRequestWithContext(context.Background(), "GET", websocketServer.URL, nil)
			require.NoError(t, err)
			rt, _, err := RoundTripperFor(&restclient.Config{Host: websocketLocation.Host})
			require.NoError(t, err)
			_, err = rt.RoundTrip(req)
			require.Error(t, err)
			assert.True(t, httpstream.IsUpgradeFailure(err))
			if testCase.status != nil {
				// 中文：结构化错误需可经 errors.As 逐层还原为 StatusError。
				upgradeErr := &httpstream.UpgradeFailureError{}
				validErr := errors.As(err, &upgradeErr)
				assert.True(t, validErr, "could not cast error as httpstream.UpgradeFailureError")
				statusErr := upgradeErr.Cause
				apiErr := &apierrors.StatusError{}
				validErr = errors.As(statusErr, &apiErr)
				assert.True(t, validErr, "could not cast error as apierrors.StatusError")
				assert.Equal(t, *testCase.status, apiErr.ErrStatus)
			} else {
				assert.Contains(t, err.Error(), testCase.expectedError,
					"expected (%s), got (%s)", testCase.expectedError, err.Error())
			}
		})
	}
}

// TestWebSocketRoundTripper_NegotiateCreatesConnection 验证 Negotiate 直接完成升级
// 并返回可用的 WebSocket 连接。
//
// 中文：与 RoundTrip 路径不同，Negotiate 不经过外层 RoundTripper 的响应处理。
func TestWebSocketRoundTripper_NegotiateCreatesConnection(t *testing.T) {
	// Create fake WebSocket server.
	websocketServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		conns, err := webSocketServerStreams(req, w)
		if err != nil {
			t.Fatalf("error on webSocketServerStreams: %v", err)
		}
		defer conns.conn.Close()
	}))
	defer websocketServer.Close()

	// Create the websocket roundtripper and call "Negotiate" to create websocket connection.
	websocketLocation, err := url.Parse(websocketServer.URL)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(context.Background(), "GET", websocketServer.URL, nil)
	require.NoError(t, err)
	rt, wsRt, err := RoundTripperFor(&restclient.Config{Host: websocketLocation.Host})
	require.NoError(t, err)
	requestedProtocol := remotecommand.StreamProtocolV5Name
	conn, err := Negotiate(rt, wsRt, req, requestedProtocol)
	require.NoError(t, err)
	// Compare the expected negotiated subprotocol with the actual subprotocol.
	actualProtocol := conn.Subprotocol()
	assert.Equal(t, requestedProtocol, actualProtocol)
}

// websocketStreams contains the WebSocket connection and streams from a server.
type websocketStreams struct {
	conn io.Closer
}

// webSocketServerStreams 在服务端接受升级，并注册 V5 协议所需的通道配置。
// idle timeout 设为 4 小时，远大于测试时长，避免连接在断言前因空闲被
// 关闭而产生偶发失败。
func webSocketServerStreams(req *http.Request, w http.ResponseWriter) (*websocketStreams, error) {
	conn := wsstream.NewConn(map[string]wsstream.ChannelProtocolConfig{
		remotecommand.StreamProtocolV5Name: {
			Binary:   true,
			Channels: []wsstream.ChannelType{},
		},
	})
	conn.SetIdleTimeout(4 * time.Hour)
	// Opening the connection responds to WebSocket client, negotiating
	// the WebSocket upgrade connection and the subprotocol.
	_, _, err := conn.Open(w, req)
	if err != nil {
		return nil, err
	}
	return &websocketStreams{conn: conn}, nil
}
