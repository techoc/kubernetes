/*
Copyright 2021 The Kubernetes Authors.

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
	"net/http"
	"testing"

	authorizationv1 "k8s.io/api/authorization/v1"
	"k8s.io/apiserver/pkg/apis/apiserver"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/authorization/cel"
)

// TestAuthorizerMetrics 验证 webhook 授权器在各种结果下写入的指标标签：
// 成功、超时、取消、服务端错误与 TLS 失败分别对应不同的 result。
// 这些标签是对外暴露的指标契约，因此用测试固定下来。
func TestAuthorizerMetrics(t *testing.T) {
	scenarios := []struct {
		name                            string
		canceledRequest                 bool
		clientCert, clientKey, clientCA []byte
		serverCert, serverKey, serverCA []byte
		authzFakeServiceStatusCode      int
		authFakeServiceDeny             bool
		expectedRegisteredStatusCode    string
		expectEvalutionResult           string
		expectDurationResult            string
		expectFailOpenResult            string
		wantErr                         bool
	}{
		{
			name:       "happy path",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: caCert,
			expectedRegisteredStatusCode: "200",
			expectEvalutionResult:        "success",
			expectDurationResult:         "success",
			expectFailOpenResult:         "",
		},

		{
			name:       "timed out request",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: caCert,
			authzFakeServiceStatusCode:   http.StatusGatewayTimeout,
			expectedRegisteredStatusCode: "504",
			expectEvalutionResult:        "timeout",
			expectDurationResult:         "timeout",
			expectFailOpenResult:         "timeout",
		},

		{
			name:       "canceled request",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: caCert,
			canceledRequest:              true,
			expectedRegisteredStatusCode: "<error>",
			expectEvalutionResult:        "canceled",
			expectDurationResult:         "canceled",
			expectFailOpenResult:         "",
		},

		{
			name:       "an internal error returned from the webhook",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: caCert,
			authzFakeServiceStatusCode:   500,
			expectedRegisteredStatusCode: "500",
			expectEvalutionResult:        "error",
			expectDurationResult:         "error",
			expectFailOpenResult:         "error",
		},

		{
			name:       "incorrect client certificate used, the webhook not called, an error is recorded",
			clientCert: clientCert, clientKey: clientKey, clientCA: caCert,
			serverCert: serverCert, serverKey: serverKey, serverCA: badCACert,
			expectedRegisteredStatusCode: "<error>",
			expectEvalutionResult:        "error",
			expectDurationResult:         "error",
			expectFailOpenResult:         "error",
			wantErr:                      true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			service := new(mockV1Service)
			service.statusCode = scenario.authzFakeServiceStatusCode
			if service.statusCode == 0 {
				service.statusCode = 200
			}

			testFinishedCtx, testFinishedCancel := context.WithCancel(context.Background())
			defer testFinishedCancel()
			if scenario.canceledRequest {
				service.reviewHook = func(*authorizationv1.SubjectAccessReview) {
					cancel()
					// net/http transport 在处理 context 取消时，
					// 仍可能尝试使用此刻可用的响应，
					// 因此需要延迟响应。
					<-testFinishedCtx.Done()
				}
			}
			service.allow = !scenario.authFakeServiceDeny

			server, err := NewV1TestServer(service, scenario.serverCert, scenario.serverKey, scenario.serverCA)
			if err != nil {
				t.Errorf("%s: failed to create server: %v", scenario.name, err)
				return
			}
			defer server.Close()
			// testFinishedCtx 必须在关闭 server 前取消，否则
			// 关闭 server 会卡在 listener 的活跃连接上。
			defer testFinishedCancel()

			fakeAuthzMetrics := &fakeAuthorizerMetrics{}
			wh, err := newV1Authorizer(server.URL, scenario.clientCert, scenario.clientKey, scenario.clientCA, 0, fakeAuthzMetrics, cel.NewDefaultCompiler(), []apiserver.WebhookMatchCondition{}, "")
			if err != nil {
				t.Error("failed to create client")
				return
			}

			attr := authorizer.AttributesRecord{User: &user.DefaultInfo{}}
			_, _, err = wh.Authorize(ctx, attr)
			if scenario.wantErr {
				if err == nil {
					t.Errorf("expected error making authorization request: %v", err)
				}
			}

			if fakeAuthzMetrics.totalCode != scenario.expectedRegisteredStatusCode {
				t.Errorf("incorrect status code recorded for RecordRequestTotal method, expected = %v, got %v", scenario.expectedRegisteredStatusCode, fakeAuthzMetrics.totalCode)
			}

			if fakeAuthzMetrics.latencyCode != scenario.expectedRegisteredStatusCode {
				t.Errorf("incorrect status code recorded for RecordRequestLatency method, expected = %v, got %v", scenario.expectedRegisteredStatusCode, fakeAuthzMetrics.latencyCode)
			}

			if fakeAuthzMetrics.evaluationsResult != scenario.expectEvalutionResult {
				t.Errorf("expected evaluationsResult %q, got %q", scenario.expectEvalutionResult, fakeAuthzMetrics.evaluationsResult)
			}
			if fakeAuthzMetrics.durationResult != scenario.expectDurationResult {
				t.Errorf("expected durationResult %q, got %q", scenario.expectDurationResult, fakeAuthzMetrics.durationResult)
			}
			if fakeAuthzMetrics.failOpenResult != scenario.expectFailOpenResult {
				t.Errorf("expected failOpenResult %q, got %q", scenario.expectFailOpenResult, fakeAuthzMetrics.failOpenResult)
			}
		})
	}
}

type fakeAuthorizerMetrics struct {
	// totalCode 记录 RecordRequestTotal 收到的状态码标签。
	totalCode string

	latency     float64
	latencyCode string

	evaluations       int
	evaluationsResult string

	duration       float64
	durationResult string

	failOpen       int
	failOpenResult string

	cel.NoopMatcherMetrics
}

// RecordRequestTotal 记录请求总数所用的状态码。
func (f *fakeAuthorizerMetrics) RecordRequestTotal(_ context.Context, code string) {
	f.totalCode = code
}

// RecordRequestLatency 记录请求延迟及对应状态码。
func (f *fakeAuthorizerMetrics) RecordRequestLatency(_ context.Context, code string, latency float64) {
	f.latency = latency
	f.latencyCode = code
}

// RecordWebhookEvaluation 记录 webhook 求值结果。
func (f *fakeAuthorizerMetrics) RecordWebhookEvaluation(ctx context.Context, name, result string) {
	f.evaluations += 1
	f.evaluationsResult = result
}

// RecordWebhookDuration 记录 webhook 调用耗时及结果。
func (f *fakeAuthorizerMetrics) RecordWebhookDuration(ctx context.Context, name, result string, duration float64) {
	f.duration = duration
	f.durationResult = result
}

// RecordWebhookFailOpen 记录 fail-open 次数及原因。
func (f *fakeAuthorizerMetrics) RecordWebhookFailOpen(ctx context.Context, name, result string) {
	f.failOpen += 1
	f.failOpenResult = result
}
