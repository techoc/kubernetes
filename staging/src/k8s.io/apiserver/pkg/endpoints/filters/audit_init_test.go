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

package filters

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"k8s.io/apiserver/pkg/audit"
)

// TestWithAuditID 验证审计 ID 的选取与回显规则：
//   - 客户端自带 Audit-ID 时原样沿用；
//   - 未提供时由生成函数产生；
//   - 过长（超过 UUID 两倍）也不截断，因为审计 ID 是关联日志的关键，截断会破坏关联。
func TestWithAuditID(t *testing.T) {
	largeAuditID := fmt.Sprintf("%s-%s", uuid.New().String(), uuid.New().String())
	tests := []struct {
		name             string
		newAuditIDFunc   func() string
		auditIDSpecified string
		auditIDExpected  string
	}{
		{
			name:             "user specifies a value for Audit-ID in the request header",
			auditIDSpecified: "foo-bar-baz",
			auditIDExpected:  "foo-bar-baz",
		},
		{
			name: "user does not specify a value for Audit-ID in the request header",
			newAuditIDFunc: func() string {
				return "foo-bar-baz"
			},
			auditIDExpected: "foo-bar-baz",
		},
		{
			name:             "the value in Audit-ID request header is too large, should not be truncated",
			auditIDSpecified: largeAuditID,
			auditIDExpected:  largeAuditID,
		},
		{
			name: "the generated Audit-ID is too large, should not be truncated",
			newAuditIDFunc: func() string {
				return largeAuditID
			},
			auditIDExpected: largeAuditID,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			const auditKey = "Audit-ID"
			var (
				innerHandlerCallCount int
				auditIDGot            string
				found                 bool
			)
			handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
				innerHandlerCallCount++

				// 内层 handler 是否能看到审计 ID？
				// 内层 handler 必须能从 context 读到同一个 ID。
				v, ok := audit.AuditIDFrom(req.Context())

				found = ok
				auditIDGot = string(v)
			})

			wrapped := WithAuditInit(handler)
			// 生产入口 WithAuditInit 固定使用 UUID 生成器；测试替换为可控生成器。
			if test.newAuditIDFunc != nil {
				wrapped = withAuditInit(handler, test.newAuditIDFunc)
			}

			testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/namespaces", nil)
			if err != nil {
				t.Fatalf("failed to create new http request - %v", err)
			}
			if len(test.auditIDSpecified) > 0 {
				testRequest.Header.Set(auditKey, test.auditIDSpecified)
			}

			w := httptest.NewRecorder()
			wrapped.ServeHTTP(w, testRequest)

			if innerHandlerCallCount != 1 {
				t.Errorf("WithAuditID: expected the inner handler to be invoked once, but was invoked %d times", innerHandlerCallCount)
			}
			if !found {
				t.Error("WithAuditID: expected request.AuditIDFrom to return true, but got false")
			}
			if test.auditIDExpected != auditIDGot {
				t.Errorf("WithAuditID: expected the request context to have: %q, but got=%q", test.auditIDExpected, auditIDGot)
			}

			// 响应头必须回显审计 ID，客户端据此与服务端日志对账。
			auditIDEchoed := w.Header().Get(auditKey)
			if test.auditIDExpected != auditIDEchoed {
				t.Errorf("WithAuditID: expected Audit-ID response header: %q, but got: %q", test.auditIDExpected, auditIDEchoed)
			}
		})
	}
}
