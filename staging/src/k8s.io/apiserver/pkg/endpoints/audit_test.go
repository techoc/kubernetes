/*
Copyright 2017 The Kubernetes Authors.

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

package endpoints

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/endpoints/request"
	genericapitesting "k8s.io/apiserver/pkg/endpoints/testing"
	"k8s.io/apiserver/pkg/registry/rest"
)

// fakeAuditSink 是线程安全的审计事件收集器：审计后端可能从
// 多个 goroutine 投递事件（例如异步批处理）。
type fakeAuditSink struct {
	lock   sync.Mutex
	events []*auditinternal.Event
}

// ProcessEvents 深拷贝事件后保存，避免被测代码后续修改事件影响断言。
func (s *fakeAuditSink) ProcessEvents(evs ...*auditinternal.Event) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, ev := range evs {
		e := ev.DeepCopy()
		s.events = append(s.events, e)
	}
	return true
}

// Events 返回当前事件的副本，供测试在不持锁的情况下断言。
func (s *fakeAuditSink) Events() []*auditinternal.Event {
	s.lock.Lock()
	defer s.lock.Unlock()
	return append([]*auditinternal.Event{}, s.events...)
}

// TestAudit 驱动完整的 REST 端点（get/list/create/delete/update/patch/watch），
// 断言每种请求产生的审计事件数量、阶段顺序、请求/响应体可见性以及
// User-Agent/状态码等字段。
// 重点在于验证审计策略对敏感时机的取舍：例如列表响应会被记录，
// watch 的响应体不会被记录，被路由层提前拒绝的请求不会带请求体。
func TestAudit(t *testing.T) {
	type eventCheck func(events []*auditinternal.Event) error

	// fixtures
	simpleFoo := &genericapitesting.Simple{Other: "foo"}
	simpleFooJSON, _ := runtime.Encode(testCodec, simpleFoo)

	simpleCPrime := &genericapitesting.Simple{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "other"},
		Other:      "bla",
	}
	simpleCPrimeJSON, _ := runtime.Encode(testCodec, simpleCPrime)
	userAgent := "audit-test"

	// event checks
	noRequestBody := func(i int) eventCheck {
		return func(events []*auditinternal.Event) error {
			if events[i].RequestObject == nil {
				return nil
			}
			return fmt.Errorf("expected RequestBody to be nil, got non-nil '%s'", events[i].RequestObject.Raw)
		}
	}
	requestBodyIs := func(i int, text string) eventCheck {
		return func(events []*auditinternal.Event) error {
			if events[i].RequestObject == nil {
				if text != "" {
					return fmt.Errorf("expected RequestBody %q, got <nil>", text)
				}
				return nil
			}
			if string(events[i].RequestObject.Raw) != text {
				return fmt.Errorf("expected RequestBody %q, got %q", text, string(events[i].RequestObject.Raw))
			}
			return nil
		}
	}
	requestBodyMatches := func(i int, pattern string) eventCheck {
		return func(events []*auditinternal.Event) error {
			if events[i].RequestObject == nil {
				return fmt.Errorf("expected non nil request object")
			}
			if matched, _ := regexp.Match(pattern, events[i].RequestObject.Raw); !matched {
				return fmt.Errorf("expected RequestBody to match %q, but didn't: %q", pattern, string(events[i].RequestObject.Raw))
			}
			return nil
		}
	}
	noResponseBody := func(i int) eventCheck {
		return func(events []*auditinternal.Event) error {
			if events[i].ResponseObject == nil {
				return nil
			}
			return fmt.Errorf("expected ResponseBody to be nil, got non-nil '%s'", events[i].ResponseObject.Raw)
		}
	}
	responseBodyMatches := func(i int, pattern string) eventCheck {
		return func(events []*auditinternal.Event) error {
			if events[i].ResponseObject == nil {
				return fmt.Errorf("expected non nil response object")
			}
			if matched, _ := regexp.Match(pattern, events[i].ResponseObject.Raw); !matched {
				return fmt.Errorf("expected ResponseBody to match %q, but didn't: %q", pattern, string(events[i].ResponseObject.Raw))
			}
			return nil
		}
	}
	// requestUserAgentMatches 要求所有事件都带上原始 UA，便于关联客户端。
	requestUserAgentMatches := func(userAgent string) eventCheck {
		return func(events []*auditinternal.Event) error {
			for i := range events {
				if events[i].UserAgent != userAgent {
					return fmt.Errorf("expected request user agent to match %q, but got: %q", userAgent, events[i].UserAgent)
				}
			}
			return nil
		}
	}
	// expectedStages 校验事件按阶段产生且顺序符合审计流程。
	expectedStages := func(stages ...auditinternal.Stage) eventCheck {
		return func(events []*auditinternal.Event) error {
			if len(stages) != len(events) {
				return fmt.Errorf("expected %d stages, but got %d events", len(stages), len(events))
			}
			for i, stage := range stages {
				if events[i].Stage != stage {
					return fmt.Errorf("expected stage %q, got %q", stage, events[i].Stage)
				}
			}
			return nil
		}
	}

	for _, test := range []struct {
		desc   string
		req    func(ctx context.Context, server string) (*http.Request, error)
		code   int
		events int
		checks []eventCheck
	}{
		{
			// GET 单对象：请求体不记录，响应体记录。
			"get",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodGet, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/simple/c", bytes.NewBuffer(simpleFooJSON))
			},
			200,
			2,
			[]eventCheck{
				noRequestBody(1),
				responseBodyMatches(1, `{.*"name":"c".*}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// LIST 带 labelSelector，响应体包含全部匹配对象。
			"list",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodGet, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/simple?labelSelector=a%3Dfoobar", nil)
			},
			200,
			2,
			[]eventCheck{
				noRequestBody(1),
				responseBodyMatches(1, `{.*"name":"a".*"name":"b".*}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// CREATE：请求体完整记录（受审计策略级别控制）。
			"create",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodPost, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple", bytes.NewBuffer(simpleFooJSON))
			},
			201,
			2,
			[]eventCheck{
				requestBodyIs(1, string(simpleFooJSON)),
				responseBodyMatches(1, `{.*"foo".*}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// 带名字的 POST 在路由层就被 405 拒绝，尚未读取请求体，
			// 因此审计事件中既无请求体也无响应体。
			"not-allowed-named-create",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodPost, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/named", bytes.NewBuffer(simpleFooJSON))
			},
			405,
			2,
			[]eventCheck{
				noRequestBody(1),  // the 405 is thrown long before the create handler would be executed
				noResponseBody(1), // the 405 is thrown long before the create handler would be executed
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// DELETE 无请求体，响应为 Status 对象。
			"delete",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodDelete, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/a", nil)
			},
			200,
			2,
			[]eventCheck{
				noRequestBody(1),
				responseBodyMatches(1, `{.*"kind":"Status".*"status":"Success".*}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// DELETE 可携带 DeleteOptions 请求体，需被记录。
			"delete-with-options-in-body",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodDelete, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/a", bytes.NewBuffer([]byte(`{"kind":"DeleteOptions"}`)))
			},
			200,
			2,
			[]eventCheck{
				requestBodyMatches(1, "DeleteOptions"),
				responseBodyMatches(1, `{.*"kind":"Status".*"status":"Success".*}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// UPDATE：请求体与响应体均记录。
			"update",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodPut, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/simple/c", bytes.NewBuffer(simpleCPrimeJSON))
			},
			200,
			2,
			[]eventCheck{
				requestBodyIs(1, string(simpleCPrimeJSON)),
				responseBodyMatches(1, `{.*"bla".*}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// 对象命名空间与 URL 命名空间不一致，返回 400，响应体是 Failure Status。
			"update-wrong-namespace",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodPut, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/default/simple/c", bytes.NewBuffer(simpleCPrimeJSON))
			},
			400,
			2,
			[]eventCheck{
				requestBodyIs(1, string(simpleCPrimeJSON)),
				responseBodyMatches(1, `"Status".*"status":"Failure".*"code":400}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// PATCH：记录原始 patch 字节，而非合并后的对象。
			"patch",
			func(ctx context.Context, server string) (*http.Request, error) {
				req, _ := http.NewRequestWithContext(ctx, request.MethodPatch, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/simple/c", bytes.NewReader([]byte(`{"labels":{"foo":"bar"}}`)))
				req.Header.Set("Content-Type", "application/merge-patch+json; charset=UTF-8")
				return req, nil
			},
			200,
			2,
			[]eventCheck{
				requestBodyIs(1, `{"labels":{"foo":"bar"}}`),
				responseBodyMatches(1, `"name":"c".*"labels":{"foo":"bar"}`),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseComplete),
			},
		},
		{
			// WATCH 有第三个事件（ResponseStarted 与 ResponseComplete 分离）；
			// 事件流可能包含大量对象，因此不记录请求/响应体。
			"watch",
			func(ctx context.Context, server string) (*http.Request, error) {
				return http.NewRequestWithContext(ctx, request.MethodGet, server+"/"+prefix+"/"+testGroupVersion.Group+"/"+testGroupVersion.Version+"/namespaces/other/simple?watch=true", nil)
			},
			200,
			3,
			[]eventCheck{
				noRequestBody(2),
				noResponseBody(2),
				expectedStages(auditinternal.StageRequestReceived, auditinternal.StageResponseStarted, auditinternal.StageResponseComplete),
			},
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := t.Context()
			sink := &fakeAuditSink{}
			handler := handleInternal(map[string]rest.Storage{
				"simple": &SimpleRESTStorage{
					list: []genericapitesting.Simple{
						{
							ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "other"},
							Other:      "foo",
						},
						{
							ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "other"},
							Other:      "foo",
						},
					},
					item: genericapitesting.Simple{
						ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "other", UID: "uid"},
						Other:      "foo",
					},
				},
			}, admissionControl, sink)

			server := httptest.NewServer(handler)
			defer server.Close()
			client := http.Client{Timeout: 2 * time.Second}

			req, err := test.req(ctx, server.URL)
			if err != nil {
				t.Errorf("[%s] error creating the request: %v", test.desc, err)
			}

			req.Header.Set("User-Agent", userAgent)

			response, err := client.Do(req)
			if err != nil {
				t.Errorf("[%s] error: %v", test.desc, err)
			}

			if response.StatusCode != test.code {
				t.Errorf("[%s] expected http code %d, got %#v", test.desc, test.code, response)
			}

			// 关闭 body，因为处理器可能阻塞在 Flush 中，无法发送剩余事件。
			response.Body.Close()

			// 等待事件到达，至少达到测试给定的数量
			events := []*auditinternal.Event{}
			err = wait.Poll(50*time.Millisecond, testTimeout(t), wait.ConditionFunc(func() (done bool, err error) {
				events = sink.Events()
				return len(events) >= test.events, nil
			}))
			if err != nil {
				t.Errorf("[%s] timeout waiting for events", test.desc)
			}

			if got := len(events); got != test.events {
				t.Errorf("[%s] expected %d audit events, got %d", test.desc, test.events, got)
			} else {
				for i, check := range test.checks {
					err := check(events)
					if err != nil {
						t.Errorf("[%s,%d] %v", test.desc, i, err)
					}
				}

				if err := requestUserAgentMatches(userAgent)(events); err != nil {
					t.Errorf("[%s] %v", test.desc, err)
				}
			}

			if len(events) > 0 {
				status := events[len(events)-1].ResponseStatus
				if status == nil {
					t.Errorf("[%s] expected non-nil ResponseStatus in last event", test.desc)
				} else if int(status.Code) != test.code {
					t.Errorf("[%s] expected ResponseStatus.Code=%d, got %d", test.desc, test.code, status.Code)
				}
			}
		})
	}
}

// testTimeout 返回 "ForeverTestTimeout" 与 testing 截止时间（含清理时间）
// 中的较小值。
// 取二者较小值可确保在 `go test -timeout` 到期前主动失败并打印上下文，
// 同时为清理留出 5 秒。
func testTimeout(t *testing.T) time.Duration {
	defaultTimeout := wait.ForeverTestTimeout
	const cleanupTime = 5 * time.Second
	if deadline, ok := t.Deadline(); ok {
		maxTimeout := time.Until(deadline) - cleanupTime
		if maxTimeout < defaultTimeout {
			return maxTimeout
		}
	}
	return defaultTimeout
}
