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

package filters

import (
	"context"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/audit/policy"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
	"k8s.io/apiserver/plugin/pkg/audit/buffered"
	"k8s.io/apiserver/plugin/pkg/audit/log"
	"k8s.io/apiserver/plugin/pkg/audit/webhook"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/retry"
)

// fakeAuditSink 是并发安全的审计后端替身：记录收到的所有事件副本。
// 用锁而非 channel，是因为审计过滤器可能在请求 goroutine 与后台 goroutine 中写入。
type fakeAuditSink struct {
	lock   sync.Mutex
	events []*auditinternal.Event
}

// ProcessEvents 深拷贝后保存事件：审计后端可能复用事件对象，
// 不拷贝会导致测试断言看到被后续请求改写的字段。
func (s *fakeAuditSink) ProcessEvents(evs ...*auditinternal.Event) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	for _, e := range evs {
		event := e.DeepCopy()
		s.events = append(s.events, event)
	}
	return true
}

// Events 返回当前事件快照，避免调用方与写入方共享底层切片。
func (s *fakeAuditSink) Events() []*auditinternal.Event {
	s.lock.Lock()
	defer s.lock.Unlock()
	return append([]*auditinternal.Event{}, s.events...)
}

// Pop 等待并取出最早的一条事件，用于需要与请求 goroutine 同步的测试。
func (s *fakeAuditSink) Pop(timeout time.Duration) (*auditinternal.Event, error) {
	var result *auditinternal.Event
	err := wait.Poll(50*time.Millisecond, wait.ForeverTestTimeout, wait.ConditionFunc(func() (done bool, err error) {
		s.lock.Lock()
		defer s.lock.Unlock()
		if len(s.events) == 0 {
			return false, nil
		}
		result = s.events[0]
		s.events = s.events[1:]
		return true, nil
	}))
	return result, err
}

// TestConstructResponseWriter 验证审计响应包装器保留底层 writer 的可选接口：
// Flusher/CloseNotifier/Hijacker 必须按内层实现情况透出，
// 否则流式响应、连接升级等依赖类型断言的功能会静默失效。
func TestConstructResponseWriter(t *testing.T) {
	inner := &responsewriter.FakeResponseWriter{}
	actual := decorateResponseWriter(context.Background(), inner, false)
	switch v := actual.(type) {
	case *auditResponseWriter:
	default:
		t.Errorf("Expected auditResponseWriter, got %v", reflect.TypeOf(v))
	}
	if innerGot := actual.(responsewriter.UserProvidedDecorator).Unwrap(); inner != innerGot {
		t.Errorf("Expected the decorator to return the inner http.ResponseWriter object")
	}

	actual = decorateResponseWriter(context.Background(), &responsewriter.FakeResponseWriterFlusherCloseNotifier{}, false)
	//lint:file-ignore SA1019 Keep supporting deprecated http.CloseNotifier
	if _, ok := actual.(http.CloseNotifier); !ok {
		t.Errorf("Expected http.ResponseWriter to implement http.CloseNotifier")
	}
	if _, ok := actual.(http.Flusher); !ok {
		t.Errorf("Expected the wrapper to implement http.Flusher")
	}
	if _, ok := actual.(http.Hijacker); ok {
		t.Errorf("Expected http.ResponseWriter not to implement http.Hijacker")
	}

	actual = decorateResponseWriter(context.Background(), &responsewriter.FakeResponseWriterFlusherCloseNotifierHijacker{}, false)
	//lint:file-ignore SA1019 Keep supporting deprecated http.CloseNotifier
	if _, ok := actual.(http.CloseNotifier); !ok {
		t.Errorf("Expected http.ResponseWriter to implement http.CloseNotifier")
	}
	if _, ok := actual.(http.Flusher); !ok {
		t.Errorf("Expected the wrapper to implement http.Flusher")
	}
	if _, ok := actual.(http.Hijacker); !ok {
		t.Errorf("Expected http.ResponseWriter to implement http.Hijacker")
	}
}

// TestDecorateResponseWriterWithoutChannel 验证未启用 channel 时，
// WriteHeader 直接记录状态码，不阻塞。
func TestDecorateResponseWriterWithoutChannel(t *testing.T) {
	ctx := audit.WithAuditContext(context.Background())
	ac := audit.AuditContextFrom(ctx)
	actual := decorateResponseWriter(ctx, &responsewriter.FakeResponseWriter{}, false)

	// 写入状态。由于 firstEventSentCh 为 nil，这里不会阻塞。
	actual.WriteHeader(42)
	if ac.GetEventResponseStatus() == nil {
		t.Fatalf("Expected ResponseStatus to be non-nil")
	}
	if ac.GetEventResponseStatus().Code != 42 {
		t.Errorf("expected status code 42, got %d", ac.GetEventResponseStatus().Code)
	}
}

// TestDecorateResponseWriterWithImplicitWrite 验证未显式调用 WriteHeader 时，
// 首次 Write 应被视为隐式 200，审计事件中的状态码也必须是 200。
func TestDecorateResponseWriterWithImplicitWrite(t *testing.T) {
	ctx := audit.WithAuditContext(context.Background())
	ac := audit.AuditContextFrom(ctx)
	actual := decorateResponseWriter(ctx, &responsewriter.FakeResponseWriter{}, false)

	// 写入状态。由于 firstEventSentCh 为 nil，这里不会阻塞。
	actual.Write([]byte("foo"))
	if ac.GetEventResponseStatus() == nil {
		t.Fatalf("Expected ResponseStatus to be non-nil")
	}
	if ac.GetEventResponseStatus().Code != 200 {
		t.Errorf("expected status code 200, got %d", ac.GetEventResponseStatus().Code)
	}
}

// TestDecorateResponseWriterChannel 验证启用 channel 时，WriteHeader 会阻塞
// 直到首个事件（RequestReceived）被发出，从而保证审计事件顺序；
// 事件发出后再次 Write 不得阻塞。
func TestDecorateResponseWriterChannel(t *testing.T) {
	ctx := audit.WithAuditContext(context.Background())
	sink := &fakeAuditSink{}
	auditContext := audit.AuditContextFrom(ctx)
	if err := auditContext.Init(audit.RequestAuditConfig{}, sink); err != nil {
		t.Fatal(err)
	}
	actual := decorateResponseWriter(ctx, &responsewriter.FakeResponseWriter{}, true)

	done := make(chan struct{})
	go func() {
		t.Log("Writing status code 42")
		actual.WriteHeader(42)
		t.Log("Finished writing status code 42")
		close(done)

		actual.Write([]byte("foo"))
	}()

	// 休眠一会儿，给 write 犯错的机会。
	time.Sleep(100 * time.Millisecond)

	t.Log("Waiting for event in the channel")
	ev1, err := sink.Pop(time.Second)
	if err != nil {
		t.Fatal("Timeout waiting for events")
	}
	t.Logf("Seen event with status %v", ev1.ResponseStatus)

	ev := getAuditContextEvent(auditContext)
	if diff := cmp.Diff(ev, ev1, cmp.FilterPath(func(p cmp.Path) bool {
		return p.String() == "StageTimestamp"
	}, cmp.Ignore())); diff != "" {
		t.Fatalf("ev1 and ev must be equal, diff: %s", diff)
	}

	<-done
	t.Log("Seen the go routine finished")

	// 再次写入。
	_, err = actual.Write([]byte("foo"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// getAuditContextEvent 通过 unsafe 读取 AuditContext 的私有 event 字段，
// 以便测试直接检查“当前正在构建”的事件（尚未发送到后端）。
// 仅测试代码可使用；依赖内部字段布局，改动 AuditContext 结构时需同步更新。
func getAuditContextEvent(ac *audit.AuditContext) *auditinternal.Event {
	// 取出 AuditContext 的 reflect.Value。
	val := reflect.ValueOf(ac).Elem()

	// 访问未导出的 `event` 字段。
	eventField := val.FieldByName("event")

	// 用 unsafe 取得该字段的指针。
	eventPtr := unsafe.Pointer(eventField.UnsafeAddr())

	// 把指针转换为正确的类型。
	return (*auditinternal.Event)(eventPtr)
}

// fakeHTTPHandler 是始终返回 200 的最小 handler。
type fakeHTTPHandler struct{}

// ServeHTTP 返回 200。
func (*fakeHTTPHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
}

// TestAudit 是审计过滤器的核心行为矩阵，覆盖：
//   - 短请求与长请求（watch）不同的阶段序列；
//   - handler 空返回、写响应体、返回 403、panic 等路径；
//   - 省略特定阶段（omitStages）时的事件过滤；
//   - Audit-ID 头透传、同一请求所有事件共享同一 AuditID；
//   - 延迟注解格式（仅校验正则，避免依赖具体耗时）。
func TestAudit(t *testing.T) {
	shortRunningPath := "/api/v1/namespaces/default/pods/foo"
	longRunningPath := "/api/v1/namespaces/default/pods?watch=true"

	delay := 501 * time.Millisecond

	for _, test := range []struct {
		desc       string
		path       string
		verb       string
		auditID    string
		omitStages []auditinternal.Stage
		handler    func(http.ResponseWriter, *http.Request)
		expected   []auditinternal.Event
		respHeader bool
	}{
		// 使用只读动词的短耗时请求
		{
			"read-only empty",
			shortRunningPath,
			request.MethodGet,
			"",
			nil,
			func(http.ResponseWriter, *http.Request) {},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "get",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "get",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			false,
		},
		{
			"short running with auditID",
			shortRunningPath,
			request.MethodGet,
			uuid.New().String(),
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "get",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "get",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			true,
		},
		{
			"read-only panic",
			shortRunningPath,
			request.MethodGet,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				panic("kaboom")
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "get",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StagePanic,
					Verb:           "get",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 500},
				},
			},
			false,
		},
		// 使用非只读动词的短耗时请求
		{
			"writing empty",
			shortRunningPath,
			request.MethodPut,
			"",
			nil,
			func(http.ResponseWriter, *http.Request) {},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "update",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "update",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			false,
		},
		{
			"writing sleep",
			shortRunningPath,
			request.MethodPut,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
				time.Sleep(delay)
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "update",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "update",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
					Annotations: map[string]string{
						"apiserver.latency.k8s.io/response-write": "^[0-9.]+[µnm]s$",
						"apiserver.latency.k8s.io/total":          "^[0-9.]+[µnm]s$",
					},
				},
			},
			true,
		},
		{
			"writing 403+write",
			shortRunningPath,
			request.MethodPut,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(403)
				w.Write([]byte("foo"))
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "update",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "update",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 403},
				},
			},
			true,
		},
		{
			"writing panic",
			shortRunningPath,
			request.MethodPut,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				panic("kaboom")
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "update",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StagePanic,
					Verb:           "update",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 500},
				},
			},
			false,
		},
		{
			"writing write+panic",
			shortRunningPath,
			request.MethodPut,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
				panic("kaboom")
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "update",
					RequestURI: shortRunningPath,
				},
				{
					Stage:          auditinternal.StagePanic,
					Verb:           "update",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 500},
				},
			},
			true,
		},
		// 长耗时请求
		{
			"empty longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(http.ResponseWriter, *http.Request) {},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			false,
		},
		{
			"empty longrunning with audit id",
			longRunningPath,
			request.MethodGet,
			uuid.New().String(),
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			true,
		},
		{
			"sleep longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(http.ResponseWriter, *http.Request) {
				time.Sleep(delay)
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			false,
		},
		{
			"sleep+403 longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				time.Sleep(delay)
				w.WriteHeader(403)
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 403},
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 403},
				},
			},
			true,
		},
		{
			"write longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			true,
		},
		{
			"403+write longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(403)
				w.Write([]byte("foo"))
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 403},
				},
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 403},
				},
			},
			true,
		},
		{
			"panic longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				panic("kaboom")
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StagePanic,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 500},
				},
			},
			false,
		},
		{
			"write+panic longrunning",
			longRunningPath,
			request.MethodGet,
			"",
			nil,
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
				panic("kaboom")
			},
			[]auditinternal.Event{
				{
					Stage:      auditinternal.StageRequestReceived,
					Verb:       "watch",
					RequestURI: longRunningPath,
				},
				{
					Stage:          auditinternal.StageResponseStarted,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
				{
					Stage:          auditinternal.StagePanic,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 500},
				},
			},
			true,
		},
		{
			"omit RequestReceived",
			shortRunningPath,
			request.MethodGet,
			"",
			[]auditinternal.Stage{auditinternal.StageRequestReceived},
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
			},
			[]auditinternal.Event{
				{
					Stage:          auditinternal.StageResponseComplete,
					Verb:           "get",
					RequestURI:     shortRunningPath,
					ResponseStatus: &metav1.Status{Code: 200},
				},
			},
			true,
		},
		{
			"emit Panic only",
			longRunningPath,
			request.MethodGet,
			"",
			[]auditinternal.Stage{auditinternal.StageRequestReceived, auditinternal.StageResponseStarted, auditinternal.StageResponseComplete},
			func(w http.ResponseWriter, req *http.Request) {
				w.Write([]byte("foo"))
				panic("kaboom")
			},
			[]auditinternal.Event{
				{
					Stage:          auditinternal.StagePanic,
					Verb:           "watch",
					RequestURI:     longRunningPath,
					ResponseStatus: &metav1.Status{Code: 500},
				},
			},
			true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := t.Context()
			sink := &fakeAuditSink{}
			fakeRuleEvaluator := policy.NewFakePolicyRuleEvaluator(auditinternal.LevelRequestResponse, test.omitStages)
			handler := WithAudit(http.HandlerFunc(test.handler), sink, fakeRuleEvaluator, func(r *http.Request, ri *request.RequestInfo) bool {
				// 简化的长耗时检查
				return ri.Verb == "watch"
			})
			handler = WithLatencyTrackers(handler)
			handler = WithAuditInit(handler)

			req, _ := http.NewRequestWithContext(ctx, test.verb, test.path, nil)
			req = withTestContext(req, &user.DefaultInfo{Name: "admin"}, nil)
			if test.auditID != "" {
				req.Header.Add("Audit-ID", test.auditID)
			}
			req.RemoteAddr = "127.0.0.1"

			// panic 用例依赖外层 recover 让测试继续；handler 内部的 recover 由审计过滤器负责。
			func() {
				defer func() {
					recover()
				}()
				handler.ServeHTTP(httptest.NewRecorder(), req)
			}()

			events := sink.Events()
			t.Logf("audit log: %v", events)

			if len(events) != len(test.expected) {
				t.Fatalf("Unexpected amount of lines in audit log: %d", len(events))
			}
			expectedID := types.UID("")
			for i, expect := range test.expected {
				event := events[i]
				if event.User.Username != "admin" {
					t.Errorf("Unexpected username: %s", event.User.Username)
				}
				if event.Stage != expect.Stage {
					t.Errorf("Unexpected Stage: %s", event.Stage)
				}
				if event.Verb != expect.Verb {
					t.Errorf("Unexpected Verb: %s", event.Verb)
				}
				if event.RequestURI != expect.RequestURI {
					t.Errorf("Unexpected RequestURI: %s", event.RequestURI)
				}

				// 客户端提供 Audit-ID 时必须原样回显，便于跨组件关联。
				if test.auditID != "" && event.AuditID != types.UID(test.auditID) {
					t.Errorf("Unexpected AuditID in audit event, AuditID should be the same with Audit-ID http header")
				}
				// 同一请求的所有阶段事件必须共享 AuditID。
				if expectedID == types.UID("") {
					expectedID = event.AuditID
				} else if expectedID != event.AuditID {
					t.Errorf("Audits for one request should share the same AuditID, %s differs from %s", expectedID, event.AuditID)
				}
				if event.ObjectRef.APIVersion != "v1" {
					t.Errorf("Unexpected apiVersion: %s", event.ObjectRef.APIVersion)
				}
				if event.ObjectRef.APIGroup != "" {
					t.Errorf("Unexpected apiGroup: %s", event.ObjectRef.APIGroup)
				}
				if (event.ResponseStatus == nil) != (expect.ResponseStatus == nil) {
					t.Errorf("Unexpected ResponseStatus: %v", event.ResponseStatus)
					continue
				}
				if (event.ResponseStatus != nil) && (event.ResponseStatus.Code != expect.ResponseStatus.Code) {
					t.Errorf("Unexpected status code : %d", event.ResponseStatus.Code)
				}

				// 注解用正则匹配，避免断言具体耗时时长导致测试不稳定。
				for k, v := range expect.Annotations {
					if actual, exists := event.Annotations[k]; !exists {
						t.Errorf("Expect key %s in the annotations but it does not exist", k)
					} else if matched, _ := regexp.MatchString(v, actual); !matched {
						t.Errorf("Annotation %s value %q does not match regex %q", k, actual, v)
					}
				}
				// 反向检查：不应出现预期之外的注解，防止误加调试信息。
				for k := range event.Annotations {
					if _, exists := expect.Annotations[k]; !exists {
						t.Errorf("Unexpected key %s in the annotations", k)
					}
				}
			}
		})
	}
}

// TestAuditNoPanicOnNilUser 验证上下文中没有用户时审计过滤器不会 panic：
// 认证失败的请求同样会经过审计路径。
func TestAuditNoPanicOnNilUser(t *testing.T) {
	ctx := t.Context()
	fakeRuleEvaluator := policy.NewFakePolicyRuleEvaluator(auditinternal.LevelRequestResponse, nil)
	handler := WithAudit(&fakeHTTPHandler{}, &fakeAuditSink{}, fakeRuleEvaluator, nil)
	req, _ := http.NewRequestWithContext(ctx, request.MethodGet, "/api/v1/namespaces/default/pods", nil)
	req = withTestContext(req, nil, nil)
	req.RemoteAddr = "127.0.0.1"
	handler.ServeHTTP(httptest.NewRecorder(), req)
}

// TestAuditLevelNone 验证审计级别为 None 时不产生任何事件，
// 即使用户与请求信息齐备。
func TestAuditLevelNone(t *testing.T) {
	ctx := t.Context()
	sink := &fakeAuditSink{}
	var handler http.Handler
	handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})
	fakeRuleEvaluator := policy.NewFakePolicyRuleEvaluator(auditinternal.LevelNone, nil)
	handler = WithAudit(handler, sink, fakeRuleEvaluator, nil)

	req, _ := http.NewRequestWithContext(ctx, request.MethodGet, "/api/v1/namespaces/default/pods", nil)
	req.RemoteAddr = "127.0.0.1"
	req = withTestContext(req, &user.DefaultInfo{Name: "admin"}, nil)

	handler.ServeHTTP(httptest.NewRecorder(), req)
	if len(sink.events) > 0 {
		t.Errorf("Generated events, but should not have: %#v", sink.events)
	}
}

// TestAuditIDHttpHeader 验证响应中始终返回 Audit-ID：即使审计级别为 None，
// 该 ID 仍用于日志/追踪以及向聚合 apiserver 传播以改善关联性；
// 客户端已提供时则原样回显。
func TestAuditIDHttpHeader(t *testing.T) {
	for _, test := range []struct {
		desc           string
		requestHeader  string
		level          auditinternal.Level
		expectedHeader bool
	}{
		{
			// 我们总是希望有审计 ID，因为它会出现在日志/追踪中，并会传播给
			// 聚合 apiserver 以改善关联能力。
			"http header when there is no audit",
			"",
			auditinternal.LevelNone,
			true,
		},
		{
			// 我们总是希望有审计 ID，因为它会出现在日志/追踪中，并会传播给
			// 聚合 apiserver 以改善关联能力。
			"http header when there is no audit even the request header specified",
			uuid.New().String(),
			auditinternal.LevelNone,
			true,
		},
		{
			"server generated header",
			"",
			auditinternal.LevelRequestResponse,
			true,
		},
		{
			"user provided header",
			uuid.New().String(),
			auditinternal.LevelRequestResponse,
			true,
		},
	} {
		t.Run(test.desc, func(t *testing.T) {
			ctx := t.Context()
			sink := &fakeAuditSink{}
			var handler http.Handler
			handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(200)
			})
			fakeRuleEvaluator := policy.NewFakePolicyRuleEvaluator(test.level, nil)
			handler = WithAudit(handler, sink, fakeRuleEvaluator, nil)
			handler = WithAuditInit(handler)

			req, _ := http.NewRequestWithContext(ctx, request.MethodGet, "/api/v1/namespaces/default/pods", nil)
			req.RemoteAddr = "127.0.0.1"
			req = withTestContext(req, &user.DefaultInfo{Name: "admin"}, nil)
			if test.requestHeader != "" {
				req.Header.Add("Audit-ID", test.requestHeader)
			}

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			resp := w.Result()
			if test.expectedHeader {
				if resp.Header.Get("Audit-ID") == "" {
					t.Errorf("[%s] expected Audit-ID http header returned, but not returned", test.desc)
					return
				}
				// 如果返回了 Audit-ID，应当与请求中携带的那个一致
				if test.requestHeader != "" && resp.Header.Get("Audit-ID") != test.requestHeader {
					t.Errorf("[%s] returned audit http header is not the same with the requested http header, expected: %s, get %s", test.desc, test.requestHeader, resp.Header.Get("Audit-ID"))
				}
			} else {
				if resp.Header.Get("Audit-ID") != "" {
					t.Errorf("[%s] expected no Audit-ID http header returned, but got %s", test.desc, resp.Header.Get("Audit-ID"))
				}
			}
		})
	}
}

// withTestContext 为测试请求补上审计上下文、用户与 RequestInfo，
// 模拟审计过滤器链在真实请求中依赖的前置条件。
func withTestContext(req *http.Request, user user.Info, ae *auditinternal.Event) *http.Request {
	ctx := audit.WithAuditContext(req.Context())
	if user != nil {
		ctx = request.WithUser(ctx, user)
	}
	if ae != nil {
		ev := getAuditContextEvent(audit.AuditContextFrom(ctx))
		*ev = *ae
	}
	if info, err := newTestRequestInfoResolver().NewRequestInfo(req); err == nil {
		ctx = request.WithRequestInfo(ctx, info)
	}
	return req.WithContext(ctx)
}

// fakeAuditFile 模拟慢速磁盘写入，用于放大审计后端之间的时序差异。
type fakeAuditFile struct{}

// Write 随机延迟后丢弃数据，仅用于制造并发压力。
func (s fakeAuditFile) Write(p []byte) (n int, err error) {
	time.Sleep(time.Duration(rand.Int63n(10000)))
	return len(p), nil
}

// fakeAuditWebhookAuditBackend 模拟慢速 webhook 传输。
type fakeAuditWebhookAuditBackend struct {
}

// RoundTrip 随机延迟后返回 200，用于制造并发压力。
func (f fakeAuditWebhookAuditBackend) RoundTrip(r *http.Request) (*http.Response, error) {
	time.Sleep(time.Duration(rand.Int63n(10000)))
	return &http.Response{
		StatusCode: http.StatusOK,
	}, nil
}

// 针对 https://github.com/kubernetes/kubernetes/issues/120507 的测试用例
// 要测试审计后端的竞态条件，使用以下命令：
// `go test ./ -race --run=TestAuditBackendRaceCondition -v`
//
// 该测试对每种审计后端（log/buffered/webhook/union）反复处理请求，
// 同时由另一 goroutine 并发向同一请求上下文添加注解，用于在 -race 下
// 暴露审计后端与注解写入之间的数据竞争（回归 #120507）。
func TestAuditBackendRaceCondition(t *testing.T) {
	defaultFakeLogBackend := log.NewBackend(fakeAuditFile{}, log.FormatJson, auditv1.SchemeGroupVersion)
	testCases := []struct {
		name           string
		backendBuilder func() audit.Backend
	}{
		{
			"log audit backend",
			func() audit.Backend {
				return defaultFakeLogBackend
			},
		},
		{
			"buffered audit backend",
			func() audit.Backend {
				backend := buffered.NewBackend(defaultFakeLogBackend, buffered.BatchConfig{
					BufferSize:     10000,
					MaxBatchSize:   1,
					ThrottleEnable: false,
					AsyncDelegate:  false,
				})
				err := backend.Run(wait.NeverStop)
				if err != nil {
					t.Fatal(err)
				}
				return backend
			},
		},
		{
			name: "webhook audit backend",
			backendBuilder: func() audit.Backend {
				codecFactory := audit.Codecs
				codec := codecFactory.LegacyCodec(auditv1.SchemeGroupVersion)
				negotiatedSerializer := serializer.NegotiatedSerializerWrapper(runtime.SerializerInfo{Serializer: codec})
				client, err := rest.NewRESTClient(&url.URL{}, "/hello", rest.ClientContentConfig{
					ContentType: "application/json",
					Negotiator:  runtime.NewClientNegotiator(negotiatedSerializer, auditv1.SchemeGroupVersion),
				}, flowcontrol.NewTokenBucketRateLimiter(100, 200), &http.Client{Transport: fakeAuditWebhookAuditBackend{}})
				if err != nil {
					t.Fatal(err)
				}
				return webhook.NewDynamicBackend(client, retry.DefaultBackoff)
			},
		},
		{
			"union audit backend",
			func() audit.Backend {
				return audit.Union(defaultFakeLogBackend, defaultFakeLogBackend)
			},
		},
	}
	fakeRuleEvaluator := policy.NewFakePolicyRuleEvaluator(auditinternal.LevelRequestResponse, nil)
	longRunningCheck := func(r *http.Request, ri *request.RequestInfo) bool { return false }
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
			defer cancel()
			// 循环直到超时，持续制造并发窗口。
			for {
				select {
				case <-ctx.Done():
					// 测试结束
					return
				default:
				}
				serveStarted := make(chan struct{})
				req, _ := http.NewRequest(http.MethodGet, "/api/v1/namespaces/default/pods/foo", nil)
				req = withTestContext(req, &user.DefaultInfo{Name: "admin"}, nil)
				backend := tc.backendBuilder()
				// 注解写入与 handler 执行并行，是竞争的关键来源。
				go func() {
					<-serveStarted
					for {
						select {
						case <-ctx.Done():
							// 测试结束
							backend.Shutdown()
							return
						default:
						}
						audit.AddAuditAnnotations(req.Context(), "a", "b")
					}
				}()
				realHandler := http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
					close(serveStarted)
					// 模拟一段业务逻辑
					time.Sleep(time.Millisecond)
				})
				handler := WithAudit(realHandler, backend, fakeRuleEvaluator, longRunningCheck)
				handler = WithAuditInit(handler)
				serveFinished := make(chan struct{})
				go func() {
					defer close(serveFinished)
					handler.ServeHTTP(httptest.NewRecorder(), req)
				}()
				<-serveFinished
			}
		})
	}
}
