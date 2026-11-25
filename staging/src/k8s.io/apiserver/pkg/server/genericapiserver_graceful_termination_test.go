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

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	kubeopenapi "k8s.io/kube-openapi/pkg/common"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/net/http2"
)

// TestMain 初始化 klog 标志，使测试中的 klog 输出遵循统一配置。
func TestMain(m *testing.M) {
	klog.InitFlags(nil)
	os.Exit(m.Run())
}

// doer 向服务端发送一个请求。
// 把“发请求并等待结果”封装成可调用对象，便于在不同连接策略下复用。
type doer func(client *http.Client, gci func(httptrace.GotConnInfo), path string, timeout time.Duration) result

// Do 调用底层函数。
func (d doer) Do(client *http.Client, gci func(httptrace.GotConnInfo), path string, timeout time.Duration) result {
	return d(client, gci, path, timeout)
}

// result 保存请求结果；response 可能为 nil（连接失败时），断言时需先检查。
type result struct {
	err      error
	response *http.Response
}

// wrappedLifecycleSignal 对 lifecycleSignal 做一层包装，使测试可以注入自己的回调。
type wrappedLifecycleSignal struct {
	lifecycleSignal
	before func(lifecycleSignal)
	after  func(lifecycleSignal)
}

// Signal 在真实信号前后执行注入的回调，用于在正确的生命周期点做断言。
func (w *wrappedLifecycleSignal) Signal() {
	if w.before != nil {
		w.before(w.lifecycleSignal)
	}
	w.lifecycleSignal.Signal()
	if w.after != nil {
		w.after(w.lifecycleSignal)
	}
}

// wrapLifecycleSignalsWithRecorder 为所有生命周期信号安装记录回调，
// 以便最终校验信号顺序。
func wrapLifecycleSignalsWithRecorder(t *testing.T, signals *lifecycleSignals, before func(lifecycleSignal)) {
	// 必须在 'before' 回调中记录正在触发的信号，以避免用例抖动（flake），
	// 因为服务端触发事件是一个异步过程。
	signals.AfterShutdownDelayDuration = wrapLifecycleSignal(t, signals.AfterShutdownDelayDuration, before, nil)
	signals.PreShutdownHooksStopped = wrapLifecycleSignal(t, signals.PreShutdownHooksStopped, before, nil)
	signals.NotAcceptingNewRequest = wrapLifecycleSignal(t, signals.NotAcceptingNewRequest, before, nil)
	signals.HTTPServerStoppedListening = wrapLifecycleSignal(t, signals.HTTPServerStoppedListening, before, nil)
	signals.InFlightRequestsDrained = wrapLifecycleSignal(t, signals.InFlightRequestsDrained, before, nil)
	signals.ShutdownInitiated = wrapLifecycleSignal(t, signals.ShutdownInitiated, before, nil)
}

// wrapLifecycleSignal 用包装器替换原信号，保留原信号语义。
func wrapLifecycleSignal(t *testing.T, delegated lifecycleSignal, before, after func(_ lifecycleSignal)) lifecycleSignal {
	return &wrappedLifecycleSignal{
		lifecycleSignal: delegated,
		before:          before,
		after:           after,
	}
}

// 服务端在触发两个事件之间停留的时间，可能不足以让测试执行自己的步骤；
// 该结构允许我们拦截信号，并在执行测试的 goroutine 内运行验证步骤。
//
// 通过让信号发送方阻塞在 doneCh 上，测试可以在两个信号之间插入
// 任意验证逻辑，而无需依赖脆弱的 sleep。
type signalInterceptingTestStep struct {
	doneCh chan struct{}
}

// done 返回完成信号，信号发送方等待它关闭后才继续。
func (ts signalInterceptingTestStep) done() <-chan struct{} {
	return ts.doneCh
}

// execute 执行测试步骤，完成后关闭 doneCh 放行服务端。
func (ts signalInterceptingTestStep) execute(fn func()) {
	defer close(ts.doneCh)
	fn()
}

// newSignalInterceptingTestStep 创建拦截步骤。
func newSignalInterceptingTestStep() *signalInterceptingTestStep {
	return &signalInterceptingTestStep{
		doneCh: make(chan struct{}),
	}
}

//	 本用例验证下图所描述的优雅停机（graceful termination）场景：
//	   - 每条竖线都是一条独立的时间线
//	   - 最左侧的竖线代表执行 GenericAPIServer.Run 方法的 goroutine
//	   - （signal name）表示对应的生命周期信号已被触发
//
//	                                 stopCh
//	                                   |
//	             |--------------------------------------------|
//	             |                                            |
//		    call PreShutdownHooks                        (ShutdownInitiated)
//	             |                                            |
//	  (PreShutdownHooksStopped)                   Sleep(ShutdownDelayDuration)
//	             |                                            |
//	             |                                 (AfterShutdownDelayDuration)
//	             |                                            |
//	             |                                            |
//	             |--------------------------------------------|
//	             |                                            |
//	             |                                 (NotAcceptingNewRequest)
//	             |                                            |
//	             |                       |-------------------------------------------------|
//	             |                       |                                                 |
//	             |             close(stopHttpServerCh)                         NonLongRunningRequestWaitGroup.Wait()
//	             |                       |                                                 |
//	             |            server.Shutdown(timeout=60s)                                 |
//	             |                       |                                         WatchRequestWaitGroup.Wait()
//	             |              stop listener (net/http)                                   |
//	             |                       |                                                 |
//	             |          |-------------------------------------|                        |
//	             |          |                                     |                        |
//	             |          |                      (HTTPServerStoppedListening)            |
//	             |          |                                                              |
//	             |    wait up to 60s                                                       |
//	             |          |                                                  (InFlightRequestsDrained)
//	             |          |
//	             |          |
//	             |	stoppedCh is closed
//	             |
//	             |
//	   <-drainedCh.Signaled()
//	             |
//	  s.AuditBackend.Shutdown()
//	             |
//	     <-listenerStoppedCh
//	             |
//	        <-stoppedCh
//	             |
//	         return nil
//
// 本用例关闭 ShutdownSendRetryAfter：HTTP server 停止监听后，新连接直接
// 被拒绝（connection refused），而不是收到 429；已有的 in-flight 请求
// 仍需被处理完成并写入审计。
func TestGracefulTerminationWithKeepListeningDuringGracefulTerminationDisabled(t *testing.T) {
	fakeAudit := &fakeAudit{}
	s := newGenericAPIServer(t, fakeAudit, false)
	connReusingClient := newClient(false)
	doer := setupDoer(t, s.SecureServingInfo)

	// 处理非长运行请求与 watch 请求的 handler：我们想让这两类请求
	// 一直保持 in-flight 直到最后。
	inflightNonLongRunning := setupInFlightNonLongRunningRequestHandler(s)
	inflightWatch := setupInFlightWatchRequestHandler(s)

	// 来自 pre-shutdown hook 的 API 调用必须成功，直到 HTTP server
	// 关闭为止。
	preShutdownHook := setupPreShutdownHookHandler(t, s, doer, newClient(true))

	signals := &s.lifecycleSignals
	recorder := &signalRecorder{}
	wrapLifecycleSignalsWithRecorder(t, signals, recorder.before)

	// 在 AfterShutdownDelayDuration 信号触发之前，我们希望测试先执行
	// 一个验证步骤。
	beforeShutdownDelayDurationStep := newSignalInterceptingTestStep()
	signals.AfterShutdownDelayDuration = wrapLifecycleSignal(t, signals.AfterShutdownDelayDuration, func(_ lifecycleSignal) {
		// 等待测试执行完验证步骤后，服务端再继续触发后续信号
		<-beforeShutdownDelayDurationStep.done()
	}, nil)

	// 启动 API server
	_, ctx := ktesting.NewTestContext(t)
	stopCtx, stop := context.WithCancelCause(ctx)
	defer stop(errors.New("test has completed"))
	runCompletedCh := make(chan struct{})
	go func() {
		defer close(runCompletedCh)
		if err := s.PrepareRun().RunWithContext(stopCtx); err != nil {
			t.Errorf("unexpected error from RunWithContext: %v", err)
		}
	}()
	waitForAPIServerStarted(t, doer)

	// 发出非长运行请求和 watch 请求，让它们此刻在服务端处于 in-flight
	// 状态；我们会在 ShutdownDelayDuration 到期后再放行它们。
	inflightNonLongRunning.launch(doer, connReusingClient)
	waitForeverUntil(t, inflightNonLongRunning.startedCh, "in-flight non long-running request did not reach the server")
	inflightWatch.launch(doer, connReusingClient)
	waitForeverUntil(t, inflightWatch.startedCh, "in-flight watch request did not reach the server")

	// /readyz 应当返回 OK
	resultGot := doer.Do(newClient(true), func(httptrace.GotConnInfo) {}, "/readyz", time.Second)
	if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 触发停机事件：发起 shutdown
	stop(errors.New("shutting down"))
	waitForeverUntilSignaled(t, signals.ShutdownInitiated)

	// /readyz 必须返回错误，但需要给它一点时间
	err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (done bool, err error) {
		resultGot := doer.Do(newClient(true), func(httptrace.GotConnInfo) {}, "/readyz", time.Second)
		// 轮询直到出现非 200 响应
		if resultGot.response != nil && resultGot.response.StatusCode == http.StatusOK {
			return false, nil
		}

		if err := assertResponseStatusCode(resultGot, http.StatusInternalServerError); err != nil {
			return true, err
		}
		return true, nil
	})
	if err != nil {
		t.Errorf("Expected /readyz to return 500 status code, but got: %v", err)
	}

	// 在 ShutdownDelayDuration 到期之前，新请求应当被成功处理。
	beforeShutdownDelayDurationStep.execute(func() {
		t.Log("Before ShutdownDelayDuration elapses new request(s) should be served")
		resultGot := doer.Do(connReusingClient, shouldReuseConnection(t), "/echo?message=request-on-an-existing-connection-should-succeed", time.Second)
		if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
			t.Errorf("%s", err.Error())
		}
		resultGot = doer.Do(newClient(true), shouldUseNewConnection(t), "/echo?message=request-on-a-new-tcp-connection-should-succeed", time.Second)
		if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
			t.Errorf("%s", err.Error())
		}
	})

	waitForeverUntilSignaled(t, signals.AfterShutdownDelayDuration)

	// preshutdown hook 尚未执行完毕，新进入的请求应当成功
	resultGot = doer.Do(newClient(true), shouldUseNewConnection(t), "/echo?message=request-on-a-new-tcp-connection-should-succeed", time.Second)
	if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 让 preshutdown hook 现在发起一次 API 调用，然后等待它返回结果。
	close(preShutdownHook.blockedCh)
	preShutdownHookResult := <-preShutdownHook.resultCh
	waitForeverUntilSignaled(t, signals.PreShutdownHooksStopped)
	if err := assertResponseStatusCode(preShutdownHookResult, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}

	waitForeverUntilSignaled(t, signals.PreShutdownHooksStopped)
	// AfterShutdownDelayDuration 与 PreShutdownHooksCompleted 都已触发，
	// 此时我们不应再接纳新请求
	waitForeverUntilSignaled(t, signals.NotAcceptingNewRequest)
	waitForeverUntilSignaled(t, signals.HTTPServerStoppedListening)

	resultGot = doer.Do(newClient(true), shouldUseNewConnection(t), "/echo?message=request-on-a-new-tcp-connection-should-fail-with-503", time.Second)
	if !utilnet.IsConnectionRefused(resultGot.err) {
		t.Errorf("Expected error %v, but got: %v %v", syscall.ECONNREFUSED, resultGot.err, resultGot.response)
	}

	// 尽管 Server.Serve() 已经返回，服务端上的既有连接仍可能需要一些时间
	// 才进入 "closing" 状态；下面的轮询用于消除该延迟带来的用例抖动。
	if err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (done bool, err error) {
		result := doer.Do(connReusingClient, shouldReuseConnection(t), "/echo?message=waiting-for-the-existing-connection-to-reject-incoming-request", time.Second)
		if result.response != nil {
			t.Logf("Still waiting for the server to return error - response: %v", result.response)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Errorf("Expected no error, but got: %v", err)
	}

	// TODO: 我们原本的意图是让任何进入的请求经由 WithWaitGroup 过滤器收到 503，
	// 但走到这一步时，由于 net/http server 已停止监听，任何进入的请求都会得到
	// 'connection refused' 错误。
	resultGot = doer.Do(connReusingClient, shouldReuseConnection(t), "/echo?message=request-on-an-existing-connection-should-fail-with-error", time.Second)
	if !utilnet.IsConnectionRefused(resultGot.err) {
		t.Errorf("Expected error %v, but got: %v %v", syscall.ECONNREFUSED, resultGot.err, resultGot.response)
	}

	// 服务端已停止监听，但我们仍有一个非长运行请求和一个 watch 请求处于
	// in-flight 状态；放行这两个请求，期望它们向调用方返回恰当的响应。
	inflightNonLongRunningResultGot := inflightNonLongRunning.unblockAndWaitForResult(t)
	if err := assertResponseStatusCode(inflightNonLongRunningResultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}
	if err := assertRequestAudited(inflightNonLongRunningResultGot, fakeAudit); err != nil {
		t.Errorf("%s", err.Error())
	}
	inflightWatchResultGot := inflightWatch.unblockAndWaitForResult(t)
	if err := assertResponseStatusCode(inflightWatchResultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}
	if err := assertRequestAudited(inflightWatchResultGot, fakeAudit); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 所有 in-flight 请求都已排空
	waitForeverUntilSignaled(t, signals.InFlightRequestsDrained)

	t.Log("Waiting for the apiserver Run method to return")
	waitForeverUntil(t, runCompletedCh, "the apiserver Run method did not return")

	if !fakeAudit.shutdownCompleted() {
		t.Errorf("Expected AuditBackend.Shutdown to be completed")
	}

	if err := recorder.verify([]string{
		"ShutdownInitiated",
		"AfterShutdownDelayDuration",
		"PreShutdownHooksStopped",
		"NotAcceptingNewRequest",
		"HTTPServerStoppedListening",
		"InFlightRequestsDrained",
	}); err != nil {
		t.Errorf("%s", err.Error())
	}
}

// 本用例验证下图所描述的优雅停机（graceful termination）场景：
//
//   - 每条竖线都是一条独立的时间线
//
//   - 最左侧的竖线代表执行 GenericAPIServer.Run 方法的 goroutine
//
//   - （signal）表示对应的生命周期信号已被触发
//
//     stopCh
//     |
//     |--------------------------------------------|
//     |                                            |
//     call PreShutdownHooks                       (ShutdownInitiated)
//     |                                            |
//     (PreShutdownHooksCompleted)                  Sleep(ShutdownDelayDuration)
//     |                                            |
//     |                                 (AfterShutdownDelayDuration)
//     |                                            |
//     |                                            |
//     |--------------------------------------------|
//     |                                            |
//     |                               (NotAcceptingNewRequest)
//     |                                            |
//     |                              NonLongRunningRequestWaitGroup.Wait()
//     |                                            |
//     |                                 WatchRequestWaitGroup.Wait()
//     |                                            |
//     |                                (InFlightRequestsDrained)
//     |                                            |
//     |                                            |
//     |------------------------------------------------------------|
//     |                                                            |
//     <-drainedCh.Signaled()                                     close(stopHttpServerCh)
//     |                                                            |
//     s.AuditBackend.Shutdown()                                 server.Shutdown(timeout=2s)
//     |                                                            |
//     |                                                   stop listener (net/http)
//     |                                                            |
//     |                                         |-------------------------------------|
//     |                                         |                                     |
//     |                                   wait up to 2s                 (HTTPServerStoppedListening)
//     <-listenerStoppedCh                                |
//     |                                stoppedCh is closed
//     <-stoppedCh
//     |
//     return nil
//
// 本用例开启 ShutdownSendRetryAfter：在 in-flight 请求排空期间继续监听，
// 新请求收到带 Retry-After 的 429，给客户端明确的“换实例重试”信号。
func TestGracefulTerminationWithKeepListeningDuringGracefulTerminationEnabled(t *testing.T) {
	fakeAudit := &fakeAudit{}
	s := newGenericAPIServer(t, fakeAudit, true)
	connReusingClient := newClient(false)
	doer := setupDoer(t, s.SecureServingInfo)

	// 处理非长运行请求与 watch 请求的 handler：我们想让这两类请求
	// 一直保持 in-flight 直到最后。
	inflightNonLongRunning := setupInFlightNonLongRunningRequestHandler(s)
	inflightWatch := setupInFlightWatchRequestHandler(s)

	// 来自 pre-shutdown hook 的 API 调用必须成功，直到 HTTP server
	// 关闭为止。
	preShutdownHook := setupPreShutdownHookHandler(t, s, doer, newClient(true))

	signals := &s.lifecycleSignals
	recorder := &signalRecorder{}
	wrapLifecycleSignalsWithRecorder(t, signals, recorder.before)

	// 在 AfterShutdownDelayDuration 信号触发之前，我们希望测试先执行
	// 一个验证步骤。
	beforeShutdownDelayDurationStep := newSignalInterceptingTestStep()
	signals.AfterShutdownDelayDuration = wrapLifecycleSignal(t, signals.AfterShutdownDelayDuration, func(_ lifecycleSignal) {
		// 在 AfterShutdownDelayDuration 事件触发之前，测试会发送请求，
		// 对预期行为进行断言。
		<-beforeShutdownDelayDurationStep.done()
	}, nil)

	// 启动 API server
	_, ctx := ktesting.NewTestContext(t)
	stopCtx, stop := context.WithCancelCause(ctx)
	defer stop(errors.New("test has completed"))
	runCompletedCh := make(chan struct{})
	go func() {
		defer close(runCompletedCh)
		if err := s.PrepareRun().RunWithContext(stopCtx); err != nil {
			t.Errorf("unexpected error from RunWithContext: %v", err)
		}
	}()
	waitForAPIServerStarted(t, doer)

	// 发出非长运行请求和 watch 请求，让它们此刻在服务端处于 in-flight
	// 状态；我们会在 ShutdownDelayDuration 到期后再放行它们。
	inflightNonLongRunning.launch(doer, connReusingClient)
	waitForeverUntil(t, inflightNonLongRunning.startedCh, "in-flight request did not reach the server")
	inflightWatch.launch(doer, connReusingClient)
	waitForeverUntil(t, inflightWatch.startedCh, "in-flight watch request did not reach the server")

	// /readyz 应当返回 OK
	resultGot := doer.Do(newClient(true), func(httptrace.GotConnInfo) {}, "/readyz", time.Second)
	if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 触发停机事件：发起 shutdown
	stop(errors.New("shutting down"))
	waitForeverUntilSignaled(t, signals.ShutdownInitiated)

	// /readyz 必须返回错误，但需要给它一点时间
	err := wait.PollImmediate(100*time.Millisecond, wait.ForeverTestTimeout, func() (done bool, err error) {
		resultGot := doer.Do(newClient(true), func(httptrace.GotConnInfo) {}, "/readyz", time.Second)
		// 轮询直到出现非 200 响应
		if resultGot.response != nil && resultGot.response.StatusCode == http.StatusOK {
			return false, nil
		}

		if err := assertResponseStatusCode(resultGot, http.StatusInternalServerError); err != nil {
			return true, err
		}
		return true, nil
	})
	if err != nil {
		t.Errorf("Expected /readyz to return 500 status code, but got: %v", err)
	}

	// 在 ShutdownDelayDuration 到期之前，新请求应当被成功处理。
	beforeShutdownDelayDurationStep.execute(func() {
		t.Log("Before ShutdownDelayDuration elapses new request(s) should be served")
		resultGot := doer.Do(connReusingClient, shouldReuseConnection(t), "/echo?message=request-on-an-existing-connection-should-succeed", time.Second)
		if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
			t.Errorf("%s", err.Error())
		}
		resultGot = doer.Do(newClient(true), shouldUseNewConnection(t), "/echo?message=request-on-a-new-tcp-connection-should-succeed", time.Second)
		if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
			t.Errorf("%s", err.Error())
		}
	})

	waitForeverUntilSignaled(t, signals.AfterShutdownDelayDuration)

	// preshutdown hook 尚未执行完毕，新进入的请求应当成功
	resultGot = doer.Do(newClient(true), shouldUseNewConnection(t), "/echo?message=request-on-a-new-tcp-connection-should-succeed", time.Second)
	if err := assertResponseStatusCode(resultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 让 preshutdown hook 现在发起一次 API 调用，然后等待它返回结果，
	// 该调用应当成功。
	close(preShutdownHook.blockedCh)
	preShutdownHookResult := <-preShutdownHook.resultCh
	waitForeverUntilSignaled(t, signals.PreShutdownHooksStopped)
	if err := assertResponseStatusCode(preShutdownHookResult, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}

	waitForeverUntilSignaled(t, signals.NotAcceptingNewRequest)

	// AfterShutdownDelayDuration 与 PreShutdownHooksCompleted 都已触发，
	// 任何进入的请求都应收到 429
	resultGot = doer.Do(newClient(true), shouldUseNewConnection(t), "/echo?message=request-on-a-new-tcp-connection-should-fail-with-429", time.Second)
	if err := requestMustFailWithRetryHeader(resultGot, http.StatusTooManyRequests); err != nil {
		t.Errorf("%s", err.Error())
	}
	resultGot = doer.Do(connReusingClient, shouldReuseConnection(t), "/echo?message=request-on-an-existing-connection-should-fail-with-429", time.Second)
	if err := requestMustFailWithRetryHeader(resultGot, http.StatusTooManyRequests); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 我们仍有一个非长运行请求和一个 watch 请求处于 in-flight 状态；
	// 放行这两个请求，期望它们向调用方返回恰当的响应。
	inflightNonLongRunningResultGot := inflightNonLongRunning.unblockAndWaitForResult(t)
	if err := assertResponseStatusCode(inflightNonLongRunningResultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}
	if err := assertRequestAudited(inflightNonLongRunningResultGot, fakeAudit); err != nil {
		t.Errorf("%s", err.Error())
	}
	inflightWatchResultGot := inflightWatch.unblockAndWaitForResult(t)
	if err := assertResponseStatusCode(inflightWatchResultGot, http.StatusOK); err != nil {
		t.Errorf("%s", err.Error())
	}
	if err := assertRequestAudited(inflightWatchResultGot, fakeAudit); err != nil {
		t.Errorf("%s", err.Error())
	}

	// 所有 in-flight 请求都已排空
	waitForeverUntilSignaled(t, signals.InFlightRequestsDrained)
	waitForeverUntilSignaled(t, signals.HTTPServerStoppedListening)

	t.Log("Waiting for the apiserver Run method to return")
	waitForeverUntil(t, runCompletedCh, "the apiserver Run method did not return")

	if !fakeAudit.shutdownCompleted() {
		t.Errorf("Expected AuditBackend.Shutdown to be completed")
	}

	if err := recorder.verify([]string{
		"ShutdownInitiated",
		"AfterShutdownDelayDuration",
		"PreShutdownHooksStopped",
		"NotAcceptingNewRequest",
		"InFlightRequestsDrained",
		"HTTPServerStoppedListening",
	}); err != nil {
		t.Errorf("%s", err.Error())
	}
}

// TestMuxAndDiscoveryComplete 验证 MuxAndDiscoveryComplete 信号只在所有
// 注册的 mux/discovery 子信号完成后才触发。
func TestMuxAndDiscoveryComplete(t *testing.T) {
	// 准备阶段
	testSignal1 := make(chan struct{})
	testSignal2 := make(chan struct{})
	s := newGenericAPIServer(t, &fakeAudit{}, true)
	s.muxAndDiscoveryCompleteSignals["TestSignal1"] = testSignal1
	s.muxAndDiscoveryCompleteSignals["TestSignal2"] = testSignal2
	doer := setupDoer(t, s.SecureServingInfo)
	isChanClosed := func(ch <-chan struct{}, delay time.Duration) bool {
		time.Sleep(delay)
		select {
		case <-ch:
			return true
		default:
			return false
		}
	}

	// 启动 API server
	_, ctx := ktesting.NewTestContext(t)
	stopCtx, stop := context.WithCancelCause(ctx)
	defer stop(errors.New("test has completed"))
	runCompletedCh := make(chan struct{})
	go func() {
		defer close(runCompletedCh)
		if err := s.PrepareRun().RunWithContext(stopCtx); err != nil {
			t.Errorf("unexpected error from RunWithContext: %v", err)
		}
	}()
	waitForAPIServerStarted(t, doer)

	// 执行阶段
	if isChanClosed(s.lifecycleSignals.MuxAndDiscoveryComplete.Signaled(), 1*time.Second) {
		t.Fatalf("%s is closed whereas the TestSignal is still open", s.lifecycleSignals.MuxAndDiscoveryComplete.Name())
	}

	close(testSignal1)
	if isChanClosed(s.lifecycleSignals.MuxAndDiscoveryComplete.Signaled(), 1*time.Second) {
		t.Fatalf("%s is closed whereas the TestSignal2 is still open", s.lifecycleSignals.MuxAndDiscoveryComplete.Name())
	}

	close(testSignal2)
	if !isChanClosed(s.lifecycleSignals.MuxAndDiscoveryComplete.Signaled(), 1*time.Second) {
		t.Fatalf("%s wasn't closed", s.lifecycleSignals.MuxAndDiscoveryComplete.Name())
	}
}

// TestPreShutdownHooks 验证 pre-shutdown hook 在 ShutdownDelayDuration 之后
// 仍能成功访问 API server（两种 ShutdownSendRetryAfter 配置下都成立），
// 这是 hook 用于优雅摘流的前提。
func TestPreShutdownHooks(t *testing.T) {
	tests := []struct {
		name   string
		server func() *GenericAPIServer
	}{
		{
			name: "ShutdownSendRetryAfter is disabled",
			server: func() *GenericAPIServer {
				return newGenericAPIServer(t, &fakeAudit{}, false)
			},
		},
		{
			name: "ShutdownSendRetryAfter is enabled",
			server: func() *GenericAPIServer {
				return newGenericAPIServer(t, &fakeAudit{}, true)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			stopCtx, stop := context.WithCancelCause(ctx)
			defer stop(errors.New("test has completed"))
			s := test.server()
			doer := setupDoer(t, s.SecureServingInfo)

			// preshutdown hook 向错误通道发送数据时不应发生阻塞
			preShutdownHookErrCh := make(chan error, 1)
			err := s.AddPreShutdownHook("test-backend", func() error {
				// 该 pre-shutdown hook 会等待 shutdown duration 到期，然后向
				// apiserver 发送一系列请求；我们期望这一系列请求全部成功完成。
				<-s.lifecycleSignals.AfterShutdownDelayDuration.Signaled()

				// 我们发送 5 个请求，每秒一个
				var err error
				client := newClient(true)
				for i := 0; i < 5; i++ {
					r := doer.Do(client, func(httptrace.GotConnInfo) {}, fmt.Sprintf("/echo?message=attempt-%d", i), 1*time.Second)
					err = r.err
					if err == nil && r.response.StatusCode != http.StatusOK {
						err = fmt.Errorf("did not get status code 200 - %#v", r.response)
						break
					}
					time.Sleep(time.Second)
				}
				preShutdownHookErrCh <- err
				return nil
			})
			if err != nil {
				t.Fatalf("Failed to add pre-shutdown hook - %v", err)
			}

			// 启动 API server
			runCompletedCh := make(chan struct{})
			go func() {
				defer close(runCompletedCh)
				if err := s.PrepareRun().RunWithContext(stopCtx); err != nil {
					t.Errorf("unexpected error from RunWithContext: %v", err)
				}
			}()
			waitForAPIServerStarted(t, doer)

			stop(errors.New("shutting down"))

			waitForeverUntil(t, runCompletedCh, "the apiserver Run method did not return")

			select {
			case err := <-preShutdownHookErrCh:
				if err != nil {
					t.Errorf("PreSHutdown hook can not access the API server - %v", err)
				}
			case <-time.After(wait.ForeverTestTimeout):
				t.Fatalf("pre-shutdown hook did not complete as expected")
			}
		})
	}
}

// signalRecorder 线程安全地记录生命周期信号的触发顺序。
type signalRecorder struct {
	lock  sync.Mutex
	order []string
}

// before 记录信号名。
func (r *signalRecorder) before(s lifecycleSignal) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.order = append(r.order, s.Name())
}

// verify 比对实际记录顺序与期望顺序。
func (r *signalRecorder) verify(got []string) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	want := r.order
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("Expected order of termination event signal to match, diff: %s", cmp.Diff(want, got))
	}
	return nil
}

// inFlightRequest 描述一个被故意阻塞、贯穿关闭流程的请求。
type inFlightRequest struct {
	blockedCh, startedCh chan struct{}
	resultCh             chan result
	url                  string
}

// setupInFlightNonLongRunningRequestHandler 注册一个普通请求处理器，
// 它先通知测试“已到达服务端”，再阻塞直到被显式放行。
func setupInFlightNonLongRunningRequestHandler(s *GenericAPIServer) *inFlightRequest {
	inflight := &inFlightRequest{
		blockedCh: make(chan struct{}),
		startedCh: make(chan struct{}),
		resultCh:  make(chan result),
		url:       "/in-flight-non-long-running-request-as-designed",
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(inflight.startedCh)
		// 该请求处理器会一直阻塞，直到我们主动放行它。
		<-inflight.blockedCh
		w.WriteHeader(http.StatusOK)
	})
	s.Handler.NonGoRestfulMux.Handle(inflight.url, handler)
	return inflight
}

// setupInFlightWatchRequestHandler 模拟 watch：阻塞到被放行后，再等待
// ServerShutdownSignal.ShuttingDown，以验证关闭信号会传递给 watch 处理器。
func setupInFlightWatchRequestHandler(s *GenericAPIServer) *inFlightRequest {
	inflight := &inFlightRequest{
		blockedCh: make(chan struct{}),
		startedCh: make(chan struct{}),
		resultCh:  make(chan result),
		url:       "/apis/watches.group/v1/namespaces/foo/bar?watch=true",
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		close(inflight.startedCh)
		// 该请求处理器会一直阻塞，直到我们主动放行它。
		<-inflight.blockedCh

		// 对本测试而言，这样模拟 watch 已经足够
		signals := apirequest.ServerShutdownSignalFrom(req.Context())
		if signals == nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		<-signals.ShuttingDown()
		w.WriteHeader(http.StatusOK)
	})
	s.Handler.NonGoRestfulMux.Handle("/apis/watches.group/v1/namespaces/foo/bar", handler)
	return inflight
}

// launch 异步发送请求，结果写入 resultCh。
func (ifr *inFlightRequest) launch(doer doer, client *http.Client) {
	go func() {
		result := doer.Do(client, func(httptrace.GotConnInfo) {}, ifr.url, 0)
		ifr.resultCh <- result
	}()
}

// unblockAndWaitForResult 放行被阻塞的请求并等待其响应。
func (ifr *inFlightRequest) unblockAndWaitForResult(t *testing.T) result {
	close(ifr.blockedCh)

	var resultGot result
	select {
	case resultGot = <-ifr.resultCh:
		return resultGot
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("Expected the server to send a response")
	}
	return resultGot
}

// preShutdownHookHandler 用于协调测试与 pre-shutdown hook 的执行时机。
type preShutdownHookHandler struct {
	blockedCh chan struct{}
	resultCh  chan result
}

// setupPreShutdownHookHandler 注册一个会向 API server 发起调用的
// pre-shutdown hook，调用时机由测试通过 blockedCh 控制。
func setupPreShutdownHookHandler(t *testing.T, s *GenericAPIServer, doer doer, client *http.Client) *preShutdownHookHandler {
	hook := &preShutdownHookHandler{
		blockedCh: make(chan struct{}),
		resultCh:  make(chan result),
	}
	if err := s.AddPreShutdownHook("test-preshutdown-hook", func() error {
		// 等待测试下达指令，让该 pre-shutdown hook 发起一次 API 调用。
		<-hook.blockedCh

		resultGot := doer.Do(client, func(httptrace.GotConnInfo) {}, "/echo?message=request-from-pre-shutdown-hook-should-succeed", time.Second)
		hook.resultCh <- resultGot
		return nil
	}); err != nil {
		t.Fatalf("Failed to register preshutdown hook - %v", err)
	}

	return hook
}

// fakeAudit 同时充当审计后端与策略评估器，记录已审计的请求 ID
// 以及 Shutdown 是否完成。
type fakeAudit struct {
	shutdownCh chan struct{}
	lock       sync.Mutex
	audits     map[string]struct{}
	completed  bool
}

// Run 启动一个等待 stopCh 的 goroutine，模拟审计后端的生命周期。
func (a *fakeAudit) Run(stopCh <-chan struct{}) error {
	a.shutdownCh = make(chan struct{})
	go func() {
		defer close(a.shutdownCh)
		<-stopCh
	}()
	return nil
}

// Shutdown 等待 Run 的 stopCh 关闭后标记完成。
func (a *fakeAudit) Shutdown() {
	<-a.shutdownCh

	a.lock.Lock()
	defer a.lock.Unlock()
	a.completed = true
}

// String 返回后端标识。
func (a *fakeAudit) String() string {
	return "fake-audit"
}

// shutdownCompleted 报告 Shutdown 是否已被调用完成。
func (a *fakeAudit) shutdownCompleted() bool {
	a.lock.Lock()
	defer a.lock.Unlock()

	return a.completed
}

// ProcessEvents 记录事件中的 AuditID。
func (a *fakeAudit) ProcessEvents(events ...*auditinternal.Event) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	if len(a.audits) == 0 {
		a.audits = map[string]struct{}{}
	}
	for _, event := range events {
		a.audits[string(event.AuditID)] = struct{}{}
	}

	return true
}

// requestAudited 判断指定 AuditID 是否被记录。
func (a *fakeAudit) requestAudited(auditID string) bool {
	a.lock.Lock()
	defer a.lock.Unlock()
	_, exists := a.audits[auditID]
	return exists
}

// EvaluatePolicyRule 对所有请求返回 Metadata 级别，足以验证审计链路。
func (a *fakeAudit) EvaluatePolicyRule(attrs authorizer.Attributes) audit.RequestAuditConfig {
	return audit.RequestAuditConfig{
		Level: auditinternal.LevelMetadata,
	}
}

// assertRequestAudited 校验响应头带 audit-id 且后端确实收到该事件。
func assertRequestAudited(resultGot result, backend *fakeAudit) error {
	resp := resultGot.response
	if resp == nil {
		return fmt.Errorf("Expected a response, but got nil")
	}
	auditIDGot := resp.Header.Get(auditinternal.HeaderAuditID)
	if len(auditIDGot) == 0 {
		return fmt.Errorf("Expected non-empty %q response header, but got: %v", auditinternal.HeaderAuditID, resp)
	}
	if !backend.requestAudited(auditIDGot) {
		return fmt.Errorf("Expected the request to be audited: %q", auditIDGot)
	}
	return nil
}

// waitForeverUntilSignaled 等待生命周期信号，超时即测试失败。
func waitForeverUntilSignaled(t *testing.T, s lifecycleSignal) {
	waitForeverUntil(t, s.Signaled(), fmt.Sprintf("Expected the server to signal %s event", s.Name()))
}

// waitForeverUntil 等待 channel 关闭；超时使用统一上限，避免永久挂起。
func waitForeverUntil(t *testing.T, ch <-chan struct{}, msg string) {
	select {
	case <-ch:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("%s", msg)
	}
}

// shouldReuseConnection 断言请求复用了已有 TCP 连接。
func shouldReuseConnection(t *testing.T) func(httptrace.GotConnInfo) {
	return func(ci httptrace.GotConnInfo) {
		if !ci.Reused {
			t.Errorf("Expected the request to use an existing TCP connection, but got: %+v", ci)
		}
	}
}

// shouldUseNewConnection 断言请求使用了新的 TCP 连接。
func shouldUseNewConnection(t *testing.T) func(httptrace.GotConnInfo) {
	return func(ci httptrace.GotConnInfo) {
		if ci.Reused {
			t.Errorf("Expected the request to use a new TCP connection, but got: %+v", ci)
		}
	}
}

// assertResponseStatusCode 断言请求无错误且状态码符合预期。
func assertResponseStatusCode(resultGot result, statusCodeExpected int) error {
	if resultGot.err != nil {
		return fmt.Errorf("Expected no error, but got: %v", resultGot.err)
	}
	if resultGot.response.StatusCode != statusCodeExpected {
		return fmt.Errorf("Expected Status Code: %d, but got: %d", statusCodeExpected, resultGot.response.StatusCode)
	}
	return nil
}

// requestMustFailWithRetryHeader 断言请求被 429 拒绝且带 Retry-After: 5。
func requestMustFailWithRetryHeader(resultGot result, statusCodedExpected int) error {
	if resultGot.err != nil {
		return fmt.Errorf("Expected no error, but got: %v", resultGot.err)
	}
	if statusCodedExpected != resultGot.response.StatusCode {
		return fmt.Errorf("Expected Status Code: %d, but got: %d", statusCodedExpected, resultGot.response.StatusCode)
	}
	retryAfterGot := resultGot.response.Header.Get("Retry-After")
	if retryAfterGot != "5" {
		return fmt.Errorf("Expected Retry-After Response Header, but got: %v", resultGot.response)
	}
	return nil
}

// waitForAPIServerStarted 轮询 /echo 直到服务端可用，避免固定 sleep。
func waitForAPIServerStarted(t *testing.T, doer doer) {
	client := newClient(true)
	i := 1
	err := wait.PollImmediate(100*time.Millisecond, 5*time.Second, func() (done bool, err error) {
		result := doer.Do(client, func(httptrace.GotConnInfo) {}, fmt.Sprintf("/echo?message=attempt-%d", i), time.Second)
		i++

		if result.err != nil {
			t.Logf("Still waiting for the server to start - err: %v", err)
			return false, nil
		}
		if result.response.StatusCode != http.StatusOK {
			t.Logf("Still waiting for the server to start - expecting: %d, but got: %v", http.StatusOK, result.response)
			return false, nil
		}

		t.Log("The API server has started")
		return true, nil
	})

	if err != nil {
		t.Fatalf("The server has failed to start - err: %v", err)
	}
}

// setupDoer 构造指向测试 server 的 doer，并支持通过 httptrace 观察连接复用。
func setupDoer(t *testing.T, info *SecureServingInfo) doer {
	_, port, err := info.HostPort()
	if err != nil {
		t.Fatalf("Expected host, port from SecureServingInfo, but got: %v", err)
	}

	return func(client *http.Client, callback func(httptrace.GotConnInfo), path string, timeout time.Duration) result {
		url := fmt.Sprintf("https://%s:%d%s", "127.0.0.1", port, path)
		t.Logf("Sending request - timeout: %s, url: %s", timeout, url)

		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return result{response: nil, err: err}
		}

		// 设置请求超时
		// timeout 为 0 表示不设超时（用于需要长期保持的 in-flight 请求）。
		var ctx context.Context
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(req.Context(), timeout)
			defer cancel()

			req = req.WithContext(ctx)
		}

		// 设置 trace
		trace := &httptrace.ClientTrace{
			GotConn: func(connInfo httptrace.GotConnInfo) {
				callback(connInfo)
			},
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

		response, err := client.Do(req)
		// 在本测试中我们不依赖响应体内容，因此可以在此关闭 Body，
		// 以确保底层传输连接能够被复用
		if response != nil {
			io.ReadAll(response.Body)
			response.Body.Close()
		}
		return result{
			err:      err,
			response: response,
		}
	}
}

// newClient 创建信任测试 CA 的 HTTP/2 客户端；
// useNewConnection 为 true 时禁用 keep-alive，强制每次新建连接。
func newClient(useNewConnection bool) *http.Client {
	clientCACertPool := x509.NewCertPool()
	clientCACertPool.AppendCertsFromPEM(backendCrt)
	tlsConfig := &tls.Config{
		RootCAs:    clientCACertPool,
		NextProtos: []string{http2.NextProtoTLS},
	}

	tr := &http.Transport{
		TLSClientConfig:   tlsConfig,
		DisableKeepAlives: useNewConnection,
	}
	if err := http2.ConfigureTransport(tr); err != nil {
		log.Fatalf("Failed to configure HTTP2 transport: %v", err)
	}
	return &http.Client{
		Timeout:   0,
		Transport: tr,
	}
}

// getOpenAPIDefinitionsForTest 提供最小的 OpenAPI 定义集合，
// 让 server 初始化无需依赖完整 API 类型。
func getOpenAPIDefinitionsForTest(_ kubeopenapi.ReferenceCallback) map[string]kubeopenapi.OpenAPIDefinition {
	return map[string]kubeopenapi.OpenAPIDefinition{
		"io.k8s.apimachinery.pkg.apis.meta.v1.APIGroupList": {},
	}
}

// newGenericAPIServer 构造测试用 GenericAPIServer。
// keepListening 控制 ShutdownSendRetryAfter；ShutdownDelayDuration 设短
// 以缩短测试时间，但仍留出验证窗口。
func newGenericAPIServer(t *testing.T, fAudit *fakeAudit, keepListening bool) *GenericAPIServer {
	config, _ := setUp(t)
	config.ShutdownDelayDuration = 100 * time.Millisecond
	config.ShutdownSendRetryAfter = keepListening
	// 我们启用 watch 排空（draining），任何正值都会启用它
	config.ShutdownWatchTerminationGracePeriod = 2 * time.Second
	config.AuditPolicyRuleEvaluator = fAudit
	config.AuditBackend = fAudit
	namer := openapi.NewDefinitionNamer(clientscheme.Scheme)
	config.OpenAPIConfig = DefaultOpenAPIConfig(getOpenAPIDefinitionsForTest, namer)
	config.OpenAPIV3Config = DefaultOpenAPIV3Config(getOpenAPIDefinitionsForTest, namer)

	s, err := config.Complete(nil).New("test", NewEmptyDelegate())
	if err != nil {
		t.Fatalf("Error in bringing up the server: %v", err)
	}

	ln, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("failed to listen on %v: %v", "0.0.0.0:0", err)
	}
	s.SecureServingInfo = &SecureServingInfo{}
	s.SecureServingInfo.Listener = &wrappedListener{ln, t}

	cert, err := dynamiccertificates.NewStaticCertKeyContent("serving-cert", backendCrt, backendKey)
	if err != nil {
		t.Fatalf("failed to load cert - %v", err)
	}
	s.SecureServingInfo.Cert = cert

	// 我们用这个 handler 向服务端发送测试请求。
	s.Handler.NonGoRestfulMux.Handle("/echo", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		t.Logf("[server] received a request, proto: %s, url: %s", req.Proto, req.RequestURI)

		w.Header().Add("echo", req.URL.Query().Get("message"))
		w.WriteHeader(http.StatusOK)
	}))

	return s
}

// wrappedListener 记录新连接，便于观察连接复用行为。
type wrappedListener struct {
	net.Listener
	t *testing.T
}

// Accept 接受连接并记录是否为 TCP 连接。
func (ln wrappedListener) Accept() (net.Conn, error) {
	c, err := ln.Listener.Accept()

	if tc, ok := c.(*net.TCPConn); ok {
		ln.t.Logf("[server] seen new connection: %#v", tc)
	}
	return c, err
}
