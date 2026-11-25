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
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
	"k8s.io/apiserver/pkg/authentication/user"
	apifilters "k8s.io/apiserver/pkg/endpoints/filters"
	epmetrics "k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/mux"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fcmetrics "k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"

	"github.com/google/go-cmp/cmp"
)

// TestMain 初始化 klog 标志，使测试可通过 -v 观察过滤器的日志。
func TestMain(m *testing.M) {
	klog.InitFlags(nil)
	os.Exit(m.Run())
}

// mockDecision 枚举假过滤器对一次请求可以做出的处理结果。
// 用例通过选择不同结果，覆盖 APF 过滤器对每种决策的分支处理。
type mockDecision int

const (
	decisionNoQueuingExecute mockDecision = iota
	decisionQueuingExecute
	decisionCancelWait
	decisionReject
	decisionSkipFilter
)

// defaultRequestWorkEstimator 为每个请求固定估算 1 个席位，
// 让测试中的并发行为只取决于配置，而不受工作量估算影响。
var defaultRequestWorkEstimator = func(req *http.Request, fsName, plName string) fcrequest.WorkEstimate {
	return fcrequest.WorkEstimate{InitialSeats: 1}
}

// fakeApfFilter 用预设决策模拟 APF 过滤器的行为，不启动真实队列。
type fakeApfFilter struct {
	mockDecision mockDecision
	postEnqueue  func()
	postDequeue  func()

	utilflowcontrol.WatchTracker
	utilflowcontrol.MaxSeatsTracker
}

// Handle 依据 mockDecision 演练一次完整的 APF 生命周期，
// 并在入队/出队时调用 hook，供用例断言当前排队数。
func (t fakeApfFilter) Handle(ctx context.Context,
	requestDigest utilflowcontrol.RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	workEstimator func() fcrequest.WorkEstimate,
	queueNoteFn fq.QueueNoteFn,
	execFn func(),
) {
	if t.mockDecision == decisionSkipFilter {
		panic("Handle should not be invoked")
	}
	noteFn(bootstrap.SuggestedFlowSchemaGlobalDefault, bootstrap.SuggestedPriorityLevelConfigurationGlobalDefault, requestDigest.User.GetName())
	switch t.mockDecision {
	case decisionNoQueuingExecute:
		execFn()
	case decisionQueuingExecute:
		queueNoteFn(true)
		t.postEnqueue()
		queueNoteFn(false)
		t.postDequeue()
		execFn()
	case decisionCancelWait:
		queueNoteFn(true)
		t.postEnqueue()
		queueNoteFn(false)
		t.postDequeue()
	case decisionReject:
		return
	}
}

// Run 无操作：测试不使用真实控制器。
func (t fakeApfFilter) Run(stopCh <-chan struct{}) error {
	return nil
}

// Install 无操作：测试不暴露 APF 的调试端点。
func (t fakeApfFilter) Install(c *mux.PathRecorderMux) {
}

// newApfServerWithSingleRequest 构造只处理单个请求的测试服务器，并在执行、
// 入队、出队三个时点断言计数器的瞬时值恰为 1 或 0。
func newApfServerWithSingleRequest(t *testing.T, decision mockDecision) *httptest.Server {
	onExecuteFunc := func() {
		if decision == decisionCancelWait {
			t.Errorf("execute should not be invoked")
		}
		// 由于每次只测试一个请求，atomicReadOnlyExecuting 可能是 0 或 1。
		currentValue := atomicReadOnlyExecuting.Load()
		if decision != decisionSkipFilter && currentValue != 1 {
			t.Errorf("Wanted %d requests executing, got %d", 1, currentValue)
		}
	}

	postExecuteFunc := func() {}
	// 由于每次只测试一个请求，atomicReadOnlyWaiting 可能是 0 或 1。
	postEnqueueFunc := func() {
		currentValue := atomicReadOnlyWaiting.Load()
		if currentValue != 1 {
			t.Errorf("Wanted %d requests in queue, got %d", 1, currentValue)
		}
	}

	postDequeueFunc := func() {
		currentValue := atomicReadOnlyWaiting.Load()
		if currentValue != 0 {
			t.Errorf("Wanted %d requests in queue, got %d", 0, currentValue)
		}
	}
	return newApfServerWithHooks(t, decision, onExecuteFunc, postExecuteFunc, postEnqueueFunc, postDequeueFunc)
}

// newApfServerWithHooks 用注入的 hook 构造测试服务器，供多请求用例协调时序。
func newApfServerWithHooks(t *testing.T, decision mockDecision, onExecute, postExecute, postEnqueue, postDequeue func()) *httptest.Server {
	fakeFilter := fakeApfFilter{
		mockDecision:    decision,
		postEnqueue:     postEnqueue,
		postDequeue:     postDequeue,
		WatchTracker:    utilflowcontrol.NewWatchTracker(),
		MaxSeatsTracker: utilflowcontrol.NewMaxSeatsTracker(),
	}
	return newApfServerWithFilter(t, fakeFilter, time.Minute/4, onExecute, postExecute)
}

// newApfServerWithFilter 用任意过滤器实现构造测试服务器；注册指标是必需的，
// 因为部分用例会抓取全局注册表。
func newApfServerWithFilter(t *testing.T, flowControlFilter utilflowcontrol.Interface, defaultWaitLimit time.Duration, onExecute, postExecute func()) *httptest.Server {
	epmetrics.Register()
	fcmetrics.Register()
	apfServer := httptest.NewServer(newApfHandlerWithFilter(t, flowControlFilter, defaultWaitLimit, onExecute, postExecute))
	return apfServer
}

// newApfHandlerWithFilter 组装与生产一致的处理器链：RequestInfo -> 注入用户 -> APF。
// 在 APF 结束后断言"执行中"计数归零，确保无论执行还是拒绝都完成了记账。
func newApfHandlerWithFilter(t *testing.T, flowControlFilter utilflowcontrol.Interface, defaultWaitLimit time.Duration, onExecute, postExecute func()) http.Handler {
	requestInfoFactory := &apirequest.RequestInfoFactory{APIPrefixes: sets.NewString("apis", "api"), GrouplessAPIPrefixes: sets.NewString("api")}
	longRunningRequestCheck := BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString("proxy"))

	apfHandler := WithPriorityAndFairness(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		onExecute()
	}), longRunningRequestCheck, flowControlFilter, defaultRequestWorkEstimator, defaultWaitLimit)

	handler := apifilters.WithRequestInfo(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(apirequest.WithUser(r.Context(), &user.DefaultInfo{
			Groups: []string{user.AllUnauthenticated},
		}))
		func() {
			// APF 处理器结束运行，无论是正常返回还是
			// panic，两种情况下所有 APF 记账此刻都必须
			// 已完成。此外，无论请求是被执行还是被拒绝，
			// 我们都期望该计数器的值为零。
			// TODO: 使用此过滤器的所有测试必须
			// 相互串行运行
			defer func() {
				currentValue := atomicReadOnlyExecuting.Load()
				if currentValue != 0 {
					t.Errorf("Wanted %d requests executing, got %d", 0, currentValue)
				}
			}()
			apfHandler.ServeHTTP(w, r)
			postExecute()
		}()
	}), requestInfoFactory)

	return handler
}

// TestApfSkipLongRunningRequest 验证长时请求（此处是 proxy）绕过 APF：
// 假过滤器被设为"不应被调用"，若被调用会 panic，因此能通过即证明跳过生效。
func TestApfSkipLongRunningRequest(t *testing.T) {
	server := newApfServerWithSingleRequest(t, decisionSkipFilter)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartPriorityAndFairnessWatermarkMaintenance(ctx.Done())

	// 发送一个 watch 请求，用于测试长时请求被跳过
	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/foos/foo/proxy", server.URL), http.StatusOK); err != nil {
		// 该请求不应被拒绝
		t.Error(err)
	}
}

// TestApfRejectRequest 验证被拒绝的请求返回 429，并记录终止与请求总数指标。
func TestApfRejectRequest(t *testing.T) {
	server := newApfServerWithSingleRequest(t, decisionReject)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartPriorityAndFairnessWatermarkMaintenance(ctx.Done())

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default", server.URL), http.StatusTooManyRequests); err != nil {
		t.Error(err)
	}

	checkForExpectedMetrics(t, []string{
		"apiserver_request_terminations_total",
		"apiserver_request_total",
	})
}

// TestApfExemptRequest 验证免排队（exempt）路径直接执行且不产生排队统计。
func TestApfExemptRequest(t *testing.T) {
	server := newApfServerWithSingleRequest(t, decisionNoQueuingExecute)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartPriorityAndFairnessWatermarkMaintenance(ctx.Done())

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default", server.URL), http.StatusOK); err != nil {
		t.Error(err)
	}

	checkForExpectedMetrics(t, []string{
		"apiserver_current_inflight_requests",
		"apiserver_flowcontrol_read_vs_write_current_requests",
	})
}

// TestApfExecuteRequest 验证经过排队后成功执行，并同时出现 inflight 与 inqueue 指标。
func TestApfExecuteRequest(t *testing.T) {
	server := newApfServerWithSingleRequest(t, decisionQueuingExecute)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartPriorityAndFairnessWatermarkMaintenance(ctx.Done())

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default", server.URL), http.StatusOK); err != nil {
		t.Error(err)
	}

	checkForExpectedMetrics(t, []string{
		"apiserver_current_inflight_requests",
		"apiserver_current_inqueue_requests",
		"apiserver_flowcontrol_read_vs_write_current_requests",
	})
}

// TestApfExecuteMultipleRequests 验证多个请求同时在排队/执行时计数正确。
//
// 通过六组 WaitGroup 让所有请求先全部入队、再全部出队、再全部执行，
// 从而在小并发下确定性地命中"计数值等于并发数"的断言，而不靠 sleep 碰运气。
func TestApfExecuteMultipleRequests(t *testing.T) {
	concurrentRequests := 5
	preStartExecute, postStartExecute := &sync.WaitGroup{}, &sync.WaitGroup{}
	preEnqueue, postEnqueue := &sync.WaitGroup{}, &sync.WaitGroup{}
	preDequeue, postDequeue := &sync.WaitGroup{}, &sync.WaitGroup{}
	finishExecute := &sync.WaitGroup{}
	for _, wg := range []*sync.WaitGroup{preStartExecute, postStartExecute, preEnqueue, postEnqueue, preDequeue, postDequeue, finishExecute} {
		wg.Add(concurrentRequests)
	}

	onExecuteFunc := func() {
		preStartExecute.Done()
		preStartExecute.Wait()
		currentValue := atomicReadOnlyExecuting.Load()
		if int(currentValue) != concurrentRequests {
			t.Errorf("Wanted %d requests executing, got %d", concurrentRequests, currentValue)
		}
		postStartExecute.Done()
		postStartExecute.Wait()
	}

	postEnqueueFunc := func() {
		preEnqueue.Done()
		preEnqueue.Wait()
		currentValue := atomicReadOnlyWaiting.Load()
		if int(currentValue) != concurrentRequests {
			t.Errorf("Wanted %d requests in queue, got %d", concurrentRequests, currentValue)
		}
		postEnqueue.Done()
		postEnqueue.Wait()
	}

	postDequeueFunc := func() {
		preDequeue.Done()
		preDequeue.Wait()
		currentValue := atomicReadOnlyWaiting.Load()
		if currentValue != 0 {
			t.Errorf("Wanted %d requests in queue, got %d", 0, currentValue)
		}
		postDequeue.Done()
		postDequeue.Wait()
	}

	postExecuteFunc := func() {
		finishExecute.Done()
		finishExecute.Wait()
	}

	server := newApfServerWithHooks(t, decisionQueuingExecute, onExecuteFunc, postExecuteFunc, postEnqueueFunc, postDequeueFunc)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartPriorityAndFairnessWatermarkMaintenance(ctx.Done())

	var wg sync.WaitGroup
	wg.Add(concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		go func() {
			defer wg.Done()
			if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default", server.URL), http.StatusOK); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	checkForExpectedMetrics(t, []string{
		"apiserver_current_inflight_requests",
		"apiserver_current_inqueue_requests",
		"apiserver_flowcontrol_read_vs_write_current_requests",
	})
}

// TestApfCancelWaitRequest 验证请求在排队期间被取消时以 429 结束，
// 并计入终止指标——取消同样是一种"未被执行"的终态。
func TestApfCancelWaitRequest(t *testing.T) {
	server := newApfServerWithSingleRequest(t, decisionCancelWait)
	defer server.Close()

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default", server.URL), http.StatusTooManyRequests); err != nil {
		t.Error(err)
	}

	checkForExpectedMetrics(t, []string{
		"apiserver_current_inflight_requests",
		"apiserver_request_terminations_total",
		"apiserver_request_total",
	})
}

// fakeWatchApfFilter 以容量计数模拟并发限制，并提供在执行为前后注入 panic 的开关，
// 用于验证 APF 在 watch 场景下的记账与恢复行为。
type fakeWatchApfFilter struct {
	lock     sync.Mutex
	inflight int
	capacity int

	postExecutePanic bool
	preExecutePanic  bool

	utilflowcontrol.WatchTracker
	utilflowcontrol.MaxSeatsTracker
}

// newFakeWatchApfFilter 构造指定容量的 watch 过滤器。
func newFakeWatchApfFilter(capacity int) *fakeWatchApfFilter {
	return &fakeWatchApfFilter{
		capacity:        capacity,
		WatchTracker:    utilflowcontrol.NewWatchTracker(),
		MaxSeatsTracker: utilflowcontrol.NewMaxSeatsTracker(),
	}
}

// Handle 在容量允许时占用一个名额并执行，否则直接返回（模拟拒绝/排队超限）。
// 名额在最后释放，panic 场景由调用方保证记账仍然成对。
func (f *fakeWatchApfFilter) Handle(ctx context.Context,
	requestDigest utilflowcontrol.RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	_ func() fcrequest.WorkEstimate,
	_ fq.QueueNoteFn,
	execFn func(),
) {
	noteFn(bootstrap.SuggestedFlowSchemaGlobalDefault, bootstrap.SuggestedPriorityLevelConfigurationGlobalDefault, requestDigest.User.GetName())
	canExecute := false
	func() {
		f.lock.Lock()
		defer f.lock.Unlock()
		if f.inflight < f.capacity {
			f.inflight++
			canExecute = true
		}
	}()
	if !canExecute {
		return
	}

	if f.preExecutePanic {
		panic("pre-exec-panic")
	}
	execFn()
	if f.postExecutePanic {
		panic("post-exec-panic")
	}

	f.lock.Lock()
	defer f.lock.Unlock()
	f.inflight--
}

func (f *fakeWatchApfFilter) Run(stopCh <-chan struct{}) error {
	return nil
}

func (t *fakeWatchApfFilter) Install(c *mux.PathRecorderMux) {
}

// wait 轮询直到所有在执行的请求结束；用于让测试在信号发出后等待名额真正释放。
func (f *fakeWatchApfFilter) wait() error {
	return wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		f.lock.Lock()
		defer f.lock.Unlock()
		return f.inflight == 0, nil
	})
}

// TestApfExecuteWatchRequestsWithInitializationSignal 验证初始化信号能让被阻塞的 watch
// 请求释放席位，从而允许后续请求进入执行。
//
// 场景：先用 N 个请求占满容量，发出信号后旧请求应被放行；随后新的一组请求也能执行。
// 这里替换 newInitializationSignal 以收集信号对象，测试结束后必须恢复全局变量。
func TestApfExecuteWatchRequestsWithInitializationSignal(t *testing.T) {
	signalsLock := sync.Mutex{}
	signals := []utilflowcontrol.InitializationSignal{}
	sendSignals := func() {
		signalsLock.Lock()
		defer signalsLock.Unlock()
		for i := range signals {
			signals[i].Signal()
		}
		signals = signals[:0]
	}

	newInitializationSignal = func() utilflowcontrol.InitializationSignal {
		signalsLock.Lock()
		defer signalsLock.Unlock()
		signal := utilflowcontrol.NewInitializationSignal()
		signals = append(signals, signal)
		return signal
	}
	defer func() {
		newInitializationSignal = utilflowcontrol.NewInitializationSignal
	}()

	// 我们测试：收到初始化信号并完成初始化后，新请求是否会被允许运行。方法是：
	// - 发送 N 个请求，占满全部容量
	// - 为它们发送初始化信号
	// - 确认在途（inflight）请求数量会归零
	concurrentRequests := 5
	firstRunning := sync.WaitGroup{}
	firstRunning.Add(concurrentRequests)
	allRunning := sync.WaitGroup{}
	allRunning.Add(2 * concurrentRequests)

	fakeFilter := newFakeWatchApfFilter(concurrentRequests)

	onExecuteFunc := func() {
		firstRunning.Done()

		fakeFilter.wait()

		allRunning.Done()
		allRunning.Wait()
	}

	postExecuteFunc := func() {}

	server := newApfServerWithFilter(t, fakeFilter, time.Minute/4, onExecuteFunc, postExecuteFunc)
	defer server.Close()

	var wg sync.WaitGroup
	wg.Add(2 * concurrentRequests)
	for i := 0; i < concurrentRequests; i++ {
		go func() {
			defer wg.Done()
			if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default/pods?watch=true", server.URL), http.StatusOK); err != nil {
				t.Error(err)
			}
		}()
	}

	firstRunning.Wait()
	sendSignals()
	fakeFilter.wait()
	firstRunning.Add(concurrentRequests)

	for i := 0; i < concurrentRequests; i++ {
		go func() {
			defer wg.Done()
			if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default/pods?watch=true", server.URL), http.StatusOK); err != nil {
				t.Error(err)
			}
		}()
	}
	firstRunning.Wait()
	sendSignals()
	wg.Wait()
}

// TestApfRejectWatchRequestsWithInitializationSignal 验证容量为 0 时 watch 请求被拒绝（429）。
func TestApfRejectWatchRequestsWithInitializationSignal(t *testing.T) {
	fakeFilter := newFakeWatchApfFilter(0)

	onExecuteFunc := func() {
		t.Errorf("Request unexepectedly executing")
	}
	postExecuteFunc := func() {}

	server := newApfServerWithFilter(t, fakeFilter, time.Minute/4, onExecuteFunc, postExecuteFunc)
	defer server.Close()

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default/pods?watch=true", server.URL), http.StatusTooManyRequests); err != nil {
		t.Error(err)
	}
}

// TestApfWatchPanic 验证内层处理器 panic 时 panic 会传播到服务器（由更外层的
// 恢复逻辑负责），而 APF 不吞掉它。
func TestApfWatchPanic(t *testing.T) {
	epmetrics.Register()
	fcmetrics.Register()

	fakeFilter := newFakeWatchApfFilter(1)

	onExecuteFunc := func() {
		panic("test panic")
	}
	postExecuteFunc := func() {}

	apfHandler := newApfHandlerWithFilter(t, fakeFilter, time.Minute/4, onExecuteFunc, postExecuteFunc)
	handler := func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err == nil {
				t.Errorf("expected panic, got %v", err)
			}
		}()
		apfHandler.ServeHTTP(w, r)
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default/pods?watch=true", server.URL), http.StatusOK); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestApfWatchHandlePanic 覆盖 panic 发生在"执行前"与"执行后"两个位置的情形，
// 两者都必须传播出去，且 APF 的席位记账不能遗留。
func TestApfWatchHandlePanic(t *testing.T) {
	epmetrics.Register()
	fcmetrics.Register()
	preExecutePanicingFilter := newFakeWatchApfFilter(1)
	preExecutePanicingFilter.preExecutePanic = true

	postExecutePanicingFilter := newFakeWatchApfFilter(1)
	postExecutePanicingFilter.postExecutePanic = true

	testCases := []struct {
		name   string
		filter *fakeWatchApfFilter
	}{
		{
			name:   "pre-execute panic",
			filter: preExecutePanicingFilter,
		},
		{
			name:   "post-execute panic",
			filter: postExecutePanicingFilter,
		},
	}

	onExecuteFunc := func() {
		time.Sleep(5 * time.Second)
	}
	postExecuteFunc := func() {}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			apfHandler := newApfHandlerWithFilter(t, test.filter, time.Minute/4, onExecuteFunc, postExecuteFunc)
			handler := func(w http.ResponseWriter, r *http.Request) {
				defer func() {
					if err := recover(); err == nil {
						t.Errorf("expected panic, got %v", err)
					}
				}()
				apfHandler.ServeHTTP(w, r)
			}
			server := httptest.NewServer(http.HandlerFunc(handler))
			defer server.Close()

			if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default/pods?watch=true", server.URL), http.StatusOK); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// TestContextClosesOnRequestProcessed 确保即使服务器没有显式取消，请求上下文也会被自动取消。
// 这是必需的，以确保不会泄漏等待上下文取消的 goroutine（例如 queueset::StartRequest 方法中的）。
// 虽然生产环境中并不使用 httptest.Server，但这两种场景共享这段逻辑。
func TestContextClosesOnRequestProcessed(t *testing.T) {
	epmetrics.Register()
	fcmetrics.Register()
	wg := sync.WaitGroup{}
	wg.Add(1)
	handler := func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// 异步等待上下文关闭
		go func() {
			<-ctx.Done()
			wg.Done()
		}()
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	if err := expectHTTPGet(fmt.Sprintf("%s/api/v1/namespaces/default/pods?watch=true", server.URL), http.StatusOK); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	wg.Wait()
}

// fakeFilterRequestDigest 记录 APF 过滤器实际收到的 RequestDigest 与 WorkEstimate。
type fakeFilterRequestDigest struct {
	*fakeApfFilter
	requestDigestGot *utilflowcontrol.RequestDigest
	workEstimateGot  fcrequest.WorkEstimate
}

// Handle 捕获 digest 与工作量估算，供用例与期望值比较。
func (f *fakeFilterRequestDigest) Handle(ctx context.Context,
	requestDigest utilflowcontrol.RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	workEstimator func() fcrequest.WorkEstimate,
	_ fq.QueueNoteFn, _ func(),
) {
	f.requestDigestGot = &requestDigest
	noteFn(bootstrap.MandatoryFlowSchemaCatchAll, bootstrap.MandatoryPriorityLevelConfigurationCatchAll, "")
	f.workEstimateGot = workEstimator()
}

// TestApfWithRequestDigest 验证 APF 过滤器收到的请求摘要（请求信息 + 用户）
// 与工作量估算与构造时一致，确保上下文传递链路没有丢失字段。
func TestApfWithRequestDigest(t *testing.T) {
	epmetrics.Register()
	fcmetrics.Register()
	longRunningFunc := func(_ *http.Request, _ *apirequest.RequestInfo) bool { return false }
	fakeFilter := &fakeFilterRequestDigest{}

	reqDigestExpected := &utilflowcontrol.RequestDigest{
		RequestInfo: &apirequest.RequestInfo{Verb: "get"},
		User:        &user.DefaultInfo{Name: "foo"},
	}
	workExpected := fcrequest.WorkEstimate{
		InitialSeats:      5,
		FinalSeats:        7,
		AdditionalLatency: 3 * time.Second,
	}

	handler := WithPriorityAndFairness(http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {}),
		longRunningFunc,
		fakeFilter,
		func(_ *http.Request, _, _ string) fcrequest.WorkEstimate { return workExpected },
		time.Minute/4,
	)

	w := httptest.NewRecorder()
	req, err := http.NewRequest(http.MethodGet, "/bar", nil)
	if err != nil {
		t.Fatalf("Failed to create new http request - %v", err)
	}
	req = req.WithContext(apirequest.WithRequestInfo(req.Context(), reqDigestExpected.RequestInfo))
	req = req.WithContext(apirequest.WithUser(req.Context(), reqDigestExpected.User))

	handler.ServeHTTP(w, req)

	if !reflect.DeepEqual(reqDigestExpected, fakeFilter.requestDigestGot) {
		t.Errorf("Expected RequestDigest to match, diff: %s", cmp.Diff(reqDigestExpected, fakeFilter.requestDigestGot))
	}
	if !reflect.DeepEqual(workExpected, fakeFilter.workEstimateGot) {
		t.Errorf("Expected WorkEstimate to match, diff: %s", cmp.Diff(workExpected, fakeFilter.workEstimateGot))
	}
}

// TestPriorityAndFairnessWithPanicRecoveryAndTimeoutFilter 在完整处理器链
// （RequestInfo、Deadline、Timeout、APF、PanicRecovery）下验证 APF 的路由与记账。
//
// 每个子用例针对一种关键的交互点：panic 后席位是否释放、超时与写入的先后、
// 排队请求在超时后的归宿。使用 HTTP/2 是因为需要观察 stream reset 语义。
func TestPriorityAndFairnessWithPanicRecoveryAndTimeoutFilter(t *testing.T) {
	epmetrics.Register()
	fcmetrics.Register()
	timeFmt := "15:04:05.999"

	t.Run("priority level concurrency is set to 1, request handler panics, next request should not be rejected", func(t *testing.T) {
		const (
			userName                                              = "alice"
			fsName                                                = "test-fs"
			plName                                                = "test-pl"
			serverConcurrency, plConcurrencyShares, plConcurrency = 1, 1, 1
		)

		apfConfiguration := newConfiguration(fsName, plName, userName, plConcurrencyShares, 0)
		stopCh := make(chan struct{})
		controller, controllerCompletedCh := startAPFController(t, stopCh, apfConfiguration, serverConcurrency, plName, plConcurrency)

		headerMatcher := headerMatcher{}
		// 我们将对第一个请求主动触发一次 panic。
		firstRequestPathPanic, secondRequestPathShouldWork := "/request/panic-as-designed", "/request/should-succeed-as-expected"
		firstHandlerDoneCh, secondHandlerDoneCh := make(chan struct{}), make(chan struct{})
		requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerMatcher.inspect(t, w, r.Context(), fsName, plName)
			switch {
			case r.URL.Path == firstRequestPathPanic:
				close(firstHandlerDoneCh)
				panic(fmt.Errorf("request handler panic'd as designed - %#v", r.RequestURI))
			case r.URL.Path == secondRequestPathShouldWork:
				close(secondHandlerDoneCh)

			}
		})

		// NOTE: 服务器将对每个传入请求强制 1m 超时，
		//  而客户端的超时时间为 2m。
		handler := newHandlerChain(t, requestHandler, controller, userName, time.Minute)
		server, requestGetter := newHTTP2ServerWithClient(handler, 2*time.Minute)
		defer server.Close()

		// 我们同步发送两个请求，一次一个
		//  - 第一个请求预期会按设计 panic
		//  - 第二个请求预期会成功
		_, err := requestGetter(firstRequestPathPanic)

		// 服务器处理器是否如预期发生了 panic？
		select {
		case <-firstHandlerDoneCh:
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected the server handler to panic for request: %q", firstRequestPathPanic)
		}
		if isClientTimeout(err) {
			t.Fatalf("the client has unexpectedly timed out - request: %q error: %s", firstRequestPathPanic, err.Error())
		}
		expectResetStreamError(t, err)

		// 第二个请求应当被成功处理。
		response, err := requestGetter(secondRequestPathShouldWork)
		if err != nil {
			t.Fatalf("Expected request: %q to get a response, but got error: %#v", secondRequestPathShouldWork, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Errorf("Expected HTTP status code: %d for request: %q, but got: %#v", http.StatusOK, secondRequestPathShouldWork, response)
		}
		select {
		case <-secondHandlerDoneCh:
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected the server handler to have completed: %q", secondRequestPathShouldWork)
		}

		close(stopCh)
		t.Log("Waiting for the controller to shutdown")

		controllerErr := <-controllerCompletedCh
		if controllerErr != nil {
			t.Errorf("Expected no error from the controller, but got: %#v", controllerErr)
		}
	})

	t.Run("priority level concurrency is set to 1, request times out and inner handler hasn't written to the response yet", func(t *testing.T) {
		t.Parallel()
		const (
			userName                                              = "alice"
			fsName                                                = "test-fs"
			plName                                                = "test-pl"
			serverConcurrency, plConcurrencyShares, plConcurrency = 1, 1, 1
		)

		apfConfiguration := newConfiguration(fsName, plName, userName, plConcurrencyShares, 0)
		stopCh := make(chan struct{})
		controller, controllerCompletedCh := startAPFController(t, stopCh, apfConfiguration, serverConcurrency, plName, plConcurrency)

		headerMatcher := headerMatcher{}
		rquestTimesOutPath := "/request/time-out-as-designed"
		reqHandlerCompletedCh, callerRoundTripDoneCh := make(chan struct{}), make(chan struct{})
		requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerMatcher.inspect(t, w, r.Context(), fsName, plName)

			if r.URL.Path == rquestTimesOutPath {
				defer close(reqHandlerCompletedCh)

				// 这将迫使请求超时。
				<-callerRoundTripDoneCh
			}
		})

		// NOTE: 服务器将对每个
		// 传入请求强制 5s 超时，而客户端的超时时间为 1m。
		handler := newHandlerChain(t, requestHandler, controller, userName, 5*time.Second)
		server, requestGetter := newHTTP2ServerWithClient(handler, time.Minute)
		defer server.Close()

		// 以 1m 客户端超时同步发送请求，这可以把 CI 中 flake 的概率降到最低：
		// 客户端会等待足够长的时间，让服务器向客户端发送超时响应。
		var (
			response *http.Response
			err      error
		)
		func() {
			defer close(callerRoundTripDoneCh)

			t.Logf("Waiting for the request: %q to time out", rquestTimesOutPath)
			response, err = requestGetter(rquestTimesOutPath)
			if isClientTimeout(err) {
				t.Fatalf("the client has unexpectedly timed out - request: %q error: %s", rquestTimesOutPath, err.Error())
			}
		}()

		t.Logf("Waiting for the inner handler of the request: %q to complete", rquestTimesOutPath)
		select {
		case <-reqHandlerCompletedCh:
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected the server handler to have completed: %q", rquestTimesOutPath)
		}

		if err != nil {
			t.Fatalf("Expected request: %q to get a response, but got error: %#v", rquestTimesOutPath, err)
		}
		if response.StatusCode != http.StatusGatewayTimeout {
			t.Errorf("Expected HTTP status code: %d for request: %q, but got: %#v", http.StatusGatewayTimeout, rquestTimesOutPath, response)
		}

		close(stopCh)
		t.Log("Waiting for the controller to shutdown")

		controllerErr := <-controllerCompletedCh
		if controllerErr != nil {
			t.Errorf("Expected no error from the controller, but got: %#v", controllerErr)
		}
	})

	t.Run("priority level concurrency is set to 1, inner handler panics after the request times out", func(t *testing.T) {
		t.Parallel()
		const (
			userName                                              = "alice"
			fsName                                                = "test-fs"
			plName                                                = "test-pl"
			serverConcurrency, plConcurrencyShares, plConcurrency = 1, 1, 1
		)

		apfConfiguration := newConfiguration(fsName, plName, userName, plConcurrencyShares, 0)
		stopCh := make(chan struct{})
		controller, controllerCompletedCh := startAPFController(t, stopCh, apfConfiguration, serverConcurrency, plName, plConcurrency)

		headerMatcher := headerMatcher{}
		reqHandlerErrCh, callerRoundTripDoneCh := make(chan error, 1), make(chan struct{})
		rquestTimesOutPath := "/request/time-out-as-designed"
		requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerMatcher.inspect(t, w, r.Context(), fsName, plName)

			if r.URL.Path == rquestTimesOutPath {
				<-callerRoundTripDoneCh

				// 此刻我们预期超时处理器已经让该请求超时，此后任何
				// 对响应的写入都应返回 http.ErrHandlerTimeout 错误。
				_, innerHandlerWriteErr := w.Write([]byte("foo"))
				reqHandlerErrCh <- innerHandlerWriteErr

				panic(http.ErrAbortHandler)
			}
		})

		// NOTE: 服务器将对每个
		// 传入请求强制 5s 超时，而客户端的超时时间为 1m。
		handler := newHandlerChain(t, requestHandler, controller, userName, 5*time.Second)
		server, requestGetter := newHTTP2ServerWithClient(handler, time.Minute)
		defer server.Close()

		// 以 1m 客户端超时同步发送请求，这可以把 CI 中 flake 的概率降到最低：
		// 客户端会等待足够长的时间，让服务器向客户端发送超时响应。
		var (
			response *http.Response
			err      error
		)
		func() {
			defer close(callerRoundTripDoneCh)
			t.Logf("Waiting for the request: %q to time out", rquestTimesOutPath)
			response, err = requestGetter(rquestTimesOutPath)
			if isClientTimeout(err) {
				t.Fatalf("the client has unexpectedly timed out - request: %q error: %s", rquestTimesOutPath, err.Error())
			}
		}()

		t.Logf("Waiting for the inner handler of the request: %q to complete", rquestTimesOutPath)
		select {
		case innerHandlerWriteErr := <-reqHandlerErrCh:
			if innerHandlerWriteErr != http.ErrHandlerTimeout {
				t.Fatalf("Expected error: %#v, but got: %#v", http.ErrHandlerTimeout, err)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected the server handler to have completed: %q", rquestTimesOutPath)
		}

		if err != nil {
			t.Fatalf("Expected request: %q to get a response, but got error: %#v", rquestTimesOutPath, err)
		}
		if response.StatusCode != http.StatusGatewayTimeout {
			t.Errorf("Expected HTTP status code: %d for request: %q, but got: %#v", http.StatusGatewayTimeout, rquestTimesOutPath, response)
		}

		close(stopCh)
		t.Log("Waiting for the controller to shutdown")

		controllerErr := <-controllerCompletedCh
		if controllerErr != nil {
			t.Errorf("Expected no error from the controller, but got: %#v", controllerErr)
		}
	})

	t.Run("priority level concurrency is set to 1, inner handler writes to the response before request times out", func(t *testing.T) {
		t.Parallel()
		const (
			userName                                              = "alice"
			fsName                                                = "test-fs"
			plName                                                = "test-pl"
			serverConcurrency, plConcurrencyShares, plConcurrency = 1, 1, 1
		)

		apfConfiguration := newConfiguration(fsName, plName, userName, plConcurrencyShares, 0)
		stopCh := make(chan struct{})
		controller, controllerCompletedCh := startAPFController(t, stopCh, apfConfiguration, serverConcurrency, plName, plConcurrency)

		headerMatcher := headerMatcher{}
		rquestTimesOutPath := "/request/time-out-as-designed"
		reqHandlerErrCh, callerRoundTripDoneCh := make(chan error, 1), make(chan struct{})
		requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerMatcher.inspect(t, w, r.Context(), fsName, plName)

			if r.URL.Path == rquestTimesOutPath {

				// 内层处理器先写入响应头，然后让请求超时。
				w.WriteHeader(http.StatusBadRequest)
				<-callerRoundTripDoneCh

				// 此刻我们预期超时处理器已经让该请求超时，此后任何
				// 对响应的写入都应返回 http.ErrHandlerTimeout 错误。
				_, innerHandlerWriteErr := w.Write([]byte("foo"))
				reqHandlerErrCh <- innerHandlerWriteErr
			}
		})

		// NOTE: 服务器将对每个
		// 传入请求强制 5s 超时，而客户端的超时时间为 1m。
		handler := newHandlerChain(t, requestHandler, controller, userName, 5*time.Second)
		server, requestGetter := newHTTP2ServerWithClient(handler, time.Minute)
		defer server.Close()

		// 以 1m 客户端超时同步发送请求，这可以把 CI 中 flake 的概率降到最低：
		// 客户端会等待足够长的时间，让服务器向客户端发送超时响应。
		var err error
		func() {
			defer close(callerRoundTripDoneCh)
			t.Logf("Waiting for the request: %q to time out", rquestTimesOutPath)
			_, err = requestGetter(rquestTimesOutPath)
			if isClientTimeout(err) {
				t.Fatalf("the client has unexpectedly timed out - request: %q error: %s", rquestTimesOutPath, err.Error())
			}
		}()

		t.Logf("Waiting for the inner handler of the request: %q to complete", rquestTimesOutPath)
		select {
		case innerHandlerWriteErr := <-reqHandlerErrCh:
			if innerHandlerWriteErr != http.ErrHandlerTimeout {
				t.Fatalf("Expected error: %#v, but got: %#v", http.ErrHandlerTimeout, err)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected the server handler to have completed: %q", rquestTimesOutPath)
		}

		expectResetStreamError(t, err)

		close(stopCh)
		t.Log("Waiting for the controller to shutdown")

		controllerErr := <-controllerCompletedCh
		if controllerErr != nil {
			t.Errorf("Expected no error from the controller, but got: %#v", controllerErr)
		}
	})

	t.Run("priority level concurrency is set to 1, queue length is 1, first request should time out and second (enqueued) request should time out as well", func(t *testing.T) {
		t.Parallel()

		type result struct {
			err      error
			response *http.Response
		}

		const (
			userName                                                           = "alice"
			fsName                                                             = "test-fs"
			plName                                                             = "test-pl"
			serverConcurrency, plConcurrencyShares, plConcurrency, queueLength = 1, 1, 1, 1
		)

		apfConfiguration := newConfiguration(fsName, plName, userName, plConcurrencyShares, queueLength)
		stopCh := make(chan struct{})
		controller, controllerCompletedCh := startAPFController(t, stopCh, apfConfiguration, serverConcurrency, plName, plConcurrency)

		headerMatcher := headerMatcher{}
		firstRequestTimesOutPath, secondRequestEnqueuedPath := "/request/first/time-out-as-designed", "/request/second/enqueued-as-designed"
		firstReqHandlerErrCh, firstReqInProgressCh := make(chan error, 1), make(chan struct{})
		firstReqRoundTripDoneCh, secondReqRoundTripDoneCh := make(chan struct{}), make(chan struct{})
		requestHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headerMatcher.inspect(t, w, r.Context(), fsName, plName)
			switch {
			case r.URL.Path == firstRequestTimesOutPath:
				close(firstReqInProgressCh)
				<-firstReqRoundTripDoneCh

				// 确保我们等到第二个请求的调用方返回为止，这是为了
				// 保证第二个请求永远没有机会被执行（避免 flake）
				<-secondReqRoundTripDoneCh

				// 此刻我们预期超时处理器已经让该请求超时，此后任何
				// 对响应的写入都应返回 http.ErrHandlerTimeout 错误。
				_, firstRequestInnerHandlerWriteErr := w.Write([]byte("foo"))
				firstReqHandlerErrCh <- firstRequestInnerHandlerWriteErr

			case r.URL.Path == secondRequestEnqueuedPath:
				// 我们预期并发数被设为 1，因此该请求永远不应被执行。
				t.Errorf("Expected second request to be enqueued: %q", secondRequestEnqueuedPath)
			}
		})

		// NOTE: 服务器将对每个
		// 传入请求强制 5s 超时，而客户端的超时时间为 1m。
		handler := newHandlerChain(t, requestHandler, controller, userName, 5*time.Second)
		server, requestGetter := newHTTP2ServerWithClient(handler, time.Minute)
		defer server.Close()

		// 本测试涉及发送到同一 priority level 的两个请求，该 priority level 有 1 个队列、
		// 并发上限为 1。处理器链包含超时过滤器。
		// 每个请求由独立 goroutine 发送，客户端侧超时为 1m；另一侧，
		// 服务器强制 5s 超时（通过超时过滤器）。
		// 第一个请求应当被立即派发；执行时 (a) 先关闭那个触发第二个客户端
		// goroutine 发送其请求的通道，然后 (b) 等待两个客户端 goroutine 都拿到
		// 响应（预期都是超时）。
		// 第二个请求会停留在队列中，直到超时过滤器起作用，而过滤器对两个请求的
		// 处理是并发的。对第一个请求，这应让客户端拿到超时响应，但不直接影响其执行。
		// 对第二个请求，超时过滤器关闭其 Context.Done() 这一事实会使其被立刻
		// 逐出队列。执行 APF 处理的 goroutine 会写出一个状态码为 429 的
		// HTTP 响应消息。
		// 超时处理器在一个 goroutine 中调用其内层处理器，同时在其原始 goroutine 中
		// 对时间流逝作出反应。对超时的反应要么是 (a) 写出一个状态码为 504 的
		// HTTP 响应消息以指示超时，要么是 (b) 关闭 HTTP/2 流；当连接已被
		// "hijacked"（劫持）或其他某个 goroutine（例如运行内层处理器的）已开始写响应时，
		// 采用后者。因此在本文所测场景中，两个 goroutine 会对第二个请求的
		// 响应形成竞争，测试允许它们的任何一种响应结果。
		firstReqResultCh, secondReqResultCh := make(chan result, 1), make(chan result, 1)
		go func() {
			defer close(firstReqRoundTripDoneCh)
			t.Logf("At %s, Sending request: %q", time.Now().Format(timeFmt), firstRequestTimesOutPath)
			resp, err := requestGetter(firstRequestTimesOutPath)
			t.Logf("At %s, RoundTrip of request: %q has completed", time.Now().Format(timeFmt), firstRequestTimesOutPath)
			firstReqResultCh <- result{err: err, response: resp}
		}()
		go func() {
			// 必须等“第一个”请求开始执行后，
			// 才能发起“第二个”。
			defer close(secondReqRoundTripDoneCh)

			<-firstReqInProgressCh
			t.Logf("At %s, Sending request: %q", time.Now().Format(timeFmt), secondRequestEnqueuedPath)
			resp, err := requestGetter(secondRequestEnqueuedPath)
			t.Logf("At %s, RoundTrip of request: %q has completed", time.Now().Format(timeFmt), secondRequestEnqueuedPath)
			secondReqResultCh <- result{err: err, response: resp}
		}()

		firstReqResult := <-firstReqResultCh
		if isClientTimeout(firstReqResult.err) {
			t.Fatalf("the client has unexpectedly timed out - request: %q error: %s", firstRequestTimesOutPath, fmtError(firstReqResult.err))
		}
		t.Logf("Waiting for the inner handler of the request: %q to complete", firstRequestTimesOutPath)
		select {
		case firstRequestInnerHandlerWriteErr := <-firstReqHandlerErrCh:
			if firstRequestInnerHandlerWriteErr != http.ErrHandlerTimeout {
				t.Fatalf("Expected error: %#v, but got: %s", http.ErrHandlerTimeout, fmtError(firstRequestInnerHandlerWriteErr))
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Expected the server handler to have completed: %q", firstRequestTimesOutPath)
		}

		// 第一个请求预期会超时。
		if isStreamReset(firstReqResult.err) || firstReqResult.response.StatusCode != http.StatusGatewayTimeout {
			// 结果符合预期
		} else if firstReqResult.err != nil {
			t.Fatalf("Expected request: %q to get a response or stream reset, but got error: %s", firstRequestTimesOutPath, fmtError(firstReqResult.err))
		} else if firstReqResult.response.StatusCode != http.StatusGatewayTimeout {
			t.Errorf("Expected HTTP status code: %d for request: %q, but got: %#+v", http.StatusGatewayTimeout, firstRequestTimesOutPath, firstReqResult.response)
		}

		// 第二个请求预期要么被拒绝（理想行为），要么超时（当前对理想行为的近似实现）
		secondReqResult := <-secondReqResultCh
		if isClientTimeout(secondReqResult.err) {
			t.Fatalf("the client has unexpectedly timed out - request: %q error: %s", secondRequestEnqueuedPath, fmtError(secondReqResult.err))
		}
		if isStreamReset(secondReqResult.err) || secondReqResult.response.StatusCode == http.StatusTooManyRequests || secondReqResult.response.StatusCode == http.StatusGatewayTimeout {
			// 结果符合预期
		} else if secondReqResult.err != nil {
			t.Fatalf("Expected request: %q to get a response or stream reset, but got error: %s", secondRequestEnqueuedPath, fmtError(secondReqResult.err))
		} else if !(secondReqResult.response.StatusCode == http.StatusTooManyRequests || secondReqResult.response.StatusCode == http.StatusGatewayTimeout) {
			t.Errorf("Expected HTTP status code: %d or %d for request: %q, but got: %#+v", http.StatusTooManyRequests, http.StatusGatewayTimeout, secondRequestEnqueuedPath, secondReqResult.response)
		}

		close(stopCh)
		t.Log("Waiting for the controller to shutdown")

		controllerErr := <-controllerCompletedCh
		if controllerErr != nil {
			t.Errorf("Expected no error from the controller, but got: %s", fmtError(controllerErr))
		}
	})
}

// fmtError 同时输出错误的结构与文本，便于在失败日志里区分"包装错误"与底层原因。
func fmtError(err error) string {
	return fmt.Sprintf("%#+v=%q", err, err.Error())
}

// startAPFController 启动真实的 APF 控制器并等待目标优先级级别生效后再返回，
// 避免请求在配置尚未同步时被错误地归类。
func startAPFController(t *testing.T, stopCh <-chan struct{}, apfConfiguration []runtime.Object, serverConcurrency int,
	plName string, plConcurrency int) (utilflowcontrol.Interface, <-chan error) {
	clientset := newClientset(t, apfConfiguration...)
	// 本测试不依赖 resync，因此 resync 周期设为零
	factory := informers.NewSharedInformerFactory(clientset, 0)
	controller := utilflowcontrol.New(factory, clientset.FlowcontrolV1(), serverConcurrency)

	factory.Start(stopCh)

	// 等待 informer 缓存完成同步。
	timeout, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	cacheSyncDone := factory.WaitForCacheSync(timeout.Done())
	if names := unsyncedInformers(cacheSyncDone); len(names) > 0 {
		t.Fatalf("WaitForCacheSync did not successfully complete, resources=%#v", names)
	}

	controllerCompletedCh := make(chan error)
	var controllerErr error
	go func() {
		controllerErr = controller.Run(stopCh)
		controllerCompletedCh <- controllerErr
	}()

	// 确保 APF 控制器同步了本测试使用的 priority level 配置对象。
	// 读取指标，确认该 priority level 的并发上限被设为期望值。
	pollErr := wait.PollImmediate(100*time.Millisecond, 5*time.Second, func() (done bool, err error) {
		if err := gaugeValueMatch("apiserver_flowcontrol_nominal_limit_seats", map[string]string{"priority_level": plName}, plConcurrency); err != nil {
			t.Logf("polling retry - error: %s", err)
			return false, nil
		}
		return true, nil
	})
	if pollErr != nil {
		t.Fatalf("expected the apf controller to sync the priotity level configuration object: %s", plName)
	}

	return controller, controllerCompletedCh
}

// newHTTP2ServerWithClient 返回一个已启动的 http2 server，以及一个向该 server 发送请求的 client 函数。
// 必须启用 HTTP/2：测试要区分"普通超时响应"与"stream reset"两种语义。
func newHTTP2ServerWithClient(handler http.Handler, clientTimeout time.Duration) (*httptest.Server, func(path string) (*http.Response, error)) {
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.StartTLS()
	cli := server.Client()
	cli.Timeout = clientTimeout
	return server, func(path string) (*http.Response, error) {
		return cli.Get(server.URL + path)
	}
}

// headerMatcher 断言 APF 把匹配到的 FlowSchema / PriorityLevelConfiguration UID
// 写入了响应头，便于调用方观测自己落在哪个流控桶中。
type headerMatcher struct{}

// 验证期望的 FlowSchema 和 PriorityLevelConfiguration UID 已附加到响应头上。
func (m *headerMatcher) inspect(t *testing.T, w http.ResponseWriter, ctx context.Context, expectedFS, expectedPL string) {
	t.Helper()

	if w == nil {
		t.Errorf("expected a non nil HTTP response")
		return
	}

	checkHeader := func(key, expected string) {
		actual := w.Header().Get(key)
		if actual == expected {
			return
		}

		// 请求上下文被取消后，响应头的写入是尽力而为的。
		if actual == "" {
			select {
			case <-ctx.Done():
				t.Logf("Skipping APF header assertion for %s: request context already done", key)
				return
			default:
			}

			// 超时过滤器可能早在 APF 有机会附加其响应头之前
			// 就已提交了响应。
			if ctx.Err() != nil {
				t.Logf("Skipping APF header assertion for %s: response committed after timeout", key)
				return
			}
		}

		t.Errorf("expected HTTP header %s to have value %q, but got: %q", key, expected, actual)
	}

	checkHeader(flowcontrol.ResponseHeaderMatchedFlowSchemaUID, expectedFS)
	checkHeader(flowcontrol.ResponseHeaderMatchedPriorityLevelConfigurationUID, expectedPL)
}

// 当请求 panic 时，http2 会以 INTERNAL_ERROR 消息重置该 stream
func expectResetStreamError(t *testing.T, err error) {
	if err == nil {
		t.Fatalf("expected the server to send an error, but got nil")
	}
	if isStreamReset(err) {
		return
	}
	t.Fatalf("expected a stream reset error, but got %#+v=%s", err, err.Error())
}

// newClientset 构造 fake clientset；创建失败属于测试基础设施错误，直接 Fatal。
func newClientset(t *testing.T, objects ...runtime.Object) clientset.Interface {
	clientset := fake.NewSimpleClientset(objects...)
	if clientset == nil {
		t.Fatal("unable to create fake client set")
	}
	return clientset
}

// 构建包含 panic 恢复与超时过滤器的处理器链，以便模拟真实 apiserver 的行为。
// 指定用户会作为已认证用户加入请求上下文。
//
// 链的顺序即语义：AuditInit -> PanicRecovery -> RequestInfo -> Deadline -> Timeout -> 用户注入 -> APF。
// 超时过滤器必须位于 APF 内层，这样 APF 的记账能观察到超时导致的取消。
func newHandlerChain(t *testing.T, handler http.Handler, filter utilflowcontrol.Interface, userName string, requestTimeout time.Duration) http.Handler {
	requestInfoFactory := &apirequest.RequestInfoFactory{APIPrefixes: sets.NewString("apis", "api"), GrouplessAPIPrefixes: sets.NewString("api")}
	longRunningRequestCheck := BasicLongRunningRequestCheck(sets.NewString("watch"), sets.NewString("proxy"))

	apfHandler := WithPriorityAndFairness(handler, longRunningRequestCheck, filter, defaultRequestWorkEstimator, time.Minute/4)

	// 在链中加入把指定用户写入请求上下文的处理器
	handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r = r.WithContext(apirequest.WithUser(r.Context(), &user.DefaultInfo{
			Name:   userName,
			Groups: []string{user.AllAuthenticated},
		}))

		apfHandler.ServeHTTP(w, r)
	})

	handler = WithTimeoutForNonLongRunningRequests(handler, longRunningRequestCheck)
	// 我们没有超时值非法的请求，因此 audit policy 和 sink 保持为 nil。
	handler = apifilters.WithRequestDeadline(handler, nil, nil, longRunningRequestCheck, nil, requestTimeout)
	handler = apifilters.WithRequestInfo(handler, requestInfoFactory)
	handler = WithPanicRecovery(handler, requestInfoFactory)
	handler = apifilters.WithAuditInit(handler)
	return handler
}

// unsyncedInformers 返回尚未同步的 informer 类型名，用于在失败信息中点名具体资源。
func unsyncedInformers(status map[reflect.Type]bool) []string {
	names := make([]string, 0)

	for objType, synced := range status {
		if !synced {
			names = append(names, objType.Name())
		}
	}

	return names
}

// newConfiguration 构造 FlowSchema + PriorityLevelConfiguration 两个对象。
//
// queueLength 为 0 时使用 Reject 响应类型（立即拒绝），大于 0 时切换到 Queuing，
// 使同一套用例能覆盖"无队列"和"有队列"两种限流形态。
func newConfiguration(fsName, plName, user string, concurrency int32, queueLength int32) []runtime.Object {
	fs := &flowcontrol.FlowSchema{
		ObjectMeta: metav1.ObjectMeta{
			Name: fsName,
			UID:  types.UID(fsName),
		},
		Spec: flowcontrol.FlowSchemaSpec{
			MatchingPrecedence: 1,
			PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{
				Name: plName,
			},
			DistinguisherMethod: &flowcontrol.FlowDistinguisherMethod{
				Type: flowcontrol.FlowDistinguisherMethodByUserType,
			},
			Rules: []flowcontrol.PolicyRulesWithSubjects{
				{
					Subjects: []flowcontrol.Subject{
						{
							Kind: flowcontrol.SubjectKindUser,
							User: &flowcontrol.UserSubject{
								Name: user,
							},
						},
					},
					NonResourceRules: []flowcontrol.NonResourcePolicyRule{
						{
							Verbs:           []string{flowcontrol.VerbAll},
							NonResourceURLs: []string{flowcontrol.NonResourceAll},
						},
					},
				},
			},
		},
	}

	var (
		responseType flowcontrol.LimitResponseType = flowcontrol.LimitResponseTypeReject
		qcfg         *flowcontrol.QueuingConfiguration
	)
	if queueLength > 0 {
		responseType = flowcontrol.LimitResponseTypeQueue
		qcfg = &flowcontrol.QueuingConfiguration{
			Queues:           1,
			QueueLengthLimit: queueLength,
			HandSize:         1,
		}
	}
	pl := &flowcontrol.PriorityLevelConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: plName,
			UID:  types.UID(plName),
		},
		Spec: flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(concurrency),
				LimitResponse: flowcontrol.LimitResponse{
					Type:    responseType,
					Queuing: qcfg,
				},
			},
		},
	}

	return []runtime.Object{fs, pl}
}

// 收集并检查指标。
func checkForExpectedMetrics(t *testing.T, expectedMetrics []string) {
	metricsFamily, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Failed to gather metrics %v", err)
	}

	metrics := map[string]interface{}{}
	for _, mf := range metricsFamily {
		metrics[*mf.Name] = mf
	}

	for _, metricName := range expectedMetrics {
		if _, ok := metrics[metricName]; !ok {
			if !ok {
				t.Errorf("Scraped metrics did not include expected metric %s", metricName)
			}
		}
	}
}

// gaugeValueMatch 确保带 labelFilter 的 gauge 指标取值符合预期。
// 汇总所有匹配标签的序列，兼容同名 gauge 因其他维度拆分成多条的情况。
func gaugeValueMatch(name string, labelFilter map[string]string, wantValue int) error {
	metrics, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics: %s", err)
	}

	sum := 0
	familyMatch, labelMatch := false, false
	for _, mf := range metrics {
		if mf.GetName() != name {
			continue
		}

		familyMatch = true
		for _, metric := range mf.GetMetric() {
			if !testutil.LabelsMatch(metric, labelFilter) {
				continue
			}

			labelMatch = true
			sum += int(metric.GetGauge().GetValue())
		}
	}
	if !familyMatch {
		return fmt.Errorf("expected to find the metric family: %s in the gathered result", name)
	}
	if !labelMatch {
		return fmt.Errorf("expected to find metrics with matching labels: %#+v", labelFilter)
	}
	if wantValue != sum {
		return fmt.Errorf("expected the sum to be: %d, but got: %d for gauge metric: %s with labels %#+v", wantValue, sum, name, labelFilter)
	}

	return nil
}

// isClientTimeout 判断错误是否为客户端侧超时；用于区分"服务端响应超时"与"测试环境过慢"。
func isClientTimeout(err error) bool {
	if urlErr, ok := err.(*url.Error); ok {
		return urlErr.Timeout()
	}
	return false
}

// isStreamReset 通过错误文本中的 INTERNAL_ERROR 识别 HTTP/2 stream reset；
// 客户端库没有暴露更精确的类型，只能字符串匹配。
func isStreamReset(err error) bool {
	if err == nil {
		return false
	}
	if urlErr, ok := err.(*url.Error); ok {
		// 遗憾的是，客户端接收不到关于 stream reset 的
		// 更具体的指示。
		return strings.Contains(urlErr.Err.Error(), "INTERNAL_ERROR")
	}
	return false
}

// TestGetRequestWaitContext 验证排队等待上限的计算规则：
// 取剩余 deadline 的四分之一，且不超过硬上限 1 分钟；同时从"请求接收时间"起算，
// 防止慢客户端把排队时间也算进新的等待窗口。
func TestGetRequestWaitContext(t *testing.T) {
	tests := []struct {
		name                    string
		defaultRequestWaitLimit time.Duration
		parent                  func(t time.Time) (context.Context, context.CancelFunc)
		newReqWaitCtxExpected   bool
		reqWaitLimitExpected    time.Duration
	}{
		{
			name: "context deadline has exceeded",
			parent: func(time.Time) (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "context has a deadline, 'received at' is not set, wait limit should be one fourth of the remaining deadline from now",
			parent: func(now time.Time) (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), now.Add(60*time.Second))
			},
			newReqWaitCtxExpected: true,
			reqWaitLimitExpected:  15 * time.Second,
		},
		{
			name: "context has a deadline, 'received at' is set, wait limit should be one fourth of the deadline starting from the 'received at' time",
			parent: func(now time.Time) (context.Context, context.CancelFunc) {
				ctx := apirequest.WithReceivedTimestamp(context.Background(), now.Add(-10*time.Second))
				return context.WithDeadline(ctx, now.Add(50*time.Second))
			},
			newReqWaitCtxExpected: true,
			reqWaitLimitExpected:  5 * time.Second, // from now
		},
		{
			name:                    "context does not have any deadline, 'received at' is not set, default wait limit should be in effect from now",
			defaultRequestWaitLimit: 15 * time.Second,
			parent: func(time.Time) (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			newReqWaitCtxExpected: true,
			reqWaitLimitExpected:  15 * time.Second,
		},
		{
			name:                    "context does not have any deadline, 'received at' is set, default wait limit should be in effect starting from the 'received at' time",
			defaultRequestWaitLimit: 15 * time.Second,
			parent: func(now time.Time) (context.Context, context.CancelFunc) {
				ctx := apirequest.WithReceivedTimestamp(context.Background(), now.Add(-10*time.Second))
				return context.WithCancel(ctx)
			},
			newReqWaitCtxExpected: true,
			reqWaitLimitExpected:  5 * time.Second, // from now
		},
		{
			name: "context has a deadline, wait limit should not exceed the hard limit of 1m",
			parent: func(now time.Time) (context.Context, context.CancelFunc) {
				// 让剩余 deadline 的 1/4 超过硬上限
				return context.WithDeadline(context.Background(), now.Add(8*time.Minute))
			},
			newReqWaitCtxExpected: true,
			reqWaitLimitExpected:  time.Minute,
		},
		{
			name:                    "context has no deadline, wait limit should not exceed the hard limit of 1m",
			defaultRequestWaitLimit: 2 * time.Minute, // it exceeds the hard limit
			parent: func(now time.Time) (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			newReqWaitCtxExpected: true,
			reqWaitLimitExpected:  time.Minute,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Now()
			parent, cancel := test.parent(now)
			defer cancel()

			clock := clocktesting.NewFakePassiveClock(now)
			newReqWaitCtxGot, cancelGot := getRequestWaitContext(parent, test.defaultRequestWaitLimit, clock)
			if cancelGot == nil {
				t.Errorf("Expected a non nil context.CancelFunc")
				return
			}
			defer cancelGot()

			switch {
			case test.newReqWaitCtxExpected:
				deadlineGot, ok := newReqWaitCtxGot.Deadline()
				if !ok {
					t.Errorf("Expected the new wait limit context to have a deadline")
				}
				if waitLimitGot := deadlineGot.Sub(now); test.reqWaitLimitExpected != waitLimitGot {
					t.Errorf("Expected request wait limit %s, but got: %s", test.reqWaitLimitExpected, waitLimitGot)
				}
			default:
				if parent != newReqWaitCtxGot {
					t.Errorf("Expected the parent context to be returned: want: %#v, got %#v", parent, newReqWaitCtxGot)
				}
			}
		})
	}
}
