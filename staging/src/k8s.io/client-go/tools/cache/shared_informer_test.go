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

package cache

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	fcache "k8s.io/client-go/tools/cache/testing"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/klog/v2/textlogger"
	testingclock "k8s.io/utils/clock/testing"
)

// testListener 是记录收到对象名的测试事件处理器，可校验 resync 是否按周期触发。
type testListener struct {
	printlnFunc       func(string) // Some, but not all tests use this for per-test output.
	lock              sync.RWMutex
	resyncPeriod      time.Duration
	expectedItemNames sets.Set[string]
	receivedItemNames []string
	name              string
}

func newTestListener(name string, resyncPeriod time.Duration, expected ...string) *testListener {
	l := &testListener{
		resyncPeriod:      resyncPeriod,
		expectedItemNames: sets.New(expected...),
		name:              name,
	}
	return l
}

func (l *testListener) OnAdd(obj interface{}, isInInitialList bool) {
	l.handle(obj)
}

func (l *testListener) OnUpdate(old, new interface{}) {
	l.handle(new)
}

func (l *testListener) OnDelete(obj interface{}) {
}

func (l *testListener) println(msg string) {
	msg = l.name + ": " + msg
	if l.printlnFunc != nil {
		l.printlnFunc(msg)
	} else {
		fmt.Println(msg)
	}
}

func (l *testListener) handle(obj interface{}) {
	key, _ := MetaNamespaceKeyFunc(obj)
	l.println(fmt.Sprintf("handle: %v", key))
	l.lock.Lock()
	defer l.lock.Unlock()
	objectMeta, _ := meta.Accessor(obj)
	l.receivedItemNames = append(l.receivedItemNames, objectMeta.GetName())
}

// ok 轮询等待收到预期对象集合，再额外等待一秒以确保没有多余事件。
func (l *testListener) ok() bool {
	l.println("polling")
	err := wait.PollImmediate(100*time.Millisecond, 2*time.Second, func() (bool, error) {
		if l.satisfiedExpectations() {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return false
	}

	// 稍等片刻，让意外滞留的事件有机会到达。
	l.println("sleeping")
	time.Sleep(1 * time.Second)
	l.println("final check")
	return l.satisfiedExpectations()
}

func (l *testListener) satisfiedExpectations() bool {
	l.lock.RLock()
	defer l.lock.RUnlock()

	return sets.New(l.receivedItemNames...).Equal(l.expectedItemNames)
}

func (l *testListener) reset() {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.receivedItemNames = nil
}

// eventHandlerCount 直接读取内部 processor 的监听器数量；仅测试包内可用。
func eventHandlerCount(i SharedInformer) int {
	s := i.(*sharedIndexInformer)
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	return len(s.processor.listeners)
}

// isStarted 在锁保护下读取 informer 的 started 标志。
func isStarted(i SharedInformer) bool {
	s := i.(*sharedIndexInformer)
	s.startedLock.Lock()
	defer s.startedLock.Unlock()
	return s.started
}

// isRegistered 通过 processor 内部映射判断注册句柄是否仍然有效。
func isRegistered(i SharedInformer, h ResourceEventHandlerRegistration) bool {
	s := i.(*sharedIndexInformer)
	return s.processor.getListener(h) != nil
}

// TestIndexer 验证在运行中以及事后添加索引器都能正确建立索引，
// 且 ListIndexFuncValues 能反映所有已索引的标签值。
func TestIndexer(t *testing.T) {
	assert := assert.New(t)
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", Labels: map[string]string{"a": "a-val", "b": "b-val1"}}}
	pod2 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2", Labels: map[string]string{"b": "b-val2"}}}
	pod3 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod3", Labels: map[string]string{"a": "a-val2"}}}
	source.Add(pod1)
	source.Add(pod2)

	// 创建共享 informer，每 1 秒 resync 一次。
	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	err := informer.AddIndexers(map[string]IndexFunc{
		"labels": func(obj interface{}) ([]string, error) {
			res := []string{}
			for k := range obj.(*v1.Pod).Labels {
				res = append(res, k)
			}
			return res, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	WaitForCacheSync(stop, informer.HasSynced)

	cmpOps := cmpopts.SortSlices(func(a, b any) bool {
		return a.(*v1.Pod).Name < b.(*v1.Pod).Name
	})

	// 应能按索引查找。
	res, err := informer.GetIndexer().ByIndex("labels", "a")
	assert.NoError(err)
	if diff := cmp.Diff([]any{pod1}, res); diff != "" {
		t.Fatal(diff)
	}

	// 之后新增条目同样可以。
	source.Add(pod3)
	// 事件是异步的，需要轮询等待。
	assert.Eventually(func() bool {
		res, _ := informer.GetIndexer().ByIndex("labels", "a")
		return cmp.Diff([]any{pod1, pod3}, res, cmpOps) == ""
	}, time.Second*3, time.Millisecond)

	// 之后添加索引同样可以。
	err = informer.AddIndexers(map[string]IndexFunc{
		"labels-again": func(obj interface{}) ([]string, error) {
			res := []string{}
			for k := range obj.(*v1.Pod).Labels {
				res = append(res, k)
			}
			return res, nil
		},
	})
	assert.NoError(err)

	// 应立即可用。
	res, err = informer.GetIndexer().ByIndex("labels-again", "a")
	assert.NoError(err)
	if diff := cmp.Diff([]any{pod1, pod3}, res, cmpOps); diff != "" {
		t.Fatal(diff)
	}
	if got := informer.GetIndexer().ListIndexFuncValues("labels"); !sets.New(got...).Equal(sets.New("a", "b")) {
		t.Fatalf("got %v", got)
	}
	if got := informer.GetIndexer().ListIndexFuncValues("labels-again"); !sets.New(got...).Equal(sets.New("a", "b")) {
		t.Fatalf("got %v", got)
	}
}

// TestListenerResyncPeriods 使用 synctest 的虚拟时钟验证不同监听器各自按
// 自己的 resync 周期触发，且互不串扰。
func TestListenerResyncPeriods(t *testing.T) {
	synctest.Test(t, func(t *testing.T) { testListenerResyncPeriods(t, 0) })
}

func TestListenerResyncPeriodsDelayed(t *testing.T) {
	// sharedProcessor.run 曾在这里发生死锁：
	// 经过第一次 sharedProcessor.distribute 后才会到达；这里人为加延迟模拟该情况。
	//
	// 不能与其他测试并行，但其实无需并行：它不按真实时间睡眠，很快就能完成。
	//
	// 该用例人为让 processor.run 晚于第一次 distribute，覆盖历史死锁回归。
	synctest.Test(t, func(t *testing.T) { testListenerResyncPeriods(t, time.Second) })
}

// testListenerResyncPeriods 的核心场景：三个监听器分别 0/2s/3s 重同步，
// 逐个推进虚拟时间，验证只有到期者收到事件。
func testListenerResyncPeriods(t *testing.T, startupDelay time.Duration) {
	start := time.Now()
	println := func(msg string) {
		delta := time.Since(start)
		t.Logf("%s: %s", delta, msg)
	}

	if startupDelay > 0 {
		hook := func() {
			time.Sleep(startupDelay)
		}
		sharedProcessorRunHook.Store(&hook)
		defer sharedProcessorRunHook.Store(nil)
	}

	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2"}})

	// 创建共享 informer，每 1 秒 resync 一次。
	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	// listener1：从不 resync。
	listener1 := newTestListener("listener1", 0, "pod1", "pod2")
	listener1.printlnFunc = println
	informer.AddEventHandlerWithResyncPeriod(listener1, listener1.resyncPeriod)

	// listener2：每 2 秒 resync。
	listener2 := newTestListener("listener2", 2*time.Second, "pod1", "pod2")
	listener2.printlnFunc = println
	informer.AddEventHandlerWithResyncPeriod(listener2, listener2.resyncPeriod)

	// listener3：每 3 秒 resync。
	listener3 := newTestListener("listener3", 3*time.Second, "pod1", "pod2")
	listener3.printlnFunc = println
	informer.AddEventHandlerWithResyncPeriod(listener3, listener3.resyncPeriod)
	listeners := []*testListener{listener1, listener2, listener3}

	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	// 可能需要推进时间以解除 sharedProcess.run 的阻塞，否则下面的 synctest.Wait()
	// 会在什么都没发生时返回。
	time.Sleep(startupDelay)

	// 确认在初始时刻完成初次处理后，所有 listener 都收到了初始事件。
	//
	// synctest.Wait 检测不到涉及互斥量的死锁：这类阻塞不算「持久阻塞」，因此会一直等到整个单元测试
	// 否则会超时，回溯信息将显示死锁。
	synctest.Wait()
	for _, listener := range listeners {
		if !listener.satisfiedExpectations() {
			t.Errorf("%s: %s: expected %v, got %v", time.Since(start), listener.name, listener.expectedItemNames, listener.receivedItemNames)
		}
	}

	// reset
	for _, listener := range listeners {
		listener.reset()
	}

	// 推进时间，让 listener2 收到 resync。
	time.Sleep(2*time.Second - startupDelay)
	synctest.Wait()

	// 确认 listener2 收到了 resync。
	if !listener2.satisfiedExpectations() {
		t.Errorf("%s: %s: expected %v, got %v", time.Since(start), listener2.name, listener2.expectedItemNames, listener2.receivedItemNames)
	}

	// 确认 listener1 与 listener3 没有收到任何事件。
	if len(listener1.receivedItemNames) != 0 {
		t.Errorf("%s: listener1: should not have resynced (got %d)", time.Since(start), len(listener1.receivedItemNames))
	}
	if len(listener3.receivedItemNames) != 0 {
		t.Errorf("%s: listener3: should not have resynced (got %d)", time.Since(start), len(listener3.receivedItemNames))
	}

	// reset
	for _, listener := range listeners {
		listener.reset()
	}

	// 推进时间，让 listener3 收到 resync。
	time.Sleep(1 * time.Second)
	synctest.Wait()

	// 确认 listener3 收到了 resync。
	if !listener3.satisfiedExpectations() {
		t.Errorf("%s: %s: expected %v, got %v", time.Since(start), listener3.name, listener3.expectedItemNames, listener3.receivedItemNames)
	}

	// 确认 listener1 与 listener2 没有收到任何事件。
	if len(listener1.receivedItemNames) != 0 {
		t.Errorf("%s: listener1: should not have resynced (got %d)", time.Since(start), len(listener1.receivedItemNames))
	}
	if len(listener2.receivedItemNames) != 0 {
		t.Errorf("%s: listener2: should not have resynced (got %d)", time.Since(start), len(listener2.receivedItemNames))
	}
}

// TestResyncCheckPeriod 验证 resyncCheckPeriod 取所有监听器周期的最小值，
// 已有监听器的 resync 周期不会因后续注册而改变。
func TestResyncCheckPeriod(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	// 创建共享 informer，每 12 小时 resync 一次。
	informer := NewSharedInformer(source, &v1.Pod{}, 12*time.Hour).(*sharedIndexInformer)
	gl := informer.processor.getListener

	clock := testingclock.NewFakeClock(time.Now())
	informer.clock = clock
	informer.processor.clock = clock

	// listener1：从不 resync。
	listener1 := newTestListener("listener1", 0)
	handler1, _ := informer.AddEventHandlerWithResyncPeriod(listener1, listener1.resyncPeriod)

	if e, a := 12*time.Hour, informer.resyncCheckPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := time.Duration(0), gl(handler1).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}

	// listener2：每分钟 resync。
	listener2 := newTestListener("listener2", 1*time.Minute)
	handler2, _ := informer.AddEventHandlerWithResyncPeriod(listener2, listener2.resyncPeriod)
	if e, a := 1*time.Minute, informer.resyncCheckPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := time.Duration(0), gl(handler1).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := 1*time.Minute, gl(handler2).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}

	// listener3：每 55 秒 resync。
	listener3 := newTestListener("listener3", 55*time.Second)
	handler3, _ := informer.AddEventHandlerWithResyncPeriod(listener3, listener3.resyncPeriod)
	if e, a := 55*time.Second, informer.resyncCheckPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := time.Duration(0), gl(handler1).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := 1*time.Minute, gl(handler2).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := 55*time.Second, gl(handler3).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}

	// listener4：每 5 秒 resync。
	listener4 := newTestListener("listener4", 5*time.Second)
	handler4, _ := informer.AddEventHandlerWithResyncPeriod(listener4, listener4.resyncPeriod)
	if e, a := 5*time.Second, informer.resyncCheckPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := time.Duration(0), gl(handler1).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := 1*time.Minute, gl(handler2).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := 55*time.Second, gl(handler3).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
	if e, a := 5*time.Second, gl(handler4).resyncPeriod; e != a {
		t.Errorf("expected %d, got %d", e, a)
	}
}

// 验证 https://github.com/kubernetes/kubernetes/issues/59822 已修复。
// 并发 AddEventHandler 与 Run 不应产生数据竞争或 panic。
func TestSharedInformerInitializationRace(t *testing.T) {
	source := newFakeControllerSource(t)
	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	listener := newTestListener("raceListener", 0)

	go informer.AddEventHandlerWithResyncPeriod(listener, listener.resyncPeriod)
	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()
}

// TestSharedInformerWatchDisruption 模拟 watch 被关闭的场景。
// 同时更新 store。确保启用/未启用 resync 的 handler 都能看到预期状态。
//
// 关键语义：不重同步的监听器在 relist 后只会看到发生变化的增量对象，
// 而重同步的监听器会看到全量对象。
func TestSharedInformerWatchDisruption(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", UID: "pod1", ResourceVersion: "1"}})
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: "pod2", ResourceVersion: "2"}})

	// 创建共享 informer，每 1 秒 resync 一次。
	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	clock := testingclock.NewFakeClock(time.Now())
	informer.clock = clock
	informer.processor.clock = clock

	// listener, never resync
	listenerNoResync := newTestListener("listenerNoResync", 0, "pod1", "pod2")
	informer.AddEventHandlerWithResyncPeriod(listenerNoResync, listenerNoResync.resyncPeriod)

	listenerResync := newTestListener("listenerResync", 1*time.Second, "pod1", "pod2")
	informer.AddEventHandlerWithResyncPeriod(listenerResync, listenerResync.resyncPeriod)
	listeners := []*testListener{listenerNoResync, listenerResync}

	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	for _, listener := range listeners {
		if !listener.ok() {
			t.Errorf("%s: expected %v, got %v", listener.name, listener.expectedItemNames, listener.receivedItemNames)
		}
	}

	// 加入 pod3，更新 pod2 但不广播，使该变更只在 relist 时可见。
	source.AddDropWatch(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod3", UID: "pod3", ResourceVersion: "3"}})
	source.ModifyDropWatch(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: "pod2", ResourceVersion: "4"}})

	// 确认没有任何 handler 看到变更。
	for _, listener := range listeners {
		if !listener.ok() {
			t.Errorf("%s: expected %v, got %v", listener.name, listener.expectedItemNames, listener.receivedItemNames)
		}
	}

	for _, listener := range listeners {
		listener.receivedItemNames = []string{}
	}

	listenerNoResync.expectedItemNames = sets.New("pod2", "pod3")
	listenerResync.expectedItemNames = sets.New("pod1", "pod2", "pod3")

	// 这会调用 shouldSync，把 noResync 从同步中的 listener 列表里移除。
	clock.Step(1 * time.Second)

	// 模拟连接断开（或 watch 过旧）。
	source.ResetWatch()

	// 等待足够长的时间，让 reflector 退出且退避函数开始在假时钟上等待，否则推进假时钟无效。
	// TODO：通过统计 FakeClock 上的等待者数量让这里变得确定。
	time.Sleep(10 * time.Millisecond)

	// 推进时钟使退避等待结束。
	clock.Step(1601 * time.Millisecond)

	// 等待足够长时间，让退避后再次调用 ListWatch 并向 listener 分发事件。
	time.Sleep(10 * time.Millisecond)

	for _, listener := range listeners {
		if !listener.ok() {
			t.Errorf("%s: expected %v, got %v", listener.name, listener.expectedItemNames, listener.receivedItemNames)
		}
	}
}

// TestSharedInformerErrorHandling 验证 List 错误会交给 SetWatchErrorHandler 注册的回调。
func TestSharedInformerErrorHandling(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})
	source.ListError = fmt.Errorf("Access Denied")

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	errCh := make(chan error)
	_ = informer.SetWatchErrorHandler(func(_ *Reflector, err error) {
		errCh <- err
	})

	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "Access Denied") {
			t.Errorf("Expected 'Access Denied' error. Actual: %v", err)
		}
	case <-time.After(time.Second):
		t.Errorf("Timeout waiting for error handler call")
	}
}

// TestSharedInformerStartRace 是回归测试，确保以下两者之间不存在竞态：
// 覆盖 Run 与 SetWatchErrorHandler、Run 与 SetTransform 的组合。
func TestSharedInformerStartRace(t *testing.T) {
	source := newFakeControllerSource(t)
	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			// 设置占位函数，仅用于检测竞态。
			informer.SetTransform(func(i interface{}) (interface{}, error) {
				return i, nil
			})
			informer.SetWatchErrorHandler(func(r *Reflector, err error) {
			})
		}
	}()

	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()
}

// TestSharedInformerTransformer 验证 SetTransform 改写后的事件会以变换后的形式送达监听器。
func TestSharedInformerTransformer(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", UID: "pod1", ResourceVersion: "1"}})
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: "pod2", ResourceVersion: "2"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	informer.SetTransform(func(obj interface{}) (interface{}, error) {
		if pod, ok := obj.(*v1.Pod); ok {
			name := pod.GetName()

			if upper := strings.ToUpper(name); upper != name {
				pod.SetName(upper)
				return pod, nil
			}
		}
		return obj, nil
	})

	listenerTransformer := newTestListener("listenerTransformer", 0, "POD1", "POD2")
	informer.AddEventHandler(listenerTransformer)

	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	if !listenerTransformer.ok() {
		t.Errorf("%s: expected %v, got %v", listenerTransformer.name, listenerTransformer.expectedItemNames, listenerTransformer.receivedItemNames)
	}
}

// TestSharedInformerRemoveHandler 验证移除句柄后监听器计数正确递减。
func TestSharedInformerRemoveHandler(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second)

	handler1 := &ResourceEventHandlerFuncs{}
	handle1, err := informer.AddEventHandler(handler1)
	if err != nil {
		t.Errorf("informer did not add handler1: %s", err)
		return
	}
	handler2 := &ResourceEventHandlerFuncs{}
	handle2, err := informer.AddEventHandler(handler2)
	if err != nil {
		t.Errorf("informer did not add handler2: %s", err)
		return
	}

	if eventHandlerCount(informer) != 2 {
		t.Errorf("informer has %d registered handler, instead of 2", eventHandlerCount(informer))
	}

	if err := informer.RemoveEventHandler(handle2); err != nil {
		t.Errorf("removing of second pointer handler failed: %s", err)
	}
	if eventHandlerCount(informer) != 1 {
		t.Errorf("after removing handler informer has %d registered handler(s), instead of 1", eventHandlerCount(informer))
	}

	if err := informer.RemoveEventHandler(handle1); err != nil {
		t.Errorf("removing of first pointer handler failed: %s", err)
	}
	if eventHandlerCount(informer) != 0 {
		t.Errorf("informer still has registered handlers after removing both handlers")
	}
}

// TestSharedInformerRemoveForeignHandler 验证对另一个 informer 使用本 informer 的
// 句柄移除不会产生副作用（句柄按 informer 隔离）。
func TestSharedInformerRemoveForeignHandler(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	source2 := newFakeControllerSource(t)
	source2.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer2 := NewSharedInformer(source2, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	handler1 := &ResourceEventHandlerFuncs{}
	handle1, err := informer.AddEventHandler(handler1)
	if err != nil {
		t.Errorf("informer did not add handler1: %s", err)
		return
	}
	handler2 := &ResourceEventHandlerFuncs{}
	handle2, err := informer.AddEventHandler(handler2)
	if err != nil {
		t.Errorf("informer did not add handler2: %s", err)
		return
	}

	if eventHandlerCount(informer) != 2 {
		t.Errorf("informer has %d registered handler, instead of 2", eventHandlerCount(informer))
	}
	if eventHandlerCount(informer2) != 0 {
		t.Errorf("informer2 has %d registered handler, instead of 0", eventHandlerCount(informer2))
	}

	// 移除另一个 informer 上的 handler。
	if isRegistered(informer2, handle1) {
		t.Errorf("handle1 registered for informer2")
	}
	if isRegistered(informer2, handle2) {
		t.Errorf("handle2 registered for informer2")
	}
	if err := informer2.RemoveEventHandler(handle1); err != nil {
		t.Errorf("removing of second pointer handler failed: %s", err)
	}
	if eventHandlerCount(informer) != 2 {
		t.Errorf("informer has %d registered handler, instead of 2", eventHandlerCount(informer))
	}
	if eventHandlerCount(informer2) != 0 {
		t.Errorf("informer2 has %d registered handler, instead of 0", eventHandlerCount(informer2))
	}
	if !isRegistered(informer, handle1) {
		t.Errorf("handle1 not registered anymore for informer")
	}
	if !isRegistered(informer, handle2) {
		t.Errorf("handle2 not registered anymore for informer")
	}

	if eventHandlerCount(informer) != 2 {
		t.Errorf("informer has %d registered handler, instead of 2", eventHandlerCount(informer))
	}
	if eventHandlerCount(informer2) != 0 {
		t.Errorf("informer2 has %d registered handler, instead of 0", eventHandlerCount(informer2))
	}
	if !isRegistered(informer, handle1) {
		t.Errorf("handle1 not registered anymore for informer")
	}
	if !isRegistered(informer, handle2) {
		t.Errorf("handle2 not registered anymore for informer")
	}

	if err := informer.RemoveEventHandler(handle2); err != nil {
		t.Errorf("removing of second pointer handler failed: %s", err)
	}
	if eventHandlerCount(informer) != 1 {
		t.Errorf("after removing handler informer has %d registered handler(s), instead of 1", eventHandlerCount(informer))
	}

	if err := informer.RemoveEventHandler(handle1); err != nil {
		t.Errorf("removing of first pointer handler failed: %s", err)
	}
	if eventHandlerCount(informer) != 0 {
		t.Errorf("informer still has registered handlers after removing both handlers")
	}
}

// TestSharedInformerMultipleRegistration 验证同一 handler 注册多次会得到
// 独立句柄，移除其一不影响另一个（每次注册都是独立监听器）。
// TestSharedInformerMultipleRegistration 验证同一 handler 注册多次会得到
// 独立句柄，移除其一不影响另一个（每次注册都是独立监听器）。
func TestSharedInformerMultipleRegistration(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	handler1 := &ResourceEventHandlerFuncs{}
	reg1, err := informer.AddEventHandler(handler1)
	if err != nil {
		t.Errorf("informer did not add handler for the first time: %s", err)
		return
	}

	if !isRegistered(informer, reg1) {
		t.Errorf("handle1 is not active after successful registration")
		return
	}

	reg2, err := informer.AddEventHandler(handler1)
	if err != nil {
		t.Errorf("informer did not add handler for the second: %s", err)
		return
	}

	if !isRegistered(informer, reg2) {
		t.Errorf("handle2 is not active after successful registration")
		return
	}

	if eventHandlerCount(informer) != 2 {
		t.Errorf("informer has %d registered handler(s), instead of 2", eventHandlerCount(informer))
	}

	if err := informer.RemoveEventHandler(reg1); err != nil {
		t.Errorf("removing of duplicate handler registration failed: %s", err)
	}

	if isRegistered(informer, reg1) {
		t.Errorf("handle1 is still active after successful remove")
		return
	}
	if !isRegistered(informer, reg2) {
		t.Errorf("handle2 is not active after removing handle1")
		return
	}

	if eventHandlerCount(informer) != 1 {
		if eventHandlerCount(informer) == 0 {
			t.Errorf("informer has no registered handler anymore after removal of duplicate registrations")
		} else {
			t.Errorf("informer has unexpected number (%d) of handlers after removal of duplicate handler registration", eventHandlerCount(informer))
		}
	}

	if err := informer.RemoveEventHandler(reg2); err != nil {
		t.Errorf("removing of second handler registration failed: %s", err)
	}

	if isRegistered(informer, reg2) {
		t.Errorf("handle2 is still active after successful remove")
		return
	}

	if eventHandlerCount(informer) != 0 {
		t.Errorf("informer has unexpected number (%d) of handlers after removal of second handler registrations", eventHandlerCount(informer))
	}
}

// TestRemovingRemovedSharedInformer 验证重复移除同一句柄是幂等的、不报错。
func TestRemovingRemovedSharedInformer(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	handler := &ResourceEventHandlerFuncs{}
	reg, err := informer.AddEventHandler(handler)

	if err != nil {
		t.Errorf("informer did not add handler for the first time: %s", err)
		return
	}
	if err := informer.RemoveEventHandler(reg); err != nil {
		t.Errorf("removing of handler registration failed: %s", err)
		return
	}
	if isRegistered(informer, reg) {
		t.Errorf("handle is still active after successful remove")
		return
	}
	if err := informer.RemoveEventHandler(reg); err != nil {
		t.Errorf("removing of already removed registration yields unexpected error: %s", err)
	}
	if isRegistered(informer, reg) {
		t.Errorf("handle is still active after second remove")
		return
	}
}

// 展示多个并发 goroutine 可以操作共享 informer 的 listener 而不会出错。这里断言不多，
// 用于发现竞态，应以 -race 运行。
//
// 100 个 worker 随机增删监听器，同时持续产生 10k 个事件，重点在于竞争检测。
func TestSharedInformerHandlerAbuse(t *testing.T) {
	source := newFakeControllerSource(t)
	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)

	ctx, cancel := context.WithCancel(context.Background())
	informerCtx, informerCancel := context.WithCancel(context.Background())
	var informerWg wait.Group
	informerWg.StartWithChannel(informerCtx.Done(), informer.Run)
	defer func() {
		cancel()
		informerWg.Wait()
	}()

	worker := func() {
		// 不断添加和移除 handler，检查是否出现重复事件。
		funcs := ResourceEventHandlerDetailedFuncs{
			AddFunc:    func(obj interface{}, isInInitialList bool) {},
			UpdateFunc: func(oldObj, newObj interface{}) {},
			DeleteFunc: func(obj interface{}) {},
		}
		handles := []ResourceEventHandlerRegistration{}

		for {
			select {
			case <-ctx.Done():
				return
			default:
				switch rand.Intn(2) {
				case 0:
					// 再次注册 handler。
					reg, err := informer.AddEventHandlerWithResyncPeriod(funcs, 1*time.Second)
					if err != nil {
						if strings.Contains(err.Error(), "stopped already") {
							// 测试结束。
							return
						}
						t.Errorf("failed to add handler: %v", err)
						return
					}
					handles = append(handles, reg)
				case 1:
					// 随机移除一个 handler。
					if len(handles) == 0 {
						continue
					}

					idx := rand.Intn(len(handles))
					err := informer.RemoveEventHandler(handles[idx])
					if err != nil {
						if strings.Contains(err.Error(), "stopped already") {
							// 测试结束。
							return
						}
						t.Errorf("failed to remove handler: %v", err)
						return
					}
					handles = append(handles[:idx], handles[idx+1:]...)
				}
			}
		}
	}

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			worker()
			wg.Done()
		}()
	}

	objs := []*v1.Pod{}

	// worker 运行期间，随机为 informer 制造事件。
	for i := 0; i < 10000; i++ {
		if len(objs) == 0 {
			// 确保始终有一个对象存在。
			obj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "pod" + strconv.Itoa(i),
			}}
			objs = append(objs, obj)

			// 加入前先深拷贝，因为 Modify 会修改原对象。
			source.Add(obj.DeepCopy())
		}

		switch rand.Intn(3) {
		case 0:
			// 添加对象。
			obj := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "pod" + strconv.Itoa(i),
			}}
			objs = append(objs, obj)
			source.Add(obj.DeepCopy())
		case 1:
			// 更新对象。
			idx := rand.Intn(len(objs))
			source.Modify(objs[idx].DeepCopy())

		case 2:
			// 删除对象。
			idx := rand.Intn(len(objs))
			source.Delete(objs[idx].DeepCopy())
			objs = append(objs[:idx], objs[idx+1:]...)
		}
	}

	// 停止 informer 也会停止 worker。先停 informer 以模拟关闭期间的竞争。
	informerCancel()

	// 等待 worker 结束，因为它们可能抛出错误。
	wg.Wait()
}

// TestStateSharedInformer 验证 Started/Stopped 状态转换：
// 创建后未启动、运行中已启动未停止、关闭 stop 通道后变为已停止但仍记录曾经启动。
func TestStateSharedInformer(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	listener := newTestListener("listener", 0, "pod1")
	informer.AddEventHandlerWithResyncPeriod(listener, listener.resyncPeriod)

	if isStarted(informer) {
		t.Errorf("informer already started after creation")
		return
	}
	if informer.IsStopped() {
		t.Errorf("informer already stopped after creation")
		return
	}
	var wg wait.Group
	stop := make(chan struct{})
	wg.StartWithChannel(stop, informer.Run)
	defer wg.Wait()
	if !listener.ok() {
		t.Errorf("informer did not report initial objects")
		close(stop)
		return
	}

	if !isStarted(informer) {
		t.Errorf("informer does not report to be started although handling events")
		close(stop)
		return
	}
	if informer.IsStopped() {
		t.Errorf("informer reports to be stopped although stop channel not closed")
		close(stop)
		return
	}

	close(stop)
	fmt.Println("sleeping")
	time.Sleep(1 * time.Second)

	if !informer.IsStopped() {
		t.Errorf("informer reports not to be stopped although stop channel closed")
		return
	}
	if !isStarted(informer) {
		t.Errorf("informer reports not to be started after it has been started and stopped")
		return
	}
}

// TestAddOnStoppedSharedInformer 验证 informer 停止后 AddEventHandler 返回明确错误。
func TestAddOnStoppedSharedInformer(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	listener := newTestListener("listener", 0, "pod1")
	stop := make(chan struct{})
	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer wg.Wait()
	close(stop)

	err := wait.PollImmediate(100*time.Millisecond, 2*time.Second, func() (bool, error) {
		if informer.IsStopped() {
			return true, nil
		}
		return false, nil
	})

	if err != nil {
		t.Errorf("informer reports not to be stopped although stop channel closed")
		return
	}

	_, err = informer.AddEventHandlerWithResyncPeriod(listener, listener.resyncPeriod)
	if err == nil {
		t.Errorf("stopped informer did not reject add handler")
		return
	}
	if !strings.HasSuffix(err.Error(), "was not added to shared informer because it has stopped already") {
		t.Errorf("adding handler to a stopped informer yields unexpected error: %s", err)
		return
	}
}

// TestRemoveOnStoppedSharedInformer 验证停止后仍可移除既有句柄（清理路径必须可用）。
func TestRemoveOnStoppedSharedInformer(t *testing.T) {
	source := newFakeControllerSource(t)
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second).(*sharedIndexInformer)
	listener := newTestListener("listener", 0, "pod1")
	handle, err := informer.AddEventHandlerWithResyncPeriod(listener, listener.resyncPeriod)
	if err != nil {
		t.Errorf("informer did not add handler: %s", err)
		return
	}
	stop := make(chan struct{})
	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer wg.Wait()
	close(stop)
	fmt.Println("sleeping")
	time.Sleep(1 * time.Second)

	if !informer.IsStopped() {
		t.Errorf("informer reports not to be stopped although stop channel closed")
		return
	}
	err = informer.RemoveEventHandler(handle)
	if err != nil {
		t.Errorf("informer does not remove handler on stopped informer")
		return
	}
}

// TestRemoveWhileActive 验证运行中移除监听器后，后续事件不再送达该监听器。
func TestRemoveWhileActive(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	// 创建共享 informer，每 12 小时 resync 一次。
	informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)

	listener := newTestListener("listener", 0, "pod1")
	handle, _ := informer.AddEventHandler(listener)

	stop := make(chan struct{})
	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	if !listener.ok() {
		t.Errorf("event did not occur")
		return
	}

	informer.RemoveEventHandler(handle)

	if isRegistered(informer, handle) {
		t.Errorf("handle is still active after successful remove")
		return
	}

	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2"}})

	if !listener.ok() {
		t.Errorf("unexpected event occurred")
		return
	}
}

// TestAddWhileActive 验证运行中新增的监听器会先补发当前全量对象完成同步，
// 之后才接收增量事件；HasSynced 在同步前为 false。
func TestAddWhileActive(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	// 创建共享 informer，每 12 小时 resync 一次。
	informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)
	listener1 := newTestListener("originalListener", 0, "pod1")
	listener2 := newTestListener("listener2", 0, "pod1", "pod2")
	handle1, _ := informer.AddEventHandler(listener1)

	if handle1.HasSynced() {
		t.Error("Synced before Run??")
	}

	stop := make(chan struct{})
	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	if !listener1.ok() {
		t.Errorf("events on listener1 did not occur")
		return
	}

	select {
	case <-handle1.HasSyncedChecker().Done():
		if !handle1.HasSynced() {
			t.Error("Not synced after channel said we are synced??")
		}
	case <-time.After(10 * time.Second):
		t.Error("Not synced 10 seconds after Run??")
	}

	listener2.lock.Lock() // ensure we observe it before it has synced
	handle2, _ := informer.AddEventHandler(listener2)
	if handle2.HasSynced() {
		t.Error("Synced before processing anything?")
	}
	listener2.lock.Unlock() // permit it to proceed and sync

	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2"}})

	if !listener2.ok() {
		t.Errorf("event on listener2 did not occur")
		return
	}

	if !handle2.HasSynced() {
		t.Error("Not synced even after processing?")
	}

	if !isRegistered(informer, handle1) {
		t.Errorf("handle1 is not active")
		return
	}
	if !isRegistered(informer, handle2) {
		t.Errorf("handle2 is not active")
		return
	}

	listener1.expectedItemNames = listener2.expectedItemNames
	if !listener1.ok() {
		t.Errorf("events on listener1 did not occur")
		return
	}
}

// TestShutdown 依赖 main_test.go 中的 goleak.VerifyTestMain 来验证：
// 各场景下所有 goroutine 确实都已停止。
//
// 覆盖“先加 handler 再 Run”“Run 后再加 handler”“只加 handler 不 Run”三种生命周期。
func TestShutdown(t *testing.T) {
	t.Run("no-context", func(t *testing.T) {
		source := newFakeControllerSource(t)

		informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second)
		handler, err := informer.AddEventHandler(ResourceEventHandlerFuncs{
			AddFunc: func(_ any) {},
		})
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, informer.RemoveEventHandler(handler))
		}()

		var wg wait.Group
		stop := make(chan struct{})
		wg.StartWithChannel(stop, informer.Run)
		defer func() {
			close(stop)
			wg.Wait()
		}()

		require.Eventually(t, informer.HasSynced, time.Minute, time.Millisecond, "informer has synced")
	})

	t.Run("no-context-later", func(t *testing.T) {
		source := newFakeControllerSource(t)
		informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second)

		var wg wait.Group
		stop := make(chan struct{})
		wg.StartWithChannel(stop, informer.Run)
		defer func() {
			close(stop)
			wg.Wait()
		}()

		require.Eventually(t, informer.HasSynced, time.Minute, time.Millisecond, "informer has synced")

		handler, err := informer.AddEventHandler(ResourceEventHandlerFuncs{
			AddFunc: func(_ any) {},
		})
		require.NoError(t, err)
		assert.NoError(t, informer.RemoveEventHandler(handler))
	})

	t.Run("no-run", func(t *testing.T) {
		source := newFakeControllerSource(t)
		informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second)
		_, err := informer.AddEventHandler(ResourceEventHandlerFuncs{
			AddFunc: func(_ any) {},
		})
		require.NoError(t, err)

		// 此时 informer 与 handler 都没有 goroutine 在运行，是否再停止或移除都无关紧要。
	})
}

// TestEventPanics 验证处理器 panic 会被 runtime 捕获并记录，而不会拖垮 informer；
// 每个 panic 后处理器会退避，且不同 handler 的日志带各自的名字。
func TestEventPanics(t *testing.T) {
	// timeInUTC := time.Date(2009, 12, 1, 13, 30, 40, 42000, time.UTC)
	// timeString := "1201 13:30:40.000042"
	// 由 init 初始化。
	var (
		buffer threadSafeBuffer
		logger klog.Logger
		source *fcache.FakeControllerSource
	)

	init := func(t *testing.T) {
		// 恢复状态对顺序非常敏感：测试启动的所有 goroutine 必须结束，且必须有竞态检测器
		// 可见的完成检查。其他测试也必须遵守！
		t.Cleanup(klog.CaptureState().Restore) //nolint:logcheck // CaptureState shouldn't be used in packages with contextual logging, but here it is okay.
		buffer.buffer.Reset()
		logger = textlogger.NewLogger(textlogger.NewConfig(
			// textlogger.FixedTime(timeInUTC),
			textlogger.Output(&buffer),
		))
		oldReallyCrash := utilruntime.ReallyCrash
		utilruntime.ReallyCrash = false
		t.Cleanup(func() { utilruntime.ReallyCrash = oldReallyCrash })

		source = newFakeControllerSource(t)
		source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})
	}

	newHandler := func(ctx context.Context) ResourceEventHandlerFuncs {
		logger := klog.FromContext(ctx)
		return ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				logger.Info("Add func will panic now", "pod", klog.KObj(obj.(*v1.Pod)))
				panic("fake panic")
			},
		}
	}
	_, _, panicLine, _ := runtime.Caller(0)
	panicLine -= 4
	expectedLog := func(name string) string {
		if name == "" {
			return fmt.Sprintf(`shared_informer_test.go:%d] "Observed a panic" panic="fake panic"`, panicLine)
		}
		return fmt.Sprintf(`shared_informer_test.go:%d] "Observed a panic" logger=%q panic="fake panic"`, panicLine, name)
	}
	handler := newHandler(context.Background())

	t.Run("simple", func(t *testing.T) {
		init(t)
		klog.SetLogger(logger)
		stop := make(chan struct{})
		informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second)
		handle, err := informer.AddEventHandler(handler)
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, informer.RemoveEventHandler(handle))
		}()
		var wg wait.Group
		wg.StartWithChannel(stop, informer.Run)
		defer func() {
			close(stop)
			assert.Eventually(t, informer.IsStopped, time.Minute, time.Millisecond, "informer has stopped")
			wg.Wait() // For race detector...
		}()
		require.Eventually(t, informer.HasSynced, time.Minute, time.Millisecond, "informer has synced")

		// 这会超时（https://github.com/kubernetes/kubernetes/issues/129024）：回调 panic 时 handler 永远不会同步。
		//    require.Eventually(t, handle.HasSynced, time.Minute, time.Millisecond, "handler has synced")
		//
		// 改为等待非空缓冲区，这意味着必须让缓冲区线程安全，否则本不需要。
		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			assert.Contains(t, buffer.String(), expectedLog(""))
		}, time.Minute, time.Millisecond, "handler has panicked")
	})

	t.Run("many", func(t *testing.T) {
		init(t)
		// init 中已创建一个 Pod，这里再加一些。
		numPods := 5
		for i := 1; i < numPods; i++ {
			source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod%d", i+1)}})
		}
		_, ctx := ktesting.NewTestContext(t)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		informer := NewSharedInformer(source, &v1.Pod{}, 1*time.Second)
		name1 := "fake-event-handler-1"
		logger1 := klog.LoggerWithName(logger, name1)
		ctx1 := klog.NewContext(ctx, logger1)
		handle1, err := informer.AddEventHandlerWithOptions(newHandler(ctx1), HandlerOptions{Logger: &logger1})
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, informer.RemoveEventHandler(handle1))
		}()
		name2 := "fake-event-handler-2"
		logger2 := klog.LoggerWithName(logger, name2)
		ctx2 := klog.NewContext(ctx, logger2)
		handle2, err := informer.AddEventHandlerWithOptions(newHandler(ctx2), HandlerOptions{Logger: &logger2})
		require.NoError(t, err)
		defer func() {
			assert.NoError(t, informer.RemoveEventHandler(handle2))
		}()

		start := time.Now()
		var wg wait.Group
		informerName := "informer"
		informerLogger := klog.LoggerWithName(logger, informerName)
		informerCtx := klog.NewContext(ctx, informerLogger)
		wg.StartWithContext(informerCtx, informer.RunWithContext)
		defer func() {
			cancel()
			assert.Eventually(t, informer.IsStopped, time.Minute, time.Millisecond, "informer has stopped")
			wg.Wait() // For race detector...
		}()
		require.Eventually(t, informer.HasSynced, time.Minute, time.Millisecond, "informer has synced")

		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			output := buffer.String()
			expected := expectedLog(name1)
			if !assert.Equal(t, numPods, numOccurrences(output, expected), "Log output should have the right number of panics for %q (search string: %q), got instead:\n%s", name1, expected, output) {
				return
			}
			expected = expectedLog(name2)
			assert.Equal(t, numPods, numOccurrences(output, expected), "Log output should have the right number of panics for %q (search string %q, got instead:\n%s", name2, expected, output)
		}, 30*time.Second, time.Millisecond, "handler has panicked")

		// 除最后一个 Pod 事件（此时输入 channel 会关闭）外，两个 handler 应在每次 panic 后各睡一秒。
		assert.GreaterOrEqual(t, time.Since(start), time.Duration(numPods-1)*time.Second, "Delay in processorListener.run")
	})
}

// numOccurrences 统计 needle 在 hay 中出现的次数。
func numOccurrences(hay, needle string) int {
	count := 0
	for {
		index := strings.Index(hay, needle)
		if index < 0 {
			return count
		}
		count++
		hay = hay[index+len(needle):]
	}
}

// TestRemoveEventHandler_AsynchronousPanic 验证异步处理器运行期间调用
// RemoveEventHandler 会立即返回，而不会等待正在阻塞的处理器。
func TestRemoveEventHandler_AsynchronousPanic(t *testing.T) {
	source := newFakeControllerSource(t)
	informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)

	stop := make(chan struct{})
	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	scheduledPods := make(chan *v1.Pod, 1)
	blockChan := make(chan struct{})
	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})
	var panicVal any
	var panicked bool

	handler := ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			close(handlerStarted)
			<-blockChan
			defer close(handlerFinished)
			defer func() {
				if r := recover(); r != nil {
					panicVal = r
					panicked = true
				}
			}()
			scheduledPods <- obj.(*v1.Pod)
		},
	}

	handle, err := informer.AddEventHandler(handler)
	if err != nil {
		t.Fatalf("Failed to add event handler: %v", err)
	}

	// 触发事件。
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	// 等待 handler 启动并阻塞。
	<-handlerStarted

	// 现在移除 handler，应立即返回。
	err = informer.RemoveEventHandler(handle)
	if err != nil {
		t.Fatalf("Failed to remove event handler: %v", err)
	}

	// 关闭 handler 写入的 channel。
	close(scheduledPods)

	// 解除 handler 的阻塞。
	close(blockChan)

	// 等待 handler 结束。
	<-handlerFinished

	if !panicked {
		t.Fatalf("Expected handler to panic (send on closed channel), but it did not")
	}
	t.Logf("Caught expected panic: %v", panicVal)
}

// TestRemoveEventHandler_SynchronousShutdown 验证 ShutDownEventHandler 会等待
// 处理器完全退出，之后再关闭其依赖的资源才是安全的。
func TestRemoveEventHandler_SynchronousShutdown(t *testing.T) {
	source := newFakeControllerSource(t)
	informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)

	stop := make(chan struct{})
	var wg wait.Group
	wg.StartWithChannel(stop, informer.Run)
	defer func() {
		close(stop)
		wg.Wait()
	}()

	scheduledPods := make(chan *v1.Pod, 1)
	blockChan := make(chan struct{})
	handlerStarted := make(chan struct{})
	handlerFinished := make(chan struct{})
	var panicVal any
	var panicked bool

	handler := ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			close(handlerStarted)
			<-blockChan
			defer close(handlerFinished)
			defer func() {
				if r := recover(); r != nil {
					panicVal = r
					panicked = true
				}
			}()
			scheduledPods <- obj.(*v1.Pod)
		},
	}

	handle, err := informer.AddEventHandler(handler)
	if err != nil {
		t.Fatalf("Failed to add event handler: %v", err)
	}

	// 触发事件。
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1"}})

	// 等待 handler 启动并阻塞。
	<-handlerStarted

	// 解除 handler 的阻塞，使其运行并退出。
	close(blockChan)

	// 现在移除 handler 并等待其完全停止。
	err = ShutDownEventHandler(informer, handle)
	if err != nil {
		t.Fatalf("Failed to shutdown event handler: %v", err)
	}

	// 现在可以安全地关闭 channel。
	close(scheduledPods)

	// 等待 handler 函数结束（应已完成）。
	<-handlerFinished

	if panicked {
		t.Fatalf("Handler panicked unexpectedly: %v", panicVal)
	}
}

// TestShutDownEventHandler_Lifecycles 覆盖 ShutDownEventHandler 在三种生命周期下的行为：
// informer 从未启动、启动后新增再关闭、新增后启动再关闭，均应正常返回。
func TestShutDownEventHandler_Lifecycles(t *testing.T) {
	t.Run("InformerNeverStarted", func(t *testing.T) {
		source := newFakeControllerSource(t)
		informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)

		handler := ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {},
		}
		handle, err := informer.AddEventHandler(handler)
		if err != nil {
			t.Fatalf("Failed to add event handler: %v", err)
		}

		// informer 从未启动，Shutdown 应立即返回。
		err = ShutDownEventHandler(informer, handle)
		if err != nil {
			t.Fatalf("Failed to shutdown event handler: %v", err)
		}
	})

	t.Run("StartedThenAddedThenShutDown", func(t *testing.T) {
		source := newFakeControllerSource(t)
		informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)

		stop := make(chan struct{})
		var wg wait.Group
		wg.StartWithChannel(stop, informer.Run)
		defer func() {
			close(stop)
			wg.Wait()
		}()

		// 确定性地等待 informer 同步（证明它已启动）。
		syncCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if !WaitFor(syncCtx, "informer sync", informer.HasSyncedChecker()) {
			t.Fatalf("Informer did not sync")
		}

		handler := ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {},
		}
		handle, err := informer.AddEventHandler(handler)
		if err != nil {
			t.Fatalf("Failed to add event handler: %v", err)
		}

		err = ShutDownEventHandler(informer, handle)
		if err != nil {
			t.Fatalf("Failed to shutdown event handler: %v", err)
		}
	})

	t.Run("AddedThenStartedThenShutDown", func(t *testing.T) {
		source := newFakeControllerSource(t)
		informer := NewSharedInformer(source, &v1.Pod{}, 0).(*sharedIndexInformer)

		handler := ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {},
		}
		handle, err := informer.AddEventHandler(handler)
		if err != nil {
			t.Fatalf("Failed to add event handler: %v", err)
		}

		stop := make(chan struct{})
		var wg wait.Group
		wg.StartWithChannel(stop, informer.Run)
		defer func() {
			close(stop)
			wg.Wait()
		}()

		// 确定性地等待 informer 同步（证明它已启动）。
		syncCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if !WaitFor(syncCtx, "informer sync", informer.HasSyncedChecker()) {
			t.Fatalf("Informer did not sync")
		}

		err = ShutDownEventHandler(informer, handle)
		if err != nil {
			t.Fatalf("Failed to shutdown event handler: %v", err)
		}
	})
}
