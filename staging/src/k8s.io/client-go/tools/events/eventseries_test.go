/*
Copyright 2019 The Kubernetes Authors.

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

package events

import (
	"context"
	"strconv"
	"testing"
	"time"

	"os"
	"strings"

	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	fake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/klog/v2/ktesting"
)

// testEventSeriesSink 是可注入回调的 fake EventSink，让测试能观察 Create/Update/Patch
// 被调用的时机与内容；未设置回调时原样返回事件。
type testEventSeriesSink struct {
	OnCreate func(e *eventsv1.Event) (*eventsv1.Event, error)
	OnUpdate func(e *eventsv1.Event) (*eventsv1.Event, error)
	OnPatch  func(e *eventsv1.Event, p []byte) (*eventsv1.Event, error)
}

// Create records the event for testing.
func (t *testEventSeriesSink) Create(ctx context.Context, e *eventsv1.Event) (*eventsv1.Event, error) {
	if t.OnCreate != nil {
		return t.OnCreate(e)
	}
	return e, nil
}

// Update records the event for testing.
func (t *testEventSeriesSink) Update(ctx context.Context, e *eventsv1.Event) (*eventsv1.Event, error) {
	if t.OnUpdate != nil {
		return t.OnUpdate(e)
	}
	return e, nil
}

// Patch records the event for testing.
func (t *testEventSeriesSink) Patch(ctx context.Context, e *eventsv1.Event, p []byte) (*eventsv1.Event, error) {
	if t.OnPatch != nil {
		return t.OnPatch(e, p)
	}
	return e, nil
}

// TestEventSeriesf 验证事件聚合：相同（regarding/related/type/reason/action）的事件
// 会走 Patch 并递增 Series.Count，而 action 不同的事件则作为新事件 Create。
func TestEventSeriesf(t *testing.T) {
	hostname, _ := os.Hostname()

	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "baz",
			UID:       "bar",
		},
	}

	regarding, err := ref.GetPartialReference(scheme.Scheme, testPod, ".spec.containers[1]")
	if err != nil {
		t.Fatal(err)
	}

	related, err := ref.GetPartialReference(scheme.Scheme, testPod, ".spec.containers[0]")
	if err != nil {
		t.Fatal(err)
	}

	expectedEvent := &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "baz",
		},
		EventTime:           metav1.MicroTime{Time: time.Now()},
		ReportingController: "eventTest",
		ReportingInstance:   "eventTest-" + hostname,
		Action:              "started",
		Reason:              "test",
		Regarding:           *regarding,
		Related:             related,
		Note:                "some verbose message: 1",
		Type:                v1.EventTypeNormal,
	}

	// isomorphic 与 nonIsomorphic 分别代表可聚合与不可聚合的事件：
	// 仅 action 不同即视为不同事件，不应被合并。
	isomorphicEvent := expectedEvent.DeepCopy()

	nonIsomorphicEvent := expectedEvent.DeepCopy()
	nonIsomorphicEvent.Action = "stopped"

	// 期望的聚合事件带 Series.Count=2（首次创建 + 一次增量）。
	expectedEvent.Series = &eventsv1.EventSeries{Count: 2}
	table := []struct {
		regarding    k8sruntime.Object
		related      k8sruntime.Object
		actual       *eventsv1.Event
		elements     []interface{}
		expect       *eventsv1.Event
		expectUpdate bool
	}{
		{
			regarding:    regarding,
			related:      related,
			actual:       isomorphicEvent,
			elements:     []interface{}{1},
			expect:       expectedEvent,
			expectUpdate: true,
		},
		{
			regarding:    regarding,
			related:      related,
			actual:       nonIsomorphicEvent,
			elements:     []interface{}{1},
			expect:       nonIsomorphicEvent,
			expectUpdate: false,
		},
	}

	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 用 channel 接收 sink 回调，测试依次断言“Patch 或 Create”路径。
	createEvent := make(chan *eventsv1.Event)
	updateEvent := make(chan *eventsv1.Event)
	patchEvent := make(chan *eventsv1.Event)

	testEvents := testEventSeriesSink{
		OnCreate: func(event *eventsv1.Event) (*eventsv1.Event, error) {
			createEvent <- event
			return event, nil
		},
		OnUpdate: func(event *eventsv1.Event) (*eventsv1.Event, error) {
			updateEvent <- event
			return event, nil
		},
		OnPatch: func(event *eventsv1.Event, patch []byte) (*eventsv1.Event, error) {
			// event we receive is already patched, usually the sink uses it only to retrieve the name and namespace, here
			// we'll use it directly
			patchEvent <- event
			return event, nil
		},
	}
	eventBroadcaster := newBroadcaster(&testEvents, 0, map[eventKey]*eventsv1.Event{})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, "eventTest")
	broadcaster := eventBroadcaster.(*eventBroadcasterImpl)
	// Don't call StartRecordingToSink, as we don't need neither refreshing event
	// series nor finishing them in this tests and additional events updated would
	// race with our expected ones.
	err = broadcaster.startRecordingEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 先发一条事件填充聚合缓存，之后才能验证同键事件的 Patch 路径。
	recorder.Eventf(regarding, related, isomorphicEvent.Type, isomorphicEvent.Reason, isomorphicEvent.Action, isomorphicEvent.Note, []interface{}{1})
	// read from the chan as this was needed only to populate the cache
	<-createEvent
	for index, item := range table {
		actual := item.actual
		recorder.Eventf(item.regarding, item.related, actual.Type, actual.Reason, actual.Action, actual.Note, item.elements)
		// validate event
		if item.expectUpdate {
			actualEvent := <-patchEvent
			t.Logf("%v - validating event affected by patch request", index)
			validateEvent(strconv.Itoa(index), true, actualEvent, item.expect, t)
		} else {
			actualEvent := <-createEvent
			t.Logf("%v - validating event affected by a create request", index)
			validateEvent(strconv.Itoa(index), false, actualEvent, item.expect, t)
		}
	}
}

// TestEventSeriesWithEventSinkImplRace verifies that when Events are emitted to
// an EventSink consecutively there is no data race.  This test is meant to be
// run with the `-race` option.
//
// 连续发出两条相同事件，最终应只留下一条带 Series 的记录；配合 -race 检测并发问题。
func TestEventSeriesWithEventSinkImplRace(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()

	eventSink := &EventSinkImpl{Interface: kubeClient.EventsV1()}
	eventBroadcaster := NewBroadcaster(eventSink)

	stopCh := make(chan struct{})
	eventBroadcaster.StartRecordingToSink(stopCh)

	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, "test")

	recorder.Eventf(&v1.ObjectReference{}, nil, v1.EventTypeNormal, "reason", "action", "", "")
	recorder.Eventf(&v1.ObjectReference{}, nil, v1.EventTypeNormal, "reason", "action", "", "")

	// PollImmediate 轮询等待聚合结果：一条事件、且 Series 非空。
	err := wait.PollImmediate(100*time.Millisecond, 5*time.Second, func() (done bool, err error) {
		events, err := kubeClient.EventsV1().Events(metav1.NamespaceDefault).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return false, err
		}

		if len(events.Items) != 1 {
			return false, nil
		}

		if events.Items[0].Series == nil {
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		t.Fatal("expected that 2 identical Eventf calls would result in the creation of an Event with a Serie")
	}
}

// validateEvent 校验事件是否满足预期：时间戳必须设置；
// 期望聚合时 Series 计数一致且名称带前缀，否则 Series 必须为 nil。
func validateEvent(messagePrefix string, expectedUpdate bool, actualEvent *eventsv1.Event, expectedEvent *eventsv1.Event, t *testing.T) {
	recvEvent := *actualEvent

	// Just check that the timestamp was set.
	if recvEvent.EventTime.IsZero() {
		t.Errorf("%v - timestamp wasn't set: %#v", messagePrefix, recvEvent)
	}

	if expectedUpdate {
		if recvEvent.Series == nil {
			t.Errorf("%v - Series was nil but expected: %#v", messagePrefix, recvEvent.Series)

		} else {
			if recvEvent.Series.Count != expectedEvent.Series.Count {
				t.Errorf("%v - Series mismatch actual was: %#v but expected: %#v", messagePrefix, recvEvent.Series, expectedEvent.Series)
			}
		}

		// Check that name has the right prefix.
		if n, en := recvEvent.Name, expectedEvent.Name; !strings.HasPrefix(n, en) {
			t.Errorf("%v - Name '%v' does not contain prefix '%v'", messagePrefix, n, en)
		}
	} else {
		if recvEvent.Series != nil {
			t.Errorf("%v - series was expected to be nil but was: %#v", messagePrefix, recvEvent.Series)
		}
	}

}

// TestFinishSeries 验证 finishSeries 只处理已过期的聚合事件：
// 缓存中未过期的事件保留，过期事件被 Patch 收尾并从缓存移除。
func TestFinishSeries(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	hostname, _ := os.Hostname()
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "baz",
			UID:       "bar",
		},
	}
	regarding, err := ref.GetPartialReference(scheme.Scheme, testPod, ".spec.containers[1]")
	if err != nil {
		t.Fatal(err)
	}
	related, err := ref.GetPartialReference(scheme.Scheme, testPod, ".spec.containers[0]")
	if err != nil {
		t.Fatal(err)
	}
	LastObservedTime := metav1.MicroTime{Time: time.Now().Add(-9 * time.Minute)}

	createEvent := make(chan *eventsv1.Event, 10)
	updateEvent := make(chan *eventsv1.Event, 10)
	patchEvent := make(chan *eventsv1.Event, 10)
	testEvents := testEventSeriesSink{
		OnCreate: func(event *eventsv1.Event) (*eventsv1.Event, error) {
			createEvent <- event
			return event, nil
		},
		OnUpdate: func(event *eventsv1.Event) (*eventsv1.Event, error) {
			updateEvent <- event
			return event, nil
		},
		OnPatch: func(event *eventsv1.Event, patch []byte) (*eventsv1.Event, error) {
			// event we receive is already patched, usually the sink uses it
			// only to retrieve the name and namespace, here we'll use it directly
			patchEvent <- event
			return event, nil
		},
	}
	cache := map[eventKey]*eventsv1.Event{}
	eventBroadcaster := newBroadcaster(&testEvents, 0, cache).(*eventBroadcasterImpl)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, "k8s.io/kube-foo").(*recorderImplLogger)
	cachedEvent := recorder.makeEvent(regarding, related, metav1.MicroTime{Time: time.Now()}, nil, v1.EventTypeNormal, "test", "some verbose message: 1", "eventTest", "eventTest-"+hostname, "started")
	nonFinishedEvent := cachedEvent.DeepCopy()
	nonFinishedEvent.ReportingController = "nonFinished-controller"
	cachedEvent.Series = &eventsv1.EventSeries{
		Count:            10,
		LastObservedTime: LastObservedTime,
	}
	cache[getKey(cachedEvent)] = cachedEvent
	cache[getKey(nonFinishedEvent)] = nonFinishedEvent
	eventBroadcaster.finishSeries(ctx)
	select {
	case actualEvent := <-patchEvent:
		t.Logf("validating event affected by patch request")
		eventBroadcaster.mu.Lock()
		defer eventBroadcaster.mu.Unlock()
		if len(cache) != 1 {
			t.Errorf("cache should be empty, but instead got a size of %v", len(cache))
		}
		if !actualEvent.Series.LastObservedTime.Equal(&cachedEvent.Series.LastObservedTime) {
			t.Errorf("series was expected be seen with LastObservedTime %v, but instead got %v ", cachedEvent.Series.LastObservedTime, actualEvent.Series.LastObservedTime)
		}
		// check that we emitted only one event
		if len(patchEvent) != 0 || len(createEvent) != 0 || len(updateEvent) != 0 {
			t.Errorf("exactly one event should be emitted, but got %v", len(patchEvent))
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("timeout after %v", wait.ForeverTestTimeout)
	}
}

// TestRefreshExistingEventSeries 验证周期性刷新：对聚合事件执行 Patch 以更新
// LastObservedTime，同时覆盖 Patch 失败时不破坏缓存的情况。
func TestRefreshExistingEventSeries(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	hostname, _ := os.Hostname()
	testPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "baz",
			UID:       "bar",
		},
	}
	regarding, err := ref.GetPartialReference(scheme.Scheme, testPod, ".spec.containers[1]")
	if err != nil {
		t.Fatal(err)
	}
	related, err := ref.GetPartialReference(scheme.Scheme, testPod, ".spec.containers[0]")
	if err != nil {
		t.Fatal(err)
	}
	LastObservedTime := metav1.MicroTime{Time: time.Now().Add(-9 * time.Minute)}
	createEvent := make(chan *eventsv1.Event, 10)
	updateEvent := make(chan *eventsv1.Event, 10)
	patchEvent := make(chan *eventsv1.Event, 10)

	table := []struct {
		patchFunc func(event *eventsv1.Event, patch []byte) (*eventsv1.Event, error)
	}{
		{
			patchFunc: func(event *eventsv1.Event, patch []byte) (*eventsv1.Event, error) {
				// event we receive is already patched, usually the sink uses it
				//only to retrieve the name and namespace, here we'll use it directly.
				patchEvent <- event
				return event, nil
			},
		},
		{
			patchFunc: func(event *eventsv1.Event, patch []byte) (*eventsv1.Event, error) {
				// we simulate an apiserver error here
				patchEvent <- nil
				return nil, &restclient.RequestConstructionError{}
			},
		},
	}
	for _, item := range table {
		testEvents := testEventSeriesSink{
			OnCreate: func(event *eventsv1.Event) (*eventsv1.Event, error) {
				createEvent <- event
				return event, nil
			},
			OnUpdate: func(event *eventsv1.Event) (*eventsv1.Event, error) {
				updateEvent <- event
				return event, nil
			},
			OnPatch: item.patchFunc,
		}
		cache := map[eventKey]*eventsv1.Event{}
		eventBroadcaster := newBroadcaster(&testEvents, 0, cache).(*eventBroadcasterImpl)
		recorder := eventBroadcaster.NewRecorder(scheme.Scheme, "k8s.io/kube-foo").(*recorderImplLogger)
		cachedEvent := recorder.makeEvent(regarding, related, metav1.MicroTime{Time: time.Now()}, nil, v1.EventTypeNormal, "test", "some verbose message: 1", "eventTest", "eventTest-"+hostname, "started")
		cachedEvent.Series = &eventsv1.EventSeries{
			Count:            10,
			LastObservedTime: LastObservedTime,
		}
		cacheKey := getKey(cachedEvent)
		cache[cacheKey] = cachedEvent

		eventBroadcaster.refreshExistingEventSeries(ctx)
		select {
		case <-patchEvent:
			t.Logf("validating event affected by patch request")
			eventBroadcaster.mu.Lock()
			defer eventBroadcaster.mu.Unlock()
			if len(cache) != 1 {
				t.Errorf("cache should be with same size, but instead got a size of %v", len(cache))
			}
			// check that we emitted only one event
			if len(patchEvent) != 0 || len(createEvent) != 0 || len(updateEvent) != 0 {
				t.Errorf("exactly one event should be emitted, but got %v", len(patchEvent))
			}
			cacheEvent, exists := cache[cacheKey]

			if cacheEvent == nil || !exists {
				t.Errorf("expected event to exist and not being nil, but instead event: %v and exists: %v", cacheEvent, exists)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Fatalf("timeout after %v", wait.ForeverTestTimeout)
		}
	}
}
