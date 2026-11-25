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

package record

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	testclocks "k8s.io/utils/clock/testing"
)

// makeObjectReference 构造测试用的 InvolvedObject 引用，字段值刻意保持稳定，
// 使聚合键与去重键可预测。
func makeObjectReference(kind, name, namespace string) v1.ObjectReference {
	return v1.ObjectReference{
		Kind:       kind,
		Name:       name,
		Namespace:  namespace,
		UID:        "C934D34AFB20242",
		APIVersion: "version",
		FieldPath:  "spec.containers{mycontainer}",
	}
}

// makeEvent 构造一条 Normal 类型的完整事件，时间戳取当前时间。
func makeEvent(reason, message string, involvedObject v1.ObjectReference) v1.Event {
	eventTime := metav1.Now()
	event := v1.Event{
		Reason:         reason,
		Message:        message,
		InvolvedObject: involvedObject,
		Source: v1.EventSource{
			Component: "kubelet",
			Host:      "kublet.node1",
		},
		Count:          1,
		FirstTimestamp: eventTime,
		LastTimestamp:  eventTime,
		Type:           v1.EventTypeNormal,
	}
	return event
}

// makeEvents 生成 num 条完全相同的事件（用于测试计数聚合）。
func makeEvents(num int, template v1.Event) []v1.Event {
	events := []v1.Event{}
	for i := 0; i < num; i++ {
		events = append(events, template)
	}
	return events
}

// makeUniqueEvents 生成 num 条完全不同的事件（对象名/命名空间/原因/消息都不同），
// 用于验证非相似事件不会被聚合。
func makeUniqueEvents(num int) []v1.Event {
	events := []v1.Event{}
	kind := "Pod"
	for i := 0; i < num; i++ {
		reason := strings.Join([]string{"reason", strconv.Itoa(i)}, "-")
		message := strings.Join([]string{"message", strconv.Itoa(i)}, "-")
		name := strings.Join([]string{"pod", strconv.Itoa(i)}, "-")
		namespace := strings.Join([]string{"ns", strconv.Itoa(i)}, "-")
		involvedObject := makeObjectReference(kind, name, namespace)
		events = append(events, makeEvent(reason, message, involvedObject))
	}
	return events
}

// makeSimilarEvents 在模板事件基础上给 message 加唯一前缀，使它们共享聚合键
// （同 reason、同对象）但 message 不同，用于测试相似事件聚合。
func makeSimilarEvents(num int, template v1.Event, messagePrefix string) []v1.Event {
	events := makeEvents(num, template)
	for i := range events {
		events[i].Message = strings.Join([]string{messagePrefix, strconv.Itoa(i), events[i].Message}, "-")
	}
	return events
}

// setCount 返回 count 字段被改写后的副本。
func setCount(event v1.Event, count int) v1.Event {
	event.Count = int32(count)
	return event
}

// validateEvent 校验关联后的事件：时间戳的一/多发性与 Count 一致、名称带期望前缀，
// 其余字段与期望事件相等。
// FirstTimestamp 与 LastTimestamp 相等表示只发生一次；不等则表示发生过
// 压缩聚合（Count > 1）。比较前把时间戳替换为期望值并恢复，因为具体时间取决于
// 测试时钟而非固定值。名称只校验前缀（真实名称带有随机后缀）。
func validateEvent(messagePrefix string, actualEvent *v1.Event, expectedEvent *v1.Event, t *testing.T) (*v1.Event, error) {
	recvEvent := *actualEvent
	expectCompression := expectedEvent.Count > 1
	t.Logf("%v - expectedEvent.Count is %d\n", messagePrefix, expectedEvent.Count)
	// 只需检查时间戳已设置。
	if recvEvent.FirstTimestamp.IsZero() || recvEvent.LastTimestamp.IsZero() {
		t.Errorf("%v - timestamp wasn't set: %#v", messagePrefix, recvEvent)
	}
	actualFirstTimestamp := recvEvent.FirstTimestamp
	actualLastTimestamp := recvEvent.LastTimestamp
	// 中文：根据 Count 推断时间戳应相等还是不相等，二者必须自洽。
	if actualFirstTimestamp.Equal(&actualLastTimestamp) {
		if expectCompression {
			t.Errorf("%v - FirstTimestamp (%q) and LastTimestamp (%q) must be different to indicate event compression happened, but were the same. Actual Event: %#v", messagePrefix, actualFirstTimestamp, actualLastTimestamp, recvEvent)
		}
	} else {
		if expectedEvent.Count == 1 {
			t.Errorf("%v - FirstTimestamp (%q) and LastTimestamp (%q) must be equal to indicate only one occurrence of the event, but were different. Actual Event: %#v", messagePrefix, actualFirstTimestamp, actualLastTimestamp, recvEvent)
		}
	}
	// 比较前先临时清空时间戳，因为具体值对比较无关紧要
	recvEvent.FirstTimestamp = expectedEvent.FirstTimestamp
	recvEvent.LastTimestamp = expectedEvent.LastTimestamp

	recvEvent.ReportingController = expectedEvent.ReportingController

	// 检查 name 带有正确的前缀。
	if n, en := recvEvent.Name, expectedEvent.Name; !strings.HasPrefix(n, en) {
		t.Errorf("%v - Name '%v' does not contain prefix '%v'", messagePrefix, n, en)
	}
	recvEvent.Name = expectedEvent.Name
	if e, a := expectedEvent, &recvEvent; !reflect.DeepEqual(e, a) {
		t.Errorf("%v - diff: %s", messagePrefix, cmp.Diff(e, a))
	}
	recvEvent.FirstTimestamp = actualFirstTimestamp
	recvEvent.LastTimestamp = actualLastTimestamp
	return actualEvent, nil
}

// TestEventAggregatorByReasonFunc 确保仅 event.message 不同的两个事件会被聚合
// 聚合需要同时看两个键——aggKey 决定“哪些事件算同一类”，localKey 用于
// 区分同类事件的具体内容。仅 message 不同时 aggKey 相同但 localKey 不同；reason
// 不同时 aggKey 也不同。
func TestEventAggregatorByReasonFunc(t *testing.T) {
	event1 := makeEvent("end-of-world", "it was fun", makeObjectReference("Pod", "pod1", "other"))
	event2 := makeEvent("end-of-world", "it was awful", makeObjectReference("Pod", "pod1", "other"))
	event3 := makeEvent("nevermind", "it was a bug", makeObjectReference("Pod", "pod1", "other"))

	aggKey1, localKey1 := EventAggregatorByReasonFunc(&event1)
	aggKey2, localKey2 := EventAggregatorByReasonFunc(&event2)
	aggKey3, _ := EventAggregatorByReasonFunc(&event3)

	if aggKey1 != aggKey2 {
		t.Errorf("Expected %v equal %v", aggKey1, aggKey2)
	}
	if localKey1 == localKey2 {
		t.Errorf("Expected %v to not equal %v", aggKey1, aggKey3)
	}
	if aggKey1 == aggKey3 {
		t.Errorf("Expected %v to not equal %v", aggKey1, aggKey3)
	}
}

// TestEventAggregatorByReasonMessageFunc 校验聚合消息的输出是否正确
//
// 中文：聚合消息必须以固定前缀开头，便于用户识别这是合并后的事件。
func TestEventAggregatorByReasonMessageFunc(t *testing.T) {
	expectedPrefix := "(combined from similar events): "
	event1 := makeEvent("end-of-world", "it was fun", makeObjectReference("Pod", "pod1", "other"))
	actual := EventAggregatorByReasonMessageFunc(&event1)
	if !strings.HasPrefix(actual, expectedPrefix) {
		t.Errorf("Expected %v to begin with prefix %v", actual, expectedPrefix)
	}
}

// TestEventCorrelator 校验事件的计数与聚合行为。
//
// 用 SimpleIntervalClock 精确推进测试时钟，覆盖事件关联器的核心规则：
// 相同事件计数、超过聚合上限、突然爆发的刷屏过滤、相似事件聚合、跨容器不聚合、
// 以及超过聚合窗口后不再聚合。
func TestEventCorrelator(t *testing.T) {
	firstEvent := makeEvent("first", "i am first", makeObjectReference("Pod", "my-pod", "my-ns"))
	duplicateEvent := makeEvent("duplicate", "me again", makeObjectReference("Pod", "my-pod", "my-ns"))
	uniqueEvent := makeEvent("unique", "snowflake", makeObjectReference("Pod", "my-pod", "my-ns"))
	similarEvent := makeEvent("similar", "similar message", makeObjectReference("Pod", "my-pod", "my-ns"))
	// 中文：FieldPath 区分同一 Pod 内的不同容器，是聚合键的一部分。
	similarEvent.InvolvedObject.FieldPath = "spec.containers{container1}"
	aggregateEvent := makeEvent(similarEvent.Reason, EventAggregatorByReasonMessageFunc(&similarEvent), similarEvent.InvolvedObject)
	similarButDifferentContainerEvent := similarEvent
	similarButDifferentContainerEvent.InvolvedObject.FieldPath = "spec.containers{container2}"
	scenario := map[string]struct {
		previousEvents  []v1.Event
		newEvent        v1.Event
		expectedEvent   v1.Event
		intervalSeconds int
		expectedSkip    bool
	}{
		"create-a-single-event": {
			previousEvents:  []v1.Event{},
			newEvent:        firstEvent,
			expectedEvent:   setCount(firstEvent, 1),
			intervalSeconds: 5,
		},
		"the-same-event-should-just-count": {
			// 中文：重复事件只增加 Count，不产生新事件。
			previousEvents:  makeEvents(1, duplicateEvent),
			newEvent:        duplicateEvent,
			expectedEvent:   setCount(duplicateEvent, 2),
			intervalSeconds: 5,
		},
		"the-same-event-should-just-count-even-if-more-than-aggregate": {
			// 中文：即使超过聚合上限，重复事件仍只计数；间隔加大到 30s 以避开刷屏判定。
			previousEvents:  makeEvents(defaultAggregateMaxEvents, duplicateEvent),
			newEvent:        duplicateEvent,
			expectedEvent:   setCount(duplicateEvent, defaultAggregateMaxEvents+1),
			intervalSeconds: 30, // larger interval induces aggregation but not spam.
		},
		"the-same-event-is-spam-if-happens-too-frequently": {
			// 中文：短时间内超过 burst 阈值的事件被视为刷屏并被丢弃。
			previousEvents:  makeEvents(defaultSpamBurst+1, duplicateEvent),
			newEvent:        duplicateEvent,
			expectedSkip:    true,
			intervalSeconds: 1,
		},
		"create-many-unique-events": {
			// 中文：大量互不相同的事件不应被合并，新事件从 Count=1 开始。
			previousEvents:  makeUniqueEvents(30),
			newEvent:        uniqueEvent,
			expectedEvent:   setCount(uniqueEvent, 1),
			intervalSeconds: 5,
		},
		"similar-events-should-aggregate-event": {
			// 中文：已有 N-1 条相似事件，再来一条达到上限，产生一条聚合事件。
			previousEvents:  makeSimilarEvents(defaultAggregateMaxEvents-1, similarEvent, similarEvent.Message),
			newEvent:        similarEvent,
			expectedEvent:   setCount(aggregateEvent, 1),
			intervalSeconds: 5,
		},
		"similar-events-many-times-should-count-the-aggregate": {
			// 中文：已产生聚合事件后，后续相似事件只增加该聚合事件的 Count。
			previousEvents:  makeSimilarEvents(defaultAggregateMaxEvents, similarEvent, similarEvent.Message),
			newEvent:        similarEvent,
			expectedEvent:   setCount(aggregateEvent, 2),
			intervalSeconds: 5,
		},
		"events-from-different-containers-do-not-aggregate": {
			// 中文：FieldPath（容器）不同，即使 reason 与对象相同也不聚合。
			previousEvents:  makeEvents(1, similarButDifferentContainerEvent),
			newEvent:        similarEvent,
			expectedEvent:   setCount(similarEvent, 1),
			intervalSeconds: 5,
		},
		"similar-events-whose-interval-is-greater-than-aggregate-interval-do-not-aggregate": {
			// 中文：事件间隔达到聚合窗口上限后，聚合状态过期，不再合并。
			previousEvents:  makeSimilarEvents(defaultAggregateMaxEvents-1, similarEvent, similarEvent.Message),
			newEvent:        similarEvent,
			expectedEvent:   setCount(similarEvent, 1),
			intervalSeconds: defaultAggregateIntervalInSeconds,
		},
	}

	for testScenario, testInput := range scenario {
		eventInterval := time.Duration(testInput.intervalSeconds) * time.Second
		// 中文：固定步长的时钟让“间隔”变得确定，避免依赖真实时间。
		clock := testclocks.SimpleIntervalClock{Time: time.Now(), Duration: eventInterval}
		correlator := NewEventCorrelator(&clock)
		for i := range testInput.previousEvents {
			event := testInput.previousEvents[i]
			now := metav1.NewTime(clock.Now())
			event.FirstTimestamp = now
			event.LastTimestamp = now
			result, err := correlator.EventCorrelate(&event)
			if err != nil {
				t.Errorf("scenario %v: unexpected error playing back prevEvents %v", testScenario, err)
			}
			// 若该事件被跳过，就无需更新状态
			if !result.Skip {
				correlator.UpdateState(result.Event)
			}
		}

		// 把输入更新为当前时钟值
		now := metav1.NewTime(clock.Now())
		testInput.newEvent.FirstTimestamp = now
		testInput.newEvent.LastTimestamp = now
		result, err := correlator.EventCorrelate(&testInput.newEvent)
		if err != nil {
			t.Errorf("scenario %v: unexpected error correlating input event %v", testScenario, err)
		}

		// 校验没有意外地被过滤函数 skip……
		if result.Skip != testInput.expectedSkip {
			t.Errorf("scenario %v: expected skip %v, but got %v", testScenario, testInput.expectedSkip, result.Skip)
			continue
		}

		// 本就想跳过，因此没有事件可供校验
		if testInput.expectedSkip {
			continue
		}

		// 校验事件
		_, err = validateEvent(testScenario, result.Event, &testInput.expectedEvent, t)
		if err != nil {
			t.Errorf("scenario %v: unexpected error validating result %v", testScenario, err)
		}
	}
}

// TestEventSpamFilter 验证 SpamKeyFunc 可定制刷屏判定粒度。
// 默认 key 基于对象引用，因此同一对象的不同 reason 也会被当作刷屏；自定义
// key 加入 reason 后，只有对象与 reason 都相同才判为刷屏。BurstSize=1 且间隔 1s，
// 使第二条事件立即触发阈值。
func TestEventSpamFilter(t *testing.T) {
	spamKeyFuncBasedOnObjectsAndReason := func(e *v1.Event) string {
		return strings.Join([]string{
			e.Source.Component,
			e.Source.Host,
			e.InvolvedObject.Kind,
			e.InvolvedObject.Namespace,
			e.InvolvedObject.Name,
			string(e.InvolvedObject.UID),
			e.InvolvedObject.APIVersion,
			e.Reason,
		},
			"")
	}
	burstSize := 1
	eventInterval := time.Duration(1) * time.Second
	originalEvent := makeEvent("original", "i am first", makeObjectReference("Pod", "my-pod", "my-ns"))
	differentReasonEvent := makeEvent("duplicate", "me again", makeObjectReference("Pod", "my-pod", "my-ns"))
	spamEvent := makeEvent("original", "me again", makeObjectReference("Pod", "my-pod", "my-ns"))
	testCases := map[string]struct {
		newEvent      v1.Event
		expectedEvent v1.Event
		expectedSkip  bool
		spamKeyFunc   EventSpamKeyFunc
	}{
		"event should be reported as spam if object reference is the same for default spam filter": {
			// 中文：默认 key 只看对象，reason 不同仍判刷屏。
			newEvent:     differentReasonEvent,
			expectedSkip: true,
		},
		"event should not be reported as spam if object reference is the same, but reason is different for custom spam filter": {
			// 中文：自定义 key 含 reason，因此不同 reason 不判刷屏。
			newEvent:      differentReasonEvent,
			expectedEvent: differentReasonEvent,
			expectedSkip:  false,
			spamKeyFunc:   spamKeyFuncBasedOnObjectsAndReason,
		},
		"event should  be reported as spam if object reference and reason is the same, but message is different for custom spam filter": {
			// 中文：key 相同（仅 message 不同）仍判刷屏，说明 message 不参与 key。
			newEvent:     spamEvent,
			expectedSkip: true,
			spamKeyFunc:  spamKeyFuncBasedOnObjectsAndReason,
		},
	}

	for testDescription, testInput := range testCases {
		c := testclocks.SimpleIntervalClock{Time: time.Now(), Duration: eventInterval}
		correlator := NewEventCorrelatorWithOptions(CorrelatorOptions{
			Clock:       &c,
			SpamKeyFunc: testInput.spamKeyFunc,
			BurstSize:   burstSize,
		})
		// 发出原始事件
		result, err := correlator.EventCorrelate(&originalEvent)
		if err != nil {
			t.Errorf("scenario %v: unexpected error correlating originalEvent %v", testDescription, err)
		}
		// 若该事件被跳过，就无需更新状态
		if !result.Skip {
			correlator.UpdateState(result.Event)
		}

		result, err = correlator.EventCorrelate(&testInput.newEvent)
		if err != nil {
			t.Errorf("scenario %v: unexpected error correlating input event %v", testDescription, err)
		}

		// 校验没有意外地被过滤函数 skip……
		if result.Skip != testInput.expectedSkip {
			t.Errorf("scenario %v: expected skip %v, but got %v", testDescription, testInput.expectedSkip, result.Skip)
			continue
		}

		// 本就想跳过，因此没有事件可供校验
		if testInput.expectedSkip {
			continue
		}

		// 校验事件
		_, err = validateEvent(testDescription, result.Event, &testInput.expectedEvent, t)
		if err != nil {
			t.Errorf("scenario %v: unexpected error validating result %v", testDescription, err)
		}
	}
}
