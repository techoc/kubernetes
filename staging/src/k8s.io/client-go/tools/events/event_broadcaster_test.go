/*
Copyright 2022 The Kubernetes Authors.

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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/klog/v2/ktesting"
)

// TestRecordEventToSink 验证 recordEvent 在 sink 已存在同名 Event 时如何合并：
// 后写入的 Event 会替换已有对象（fake 客户端按命名空间+名称覆盖），
// 因此最终只保留一条记录。
func TestRecordEventToSink(t *testing.T) {
	// 两个用例对象：一个不带 Series，一个带 Series.Count=2；
	// “isomorphic”借自事件聚合的概念，即结构兼容、可合并的事件。
	nonIsomorphicEvent := eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: metav1.NamespaceDefault,
		},
		Series: nil,
	}

	isomorphicEvent := *nonIsomorphicEvent.DeepCopy()
	isomorphicEvent.Series = &eventsv1.EventSeries{Count: 2}

	// 各用例覆盖：单条事件、原始事件后写入可合并事件、顺序颠倒、以及只写入可合并事件。
	// 由于 fake sink 以 name 为键覆盖，四种情况最终都只应剩一条记录。
	testCases := []struct {
		name                  string
		eventsToRecord        []eventsv1.Event
		expectedRecordedEvent eventsv1.Event
	}{
		{
			name: "record one Event",
			eventsToRecord: []eventsv1.Event{
				nonIsomorphicEvent,
			},
			expectedRecordedEvent: nonIsomorphicEvent,
		},
		{
			name: "record one Event followed by an isomorphic one",
			eventsToRecord: []eventsv1.Event{
				nonIsomorphicEvent,
				isomorphicEvent,
			},
			expectedRecordedEvent: isomorphicEvent,
		},
		{
			name: "record one isomorphic Event before the original",
			eventsToRecord: []eventsv1.Event{
				isomorphicEvent,
				nonIsomorphicEvent,
			},
			expectedRecordedEvent: isomorphicEvent,
		},
		{
			name: "record one isomorphic Event without one already existing",
			eventsToRecord: []eventsv1.Event{
				isomorphicEvent,
			},
			expectedRecordedEvent: isomorphicEvent,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// ktesting.NewTestContext 提供带 klog 记录器的 ctx，便于观察日志而不污染测试输出。
			_, ctx := ktesting.NewTestContext(t)
			kubeClient := fake.NewSimpleClientset()
			eventSink := &EventSinkImpl{Interface: kubeClient.EventsV1()}

			for _, ev := range tc.eventsToRecord {
				recordEvent(ctx, eventSink, &ev)
			}

			recordedEvents, err := kubeClient.EventsV1().Events(metav1.NamespaceDefault).List(context.TODO(), metav1.ListOptions{})
			if err != nil {
				t.Errorf("expected to be able to list Events from fake client")
			}

			// 同名事件应互相覆盖，因此只保留一条。
			if len(recordedEvents.Items) != 1 {
				t.Errorf("expected one Event to be recorded, found: %d", len(recordedEvents.Items))
			}

			recordedEvent := recordedEvents.Items[0]
			if !reflect.DeepEqual(recordedEvent, tc.expectedRecordedEvent) {
				t.Errorf("expected to have recorded Event: %#+v, got: %#+v\n diff: %s", tc.expectedRecordedEvent, recordedEvent, cmp.Diff(tc.expectedRecordedEvent, recordedEvent))
			}
		})
	}
}
