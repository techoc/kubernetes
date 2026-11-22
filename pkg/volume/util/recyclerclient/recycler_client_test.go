/*
Copyright 2018 The Kubernetes Authors.

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

package recyclerclient

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// testcase 描述一个回收器行为测试用例。
type testcase struct {
	// 测试输入
	name        string
	existingPod *v1.Pod
	createPod   *v1.Pod
	// eventSequence 是回收过程中模拟的事件序列，既可以是回收 Pod 产生的事件，
	// 也可以是 Pod 自身的状态变化（见下方的 newPodEvent 和 newEvent）。
	eventSequence []watch.Event

	// 预期输出
	// expectedEvents 是发送到被回收卷的事件列表。
	expectedEvents []mockEvent
	expectedError  string
}

// newPodEvent 构造一条携带 Pod 状态信息的 watch 事件。
func newPodEvent(eventtype watch.EventType, name string, phase v1.PodPhase, message string) watch.Event {
	return watch.Event{
		Type:   eventtype,
		Object: newPod(name, phase, message),
	}
}

// newEvent 构造一条模拟的 Event 对象 watch 事件。
func newEvent(eventtype, message string) watch.Event {
	return watch.Event{
		Type: watch.Added,
		Object: &v1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: metav1.NamespaceDefault,
			},
			Reason:  "MockEvent",
			Message: message,
			Type:    eventtype,
		},
	}
}

// newPod 构造指定名称和状态的测试 Pod。
func newPod(name string, phase v1.PodPhase, message string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: metav1.NamespaceDefault,
			Name:      name,
		},
		Status: v1.PodStatus{
			Phase:   phase,
			Message: message,
		},
	}
}

// TestRecyclerPod 验证回收器在成功、失败、Pod 被删除及旧 Pod 仍在运行时的行为。
func TestRecyclerPod(t *testing.T) {
	tests := []testcase{
		{
			// 回收成功并产生若干事件
			name:      "RecyclerSuccess",
			createPod: newPod("podRecyclerSuccess", v1.PodPending, ""),
			eventSequence: []watch.Event{
				// Pod 进入 Running，最终 Succeeded
				newPodEvent(watch.Added, "podRecyclerSuccess", v1.PodPending, ""),
				newEvent(v1.EventTypeNormal, "Successfully assigned recycler-for-podRecyclerSuccess to 127.0.0.1"),
				newEvent(v1.EventTypeNormal, "Pulling image \"registry.k8s.io/busybox\""),
				newEvent(v1.EventTypeNormal, "Successfully pulled image \"registry.k8s.io/busybox\""),
				newEvent(v1.EventTypeNormal, "Created container with docker id 83d929aeac82"),
				newEvent(v1.EventTypeNormal, "Started container with docker id 83d929aeac82"),
				newPodEvent(watch.Modified, "podRecyclerSuccess", v1.PodRunning, ""),
				newPodEvent(watch.Modified, "podRecyclerSuccess", v1.PodSucceeded, ""),
			},
			expectedEvents: []mockEvent{
				{v1.EventTypeNormal, "Successfully assigned recycler-for-podRecyclerSuccess to 127.0.0.1"},
				{v1.EventTypeNormal, "Pulling image \"registry.k8s.io/busybox\""},
				{v1.EventTypeNormal, "Successfully pulled image \"registry.k8s.io/busybox\""},
				{v1.EventTypeNormal, "Created container with docker id 83d929aeac82"},
				{v1.EventTypeNormal, "Started container with docker id 83d929aeac82"},
			},
			expectedError: "",
		},
		{
			// 回收失败并产生若干事件
			name:      "RecyclerFailure",
			createPod: newPod("podRecyclerFailure", v1.PodPending, ""),
			eventSequence: []watch.Event{
				// Pod 进入 Running，最终 Succeeded
				newPodEvent(watch.Added, "podRecyclerFailure", v1.PodPending, ""),
				newEvent(v1.EventTypeNormal, "Successfully assigned recycler-for-podRecyclerFailure to 127.0.0.1"),
				newEvent(v1.EventTypeWarning, "Unable to mount volumes for pod \"recycler-for-podRecyclerFailure_default(3c9809e5-347c-11e6-a79b-3c970e965218)\": timeout expired waiting for volumes to attach/mount"),
				newEvent(v1.EventTypeWarning, "Error syncing pod, skipping: timeout expired waiting for volumes to attach/mount for pod \"default\"/\"recycler-for-podRecyclerFailure\". list of unattached/unmounted"),
				newPodEvent(watch.Modified, "podRecyclerFailure", v1.PodRunning, ""),
				newPodEvent(watch.Modified, "podRecyclerFailure", v1.PodFailed, "Pod was active on the node longer than specified deadline"),
			},
			expectedEvents: []mockEvent{
				{v1.EventTypeNormal, "Successfully assigned recycler-for-podRecyclerFailure to 127.0.0.1"},
				{v1.EventTypeWarning, "Unable to mount volumes for pod \"recycler-for-podRecyclerFailure_default(3c9809e5-347c-11e6-a79b-3c970e965218)\": timeout expired waiting for volumes to attach/mount"},
				{v1.EventTypeWarning, "Error syncing pod, skipping: timeout expired waiting for volumes to attach/mount for pod \"default\"/\"recycler-for-podRecyclerFailure\". list of unattached/unmounted"},
			},
			expectedError: "failed to recycle volume: Pod was active on the node longer than specified deadline",
		},
		{
			// 回收 Pod 被删除
			name:      "RecyclerDeleted",
			createPod: newPod("podRecyclerDeleted", v1.PodPending, ""),
			eventSequence: []watch.Event{
				// Pod 进入 Running，最终 Succeeded
				newPodEvent(watch.Added, "podRecyclerDeleted", v1.PodPending, ""),
				newEvent(v1.EventTypeNormal, "Successfully assigned recycler-for-podRecyclerDeleted to 127.0.0.1"),
				newPodEvent(watch.Deleted, "podRecyclerDeleted", v1.PodPending, ""),
			},
			expectedEvents: []mockEvent{
				{v1.EventTypeNormal, "Successfully assigned recycler-for-podRecyclerDeleted to 127.0.0.1"},
			},
			expectedError: "failed to recycle volume: recycler pod was deleted",
		},
		{
			// 已有另一个回收 Pod 在运行
			name:          "RecyclerRunning",
			existingPod:   newPod("podOldRecycler", v1.PodRunning, ""),
			createPod:     newPod("podNewRecycler", v1.PodFailed, "mock message"),
			eventSequence: []watch.Event{},
			expectedError: "old recycler pod found, will retry later",
		},
	}

	for _, test := range tests {
		t.Logf("Test %q", test.name)
		client := &mockRecyclerClient{
			events: test.eventSequence,
			pod:    test.existingPod,
		}
		err := internalRecycleVolumeByWatchingPodUntilCompletion(test.createPod.Name, test.createPod, client)
		receivedError := ""
		if err != nil {
			receivedError = err.Error()
		}
		if receivedError != test.expectedError {
			t.Errorf("Test %q failed, expected error %q, got %q", test.name, test.expectedError, receivedError)
			continue
		}
		if !client.deletedCalled {
			t.Errorf("Test %q failed, expected deferred client.Delete to be called on recycler pod", test.name)
			continue
		}
		for i, expectedEvent := range test.expectedEvents {
			if len(client.receivedEvents) <= i {
				t.Errorf("Test %q failed, expected event %d: %q not received", test.name, i, expectedEvent.message)
				continue
			}
			receivedEvent := client.receivedEvents[i]
			if expectedEvent.eventtype != receivedEvent.eventtype {
				t.Errorf("Test %q failed, event %d does not match: expected eventtype %q, got %q", test.name, i, expectedEvent.eventtype, receivedEvent.eventtype)
			}
			if expectedEvent.message != receivedEvent.message {
				t.Errorf("Test %q failed, event %d does not match: expected message %q, got %q", test.name, i, expectedEvent.message, receivedEvent.message)
			}
		}
		for i := len(test.expectedEvents); i < len(client.receivedEvents); i++ {
			t.Errorf("Test %q failed, unexpected event received: %s, %q", test.name, client.receivedEvents[i].eventtype, client.receivedEvents[i].message)
		}
	}
}

// mockRecyclerClient 模拟回收器使用的 Pod 客户端，记录收到的事件。
type mockRecyclerClient struct {
	pod            *v1.Pod
	deletedCalled  bool
	receivedEvents []mockEvent
	events         []watch.Event
}

// mockEvent 记录一条事件的类型与消息。
type mockEvent struct {
	eventtype, message string
}

// CreatePod 记录创建的 Pod；若已有 Pod 则模拟"已存在"错误。
func (c *mockRecyclerClient) CreatePod(pod *v1.Pod) (*v1.Pod, error) {
	if c.pod == nil {
		c.pod = pod
		return c.pod, nil
	}
	// 模拟"已存在"错误
	return nil, errors.NewAlreadyExists(api.Resource("pods"), pod.Name)
}

// GetPod 返回已记录的 Pod，若不存在则报错。
func (c *mockRecyclerClient) GetPod(name, namespace string) (*v1.Pod, error) {
	if c.pod != nil {
		return c.pod, nil
	}
	return nil, fmt.Errorf("pod does not exist")
}

// DeletePod 记录删除调用。
func (c *mockRecyclerClient) DeletePod(name, namespace string) error {
	c.deletedCalled = true
	return nil
}

// WatchPod 返回按顺序发送预设事件的监听通道。
func (c *mockRecyclerClient) WatchPod(name, namespace string, stopChannel chan struct{}) (<-chan watch.Event, error) {
	eventCh := make(chan watch.Event)
	go func() {
		for _, e := range c.events {
			eventCh <- e
		}
	}()
	return eventCh, nil
}

// Event 记录回收器上报的事件。
func (c *mockRecyclerClient) Event(eventtype, message string) {
	c.receivedEvents = append(c.receivedEvents, mockEvent{eventtype, message})
}
