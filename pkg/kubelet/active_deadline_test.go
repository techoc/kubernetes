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

package kubelet

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// mockPodStatusProvider 按 UID 返回指定 Pod 的状态。
type mockPodStatusProvider struct {
	pods []*v1.Pod
}

// GetPodStatus 返回 UID 匹配的 Pod 状态；找不到时返回 false。
func (m *mockPodStatusProvider) GetPodStatus(uid types.UID) (v1.PodStatus, bool) {
	for _, pod := range m.pods {
		if pod.UID == uid {
			return pod.Status, true
		}
	}
	return v1.PodStatus{}, false
}

// TestNewActiveDeadlineHandler 验证构造器只有在 statusProvider、recorder 和 clock 三者齐全时才能成功。
func TestNewActiveDeadlineHandler(t *testing.T) {
	pods := newTestPods(1)
	podStatusProvider := &mockPodStatusProvider{pods: pods}
	fakeRecorder := &record.FakeRecorder{}
	fakeClock := testingclock.NewFakeClock(time.Now())

	// 逐一组合三个依赖的有无，覆盖全部缺失路径。
	testCases := []struct {
		podStatusProvider status.PodStatusProvider
		recorder          record.EventRecorder
		clock             clock.Clock
	}{
		{podStatusProvider, fakeRecorder, fakeClock},
		{podStatusProvider, fakeRecorder, nil},
		{podStatusProvider, nil, fakeClock},
		{podStatusProvider, nil, nil},
		{nil, fakeRecorder, fakeClock},
		{nil, fakeRecorder, nil},
		{nil, nil, fakeClock},
		{nil, nil, nil},
	}

	for i, testCase := range testCases {
		actual, err := newActiveDeadlineHandler(testCase.podStatusProvider, testCase.recorder, testCase.clock)

		// 只有第 0 个用例依赖齐全，其余用例都应返回错误。
		if i == 0 {
			expected := &activeDeadlineHandler{
				clock:             fakeClock,
				podStatusProvider: podStatusProvider,
				recorder:          fakeRecorder,
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("[%d] newActiveDeadlineHandler expected %#v, got %#v", i, expected, actual)
			}
			assert.NoError(t, err)

			continue
		}

		assert.Error(t, err)
	}
}

// TestActiveDeadlineHandler 验证活动截止时间处理器对各类 Pod 的同步与驱逐判定。
func TestActiveDeadlineHandler(t *testing.T) {
	pods := newTestPods(5)
	fakeClock := testingclock.NewFakeClock(time.Now())
	podStatusProvider := &mockPodStatusProvider{pods: pods}
	fakeRecorder := &record.FakeRecorder{}
	handler, err := newActiveDeadlineHandler(podStatusProvider, fakeRecorder, fakeClock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 统一以当前时间构造启动时间，便于精确控制是否超期。
	now := metav1.Now()
	startTime := metav1.NewTime(now.Time.Add(-1 * time.Minute))

	// 该 Pod 已运行 1 分钟而截止时间为 30 秒，属于已超期。
	exceededActiveDeadlineSeconds := int64(30)
	pods[0].Status.StartTime = &startTime
	pods[0].Spec.ActiveDeadlineSeconds = &exceededActiveDeadlineSeconds

	// 该 Pod 截止时间为 120 秒，尚未超期。
	notYetActiveDeadlineSeconds := int64(120)
	pods[1].Status.StartTime = &startTime
	pods[1].Spec.ActiveDeadlineSeconds = &notYetActiveDeadlineSeconds

	// 该 Pod 未设置截止时间。
	pods[2].Status.StartTime = &startTime
	pods[2].Spec.ActiveDeadlineSeconds = nil

	// 该 Pod 没有启动时间，无法判断是否超期。
	pods[3].Status.StartTime = nil
	pods[3].Spec.ActiveDeadlineSeconds = &notYetActiveDeadlineSeconds

	// 期望值依次为：已超期、未超期、无截止时间、无启动时间、未做任何配置。
	testCases := []struct {
		pod      *v1.Pod
		expected bool
	}{{pods[0], true}, {pods[1], false}, {pods[2], false}, {pods[3], false}, {pods[4], false}}

	for i, testCase := range testCases {
		if actual := handler.ShouldSync(testCase.pod); actual != testCase.expected {
			t.Errorf("[%d] ShouldSync expected %#v, got %#v", i, testCase.expected, actual)
		}
		actual := handler.ShouldEvict(testCase.pod)
		if actual.Evict != testCase.expected {
			t.Errorf("[%d] ShouldEvict.Evict expected %#v, got %#v", i, testCase.expected, actual.Evict)
		}
		if testCase.expected {
			if actual.Reason != reason {
				t.Errorf("[%d] ShouldEvict.Reason expected %#v, got %#v", i, message, actual.Reason)
			}
			if actual.Message != message {
				t.Errorf("[%d] ShouldEvict.Message expected %#v, got %#v", i, message, actual.Message)
			}
		}
	}
}
