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

package pleg

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

const (
	testContainerRuntimeType = "fooRuntime"
	// largeChannelCap 的容量足够容纳单个测试产生的所有事件。
	largeChannelCap = 100
)

// TestGenericPLEG 聚合 GenericPLEG、假运行时与假时钟，便于各测试复用。
type TestGenericPLEG struct {
	pleg    *GenericPLEG
	runtime *containertest.FakeRuntime
	clock   *testingclock.FakeClock
}

// newTestGenericPLEG 以默认通道容量创建测试用 GenericPLEG。
func newTestGenericPLEG() *TestGenericPLEG {
	return newTestGenericPLEGWithChannelSize(largeChannelCap)
}

// newTestGenericPLEGWithChannelSize 以指定事件通道容量创建测试用 GenericPLEG。
func newTestGenericPLEGWithChannelSize(eventChannelCap int) *TestGenericPLEG {
	fakeRuntime := &containertest.FakeRuntime{}
	fakeCache := containertest.NewFakeCache(fakeRuntime)
	clock := testingclock.NewFakeClock(time.Time{})
	// 通道容量需足够容纳单个测试产生的所有事件。
	pleg := NewGenericPLEG(
		fakeRuntime,
		make(chan *PodLifecycleEvent, eventChannelCap),
		&RelistDuration{RelistPeriod: time.Hour, RelistThreshold: 3 * time.Minute},
		fakeCache,
		clock,
	).(*GenericPLEG)
	return &TestGenericPLEG{pleg: pleg, runtime: fakeRuntime, clock: clock}
}

// getEventsFromChannel 取出通道中已有的全部事件，用于断言。
func getEventsFromChannel(ch <-chan *PodLifecycleEvent) []*PodLifecycleEvent {
	events := []*PodLifecycleEvent{}
	for len(ch) > 0 {
		e := <-ch
		events = append(events, e)
	}
	return events
}

// createTestContainer 构造指定 ID 与状态的测试容器。
func createTestContainer(id string, state kubecontainer.State) *kubecontainer.Container {
	return &kubecontainer.Container{
		ID:    kubecontainer.ContainerID{Type: testContainerRuntimeType, ID: id},
		Name:  id,
		State: state,
	}
}

// sortableEvents 让事件可按 ID 与数据排序，便于比较。
type sortableEvents []*PodLifecycleEvent

// Len 返回事件数量。
func (a sortableEvents) Len() int { return len(a) }

// Swap 交换两个事件的位置。
func (a sortableEvents) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 先按 Pod ID、再按事件数据排序。
func (a sortableEvents) Less(i, j int) bool {
	if a[i].ID != a[j].ID {
		return a[i].ID < a[j].ID
	}
	return a[i].Data.(string) < a[j].Data.(string)
}

// verifyEvents 排序后比较期望事件与实际事件。
func verifyEvents(t *testing.T, expected, actual []*PodLifecycleEvent) {
	sort.Sort(sortableEvents(expected))
	sort.Sort(sortableEvents(actual))
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Actual events differ from the expected; diff:\n %v", cmp.Diff(expected, actual))
	}
}

// TestRelisting 验证 relist 能上报容器启动、死亡与移除，且状态未变化时不发事件。
func TestRelisting(t *testing.T) {
	tCtx := ktesting.Init(t)
	testPleg := newTestGenericPLEG()
	pleg, runtime := testPleg.pleg, testPleg.runtime
	ch := pleg.Watch()
	// 第一次 relist 应向每个 Pod 发送 PodSync 事件。
	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
				createTestContainer("c2", kubecontainer.ContainerStateRunning),
				createTestContainer("c3", kubecontainer.ContainerStateUnknown),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
			},
		}},
	}
	pleg.Relist(tCtx)
	// 首次看到运行中或已退出的容器时，都要上报对应事件。
	expected := []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerStarted, Data: "c2"},
		{ID: "4567", Type: ContainerDied, Data: "c1"},
		{ID: "1234", Type: ContainerDied, Data: "c1"},
	}
	actual := getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)

	// 第二次 relist 不应发出任何事件，因为容器状态未变化。
	pleg.Relist(tCtx)
	actual = getEventsFromChannel(ch)
	assert.Empty(t, actual, "no container has changed, event length should be 0")

	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c2", kubecontainer.ContainerStateExited),
				createTestContainer("c3", kubecontainer.ContainerStateRunning),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Containers: []*kubecontainer.Container{
				createTestContainer("c4", kubecontainer.ContainerStateRunning),
			},
		}},
	}
	pleg.Relist(tCtx)
	// 只上报切换到运行中或已退出状态的容器。
	expected = []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerRemoved, Data: "c1"},
		{ID: "1234", Type: ContainerDied, Data: "c2"},
		{ID: "1234", Type: ContainerStarted, Data: "c3"},
		{ID: "4567", Type: ContainerRemoved, Data: "c1"},
		{ID: "4567", Type: ContainerStarted, Data: "c4"},
	}

	actual = getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)
}

// TestEventChannelFull 验证事件通道满时，事件会被丢弃。
func TestEventChannelFull(t *testing.T) {
	tCtx := ktesting.Init(t)
	testPleg := newTestGenericPLEGWithChannelSize(4)
	pleg, runtime := testPleg.pleg, testPleg.runtime
	ch := pleg.Watch()
	// 第一次 relist 应向每个 Pod 发送 PodSync 事件。
	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
				createTestContainer("c2", kubecontainer.ContainerStateRunning),
				createTestContainer("c3", kubecontainer.ContainerStateUnknown),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
			},
		}},
	}
	pleg.Relist(tCtx)
	// 首次看到运行中或已退出的容器时，都要上报对应事件。
	expected := []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerStarted, Data: "c2"},
		{ID: "4567", Type: ContainerDied, Data: "c1"},
		{ID: "1234", Type: ContainerDied, Data: "c1"},
	}
	actual := getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)

	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c2", kubecontainer.ContainerStateExited),
				createTestContainer("c3", kubecontainer.ContainerStateRunning),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Containers: []*kubecontainer.Container{
				createTestContainer("c4", kubecontainer.ContainerStateRunning),
			},
		}},
	}
	pleg.Relist(tCtx)
	allEvents := []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerRemoved, Data: "c1"},
		{ID: "1234", Type: ContainerDied, Data: "c2"},
		{ID: "1234", Type: ContainerStarted, Data: "c3"},
		{ID: "4567", Type: ContainerRemoved, Data: "c1"},
		{ID: "4567", Type: ContainerStarted, Data: "c4"},
	}
	// 事件通道已满，后续事件被丢弃
	actual = getEventsFromChannel(ch)
	assert.Len(t, actual, 4, "channel length should be 4")
	assert.Subsetf(t, allEvents, actual, "actual events should in all events")
}

// TestDetectingContainerDeaths 以不同的 relist 次数验证容器/ Pod 消失事件的上报。
func TestDetectingContainerDeaths(t *testing.T) {
	// 变化容器启动后到死亡前 relist 的次数，以覆盖 pleg 内部状态的不同变化。
	testReportMissingContainers(t, 1)
	testReportMissingPods(t, 1)

	testReportMissingContainers(t, 3)
	testReportMissingPods(t, 3)
}

// testReportMissingContainers 验证容器在两次 relist 之间消失时的死亡与移除事件。
func testReportMissingContainers(t *testing.T, numRelists int) {
	tCtx := ktesting.Init(t)
	testPleg := newTestGenericPLEG()
	pleg, runtime := testPleg.pleg, testPleg.runtime
	ch := pleg.Watch()
	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateRunning),
				createTestContainer("c2", kubecontainer.ContainerStateRunning),
				createTestContainer("c3", kubecontainer.ContainerStateExited),
			},
		}},
	}
	// relist 并排空通道中的事件。
	for i := 0; i < numRelists; i++ {
		pleg.Relist(tCtx)
		getEventsFromChannel(ch)
	}

	// 容器 c2 在两次 relist 之间被停止并移除，应上报事件。
	// 已退出的容器 c3 在两次 relist 之间被垃圾回收（即被移除），该事件应被忽略。
	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateRunning),
			},
		}},
	}
	pleg.Relist(tCtx)
	expected := []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerDied, Data: "c2"},
		{ID: "1234", Type: ContainerRemoved, Data: "c2"},
		{ID: "1234", Type: ContainerRemoved, Data: "c3"},
	}
	actual := getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)
}

// testReportMissingPods 验证 Pod 整体消失时的死亡与移除事件。
func testReportMissingPods(t *testing.T, numRelists int) {
	tCtx := ktesting.Init(t)
	testPleg := newTestGenericPLEG()
	pleg, runtime := testPleg.pleg, testPleg.runtime
	ch := pleg.Watch()
	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c2", kubecontainer.ContainerStateRunning),
			},
		}},
	}
	// relist 并排空通道中的事件。
	for i := 0; i < numRelists; i++ {
		pleg.Relist(tCtx)
		getEventsFromChannel(ch)
	}

	// 容器 c2 在两次 relist 之间被停止并移除，应上报事件。
	runtime.AllPodList = []*containertest.FakePod{}
	pleg.Relist(tCtx)
	expected := []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerDied, Data: "c2"},
		{ID: "1234", Type: ContainerRemoved, Data: "c2"},
	}
	actual := getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)
}

// newTestGenericPLEGWithRuntimeMock 用 mock 运行时与真实时钟创建 GenericPLEG。
func newTestGenericPLEGWithRuntimeMock(runtimeMock kubecontainer.Runtime) *GenericPLEG {
	pleg := NewGenericPLEG(
		runtimeMock,
		make(chan *PodLifecycleEvent, 1000),
		&RelistDuration{RelistPeriod: time.Hour, RelistThreshold: 2 * time.Hour},
		kubecontainer.NewCache(),
		clock.RealClock{},
	).(*GenericPLEG)
	return pleg
}

// createTestPodsStatusesAndEvents 构造指定数量的 Pod、状态与事件三元组。
func createTestPodsStatusesAndEvents(num int) ([]*kubecontainer.Pod, []*kubecontainer.PodStatus, []*PodLifecycleEvent) {
	var pods []*kubecontainer.Pod
	var statuses []*kubecontainer.PodStatus
	var events []*PodLifecycleEvent
	for i := 0; i < num; i++ {
		id := types.UID(fmt.Sprintf("test-pod-%d", i))
		cState := kubecontainer.ContainerStateRunning
		container := createTestContainer(fmt.Sprintf("c%d", i), cState)
		pod := &kubecontainer.Pod{
			ID:         id,
			Containers: []*kubecontainer.Container{container},
		}
		status := &kubecontainer.PodStatus{
			ID:                id,
			ContainerStatuses: []*kubecontainer.Status{{ID: container.ID, State: cState}},
		}
		event := &PodLifecycleEvent{ID: pod.ID, Type: ContainerStarted, Data: container.ID.ID}
		pods = append(pods, pod)
		statuses = append(statuses, status)
		events = append(events, event)

	}
	return pods, statuses, events
}

// TestRelistWithCache 验证 relist 会缓存 Pod 状态，并按错误情况决定是否产生事件。
func TestRelistWithCache(t *testing.T) {
	tCtx := ktesting.Init(t)
	runtimeMock := containertest.NewMockRuntime(t)

	pleg := newTestGenericPLEGWithRuntimeMock(runtimeMock)
	ch := pleg.Watch()

	pods, statuses, events := createTestPodsStatusesAndEvents(2)
	runtimeMock.EXPECT().GetPods(mock.Anything, true).Return(pods, nil).Maybe()
	runtimeMock.EXPECT().GetPodStatus(mock.Anything, pods[0]).Return(statuses[0], nil).Times(1)
	// 在查询 pods[1] 状态时注入错误。
	statusErr := fmt.Errorf("unable to get status")
	// 失败查询时缓存仍保留空状态与错误，供后续断言。
	runtimeMock.EXPECT().GetPodStatus(mock.Anything, pods[1]).Return(&kubecontainer.PodStatus{}, statusErr).Times(1)

	pleg.Relist(tCtx)
	actualEvents := getEventsFromChannel(ch)
	cases := []struct {
		pod    *kubecontainer.Pod
		status *kubecontainer.PodStatus
		error  error
	}{
		{pod: pods[0], status: statuses[0], error: nil},
		{pod: pods[1], status: &kubecontainer.PodStatus{}, error: statusErr},
	}
	for i, c := range cases {
		testStr := fmt.Sprintf("test[%d]", i)
		actualStatus, actualErr := pleg.cache.Get(c.pod.ID)
		assert.Equal(t, c.status, actualStatus, testStr)
		assert.Equal(t, c.error, actualErr, testStr)
	}
	// 由于出错，pleg 不应为 pods[1] 生成任何事件。
	assert.Exactly(t, []*PodLifecycleEvent{events[0]}, actualEvents)

	// 恢复 pods[1] 的正常状态。
	runtimeMock.EXPECT().GetPodStatus(mock.Anything, pods[1]).Return(statuses[1], nil).Times(1)
	pleg.Relist(tCtx)
	actualEvents = getEventsFromChannel(ch)
	cases = []struct {
		pod    *kubecontainer.Pod
		status *kubecontainer.PodStatus
		error  error
	}{
		{pod: pods[0], status: statuses[0], error: nil},
		{pod: pods[1], status: statuses[1], error: nil},
	}
	for i, c := range cases {
		testStr := fmt.Sprintf("test[%d]", i)
		actualStatus, actualErr := pleg.cache.Get(c.pod.ID)
		assert.Equal(t, c.status, actualStatus, testStr)
		assert.Equal(t, c.error, actualErr, testStr)
	}
	// 现在可以查询到 pods[1] 的状态，pleg 应生成事件。
	assert.Exactly(t, []*PodLifecycleEvent{events[1]}, actualEvents)
}

// TestRemoveCacheEntry 验证 Pod 从运行时消失后其缓存条目会被移除。
func TestRemoveCacheEntry(t *testing.T) {
	tCtx := ktesting.Init(t)
	runtimeMock := containertest.NewMockRuntime(t)
	pleg := newTestGenericPLEGWithRuntimeMock(runtimeMock)

	pods, statuses, _ := createTestPodsStatusesAndEvents(1)
	runtimeMock.EXPECT().GetPods(mock.Anything, true).Return(pods, nil).Times(1)
	runtimeMock.EXPECT().GetPodStatus(mock.Anything, pods[0]).Return(statuses[0], nil).Times(1)
	// 执行一次 relist 以填充缓存。
	pleg.Relist(tCtx)
	// 从运行时删除 Pod，验证 relist 后缓存条目已被移除。
	runtimeMock.EXPECT().GetPods(mock.Anything, true).Return([]*kubecontainer.Pod{}, nil).Times(1)
	pleg.Relist(tCtx)
	actualStatus, actualErr := pleg.cache.Get(pods[0].ID)
	assert.Equal(t, &kubecontainer.PodStatus{ID: pods[0].ID}, actualStatus)
	assert.NoError(t, actualErr)
}

// TestHealthy 验证 relist 时间阈值决定 pleg 的健康状态。
func TestHealthy(t *testing.T) {
	tCtx := ktesting.Init(t)
	testPleg := newTestGenericPLEG()

	// pleg 初始应处于不健康状态
	pleg, _, clock := testPleg.pleg, testPleg.runtime, testPleg.clock
	ok, _ := pleg.Healthy()
	assert.False(t, ok, "pleg should be unhealthy")

	// 推进时钟但不进行 relist。
	clock.Step(time.Minute * 10)
	ok, _ = pleg.Healthy()
	assert.False(t, ok, "pleg should be unhealthy")

	// relist 后推进 1 分钟，pleg 应处于健康状态，因为仍在允许阈值内。
	pleg.Relist(tCtx)
	clock.Step(time.Minute * 1)
	ok, _ = pleg.Healthy()
	assert.True(t, ok, "pleg should be healthy")

	// 在未 relist 的情况下推进 relistThreshold，pleg 应不健康，因为距上次 relist 已超过该阈值。
	clock.Step(pleg.relistDuration.RelistThreshold)
	ok, _ = pleg.Healthy()
	assert.False(t, ok, "pleg should be unhealthy")
}

// TestReinspect 验证 reinspect 请求在不同成功/失败组合下的处理与状态变化。
func TestReinspect(t *testing.T) {
	tCtx := ktesting.Init(t)

	tests := []struct {
		name              string
		requestReinspect  bool
		alreadyReinspect  bool
		updateCacheError  error
		podDeleted        bool
		expectUpdateCache bool
		expectReinspect   bool // value in podsToReinspect AFTER relist
		expectEvent       bool
		expectStatus      bool
	}{
		{
			name:              "RequestReinspect a pod not previously listed, success",
			requestReinspect:  true,
			alreadyReinspect:  false,
			updateCacheError:  nil,
			expectUpdateCache: true,
			expectReinspect:   false,
			expectEvent:       true,
			expectStatus:      true,
		},
		{
			name:              "RequestReinspect a pod not previously listed, failure",
			requestReinspect:  true,
			alreadyReinspect:  false,
			updateCacheError:  errors.New("fail"),
			expectUpdateCache: true,
			expectReinspect:   true,
			expectEvent:       false,
			expectStatus:      true,
		},
		{
			name:              "RequestReinspect of a pod already listed for reinspection, success",
			requestReinspect:  true,
			alreadyReinspect:  true,
			updateCacheError:  nil,
			expectUpdateCache: true,
			expectReinspect:   false,
			expectEvent:       true,
			expectStatus:      true,
		},
		{
			name:              "RequestReinspect of a pod already listed for reinspection, failure",
			requestReinspect:  true,
			alreadyReinspect:  true,
			updateCacheError:  errors.New("fail"),
			expectUpdateCache: true,
			expectReinspect:   true,
			expectEvent:       false,
			expectStatus:      true,
		},
		{
			name:              "Don't request reinspection",
			requestReinspect:  false,
			alreadyReinspect:  false,
			updateCacheError:  nil,
			expectUpdateCache: false,
			expectReinspect:   false,
			expectEvent:       false,
			expectStatus:      false,
		},
		{
			name:              "Don't request reinspection, but already listed for reinspection, success",
			requestReinspect:  false,
			alreadyReinspect:  true,
			updateCacheError:  nil,
			expectUpdateCache: true,
			expectReinspect:   false,
			expectEvent:       true,
			expectStatus:      true,
		},
		{
			name:              "Don't request reinspection, but already listed for reinspection, failure",
			requestReinspect:  false,
			alreadyReinspect:  true,
			updateCacheError:  errors.New("fail"),
			expectUpdateCache: true,
			expectReinspect:   true,
			expectEvent:       false,
			expectStatus:      true,
		},
		{
			name:              "Pod deleted, should clear reinspect",
			requestReinspect:  false,
			alreadyReinspect:  true,
			podDeleted:        true,
			updateCacheError:  nil,
			expectUpdateCache: true,
			expectReinspect:   false,
			expectEvent:       true,
			expectStatus:      false, // Deleted from cache
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runtimeMock := containertest.NewMockRuntime(t)
			pleg := newTestGenericPLEGWithRuntimeMock(runtimeMock)
			ch := pleg.Watch()

			podID := types.UID("test-pod")
			if tc.alreadyReinspect || tc.requestReinspect {
				pleg.RequestReinspect(podID)
			}

			pod := &kubecontainer.Pod{ID: podID, Name: "name", Namespace: "ns"}
			// 填充 podRecords。
			pleg.podRecords[podID] = &podRecord{old: pod, current: pod}

			if tc.podDeleted {
				runtimeMock.EXPECT().GetPods(mock.Anything, true).Return([]*kubecontainer.Pod{}, nil)
			} else {
				runtimeMock.EXPECT().GetPods(mock.Anything, true).Return([]*kubecontainer.Pod{pod}, nil)
			}

			var expectedStatus *kubecontainer.PodStatus
			if tc.updateCacheError == nil {
				expectedStatus = &kubecontainer.PodStatus{ID: podID, TimeStamp: time.Now()}
			}
			if tc.expectUpdateCache {
				if tc.podDeleted {
					// updateCache(tCtx, nil, podID) 会被调用，它不会调用 GetPodStatus
				} else {
					runtimeMock.EXPECT().GetPodStatus(mock.Anything, pod).Return(expectedStatus, tc.updateCacheError)
				}
			}

			pleg.Relist(tCtx)

			_, actualReinspect := pleg.podsToReinspect.Load(podID)
			assert.Equal(t, tc.expectReinspect, actualReinspect)

			actualEvents := getEventsFromChannel(ch)
			if tc.expectEvent {
				assert.NotEmpty(t, actualEvents, "Expected events to be emitted")
				// 由于测试设置中 Pod 状态未变化，预期至少有一个 PodSync 事件
				hasPodSync := false
				for _, e := range actualEvents {
					if e.ID == podID && e.Type == PodSync {
						hasPodSync = true
						break
					}
				}
				assert.True(t, hasPodSync, "Expected PodSync event for pod")
			} else {
				assert.Empty(t, actualEvents, "Expected no events to be emitted")
			}

			if tc.expectStatus {
				actualStatus, actualErr := pleg.cache.Get(podID)
				assert.Equal(t, expectedStatus, actualStatus)
				assert.Equal(t, tc.updateCacheError, actualErr)
			} else if tc.podDeleted {
				actualStatus, _ := pleg.cache.Get(podID)
				// 删除后，Get 返回带 ID 的空状态
				assert.Equal(t, &kubecontainer.PodStatus{ID: podID}, actualStatus)
			}
		})
	}
}

// TestUpdateCacheUsesPodTimestampWhenEventedPLEGIsEnabled 验证开启 EventedPLEG 时以 Pod 时间戳决定缓存更新。
func TestUpdateCacheUsesPodTimestampWhenEventedPLEGIsEnabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.EventedPLEG, true)

	ctx := t.Context()
	podID := types.UID("test-pod")
	cachedTimestamp := time.Unix(0, 20)
	statusTimestamp := time.Unix(0, 10)
	podTimestamp := time.Unix(0, 30)

	runtimeMock := containertest.NewMockRuntime(t)
	cache := kubecontainer.NewCache()
	pleg := NewGenericPLEG(
		runtimeMock,
		make(chan *PodLifecycleEvent, largeChannelCap),
		&RelistDuration{RelistPeriod: time.Hour, RelistThreshold: 3 * time.Minute},
		cache,
		testingclock.NewFakeClock(time.Time{}),
	).(*GenericPLEG)

	cache.Set(podID, &kubecontainer.PodStatus{ID: podID}, nil, cachedTimestamp)

	pod := &kubecontainer.Pod{ID: podID, Name: "name", Namespace: "ns", Timestamp: podTimestamp}
	expectedStatus := &kubecontainer.PodStatus{ID: podID, TimeStamp: statusTimestamp}
	runtimeMock.EXPECT().GetPodStatus(ctx, pod).Return(expectedStatus, nil)

	status, err := pleg.updateCache(ctx, pod, podID)
	require.NoError(t, err)
	assert.Equal(t, expectedStatus, status)

	cachedStatus, cachedErr := cache.Get(podID)
	require.NoError(t, cachedErr)
	assert.Equal(t, expectedStatus, cachedStatus)
}

// 测试沙箱状态变化的检测。
// TestRelistingWithSandboxes 验证沙箱状态变化的检测与事件上报。
func TestRelistingWithSandboxes(t *testing.T) {
	tCtx := ktesting.Init(t)
	testPleg := newTestGenericPLEG()
	pleg, runtime := testPleg.pleg, testPleg.runtime
	ch := pleg.Watch()
	// 第一次 relist 应向每个 Pod 发送 PodSync 事件。
	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Sandboxes: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
				createTestContainer("c2", kubecontainer.ContainerStateRunning),
				createTestContainer("c3", kubecontainer.ContainerStateUnknown),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Sandboxes: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
			},
		}},
	}
	pleg.Relist(tCtx)
	// 首次看到运行中或已退出的容器时，都要上报对应事件。
	expected := []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerStarted, Data: "c2"},
		{ID: "4567", Type: ContainerDied, Data: "c1"},
		{ID: "1234", Type: ContainerDied, Data: "c1"},
	}
	actual := getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)

	// 第二次 relist 不应发出任何事件，因为容器状态未变化。
	pleg.Relist(tCtx)
	verifyEvents(t, expected, actual)

	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Sandboxes: []*kubecontainer.Container{
				createTestContainer("c2", kubecontainer.ContainerStateExited),
				createTestContainer("c3", kubecontainer.ContainerStateRunning),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Sandboxes: []*kubecontainer.Container{
				createTestContainer("c4", kubecontainer.ContainerStateRunning),
			},
		}},
	}
	pleg.Relist(tCtx)
	// 只上报切换到运行中或已退出状态的容器。
	expected = []*PodLifecycleEvent{
		{ID: "1234", Type: ContainerRemoved, Data: "c1"},
		{ID: "1234", Type: ContainerDied, Data: "c2"},
		{ID: "1234", Type: ContainerStarted, Data: "c3"},
		{ID: "4567", Type: ContainerRemoved, Data: "c1"},
		{ID: "4567", Type: ContainerStarted, Data: "c4"},
	}

	actual = getEventsFromChannel(ch)
	verifyEvents(t, expected, actual)
}

// TestRelistIPChange 验证 IP 变化与容器终止时的状态与事件。
func TestRelistIPChange(t *testing.T) {
	tCtx := ktesting.Init(t)
	testCases := []struct {
		name   string
		podID  string
		podIPs []string
	}{
		{
			name:   "test-0",
			podID:  "test-pod-0",
			podIPs: []string{"192.168.1.5"},
		},
		{
			name:   "tets-1",
			podID:  "test-pod-1",
			podIPs: []string{"192.168.1.5/24", "2000::"},
		},
	}

	for _, tc := range testCases {
		runtimeMock := containertest.NewMockRuntime(t)

		pleg := newTestGenericPLEGWithRuntimeMock(runtimeMock)
		ch := pleg.Watch()

		id := types.UID(tc.podID)
		cState := kubecontainer.ContainerStateRunning
		container := createTestContainer("c0", cState)
		pod := &kubecontainer.Pod{
			ID:         id,
			Containers: []*kubecontainer.Container{container},
		}
		status := &kubecontainer.PodStatus{
			ID:                id,
			IPs:               tc.podIPs,
			ContainerStatuses: []*kubecontainer.Status{{ID: container.ID, State: cState}},
		}
		event := &PodLifecycleEvent{ID: pod.ID, Type: ContainerStarted, Data: container.ID.ID}

		runtimeMock.EXPECT().GetPods(mock.Anything, true).Return([]*kubecontainer.Pod{pod}, nil).Times(1)
		runtimeMock.EXPECT().GetPodStatus(mock.Anything, pod).Return(status, nil).Times(1)

		pleg.Relist(tCtx)
		actualEvents := getEventsFromChannel(ch)
		actualStatus, actualErr := pleg.cache.Get(pod.ID)
		assert.Equal(t, status, actualStatus, tc.name)
		assert.NoError(t, actualErr, tc.name)
		assert.Exactly(t, []*PodLifecycleEvent{event}, actualEvents)

		// 清空 IP 并标记容器已终止
		container = createTestContainer("c0", kubecontainer.ContainerStateExited)
		pod = &kubecontainer.Pod{
			ID:         id,
			Containers: []*kubecontainer.Container{container},
		}
		status = &kubecontainer.PodStatus{
			ID:                id,
			ContainerStatuses: []*kubecontainer.Status{{ID: container.ID, State: kubecontainer.ContainerStateExited}},
		}
		event = &PodLifecycleEvent{ID: pod.ID, Type: ContainerDied, Data: container.ID.ID}
		runtimeMock.EXPECT().GetPods(mock.Anything, true).Return([]*kubecontainer.Pod{pod}, nil).Times(1)
		runtimeMock.EXPECT().GetPodStatus(mock.Anything, pod).Return(status, nil).Times(1)

		pleg.Relist(tCtx)
		actualEvents = getEventsFromChannel(ch)
		actualStatus, actualErr = pleg.cache.Get(pod.ID)
		// 必须复制状态再比较，因为该指针会一直传递到事件中。
		statusCopy := *status
		statusCopy.IPs = tc.podIPs
		assert.Equal(t, &statusCopy, actualStatus, tc.name)
		require.NoError(t, actualErr, tc.name)
		assert.Exactly(t, []*PodLifecycleEvent{event}, actualEvents)
	}
}

// TestRunningPodAndContainerCount 验证运行中容器与运行中沙箱数量的指标输出。
func TestRunningPodAndContainerCount(t *testing.T) {
	tCtx := ktesting.Init(t)
	metrics.Register()
	testPleg := newTestGenericPLEG()
	pleg, runtime := testPleg.pleg, testPleg.runtime

	runtime.AllPodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID: "1234",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateRunning),
				createTestContainer("c2", kubecontainer.ContainerStateUnknown),
				createTestContainer("c3", kubecontainer.ContainerStateUnknown),
			},
			Sandboxes: []*kubecontainer.Container{
				createTestContainer("s1", kubecontainer.ContainerStateRunning),
				createTestContainer("s2", kubecontainer.ContainerStateRunning),
				createTestContainer("s3", kubecontainer.ContainerStateUnknown),
			},
		}},
		{Pod: &kubecontainer.Pod{
			ID: "4567",
			Containers: []*kubecontainer.Container{
				createTestContainer("c1", kubecontainer.ContainerStateExited),
			},
			Sandboxes: []*kubecontainer.Container{
				createTestContainer("s1", kubecontainer.ContainerStateRunning),
				createTestContainer("s2", kubecontainer.ContainerStateExited),
			},
		}},
	}

	pleg.Relist(tCtx)

	tests := []struct {
		name        string
		metricsName string
		wants       string
	}{
		{
			name:        "test container count",
			metricsName: "kubelet_running_containers",
			wants: `
# HELP kubelet_running_containers [ALPHA] Number of containers currently running
# TYPE kubelet_running_containers gauge
kubelet_running_containers{container_state="exited"} 1
kubelet_running_containers{container_state="running"} 1
kubelet_running_containers{container_state="unknown"} 2
`,
		},
		{
			name:        "test pod count",
			metricsName: "kubelet_running_pods",
			wants: `
# HELP kubelet_running_pods [ALPHA] Number of pods that have a running pod sandbox
# TYPE kubelet_running_pods gauge
kubelet_running_pods 2
`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(tc.wants), tc.metricsName); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWorkerLoop 验证按需 relist 的工作循环：单 Pod relist、全局 relist 优先级与 reinspect。
func TestWorkerLoop(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.PLEGOnDemandRelist, true)
	synctest.Test(t, func(t *testing.T) {
		tCtx := ktesting.Init(t)
		runtimeMock := containertest.NewMockRuntime(t)
		cache := kubecontainer.NewCache()
		pleg := NewGenericPLEG(
			runtimeMock,
			make(chan *PodLifecycleEvent, 100),
			&RelistDuration{RelistPeriod: 2 * time.Second},
			cache,
			clock.RealClock{},
		).(*GenericPLEG)

		pod1 := &kubecontainer.Pod{ID: "pod1", Name: "pod1", Containers: []*kubecontainer.Container{{ID: kubecontainer.ContainerID{Type: "test", ID: "c1"}, State: kubecontainer.ContainerStateRunning}}}
		pod2 := &kubecontainer.Pod{ID: "pod2", Name: "pod2", Containers: []*kubecontainer.Container{{ID: kubecontainer.ContainerID{Type: "test", ID: "c2"}, State: kubecontainer.ContainerStateRunning}}}

		var call *mock.Call // Used to assert order of mock calls.

		startTime := time.Now()
		pleg.globalRelistTimer = pleg.clock.NewTimer(2 * time.Second)

		// pod1 与 pod2 的请求初始应处于阻塞状态。
		p1res := getNewerThanAsync(t, cache, pod1.ID, startTime)
		requireBlocked(t, p1res)
		p2res := getNewerThanAsync(t, cache, pod2.ID, startTime)
		requireBlocked(t, p2res)

		t.Log("Test single-pod relist (no reinspect)")
		pleg.RequestRelist(tCtx.Logger(), pod1.ID)

		call = runtimeMock.EXPECT().GetPod(mock.Anything, pod1.ID).RunAndReturn(func(ctx context.Context, uid types.UID) (*kubecontainer.Pod, error) {
			assert.Equal(t, pod1.ID, uid)
			pod1.Timestamp = time.Now()
			return pod1, nil
		}).Once()
		call = runtimeMock.EXPECT().GetPodStatus(mock.Anything, pod1).RunAndReturn(func(_ context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error) {
			assert.Equal(t, pod1, pod)
			return &kubecontainer.PodStatus{ID: pod1.ID, TimeStamp: time.Now()}, nil
		}).NotBefore(call).Once()

		pleg.workerLoopIteration(tCtx)

		p1Status := requireUnblocked(t, p1res) // pod1 现已解除阻塞
		assert.Equal(t, pod1.ID, p1Status.ID)
		assert.Equal(t, time.Now(), p1Status.TimeStamp)
		requireBlocked(t, p2res) // pod2 is still blocked

		p1NewRes := getNewerThanAsync(t, cache, pod1.ID, startTime.Add(2*time.Second))
		requireBlocked(t, p1NewRes)

		t.Log("Test triggering both global relist and pod2 relist to ensure the global relist gets priority.")
		pleg.RequestRelist(tCtx.Logger(), pod2.ID)
		time.Sleep(2 * time.Second)

		call = runtimeMock.EXPECT().GetPods(mock.Anything, true).RunAndReturn(func(_ context.Context, _ bool) ([]*kubecontainer.Pod, error) {
			pod1.Timestamp = time.Now()
			pod2.Timestamp = time.Now()
			return []*kubecontainer.Pod{pod1, pod2}, nil
		}).NotBefore(call).Once()
		call = runtimeMock.EXPECT().GetPodStatus(mock.Anything, pod2).RunAndReturn(func(_ context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error) {
			assert.Equal(t, pod2, pod)
			return &kubecontainer.PodStatus{ID: pod2.ID, TimeStamp: time.Now()}, nil
		}).NotBefore(call).Once()

		pleg.workerLoopIteration(tCtx)

		// 全局 relist 应已解除 p2res 与 p1NewRes 的阻塞。
		p2Status := requireUnblocked(t, p2res)
		assert.Equal(t, pod2.ID, p2Status.ID)
		p1NewStatus := requireUnblocked(t, p1NewRes)
		assert.Equal(t, p1Status, p1NewStatus) // 状态时间戳不应变化

		// pod2 的 relist 请求不应再触发 relist，因为它发出早于全局 relist。
		// 从通道排空它以验证（若为它调用 GetPod，mock 会报错）。
		pleg.workerLoopIteration(tCtx)

		t.Log("Test reinspection")
		p1ReinpsectRes := getNewerThanAsync(t, cache, pod1.ID, time.Now())
		requireBlocked(t, p1ReinpsectRes)

		// 排队下一个用例：reinspect pod1。
		pleg.RequestReinspect(pod1.ID)
		pleg.RequestRelist(tCtx.Logger(), pod1.ID)
		time.Sleep(2 * time.Second)

		call = runtimeMock.EXPECT().GetPods(mock.Anything, true).RunAndReturn(func(_ context.Context, _ bool) ([]*kubecontainer.Pod, error) {
			pod1.Timestamp = time.Now()
			pod2.Timestamp = time.Now()
			return []*kubecontainer.Pod{pod1, pod2}, nil
		}).NotBefore(call).Once()
		runtimeMock.EXPECT().GetPodStatus(mock.Anything, pod1).RunAndReturn(func(_ context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error) {
			assert.Equal(t, pod1, pod)
			return &kubecontainer.PodStatus{ID: pod1.ID, TimeStamp: time.Now()}, nil
		}).NotBefore(call).Once()

		pleg.workerLoopIteration(tCtx)

		p1ReinspectStatus := requireUnblocked(t, p1ReinpsectRes)
		assert.Equal(t, pod1.ID, p1ReinspectStatus.ID)
		assert.Equal(t, time.Now(), p1ReinspectStatus.TimeStamp) // 状态时间戳应被更新。

		// pod1 的 relist 请求不应再触发 relist，因为它发出早于全局 relist。
		// 从通道排空它以验证（若为它调用 GetPod，mock 会报错）。
		pleg.workerLoopIteration(tCtx)
	})
}

// getNewerThanAsync 异步调用 GetNewerThan，返回结果通道用于断言阻塞状态。
func getNewerThanAsync(t *testing.T, cache kubecontainer.ROCache, podID types.UID, minTime time.Time) <-chan *kubecontainer.PodStatus {
	resCh := make(chan *kubecontainer.PodStatus, 1)
	go func() {
		s, err := cache.GetNewerThan(podID, minTime)
		assert.NoError(t, err)
		resCh <- s
	}()
	return resCh
}

// requireBlocked 断言通道在同步等待后仍无结果，即操作处于阻塞状态。
func requireBlocked[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("Receive should have blocked, but got: %v", r)
	default:
		// OK.
	}
}

// requireUnblocked 断言通道在同步等待后已有结果，即操作已解除阻塞。
func requireUnblocked[T any](t *testing.T, ch <-chan T) (received T) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		return r
	default:
		t.Fatal("Receive should not have been blocked")
		return
	}
}
