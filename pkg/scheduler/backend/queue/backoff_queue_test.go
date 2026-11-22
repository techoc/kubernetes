/*
Copyright 2025 The Kubernetes Authors.

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

package queue

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// TestBackoffQueue_getBackoffTime 验证退避到期时间的计算：
// 缓存中的到期时间优先，其次是失败次数或连续错误次数，并考虑禁用退避的情形。
func TestBackoffQueue_getBackoffTime(t *testing.T) {
	tests := []struct {
		name                   string
		initialBackoffDuration time.Duration
		maxBackoffDuration     time.Duration
		podInfo                *framework.QueuedPodInfo
		want                   time.Time
	}{
		{
			name:                   "no backoff",
			initialBackoffDuration: 1 * time.Second,
			maxBackoffDuration:     32 * time.Second,
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulableCount: 0,
					Timestamp:          time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			want: time.Time{},
		},
		{
			name:                   "backoff is returned from the cache",
			initialBackoffDuration: 1 * time.Second,
			maxBackoffDuration:     32 * time.Second,
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulableCount: 1,
					Timestamp:          time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
					BackoffExpiration:  time.Date(2023, 10, 1, 0, 0, 0, 1, time.UTC),
				},
			},
			want: time.Date(2023, 10, 1, 0, 0, 0, 1, time.UTC),
		},
		{
			name:                   "backoff by UnschedulableCount",
			initialBackoffDuration: 1 * time.Second,
			maxBackoffDuration:     32 * time.Second,
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulableCount: 16,
					Timestamp:          time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			want: time.Date(2023, 10, 1, 0, 0, 32, 0, time.UTC),
		},
		{
			name:                   "backoff is calculated with ConsecutiveErrorsCount",
			initialBackoffDuration: 1 * time.Second,
			maxBackoffDuration:     32 * time.Second,
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulableCount:     5,
					ConsecutiveErrorsCount: 16,
					Timestamp:              time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			want: time.Date(2023, 10, 1, 0, 0, 32, 0, time.UTC),
		},
		{
			name:                   "zero maxBackoffDuration means no backoff",
			initialBackoffDuration: 0,
			maxBackoffDuration:     0,
			podInfo: &framework.QueuedPodInfo{
				QueueingParams: framework.QueueingParams{
					UnschedulableCount: 16,
					Timestamp:          time.Date(2023, 10, 1, 0, 0, 0, 0, time.UTC),
				},
			},
			want: time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := newBackoffQueue(clock.RealClock{}, tt.initialBackoffDuration, tt.maxBackoffDuration, convertLessFn(newDefaultQueueSort()), true)
			if got := bq.getBackoffTime(tt.podInfo); got != tt.want {
				t.Errorf("backoffQueue.getBackoffTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBackoffQueue_calculateBackoffDuration 验证指数退避的时长计算，
// 包括封顶到最大退避时长、位移溢出以及大实体的放宽上限逻辑。
func TestBackoffQueue_calculateBackoffDuration(t *testing.T) {
	tests := []struct {
		name                   string
		initialBackoffDuration time.Duration
		maxBackoffDuration     time.Duration
		count                  int
		entitySize             int
		want                   time.Duration
	}{
		{
			name:                   "no backoff",
			initialBackoffDuration: 1 * time.Nanosecond,
			maxBackoffDuration:     32 * time.Nanosecond,
			count:                  0,
			entitySize:             1,
			want:                   0,
		},
		{
			name:                   "normal",
			initialBackoffDuration: 3 * time.Nanosecond,
			maxBackoffDuration:     1000 * time.Nanosecond,
			count:                  5,
			entitySize:             1,
			want:                   48 * time.Nanosecond, // 3 * 2^4 = 48
		},
		{
			name:                   "hitting max backoff duration",
			initialBackoffDuration: 1 * time.Nanosecond,
			maxBackoffDuration:     32 * time.Nanosecond,
			count:                  16,
			entitySize:             1,
			want:                   32 * time.Nanosecond,
		},
		{
			name:                   "overflow_32bit",
			initialBackoffDuration: 1 * time.Nanosecond,
			maxBackoffDuration:     math.MaxInt32 * time.Nanosecond,
			count:                  32,
			entitySize:             1,
			want:                   math.MaxInt32 * time.Nanosecond,
		},
		{
			name:                   "overflow_64bit",
			initialBackoffDuration: 1 * time.Nanosecond,
			maxBackoffDuration:     math.MaxInt64 * time.Nanosecond,
			count:                  64,
			entitySize:             1,
			want:                   math.MaxInt64 * time.Nanosecond,
		},
		{
			name:                   "larger entity size",
			initialBackoffDuration: 1 * time.Nanosecond,
			maxBackoffDuration:     32 * time.Nanosecond,
			count:                  7,
			entitySize:             10,
			want:                   64 * time.Nanosecond,
		},
		{
			name:                   "hitting max backoff duration with larger entity size",
			initialBackoffDuration: 1 * time.Nanosecond,
			maxBackoffDuration:     32 * time.Nanosecond,
			count:                  16,
			entitySize:             10,
			want:                   101 * time.Nanosecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bq := newBackoffQueue(clock.RealClock{}, tt.initialBackoffDuration, tt.maxBackoffDuration, convertLessFn(newDefaultQueueSort()), true)
			if got := bq.calculateBackoffDuration(tt.count, tt.entitySize); got != tt.want {
				t.Errorf("backoffQueue.calculateBackoffDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestBackoffQueue_popAllBackoffCompleted 验证从普通退避队列与错误退避队列中
// 弹出已完成退避的 Pod，并覆盖特性开关两种取值。
func TestBackoffQueue_popAllBackoffCompleted(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	podInfos := map[string]*framework.QueuedPodInfo{
		"pod0": {
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod0").Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(-2 * time.Second),
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		"pod1": {
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod1").Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(time.Second),
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		"pod2": {
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod2").Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:              fakeClock.Now().Add(-2 * time.Second),
				ConsecutiveErrorsCount: 1,
			},
		},
		"pod3": {
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod3").Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:              fakeClock.Now().Add(time.Second),
				ConsecutiveErrorsCount: 1,
			},
		},
	}
	tests := []struct {
		name          string
		podsInBackoff []string
		wantPods      []string
	}{
		{
			name:          "Both queues empty, no pods moved to activeQ",
			podsInBackoff: []string{},
			wantPods:      nil,
		},
		{
			name:          "Pods only in backoffQ, some pods moved to activeQ",
			podsInBackoff: []string{"pod0", "pod1"},
			wantPods:      []string{"pod0"},
		},
		{
			name:          "Pods only in errorBackoffQ, some pods moved to activeQ",
			podsInBackoff: []string{"pod2", "pod3"},
			wantPods:      []string{"pod2"},
		},
		{
			name:          "Pods in both queues, some pods moved to activeQ",
			podsInBackoff: []string{"pod0", "pod1", "pod2", "pod3"},
			wantPods:      []string{"pod0", "pod2"},
		},
		{
			name:          "Pods in both queues, all pods moved to activeQ",
			podsInBackoff: []string{"pod0", "pod2"},
			wantPods:      []string{"pod0", "pod2"},
		},
		{
			name:          "Pods in both queues, no pods moved to activeQ",
			podsInBackoff: []string{"pod1", "pod3"},
			wantPods:      nil,
		},
	}
	for _, tt := range tests {
		// 同一组输入对特性开关的两种取值都应成立，避免排序逻辑影响弹出的数量。
		for _, popFromBackoffQEnabled := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s popFromBackoffQEnabled(%v)", tt.name, popFromBackoffQEnabled), func(t *testing.T) {
				logger, _ := ktesting.NewTestContext(t)
				bq := newBackoffQueue(fakeClock, DefaultPodInitialBackoffDuration, DefaultPodMaxBackoffDuration, convertLessFn(newDefaultQueueSort()), popFromBackoffQEnabled)
				for _, podName := range tt.podsInBackoff {
					bq.add(logger, podInfos[podName], framework.EventUnscheduledPodAdd.Label(), nil)
				}
				gotPodInfos := bq.popAllBackoffCompleted(logger)
				var gotPods []string
				for _, pInfo := range gotPodInfos {
					gotPods = append(gotPods, pInfo.(*framework.QueuedPodInfo).Pod.Name)
				}
				if diff := cmp.Diff(tt.wantPods, gotPods); diff != "" {
					t.Errorf("Unexpected pods moved (-want, +got):\n%s", diff)
				}
				// 弹出数量等于入队数减去期望移出的数量，以此校验未到期 Pod 仍留在退避队列中。
				podsToStayInBackoff := len(tt.podsInBackoff) - len(tt.wantPods)
				if bq.len() != podsToStayInBackoff {
					t.Errorf("Expected %v pods to stay in backoffQ, but got: %v", podsToStayInBackoff, bq.len())
				}
			})
		}
	}
}

// TestBackoffQueueOrdering 验证同一排序窗口内的 Pod 排布：
// 开启 PopFromBackoffQ 时按优先级排序，关闭时只按退避时间排序。
func TestBackoffQueueOrdering(t *testing.T) {
	// 让假时钟与排序窗口边界对齐，避免退避到期时间落在窗口中间影响排序断言。
	fakeClock := testingclock.NewFakeClock(time.Now().Truncate(backoffQOrderingWindowDuration))
	podInfos := []*framework.QueuedPodInfo{
		{
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod0").Priority(1).Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now(),
				Attempts:             1,
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		{
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod1").Priority(1).Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(-time.Second),
				Attempts:             1,
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		{
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod2").Priority(2).Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(-2*time.Second + time.Millisecond),
				Attempts:             1,
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		{
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod3").Priority(1).Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(-2 * time.Second),
				Attempts:             1,
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		{
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod4").Priority(2).Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(-2 * time.Second),
				Attempts:             1,
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
		{
			PodInfo: &framework.PodInfo{
				Pod: st.MakePod().Name("pod5").Priority(1).Obj(),
			},
			QueueingParams: framework.QueueingParams{
				Timestamp:            fakeClock.Now().Add(-3 * time.Second),
				Attempts:             1,
				UnschedulableCount:   1,
				UnschedulablePlugins: sets.New("plugin"),
			},
		},
	}
	tests := []struct {
		name                   string
		popFromBackoffQEnabled bool
		wantPods               []string
	}{
		{
			name:                   "Pods with the same window are ordered by priority if PopFromBackoffQ is enabled",
			popFromBackoffQEnabled: true,
			wantPods:               []string{"pod5", "pod4", "pod2", "pod3"},
		},
		{
			name:                   "Pods priority doesn't matter if PopFromBackoffQ is disabled",
			popFromBackoffQEnabled: false,
			wantPods:               []string{"pod5", "pod3", "pod4", "pod2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			bq := newBackoffQueue(fakeClock, DefaultPodInitialBackoffDuration, DefaultPodMaxBackoffDuration, convertLessFn(newDefaultQueueSort()), tt.popFromBackoffQEnabled)
			for _, podInfo := range podInfos {
				bq.add(logger, podInfo, framework.EventUnscheduledPodAdd.Label(), nil)
			}
			gotPodInfos := bq.popAllBackoffCompleted(logger)
			var gotPods []string
			for _, pInfo := range gotPodInfos {
				gotPods = append(gotPods, pInfo.(*framework.QueuedPodInfo).Pod.Name)
			}
			if diff := cmp.Diff(tt.wantPods, gotPods); diff != "" {
				t.Errorf("Unexpected pods moved (-want, +got):\n%s", diff)
			}
		})
	}
}
