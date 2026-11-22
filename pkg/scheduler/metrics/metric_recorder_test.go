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

package metrics

import (
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"k8s.io/component-base/metrics/testutil"
	fwk "k8s.io/kube-scheduler/framework"
)

var _ MetricRecorder = &fakePodsRecorder{}

// testEntity 是 Entity 的最小测试实现，size 决定计数增量，t 决定标签映射结果。
type testEntity struct {
	size int
	t    fwk.EntityKeyType
}

// Size 返回实体的规模。
func (te *testEntity) Size() int {
	return te.size
}

// Type 返回实体的类型。
func (te *testEntity) Type() fwk.EntityKeyType {
	return te.t
}

// fakePodsRecorder 用原子计数器记录实体规模的变化，避免测试中的并发读写竞争。
type fakePodsRecorder struct {
	counter int64
}

// Add 增加实体规模对应的计数。
func (r *fakePodsRecorder) Add(entity Entity) {
	atomic.AddInt64(&r.counter, int64(entity.Size()))
}

// Remove 减去实体规模对应的计数。
func (r *fakePodsRecorder) Remove(entity Entity) {
	atomic.AddInt64(&r.counter, -int64(entity.Size()))
}

// Update 按新旧实体的规模差值调整计数。
func (r *fakePodsRecorder) Update(oldEntity, newEntity Entity) {
	atomic.AddInt64(&r.counter, int64(newEntity.Size()-oldEntity.Size()))
}

// Clear 将计数归零。
func (r *fakePodsRecorder) Clear() {
	atomic.StoreInt64(&r.counter, 0)
}

// TestAdd 验证并发 Add 的计数结果等于所有实体规模之和。
func TestAdd(t *testing.T) {
	fakeRecorder := fakePodsRecorder{}
	var wg sync.WaitGroup
	// 每个 goroutine 贡献的规模不同，最终应为 0+1+...+(loops-1)。
	loops := 100
	wg.Add(loops)
	for i := range loops {
		go func() {
			fakeRecorder.Add(&testEntity{size: i, t: "pod"})
			wg.Done()
		}()
	}
	wg.Wait()
	if fakeRecorder.counter != int64(loops*(loops-1)/2) {
		t.Errorf("Expected %v, got %v", loops, fakeRecorder.counter)
	}
}

// TestRemove 验证并发 Remove 能正确扣减计数，且不会出现负数残留。
func TestRemove(t *testing.T) {
	fakeRecorder := fakePodsRecorder{counter: 100}
	var wg sync.WaitGroup
	loops := 50
	// 每个 goroutine 扣除 2，初始 100 恰好扣到 0。
	wg.Add(loops)
	for range loops {
		go func() {
			fakeRecorder.Remove(&testEntity{size: 2, t: "podgroup"})
			wg.Done()
		}()
	}
	wg.Wait()
	if fakeRecorder.counter != int64(0) {
		t.Errorf("Expected %v, got %v", 0, fakeRecorder.counter)
	}
}

// TestClear 验证 Clear 会将并发变更后的计数归零。
func TestClear(t *testing.T) {
	fakeRecorder := fakePodsRecorder{}
	var wg sync.WaitGroup
	// 增减两组 goroutine 并发执行，验证混合操作下的最终计数。
	incLoops, decLoops := 100, 80
	wg.Add(incLoops + decLoops)
	for range incLoops {
		go func() {
			fakeRecorder.Add(&testEntity{size: 1, t: "pod"})
			fakeRecorder.Add(&testEntity{size: 2, t: "podgroup"})
			wg.Done()
		}()
	}
	for range decLoops {
		go func() {
			fakeRecorder.Remove(&testEntity{size: 1, t: "pod"})
			wg.Done()
		}()
	}
	wg.Wait()
	expected := int64(incLoops*3 - decLoops)
	if fakeRecorder.counter != expected {
		t.Errorf("Expected %v, got %v", expected, fakeRecorder.counter)
	}
	// 验证 Clear 在已有计数的情况下也能归零
	fakeRecorder.Clear()
	if fakeRecorder.counter != int64(0) {
		t.Errorf("Expected %v, got %v", 0, fakeRecorder.counter)
	}
}

// TestInFlightEventAsync 验证在途事件指标的异步聚合：重复标签的值累加，超过刷新间隔或强制刷新时将聚合值写入缓冲区。
func TestInFlightEventAsync(t *testing.T) {
	// 注册全局指标，保证记录器刷新时能拿到对应的 gauge 向量
	Register()
	r := &MetricAsyncRecorder{
		aggregatedInflightEventMetric:              map[gaugeVecMetricKey]int{},
		aggregatedInflightEventMetricLastFlushTime: time.Now(),
		aggregatedInflightEventMetricBufferCh:      make(chan *gaugeVecMetric, 100),
		// 用一小时的大间隔避免测试中自动刷新，刷新时机由测试手动控制
		interval: time.Hour,
	}

	podAddLabel := "Pod/Add"
	r.ObserveInFlightEventsAsync(podAddLabel, 10, false)
	r.ObserveInFlightEventsAsync(podAddLabel, -1, false)
	r.ObserveInFlightEventsAsync(PodPoppedInFlightEvent, 1, false)

	if d := cmp.Diff(r.aggregatedInflightEventMetric, map[gaugeVecMetricKey]int{
		{metricName: InFlightEvents.Name, labelValue: podAddLabel}:            9,
		{metricName: InFlightEvents.Name, labelValue: PodPoppedInFlightEvent}: 1,
	}, cmp.AllowUnexported(gaugeVecMetric{})); d != "" {
		t.Errorf("unexpected aggregatedInflightEventMetric: %s", d)
	}

	// 将上次刷新时间回拨一个间隔，使下一次观测触发基于间隔的刷新
	r.aggregatedInflightEventMetricLastFlushTime = time.Now().Add(-time.Hour)

	// 加上 -4 后 podAddLabel 的聚合值为 5，并刷新到 channel 中。
	r.ObserveInFlightEventsAsync(podAddLabel, -4, false)
	if len(r.aggregatedInflightEventMetric) != 0 {
		t.Errorf("aggregatedInflightEventMetric should be cleared, but got: %v", r.aggregatedInflightEventMetric)
	}

	got := []gaugeVecMetric{}
	for {
		select {
		case m := <-r.aggregatedInflightEventMetricBufferCh:
			got = append(got, *m)
			continue
		default:
		}
		// 缓冲区已排空
		break
	}

	// 排序后再比较，避免 map 遍历顺序导致测试不稳定
	sort.Slice(got, func(i, j int) bool {
		return got[i].labelValues[0] < got[j].labelValues[0]
	})

	if d := cmp.Diff(got, []gaugeVecMetric{
		{
			labelValues: []string{podAddLabel},
			valueToAdd:  5,
		},
		{
			labelValues: []string{PodPoppedInFlightEvent},
			valueToAdd:  1,
		},
	}, cmp.AllowUnexported(gaugeVecMetric{}), cmpopts.IgnoreFields(gaugeVecMetric{}, "metric")); d != "" {
		t.Errorf("unexpected metrics are sent to aggregatedInflightEventMetricBufferCh: %s", d)
	}

	// 验证 force 参数可跳过间隔限制立即刷新
	r.ObserveInFlightEventsAsync(podAddLabel, 1, true)
	if len(r.aggregatedInflightEventMetric) != 0 {
		t.Errorf("aggregatedInflightEventMetric should be force-flushed, but got: %v", r.aggregatedInflightEventMetric)
	}
}

// TestEntityToLabel 验证 EntityToLabel 对 nil、已知类型和未知类型实体的处理。
func TestEntityToLabel(t *testing.T) {
	tests := []struct {
		name      string
		entity    Entity
		wantLabel string
		wantOK    bool
	}{
		{
			name:   "nil entity",
			entity: nil,
			wantOK: false,
		},
		{
			name:      "pod entity",
			entity:    &testEntity{t: "pod"},
			wantLabel: Pod,
			wantOK:    true,
		},
		{
			name:      "podgroup entity",
			entity:    &testEntity{t: "podgroup"},
			wantLabel: PodGroup,
			wantOK:    true,
		},
		{
			name:   "unknown entity type",
			entity: &testEntity{t: "unknown"},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLabel, gotOK := EntityToLabel(tt.entity)
			if gotLabel != tt.wantLabel || gotOK != tt.wantOK {
				t.Errorf("EntityToLabel() = (%v, %v), want (%v, %v)", gotLabel, gotOK, tt.wantLabel, tt.wantOK)
			}
		})
	}
}

// TestQueuedEntitiesRecorder 验证活跃实体记录器在增、删、改、清空操作后暴露的队列指标。
func TestQueuedEntitiesRecorder(t *testing.T) {
	Register()

	recorder := NewActiveEntitiesRecorder()
	// 先清空，避免其他测试遗留的全局指标影响断言
	recorder.Clear()

	pInfo1 := &testEntity{size: 1, t: "pod"}
	pInfo2 := &testEntity{size: 5, t: "podgroup"}
	pInfo3 := &testEntity{size: 1, t: "pod"}
	updatedPInfo2 := &testEntity{size: 8, t: "podgroup"}

	tests := []struct {
		name string
		op   func()
		want string
	}{
		{
			name: "Add entity",
			op: func() {
				recorder.Add(pInfo1)
				recorder.Add(pInfo2)
				recorder.Add(pInfo3)
			},
			want: `
				# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
				# TYPE scheduler_pending_pods gauge
				scheduler_pending_pods{queue="active"} 7
				# HELP scheduler_queued_entities [ALPHA] Number of queued scheduling entities ('pod' or 'podgroup'; 'pod' stands for individual pods that are not members of any podgroup) by the queue type. 'active' means number of entities in activeQ; 'backoff' means number of entities in backoffQ; 'unschedulable' means number of entities in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable entities that the scheduler never attempted to schedule because they are gated.
				# TYPE scheduler_queued_entities gauge
				scheduler_queued_entities{queue="active",type="pod"} 2
				scheduler_queued_entities{queue="active",type="podgroup"} 1
			`,
		},
		{
			name: "Remove entity",
			op: func() {
				recorder.Remove(pInfo1)
			},
			want: `
				# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
				# TYPE scheduler_pending_pods gauge
				scheduler_pending_pods{queue="active"} 6
				# HELP scheduler_queued_entities [ALPHA] Number of queued scheduling entities ('pod' or 'podgroup'; 'pod' stands for individual pods that are not members of any podgroup) by the queue type. 'active' means number of entities in activeQ; 'backoff' means number of entities in backoffQ; 'unschedulable' means number of entities in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable entities that the scheduler never attempted to schedule because they are gated.
				# TYPE scheduler_queued_entities gauge
				scheduler_queued_entities{queue="active",type="pod"} 1
				scheduler_queued_entities{queue="active",type="podgroup"} 1
			`,
		},
		{
			name: "Update entity",
			op: func() {
				recorder.Update(pInfo2, updatedPInfo2)
			},
			want: `
				# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
				# TYPE scheduler_pending_pods gauge
				scheduler_pending_pods{queue="active"} 9
				# HELP scheduler_queued_entities [ALPHA] Number of queued scheduling entities ('pod' or 'podgroup'; 'pod' stands for individual pods that are not members of any podgroup) by the queue type. 'active' means number of entities in activeQ; 'backoff' means number of entities in backoffQ; 'unschedulable' means number of entities in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable entities that the scheduler never attempted to schedule because they are gated.
				# TYPE scheduler_queued_entities gauge
				scheduler_queued_entities{queue="active",type="pod"} 1
				scheduler_queued_entities{queue="active",type="podgroup"} 1
			`,
		},
		{
			name: "Clear",
			op: func() {
				recorder.Clear()
			},
			want: `
				# HELP scheduler_pending_pods [STABLE] Number of pending pods, by the queue type. 'active' means number of pods in activeQ; 'backoff' means number of pods in backoffQ; 'unschedulable' means number of pods in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable pods that the scheduler never attempted to schedule because they are gated; 'incomplete' means number of pods in incompletePodGroupPods; 'pending' means number of pods in pendingPodGroupPods.
				# TYPE scheduler_pending_pods gauge
				scheduler_pending_pods{queue="active"} 0
				# HELP scheduler_queued_entities [ALPHA] Number of queued scheduling entities ('pod' or 'podgroup'; 'pod' stands for individual pods that are not members of any podgroup) by the queue type. 'active' means number of entities in activeQ; 'backoff' means number of entities in backoffQ; 'unschedulable' means number of entities in unschedulableEntities that the scheduler attempted to schedule and failed; 'gated' is the number of unschedulable entities that the scheduler never attempted to schedule because they are gated.
				# TYPE scheduler_queued_entities gauge
				scheduler_queued_entities{queue="active",type="pod"} 0
				scheduler_queued_entities{queue="active",type="podgroup"} 0
			`,
		},
	}

	for _, step := range tests {
		t.Run(step.name, func(t *testing.T) {
			step.op()
			if err := testutil.GatherAndCompare(GetGather(), strings.NewReader(step.want), "scheduler_pending_pods", "scheduler_queued_entities"); err != nil {
				t.Errorf("unexpected metrics %s: %v", step.name, err)
			}
		})
	}
}

// TestNewMetricsAsyncRecorder 验证异步指标记录器的缓冲、按间隔刷新和停止行为。
func TestNewMetricsAsyncRecorder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		Register()

		bufferSize := 10
		interval := 1 * time.Second
		stop := make(chan struct{})

		r := NewMetricsAsyncRecorder(bufferSize, interval, stop)

		var buffered int
		var flushed uint64
		check := func(step string) {
			t.Helper()

			actualBuffered := len(r.bufferCh)
			if actualBuffered != buffered {
				t.Errorf("%s: expected %d elements in the buffer waiting to be flushed, got %d", step, buffered, actualBuffered)
			}

			actualFlushed, err := testutil.GetHistogramMetricCount(PluginExecutionDuration.WithLabelValues("", "", ""))
			if err != nil {
				t.Errorf("%s: error getting metric: %v", step, err)
			}
			if actualFlushed != flushed {
				t.Errorf("%s: expected metric to have count %v, got %v", step, flushed, actualFlushed)
			}
		}

		check("nothing should be buffered or flushed immediately after starting")

		synctest.Wait()
		r.ObservePluginDurationAsync("", "", "", 1)
		buffered++
		check("a metric observed immediately after flushing should only be buffered")

		time.Sleep(interval / 2)
		r.ObservePluginDurationAsync("", "", "", 1)
		buffered++
		check("a metric observed during the interval should only be buffered")

		time.Sleep(interval / 2)
		synctest.Wait()
		flushed += uint64(buffered)
		buffered = 0
		check("when the interval is reached, buffered metrics should be flushed")

		r.ObservePluginDurationAsync("", "", "", 1)
		time.Sleep(interval)
		synctest.Wait()
		flushed++
		check("metrics should be flushed at each interval")

		r.ObservePluginDurationAsync("", "", "", 1)
		time.Sleep(interval / 2)
		buffered++
		check("metrics should continue to be buffered during the last interval")

		closedAt := time.Now()
		close(stop)
		<-r.IsStoppedCh
		if actualStopDelay := time.Since(closedAt); actualStopDelay != 0 {
			t.Errorf("recorder was stopped halfway through its %v interval, expected it to stop immediately but it actually stopped after %v", interval, actualStopDelay)
		}

		check("buffered metrics are not flushed after stopping")
	})
}
