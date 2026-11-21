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

package scheduler

import (
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2/ktesting"
)

// CheckQueueEq 按顺序比较字符串切片与 TimedQueue 中的 Value，
// 只检查 lhs 覆盖的前缀，用于验证队列的出队顺序。
func CheckQueueEq(lhs []string, rhs TimedQueue) bool {
	for i := 0; i < len(lhs); i++ {
		if rhs[i].Value != lhs[i] {
			return false
		}
	}
	return true
}

// CheckSetEq 判断两个集合是否包含相同元素（双向子集），
// 因为队列内部集合的遍历顺序不确定，比较时必须忽略顺序。
func CheckSetEq(lhs, rhs sets.Set[string]) bool {
	return lhs.IsSuperset(rhs) && rhs.IsSuperset(lhs)
}

// TestUniqueQueueGet 验证 UniqueQueue 的 Get 行为：元素出队时同时从 set 中移除，
// 且队列与集合始终保持一致；用递增的假时钟让 AddedAt/ProcessAt 可预测。
func TestUniqueQueueGet(t *testing.T) {
	var tick int64
	now = func() time.Time {
		t := time.Unix(tick, 0)
		tick++
		return t
	}

	queue := UniqueQueue{
		queue: TimedQueue{},
		set:   sets.New[string](),
	}
	queue.Add(TimedValue{Value: "first", UID: "11111", AddedAt: now(), ProcessAt: now()})
	queue.Add(TimedValue{Value: "second", UID: "22222", AddedAt: now(), ProcessAt: now()})
	queue.Add(TimedValue{Value: "third", UID: "33333", AddedAt: now(), ProcessAt: now()})

	queuePattern := []string{"first", "second", "third"}
	if len(queue.queue) != len(queuePattern) {
		t.Fatalf("Queue %v should have length %d", queue.queue, len(queuePattern))
	}
	if !CheckQueueEq(queuePattern, queue.queue) {
		t.Errorf("Invalid queue. Got %v, expected %v", queue.queue, queuePattern)
	}

	setPattern := sets.New[string]("first", "second", "third")
	if len(queue.set) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, queue.set) {
		t.Errorf("Invalid map. Got %v, expected %v", queue.set, setPattern)
	}

	queue.Get()
	queuePattern = []string{"second", "third"}
	if len(queue.queue) != len(queuePattern) {
		t.Fatalf("Queue %v should have length %d", queue.queue, len(queuePattern))
	}
	if !CheckQueueEq(queuePattern, queue.queue) {
		t.Errorf("Invalid queue. Got %v, expected %v", queue.queue, queuePattern)
	}

	setPattern = sets.New[string]("second", "third")
	if len(queue.set) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, queue.set) {
		t.Errorf("Invalid map. Got %v, expected %v", queue.set, setPattern)
	}
}

// TestAddNode 验证用 AlwaysRateLimiter 添加三个节点后，
// 队列顺序和去重集合都正确记录了全部节点。
func TestAddNode(t *testing.T) {
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")

	queuePattern := []string{"first", "second", "third"}
	if len(evictor.queue.queue) != len(queuePattern) {
		t.Fatalf("Queue %v should have length %d", evictor.queue.queue, len(queuePattern))
	}
	if !CheckQueueEq(queuePattern, evictor.queue.queue) {
		t.Errorf("Invalid queue. Got %v, expected %v", evictor.queue.queue, queuePattern)
	}

	setPattern := sets.New[string]("first", "second", "third")
	if len(evictor.queue.set) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", evictor.queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, evictor.queue.set) {
		t.Errorf("Invalid map. Got %v, expected %v", evictor.queue.set, setPattern)
	}
}

// TestDelNode 验证删除队首、队中、队尾节点后，队列和集合都能同步移除对应元素，
// 确保后续 Try 不会处理已删除的节点。
func TestDelNode(t *testing.T) {
	defer func() { now = time.Now }()
	var tick int64
	now = func() time.Time {
		t := time.Unix(tick, 0)
		tick++
		return t
	}
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")
	evictor.Remove("first")

	queuePattern := []string{"second", "third"}
	if len(evictor.queue.queue) != len(queuePattern) {
		t.Fatalf("Queue %v should have length %d", evictor.queue.queue, len(queuePattern))
	}
	if !CheckQueueEq(queuePattern, evictor.queue.queue) {
		t.Errorf("Invalid queue. Got %v, expected %v", evictor.queue.queue, queuePattern)
	}

	setPattern := sets.New[string]("second", "third")
	if len(evictor.queue.set) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", evictor.queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, evictor.queue.set) {
		t.Errorf("Invalid map. Got %v, expected %v", evictor.queue.set, setPattern)
	}

	evictor = NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")
	evictor.Remove("second")

	queuePattern = []string{"first", "third"}
	if len(evictor.queue.queue) != len(queuePattern) {
		t.Fatalf("Queue %v should have length %d", evictor.queue.queue, len(queuePattern))
	}
	if !CheckQueueEq(queuePattern, evictor.queue.queue) {
		t.Errorf("Invalid queue. Got %v, expected %v", evictor.queue.queue, queuePattern)
	}

	setPattern = sets.New[string]("first", "third")
	if len(evictor.queue.set) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", evictor.queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, evictor.queue.set) {
		t.Errorf("Invalid map. Got %v, expected %v", evictor.queue.set, setPattern)
	}

	evictor = NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")
	evictor.Remove("third")

	queuePattern = []string{"first", "second"}
	if len(evictor.queue.queue) != len(queuePattern) {
		t.Fatalf("Queue %v should have length %d", evictor.queue.queue, len(queuePattern))
	}
	if !CheckQueueEq(queuePattern, evictor.queue.queue) {
		t.Errorf("Invalid queue. Got %v, expected %v", evictor.queue.queue, queuePattern)
	}

	setPattern = sets.New[string]("first", "second")
	if len(evictor.queue.set) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", evictor.queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, evictor.queue.set) {
		t.Errorf("Invalid map. Got %v, expected %v", evictor.queue.set, setPattern)
	}
}

// TestTry 验证 Try 会跳过已被 Remove 的元素：回调只收到队列中仍存在的
// first 和 third，被删除的 second 不会被处理。
func TestTry(t *testing.T) {
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")
	evictor.Remove("second")

	deletedMap := sets.New[string]()
	logger, _ := ktesting.NewTestContext(t)
	evictor.Try(logger, func(value TimedValue) (bool, time.Duration) {
		deletedMap.Insert(value.Value)
		return true, 0
	})

	setPattern := sets.New[string]("first", "third")
	if len(deletedMap) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", evictor.queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, deletedMap) {
		t.Errorf("Invalid map. Got %v, expected %v", deletedMap, setPattern)
	}
}

// TestTryOrdering 验证被重试（返回 false）的元素会按新的 ProcessAt
// 重新排队，不会阻塞其他到期元素：second 延迟 2ms 后，third 仍先被处理；
// 同时断言每个元素的 AddedAt 在反复重试后保持不变。
func TestTryOrdering(t *testing.T) {
	defer func() { now = time.Now }()
	current := time.Unix(0, 0)
	delay := 0
	// 每次调用 now 时把当前时间推进 1ms，用假时钟精确控制
	// AddedAt 与 ProcessAt，便于断言重试延迟后的顺序。
	now = func() time.Time {
		if delay > 0 {
			delay--
		} else {
			current = current.Add(time.Millisecond)
		}
		t.Logf("time %d", current.UnixNano())
		return current
	}
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")

	order := []string{}
	count := 0
	hasQueued := false
	logger, _ := ktesting.NewTestContext(t)
	evictor.Try(logger, func(value TimedValue) (bool, time.Duration) {
		count++
		t.Logf("eviction %d", count)
		if value.ProcessAt.IsZero() {
			t.Fatalf("processAt should not be zero")
		}
		switch value.Value {
		case "first":
			if !value.AddedAt.Equal(time.Unix(0, time.Millisecond.Nanoseconds())) {
				t.Fatalf("added time for %s is %v", value.Value, value.AddedAt)
			}

		case "second":
			if !value.AddedAt.Equal(time.Unix(0, 2*time.Millisecond.Nanoseconds())) {
				t.Fatalf("added time for %s is %v", value.Value, value.AddedAt)
			}
			if hasQueued {
				if !value.ProcessAt.Equal(time.Unix(0, 6*time.Millisecond.Nanoseconds())) {
					t.Fatalf("process time for %s is %v", value.Value, value.ProcessAt)
				}
				break
			}
			hasQueued = true
			delay = 1
			t.Logf("going to delay")
			return false, 2 * time.Millisecond

		case "third":
			if !value.AddedAt.Equal(time.Unix(0, 3*time.Millisecond.Nanoseconds())) {
				t.Fatalf("added time for %s is %v", value.Value, value.AddedAt)
			}
		}
		order = append(order, value.Value)
		return true, 0
	})
	if !reflect.DeepEqual(order, []string{"first", "third"}) {
		t.Fatalf("order was wrong: %v", order)
	}
	if count != 3 {
		t.Fatalf("unexpected iterations: %d", count)
	}
}

// TestTryRemovingWhileTry 验证 Try 回调执行期间元素被并发删除后，
// 该元素不会因重试请求而重新入队，最终只处理 first 和 third。
func TestTryRemovingWhileTry(t *testing.T) {
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")

	processing := make(chan struct{})
	wait := make(chan struct{})
	order := []string{}
	count := 0
	queued := false

	// 在 Try 的回调正在处理 second 时将其删除，模拟并发移除场景：
	// 即使回调返回 false 请求重试，second 也不应再次出现。
	go func() {
		<-processing
		evictor.Remove("second")
		close(wait)
	}()
	logger, _ := ktesting.NewTestContext(t)
	evictor.Try(logger, func(value TimedValue) (bool, time.Duration) {
		count++
		if value.AddedAt.IsZero() {
			t.Fatalf("added should not be zero")
		}
		if value.ProcessAt.IsZero() {
			t.Fatalf("next should not be zero")
		}
		if !queued && value.Value == "second" {
			queued = true
			close(processing)
			<-wait
			return false, time.Millisecond
		}
		order = append(order, value.Value)
		return true, 0
	})

	if !reflect.DeepEqual(order, []string{"first", "third"}) {
		t.Fatalf("order was wrong: %v", order)
	}
	if count != 3 {
		t.Fatalf("unexpected iterations: %d", count)
	}
}

// TestClear 验证 Clear 会一次性清空队列中的所有元素。
func TestClear(t *testing.T) {
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")

	evictor.Clear()

	if len(evictor.queue.queue) != 0 {
		t.Fatalf("Clear should remove all elements from the queue.")
	}
}

// TestSwapLimiter 验证 SwapLimiter 在各种 QPS 下的行为：0 切换到 Never 限速、
// 正值切换到对应限速器；QPS 未变化时不能替换，避免重置已积累的令牌状态。
func TestSwapLimiter(t *testing.T) {
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	fakeAlways := flowcontrol.NewFakeAlwaysRateLimiter()
	qps := evictor.limiter.QPS()
	if qps != fakeAlways.QPS() {
		t.Fatalf("QPS does not match create one: %v instead of %v", qps, fakeAlways.QPS())
	}

	evictor.SwapLimiter(0)
	qps = evictor.limiter.QPS()
	fakeNever := flowcontrol.NewFakeNeverRateLimiter()
	if qps != fakeNever.QPS() {
		t.Fatalf("QPS does not match create one: %v instead of %v", qps, fakeNever.QPS())
	}

	createdQPS := float32(5.5)
	evictor.SwapLimiter(createdQPS)
	qps = evictor.limiter.QPS()
	if qps != createdQPS {
		t.Fatalf("QPS does not match create one: %v instead of %v", qps, createdQPS)
	}

	prev := evictor.limiter
	evictor.SwapLimiter(createdQPS)
	if prev != evictor.limiter {
		t.Fatalf("Limiter should not be swapped if the QPS is the same.")
	}
}

// TestAddAfterTry 验证处理过的元素即使再次 Add，只要没有显式 Remove，
// 就不会被重复处理；这是 UniqueQueue 去重语义的关键保证。
func TestAddAfterTry(t *testing.T) {
	evictor := NewRateLimitedTimedQueue(flowcontrol.NewFakeAlwaysRateLimiter())
	evictor.Add("first", "11111")
	evictor.Add("second", "22222")
	evictor.Add("third", "33333")
	evictor.Remove("second")

	deletedMap := sets.New[string]()
	logger, _ := ktesting.NewTestContext(t)
	evictor.Try(logger, func(value TimedValue) (bool, time.Duration) {
		deletedMap.Insert(value.Value)
		return true, 0
	})

	setPattern := sets.New[string]("first", "third")
	if len(deletedMap) != len(setPattern) {
		t.Fatalf("Map %v should have length %d", evictor.queue.set, len(setPattern))
	}
	if !CheckSetEq(setPattern, deletedMap) {
		t.Errorf("Invalid map. Got %v, expected %v", deletedMap, setPattern)
	}

	evictor.Add("first", "11111")
	evictor.Try(logger, func(value TimedValue) (bool, time.Duration) {
		t.Errorf("We shouldn't process the same value if the explicit remove wasn't called.")
		return true, 0
	})
}
