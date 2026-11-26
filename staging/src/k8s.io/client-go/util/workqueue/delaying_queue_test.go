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

package workqueue

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	testingclock "k8s.io/utils/clock/testing"
)

// TestSimpleQueue 验证 AddAfter 的延迟语义：到期前不出现在主队列，
// 到期后恰好入队一次，且后续心跳不会重复添加。
func TestSimpleQueue(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	q := NewDelayingQueueWithConfig(DelayingQueueConfig{Clock: fakeClock})

	first := "foo"

	q.AddAfter(first, 50*time.Millisecond)
	if err := waitForWaitingQueueToFill(q); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if q.Len() != 0 {
		t.Errorf("should not have added")
	}

	fakeClock.Step(60 * time.Millisecond)

	if err := waitForAdded(q, 1); err != nil {
		t.Errorf("should have added")
	}
	item, _ := q.Get()
	q.Done(item)

	// 推进到下一次心跳之后。
	fakeClock.Step(10 * time.Second)

	err := wait.Poll(1*time.Millisecond, 30*time.Millisecond, func() (done bool, err error) {
		if q.Len() > 0 {
			return false, fmt.Errorf("added to queue")
		}

		return false, nil
	})
	if !wait.Interrupted(err) {
		t.Errorf("expected timeout, got: %v", err)
	}

	if q.Len() != 0 {
		t.Errorf("should not have added")
	}
}

// TestDeduping 验证同一 item 的多次 AddAfter 只保留最早（或更早覆盖）的到期时间，
// 到点后只入队一次，避免重复处理。
func TestDeduping(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	q := NewDelayingQueueWithConfig(DelayingQueueConfig{Clock: fakeClock})

	first := "foo"

	q.AddAfter(first, 50*time.Millisecond)
	if err := waitForWaitingQueueToFill(q); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	q.AddAfter(first, 70*time.Millisecond)
	if err := waitForWaitingQueueToFill(q); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if q.Len() != 0 {
		t.Errorf("should not have added")
	}

	// 推进过第一段阻塞，现在应能收到条目。
	fakeClock.Step(60 * time.Millisecond)
	if err := waitForAdded(q, 1); err != nil {
		t.Errorf("should have added")
	}
	item, _ := q.Get()
	q.Done(item)

	// 推进到第二次 Add 之后。
	fakeClock.Step(20 * time.Millisecond)
	if q.Len() != 0 {
		t.Errorf("should not have added")
	}

	// 再测一次，这次较早的到期时间应覆盖较晚的。
	q.AddAfter(first, 50*time.Millisecond)
	q.AddAfter(first, 30*time.Millisecond)
	if err := waitForWaitingQueueToFill(q); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if q.Len() != 0 {
		t.Errorf("should not have added")
	}

	fakeClock.Step(40 * time.Millisecond)
	if err := waitForAdded(q, 1); err != nil {
		t.Errorf("should have added")
	}
	item, _ = q.Get()
	q.Done(item)

	// 推进到第二次 Add 之后。
	fakeClock.Step(20 * time.Millisecond)
	if q.Len() != 0 {
		t.Errorf("should not have added")
	}
}

// TestAddTwoFireEarly 验证等待队列按到期时间排序：
// 后加入但更早到期的条目先出队，且中途新增的条目不会打乱已有顺序。
func TestAddTwoFireEarly(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	q := NewDelayingQueueWithConfig(DelayingQueueConfig{Clock: fakeClock})

	first := "foo"
	second := "bar"
	third := "baz"

	q.AddAfter(first, 1*time.Second)
	q.AddAfter(second, 50*time.Millisecond)
	if err := waitForWaitingQueueToFill(q); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if q.Len() != 0 {
		t.Errorf("should not have added")
	}

	fakeClock.Step(60 * time.Millisecond)

	if err := waitForAdded(q, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	item, _ := q.Get()
	if !reflect.DeepEqual(item, second) {
		t.Errorf("expected %v, got %v", second, item)
	}

	q.AddAfter(third, 2*time.Second)

	fakeClock.Step(1 * time.Second)
	if err := waitForAdded(q, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	item, _ = q.Get()
	if !reflect.DeepEqual(item, first) {
		t.Errorf("expected %v, got %v", first, item)
	}

	fakeClock.Step(2 * time.Second)
	if err := waitForAdded(q, 1); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	item, _ = q.Get()
	if !reflect.DeepEqual(item, third) {
		t.Errorf("expected %v, got %v", third, item)
	}
}

// TestCopyShifting 一次性推进时钟跨越多个到期点，
// 验证等待队列能正确重排并按 250ms/500ms/1s 的顺序依次入队。
func TestCopyShifting(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	q := NewDelayingQueueWithConfig(DelayingQueueConfig{Clock: fakeClock})

	first := "foo"
	second := "bar"
	third := "baz"

	q.AddAfter(first, 1*time.Second)
	q.AddAfter(second, 500*time.Millisecond)
	q.AddAfter(third, 250*time.Millisecond)
	if err := waitForWaitingQueueToFill(q); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	if q.Len() != 0 {
		t.Errorf("should not have added")
	}

	fakeClock.Step(2 * time.Second)

	if err := waitForAdded(q, 3); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	actualFirst, _ := q.Get()
	if !reflect.DeepEqual(actualFirst, third) {
		t.Errorf("expected %v, got %v", third, actualFirst)
	}
	actualSecond, _ := q.Get()
	if !reflect.DeepEqual(actualSecond, second) {
		t.Errorf("expected %v, got %v", second, actualSecond)
	}
	actualThird, _ := q.Get()
	if !reflect.DeepEqual(actualThird, first) {
		t.Errorf("expected %v, got %v", first, actualThird)
	}
}

// BenchmarkDelayingQueue_AddAfter 测量大量随机延迟插入后的入队与取出成本。
func BenchmarkDelayingQueue_AddAfter(b *testing.B) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	q := NewTypedDelayingQueueWithConfig(TypedDelayingQueueConfig[string]{Clock: fakeClock})

	// 添加条目。
	for n := 0; n < b.N; n++ {
		data := fmt.Sprintf("%d", n)
		q.AddAfter(data, time.Duration(rand.Int63n(int64(10*time.Minute))))
	}

	// 同时测试条目删除。
	fakeClock.Step(11 * time.Minute)
	for n := 0; n < b.N; n++ {
		_, _ = q.Get()
	}
}

// waitForAdded 轮询直到主队列长度达到 depth，用于等待后台 goroutine 完成搬运。
func waitForAdded(q DelayingInterface, depth int) error {
	return wait.Poll(1*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		if q.Len() == depth {
			return true, nil
		}

		return false, nil
	})
}

// waitForWaitingQueueToFill 等待 waitingForAddCh 被消费完，
// 确保测试推进时钟前，AddAfter 已经被等待队列的后台循环接收。
func waitForWaitingQueueToFill(q DelayingInterface) error {
	return wait.Poll(1*time.Millisecond, 10*time.Second, func() (done bool, err error) {
		if len(q.(*delayingType[any]).waitingForAddCh) == 0 {
			return true, nil
		}

		return false, nil
	})
}
