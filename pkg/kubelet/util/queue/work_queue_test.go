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

package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// newTestBasicWorkQueue 创建使用假时钟的测试工作队列。
func newTestBasicWorkQueue() (*basicWorkQueue, *testingclock.FakeClock) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	wq := &basicWorkQueue{
		clock: fakeClock,
		queue: make(map[types.UID]time.Time),
	}
	return wq, fakeClock
}

// compareResults 以集合方式比较预期与实际的 UID 列表。
func compareResults(t *testing.T, expected, actual []types.UID) {
	expectedSet := sets.New[string]()
	for _, u := range expected {
		expectedSet.Insert(string(u))
	}
	actualSet := sets.New[string]()
	for _, u := range actual {
		actualSet.Insert(string(u))
	}
	if !expectedSet.Equal(actualSet) {
		t.Errorf("Expected %#v, got %#v", sets.List(expectedSet), sets.List(actualSet))
	}
}

// TestGetWork 验证只有到达执行时间的条目才会被取出。
func TestGetWork(t *testing.T) {
	q, clock := newTestBasicWorkQueue()
	// 前两个条目已到期，后两个尚未到期。
	q.Enqueue(types.UID("foo1"), -1*time.Minute)
	q.Enqueue(types.UID("foo2"), -1*time.Minute)
	q.Enqueue(types.UID("foo3"), 1*time.Minute)
	q.Enqueue(types.UID("foo4"), 1*time.Minute)
	expected := []types.UID{types.UID("foo1"), types.UID("foo2")}
	compareResults(t, expected, q.GetWork())
	compareResults(t, []types.UID{}, q.GetWork())
	// 把假时钟向前拨 1 小时。
	clock.Step(time.Hour)
	expected = []types.UID{types.UID("foo3"), types.UID("foo4")}
	compareResults(t, expected, q.GetWork())
	compareResults(t, []types.UID{}, q.GetWork())
}

// TestNewBasicWorkQueue 验证构造函数返回的工作队列字段符合预期。
func TestNewBasicWorkQueue(t *testing.T) {
	tests := []struct {
		clock             clock.Clock
		expectedWorkQueue WorkQueue
	}{
		{
			clock:             clock.RealClock{},
			expectedWorkQueue: &basicWorkQueue{queue: make(map[types.UID]time.Time), clock: clock.RealClock{}},
		},
	}
	for _, test := range tests {
		workQueue := NewBasicWorkQueue(test.clock)
		assert.Equal(t, test.expectedWorkQueue, workQueue)
	}
}
