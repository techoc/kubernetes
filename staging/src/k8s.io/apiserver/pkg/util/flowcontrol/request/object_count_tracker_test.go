/*
Copyright 2021 The Kubernetes Authors.

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

package request

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apiserver/pkg/storage"
	testclock "k8s.io/utils/clock/testing"
)

// TestStorageObjectCountTracker 覆盖 Get 的四种结果：未记录、计数为零、
// 正常值、以及超过陈旧阈值后仍返回旧值但附带 ObjectCountStaleErr。
func TestStorageObjectCountTracker(t *testing.T) {
	tests := []struct {
		name          string
		lastUpdated   time.Duration
		skipSetting   bool
		count         int64
		errExpected   error
		countExpected int64
	}{
		{
			name:        "object count not tracked for given resource",
			skipSetting: true,
			errExpected: ObjectCountNotFoundErr,
		},
		{
			name:          "object count is zero",
			count:         0,
			countExpected: 0,
			errExpected:   nil,
		},
		{
			name:          "object count is more than zero",
			count:         799,
			countExpected: 799,
			errExpected:   nil,
		},
		{
			name:          "object count stale",
			count:         799,
			countExpected: 799,
			lastUpdated:   staleTolerationThreshold + time.Millisecond,
			errExpected:   ObjectCountStaleErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 用假时钟精确控制“上次更新时间”，无需真实等待。
			fakeClock := &testclock.FakePassiveClock{}
			tracker := &objectCountTracker{
				clock:  fakeClock,
				counts: map[string]*timestampedStats{},
			}

			key := "foo.bar.resource"
			now := time.Now()
			fakeClock.SetTime(now.Add(-test.lastUpdated))
			if !test.skipSetting {
				tracker.Set(key, storage.Stats{ObjectCount: test.count})
			}

			fakeClock.SetTime(now)
			stats, err := tracker.Get(key)
			if test.errExpected != err {
				t.Errorf("Expected error: %v, but got: %v", test.errExpected, err)
			}
			if test.countExpected != stats.ObjectCount {
				t.Errorf("Expected count: %d, but got: %d", test.countExpected, stats.ObjectCount)
			}
			// 负数计数是无效输入，应被拒绝且不污染缓存。
			if test.count <= -1 && len(tracker.counts) > 0 {
				t.Errorf("Expected the cache to be empty, but got: %d", len(tracker.counts))
			}
		})
	}
}

// TestStorageObjectCountTrackerWithPrune 验证 prune 只删除陈旧时间达到
// 阈值的条目，边界上（恰好 60 分钟）的条目仍被保留。
func TestStorageObjectCountTrackerWithPrune(t *testing.T) {
	fakeClock := &testclock.FakePassiveClock{}
	tracker := &objectCountTracker{
		clock:  fakeClock,
		counts: map[string]*timestampedStats{},
	}

	now := time.Now()
	fakeClock.SetTime(now.Add(-61 * time.Minute))
	tracker.Set("k1", storage.Stats{ObjectCount: 61})
	fakeClock.SetTime(now.Add(-60 * time.Minute))
	tracker.Set("k2", storage.Stats{ObjectCount: 60})
	// we are going to prune keys that are stale for >= 1h
	// 因此上面的 key 应被清理，下面的 key 不应被清理。
	mostRecent := now.Add(-59 * time.Minute)
	fakeClock.SetTime(mostRecent)
	tracker.Set("k3", storage.Stats{ObjectCount: 59})
	expected := map[string]*timestampedStats{
		"k3": {
			Stats:         storage.Stats{ObjectCount: 59},
			lastUpdatedAt: mostRecent,
		},
	}

	fakeClock.SetTime(now)
	if err := tracker.prune(time.Hour); err != nil {
		t.Fatalf("Expected no error, but got: %v", err)
	}

	// 预期 map 中只剩一项，因此可用 DeepEqual 比较。
	if !reflect.DeepEqual(expected, tracker.counts) {
		t.Errorf("Expected prune to remove stale entries - diff: %s", cmp.Diff(expected, tracker.counts))
	}
}
