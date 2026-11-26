/*
Copyright The Kubernetes Authors.

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

package testing

import (
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/apis/example"
	testingclock "k8s.io/utils/clock/testing"
)

// TestWatchLatencyTimestampRoundTrip 验证 watch 延迟时间戳的序列化/反序列化
// 在纳秒精度下无损往返。
func TestWatchLatencyTimestampRoundTrip(t *testing.T) {
	now := time.Unix(0, time.Now().UnixNano()) // 按纳秒精度取整
	serialized := serializeTimestamp(now)
	parsed, err := parseTimestamp(serialized)
	if err != nil {
		t.Fatalf("Failed to parse timestamp: %v", err)
	}
	if !parsed.Equal(now) {
		t.Errorf("Round-trip failed: got %v, expected %v", parsed, now)
	}
}

// TestWatchLatencyTracker_RecordWriteAndHandleEvent 验证百分位计算：
// 在 100 个样本中 P99 应取自排序后的第 99 个值（50ms），而不是最大值（100ms）。
func TestWatchLatencyTracker_RecordWriteAndHandleEvent(t *testing.T) {
	initialTime := time.Date(2026, 6, 7, 9, 0, 0, 0, time.UTC)
	fakeClock := testingclock.NewFakeClock(initialTime)
	tracker := NewWatchLatencyTracker(fakeClock)

	// 记录 100 个事件：
	// - 98 个事件（1 到 98），延迟 10ms
	// - 1 个事件（第 99 个），延迟 50ms（第 99 百分位）
	// - 1 个事件（第 100 个，最大值），延迟 100ms
	for i := 1; i <= 100; i++ {
		pod := &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("pod-%d", i),
			},
		}
		tracker.RecordWrite(pod)

		// 通过假时钟精确控制每个事件的延迟，使统计结果可复现。
		var delay time.Duration
		switch i {
		case 99:
			delay = 50 * time.Millisecond
		case 100:
			delay = 100 * time.Millisecond
		default:
			delay = 10 * time.Millisecond
		}
		fakeClock.Step(delay)
		tracker.HandleEvent(pod)

		// 为下一个 pod 重置到 fake clock 基准时间，避免累计
		// 每个样本独立计时，避免延迟累加改变分布。
		fakeClock.SetTime(initialTime)
	}

	p99 := tracker.GetP99Latency()
	if p99 != 50*time.Millisecond {
		t.Errorf("Expected P99 watch latency to be 50ms, got %v (max was 100ms)", p99)
	}
}
