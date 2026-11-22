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

package etcd3

import (
	"bytes"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	testingclock "k8s.io/utils/clock/testing"
)

// TestBlockLogger 验证阻塞日志器“间隔 + 阈值”双重触发条件以及聚合行为：
// 只有当累计等待超过阈值且距上次记录已过 interval 时才会输出一条日志，
// 输出中的 eventCount/timeWaiting 是聚合值而非单次事件。
func TestBlockLogger(t *testing.T) {
	groupResource := schema.GroupResource{Group: "test", Resource: "pods"}
	msg := "Test message"

	tests := []struct {
		name          string
		interval      time.Duration
		threshold     time.Duration
		waits         []time.Duration
		timePassed    time.Duration
		expectLog     bool
		expectedCount int
		expectedWait  time.Duration
	}{
		{
			// 间隔已过且累计等待超阈值：应当记录，计数包含触发本次 recordWait(0)。
			name:          "log when interval passed and threshold exceeded",
			interval:      1 * time.Second,
			threshold:     100 * time.Millisecond,
			waits:         []time.Duration{200 * time.Millisecond},
			timePassed:    2 * time.Second,
			expectLog:     true,
			expectedCount: 2, // 1 from waits + 1 from the triggering recordWait
			expectedWait:  200 * time.Millisecond,
		},
		{
			// 间隔未到：即便累计等待超阈值也不应记录，避免日志刷屏。
			name:       "do not log when interval not passed",
			interval:   5 * time.Second,
			threshold:  100 * time.Millisecond,
			waits:      []time.Duration{200 * time.Millisecond},
			timePassed: 1 * time.Second,
			expectLog:  false,
		},
		{
			// 单次等待未超阈值：视为正常抖动，不记录。
			name:       "do not log when threshold not exceeded",
			interval:   1 * time.Second,
			threshold:  500 * time.Millisecond,
			waits:      []time.Duration{200 * time.Millisecond},
			timePassed: 2 * time.Second,
			expectLog:  false,
		},
		{
			// 多次事件应聚合成一条日志，计数与时长累加。
			name:          "aggregate multiple events",
			interval:      1 * time.Second,
			threshold:     100 * time.Millisecond,
			waits:         []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
			timePassed:    2 * time.Second,
			expectLog:     true,
			expectedCount: 3, // 2 from waits + 1 from the triggering recordWait
			expectedWait:  300 * time.Millisecond,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 把 klog 重定向到 buffer，以便断言日志内容而不污染测试输出。
			var buf bytes.Buffer
			flags := &flag.FlagSet{}
			klog.InitFlags(flags)
			if err := flags.Set("v", "3"); err != nil {
				t.Fatal(err)
			}
			klog.LogToStderr(false)
			klog.SetOutput(&buf)

			// 使用假时钟让间隔判断完全确定，避免依赖真实时间导致偶发失败。
			currentTime := time.Unix(0, 0)
			fakeClock := testingclock.NewFakeClock(currentTime)
			logger := newBlockLogger(tc.interval, tc.threshold, msg, "Pod", groupResource, fakeClock)

			// Record waits
			for _, w := range tc.waits {
				logger.recordWait(w)
			}

			// Simulate time passing
			fakeClock.Step(tc.timePassed)

			// Trigger another recordWait to check if it logs
			logger.recordWait(0)

			klog.Flush()
			logOutput := buf.String()
			logged := logOutput != ""

			if logged != tc.expectLog {
				t.Errorf("expected logged=%v, got %v. Output: %s", tc.expectLog, logged, logOutput)
			}

			if tc.expectLog {
				if !strings.Contains(logOutput, msg) {
					t.Errorf("expected log to contain message %q, got %q", msg, logOutput)
				}

				// Check for count and wait duration in the log output.
				countStr := fmt.Sprintf("eventCount=%d", tc.expectedCount)
				if !strings.Contains(logOutput, countStr) {
					t.Errorf("expected log to contain %q, got %q", countStr, logOutput)
				}

				waitStr := fmt.Sprintf("timeWaiting=%q", tc.expectedWait.String())
				if !strings.Contains(logOutput, waitStr) {
					t.Errorf("expected log to contain wait duration %q, got %q", waitStr, logOutput)
				}
			}
		})
	}
}
