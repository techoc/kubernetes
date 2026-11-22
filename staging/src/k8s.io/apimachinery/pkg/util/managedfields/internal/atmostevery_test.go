/*
Copyright 2020 The Kubernetes Authors.

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

package internal_test

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/managedfields/internal"
)

// TestAtMostEvery 用 1 秒窗口、2ms tick 的高频调用验证限流上界：
// 调用次数不得超过 duration/delay+1（允许边界处的额外一次）。
// 这是概率性测试，只检查上界，不检查下界，避免在慢机器上抖动。
func TestAtMostEvery(t *testing.T) {
	duration := time.Second
	delay := 179 * time.Millisecond
	atMostEvery := internal.NewAtMostEvery(delay)
	count := 0
	exit := time.NewTicker(duration)
	tick := time.NewTicker(2 * time.Millisecond)
	defer exit.Stop()
	defer tick.Stop()

	done := false
	for !done {
		select {
		case <-exit.C:
			// 1 秒到点，停止。
			done = true
		case <-tick.C:
			// 每 2ms 尝试一次，实际执行频率受 atMostEvery 限制。
			atMostEvery.Do(func() {
				count++
			})
		}
	}

	// 上界放宽一次：第一个调用可能立即放行，也可能在窗口边界再多一次。
	if expected := int(duration/delay) + 1; count > expected {
		t.Fatalf("Function called %d times, should have been called less than or equal to %d times", count, expected)
	}
}
