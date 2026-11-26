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

package cacher

import (
	"testing"
	"time"

	testingclock "k8s.io/utils/clock/testing"
)

// TestTimeBudget 用假时钟验证时间预算的累积与扣减语义。
//
// 预算模型：随时间线性累积但不超过 maxBudget，取用时一次性清零。
// 测试重点覆盖三个边界——长时间空闲后的封顶、时间回拨、以及负数归还，
// 这些是真实环境中 NTP 校时和调用方误用时可能出现的场景。
func TestTimeBudget(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())

	budget := &timeBudgetImpl{
		clock:     fakeClock,
		budget:    time.Duration(0),
		maxBudget: 200 * time.Millisecond,
		refresh:   50 * time.Millisecond,
		last:      fakeClock.Now(),
	}
	if res := budget.takeAvailable(); res != time.Duration(0) {
		t.Errorf("Expected: %v, got: %v", time.Duration(0), res)
	}

	// 等待超过 maxBudget。
	nextTime := time.Now().Add(10 * time.Second)
	fakeClock.SetTime(nextTime)
	// 空闲时间超过上限时只给到 maxBudget，防止长时间空闲后突发大额预算压垮下游。
	if res := budget.takeAvailable(); res != budget.maxBudget {
		t.Errorf("Expected: %v, got: %v", budget.maxBudget, res)
	}
	// 再加两个刷新间隔，累计 2 倍 refresh 时长。
	nextTime = nextTime.Add(2 * time.Second)
	fakeClock.SetTime(nextTime)
	if res := budget.takeAvailable(); res != 2*budget.refresh {
		t.Errorf("Expected: %v, got: %v", 2*budget.refresh, res)
	}
	// 只返回一个刷新间隔，使可用预算只有一个 refresh 时长。
	// 尚未推进时间。
	budget.returnUnused(budget.refresh)
	if res := budget.takeAvailable(); res != budget.refresh {
		t.Errorf("Expected: %v, got: %v", budget.refresh, res)
	}

	// 向预算归还一个负值——此时时间还没有向前推进。
	// 负数归还应把预算压到 0 而不是负值，否则后续累积会被无谓地抵消。
	budget.returnUnused(-time.Duration(50))
	if res := budget.takeAvailable(); res != time.Duration(0) {
		t.Errorf("Expected: %v, got: %v", time.Duration(0), res)
	}
	// 处理预算为空时时钟回拨的问题。
	// 时钟回拨：预算为空时应保持为空，不能算出负的经过时间而给出额外预算。
	nextTime = nextTime.Add(-2 * time.Minute)
	fakeClock.SetTime(nextTime)
	if res := budget.takeAvailable(); res != time.Duration(0) {
		t.Errorf("Expected: %v, got: %v", time.Duration(0), res)
	}
	// 等待超过 maxBudget。
	// 验证加负值不会产生效果。
	nextTime = nextTime.Add(10 * time.Minute)
	fakeClock.SetTime(nextTime)
	if res := budget.takeAvailable(); res != budget.maxBudget {
		t.Errorf("Expected: %v, got: %v", budget.maxBudget, res)
	}

	// 处理预算中已有时间时时钟回拨的问题。
	// 预算非空时回拨时钟：仍应给出已累积的预算，且不因时间倒退而崩溃或放大。
	budget.returnUnused(10 * time.Second)
	nextTime = nextTime.Add(-2 * time.Minute)
	fakeClock.SetTime(nextTime)
	if res := budget.takeAvailable(); res != budget.maxBudget {
		t.Errorf("Expected: %v, got: %v", budget.maxBudget, res)
	}
}
