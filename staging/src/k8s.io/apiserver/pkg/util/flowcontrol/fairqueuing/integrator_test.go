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

package fairqueuing

import (
	"math"
	"testing"
	"time"

	testclock "k8s.io/utils/clock/testing"
)

// TestIntegrator 验证积分器在假时钟下的累计语义：
// Add 计入当前区间，Reset 返回结果并清空，Set 直接改变当前值而不产生时长。
func TestIntegrator(t *testing.T) {
	now := time.Now()
	clk := testclock.NewFakeClock(now)
	igr := NewNamedIntegrator(clk, "testee")
	// 1 秒内保持值为 3：时长 1s、均值 3、偏差 0（未发生跳变）。
	igr.Add(3)
	clk.Step(time.Second)
	results := igr.GetResults()
	rToo := igr.Reset()
	if e := (IntegratorResults{Duration: time.Second.Seconds(), Average: 3, Deviation: 0, Min: 0, Max: 3}); !e.Equal(&results) {
		t.Errorf("expected %#+v, got %#+v", e, results)
	}
	// Reset 返回的结果应与 GetResults 一致，且重置不改变已返回的快照。
	if !results.Equal(&rToo) {
		t.Errorf("expected %#+v, got %#+v", results, rToo)
	}
	// Reset 后时长为 0，均值为 NaN（无观测区间），但 Min/Max 保留历史极值。
	igr.Set(2)
	results = igr.GetResults()
	if e := (IntegratorResults{Duration: 0, Average: math.NaN(), Deviation: math.NaN(), Min: 2, Max: 3}); !e.Equal(&results) {
		t.Errorf("expected %#+v, got %#+v", e, results)
	}
	// 值 2 保持 1ms，随后降为 1 再保持 1ms：均值为 (2+1)/2=1.5，偏差 0.5。
	clk.Step(time.Millisecond)
	igr.Add(-1)
	clk.Step(time.Millisecond)
	results = igr.GetResults()
	if e := (IntegratorResults{Duration: 2 * time.Millisecond.Seconds(), Average: 1.5, Deviation: 0.5, Min: 1, Max: 3}); !e.Equal(&results) {
		t.Errorf("expected %#+v, got %#+v", e, results)
	}
}
