/*
Copyright 2018 The Kubernetes Authors.

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

package logreduction

import (
	"testing"
	"time"
)

// 固定时间点用于驱动被替换的 nowfunc，使节流行为可重复。
var time0 = time.Unix(1000, 0)
var time1 = time.Unix(1001, 0)
var time2 = time.Unix(1012, 0)
var identicalErrorDelay = 10 * time.Second
var testCount = 0

const (
	mesg1 = "This is a message"
	mesg2 = "This is not a message"
	id1   = "Container1"
	id2   = "Container2"
)

// checkThat 断言该 (消息, ID) 应当被打印；testCount 用于在失败信息中定位用例序号。
func checkThat(t *testing.T, r *LogReduction, m, id string) {
	testCount++
	if !r.ShouldMessageBePrinted(m, id) {
		t.Errorf("Case %d failed (%s/%s should be printed)", testCount, m, id)
	}
}

// checkThatNot 断言该 (消息, ID) 应当被节流掉。
func checkThatNot(t *testing.T, r *LogReduction, m, id string) {
	testCount++
	if r.ShouldMessageBePrinted(m, id) {
		t.Errorf("Case %d failed (%s/%s should not be printed)", testCount, m, id)
	}
}

// TestLogReduction 覆盖按 ID 去重、时间窗口内抑制、窗口过后放行、
// 不同消息互不影响，以及 ClearID 后重新允许首条消息。
func TestLogReduction(t *testing.T) {
	var timeToReturn = time0
	nowfunc = func() time.Time { return timeToReturn }
	r := NewLogReduction(identicalErrorDelay)
	checkThat(t, r, mesg1, id1)    // 1
	checkThatNot(t, r, mesg1, id1) // 2
	checkThat(t, r, mesg1, id2)    // 3 不同 ID 独立计数
	checkThatNot(t, r, mesg1, id1) // 4
	timeToReturn = time1
	checkThatNot(t, r, mesg1, id1) // 5 窗口内仍然抑制
	timeToReturn = time2
	checkThat(t, r, mesg1, id1)    // 6 超过 10s 后放行
	checkThatNot(t, r, mesg1, id1) // 7
	checkThat(t, r, mesg2, id1)    // 8 不同消息不共享抑制状态
	checkThat(t, r, mesg1, id1)    // 9
	checkThat(t, r, mesg1, id2)    // 10
	r.ClearID(id1)
	checkThat(t, r, mesg1, id1)    // 11 清除状态后首条立即打印
	checkThatNot(t, r, mesg1, id2) // 12 ClearID 只影响指定 ID
}
