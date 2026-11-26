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

// 本文件验证可测试的假事件时钟：事件按预定时间顺序触发，
// Run 会推进虚拟时间直到没有待触发事件，SetTime 只触发已到期的事件。

package eventclock

import (
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/eventclock"
)

// TestableEventClock 在 eventclock.Interface 之上增加测试所需的控制方法。
type TestableEventClock interface {
	eventclock.Interface
	SetTime(time.Time)
	Run(*time.Time)
}

// exerciseTestableEventClock 对事件时钟做行为验证：
//  1. 注册一批相对/绝对延迟的事件，Run 后全部按时间顺序触发；
//  2. SetTime 只推进到指定时刻，仅触发已到期事件。
//
// fuzz 表示允许的额外延迟误差，用于容忍实现内部的批处理抖动。
func exerciseTestableEventClock(t *testing.T, ec TestableEventClock, fuzz time.Duration) {
	exerciseSettablePassiveClock(t, ec)
	var numDone int32
	now := ec.Now()
	strictable := true
	const batchSize = 100
	// 缓冲区略大于事件总数，避免回调阻塞；同时用通道接收顺序来断言触发次序。
	times := make(chan time.Time, batchSize+1)
	try := func(abs, strict bool, d time.Duration) {
		f := func(u time.Time) {
			realD := ec.Since(now)
			atomic.AddInt32(&numDone, 1)
			times <- u
			// 回调不得早于请求的延迟；严格模式下也不得超出延迟 + 误差。
			if realD < d || strict && strictable && realD > d+fuzz {
				t.Errorf("Asked for %v, got %v", d, realD)
			}
		}
		if abs {
			ec.EventAfterTime(f, now.Add(d))
		} else {
			ec.EventAfterDuration(f, d)
		}
	}
	try(true, true, time.Minute)
	for i := 0; i < batchSize; i++ {
		d := time.Duration(rand.Intn(30)-3) * time.Second
		// 偶数为绝对时间、奇数为相对时长，覆盖两种注册方式。
		try(i%2 == 0, d >= 0, d)
	}
	ec.Run(nil)
	if numDone != batchSize+1 {
		t.Errorf("Got only %v events", numDone)
	}
	// 负延迟事件应立即触发，因此允许其时间戳早于起点，但整体必须单调不减。
	lastTime := now.Add(-3 * time.Second)
	for i := 0; i <= batchSize; i++ {
		nextTime := <-times
		if nextTime.Before(lastTime) {
			t.Errorf("Got %s after %s", nextTime, lastTime)
		}
	}
	endTime := ec.Now()
	dx := endTime.Sub(now)
	if dx > time.Minute+fuzz {
		t.Errorf("Run started at %#+v, ended at %#+v, dx=%d", now, endTime, dx)
	}
	now = endTime
	var shouldRun int32
	// 第二轮不再要求严格上限：SetTime 触发的回调可能在同一虚拟时刻批量执行。
	strictable = false
	for i := 0; i < batchSize; i++ {
		d := time.Duration(rand.Intn(30)-3) * time.Second
		try(i%2 == 0, d >= 0, d)
		if d <= 12*time.Second {
			shouldRun++
		}
	}
	ec.SetTime(now.Add(13*time.Second - 1))
	if numDone != batchSize+1+shouldRun {
		t.Errorf("Expected %v, but %v ran", shouldRun, numDone-batchSize-1)
	}
	lastTime = now.Add(-3 * time.Second)
	for i := int32(0); i < shouldRun; i++ {
		nextTime := <-times
		if nextTime.Before(lastTime) {
			t.Errorf("Got %s after %s", nextTime, lastTime)
		}
		lastTime = nextTime
	}
}

// 从 baseclocktest 复制而来，因为它不公开
// 验证 SetTime/Now/Since 的基本一致性：SetTime 之后 Now 返回设定值，Since 基于设定值计算。
func exerciseSettablePassiveClock(t *testing.T, pc TestableEventClock) {
	t1 := time.Now()
	t2 := t1.Add(time.Hour)
	pc.SetTime(t1)
	tx := pc.Now()
	if tx != t1 {
		t.Errorf("SetTime(%#+v); Now() => %#+v", t1, tx)
	}
	dx := pc.Since(t1)
	if dx != 0 {
		t.Errorf("Since() => %v", dx)
	}
	pc.SetTime(t2)
	dx = pc.Since(t1)
	if dx != time.Hour {
		t.Errorf("Since() => %v", dx)
	}
	tx = pc.Now()
	if tx != t2 {
		t.Errorf("Now() => %#+v", tx)
	}
}

// TestFake 分别在零误差与 1 秒误差两种配置下运行同一组行为验证。
func TestFake(t *testing.T) {
	startTime := time.Now()
	fec, _ := NewFake(startTime, 0, nil)
	exerciseTestableEventClock(t, fec, 0)
	fec, _ = NewFake(startTime, time.Second, nil)
	exerciseTestableEventClock(t, fec, time.Second)
}
