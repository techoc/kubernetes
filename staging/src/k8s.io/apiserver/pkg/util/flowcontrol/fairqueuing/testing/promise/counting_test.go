/*
Copyright 2019 The Kubernetes Authors.

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

package promise

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/util/flowcontrol/counter"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/promise"
	testeventclock "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/testing/eventclock"
	"k8s.io/klog/v2"
)

// TestMain 初始化 klog 标志，使测试可以用 -v 控制日志输出。
func TestMain(m *testing.M) {
	klog.InitFlags(nil)
	os.Exit(m.Run())
}

// TestCountingWriteOnceSet 验证 Set 是"一次性"的：首次写入生效并唤醒等待者，
// 后续写入返回 false 且不改变已提交的值。
//
// 计数器的加减必须精确配对，否则上层等待"所有 goroutine 结束"会永远挂起；
// 测试里用 counter.Add(1) 显式补偿 doneCh 接收方，正是为了维持这一不变量。
func TestCountingWriteOnceSet(t *testing.T) {
	oldTime := time.Now()
	cval := &oldTime
	doneCh := make(chan struct{})
	now := time.Now()
	clock, counter := testeventclock.NewFake(now, 0, nil)
	var lock sync.Mutex
	wr := NewCountingWriteOnce(counter, &lock, nil, doneCh, cval)
	gots := make(chan interface{}, 1)
	// 先确认未 Set 前 Get 会阻塞。
	goGetExpectNotYet(t, clock, counter, wr, gots, "Set")
	aval := &now
	func() {
		lock.Lock()
		defer lock.Unlock()
		if !wr.Set(aval) {
			t.Error("Set() returned false")
		}
	}()
	clock.Run(nil)
	expectGotValue(t, gots, aval)
	goGetAndExpect(t, clock, counter, wr, gots, aval)
	later := time.Now()
	bval := &later
	func() {
		lock.Lock()
		defer lock.Unlock()
		// 第二次 Set 必须被拒绝，保证值是写一次的。
		if wr.Set(bval) {
			t.Error("second Set() returned true")
		}
	}()
	goGetAndExpect(t, clock, counter, wr, gots, aval)
	counter.Add(1) // account for unblocking the receive on doneCh
	close(doneCh)
	time.Sleep(time.Second) // give it a chance to misbehave
	// doneCh 关闭后已提交的值仍应保持不变。
	goGetAndExpect(t, clock, counter, wr, gots, aval)
}

// TestCountingWriteOnceCancel 验证取消信号使等待中的 Get 以初始值返回，
// 且取消后的 Set 不再生效——取消是终态，避免晚到的写入"复活"已放弃的请求。
func TestCountingWriteOnceCancel(t *testing.T) {
	oldTime := time.Now()
	cval := &oldTime
	clock, counter := testeventclock.NewFake(oldTime, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var lock sync.Mutex
	wr := NewCountingWriteOnce(counter, &lock, nil, ctx.Done(), cval)
	gots := make(chan interface{}, 1)
	goGetExpectNotYet(t, clock, counter, wr, gots, "cancel")
	counter.Add(1) // account for unblocking the receive on doneCh
	cancel()
	clock.Run(nil)
	expectGotValue(t, gots, cval)
	goGetAndExpect(t, clock, counter, wr, gots, cval)
	later := time.Now()
	bval := &later
	func() {
		lock.Lock()
		defer lock.Unlock()
		if wr.Set(bval) {
			t.Error("Set() after cancel returned true")
		}
	}()
	goGetAndExpect(t, clock, counter, wr, gots, cval)
}

// TestCountingWriteOnceInitial 验证带初值构造时，Get 立即返回初值，
// Set 始终返回 false，且取消不会覆盖已存在的值。
func TestCountingWriteOnceInitial(t *testing.T) {
	oldTime := time.Now()
	cval := &oldTime
	clock, counter := testeventclock.NewFake(oldTime, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var lock sync.Mutex
	now := time.Now()
	aval := &now
	wr := NewCountingWriteOnce(counter, &lock, aval, ctx.Done(), cval)
	gots := make(chan interface{}, 1)
	goGetAndExpect(t, clock, counter, wr, gots, aval)
	goGetAndExpect(t, clock, counter, wr, gots, aval) // check that a set value stays set
	later := time.Now()
	bval := &later
	func() {
		lock.Lock()
		defer lock.Unlock()
		if wr.Set(bval) {
			t.Error("Set of initialized promise returned true")
		}
	}()
	goGetAndExpect(t, clock, counter, wr, gots, aval)
	counter.Add(1) // account for unblocking receive on doneCh
	cancel()
	time.Sleep(time.Second) // give it a chance to misbehave
	goGetAndExpect(t, clock, counter, wr, gots, aval)
}

// goGetExpectNotYet 在后台发起 Get，并断言它在触发条件（Set 或 cancel）发生前不返回。
// 用一秒超时区分"阻塞"与"返回"，是这类同步原语测试的常见手法。
func goGetExpectNotYet(t *testing.T, clk *testeventclock.Fake, grc counter.GoRoutineCounter, wr promise.WriteOnce, gots chan interface{}, trigger string) {
	grc.Add(1) // count the following goroutine
	go func() {
		defer grc.Add(-1) // count completion of this goroutine
		gots <- wr.Get()
	}()
	clk.Run(nil)
	select {
	case <-gots:
		t.Errorf("Get returned before %s", trigger)
	case <-time.After(time.Second):
		t.Log("Good: Get did not return yet")
	}

}

// goGetAndExpect 在后台调用 Get，运行假时钟并断言取到期望值。
func goGetAndExpect(t *testing.T, clk *testeventclock.Fake, grc counter.GoRoutineCounter, wr promise.WriteOnce, gots chan interface{}, expected interface{}) {
	grc.Add(1)
	go func() {
		defer grc.Add(-1)
		gots <- wr.Get()
	}()
	clk.Run(nil)
	expectGotValue(t, gots, expected)
}

// expectGotValue 从 channel 取一个值并比较；超时即视为 Get 未被唤醒。
func expectGotValue(t *testing.T, gots <-chan interface{}, expected interface{}) {
	select {
	case gotVal := <-gots:
		t.Logf("Got %v", gotVal)
		if gotVal != expected {
			t.Errorf("Get returned %v, expected: %v", gotVal, expected)
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Error("Get did not return")
	}
}
