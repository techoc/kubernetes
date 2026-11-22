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
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// TestWriteOnceSet 验证正常路径：Get 在 Set 前阻塞，Set 后所有读者拿到同一个值，
// 第二次 Set 被拒绝，且 cancel 不会覆盖已经写入的值。
func TestWriteOnceSet(t *testing.T) {
	oldTime := time.Now()
	cval := &oldTime
	ctx, cancel := context.WithCancel(context.Background())
	wr := NewWriteOnce(nil, ctx, cval)
	gots := make(chan interface{})
	goGetExpectNotYet(t, wr, gots, "Set")
	now := time.Now()
	aval := &now
	if !wr.Set(aval) {
		t.Error("Set() returned false")
	}
	expectGotValue(t, gots, aval)
	goGetAndExpect(t, wr, gots, aval)
	later := time.Now()
	bval := &later
	// 只有第一次 Set 生效；后续写入返回 false 且不改变已发布的值。
	if wr.Set(bval) {
		t.Error("second Set() returned true")
	}
	goGetAndExpect(t, wr, gots, aval)
	cancel()
	time.Sleep(time.Second) // give it a chance to misbehave
	goGetAndExpect(t, wr, gots, aval)
}

// TestWriteOnceCancel 验证取消路径：未 Set 时 cancel 会以初始值解除阻塞，
// 之后 Set 不再生效（返回 false）。
func TestWriteOnceCancel(t *testing.T) {
	oldTime := time.Now()
	cval := &oldTime
	ctx, cancel := context.WithCancel(context.Background())
	wr := NewWriteOnce(nil, ctx, cval)
	gots := make(chan interface{})
	goGetExpectNotYet(t, wr, gots, "cancel")
	cancel()
	expectGotValue(t, gots, cval)
	goGetAndExpect(t, wr, gots, cval)
	later := time.Now()
	bval := &later
	if wr.Set(bval) {
		t.Error("Set() after cancel returned true")
	}
	goGetAndExpect(t, wr, gots, cval)
}

// TestWriteOnceInitial 验证带初值的 promise：构造后立即可读，Set 永远返回 false。
func TestWriteOnceInitial(t *testing.T) {
	oldTime := time.Now()
	cval := &oldTime
	ctx, cancel := context.WithCancel(context.Background())
	now := time.Now()
	aval := &now
	wr := NewWriteOnce(aval, ctx, cval)
	gots := make(chan interface{})
	goGetAndExpect(t, wr, gots, aval)
	later := time.Now()
	bval := &later
	if wr.Set(bval) {
		t.Error("Set of initialized promise returned true")
	}
	goGetAndExpect(t, wr, gots, aval)
	cancel()
	time.Sleep(time.Second) // give it a chance to misbehave
	goGetAndExpect(t, wr, gots, aval)
}

// goGetExpectNotYet 在后台发起 Get，并断言在超时时间内没有返回值。
func goGetExpectNotYet(t *testing.T, wr WriteOnce, gots chan interface{}, trigger string) {
	go func() {
		gots <- wr.Get()
	}()
	select {
	case <-gots:
		t.Errorf("Get returned before %s", trigger)
	case <-time.After(time.Second):
		t.Log("Good: Get did not return yet")
	}
}

// goGetAndExpect 在后台发起 Get 并断言立即得到期望值。
func goGetAndExpect(t *testing.T, wr WriteOnce, gots chan interface{}, expected interface{}) {
	go func() {
		gots <- wr.Get()
	}()
	expectGotValue(t, gots, expected)
}

// expectGotValue 等待 Get 的结果并与期望值做指针比较。
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
