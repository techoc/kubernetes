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

// Package eventclock 的测试实现：Fake 时钟不会自行前进，时间只由 SetTime/Run 推进。
package eventclock

import (
	"container/heap"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	baseclocktest "k8s.io/utils/clock/testing"

	"k8s.io/apiserver/pkg/util/flowcontrol/counter"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/eventclock"
	"k8s.io/klog/v2"
)

// waitGroupCounter 是用于 GoRoutineCounter 的 wait group。
// 使用这个私有类型是为了禁止直接访问 waitGroup。
type waitGroupCounter struct {
	wg sync.WaitGroup
}

// 编译期断言：waitGroupCounter 满足
// GoRoutineCounter 的要求。
var _ counter.GoRoutineCounter = (*waitGroupCounter)(nil)

// Add 记录 goroutine 计数的变化；在 klog 高详细级别下附上调用栈片段，
// 便于在测试失败时定位“谁在没有正确计数”的问题。
func (wgc *waitGroupCounter) Add(delta int) {
	klogV := klog.V(7)
	if klogV.Enabled() {
		var pcs [10]uintptr
		nCallers := runtime.Callers(2, pcs[:])
		frames := runtime.CallersFrames(pcs[:nCallers])
		callers := make(stackExcerpt, 0, 10)
		more := frames != nil
		boundary := 1
		for i := 0; more && len(callers) < cap(callers); i++ {
			var frame runtime.Frame
			frame, more = frames.Next()
			fileParts := strings.Split(frame.File, "/")
			isMine := strings.HasSuffix(frame.File, "/fairqueuing/testing/eventclock/fake.go")
			if isMine {
				boundary = 2
			}
			callers = append(callers, stackFrame{file: fileParts[len(fileParts)-1], line: frame.Line})
			if i >= boundary && !isMine {
				break
			}
		}
		klogV.InfoS("Add", "counter", fmt.Sprintf("%p", wgc), "delta", delta, "callers", callers)
	}
	wgc.wg.Add(delta)
}

// stackExcerpt / stackFrame 是调用栈的精简表示（文件名:行号），
// 只用于日志，避免在日志中输出完整路径。
type stackExcerpt []stackFrame

type stackFrame struct {
	file string
	line int
}

var _ fmt.Stringer = stackExcerpt(nil)

func (se stackExcerpt) String() string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, sf := range se {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sf.file)
		sb.WriteString(":")
		sb.WriteString(strconv.FormatInt(int64(sf.line), 10))
	}
	sb.WriteString("]")
	return sb.String()
}

func (wgc *waitGroupCounter) Wait() {
	wgc.wg.Wait()
}

// Fake 的时间不会隐式流逝，
// 只能通过调用 SetTime 显式设置。
// 每个 Fake 关联一个 GoRoutineCounter，用于
// 跟踪相关活动。
// 对 EventAfterDuration 与 EventAfterTime 方法，
// 时钟本身统计 EventFunc 的开始与结束，
// 客户端负责统计 EventFunc 内部的挂起与
// 恢复。
// Sleep 只能从被该 GoRoutineCounter 计数的
// goroutine 中调用。
type Fake struct {
	baseclocktest.FakePassiveClock

	// waiters 是按时间排序的待执行工作堆。
	waiters     eventWaiterHeap
	waitersLock sync.RWMutex

	// clientWG 可为 nil，非 nil 时对 Run 中时间
	// 的推进施加约束。其计数归零前 Run 不会选择新的时间。
	// 才推进时间。
	clientWG *waitGroupCounter

	// fuzz 是调度时加入的噪声量。计划在 T 时刻运行的事件
	// 实际会在 [T, T+fuzz] 区间内均匀随机选择一个时间运行；
	// fuzz 非零时上界为开区间。
	fuzz time.Duration

	// rand 是模糊调度使用的随机数生成器。
	rand *rand.Rand
}

var _ eventclock.Interface = &Fake{}

type eventWaiterHeap []eventWaiter

var _ heap.Interface = (*eventWaiterHeap)(nil)

type eventWaiter struct {
	targetTime time.Time
	f          eventclock.EventFunc
}

// NewFake 构造一个新的假事件时钟。给定的 `r *rand.Rand`
// 此后不得用于任何其他用途。`r` 为 nil 时会新建一个，
// 以当前真实时间作为种子。
// clientWG 可为 nil，非 nil 时用于告知 Run
// 时间推进前必须完成的额外工作。
func NewFake(t time.Time, fuzz time.Duration, r *rand.Rand) (*Fake, counter.GoRoutineCounter) {
	grc := &waitGroupCounter{}

	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Uint64()
		r.Uint64()
		r.Uint64()
	}
	return &Fake{
		FakePassiveClock: *baseclocktest.NewFakePassiveClock(t),
		clientWG:         grc,
		fuzz:             fuzz,
		rand:             r,
	}, grc
}

// GetNextTime 返回下一个安排了工作的时间，
// 以及是否存在这样的时间。
func (fec *Fake) GetNextTime() (time.Time, bool) {
	fec.waitersLock.RLock()
	defer fec.waitersLock.RUnlock()
	if len(fec.waiters) > 0 {
		return fec.waiters[0].targetTime, true
	}
	return time.Time{}, false
}

// Run 运行所有已安排的事件及其安排的事件，
// 依此类推，直到没有事件或达到 limit（非 nil 且
// 下一个时间会超过它）。关联的 GoRoutineCounter
// 控制时间的推进：所有关联工作完成后
// 时间才会推进。
func (fec *Fake) Run(limit *time.Time) {
	for {
		fec.clientWG.Wait()
		t, ok := fec.GetNextTime()
		if !ok || limit != nil && t.After(*limit) {
			break
		}
		fec.SetTime(t)
	}
}

// SetTime 设置时间，并运行到给定时刻为止所有应启动的事件——
//
// 包括它们进一步安排的事件。
//
// 事件在独立 goroutine 中执行并计入 clientWG，因此 SetTime 会等它们全部
// 完成后才返回，保证测试观察到的状态是确定的。
func (fec *Fake) SetTime(t time.Time) {
	fec.FakePassiveClock.SetTime(t)
	for {
		foundSome := false
		func() {
			fec.waitersLock.Lock()
			defer fec.waitersLock.Unlock()
			// 这个循环是因为在给定时刻运行的事件可能安排更多
			// 在该时刻或更早运行的事件。
			// 事件不应推进时钟，但以防万一……
			now := fec.Now()
			for len(fec.waiters) > 0 && !now.Before(fec.waiters[0].targetTime) {
				ew := heap.Pop(&fec.waiters).(eventWaiter)
				fec.clientWG.Add(1)
				go func(f eventclock.EventFunc, now time.Time) {
					f(now)
					fec.clientWG.Add(-1)
				}(ew.f, now)
				foundSome = true
			}
		}()
		if !foundSome {
			break
		}
		fec.clientWG.Wait()
	}
}

// Sleep 在给定时间过去后返回。
// Sleep 只能在被 Fake 关联的 GoRoutineCounter
// 计数的 goroutine 中调用。
// 与基础 FakeClock 的 Sleep 不同，本方法不自行推进时钟，
// 而是交由其他角色（如 Run）完成。
func (fec *Fake) Sleep(duration time.Duration) {
	doneCh := make(chan struct{})
	fec.EventAfterDuration(func(time.Time) {
		fec.clientWG.Add(1)
		close(doneCh)
	}, duration)
	fec.clientWG.Add(-1)
	<-doneCh
}

// EventAfterDuration 安排给定函数在给定时间过去后
// 调用一次。
//
// 若启用了 fuzz，实际触发时间会随机后移，用于暴露对精确时序的依赖。
func (fec *Fake) EventAfterDuration(f eventclock.EventFunc, d time.Duration) {
	fec.waitersLock.Lock()
	defer fec.waitersLock.Unlock()
	now := fec.Now()
	fd := time.Duration(float32(fec.fuzz) * fec.rand.Float32())
	heap.Push(&fec.waiters, eventWaiter{targetTime: now.Add(d + fd), f: f})
}

// EventAfterTime 安排给定函数在给定时间到达后
// 调用一次。
func (fec *Fake) EventAfterTime(f eventclock.EventFunc, t time.Time) {
	fec.waitersLock.Lock()
	defer fec.waitersLock.Unlock()
	fd := time.Duration(float32(fec.fuzz) * fec.rand.Float32())
	heap.Push(&fec.waiters, eventWaiter{targetTime: t.Add(fd), f: f})
}

func (ewh eventWaiterHeap) Len() int { return len(ewh) }

func (ewh eventWaiterHeap) Less(i, j int) bool { return ewh[i].targetTime.Before(ewh[j].targetTime) }

func (ewh eventWaiterHeap) Swap(i, j int) { ewh[i], ewh[j] = ewh[j], ewh[i] }

func (ewh *eventWaiterHeap) Push(x interface{}) {
	*ewh = append(*ewh, x.(eventWaiter))
}

func (ewh *eventWaiterHeap) Pop() interface{} {
	old := *ewh
	n := len(old)
	x := old[n-1]
	*ewh = old[:n-1]
	return x
}
