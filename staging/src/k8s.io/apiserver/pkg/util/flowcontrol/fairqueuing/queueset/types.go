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

package queueset

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	genericrequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/util/flowcontrol/debug"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/promise"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
)

// request 是“请求”的临时容器，带有 QueueSet 功能所需的额外跟踪字段。
// QueueSet 功能所需的跟踪字段。
//
// 字段分两段：前半段在创建后只读，后半段受 queueSet 锁保护。这个划分是
// 该包并发正确性的核心约定——凡是在锁外读取后半段字段都是 bug。
type request struct {
	ctx context.Context

	qs *queueSet

	flowDistinguisher string
	fsName            string

	// 相关的 queue；若请求未经过队列则为 nil。
	// 相关的 queue；请求未经过队列时为 nil。
	queue *queue

	// 请求工作量的估算值。
	workEstimate completedWorkEstimate

	// decision 被设置为一个 requestDecision，指示对该请求的处理方式。
	// 仅在请求从队列中移除时设置一次。取值是 decisionReject、
	// decisionCancel 或 decisionExecute 之一。
	//
	//
	// 调用 decision.Set 时必须持有 queueSet 锁。
	// 调用 decision.Get 时不持有 queueSet 锁。
	//
	// 用 write-once promise 而不是 channel：等待方可能在任意时刻多次读取结果，
	// promise 既保证单次写入，又允许无锁读。
	decision promise.WriteOnce

	// arrivalTime 是请求进入本系统的真实时间。
	arrivalTime time.Time

	// descr1 与 descr2 不参与任何逻辑，只出现在日志中。
	//
	descr1, descr2 interface{}

	queueNoteFn fq.QueueNoteFn

	// 前面的字段在创建时填充，之后不再修改；
	// 后面的字段可能被修改，只能在持有 queueSet 锁时访问。
	//

	// 把该请求从其队列中移除。若请求未进入队列，则为 nil。
	//
	removeFromQueueLocked removeFromFIFOFunc

	// arrivalR 是 R(arrivalTime)。容易混淆的是，R 也叫“虚拟时间”。
	// 该字段仅在请求于虚拟世界中等待时有意义。
	arrivalR fcrequest.SeatSeconds

	// startTime 是请求开始执行的真实时间。
	startTime time.Time

	// 表示客户端是否已调用 Request::Wait()。
	waitStarted bool
}

// completedWorkEstimate 同时保留初始工作量与最终工作量：
// 初始部分按估算的服务时长计，最终部分按实际额外延迟计，二者用于不同的公平性计算。
type completedWorkEstimate struct {
	fcrequest.WorkEstimate
	totalWork fcrequest.SeatSeconds // 初始工作量 + 最终工作量
	finalWork fcrequest.SeatSeconds // 仅最终工作量
}

// queue 是在真实与虚拟世界中都已到达但尚未执行完的请求序列。
// 在真实与虚拟世界中执行完毕。
type queue struct {
	// 尚未在真实世界执行的 requestsWaiting 保存在 FIFO 链表中。
	requestsWaiting fifo

	// nextDispatchR 是 R 进度计数器的读数，
	// 表示虚拟世界中下一个请求将被分发的位置。
	nextDispatchR fcrequest.SeatSeconds

	// requestsExecuting 是在真实世界中正在执行的请求集合。
	requestsExecuting sets.Set[*request]

	// index 是该 queue 在其 queueSet 各 queue 中的位置。
	index int

	// seatsInUse 是该队列中当前正在执行的所有请求
	// 占用的“席位”总数。
	seatsInUse int
}

// queueSum 跟踪给定队列中所有请求的初始席位数、最大席位数
// 与 totalWork 之和。
type queueSum struct {
	// InitialSeatsSum 是给定队列中所有请求的 InitialSeats 之和。
	//
	InitialSeatsSum int

	// MaxSeatsSum 是给定队列中所有请求的 MaxSeats 之和。
	//
	MaxSeatsSum int

	// TotalWorkSum 是等待中请求的 totalWork 之和。
	TotalWorkSum fcrequest.SeatSeconds
}

// totalWork 返回预估总工作量，供调度决策计算虚拟时间增量。
func (req *request) totalWork() fcrequest.SeatSeconds {
	return req.workEstimate.totalWork
}

// completeWorkEstimate 把静态估算补齐为含初始与最终两部分的完整估算。
func (qs *queueSet) completeWorkEstimate(we *fcrequest.WorkEstimate) completedWorkEstimate {
	finalWork := qs.computeFinalWork(we)
	return completedWorkEstimate{
		WorkEstimate: *we,
		totalWork:    qs.computeInitialWork(we) + finalWork,
		finalWork:    finalWork,
	}
}

// computeInitialWork 用席位乘以估算服务时长；席位代表并发占用，时长代表占用时间。
func (qs *queueSet) computeInitialWork(we *fcrequest.WorkEstimate) fcrequest.SeatSeconds {
	return fcrequest.SeatsTimesDuration(float64(we.InitialSeats), qs.estimatedServiceDuration)
}

// computeFinalWork 用最终席位乘以额外延迟，反映请求在服务尾部追加的占用。
func (qs *queueSet) computeFinalWork(we *fcrequest.WorkEstimate) fcrequest.SeatSeconds {
	return fcrequest.SeatsTimesDuration(float64(we.FinalSeats), we.AdditionalLatency)
}

// dumpLocked 生成队列的诊断快照；调用方必须持有 queueSet 锁（名字中的 Locked 即此意）。
func (q *queue) dumpLocked(includeDetails bool) debug.QueueDump {
	waitingDigest := make([]debug.RequestDump, 0, q.requestsWaiting.Length())
	q.requestsWaiting.Walk(func(r *request) bool {
		waitingDigest = append(waitingDigest, dumpRequest(includeDetails)(r))
		return true
	})
	executingDigest := SetMapReduce(dumpRequest(includeDetails), append1[debug.RequestDump])(q.requestsExecuting)

	sum := q.requestsWaiting.QueueSum()
	queueSum := debug.QueueSum{
		InitialSeatsSum: sum.InitialSeatsSum,
		MaxSeatsSum:     sum.MaxSeatsSum,
		TotalWorkSum:    sum.TotalWorkSum.String(),
	}

	return debug.QueueDump{
		NextDispatchR:     q.nextDispatchR.String(),
		Requests:          waitingDigest,
		RequestsExecuting: executingDigest,
		ExecutingRequests: q.requestsExecuting.Len(),
		SeatsInUse:        q.seatsInUse,
		QueueSum:          queueSum,
	}
}

// dumpRequest 把请求转成调试结构；includeDetails 为 true 时才附带用户与请求信息，
// 因为完整转储可能包含敏感字段，且体积可观。
func dumpRequest(includeDetails bool) func(*request) debug.RequestDump {
	return func(r *request) debug.RequestDump {
		ans := debug.RequestDump{
			MatchedFlowSchema: r.fsName,
			FlowDistinguisher: r.flowDistinguisher,
			ArriveTime:        r.arrivalTime,
			StartTime:         r.startTime,
			WorkEstimate:      r.workEstimate.WorkEstimate,
		}
		if includeDetails {
			userInfo, _ := genericrequest.UserFrom(r.ctx)
			ans.UserName = userInfo.GetName()
			requestInfo, ok := genericrequest.RequestInfoFrom(r.ctx)
			if ok {
				ans.RequestInfo = *requestInfo
			}
		}
		return ans
	}
}

// SetMapReduce 是从 sets 包的 set 类型出发的 map-reduce。
func SetMapReduce[Elt comparable, Result, Accumulator any](mapFn func(Elt) Result, reduceFn func(Accumulator, Result) Accumulator) func(map[Elt]sets.Empty) Accumulator {
	return func(set map[Elt]sets.Empty) Accumulator {
		var ans Accumulator
		for elt := range set {
			ans = reduceFn(ans, mapFn(elt))
		}
		return ans
	}
}

// SliceMapReduce 是从 slice 出发的 map-reduce。
func SliceMapReduce[Elt, Result, Accumulator any](mapFn func(Elt) Result, reduceFn func(Accumulator, Result) Accumulator) func([]Elt) Accumulator {
	return func(slice []Elt) Accumulator {
		var ans Accumulator
		for _, elt := range slice {
			ans = reduceFn(ans, mapFn(elt))
		}
		return ans
	}
}

// or 作为 reduce 函数，实现“任一为真”的归约。
func or(x, y bool) bool { return x || y }

// append1 作为 reduce 函数收集元素；具名是为了满足泛型函数值传参的需要。
func append1[Elt any](slice []Elt, next Elt) []Elt { return append(slice, next) }
