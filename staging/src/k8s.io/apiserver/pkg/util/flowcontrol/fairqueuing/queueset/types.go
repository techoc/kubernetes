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

// request is a temporary container for "requests" with additional
// tracking fields required for QueueSet functionality.
//
// 字段分两段：前半段在创建后只读，后半段受 queueSet 锁保护。这个划分是
// 该包并发正确性的核心约定——凡是在锁外读取后半段字段都是 bug。
type request struct {
	ctx context.Context

	qs *queueSet

	flowDistinguisher string
	fsName            string

	// The relevant queue.  Is nil if this request did not go through
	// a queue.
	queue *queue

	// estimated amount of work of the request
	workEstimate completedWorkEstimate

	// decision gets set to a `requestDecision` indicating what to do
	// with this request.  It gets set exactly once, when the request
	// is removed from its queue.  The value will be decisionReject,
	// decisionCancel, or decisionExecute.
	//
	// decision.Set is called with the queueSet locked.
	// decision.Get is called without the queueSet locked.
	//
	// 用 write-once promise 而不是 channel：等待方可能在任意时刻多次读取结果，
	// promise 既保证单次写入，又允许无锁读。
	decision promise.WriteOnce

	// arrivalTime is the real time when the request entered this system
	arrivalTime time.Time

	// descr1 and descr2 are not used in any logic but they appear in
	// log messages
	descr1, descr2 interface{}

	queueNoteFn fq.QueueNoteFn

	// The preceding fields are filled in at creation and not modified since;
	// the following fields may be modified later and must only be accessed while
	// holding the queueSet's lock.

	// Removes this request from its queue. If the request is not put into a
	// a queue it will be nil.
	removeFromQueueLocked removeFromFIFOFunc

	// arrivalR is R(arrivalTime).  R is, confusingly, also called "virtual time".
	// This field is meaningful only while the request is waiting in the virtual world.
	arrivalR fcrequest.SeatSeconds

	// startTime is the real time when the request began executing
	startTime time.Time

	// Indicates whether client has called Request::Wait()
	waitStarted bool
}

// completedWorkEstimate 同时保留初始工作量与最终工作量：
// 初始部分按估算的服务时长计，最终部分按实际额外延迟计，二者用于不同的公平性计算。
type completedWorkEstimate struct {
	fcrequest.WorkEstimate
	totalWork fcrequest.SeatSeconds // initial plus final work
	finalWork fcrequest.SeatSeconds // only final work
}

// queue is a sequence of requests that have arrived but not yet finished
// execution in both the real and virtual worlds.
type queue struct {
	// The requestsWaiting not yet executing in the real world are stored in a FIFO list.
	requestsWaiting fifo

	// nextDispatchR is the R progress meter reading at
	// which the next request will be dispatched in the virtual world.
	nextDispatchR fcrequest.SeatSeconds

	// requestsExecuting is the set of requests executing in the real world.
	requestsExecuting sets.Set[*request]

	// index is the position of this queue among those in its queueSet.
	index int

	// seatsInUse is the total number of "seats" currently occupied
	// by all the requests that are currently executing in this queue.
	seatsInUse int
}

// queueSum tracks the sum of initial seats, max seats, and
// totalWork from all requests in a given queue
type queueSum struct {
	// InitialSeatsSum is the sum of InitialSeats
	// associated with all requests in a given queue.
	InitialSeatsSum int

	// MaxSeatsSum is the sum of MaxSeats
	// associated with all requests in a given queue.
	MaxSeatsSum int

	// TotalWorkSum is the sum of totalWork of the waiting requests
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

// SetMapReduce is map-reduce starting from a set type in the sets package.
func SetMapReduce[Elt comparable, Result, Accumulator any](mapFn func(Elt) Result, reduceFn func(Accumulator, Result) Accumulator) func(map[Elt]sets.Empty) Accumulator {
	return func(set map[Elt]sets.Empty) Accumulator {
		var ans Accumulator
		for elt := range set {
			ans = reduceFn(ans, mapFn(elt))
		}
		return ans
	}
}

// SliceMapReduce is map-reduce starting from a slice.
func SliceMapReduce[Elt, Result, Accumulator any](mapFn func(Elt) Result, reduceFn func(Accumulator, Result) Accumulator) func([]Elt) Accumulator {
	return func(slice []Elt) Accumulator {
		var ans Accumulator
		for _, elt := range slice {
			ans = reduceFn(ans, mapFn(elt))
		}
		return ans
	}
}

// or 作为 reduce 函数用于"任一为真"的归约。
func or(x, y bool) bool { return x || y }

// append1 作为 reduce 函数用于收集元素；具名是为了满足泛型函数值传参的需要。
func append1[Elt any](slice []Elt, next Elt) []Elt { return append(slice, next) }
