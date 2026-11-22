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

// Package debug 定义 APF（API Priority and Fairness）队列的调试快照类型，
// 供 /debug/api_priority_and_fairness/dump_priority_levels 等端点序列化。
// 类型刻意保持为纯数据结构，不引用内部状态，避免调试端点影响调度行为。
//
// 注意：这些类型的 JSON 形态是调试接口的一部分，字段重命名会影响
// 依赖该输出的运维脚本。
package debug

import (
	"time"

	"k8s.io/apiserver/pkg/endpoints/request"
	flowcontrolrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
)

// QueueSetDump is an instant dump of queue-set.
// QueueSetDump 是一个优先级的队列集合在某一时刻的快照。
type QueueSetDump struct {
	Queues                     []QueueDump
	QueuelessExecutingRequests []RequestDump
	Waiting                    int
	Executing                  int
	SeatsInUse                 int
	SeatsWaiting               int
	Dispatched                 int
	Rejected                   int
	Timedout                   int
	Cancelled                  int
}

// QueueDump is an instant dump of one queue in a queue-set.
// QueueDump 是单个队列快照；Requests 只包含仍在等待的请求，
// 正在执行的请求单独放在 RequestsExecuting 中。
type QueueDump struct {
	QueueSum          QueueSum
	Requests          []RequestDump // just the waiting ones
	RequestsExecuting []RequestDump
	NextDispatchR     string
	ExecutingRequests int
	SeatsInUse        int
}

// QueueSum 汇总队列的座位与工作量统计。
type QueueSum struct {
	InitialSeatsSum int
	MaxSeatsSum     int
	// TotalWorkSum 是字符串形式，因为工作量表示可能超出 JSON 数值精度。
	TotalWorkSum string
}

// RequestDump is an instant dump of one requests pending in the queue.
// RequestDump 描述一个等待中（或执行中）请求的快照，
// 包含其匹配的 FlowSchema、到达/开始时间与工作量估算。
type RequestDump struct {
	MatchedFlowSchema string
	FlowDistinguisher string
	ArriveTime        time.Time
	StartTime         time.Time
	WorkEstimate      flowcontrolrequest.WorkEstimate
	// request details
	UserName    string
	RequestInfo request.RequestInfo
}
