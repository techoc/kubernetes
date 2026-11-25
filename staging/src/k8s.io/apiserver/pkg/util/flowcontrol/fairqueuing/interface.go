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

package fairqueuing

import (
	"context"

	"k8s.io/apiserver/pkg/util/flowcontrol/debug"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	"k8s.io/apiserver/pkg/util/flowcontrol/request"
)

// QueueSetFactory 用于创建 QueueSet 对象。创建过程与配置更新一样分两个
// 阶段：第一阶段消费 QueuingConfig，第二阶段消费 DispatchingConfig。
// 两阶段分开的原因：第一阶段产生的错误可以在尚未承诺第二阶段的并发配额
// 之前就被发现。
//
// 两阶段构造的意义：排队参数校验失败时不必回滚已分配的分发资源。
type QueueSetFactory interface {
	// BeginConstruction 执行创建 QueueSet 的第一阶段。
	// RatioedGaugePair 观测请求数量，其执行维度只覆盖常规执行阶段。
	// 等待阶段的分母是
	// max(1, QueuingConfig.QueueLengthLimit) X max(1, QueuingConfig.DesiredNumQueues)。
	// RatioedGauge 观测在执行的所有阶段中占用的承诺（seat）数。
	// 所有比值并发 gauge 的分母稍后由 DispatchingConfig 提供。
	// Gauge 观测承诺需求量（执行中 + 排队中的 seat 数）。
	BeginConstruction(QueuingConfig, metrics.RatioedGaugePair, metrics.RatioedGauge, metrics.Gauge) (QueueSetCompleter, error)
}

// QueueSetCompleter 完成创建或重配置 QueueSet 的两步流程。
type QueueSetCompleter interface {
	// Complete 返回由给定分发配置所配置的 QueueSet。
	Complete(DispatchingConfig) QueueSet
}

// QueueSet 是某一个非豁免优先级的排队与分发功能的抽象。它涵盖了
// https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/1040-priority-and-fairness/README.md
// 中 "Assignment to a Queue"、"Queuing" 和 "Dispatching" 小节描述的功能。
// 也许有一天我们会让不同优先级之间产生关联，但至少今天还不是那天。
//
// 每个非豁免优先级对应一个 QueueSet；多个 QueueSet 之间通过公平性维度解耦。
type QueueSet interface {
	// BeginConfigChange 启动配置更新的两步流程：调用 Complete 之前不会发生任何变更。
	// 若 `C := X.BeginConstruction(q)`，则 `C.Complete(d)` 返回同一个值 `X`。
	// 若 QueuingConfig 的 DesiredNumQueues 字段为零，则不改动其余排队相关配置参数，
	// 队列继续像之前一样排空。重配置是原地生效的，不会丢弃任何仍在使用的队列
	// （除非它既不再需要又为空），避免配置热更新导致已排队请求被静默拒绝。
	BeginConfigChange(QueuingConfig) (QueueSetCompleter, error)

	// IsIdle 返回一个布尔值，表示 QueueSet 在返回的那一刻是否空闲。
	// 空闲指 QueueSet 中排队中和执行中的请求均为零。
	// 该状态只可能在 (1) StartRequest 的调用过程中、以及 (2)
	// Request::Finish 的调用过程中发生变化。后一种情况下，空闲状态
	// 只可能从 false 变为 true。
	IsIdle() bool

	// StartRequest 开始处理一个请求的流程。如果该请求需要排队且队列
	// 数量大于 1，则 StartRequest 使用给定的 hashValue 作为熵源，通过
	// shuffle-sharding 把请求分入某个队列。descr1 和 descr2 不参与任何
	// 逻辑，只出现在日志消息中。本方法总是快速返回（不会等待请求出队）。
	// 如果本方法返回的 Request 值为 nil，调用方应当拒绝该请求，且返回的
	// bool 表示 QueueSet 在返回那一刻是否空闲。否则 idle==false，
	// 客户端必须且只能调用该 Request 的 Finish 方法一次。
	//
	// 注意：StartRequest 不阻塞；真正等待发生在 Request.Finish。
	// 返回 nil Request 表示队列已满，调用方应以 429 拒绝。
	StartRequest(ctx context.Context, width *request.WorkEstimate, hashValue uint64, flowDistinguisher, fsName string, descr1, descr2 interface{}, queueNoteFn QueueNoteFn) (req Request, idle bool)

	// Dump 保存并返回该 queue-set 的瞬时内部状态。
	// 注意：dump 过程会让 queue-set 暂停处理任何请求。
	// 仅用于调试。
	Dump(includeRequestDetails bool) debug.QueueSetDump
}

// QueueNoteFn 在一个请求进入和离开队列时被调用。
type QueueNoteFn func(inQueue bool)

// Request 表示单个请求处理流程的剩余部分。
type Request interface {
	// Finish 决定是执行还是拒绝该请求，并在决定执行时调用 `execute`。
	// 返回的 `idle bool` 值表示 QueueSet 在该值被计算出来时是否空闲，
	// 但到客户端读取该值时它可能已经不再准确。
	Finish(execute func()) (idle bool)
}

// QueuingConfig 定义 QueueSet 排队方面的配置。
type QueuingConfig struct {
	// Name 用于标识一个 queue set，可携带描述其用途的信息。
	Name string

	// DesiredNumQueues 是 API 当前声明应当存在的队列数量。该值可以为非正数，
	// 此时 QueueLengthLimit 和 HandSize 被忽略。
	// 取值零表示遵循 DispatchingConfig 的 ConcurrencyLimit。
	// 取值为负表示在请求到达时立即分发（即这些请求完全豁免于限制）。
	DesiredNumQueues int

	// QueueLengthLimit 是同一时刻某个队列中可以等待的请求数上限。
	QueueLengthLimit int

	// HandSize 是 shuffle sharding 的一个参数。请求到达时，随机抽取
	// （"发一手牌"）这么多个队列，然后选取其中长度最小的一个。
	HandSize int
}

// DispatchingConfig 定义 QueueSet 分发方面的配置。
type DispatchingConfig struct {
	// ConcurrencyLimit 是该 QueueSet 同一时刻允许处于执行中的请求数上限。
	ConcurrencyLimit int

	// ConcurrencyDenominator 用于并发的相对指标。
	// 它等于 ConcurrencyLimit，除非后者为零。
	// 为 0 的 ConcurrencyLimit 不能作分母，故单独提供分母值。
	ConcurrencyDenominator int
}
