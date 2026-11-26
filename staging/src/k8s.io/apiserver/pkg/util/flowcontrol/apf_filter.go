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

package flowcontrol

import (
	"context"
	"strconv"
	"time"

	endpointsrequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/httplog"
	"k8s.io/apiserver/pkg/server/mux"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	"k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/eventclock"
	fqs "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/queueset"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	flowcontrolclient "k8s.io/client-go/kubernetes/typed/flowcontrol/v1"
)

// ConfigConsumerAsFieldManager 是配置消费控制器在 ObjectMeta 的
// ManagedFieldsEntry.Manager 中显示的名称；
// 固定该名称可让多个 API server 的 server-side apply 修改同一字段时正确合并。
const ConfigConsumerAsFieldManager = "api-priority-and-fairness-config-consumer-v1"

// Interface 定义 APF 过滤器与底层系统的交互契约。
type Interface interface {
	// 按给定 digest 对请求排队并调度。noteFn 收到分类结果；
	// workEstimator 只在不豁免的请求上、且 noteFn 之后调用，被调用时必须返回工作量参数。
	// 请求若入队，queueNoteFn 先以 true、再以 false 各调用一次；未入队则不调用。
	// 决定执行时 execute() 恰好调用一次，否则不调用。
	// 即使 ctx 取消或超时，execute() 运行期间 Handle 也不得返回。
	Handle(ctx context.Context,
		requestDigest RequestDigest,
		noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
		workEstimator func() fcrequest.WorkEstimate,
		queueNoteFn fq.QueueNoteFn,
		execFn func(),
	)

	// 监听主 apiserver 的配置对象并同步调整本地行为；给定 channel 关闭后停止并返回。
	Run(stopCh <-chan struct{}) error

	// 向 Web 服务器注册调试端点。
	Install(c *mux.PathRecorderMux)

	// 提供 WatchTracker 接口。
	WatchTracker

	// 由工作量估算器调用，记录某优先级下单个请求可占用的最大 seat 数。
	MaxSeatsTracker
}

// 本请求过滤器实现了 API 优先级与公平性（APF）提案：
// https://github.com/kubernetes/enhancements/blob/master/keps/sig-api-machinery/1040-priority-and-fairness/README.md

// New 构造一个 APF 过滤器实例。
func New(
	informerFactory kubeinformers.SharedInformerFactory,
	flowcontrolClient flowcontrolclient.FlowcontrolV1Interface,
	serverConcurrencyLimit int,
) Interface {
	// 生产环境使用真实时钟；测试通过 NewTestable 注入假时钟。
	clk := eventclock.Real{}
	return NewTestable(TestableConfig{
		Name:                   "Controller",
		Clock:                  clk,
		AsFieldManager:         ConfigConsumerAsFieldManager,
		FoundToDangling:        func(found bool) bool { return !found },
		InformerFactory:        informerFactory,
		FlowcontrolClient:      flowcontrolClient,
		ServerConcurrencyLimit: serverConcurrencyLimit,
		ReqsGaugeVec:           metrics.PriorityLevelConcurrencyGaugeVec,
		ExecSeatsGaugeVec:      metrics.PriorityLevelExecutionSeatsGaugeVec,
		QueueSetFactory:        fqs.NewQueueSetFactory(clk),
	})
}

// TestableConfig 汇总便于测试的参数。
type TestableConfig struct {
	// 控制器名称
	Name string

	// 用于人为延迟计时的时钟
	Clock clock.PassiveClock

	// server-side apply 使用的 field manager 名称，通常为 `ConfigConsumerAsFieldManager`。
	// 暴露为参数以便测试多个控制器竞争时使用不同值。
	AsFieldManager string

	// 把 FlowSchema 引用的 PLC 是否存在，映射为其 Status 是否应标记悬空引用。
	// 作为参数是为了测试不同服务器对该状态判断不一致的情形。
	FoundToDangling func(bool) bool

	// 构建控制器所用的 InformerFactory
	InformerFactory kubeinformers.SharedInformerFactory

	// 操作配置对象所用的客户端
	FlowcontrolClient flowcontrolclient.FlowcontrolV1Interface

	// 控制器需执行的服务器并发上限
	ServerConcurrencyLimit int

	// 按阶段和优先级统计请求数的指标向量
	ReqsGaugeVec metrics.RatioedGaugeVec

	// 统计各执行阶段占用 seat 数的指标向量
	ExecSeatsGaugeVec metrics.RatioedGaugeVec

	// 构建排队实现所用的工厂
	QueueSetFactory fq.QueueSetFactory
}

// NewTestable 额外灵活，便于测试；
// 通过注入时钟、field manager 与队列工厂，可在单测中确定性地复现调度行为。
func NewTestable(config TestableConfig) Interface {
	return newTestableController(config)
}

// Handle 是 APF 的主入口：分类请求、排队、按公平性调度，并在获得席位后执行 execFn。
// 返回前保证 execFn 已结束，调用方可安全地认为请求处理完成。
func (cfgCtlr *configController) Handle(ctx context.Context, requestDigest RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	workEstimator func() fcrequest.WorkEstimate,
	queueNoteFn fq.QueueNoteFn,
	execFn func()) {
	fs, pl, isExempt, req, startWaitingTime := cfgCtlr.startRequest(ctx, requestDigest, noteFn, workEstimator, queueNoteFn)
	queued := startWaitingTime != time.Time{}
	// req == nil 表示被拒绝（队列满/超时），此时仍需记录等待时长。
	if req == nil {
		if queued {
			observeQueueWaitTime(ctx, pl.Name, fs.Name, strconv.FormatBool(req != nil), cfgCtlr.clock.Since(startWaitingTime))
		}
		klog.V(7).Infof("Handle(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, isExempt=%v, reject", requestDigest, fs.Name, fs.Spec.DistinguisherMethod, pl.Name, isExempt)
		return
	}
	klog.V(7).Infof("Handle(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, isExempt=%v, queued=%v", requestDigest, fs.Name, fs.Spec.DistinguisherMethod, pl.Name, isExempt, queued)
	var executed bool
	idle, panicking := true, true
	defer func() {
		klog.V(7).Infof("Handle(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, isExempt=%v, queued=%v, Finish() => panicking=%v idle=%v",
			requestDigest, fs.Name, fs.Spec.DistinguisherMethod, pl.Name, isExempt, queued, panicking, idle)
		if idle {
			cfgCtlr.maybeReap(pl.Name)
		}
	}()
	idle = req.Finish(func() {
		// Finish 只在真正获得执行机会时回调；exempt 请求不经过队列。
		if queued {
			observeQueueWaitTime(ctx, pl.Name, fs.Name, strconv.FormatBool(req != nil), cfgCtlr.clock.Since(startWaitingTime))
		}
		metrics.AddDispatch(ctx, pl.Name, fs.Name)
		fqs.OnRequestDispatched(req)
		executed = true
		startExecutionTime := cfgCtlr.clock.Now()
		defer func() {
			executionTime := cfgCtlr.clock.Since(startExecutionTime)
			httplog.AddKeyValue(ctx, "apf_execution_time", executionTime)
			metrics.ObserveExecutionDuration(ctx, pl.Name, fs.Name, executionTime)
		}()
		execFn()
	})
	// 排队但未被执行（例如 ctx 取消）时也记录等待时长，便于诊断拒绝原因。
	if queued && !executed {
		observeQueueWaitTime(ctx, pl.Name, fs.Name, strconv.FormatBool(req != nil), cfgCtlr.clock.Since(startWaitingTime))
	}
	panicking = false
}

// observeQueueWaitTime 同时上报 APF 指标与请求级延迟跟踪，供 SLO 监控使用。
func observeQueueWaitTime(ctx context.Context, priorityLevelName, flowSchemaName, execute string, waitTime time.Duration) {
	metrics.ObserveWaitingDuration(ctx, priorityLevelName, flowSchemaName, execute, waitTime)
	endpointsrequest.TrackAPFQueueWaitLatency(ctx, waitTime)
}
