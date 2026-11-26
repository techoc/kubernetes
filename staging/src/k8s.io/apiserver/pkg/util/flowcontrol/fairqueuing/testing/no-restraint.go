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

// Package testing 提供公平排队（fair queuing）的测试替身，最重要的是
// noRestraint 实现：它不做任何排队，用于在不需要流控语义的测试中
// 替换真实 QueueSet，避免引入时间与并发的不确定性。
package testing

import (
	"context"

	"k8s.io/apiserver/pkg/util/flowcontrol/debug"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
)

// NewNoRestraintFactory 构造不做任何限流的 QueueSetFactory：
// 每个请求一到达就立即派发执行。
func NewNoRestraintFactory() fq.QueueSetFactory {
	return noRestraintFactory{}
}

// noRestraintFactory 始终返回同一个无约束 QueueSet。
type noRestraintFactory struct{}

// noRestraintCompleter 直接完成构造，忽略所有配置参数。
type noRestraintCompleter struct{}

// noRestraint 是永不排队的 QueueSet：StartRequest 立即返回可执行请求。
type noRestraint struct{}

// noRestraintRequest 的 Finish 立即执行工作函数。
type noRestraintRequest struct{}

// BeginConstruction 忽略配置与指标，直接返回 completer。
func (noRestraintFactory) BeginConstruction(fq.QueuingConfig, metrics.RatioedGaugePair, metrics.RatioedGauge, metrics.Gauge) (fq.QueueSetCompleter, error) {
	return noRestraintCompleter{}, nil
}

// Complete 返回无约束 QueueSet；dCfg 不被使用。
func (noRestraintCompleter) Complete(dCfg fq.DispatchingConfig) fq.QueueSet {
	return noRestraint{}
}

// BeginConfigChange 是空操作：无约束实现没有可调整的参数。
func (noRestraint) BeginConfigChange(qCfg fq.QueuingConfig) (fq.QueueSetCompleter, error) {
	return noRestraintCompleter{}, nil
}

// IsIdle 恒为 false：没有队列，因此“空闲”概念不适用，调用方不应据此做回收。
func (noRestraint) IsIdle() bool {
	return false
}

// StartRequest 立即返回请求且 rejected=false，表示无需等待。
func (noRestraint) StartRequest(ctx context.Context, workEstimate *fcrequest.WorkEstimate, hashValue uint64, flowDistinguisher, fsName string, descr1, descr2 interface{}, queueNoteFn fq.QueueNoteFn) (fq.Request, bool) {
	return noRestraintRequest{}, false
}

// Dump 返回空快照，因为没有排队状态可导出。
func (noRestraint) Dump(bool) debug.QueueSetDump {
	return debug.QueueSetDump{}
}

// Finish 同步执行请求体，并报告队列非空闲（与 IsIdle 保持一致）。
func (noRestraintRequest) Finish(execute func()) (idle bool) {
	execute()
	return false
}
