/*
Copyright 2023 The Kubernetes Authors.

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

package runtime

import (
	"context"

	v1 "k8s.io/api/core/v1"
	compbasemetrics "k8s.io/component-base/metrics"
	fwk "k8s.io/kube-scheduler/framework"
)

// instrumentedFilterPlugin 在 FilterPlugin 外包裹一层调用计数。
type instrumentedFilterPlugin struct {
	fwk.FilterPlugin

	metric compbasemetrics.CounterMetric
}

// 编译期断言：确保实现了 FilterPlugin 接口。
var _ fwk.FilterPlugin = &instrumentedFilterPlugin{}

// Filter 记录一次调用后委托给被包裹的插件。
func (p *instrumentedFilterPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	p.metric.Inc()
	return p.FilterPlugin.Filter(ctx, state, pod, nodeInfo)
}

// instrumentedPreFilterPlugin 在 PreFilterPlugin 外包裹一层调用计数。
type instrumentedPreFilterPlugin struct {
	fwk.PreFilterPlugin

	metric compbasemetrics.CounterMetric
}

// 编译期断言：确保实现了 PreFilterPlugin 接口。
var _ fwk.PreFilterPlugin = &instrumentedPreFilterPlugin{}

// PreFilter 在被包裹插件未要求跳过时记录一次调用。
func (p *instrumentedPreFilterPlugin) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	result, status := p.PreFilterPlugin.PreFilter(ctx, state, pod, nodes)
	// 跳过的插件并未真正执行，计数会造成误解。
	if !status.IsSkip() {
		p.metric.Inc()
	}
	return result, status
}

// instrumentedPreScorePlugin 在 PreScorePlugin 外包裹一层调用计数。
type instrumentedPreScorePlugin struct {
	fwk.PreScorePlugin

	metric compbasemetrics.CounterMetric
}

// 编译期断言：确保实现了 PreScorePlugin 接口。
var _ fwk.PreScorePlugin = &instrumentedPreScorePlugin{}

// PreScore 在被包裹插件未要求跳过时记录一次调用。
func (p *instrumentedPreScorePlugin) PreScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	status := p.PreScorePlugin.PreScore(ctx, state, pod, nodes)
	if !status.IsSkip() {
		p.metric.Inc()
	}
	return status
}

// instrumentedScorePlugin 在 ScorePlugin 外包裹一层调用计数。
type instrumentedScorePlugin struct {
	fwk.ScorePlugin

	metric compbasemetrics.CounterMetric
}

// 编译期断言：确保实现了 ScorePlugin 接口。
var _ fwk.ScorePlugin = &instrumentedScorePlugin{}

// Score 记录一次调用后委托给被包裹的插件。
func (p *instrumentedScorePlugin) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	p.metric.Inc()
	return p.ScorePlugin.Score(ctx, state, pod, nodeInfo)
}
