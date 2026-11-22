/*
Copyright 2020 The Kubernetes Authors.

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

package framework

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

// ErrReasonFake 是伪插件 Filter 失败时使用的假原因。
const ErrReasonFake = "Nodes failed the fake plugin"

// FalseFilterPlugin 是一个 Filter 总是返回 Unschedulable 的测试插件。
type FalseFilterPlugin struct{}

// Name 返回插件名。
func (pl *FalseFilterPlugin) Name() string {
	return "FalseFilter"
}

// Filter 在 filter 扩展点调用。
func (pl *FalseFilterPlugin) Filter(_ context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Unschedulable, ErrReasonFake)
}

// NewFalseFilterPlugin 创建并返回一个 FalseFilterPlugin。
func NewFalseFilterPlugin(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &FalseFilterPlugin{}, nil
}

// TrueFilterPlugin 是一个 Filter 总是返回 Success 的测试插件。
type TrueFilterPlugin struct{}

// Name 返回插件名。
func (pl *TrueFilterPlugin) Name() string {
	return "TrueFilter"
}

// Filter 在 filter 扩展点调用。
func (pl *TrueFilterPlugin) Filter(_ context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	return nil
}

// NewTrueFilterPlugin 创建并返回一个 TrueFilterPlugin。
func NewTrueFilterPlugin(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &TrueFilterPlugin{}, nil
}

// FakePreFilterAndFilterPlugin 组合了 FakePreFilterPlugin 与 FakeFilterPlugin，
// 用于同时测试两个扩展点。
type FakePreFilterAndFilterPlugin struct {
	*FakePreFilterPlugin
	*FakeFilterPlugin
}

// Name 返回插件名。
func (pl FakePreFilterAndFilterPlugin) Name() string {
	return "FakePreFilterAndFilterPlugin"
}

// FakeFilterPlugin 记录 Filter() 被调用的次数，并按 failedNodeReturnCodeMap
// 对指定节点返回预设的状态码。
type FakeFilterPlugin struct {
	NumFilterCalled         int32
	FailedNodeReturnCodeMap map[string]fwk.Code
}

// Name 返回插件名。
func (pl *FakeFilterPlugin) Name() string {
	return "FakeFilter"
}

// Filter 在 filter 扩展点调用。
func (pl *FakeFilterPlugin) Filter(_ context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	atomic.AddInt32(&pl.NumFilterCalled, 1)

	if returnCode, ok := pl.FailedNodeReturnCodeMap[nodeInfo.Node().Name]; ok {
		return fwk.NewStatus(returnCode, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}

	return nil
}

// NewFakeFilterPlugin 返回一个创建 FakeFilterPlugin 的工厂函数。
func NewFakeFilterPlugin(failedNodeReturnCodeMap map[string]fwk.Code) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakeFilterPlugin{
			FailedNodeReturnCodeMap: failedNodeReturnCodeMap,
		}, nil
	}
}

// MatchFilterPlugin 在 Pod 与节点同名时返回 Success，否则返回 Unschedulable。
type MatchFilterPlugin struct{}

// Name 返回插件名。
func (pl *MatchFilterPlugin) Name() string {
	return "MatchFilter"
}

// Filter 在 filter 扩展点调用。
func (pl *MatchFilterPlugin) Filter(_ context.Context, _ fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	node := nodeInfo.Node()
	if node == nil {
		return fwk.NewStatus(fwk.Error, "node not found")
	}
	if pod.Name == node.Name {
		return nil
	}
	return fwk.NewStatus(fwk.Unschedulable, ErrReasonFake)
}

// NewMatchFilterPlugin 创建并返回一个 MatchFilterPlugin。
func NewMatchFilterPlugin(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	return &MatchFilterPlugin{}, nil
}

// FakePreFilterPlugin 是用于测试 PreFilter 扩展点的插件，直接返回预置的结果与状态。
type FakePreFilterPlugin struct {
	Result *fwk.PreFilterResult
	Status *fwk.Status
	name   string
}

// Name 返回插件名。
func (pl *FakePreFilterPlugin) Name() string {
	return pl.name
}

// PreFilter 在 PreFilter 扩展点调用。
func (pl *FakePreFilterPlugin) PreFilter(_ context.Context, _ fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	return pl.Result, pl.Status
}

// PreFilterExtensions 返回 nil：该插件不提供扩展。
func (pl *FakePreFilterPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// NewFakePreFilterPlugin 返回一个创建 FakePreFilterPlugin 的工厂函数。
func NewFakePreFilterPlugin(name string, result *fwk.PreFilterResult, status *fwk.Status) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakePreFilterPlugin{
			Result: result,
			Status: status,
			name:   name,
		}, nil
	}
}

// FakeReservePlugin 是用于测试 Reserve 扩展点的插件。
type FakeReservePlugin struct {
	Status *fwk.Status
}

// Name 返回插件名。
func (pl *FakeReservePlugin) Name() string {
	return "FakeReserve"
}

// Reserve 在 Reserve 扩展点调用。
func (pl *FakeReservePlugin) Reserve(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ string) *fwk.Status {
	return pl.Status
}

// Unreserve 在 Unreserve 扩展点调用。
func (pl *FakeReservePlugin) Unreserve(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ string) {
}

// NewFakeReservePlugin 返回一个创建 FakeReservePlugin 的工厂函数。
func NewFakeReservePlugin(status *fwk.Status) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakeReservePlugin{
			Status: status,
		}, nil
	}
}

// FakePostFilterPlugin 是用于测试 PostFilter 扩展点的插件。
type FakePostFilterPlugin struct {
	Result *fwk.PostFilterResult
	Status *fwk.Status
}

// Name 返回插件名。
func (pl *FakePostFilterPlugin) Name() string {
	return "FakePostFilter"
}

// PostFilter 在 PostFilter 扩展点调用。
func (pl *FakePostFilterPlugin) PostFilter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	return pl.Result, pl.Status
}

// NewFakePostFilterPlugin 返回一个创建 FakePostFilterPlugin 的工厂函数。
func NewFakePostFilterPlugin(result *fwk.PostFilterResult, status *fwk.Status) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakePostFilterPlugin{
			Result: result,
			Status: status,
		}, nil
	}
}

// FakePreBindPlugin 是用于测试 PreBind 扩展点的插件。
type FakePreBindPlugin struct {
	PreBindPreFlightStatus *fwk.Status
	PreBindStatus          *fwk.Status
}

// Name 返回插件名。
func (pl *FakePreBindPlugin) Name() string {
	return "FakePreBind"
}

// PreBindPreFlight 在 PreBind 预检阶段调用。
func (pl *FakePreBindPlugin) PreBindPreFlight(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	return &fwk.PreBindPreFlightResult{AllowParallel: false}, pl.PreBindPreFlightStatus
}

// PreBind 在 PreBind 扩展点调用。
func (pl *FakePreBindPlugin) PreBind(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ string) *fwk.Status {
	return pl.PreBindStatus
}

// NewFakePreBindPlugin 返回一个创建 FakePreBindPlugin 的工厂函数。
func NewFakePreBindPlugin(preBindPreFlightStatus, preBindStatus *fwk.Status) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakePreBindPlugin{
			PreBindPreFlightStatus: preBindPreFlightStatus,
			PreBindStatus:          preBindStatus,
		}, nil
	}
}

// FakePermitPlugin 是用于测试 Permit 扩展点的插件。
type FakePermitPlugin struct {
	Handle  fwk.Handle
	Status  *fwk.Status
	Timeout time.Duration
}

// Name 返回插件名。
func (pl *FakePermitPlugin) Name() string {
	return "FakePermit"
}

// Permit 在 Permit 扩展点调用。
func (pl *FakePermitPlugin) Permit(_ context.Context, _ fwk.CycleState, p *v1.Pod, _ string) (*fwk.Status, time.Duration) {
	return pl.Status, pl.Timeout
}

// NewFakePermitPlugin 返回一个创建 FakePermitPlugin 的工厂函数。
func NewFakePermitPlugin(status *fwk.Status, timeout time.Duration) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
		return &FakePermitPlugin{
			Status:  status,
			Timeout: timeout,
			Handle:  h,
		}, nil
	}
}

// FakePreScoreAndScorePlugin 是用于测试 PreScore/Score 扩展点的插件，
// 返回预置的分数与状态。
type FakePreScoreAndScorePlugin struct {
	name           string
	score          int64
	preScoreStatus *fwk.Status
	scoreStatus    *fwk.Status
}

// Name 返回插件名。
func (pl *FakePreScoreAndScorePlugin) Name() string {
	return pl.name
}

// Score 在 Score 扩展点调用，返回预置分数。
func (pl *FakePreScoreAndScorePlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	return pl.score, pl.scoreStatus
}

// ScoreExtensions 返回 nil：该插件不提供分数规整扩展。
func (pl *FakePreScoreAndScorePlugin) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// PreScore 在 PreScore 扩展点调用，返回预置状态。
func (pl *FakePreScoreAndScorePlugin) PreScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	return pl.preScoreStatus
}

// NewFakePreScoreAndScorePlugin 返回一个创建 FakePreScoreAndScorePlugin 的工厂函数。
func NewFakePreScoreAndScorePlugin(name string, score int64, preScoreStatus, scoreStatus *fwk.Status) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakePreScoreAndScorePlugin{
			name:           name,
			score:          score,
			preScoreStatus: preScoreStatus,
			scoreStatus:    scoreStatus,
		}, nil
	}
}

// NewEqualPrioritizerPlugin 返回一个工厂函数，用于构建得分恒为 1 的 EqualPrioritizerPlugin。
func NewEqualPrioritizerPlugin() frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &FakePreScoreAndScorePlugin{
			name:  "EqualPrioritizerPlugin",
			score: 1,
		}, nil
	}
}
