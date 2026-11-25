/*
Copyright 2018 The Kubernetes Authors.

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

// Package plugins 包含 scheduler 插件支持的功能测试。
// 注意：本目录中的插件行为与真实插件不同，不可用于性能测试。
package plugins

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	configv1 "k8s.io/kube-scheduler/config/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler"
	schedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
	configtesting "k8s.io/kubernetes/pkg/scheduler/apis/config/testing"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/defaultbinder"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/schedulinggates"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	schedulerutils "k8s.io/kubernetes/test/integration/scheduler"
	testutils "k8s.io/kubernetes/test/integration/util"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/utils/ptr"
)

// 从 testutils 引入。
var (
	initRegistryAndConfig = func(t *testing.T, plugins ...fwk.Plugin) (frameworkruntime.Registry, schedulerconfig.KubeSchedulerProfile) {
		return schedulerutils.InitRegistryAndConfig(t, newPlugin, plugins...)
	}
)

// newPlugin 返回指定 Plugin 的插件工厂。
func newPlugin(plugin fwk.Plugin) frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		switch pl := plugin.(type) {
		case *PermitPlugin:
			pl.fh = fh
		case *PostFilterPlugin:
			pl.fh = fh
		}
		return plugin, nil
	}
}

type QueueSortPlugin struct {
	// lessFunc 用于比较两个队列中的 Pod 信息。
	lessFunc func(info1, info2 fwk.QueuedEntityInfo) bool
}

type PreEnqueuePlugin struct {
	called atomic.Int64
	admit  bool
}

type PreEnqueueGateAfterPreBindPlugin struct {
	preBind *PreBindPlugin
}

type PreFilterPlugin struct {
	numPreFilterCalled   atomic.Int64
	failPreFilter        bool
	rejectPreFilter      bool
	preFilterResultNodes sets.Set[string]
}

type ScorePlugin struct {
	mutex          sync.Mutex
	failScore      bool
	numScoreCalled int
	highScoreNode  string
}

func (sp *ScorePlugin) deepCopy() *ScorePlugin {
	sp.mutex.Lock()
	defer sp.mutex.Unlock()

	return &ScorePlugin{
		failScore:      sp.failScore,
		numScoreCalled: sp.numScoreCalled,
		highScoreNode:  sp.highScoreNode,
	}
}

type ScoreWithNormalizePlugin struct {
	mutex                   sync.Mutex
	numScoreCalled          int
	numNormalizeScoreCalled int
}

func (sp *ScoreWithNormalizePlugin) deepCopy() *ScoreWithNormalizePlugin {
	sp.mutex.Lock()
	defer sp.mutex.Unlock()

	return &ScoreWithNormalizePlugin{
		numScoreCalled:          sp.numScoreCalled,
		numNormalizeScoreCalled: sp.numNormalizeScoreCalled,
	}
}

type FilterPlugin struct {
	mutex           sync.Mutex
	numFilterCalled int
	failFilter      bool
	rejectFilter    bool

	numCalledPerPod map[string]int
}

func (fp *FilterPlugin) deepCopy() *FilterPlugin {
	fp.mutex.Lock()
	defer fp.mutex.Unlock()

	clone := &FilterPlugin{
		numFilterCalled: fp.numFilterCalled,
		failFilter:      fp.failFilter,
		rejectFilter:    fp.rejectFilter,
		numCalledPerPod: make(map[string]int),
	}
	for pod, counter := range fp.numCalledPerPod {
		clone.numCalledPerPod[pod] = counter
	}
	return clone
}

type PostFilterPlugin struct {
	name                string
	fh                  fwk.Handle
	numPostFilterCalled int
	failPostFilter      bool
	rejectPostFilter    bool
	breakPostFilter     bool
}

type ReservePlugin struct {
	name                  string
	numReserveCalled      int
	failReserve           bool
	numUnreserveCalled    int
	pluginInvokeEventChan chan pluginInvokeEvent
}

type PreScorePlugin struct {
	numPreScoreCalled int
	failPreScore      bool
}

type PreBindPlugin struct {
	mutex            sync.Mutex
	numPreBindCalled int
	failPreBind      bool
	rejectPreBind    bool
	// 为 true 时，非首次调度尝试一律成功。
	succeedOnRetry bool
	// 记录已经尝试调度的 Pod UID。
	podUIDs map[types.UID]struct{}
}

func (pp *PreBindPlugin) set(fail, reject, succeed bool) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	pp.failPreBind = fail
	pp.rejectPreBind = reject
	pp.succeedOnRetry = succeed
}

func (pp *PreBindPlugin) deepCopy() *PreBindPlugin {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	clone := &PreBindPlugin{
		numPreBindCalled: pp.numPreBindCalled,
		failPreBind:      pp.failPreBind,
		rejectPreBind:    pp.rejectPreBind,
		succeedOnRetry:   pp.succeedOnRetry,
		podUIDs:          make(map[types.UID]struct{}),
	}
	for uid := range pp.podUIDs {
		clone.podUIDs[uid] = struct{}{}
	}
	return clone
}

type BindPlugin struct {
	mutex                 sync.Mutex
	name                  string
	numBindCalled         int
	bindStatus            *fwk.Status
	client                clientset.Interface
	pluginInvokeEventChan chan pluginInvokeEvent
}

func (bp *BindPlugin) deepCopy() *BindPlugin {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	return &BindPlugin{
		name:                  bp.name,
		numBindCalled:         bp.numBindCalled,
		bindStatus:            bp.bindStatus,
		client:                bp.client,
		pluginInvokeEventChan: bp.pluginInvokeEventChan,
	}
}

type PostBindPlugin struct {
	mutex                 sync.Mutex
	name                  string
	numPostBindCalled     int
	pluginInvokeEventChan chan pluginInvokeEvent
}

func (pp *PostBindPlugin) deepCopy() *PostBindPlugin {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	return &PostBindPlugin{
		name:                  pp.name,
		numPostBindCalled:     pp.numPostBindCalled,
		pluginInvokeEventChan: pp.pluginInvokeEventChan,
	}
}

type PermitPlugin struct {
	mutex               sync.Mutex
	name                string
	numPermitCalled     int
	failPermit          bool
	rejectPermit        bool
	timeoutPermit       bool
	waitAndRejectPermit bool
	waitAndAllowPermit  bool
	cancelled           bool
	waitingPod          string
	rejectingPod        string
	allowingPod         string
	fh                  fwk.Handle
}

func (pp *PermitPlugin) deepCopy() *PermitPlugin {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	return &PermitPlugin{
		name:                pp.name,
		numPermitCalled:     pp.numPermitCalled,
		failPermit:          pp.failPermit,
		rejectPermit:        pp.rejectPermit,
		timeoutPermit:       pp.timeoutPermit,
		waitAndRejectPermit: pp.waitAndRejectPermit,
		waitAndAllowPermit:  pp.waitAndAllowPermit,
		cancelled:           pp.cancelled,
		waitingPod:          pp.waitingPod,
		rejectingPod:        pp.rejectingPod,
		allowingPod:         pp.allowingPod,
		fh:                  pp.fh,
	}
}

const (
	queuesortPluginName          = "queuesort-plugin"
	enqueuePluginName            = "enqueue-plugin"
	prefilterPluginName          = "prefilter-plugin"
	postfilterPluginName         = "postfilter-plugin"
	scorePluginName              = "score-plugin"
	scoreWithNormalizePluginName = "score-with-normalize-plugin"
	filterPluginName             = "filter-plugin"
	preScorePluginName           = "prescore-plugin"
	reservePluginName            = "reserve-plugin"
	preBindPluginName            = "prebind-plugin"
	postBindPluginName           = "postbind-plugin"
	permitPluginName             = "permit-plugin"
	preEnqueueGatePluginName     = "preenqueue-gate-plugin"
)

var _ fwk.PreEnqueuePlugin = &PreEnqueuePlugin{}
var _ fwk.PreFilterPlugin = &PreFilterPlugin{}
var _ fwk.PostFilterPlugin = &PostFilterPlugin{}
var _ fwk.ScorePlugin = &ScorePlugin{}
var _ fwk.FilterPlugin = &FilterPlugin{}
var _ fwk.EnqueueExtensions = &FilterPlugin{}
var _ fwk.ScorePlugin = &ScorePlugin{}
var _ fwk.ScorePlugin = &ScoreWithNormalizePlugin{}
var _ fwk.EnqueueExtensions = &ScorePlugin{}
var _ fwk.ReservePlugin = &ReservePlugin{}
var _ fwk.PreScorePlugin = &PreScorePlugin{}
var _ fwk.PreBindPlugin = &PreBindPlugin{}
var _ fwk.EnqueueExtensions = &PreBindPlugin{}
var _ fwk.BindPlugin = &BindPlugin{}
var _ fwk.PostBindPlugin = &PostBindPlugin{}
var _ fwk.PermitPlugin = &PermitPlugin{}
var _ fwk.EnqueueExtensions = &PermitPlugin{}
var _ fwk.QueueSortPlugin = &QueueSortPlugin{}
var _ fwk.PreEnqueuePlugin = &PreEnqueueGateAfterPreBindPlugin{}

func (ep *QueueSortPlugin) Name() string {
	return queuesortPluginName
}

func (ep *QueueSortPlugin) Less(info1, info2 fwk.QueuedEntityInfo) bool {
	if ep.lessFunc != nil {
		return ep.lessFunc(info1, info2)
	}
	// 未提供自定义比较函数时默认返回 true。
	return true
}

func NewQueueSortPlugin(lessFunc func(info1, info2 fwk.QueuedEntityInfo) bool) *QueueSortPlugin {
	return &QueueSortPlugin{
		lessFunc: lessFunc,
	}
}

func (ep *PreEnqueuePlugin) Name() string {
	return enqueuePluginName
}

func (ep *PreEnqueuePlugin) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	ep.called.Add(1)
	if ep.admit {
		return nil
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "not ready for scheduling")
}

// Name 返回 score 插件名称。
func (sp *ScorePlugin) Name() string {
	return scorePluginName
}

// Score 返回将 Pod 调度到指定 Node 的得分。
func (sp *ScorePlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	sp.mutex.Lock()
	defer sp.mutex.Unlock()

	sp.numScoreCalled++
	if sp.failScore {
		return 0, fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", p.Name))
	}

	score := int64(1)
	if sp.numScoreCalled == 1 {
		// 第一个 Node 得最高分，其余较低。
		sp.highScoreNode = nodeInfo.Node().Name
		score = fwk.MaxNodeScore
	}
	return score, nil
}

func (sp *ScorePlugin) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

func (sp *ScorePlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, nil
}

// Name 返回 score 插件名称。
func (sp *ScoreWithNormalizePlugin) Name() string {
	return scoreWithNormalizePluginName
}

// Score 返回将 Pod 调度到指定 Node 的得分。
func (sp *ScoreWithNormalizePlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	sp.mutex.Lock()
	defer sp.mutex.Unlock()

	sp.numScoreCalled++
	score := int64(10)
	return score, nil
}

func (sp *ScoreWithNormalizePlugin) NormalizeScore(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	sp.numNormalizeScoreCalled++
	return nil
}

func (sp *ScoreWithNormalizePlugin) ScoreExtensions() fwk.ScoreExtensions {
	return sp
}

// Name 返回插件名称。
func (fp *FilterPlugin) Name() string {
	return filterPluginName
}

// Filter 是测试函数，根据 failFilter 返回错误或 nil。
func (fp *FilterPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	fp.mutex.Lock()
	defer fp.mutex.Unlock()

	fp.numFilterCalled++
	if fp.numCalledPerPod != nil {
		fp.numCalledPerPod[fmt.Sprintf("%v/%v", pod.Namespace, pod.Name)]++
	}

	if fp.failFilter {
		return fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}
	if fp.rejectFilter {
		return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("reject pod %v", pod.Name))
	}

	return nil
}

func (fp *FilterPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Delete}},
	}, nil
}

// Name 返回插件名称。
func (rp *ReservePlugin) Name() string {
	return rp.name
}

// Reserve 是测试函数，递增内部计数器，并根据 failReserve 返回错误或 nil。
func (rp *ReservePlugin) Reserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	rp.numReserveCalled++
	if rp.failReserve {
		return fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}
	return nil
}

// Unreserve 是测试函数，递增内部计数器并向 channel 发送事件。
// 实际实现通常要求幂等，这里为测试目的放宽该要求。
func (rp *ReservePlugin) Unreserve(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	rp.numUnreserveCalled++
	if rp.pluginInvokeEventChan != nil {
		select {
		case <-ctx.Done():
		case rp.pluginInvokeEventChan <- pluginInvokeEvent{pluginName: rp.Name(), val: rp.numUnreserveCalled}:
		}
	}
}

// Name 返回插件名称。
func (*PreScorePlugin) Name() string {
	return preScorePluginName
}

// PreScore 是测试函数。
func (pfp *PreScorePlugin) PreScore(ctx context.Context, _ fwk.CycleState, pod *v1.Pod, _ []fwk.NodeInfo) *fwk.Status {
	pfp.numPreScoreCalled++
	if pfp.failPreScore {
		return fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}

	return nil
}

// Name 返回插件名称。
func (pp *PreBindPlugin) Name() string {
	return preBindPluginName
}

// PreBindPreFlight 是测试函数，固定返回 nil。
func (pp *PreBindPlugin) PreBindPreFlight(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	return &fwk.PreBindPreFlightResult{AllowParallel: false}, nil
}

// PreBind 是测试函数，根据配置返回成功或错误。
func (pp *PreBindPlugin) PreBind(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) *fwk.Status {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	pp.numPreBindCalled++
	if _, tried := pp.podUIDs[pod.UID]; tried && pp.succeedOnRetry {
		return nil
	}
	pp.podUIDs[pod.UID] = struct{}{}
	if pp.failPreBind {
		return fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}
	if pp.rejectPreBind {
		return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("reject pod %v", pod.Name))
	}
	return nil
}

func (pp *PreBindPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, nil
}

func (pp *PreBindPlugin) hasTriedPreBind(uid types.UID) bool {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	if pp.podUIDs == nil {
		return false
	}
	_, ok := pp.podUIDs[uid]
	return ok
}

const bindPluginAnnotation = "bindPluginName"

func (bp *BindPlugin) Name() string {
	return bp.name
}

func (bp *BindPlugin) Bind(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) *fwk.Status {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	bp.numBindCalled++
	if bp.pluginInvokeEventChan != nil {
		select {
		case <-ctx.Done():
		case bp.pluginInvokeEventChan <- pluginInvokeEvent{pluginName: bp.Name(), val: bp.numBindCalled}:
		}
	}
	if bp.bindStatus.IsSuccess() {
		if err := bp.client.CoreV1().Pods(p.Namespace).Bind(ctx, &v1.Binding{
			ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID, Annotations: map[string]string{bindPluginAnnotation: bp.Name()}},
			Target: v1.ObjectReference{
				Kind: "Node",
				Name: nodeName,
			},
		}, metav1.CreateOptions{}); err != nil {
			return fwk.NewStatus(fwk.Error, fmt.Sprintf("bind failed: %v", err))
		}
	}
	return bp.bindStatus
}

// Name 返回插件名称。
func (pp *PostBindPlugin) Name() string {
	return pp.name
}

// PostBind 是测试函数，统计调用次数。
func (pp *PostBindPlugin) PostBind(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	pp.numPostBindCalled++
	if pp.pluginInvokeEventChan != nil {
		select {
		case <-ctx.Done():
		case pp.pluginInvokeEventChan <- pluginInvokeEvent{pluginName: pp.Name(), val: pp.numPostBindCalled}:
		}
	}
}

// Name 返回插件名称。
func (pp *PreFilterPlugin) Name() string {
	return prefilterPluginName
}

// Extensions 返回 PreFilterExtensions 接口。
func (pp *PreFilterPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// PreFilter 是测试函数，根据配置返回成功或错误。
func (pp *PreFilterPlugin) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	pp.numPreFilterCalled.Add(1)
	if pp.failPreFilter {
		return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}
	if pp.rejectPreFilter {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("reject pod %v", pod.Name))
	}
	if len(pp.preFilterResultNodes) != 0 {
		return &fwk.PreFilterResult{NodeNames: pp.preFilterResultNodes}, nil
	}
	return nil, nil
}

// Name 返回插件名称。
func (pp *PostFilterPlugin) Name() string {
	return pp.name
}

func (pp *PostFilterPlugin) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, _ fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	pp.numPostFilterCalled++
	nodeInfos, err := pp.fh.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, fwk.NewStatus(fwk.Error, err.Error())
	}

	for _, nodeInfo := range nodeInfos {
		pp.fh.RunFilterPlugins(ctx, state, pod, nodeInfo)
	}
	pp.fh.RunScorePlugins(ctx, state, pod, nodeInfos)

	if pp.failPostFilter {
		return nil, fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name))
	}
	if pp.rejectPostFilter {
		return nil, fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("injecting unschedulable for pod %v", pod.Name))
	}
	if pp.breakPostFilter {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, fmt.Sprintf("injecting unresolvable for pod %v", pod.Name))
	}

	return nil, fwk.NewStatus(fwk.Success, fmt.Sprintf("make room for pod %v to be schedulable", pod.Name))
}

// Name 返回插件名称。
func (pp *PermitPlugin) Name() string {
	return pp.name
}

// Permit 实现 permit 测试插件的逻辑。
func (pp *PermitPlugin) Permit(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeName string) (*fwk.Status, time.Duration) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	pp.numPermitCalled++
	if pp.failPermit {
		return fwk.NewStatus(fwk.Error, fmt.Sprintf("injecting failure for pod %v", pod.Name)), 0
	}
	if pp.rejectPermit {
		return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("reject pod %v", pod.Name)), 0
	}
	if pp.timeoutPermit {
		go func() {
			select {
			case <-ctx.Done():
				pp.mutex.Lock()
				defer pp.mutex.Unlock()
				pp.cancelled = true
			}
		}()
		return fwk.NewStatus(fwk.Wait, ""), 3 * time.Second
	}
	if pp.waitAndRejectPermit || pp.waitAndAllowPermit {
		if pp.waitingPod == "" || pp.waitingPod == pod.Name {
			pp.waitingPod = pod.Name
			return fwk.NewStatus(fwk.Wait, ""), 30 * time.Second
		}
		if pp.waitAndRejectPermit {
			pp.rejectingPod = pod.Name
			pp.fh.IterateOverWaitingPods(func(wp fwk.WaitingPod) {
				wp.Reject(pp.name, fmt.Sprintf("reject pod %v", wp.GetPod().Name))
			})
			return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("reject pod %v", pod.Name)), 0
		}
		if pp.waitAndAllowPermit {
			pp.allowingPod = pod.Name
			pp.allowAllPods()
			return nil, 0
		}
	}
	return nil, 0
}

// allowAllPods 放行所有等待中的 Pod。
func (pp *PermitPlugin) allowAllPods() {
	pp.fh.IterateOverWaitingPods(func(wp fwk.WaitingPod) { wp.Allow(pp.name) })
}

// rejectAllPods 拒绝所有等待中的 Pod。
func (pp *PermitPlugin) rejectAllPods() {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()
	pp.fh.IterateOverWaitingPods(func(wp fwk.WaitingPod) { wp.Reject(pp.name, "rejectAllPods") })
}

func (pp *PermitPlugin) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, nil
}

// Name 返回插件名称。
func (pl *PreEnqueueGateAfterPreBindPlugin) Name() string {
	return preEnqueueGatePluginName
}

// PreEnqueue 是测试函数，在 PreBind 尝试后阻止重新入队。
func (pl *PreEnqueueGateAfterPreBindPlugin) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	if pl.preBind != nil && pl.preBind.hasTriedPreBind(p.UID) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "test gate: block re-enqueue after PreBind was attempted")
	}
	return nil
}

// TestPreFilterPlugin 验证 prefilter 插件的调用。
func TestPreFilterPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "prefilter-plugin", nil)

	tests := []struct {
		name                 string
		fail                 bool
		reject               bool
		preFilterResultNodes sets.Set[string]
	}{
		{
			name:   "disable fail and reject flags",
			fail:   false,
			reject: false,
		},
		{
			name:   "enable fail and disable reject flags",
			fail:   true,
			reject: false,
		},
		{
			name:   "disable fail and enable reject flags",
			fail:   false,
			reject: true,
		},
		{
			name:                 "inject legal node names in PreFilterResult",
			fail:                 false,
			reject:               false,
			preFilterResultNodes: sets.New[string]("test-node-0", "test-node-1"),
		},
		{
			name:                 "inject legal and illegal node names in PreFilterResult",
			fail:                 false,
			reject:               false,
			preFilterResultNodes: sets.New[string]("test-node-0", "non-existent-node"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建插件注册表，仅注册 pre-filter 插件。
			preFilterPlugin := &PreFilterPlugin{}
			registry, prof := initRegistryAndConfig(t, preFilterPlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			preFilterPlugin.failPreFilter = test.fail
			preFilterPlugin.rejectPreFilter = test.reject
			preFilterPlugin.preFilterResultNodes = test.preFilterResultNodes
			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.reject {
				if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Didn't expect the pod to be scheduled. error: %v", err)
				}
			} else if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but got: %v", err)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}
			}

			if preFilterPlugin.numPreFilterCalled.Load() == 0 {
				t.Errorf("Expected the prefilter plugin to be called.")
			}
		})
	}
}

// TestQueueSortPlugin 验证 queueSort 插件的调用。
func TestQueueSortPlugin(t *testing.T) {
	tests := []struct {
		name           string
		podNames       []string
		expectedOrder  []string
		customLessFunc func(info1, info2 fwk.QueuedEntityInfo) bool
	}{
		{
			name:          "timestamp_sort_order",
			podNames:      []string{"pod-1", "pod-2", "pod-3"},
			expectedOrder: []string{"pod-1", "pod-2", "pod-3"},
			customLessFunc: func(info1, info2 fwk.QueuedEntityInfo) bool {
				return info1.GetTimestamp().Before(info2.GetTimestamp())
			},
		},
		{
			name:          "priority_sort_order",
			podNames:      []string{"pod-1", "pod-2", "pod-3"},
			expectedOrder: []string{"pod-3", "pod-2", "pod-1"}, // 取决于 Pod 优先级。
			customLessFunc: func(info1, info2 fwk.QueuedEntityInfo) bool {
				p1 := info1.GetPriority()
				p2 := info2.GetPriority()
				return (p1 > p2) || (p1 == p2 && info1.GetTimestamp().Before(info2.GetTimestamp()))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queueSortPlugin := NewQueueSortPlugin(tt.customLessFunc)
			registry, prof := initRegistryAndConfig(t, queueSortPlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testutils.InitTestAPIServer(t, "queuesort-plugin", nil), 2, false,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			pods := make([]*v1.Pod, 0, len(tt.podNames))
			for i, name := range tt.podNames {
				// 创建优先级各不相同的 Pod。
				priority := int32(i + 1)
				pod, err := testutils.CreatePausePod(testCtx.ClientSet,
					testutils.InitPausePod(&testutils.PausePodConfig{Name: name, Namespace: testCtx.NS.Name, Priority: &priority}))
				if err != nil {
					t.Fatalf("Error while creating %v: %v", name, err)
				}
				pods = append(pods, pod)
			}

			// 等待所有 Pod 进入调度队列。
			err := wait.PollUntilContextTimeout(testCtx.Ctx, time.Millisecond*200, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
				pendingPods, _ := testCtx.Scheduler.SchedulingQueue.PendingPods()
				if len(pendingPods) == len(pods) {
					t.Logf("All Pods are in the pending queue.")
					return true, nil
				}
				t.Logf("Waiting for all Pods to be in the scheduling queue. %d/%d", len(pendingPods), len(pods))
				return false, nil
			})
			if err != nil {
				t.Fatalf("Failed to observe all Pods in the scheduling queue: %v", err)
			}

			actualOrder := make([]string, len(tt.expectedOrder))
			for i := 0; i < len(tt.expectedOrder); i++ {
				entity := testutils.NextEntityOrDie(t, testCtx)
				actualOrder[i] = entity.GetName()
				t.Logf("Popped entity (Pod) %q", entity.GetName())
			}
			if diff := cmp.Diff(tt.expectedOrder, actualOrder); diff != "" {
				t.Errorf("Expected Pod order (-want,+got):\n%s", diff)
			} else {
				t.Logf("Pods were popped out in the expected order based on custom sorting logic.")
			}
		})
	}
}

// TestPostFilterPlugin 验证 postFilter 插件的调用。
func TestPostFilterPlugin(t *testing.T) {
	numNodes := 1
	tests := []struct {
		name                      string
		numNodes                  int
		rejectFilter              bool
		failScore                 bool
		rejectPostFilter          bool
		rejectPostFilter2         bool
		breakPostFilter           bool
		breakPostFilter2          bool
		expectFilterNumCalled     int
		expectScoreNumCalled      int
		expectPostFilterNumCalled int
	}{
		{
			name:                      "Filter passed and Score success",
			numNodes:                  30,
			rejectFilter:              false,
			failScore:                 false,
			rejectPostFilter:          false,
			expectFilterNumCalled:     30,
			expectScoreNumCalled:      30,
			expectPostFilterNumCalled: 0,
		},
		{
			name:                      "Filter failed and PostFilter passed",
			numNodes:                  numNodes,
			rejectFilter:              true,
			failScore:                 false,
			rejectPostFilter:          false,
			expectFilterNumCalled:     numNodes * 2,
			expectScoreNumCalled:      1,
			expectPostFilterNumCalled: 1,
		},
		{
			name:                      "Filter failed and PostFilter failed",
			numNodes:                  numNodes,
			rejectFilter:              true,
			failScore:                 false,
			rejectPostFilter:          true,
			expectFilterNumCalled:     numNodes * 2,
			expectScoreNumCalled:      1,
			expectPostFilterNumCalled: 2,
		},
		{
			name:                      "Score failed and PostFilter failed",
			numNodes:                  numNodes,
			rejectFilter:              true,
			failScore:                 true,
			rejectPostFilter:          true,
			expectFilterNumCalled:     numNodes * 2,
			expectScoreNumCalled:      1,
			expectPostFilterNumCalled: 2,
		},
		{
			name:                      "Filter failed and first PostFilter broken",
			numNodes:                  numNodes,
			rejectFilter:              true,
			breakPostFilter:           true,
			expectFilterNumCalled:     numNodes * 2,
			expectScoreNumCalled:      0,
			expectPostFilterNumCalled: 1,
		},
		{
			name:                      "Filter failed and second PostFilter broken",
			numNodes:                  numNodes,
			rejectFilter:              true,
			rejectPostFilter:          true,
			rejectPostFilter2:         true,
			breakPostFilter2:          true,
			expectFilterNumCalled:     numNodes * 2,
			expectScoreNumCalled:      0,
			expectPostFilterNumCalled: 2,
		},
	}

	var postFilterPluginName2 = postfilterPluginName + "2"
	testContext := testutils.InitTestAPIServer(t, "post-filter", nil)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建插件注册表，注册 filter 和 postFilter 插件组合。
			var (
				filterPlugin      = &FilterPlugin{}
				scorePlugin       = &ScorePlugin{}
				postFilterPlugin  = &PostFilterPlugin{name: postfilterPluginName}
				postFilterPlugin2 = &PostFilterPlugin{name: postFilterPluginName2}
			)
			filterPlugin.rejectFilter = tt.rejectFilter
			scorePlugin.failScore = tt.failScore
			postFilterPlugin.rejectPostFilter = tt.rejectPostFilter
			postFilterPlugin2.rejectPostFilter = tt.rejectPostFilter2
			postFilterPlugin.breakPostFilter = tt.breakPostFilter
			postFilterPlugin2.breakPostFilter = tt.breakPostFilter2

			registry := frameworkruntime.Registry{
				filterPluginName:      newPlugin(filterPlugin),
				scorePluginName:       newPlugin(scorePlugin),
				postfilterPluginName:  newPlugin(postFilterPlugin),
				postFilterPluginName2: newPlugin(postFilterPlugin2),
			}

			// 配置待测插件。
			cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
				Profiles: []configv1.KubeSchedulerProfile{{
					SchedulerName: ptr.To(v1.DefaultSchedulerName),
					Plugins: &configv1.Plugins{
						Filter: configv1.PluginSet{
							Enabled: []configv1.Plugin{
								{Name: filterPluginName},
							},
						},
						PreScore: configv1.PluginSet{
							Disabled: []configv1.Plugin{
								{Name: "*"},
							},
						},
						Score: configv1.PluginSet{
							Enabled: []configv1.Plugin{
								{Name: scorePluginName},
							},
							// 禁用默认 in-tree Score 插件，便于控制 ScorePlugin 的失败行为。
							Disabled: []configv1.Plugin{
								{Name: "*"},
							},
						},
						PostFilter: configv1.PluginSet{
							Enabled: []configv1.Plugin{
								{Name: postfilterPluginName},
								{Name: postFilterPluginName2},
							},
							// 禁用默认 in-tree PostFilter 插件，避免它们调用 RunPostFilterPlugins
							// 影响 numPostFilterCalled 计数。
							Disabled: []configv1.Plugin{
								{Name: "*"},
							},
						},
					},
				}}})

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, int(tt.numNodes), true,
				scheduler.WithProfiles(cfg.Profiles...),
				scheduler.WithFrameworkOutOfTreeRegistry(registry),
			)
			defer teardown()

			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet, testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if tt.rejectFilter {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 10*time.Second, false,
					testutils.PodUnschedulable(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Didn't expect the pod to be scheduled.")
				}

				if numFilterCalled := filterPlugin.deepCopy().numFilterCalled; numFilterCalled < tt.expectFilterNumCalled {
					t.Errorf("Expected the filter plugin to be called at least %v times, but got %v.", tt.expectFilterNumCalled, numFilterCalled)
				}
				if numScoreCalled := scorePlugin.deepCopy().numScoreCalled; numScoreCalled < tt.expectScoreNumCalled {
					t.Errorf("Expected the score plugin to be called at least %v times, but got %v.", tt.expectScoreNumCalled, numScoreCalled)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}
				if numFilterCalled := filterPlugin.deepCopy().numFilterCalled; numFilterCalled != tt.expectFilterNumCalled {
					t.Errorf("Expected the filter plugin to be called %v times, but got %v.", tt.expectFilterNumCalled, numFilterCalled)
				}
				if numScoreCalled := scorePlugin.deepCopy().numScoreCalled; numScoreCalled != tt.expectScoreNumCalled {
					t.Errorf("Expected the score plugin to be called %v times, but got %v.", tt.expectScoreNumCalled, numScoreCalled)
				}
			}

			numPostFilterCalled := postFilterPlugin.numPostFilterCalled + postFilterPlugin2.numPostFilterCalled
			if numPostFilterCalled != tt.expectPostFilterNumCalled {
				t.Errorf("Expected the postfilter plugin to be called %v times, but got %v.", tt.expectPostFilterNumCalled, numPostFilterCalled)
			}
		})
	}
}

// TestScorePlugin 验证 score 插件的调用。
func TestScorePlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "score-plugin", nil)

	tests := []struct {
		name string
		fail bool
	}{
		{
			name: "fail score plugin",
			fail: true,
		},
		{
			name: "do not fail score plugin",
			fail: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建插件注册表，仅注册 score 插件。
			scorePlugin := &ScorePlugin{}
			registry, prof := initRegistryAndConfig(t, scorePlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 10, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			scorePlugin.failScore = test.fail
			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Fatalf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but got: %v", err)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				} else {
					p, err := testutils.GetPod(testCtx.ClientSet, pod.Name, pod.Namespace)
					if err != nil {
						t.Errorf("Failed to retrieve the pod. error: %v", err)
					} else if p.Spec.NodeName != scorePlugin.highScoreNode {
						t.Errorf("Expected the pod to be scheduled on node %q, got %q", scorePlugin.highScoreNode, p.Spec.NodeName)
					}
				}
			}

			if numScoreCalled := scorePlugin.deepCopy().numScoreCalled; numScoreCalled == 0 {
				t.Errorf("Expected the score plugin to be called.")
			}
		})
	}
}

// TestNormalizeScorePlugin 验证 normalize score 插件的调用。
func TestNormalizeScorePlugin(t *testing.T) {
	// 创建插件注册表，仅注册 normalize score 插件。
	scoreWithNormalizePlugin := &ScoreWithNormalizePlugin{}
	registry, prof := initRegistryAndConfig(t, scoreWithNormalizePlugin)

	testCtx, _ := schedulerutils.InitTestSchedulerForFrameworkTest(t, testutils.InitTestAPIServer(t, "score-plugin", nil), 10, true,
		scheduler.WithProfiles(prof),
		scheduler.WithFrameworkOutOfTreeRegistry(registry))

	// 创建 best effort Pod。
	pod, err := testutils.CreatePausePod(testCtx.ClientSet,
		testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
	if err != nil {
		t.Fatalf("Error while creating a test pod: %v", err)
	}

	if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
		t.Errorf("Expected the pod to be scheduled. error: %v", err)
	}

	p := scoreWithNormalizePlugin.deepCopy()
	if p.numScoreCalled == 0 {
		t.Errorf("Expected the score plugin to be called.")
	}
	if p.numNormalizeScoreCalled == 0 {
		t.Error("Expected the normalize score plugin to be called")
	}
}

// TestReservePluginReserve 验证 reserve 插件的调用。
func TestReservePluginReserve(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "reserve-plugin-reserve", nil)

	tests := []struct {
		name string
		fail bool
	}{
		{
			name: "fail reserve plugin",
			fail: true,
		},
		{
			name: "do not fail reserve plugin",
			fail: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建插件注册表，仅注册 reserve 插件。
			reservePlugin := &ReservePlugin{}
			registry, prof := initRegistryAndConfig(t, reservePlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			reservePlugin.failReserve = test.fail
			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Didn't expect the pod to be scheduled. error: %v", err)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}
			}

			if reservePlugin.numReserveCalled == 0 {
				t.Errorf("Expected the reserve plugin to be called.")
			}
		})
	}
}

// TestPrebindPlugin 验证 prebind 插件的调用。
func TestPrebindPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "prebind-plugin", nil)

	nodesNum := 2

	tests := []struct {
		name             string
		fail             bool
		reject           bool
		succeedOnRetry   bool
		unschedulablePod *v1.Pod
	}{
		{
			name:   "disable fail and reject flags",
			fail:   false,
			reject: false,
		},
		{
			name:   "enable fail and disable reject flags",
			fail:   true,
			reject: false,
		},
		{
			name:   "disable fail and enable reject flags",
			fail:   false,
			reject: true,
		},
		{
			name:   "enable fail and reject flags",
			fail:   true,
			reject: true,
		},
		{
			name:           "fail on 1st try but succeed on retry",
			fail:           true,
			reject:         false,
			succeedOnRetry: true,
		},
		{
			name:             "failure on preBind moves unschedulable pods",
			fail:             true,
			unschedulablePod: st.MakePod().Name("unschedulable-pod").Namespace(testContext.NS.Name).Container(imageutils.GetPauseImageName()).Obj(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建插件注册表，注册 prebind 和 filter 插件。
			preBindPlugin := &PreBindPlugin{podUIDs: make(map[types.UID]struct{})}
			filterPlugin := &FilterPlugin{}
			registry := frameworkruntime.Registry{
				preBindPluginName: newPlugin(preBindPlugin),
				filterPluginName:  newPlugin(filterPlugin),
			}

			// 在不同 profile 中分别配置 prebind 和 filter 插件。
			// 第二个 profile 确保 filter 插件被单独调用，便于用其内部 numFilterCalled 精确校验。
			cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
				Profiles: []configv1.KubeSchedulerProfile{
					{
						SchedulerName: ptr.To(v1.DefaultSchedulerName),
						Plugins: &configv1.Plugins{
							PreBind: configv1.PluginSet{
								Enabled: []configv1.Plugin{
									{Name: preBindPluginName},
								},
							},
						},
					},
					{
						SchedulerName: ptr.To("2nd-scheduler"),
						Plugins: &configv1.Plugins{
							Filter: configv1.PluginSet{
								Enabled: []configv1.Plugin{
									{Name: filterPluginName},
								},
							},
						},
					},
				},
			})

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, nodesNum, true,
				scheduler.WithProfiles(cfg.Profiles...),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			if p := test.unschedulablePod; p != nil {
				p.Spec.SchedulerName = "2nd-scheduler"
				filterPlugin.rejectFilter = true
				if _, err := testutils.CreatePausePod(testCtx.ClientSet, p); err != nil {
					t.Fatalf("Error while creating an unschedulable pod: %v", err)
				}
			}

			preBindPlugin.set(test.fail, test.reject, test.succeedOnRetry)

			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if test.succeedOnRetry {
					if err = testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, testCtx.ClientSet, pod, 10*time.Second); err != nil {
						t.Errorf("Expected the pod to be schedulable on retry, but got an error: %v", err)
					}
				} else if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but didn't get it. error: %v", err)
				}
			} else if test.reject {
				if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be unschedulable")
				}
			} else if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
				t.Errorf("Expected the pod to be scheduled. error: %v", err)
			}

			p := preBindPlugin.deepCopy()
			if p.numPreBindCalled == 0 {
				t.Errorf("Expected the prebind plugin to be called.")
			}

			if test.unschedulablePod != nil {
				if err := wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 15*time.Second, false, func(ctx context.Context) (bool, error) {
					// 2 表示不可调度 Pod 至少应重试两次：一次初始尝试，加上由 preBind Pod 触发的一次。
					return filterPlugin.deepCopy().numFilterCalled >= 2*nodesNum, nil
				}); err != nil {
					t.Errorf("Timed out waiting for the unschedulable Pod to be retried at least twice.")
				}
			}
		})
	}
}

// TestUnReserveReservePlugins 验证 pre-bind 等阶段失败时 reserve 插件的 Unreserve 调用，
// 并检查 Unreserve 的调用顺序与 Reserve 相反。
func TestUnReserveReservePlugins(t *testing.T) {
	tests := []struct {
		name          string
		plugins       []*ReservePlugin
		fail          bool
		failPluginIdx int
	}{
		{
			name:          "The first Reserve plugin fails",
			failPluginIdx: 0,
			plugins: []*ReservePlugin{
				{
					name:        "failedReservePlugin1",
					failReserve: true,
				},
				{
					name:        "succeedReservePlugin",
					failReserve: false,
				},
				{
					name:        "failedReservePlugin2",
					failReserve: true,
				},
			},
			fail: true,
		},
		{
			name:          "The middle Reserve plugin fails",
			failPluginIdx: 1,
			plugins: []*ReservePlugin{
				{
					name:        "succeedReservePlugin1",
					failReserve: false,
				},
				{
					name:        "failedReservePlugin1",
					failReserve: true,
				},
				{
					name:        "succeedReservePlugin2",
					failReserve: false,
				},
			},
			fail: true,
		},
		{
			name:          "The last Reserve plugin fails",
			failPluginIdx: 2,
			plugins: []*ReservePlugin{
				{
					name:        "succeedReservePlugin1",
					failReserve: false,
				},
				{
					name:        "succeedReservePlugin2",
					failReserve: false,
				},
				{
					name:        "failedReservePlugin1",
					failReserve: true,
				},
			},
			fail: true,
		},
		{
			name:          "The Reserve plugins succeed",
			failPluginIdx: -1,
			plugins: []*ReservePlugin{
				{
					name:        "succeedReservePlugin1",
					failReserve: false,
				},
				{
					name:        "succeedReservePlugin2",
					failReserve: false,
				},
				{
					name:        "succeedReservePlugin3",
					failReserve: false,
				},
			},
			fail: false,
		},
	}

	testContext := testutils.InitTestAPIServer(t, "unreserve-reserve-plugin", nil)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var pls []fwk.Plugin
			for _, pl := range test.plugins {
				pls = append(pls, pl)
			}
			registry, prof := initRegistryAndConfig(t, pls...)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			// 创建 best effort Pod。
			podName := "test-pod"
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: podName, Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a reasons other than Unschedulable, but got: %v", err)
				}

				for i, pl := range test.plugins {
					if i <= test.failPluginIdx {
						if pl.numReserveCalled != 1 {
							t.Errorf("Reserve Plugins %s numReserveCalled = %d, want 1.", pl.name, pl.numReserveCalled)
						}
					} else {
						if pl.numReserveCalled != 0 {
							t.Errorf("Reserve Plugins %s numReserveCalled = %d, want 0.", pl.name, pl.numReserveCalled)
						}
					}

					if pl.numUnreserveCalled != 1 {
						t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 1.", pl.name, pl.numUnreserveCalled)
					}
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}

				for _, pl := range test.plugins {
					if pl.numReserveCalled != 1 {
						t.Errorf("Reserve Plugin %s numReserveCalled = %d, want 1.", pl.name, pl.numReserveCalled)
					}
					if pl.numUnreserveCalled != 0 {
						t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 0.", pl.name, pl.numUnreserveCalled)
					}
				}
			}
		})
	}
}

// TestUnReservePermitPlugins 验证 Permit 插件失败后的 Unreserve 行为。
func TestUnReservePermitPlugins(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "unreserve-reserve-plugin", nil)

	tests := []struct {
		name   string
		plugin *PermitPlugin
		reject bool
	}{
		{
			name:   "All Reserve plugins passed, but a Permit plugin was rejected",
			reject: true,
			plugin: &PermitPlugin{
				name:         "rejectedPermitPlugin",
				rejectPermit: true,
			},
		},
		{
			name:   "All Reserve plugins passed, but a Permit plugin timeout in waiting",
			reject: true,
			plugin: &PermitPlugin{
				name:          "timeoutPermitPlugin",
				timeoutPermit: true,
			},
		},
		{
			name:   "The Permit plugin succeed",
			reject: false,
			plugin: &PermitPlugin{
				name: "succeedPermitPlugin",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reservePlugin := &ReservePlugin{
				name:        "reservePlugin",
				failReserve: false,
			}
			registry, profile := initRegistryAndConfig(t, []fwk.Plugin{test.plugin, reservePlugin}...)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(profile),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			// 创建 best effort Pod。
			podName := "test-pod"
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: podName, Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.reject {
				if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Didn't expect the pod to be scheduled. error: %v", err)
				}

				// 校验 Reserve 插件。
				if reservePlugin.numUnreserveCalled != 1 {
					t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 1.", reservePlugin.name, reservePlugin.numUnreserveCalled)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}

				// 校验 Reserve 插件。
				if reservePlugin.numUnreserveCalled != 0 {
					t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 0.", reservePlugin.name, reservePlugin.numUnreserveCalled)
				}
			}

			if test.plugin.numPermitCalled != 1 {
				t.Errorf("Expected the Permit plugin to be called.")
			}
		})
	}
}

// TestUnReservePreBindPlugins 验证 Prebind 插件失败后的 Unreserve 行为。
func TestUnReservePreBindPlugins(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "unreserve-prebind-plugin", nil)

	tests := []struct {
		name       string
		plugin     *PreBindPlugin
		wantReject bool
	}{
		{
			name:       "All Reserve plugins passed, but a PreBind plugin failed",
			wantReject: true,
			plugin: &PreBindPlugin{
				podUIDs:       make(map[types.UID]struct{}),
				rejectPreBind: true,
			},
		},
		{
			name:       "All Reserve plugins passed, and PreBind plugin succeed",
			wantReject: false,
			plugin:     &PreBindPlugin{podUIDs: make(map[types.UID]struct{})},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reservePlugin := &ReservePlugin{
				name:        "reservePlugin",
				failReserve: false,
			}
			requeueGate := &PreEnqueueGateAfterPreBindPlugin{preBind: test.plugin}
			registry, profile := initRegistryAndConfig(t, []fwk.Plugin{requeueGate, test.plugin, reservePlugin}...)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(profile),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			// 创建 pause Pod。
			podName := "test-pod"
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: podName, Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.wantReject {
				if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected a reasons other than Unschedulable, but got: %v", err)
				}

				// 校验 Reserve 插件。
				if reservePlugin.numUnreserveCalled != 1 {
					t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 1.", reservePlugin.name, reservePlugin.numUnreserveCalled)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}

				// 校验 Reserve 插件。
				if reservePlugin.numUnreserveCalled != 0 {
					t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 0.", reservePlugin.name, reservePlugin.numUnreserveCalled)
				}
			}

			if test.plugin.numPreBindCalled != 1 {
				t.Errorf("Expected the Prebind plugin to be called.")
			}
		})
	}
}

// TestUnReserveBindPlugins 验证 Bind 插件失败后的 Unreserve 行为。
func TestUnReserveBindPlugins(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "unreserve-bind-plugin", nil)

	tests := []struct {
		name   string
		plugin *BindPlugin
		fail   bool
	}{
		{
			name:   "All Reserve plugins passed, and Bind plugin succeed",
			fail:   false,
			plugin: &BindPlugin{name: "SucceedBindPlugin"},
		},
		{
			name:   "All Reserve plugins passed, but a Bind plugin failed",
			fail:   false,
			plugin: &BindPlugin{name: "FailedBindPlugin"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reservePlugin := &ReservePlugin{
				name:        "reservePlugin",
				failReserve: false,
			}
			registry, profile := initRegistryAndConfig(t, []fwk.Plugin{test.plugin, reservePlugin}...)

			test.plugin.client = testContext.ClientSet

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(profile),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			// 创建 pause Pod。
			podName := "test-pod"
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: podName, Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a reasons other than Unschedulable, but got: %v", err)
				}

				// 校验 Reserve 插件。
				if reservePlugin.numUnreserveCalled != 1 {
					t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 1.", reservePlugin.name, reservePlugin.numUnreserveCalled)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}

				// 校验 Reserve 插件。
				if reservePlugin.numUnreserveCalled != 0 {
					t.Errorf("Reserve Plugin %s numUnreserveCalled = %d, want 0.", reservePlugin.name, reservePlugin.numUnreserveCalled)
				}
			}

			if test.plugin.numBindCalled != 1 {
				t.Errorf("Expected the Bind plugin to be called.")
			}
		})
	}
}

type pluginInvokeEvent struct {
	pluginName string
	val        int
}

func TestBindPlugin(t *testing.T) {

	var (
		bindPlugin1Name    = "bind-plugin-1"
		bindPlugin2Name    = "bind-plugin-2"
		reservePluginName  = "mock-reserve-plugin"
		postBindPluginName = "mock-post-bind-plugin"
	)

	testContext := testutils.InitTestAPIServer(t, "bind-plugin", nil)

	tests := []struct {
		name                   string
		enabledBindPlugins     []configv1.Plugin
		bindPluginStatuses     []*fwk.Status
		expectBoundByScheduler bool   // 为 true 表示期望由 scheduler 绑定 Pod。
		expectBoundByPlugin    bool   // 为 true 表示期望由插件绑定 Pod。
		expectBindFailed       bool   // 为 true 表示期望插件绑定 Pod 时出错。
		expectBindPluginName   string // 期望执行绑定的插件名称。
		expectInvokeEvents     []pluginInvokeEvent
	}{
		{
			name:                   "bind plugins skipped to bind the pod and scheduler bond the pod",
			enabledBindPlugins:     []configv1.Plugin{{Name: bindPlugin1Name}, {Name: bindPlugin2Name}, {Name: defaultbinder.Name}},
			bindPluginStatuses:     []*fwk.Status{fwk.NewStatus(fwk.Skip, ""), fwk.NewStatus(fwk.Skip, "")},
			expectBoundByScheduler: true,
			expectInvokeEvents:     []pluginInvokeEvent{{pluginName: bindPlugin1Name, val: 1}, {pluginName: bindPlugin2Name, val: 1}, {pluginName: postBindPluginName, val: 1}},
		},
		{
			name:                 "bindplugin2 succeeded to bind the pod",
			enabledBindPlugins:   []configv1.Plugin{{Name: bindPlugin1Name}, {Name: bindPlugin2Name}, {Name: defaultbinder.Name}},
			bindPluginStatuses:   []*fwk.Status{fwk.NewStatus(fwk.Skip, ""), fwk.NewStatus(fwk.Success, "")},
			expectBoundByPlugin:  true,
			expectBindPluginName: bindPlugin2Name,
			expectInvokeEvents:   []pluginInvokeEvent{{pluginName: bindPlugin1Name, val: 1}, {pluginName: bindPlugin2Name, val: 1}, {pluginName: postBindPluginName, val: 1}},
		},
		{
			name:                 "bindplugin1 succeeded to bind the pod",
			enabledBindPlugins:   []configv1.Plugin{{Name: bindPlugin1Name}, {Name: bindPlugin2Name}, {Name: defaultbinder.Name}},
			bindPluginStatuses:   []*fwk.Status{fwk.NewStatus(fwk.Success, ""), fwk.NewStatus(fwk.Success, "")},
			expectBoundByPlugin:  true,
			expectBindPluginName: bindPlugin1Name,
			expectInvokeEvents:   []pluginInvokeEvent{{pluginName: bindPlugin1Name, val: 1}, {pluginName: postBindPluginName, val: 1}},
		},
		{
			name:               "bind plugin fails to bind the pod",
			enabledBindPlugins: []configv1.Plugin{{Name: bindPlugin1Name}, {Name: bindPlugin2Name}, {Name: defaultbinder.Name}},
			expectBindFailed:   true,
			bindPluginStatuses: []*fwk.Status{fwk.NewStatus(fwk.Error, "failed to bind"), fwk.NewStatus(fwk.Success, "")},
			expectInvokeEvents: []pluginInvokeEvent{{pluginName: bindPlugin1Name, val: 1}, {pluginName: reservePluginName, val: 1}},
		},
		{
			name:               "all bind plugins will be skipped(this should not happen for most of the cases)",
			enabledBindPlugins: []configv1.Plugin{{Name: bindPlugin1Name}, {Name: bindPlugin2Name}},
			bindPluginStatuses: []*fwk.Status{fwk.NewStatus(fwk.Skip, ""), fwk.NewStatus(fwk.Skip, "")},
			expectInvokeEvents: []pluginInvokeEvent{{pluginName: bindPlugin1Name, val: 1}, {pluginName: bindPlugin2Name, val: 1}},
		},
	}

	var pluginInvokeEventChan chan pluginInvokeEvent
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bindPlugin1 := &BindPlugin{name: bindPlugin1Name, client: testContext.ClientSet}
			bindPlugin2 := &BindPlugin{name: bindPlugin2Name, client: testContext.ClientSet}
			reservePlugin := &ReservePlugin{name: reservePluginName}
			postBindPlugin := &PostBindPlugin{name: postBindPluginName}

			// 创建插件注册表，注册 reserve、bind 和 postBind 插件。
			registry := frameworkruntime.Registry{
				reservePlugin.Name():  newPlugin(reservePlugin),
				bindPlugin1.Name():    newPlugin(bindPlugin1),
				bindPlugin2.Name():    newPlugin(bindPlugin2),
				postBindPlugin.Name(): newPlugin(postBindPlugin),
			}

			// 配置 unreserve 和 bind 插件。
			cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
				Profiles: []configv1.KubeSchedulerProfile{{
					SchedulerName: ptr.To(v1.DefaultSchedulerName),
					Plugins: &configv1.Plugins{
						MultiPoint: configv1.PluginSet{
							Disabled: []configv1.Plugin{
								{Name: defaultbinder.Name},
							},
						},
						Reserve: configv1.PluginSet{
							Enabled: []configv1.Plugin{{Name: reservePlugin.Name()}},
						},
						Bind: configv1.PluginSet{
							// 将 DefaultBinder 放在最后。
							Enabled:  test.enabledBindPlugins,
							Disabled: []configv1.Plugin{{Name: defaultbinder.Name}},
						},
						PostBind: configv1.PluginSet{
							Enabled: []configv1.Plugin{{Name: postBindPlugin.Name()}},
						},
					},
				}},
			})

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(cfg.Profiles...),
				scheduler.WithFrameworkOutOfTreeRegistry(registry),
			)
			defer teardown()

			pluginInvokeEventChan = make(chan pluginInvokeEvent, 10)

			bindPlugin1.bindStatus = test.bindPluginStatuses[0]
			bindPlugin2.bindStatus = test.bindPluginStatuses[1]

			bindPlugin1.pluginInvokeEventChan = pluginInvokeEventChan
			bindPlugin2.pluginInvokeEventChan = pluginInvokeEventChan
			reservePlugin.pluginInvokeEventChan = pluginInvokeEventChan
			postBindPlugin.pluginInvokeEventChan = pluginInvokeEventChan

			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.expectBoundByScheduler || test.expectBoundByPlugin {
				// bind 插件跳过绑定 Pod。
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Fatalf("Expected the pod to be scheduled. error: %v", err)
				}
				pod, err = testCtx.ClientSet.CoreV1().Pods(pod.Namespace).Get(testCtx.Ctx, pod.Name, metav1.GetOptions{})
				if err != nil {
					t.Errorf("can't get pod: %v", err)
				}
				p1 := bindPlugin1.deepCopy()
				p2 := bindPlugin2.deepCopy()
				if test.expectBoundByScheduler {
					if pod.Annotations[bindPluginAnnotation] != "" {
						t.Errorf("Expected the pod to be bound by scheduler instead of by bindplugin %s", pod.Annotations[bindPluginAnnotation])
					}
					if p1.numBindCalled != 1 || p2.numBindCalled != 1 {
						t.Errorf("Expected each bind plugin to be called once, was called %d and %d times.", p1.numBindCalled, p2.numBindCalled)
					}
				} else {
					if pod.Annotations[bindPluginAnnotation] != test.expectBindPluginName {
						t.Errorf("Expected the pod to be bound by bindplugin %s instead of by bindplugin %s", test.expectBindPluginName, pod.Annotations[bindPluginAnnotation])
					}
					if p1.numBindCalled != 1 {
						t.Errorf("Expected %s to be called once, was called %d times.", p1.Name(), p1.numBindCalled)
					}
					if test.expectBindPluginName == p1.Name() && p2.numBindCalled > 0 {
						// bindplugin1 已成功绑定 Pod，bindplugin2 不应被调用。
						t.Errorf("Expected %s not to be called, was called %d times.", p2.Name(), p2.numBindCalled)
					}
				}
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (done bool, err error) {
					p := postBindPlugin.deepCopy()
					return p.numPostBindCalled == 1, nil
				}); err != nil {
					t.Errorf("Expected the postbind plugin to be called once, was called %d times.", postBindPlugin.numPostBindCalled)
				}
				if reservePlugin.numUnreserveCalled != 0 {
					t.Errorf("Expected unreserve to not be called, was called %d times.", reservePlugin.numUnreserveCalled)
				}
			} else if test.expectBindFailed {
				// bind 插件绑定 Pod 失败。
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but didn't get it. error: %v", err)
				}
				p := postBindPlugin.deepCopy()
				if p.numPostBindCalled > 0 {
					t.Errorf("Didn't expect the postbind plugin to be called %d times.", p.numPostBindCalled)
				}
			} else if postBindPlugin.numPostBindCalled > 0 {
				// 所有 bind 插件均被跳过。
				t.Errorf("Didn't expect the postbind plugin to be called %d times.", postBindPlugin.numPostBindCalled)
			}

			for j := range test.expectInvokeEvents {
				expectEvent := test.expectInvokeEvents[j]
				select {
				case event := <-pluginInvokeEventChan:
					if event.pluginName != expectEvent.pluginName {
						t.Errorf("Expect invoke event %d from plugin %s instead of %s", j, expectEvent.pluginName, event.pluginName)
					}
					if event.val != expectEvent.val {
						t.Errorf("Expect val of invoke event %d to be %d instead of %d", j, expectEvent.val, event.val)
					}
				case <-time.After(time.Second * 30):
					t.Errorf("Waiting for invoke event %d timeout.", j)
				}
			}
		})
	}
}

// TestPostBindPlugin 验证 postbind 插件的调用。
func TestPostBindPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "postbind-plugin", nil)

	tests := []struct {
		name        string
		preBindFail bool
	}{
		{
			name:        "plugin preBind fail",
			preBindFail: true,
		},
		{
			name:        "plugin preBind do not fail",
			preBindFail: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建插件注册表，注册 prebind 和 postbind 插件。
			preBindPlugin := &PreBindPlugin{
				failPreBind: test.preBindFail,
				podUIDs:     make(map[types.UID]struct{}),
			}
			postBindPlugin := &PostBindPlugin{
				name:                  postBindPluginName,
				pluginInvokeEventChan: make(chan pluginInvokeEvent, 1),
			}

			registry, prof := initRegistryAndConfig(t, preBindPlugin, postBindPlugin)
			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.preBindFail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but didn't get it. error: %v", err)
				}
				if postBindPlugin.numPostBindCalled > 0 {
					t.Errorf("Didn't expect the postbind plugin to be called %d times.", postBindPlugin.numPostBindCalled)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}
				select {
				case <-postBindPlugin.pluginInvokeEventChan:
				case <-time.After(time.Second * 15):
					t.Errorf("pluginInvokeEventChan timed out")
				}
				if postBindPlugin.numPostBindCalled == 0 {
					t.Errorf("Expected the postbind plugin to be called, was called %d times.", postBindPlugin.numPostBindCalled)
				}
			}
		})
	}
}

// TestPermitPlugin 验证 permit 插件的调用。
func TestPermitPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "permit-plugin", nil)

	tests := []struct {
		name    string
		fail    bool
		reject  bool
		timeout bool
	}{
		{
			name:    "disable fail, reject and timeout flags",
			fail:    false,
			reject:  false,
			timeout: false,
		},
		{
			name:    "enable fail, disable reject and timeout flags",
			fail:    true,
			reject:  false,
			timeout: false,
		},
		{
			name:    "disable fail and timeout, enable reject flags",
			fail:    false,
			reject:  true,
			timeout: false,
		},
		{
			name:    "enable fail and reject, disable timeout flags",
			fail:    true,
			reject:  true,
			timeout: false,
		},
		{
			name:    "disable fail and reject, disable timeout flags",
			fail:    false,
			reject:  false,
			timeout: true,
		},
		{
			name:    "disable fail and reject, enable timeout flags",
			fail:    false,
			reject:  false,
			timeout: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// 创建插件注册表，仅注册 permit 插件。
			perPlugin := &PermitPlugin{name: permitPluginName}
			registry, prof := initRegistryAndConfig(t, perPlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			perPlugin.failPermit = test.fail
			perPlugin.rejectPermit = test.reject
			perPlugin.timeoutPermit = test.timeout
			perPlugin.waitAndRejectPermit = false
			perPlugin.waitAndAllowPermit = false

			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}
			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but didn't get it. error: %v", err)
				}
			} else {
				if test.reject || test.timeout {
					if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
						t.Errorf("Didn't expect the pod to be scheduled. error: %v", err)
					}
				} else {
					if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
						t.Errorf("Expected the pod to be scheduled. error: %v", err)
					}
				}
			}

			p := perPlugin.deepCopy()
			if p.numPermitCalled == 0 {
				t.Errorf("Expected the permit plugin to be called.")
			}
		})
	}
}

// TestMultiplePermitPlugins 验证多个 permit 插件对同一 Pod 返回 Wait 的行为。
func TestMultiplePermitPlugins(t *testing.T) {
	// 创建测试用插件注册表。
	perPlugin1 := &PermitPlugin{name: "permit-plugin-1"}
	perPlugin2 := &PermitPlugin{name: "permit-plugin-2"}
	registry, prof := initRegistryAndConfig(t, perPlugin1, perPlugin2)

	// 使用测试插件集启动 API server 和 scheduler。
	testCtx, _ := schedulerutils.InitTestSchedulerForFrameworkTest(t, testutils.InitTestAPIServer(t, "multi-permit-plugin", nil), 2, true,
		scheduler.WithProfiles(prof),
		scheduler.WithFrameworkOutOfTreeRegistry(registry))

	// 两个 permit 插件都返回 Wait。
	perPlugin1.timeoutPermit = true
	perPlugin2.timeoutPermit = true

	// 创建测试 Pod。
	podName := "test-pod"
	pod, err := testutils.CreatePausePod(testCtx.ClientSet,
		testutils.InitPausePod(&testutils.PausePodConfig{Name: podName, Namespace: testCtx.NS.Name}))
	if err != nil {
		t.Errorf("Error while creating a test pod: %v", err)
	}

	var waitingPod fwk.WaitingPod
	// 等待测试 Pod 真正进入 waiting 状态。
	wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		waitingPod = perPlugin1.fh.GetWaitingPod(pod.UID)
		return waitingPod != nil, nil
	})

	// 检查待处理的 permit 数量。
	if l := len(waitingPod.GetPendingPlugins()); l != 2 {
		t.Errorf("Expected the number of pending plugins is 2, but got %d", l)
	}

	perPlugin1.allowAllPods()
	// 检查待处理的 permit 数量。
	if l := len(waitingPod.GetPendingPlugins()); l != 1 {
		t.Errorf("Expected the number of pending plugins is 1, but got %d", l)
	}

	perPlugin2.allowAllPods()
	if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
		t.Errorf("Expected the pod to be scheduled. error: %v", err)
	}

	if perPlugin1.numPermitCalled == 0 || perPlugin2.numPermitCalled == 0 {
		t.Errorf("Expected the permit plugin to be called.")
	}
}

// TestPermitPluginsCancelled 验证 Pod 被拒绝时所有 permit 插件都会收到取消。
func TestPermitPluginsCancelled(t *testing.T) {
	// 创建测试用插件注册表。
	perPlugin1 := &PermitPlugin{name: "permit-plugin-1"}
	perPlugin2 := &PermitPlugin{name: "permit-plugin-2"}
	registry, prof := initRegistryAndConfig(t, perPlugin1, perPlugin2)

	// 使用测试插件集启动 API server 和 scheduler。
	testCtx, _ := schedulerutils.InitTestSchedulerForFrameworkTest(t, testutils.InitTestAPIServer(t, "permit-plugins", nil), 2, true,
		scheduler.WithProfiles(prof),
		scheduler.WithFrameworkOutOfTreeRegistry(registry))

	// 两个 permit 插件都返回 Wait。
	perPlugin1.timeoutPermit = true
	perPlugin2.timeoutPermit = true

	// 创建测试 Pod。
	podName := "test-pod"
	pod, err := testutils.CreatePausePod(testCtx.ClientSet,
		testutils.InitPausePod(&testutils.PausePodConfig{Name: podName, Namespace: testCtx.NS.Name}))
	if err != nil {
		t.Errorf("Error while creating a test pod: %v", err)
	}

	var waitingPod fwk.WaitingPod
	// 等待测试 Pod 真正进入 waiting 状态。
	wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		waitingPod = perPlugin1.fh.GetWaitingPod(pod.UID)
		return waitingPod != nil, nil
	})

	perPlugin1.rejectAllPods()
	// 等待 permit 插件被取消。
	err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
		p1 := perPlugin1.deepCopy()
		p2 := perPlugin2.deepCopy()
		return p1.cancelled && p2.cancelled, nil
	})
	if err != nil {
		t.Errorf("Expected all permit plugins to be cancelled")
	}
}

// TestCoSchedulingWithPermitPlugin 验证 permit 插件实现协同调度时的调用行为。
func TestCoSchedulingWithPermitPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "permit-plugin", nil)

	tests := []struct {
		name       string
		waitReject bool
		waitAllow  bool
	}{
		{
			name:       "having wait reject true and wait allow false",
			waitReject: true,
			waitAllow:  false,
		},
		{
			name:       "having wait reject false and wait allow true",
			waitReject: false,
			waitAllow:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			// 创建插件注册表，仅注册 permit 插件。
			permitPlugin := &PermitPlugin{name: permitPluginName}
			registry, prof := initRegistryAndConfig(t, permitPlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			permitPlugin.failPermit = false
			permitPlugin.rejectPermit = false
			permitPlugin.timeoutPermit = false
			permitPlugin.waitAndRejectPermit = test.waitReject
			permitPlugin.waitAndAllowPermit = test.waitAllow

			// 创建两个 Pod：先进入 Permit() 的会等待，后进入的会拒绝或放行它。
			podA, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "pod-a", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating the first pod: %v", err)
			}
			podB, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "pod-b", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating the second pod: %v", err)
			}

			if test.waitReject {
				if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, podA); err != nil {
					t.Errorf("Didn't expect the first pod to be scheduled. error: %v", err)
				}
				if err = testutils.WaitForPodUnschedulable(testCtx.Ctx, testCtx.ClientSet, podB); err != nil {
					t.Errorf("Didn't expect the second pod to be scheduled. error: %v", err)
				}
				if !((permitPlugin.waitingPod == podA.Name && permitPlugin.rejectingPod == podB.Name) ||
					(permitPlugin.waitingPod == podB.Name && permitPlugin.rejectingPod == podA.Name)) {
					t.Errorf("Expect one pod to wait and another pod to reject instead %s waited and %s rejected.",
						permitPlugin.waitingPod, permitPlugin.rejectingPod)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, podA); err != nil {
					t.Errorf("Expected the first pod to be scheduled. error: %v", err)
				}
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, podB); err != nil {
					t.Errorf("Expected the second pod to be scheduled. error: %v", err)
				}
				if !((permitPlugin.waitingPod == podA.Name && permitPlugin.allowingPod == podB.Name) ||
					(permitPlugin.waitingPod == podB.Name && permitPlugin.allowingPod == podA.Name)) {
					t.Errorf("Expect one pod to wait and another pod to allow instead %s waited and %s allowed.",
						permitPlugin.waitingPod, permitPlugin.allowingPod)
				}
			}

			p := permitPlugin.deepCopy()
			if p.numPermitCalled == 0 {
				t.Errorf("Expected the permit plugin to be called.")
			}
		})
	}
}

// TestFilterPlugin 验证 filter 插件的调用。
func TestFilterPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "filter-plugin", nil)

	tests := []struct {
		name string
		fail bool
	}{
		{
			name: "fail filter plugin",
			fail: true,
		},
		{
			name: "do not fail filter plugin",
			fail: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建测试用插件注册表。 Register only a filter plugin.
			filterPlugin := &FilterPlugin{}
			registry, prof := initRegistryAndConfig(t, filterPlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 1, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			filterPlugin.failFilter = test.fail
			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but got: %v", err)
				}
				if filterPlugin.numFilterCalled < 1 {
					t.Errorf("Expected the filter plugin to be called at least 1 time, but got %v.", filterPlugin.numFilterCalled)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}
				if filterPlugin.numFilterCalled != 1 {
					t.Errorf("Expected the filter plugin to be called 1 time, but got %v.", filterPlugin.numFilterCalled)
				}
			}
		})
	}
}

// TestPreScorePlugin 验证 pre-score 插件的调用。
func TestPreScorePlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "pre-score-plugin", nil)

	tests := []struct {
		name string
		fail bool
	}{
		{
			name: "fail preScore plugin",
			fail: true,
		},
		{
			name: "do not fail preScore plugin",
			fail: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 创建仅注册 pre-score 插件的测试用注册表。
			preScorePlugin := &PreScorePlugin{}
			registry, prof := initRegistryAndConfig(t, preScorePlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			preScorePlugin.failPreScore = test.fail
			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet,
				testutils.InitPausePod(&testutils.PausePodConfig{Name: "test-pod", Namespace: testCtx.NS.Name}))
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if test.fail {
				if err = wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false,
					testutils.PodSchedulingError(testCtx.ClientSet, pod.Namespace, pod.Name)); err != nil {
					t.Errorf("Expected a scheduling error, but got: %v", err)
				}
			} else {
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, pod); err != nil {
					t.Errorf("Expected the pod to be scheduled. error: %v", err)
				}
			}

			if preScorePlugin.numPreScoreCalled == 0 {
				t.Errorf("Expected the pre-score plugin to be called.")
			}
		})
	}
}

// TestPreEnqueuePlugin 验证 enqueue 插件的调用。
func TestPreEnqueuePlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "enqueue-plugin", nil)

	tests := []struct {
		name         string
		pod          *v1.Pod
		admitEnqueue bool
	}{
		{
			name:         "pod is admitted to enqueue",
			pod:          st.MakePod().Name("p").Namespace(testContext.NS.Name).Container("pause").Obj(),
			admitEnqueue: true,
		},
		{
			name:         "pod is not admitted to enqueue",
			pod:          st.MakePod().Name("p").Namespace(testContext.NS.Name).SchedulingGates([]string{"foo"}).Container("pause").Obj(),
			admitEnqueue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建仅注册 filter 插件的测试用注册表。
			enqueuePlugin := &PreEnqueuePlugin{}
			// 接入 preFilterPlugin 以验证它是否被跳过。
			preFilterPlugin := &PreFilterPlugin{}
			registry, prof := initRegistryAndConfig(t, enqueuePlugin, preFilterPlugin)

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 1, true,
				scheduler.WithProfiles(prof),
				scheduler.WithFrameworkOutOfTreeRegistry(registry))
			defer teardown()

			enqueuePlugin.admit = tt.admitEnqueue
			// 创建 best effort Pod。
			pod, err := testutils.CreatePausePod(testCtx.ClientSet, tt.pod)
			if err != nil {
				t.Errorf("Error while creating a test pod: %v", err)
			}

			if tt.admitEnqueue {
				if err := testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, testCtx.ClientSet, pod, 10*time.Second); err != nil {
					t.Errorf("Expected the pod to be schedulable, but got: %v", err)
				}
				// 同时验证 enqueuePlugin 被调用。
				if enqueuePlugin.called.Load() == 0 {
					t.Errorf("Expected the enqueuePlugin plugin to be called at least once, but got 0")
				}
			} else {
				if err := testutils.WaitForPodSchedulingGated(testCtx.Ctx, testCtx.ClientSet, pod, 10*time.Second); err != nil {
					t.Errorf("Expected the pod to be scheduling waiting, but got: %v", err)
				}
				// 同时验证 preFilterPlugin 未被调用。
				if called := preFilterPlugin.numPreFilterCalled.Load(); called != 0 {
					t.Errorf("Expected the preFilter plugin not to be called, but got %v", called)
				}
			}
		})
	}
}

// TestPreemptWithPermitPlugin 验证有 permit 插件时 waitingPod 的抢占行为：
//   - waitingPod 被抢占时，应从内部 waitingPods map 移除，但不物理删除
//   - 会触发移动 unschedulable Pod，但不包含 waitingPod 自身
//   - waitingPod 被外部删除时，会触发移动 unschedulable Pod
func TestPreemptWithPermitPlugin(t *testing.T) {
	testContext := testutils.InitTestAPIServer(t, "preempt-with-permit-plugin", nil)

	ns := testContext.NS.Name
	lowPriority, midPriority, highPriority := int32(100), int32(150), int32(300)
	resReq := map[v1.ResourceName]string{
		v1.ResourceCPU:    "200m",
		v1.ResourceMemory: "200",
	}
	preemptorReq := map[v1.ResourceName]string{
		v1.ResourceCPU:    "400m",
		v1.ResourceMemory: "400",
	}

	nodeRes := map[v1.ResourceName]string{
		v1.ResourcePods:   "32",
		v1.ResourceCPU:    "500m",
		v1.ResourceMemory: "500",
	}

	tests := []struct {
		name                   string
		deleteWaitingPod       bool
		maxNumWaitingPodCalled int
		runningPod             *v1.Pod
		waitingPod             *v1.Pod
		preemptor              *v1.Pod
	}{
		{
			name:                   "waiting pod is not physically deleted upon preemption",
			maxNumWaitingPodCalled: 2,
			runningPod:             st.MakePod().Name("running-pod").Namespace(ns).Priority(lowPriority).Req(resReq).ZeroTerminationGracePeriod().Obj(),
			waitingPod:             st.MakePod().Name("waiting-pod").Namespace(ns).Priority(midPriority).Req(resReq).ZeroTerminationGracePeriod().Obj(),
			preemptor:              st.MakePod().Name("preemptor-pod").Namespace(ns).Priority(highPriority).Req(preemptorReq).ZeroTerminationGracePeriod().Obj(),
		},
		{
			// waitingPod 已经历过一轮调度，被抢占后是否仍可调度未知，
			// 因此需要重新调度，避免它滞留在 unschedulable Pod 池中。
			name:                   "rejecting a waiting pod to trigger retrying unschedulable pods immediately, and the waiting pod itself will be retried",
			maxNumWaitingPodCalled: 2,
			waitingPod:             st.MakePod().Name("waiting-pod").Namespace(ns).Priority(lowPriority).Req(resReq).ZeroTerminationGracePeriod().Obj(),
			preemptor:              st.MakePod().Name("preemptor-pod").Namespace(ns).Priority(highPriority).Req(preemptorReq).ZeroTerminationGracePeriod().Obj(),
		},
		{
			name:                   "deleting a waiting pod to trigger retrying unschedulable pods immediately",
			deleteWaitingPod:       true,
			maxNumWaitingPodCalled: 1,
			waitingPod:             st.MakePod().Name("waiting-pod").Namespace(ns).Priority(lowPriority).Req(resReq).ZeroTerminationGracePeriod().Obj(),
			preemptor:              st.MakePod().Name("preemptor-pod").Namespace(ns).Priority(lowPriority).Req(preemptorReq).ZeroTerminationGracePeriod().Obj(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建测试用插件注册表。 Register a permit and a filter plugin.
			permitPlugin := &PermitPlugin{}
			// 注入 fake filter 插件，利用其内部 numFilterCalled 统计 Pod 被尝试调度的次数。
			filterPlugin := &FilterPlugin{numCalledPerPod: make(map[string]int)}
			registry := frameworkruntime.Registry{
				permitPluginName: newPlugin(permitPlugin),
				filterPluginName: newPlugin(filterPlugin),
			}

			// 在 profile 中配置 permit 和 filter 插件。
			cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
				Profiles: []configv1.KubeSchedulerProfile{
					{
						SchedulerName: ptr.To(v1.DefaultSchedulerName),
						Plugins: &configv1.Plugins{
							Permit: configv1.PluginSet{
								Enabled: []configv1.Plugin{
									{Name: permitPluginName},
								},
							},
							Filter: configv1.PluginSet{
								// 确保 fake filter 插件总被调用，否则 noderesources 会先失败并退出 Filter 阶段。
								Enabled: []configv1.Plugin{
									{Name: filterPluginName},
									{Name: noderesources.Name},
								},
								Disabled: []configv1.Plugin{
									{Name: noderesources.Name},
								},
							},
						},
					},
				},
			})

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 0, true,
				scheduler.WithProfiles(cfg.Profiles...),
				scheduler.WithFrameworkOutOfTreeRegistry(registry),
			)
			defer teardown()

			_, err := testutils.CreateAndWaitForNodesInCache(testCtx, "test-node", st.MakeNode().Capacity(nodeRes), 1)
			if err != nil {
				t.Fatal(err)
			}

			permitPlugin.waitAndAllowPermit = true
			permitPlugin.waitingPod = "waiting-pod"

			if r := tt.runningPod; r != nil {
				if _, err := testutils.CreatePausePod(testCtx.ClientSet, r); err != nil {
					t.Fatalf("Error while creating the running pod: %v", err)
				}
				// 等待该 Pod 完成调度。
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, r); err != nil {
					t.Fatalf("The running pod is expected to be scheduled: %v", err)
				}
			}

			if w := tt.waitingPod; w != nil {
				if _, err := testutils.CreatePausePod(testCtx.ClientSet, w); err != nil {
					t.Fatalf("Error while creating the waiting pod: %v", err)
				}
				// 等待 waitingPod 真正进入等待状态。
				if err := wait.PollUntilContextTimeout(testCtx.Ctx, 10*time.Millisecond, 30*time.Second, false, func(ctx context.Context) (bool, error) {
					w := false
					permitPlugin.fh.IterateOverWaitingPods(func(wp fwk.WaitingPod) { w = true })
					return w, nil
				}); err != nil {
					t.Fatalf("The waiting pod is expected to be waiting: %v", err)
				}
			}

			if p := tt.preemptor; p != nil {
				if _, err := testutils.CreatePausePod(testCtx.ClientSet, p); err != nil {
					t.Fatalf("Error while creating the preemptor pod: %v", err)
				}
				// 按需删除 waitingPod。
				if w := tt.waitingPod; w != nil && tt.deleteWaitingPod {
					if err := testutils.DeletePod(testCtx.ClientSet, w.Name, w.Namespace); err != nil {
						t.Fatalf("Error while deleting the waiting pod: %v", err)
					}
				}
				if err = testutils.WaitForPodToSchedule(testCtx.Ctx, testCtx.ClientSet, p); err != nil {
					t.Fatalf("Expected the preemptor pod to be scheduled. error: %v", err)
				}
			}

			if w := tt.waitingPod; w != nil {
				if err := wait.PollUntilContextTimeout(testCtx.Ctx, 200*time.Millisecond, wait.ForeverTestTimeout, false, func(ctx context.Context) (bool, error) {
					w := false
					permitPlugin.fh.IterateOverWaitingPods(func(wp fwk.WaitingPod) { w = true })
					return !w, nil
				}); err != nil {
					t.Fatalf("Expected the waiting pod to get preempted.")
				}

				p := filterPlugin.deepCopy()
				waitingPodCalled := p.numCalledPerPod[fmt.Sprintf("%v/%v", w.Namespace, w.Name)]
				if waitingPodCalled > tt.maxNumWaitingPodCalled {
					t.Fatalf("Expected the waiting pod to be called %v times at most, but got %v", tt.maxNumWaitingPodCalled, waitingPodCalled)
				}

				if !tt.deleteWaitingPod {
					// 期望 waitingPod 仍然存在。
					if _, err := testutils.GetPod(testCtx.ClientSet, w.Name, w.Namespace); err != nil {
						t.Error("Get waiting pod in waiting pod failed.")
					}
				}

				if permitPlugin.numPermitCalled == 0 {
					t.Errorf("Expected the permit plugin to be called.")
				}
			}

			if r := tt.runningPod; r != nil {
				// 期望 runningPod 被物理删除。
				if _, err = testutils.GetPod(testCtx.ClientSet, r.Name, r.Namespace); err == nil {
					t.Error("The running pod still exists.")
				} else if !errors.IsNotFound(err) {
					t.Errorf("Get running pod failed: %v", err)
				}
			}
		})
	}
}

const (
	jobPluginName = "job plugin"
)

var _ fwk.PreFilterPlugin = &JobPlugin{}
var _ fwk.PostBindPlugin = &PostBindPlugin{}

type JobPlugin struct {
	podLister     listersv1.PodLister
	podsActivated chan struct{}
}

func (j *JobPlugin) Name() string {
	return jobPluginName
}

func (j *JobPlugin) PreFilter(_ context.Context, _ fwk.CycleState, p *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	labelSelector := labels.SelectorFromSet(labels.Set{"driver": ""})
	driverPods, err := j.podLister.Pods(p.Namespace).List(labelSelector)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if len(driverPods) == 0 {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "unable to find driver pod")
	}
	return nil, nil
}

func (j *JobPlugin) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

func (j *JobPlugin) PostBind(_ context.Context, state fwk.CycleState, p *v1.Pod, nodeName string) {
	if _, ok := p.Labels["driver"]; !ok {
		return
	}

	// 如果是 driver Pod，主动将 executor Pod 转入 activeQ 以加速调度。
	labelSelector := labels.SelectorFromSet(labels.Set{"executor": ""})
	podsToActivate, err := j.podLister.Pods(p.Namespace).List(labelSelector)
	if err == nil && len(podsToActivate) != 0 {
		c, err := state.Read(framework.PodsToActivateKey)
		if err == nil {
			if s, ok := c.(*framework.PodsToActivate); ok {
				s.Lock()
				for _, pod := range podsToActivate {
					namespacedName := fmt.Sprintf("%v/%v", pod.Namespace, pod.Name)
					s.Map[namespacedName] = pod
				}
				s.Unlock()
				j.podsActivated <- struct{}{}
			}
		}
	}
}

// TestActivatePods 模拟典型的 Spark 作业流程：
//   - 创建 N 个 executor Pod，因缺少 driver Pod 而保持 pending
//   - driver Pod 创建并调度后，主动将 executor 转入 activeQ，加速整个作业流程。
func TestActivatePods(t *testing.T) {
	var jobPlugin *JobPlugin
	// 创建测试用插件注册表。 Register a Job plugin.
	registry := frameworkruntime.Registry{jobPluginName: func(_ context.Context, _ runtime.Object, fh fwk.Handle) (fwk.Plugin, error) {
		jobPlugin = &JobPlugin{
			podLister:     fh.SharedInformerFactory().Core().V1().Pods().Lister(),
			podsActivated: make(chan struct{}, 10),
		}
		return jobPlugin, nil
	}}

	// 配置待测 filter 插件。
	cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
		Profiles: []configv1.KubeSchedulerProfile{{
			SchedulerName: ptr.To(v1.DefaultSchedulerName),
			Plugins: &configv1.Plugins{
				PreFilter: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: jobPluginName},
					},
				},
				PostBind: configv1.PluginSet{
					Enabled: []configv1.Plugin{
						{Name: jobPluginName},
					},
				},
			},
		}},
	})

	// 使用测试插件集启动 API server 和 scheduler。
	testCtx, _ := schedulerutils.InitTestSchedulerForFrameworkTest(t, testutils.InitTestAPIServer(t, "job-plugin", nil), 1, true,
		scheduler.WithProfiles(cfg.Profiles...),
		scheduler.WithFrameworkOutOfTreeRegistry(registry))

	cs := testCtx.ClientSet
	ns := testCtx.NS.Name
	pause := imageutils.GetPauseImageName()

	// 先创建两个 executor Pod。
	var pods []*v1.Pod
	for i := 1; i <= 2; i++ {
		name := fmt.Sprintf("executor-%v", i)
		executor := st.MakePod().Name(name).Namespace(ns).Label("executor", "").Container(pause).Obj()
		pods = append(pods, executor)
		if _, err := cs.CoreV1().Pods(executor.Namespace).Create(testCtx.Ctx, executor, metav1.CreateOptions{}); err != nil {
			t.Fatalf("Failed to create pod %v: %v", executor.Name, err)
		}
	}

	// 等待两个 executor Pod 变为 unschedulable。
	for _, pod := range pods {
		if err := testutils.WaitForPodUnschedulable(testCtx.Ctx, cs, pod); err != nil {
			t.Errorf("Failed to wait for Pod %v to be unschedulable: %v", pod.Name, err)
		}
	}

	// 创建 driver Pod。
	driver := st.MakePod().Name("driver").Namespace(ns).Label("driver", "").Container(pause).Obj()
	pods = append(pods, driver)
	if _, err := cs.CoreV1().Pods(driver.Namespace).Create(testCtx.Ctx, driver, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Failed to create pod %v: %v", driver.Name, err)
	}

	// 验证所有 Pod 均完成调度。
	for _, pod := range pods {
		if err := testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, cs, pod, wait.ForeverTestTimeout); err != nil {
			t.Fatalf("Failed to wait for Pod %v to be schedulable: %v", pod.Name, err)
		}
	}

	// 最后验证 Pod 激活逻辑确实被调用。
	select {
	case <-jobPlugin.podsActivated:
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("JobPlugin's pods activation logic wasn't called")
	}
}

var _ fwk.PreEnqueuePlugin = &SchedulingGatesPluginWithEvents{}
var _ fwk.EnqueueExtensions = &SchedulingGatesPluginWithEvents{}
var _ fwk.PreEnqueuePlugin = &SchedulingGatesPluginWOEvents{}
var _ fwk.EnqueueExtensions = &SchedulingGatesPluginWOEvents{}

const (
	schedulingGatesPluginWithEvents = "scheduling-gates-with-events"
	schedulingGatesPluginWOEvents   = "scheduling-gates-without-events"
)

type SchedulingGatesPluginWithEvents struct {
	called int
	schedulinggates.SchedulingGates
}

func (pl *SchedulingGatesPluginWithEvents) Name() string {
	return schedulingGatesPluginWithEvents
}

func (pl *SchedulingGatesPluginWithEvents) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	pl.called++
	klog.FromContext(ctx).Info("PreEnqueue is called", "pod", klog.KObj(p), "count", pl.called)
	return pl.SchedulingGates.PreEnqueue(ctx, p)
}

func (pl *SchedulingGatesPluginWithEvents) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.Pod, ActionType: fwk.Update}},
	}, nil
}

type SchedulingGatesPluginWOEvents struct {
	called int
	schedulinggates.SchedulingGates
}

func (pl *SchedulingGatesPluginWOEvents) Name() string {
	return schedulingGatesPluginWOEvents
}

func (pl *SchedulingGatesPluginWOEvents) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	pl.called++
	klog.FromContext(ctx).Info("PreEnqueue is called", "pod", klog.KObj(p), "count", pl.called)
	return pl.SchedulingGates.PreEnqueue(ctx, p)
}

func (pl *SchedulingGatesPluginWOEvents) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return nil, nil
}

// TestPreEnqueuePluginEventsToRegister 验证 PreEnqueue 插件注册 nil events 时行为符合预期。
func TestPreEnqueuePluginEventsToRegister(t *testing.T) {
	num := func(pl fwk.Plugin) int {
		switch item := pl.(type) {
		case *SchedulingGatesPluginWithEvents:
			return item.called
		case *SchedulingGatesPluginWOEvents:
			return item.called
		default:
			t.Error("unsupported plugin")
		}
		return 0
	}

	tests := []struct {
		name       string
		withEvents bool
		// count 为 PreEnqueue() 的预期调用次数。
		count             int
		expectedScheduled bool
	}{
		{
			name:              "preEnqueue plugin without event registered",
			withEvents:        false,
			count:             2,
			expectedScheduled: false,
		},
		{
			name:              "preEnqueue plugin with event registered",
			withEvents:        true,
			count:             3,
			expectedScheduled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedScheduled := tt.expectedScheduled

			testContext := testutils.InitTestAPIServer(t, "preenqueue-plugin", nil)
			// 每次使用新插件以清空计数。
			var plugin fwk.PreEnqueuePlugin
			if tt.withEvents {
				plugin = &SchedulingGatesPluginWithEvents{SchedulingGates: schedulinggates.SchedulingGates{}}
			} else {
				plugin = &SchedulingGatesPluginWOEvents{SchedulingGates: schedulinggates.SchedulingGates{}}
			}

			registry := frameworkruntime.Registry{
				plugin.Name(): newPlugin(plugin),
			}

			// 配置待测插件。
			cfg := configtesting.V1ToInternalWithDefaults(t, configv1.KubeSchedulerConfiguration{
				Profiles: []configv1.KubeSchedulerProfile{{
					SchedulerName: ptr.To(v1.DefaultSchedulerName),
					Plugins: &configv1.Plugins{
						PreEnqueue: configv1.PluginSet{
							Enabled: []configv1.Plugin{
								{Name: plugin.Name()},
							},
							Disabled: []configv1.Plugin{
								{Name: "*"},
							},
						},
					},
				}},
			})

			testCtx, teardown := schedulerutils.InitTestSchedulerForFrameworkTest(t, testContext, 2, true,
				scheduler.WithProfiles(cfg.Profiles...),
				scheduler.WithFrameworkOutOfTreeRegistry(registry),
			)
			defer teardown()

			t.Log("Create the gated pod")

			// 创建带 schedulingGates 的 Pod。
			gatedPod := st.MakePod().Name("p").Namespace(testContext.NS.Name).
				SchedulingGates([]string{"foo"}).
				PodAffinity("kubernetes.io/hostname", &metav1.LabelSelector{MatchLabels: map[string]string{"foo": "bar"}}, st.PodAffinityWithRequiredReq).
				Container("pause").Obj()
			gatedPod, err := testutils.CreatePausePod(testCtx.ClientSet, gatedPod)
			if err != nil {
				t.Errorf("Error while creating a gated pod: %v", err)
				return
			}

			if err := testutils.WaitForPodSchedulingGated(testCtx.Ctx, testCtx.ClientSet, gatedPod, 10*time.Second); err != nil {
				t.Errorf("Expected the pod to be gated, but got: %v", err)
				return
			}
			if num(plugin) != 1 {
				t.Errorf("Expected the preEnqueue plugin to be called once, but got %v", num(plugin))
				return
			}

			t.Log("Create the pause pod")

			// 创建 best effort Pod。
			pausePod, err := testutils.CreatePausePod(testCtx.ClientSet, testutils.InitPausePod(&testutils.PausePodConfig{
				Name:      "pause-pod",
				Namespace: testCtx.NS.Name,
				Labels:    map[string]string{"foo": "bar"},
			}))
			if err != nil {
				t.Errorf("Error while creating a pod: %v", err)
				return
			}

			// 等待该 Pod 可调度。
			if err := testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, testCtx.ClientSet, pausePod, 10*time.Second); err != nil {
				t.Errorf("Expected the pod to be schedulable, but got: %v", err)
				return
			}

			t.Log("Update the pause pod")

			// 更新 Pod，若插件注册了事件，将触发重新入队。
			pausePod, err = testCtx.ClientSet.CoreV1().Pods(pausePod.Namespace).Get(testCtx.Ctx, pausePod.Name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("Error while getting a pod: %v", err)
				return
			}
			pausePod.Annotations = map[string]string{"foo": "bar"}
			_, err = testCtx.ClientSet.CoreV1().Pods(pausePod.Namespace).Update(testCtx.Ctx, pausePod, metav1.UpdateOptions{})
			if err != nil {
				t.Errorf("Error while updating a pod: %v", err)
				return
			}

			// scheduling gates 仍存在，Pod 应保持不可调度，此次重排实际上是浪费。
			if err := testutils.WaitForPodSchedulingGated(testCtx.Ctx, testCtx.ClientSet, gatedPod, 10*time.Second); err != nil {
				t.Errorf("Expected the pod to be gated, but got: %v", err)
				return
			}
			if num(plugin) != tt.count {
				t.Errorf("Expected the preEnqueue plugin to be called %v, but got %v", tt.count, num(plugin))
				return
			}

			t.Log("Remove the scheduling gate")

			// 移除 gated Pod 的 scheduling gates。
			gatedPod, err = testCtx.ClientSet.CoreV1().Pods(gatedPod.Namespace).Get(testCtx.Ctx, gatedPod.Name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("Error while getting a pod: %v", err)
				return
			}
			gatedPod.Spec.SchedulingGates = nil
			_, err = testCtx.ClientSet.CoreV1().Pods(gatedPod.Namespace).Update(testCtx.Ctx, gatedPod, metav1.UpdateOptions{})
			if err != nil {
				t.Errorf("Error while updating a pod: %v", err)
				return
			}

			if expectedScheduled {
				if err := testutils.WaitForPodToScheduleWithTimeout(testCtx.Ctx, testCtx.ClientSet, gatedPod, 10*time.Second); err != nil {
					t.Errorf("Expected the pod to be schedulable, but got: %v", err)
				}
				return
			}
			// 等待 schedulerQueue 处理完 podUpdate 事件。
			time.Sleep(time.Second)
			// schedulingGates 插件未注册 podUpdate 事件时，Pod 不应被调度。
			if err := testutils.WaitForPodSchedulingGated(testCtx.Ctx, testCtx.ClientSet, gatedPod, 10*time.Second); err != nil {
				t.Errorf("Expected the pod to be gated, but got: %v", err)
				return
			}
		})
	}
}
