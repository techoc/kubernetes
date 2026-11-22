/*
Copyright 2025 The Kubernetes Authors.

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

package nodedeclaredfeatures

import (
	"context"
	"fmt"
	"slices"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	versionutil "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/version"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/component-helpers/resource"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// Name 是该插件在插件注册表和配置文件中使用的名称。
	Name = names.NodeDeclaredFeatures
	// preFilterStateKey 是 CycleState 中存放 Pod 所需特性的键。
	preFilterStateKey fwk.StateKey = "PreFilter" + Name
	// errReasonUnsatisfiedRequirements 是节点声明的特性不满足 Pod 所需特性时的状态原因。
	errReasonUnsatisfiedRequirements = "node(s) didn't match Pod's required features"
)

// preFilterState 在 PreFilter 阶段计算，在 Filter 阶段使用。
type preFilterState struct {
	reqs ndf.FeatureSet
}

// Clone 实现 StateData 接口。
// 状态不可变，直接共享即可。
func (s *preFilterState) Clone() fwk.StateData {
	return s
}

// NodeDeclaredFeatures 检查节点是否具备 Pod 所需的全部特性。
type NodeDeclaredFeatures struct {
	ndfFramework                                       *ndf.Framework
	version                                            *versionutil.Version
	enabled                                            bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
}

var _ fwk.PreFilterPlugin = &NodeDeclaredFeatures{}
var _ fwk.FilterPlugin = &NodeDeclaredFeatures{}
var _ fwk.EnqueueExtensions = &NodeDeclaredFeatures{}
var _ fwk.SignPlugin = &NodeDeclaredFeatures{}

// Name 返回插件名称，用于日志等场景。
func (pl *NodeDeclaredFeatures) Name() string {
	return Name
}

// New 初始化并返回一个 NodeDeclaredFeatures 插件实例。
func New(ctx context.Context, plArgs runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	if !fts.EnableNodeDeclaredFeatures {
		// 特性未开启时返回空实例，各扩展点会直接 Skip。
		return &NodeDeclaredFeatures{}, nil
	}
	ndfFramework := ndf.DefaultFramework
	ver, err := versionutil.Parse(version.Get().String())
	if err != nil {
		return nil, fmt.Errorf("failed to parse version: %w", err)
	}
	return &NodeDeclaredFeatures{ndfFramework: ndfFramework, version: ver, enabled: true, enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption}, nil
}

// PreFilter 推断 Pod 所需特性并写入 CycleState，无需求时跳过。
func (pl *NodeDeclaredFeatures) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	// 延迟扩容的 Pod 由抢占流程单独处理，这里直接跳过。
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	if !pl.enabled {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	// 此时 Pod status 尚未更新，只需把 spec 传给特性推断库。
	podInfo := &ndf.PodInfo{Spec: &pod.Spec}
	reqs, err := pl.ndfFramework.InferForPodScheduling(podInfo, pl.version)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	if reqs.IsEmpty() {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	cycleState.Write(preFilterStateKey, &preFilterState{reqs: reqs})
	return nil, nil
}

// PreFilterExtensions 返回预过滤扩展；本插件不需要逐节点增减状态的钩子。
func (pl *NodeDeclaredFeatures) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// Filter 检查节点声明的特性是否覆盖 Pod 所需的特性。
func (pl *NodeDeclaredFeatures) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	if !pl.enabled {
		return nil
	}
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}
	// 这里只需判断是否为子集，避免 ndf.MatchNodeFeatureSet 计算完整特性差异的开销。
	isMatch, err := s.reqs.IsSubset(nodeInfo.GetNodeDeclaredFeatures())
	if err != nil {
		return fwk.AsStatus(err)
	}
	if !isMatch {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, errReasonUnsatisfiedRequirements)
	}
	return nil
}

// SignPod 为 Pod 生成其所需特性的签名片段。
func (pl *NodeDeclaredFeatures) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	podInfo := &ndf.PodInfo{Spec: &pod.Spec}
	fs, err := pl.ndfFramework.InferForPodScheduling(podInfo, pl.version)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	return []fwk.SignFragment{
		{Key: fwk.FeaturesSignerName, Value: fs.String()},
	}, nil
}

// EventsToRegister 返回可能让 Pod 变得可调度的事件，EnqueueExtensions 接口要求实现此方法。
func (pl *NodeDeclaredFeatures) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	if !pl.enabled {
		return nil, nil
	}
	return []fwk.ClusterEventWithHint{
		{
			Event:          fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeDeclaredFeature},
			QueueingHintFn: pl.isSchedulableAfterNodeChange,
		},
		{
			Event:          fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.Update},
			QueueingHintFn: pl.isSchedulableAfterTargetPodUpdate,
		},
	}, nil
}

// getPreFilterState 从 CycleState 中读取并校验 PreFilter 阶段保存的特性需求。
func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		return nil, fmt.Errorf("error reading %q from cycle-state: %w", preFilterStateKey, err)
	}
	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("invalid PreFilter state, got type %T, expected %T", c, &preFilterState{})
	}
	return s, nil
}

// isSchedulableAfterTargetPodUpdate 判断目标 Pod 更新后其特性需求是否变化，
// 仅在需求变化时才有必要重新入队。
func (pl *NodeDeclaredFeatures) isSchedulableAfterTargetPodUpdate(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldPod, newPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	oldPodInfo := &ndf.PodInfo{Spec: &oldPod.Spec}
	newPodInfo := &ndf.PodInfo{Spec: &newPod.Spec}
	oldReqs, err := pl.ndfFramework.InferForPodScheduling(oldPodInfo, pl.version)
	if err != nil {
		logger.Error(err, "failed to infer old pod feature requirements", "pod", klog.KObj(pod))
		return fwk.Queue, err
	}
	newReqs, err := pl.ndfFramework.InferForPodScheduling(newPodInfo, pl.version)
	if err != nil {
		logger.Error(err, "failed to infer new pod feature requirements", "pod", klog.KObj(pod))
		return fwk.Queue, err
	}
	if newReqs.Equal(oldReqs) {
		logger.V(5).Info("pod feature requirements didn't change, skipping queueing", "pod", klog.KObj(newPod))
		return fwk.QueueSkip, nil
	}
	logger.V(4).Info("pod feature requirements changed, queueing", "pod", klog.KObj(pod))
	return fwk.Queue, nil
}

// isSchedulableAfterNodeChange 判断节点声明特性变化后是否值得重新入队；
// 特性未变时（如新增节点），跳过入队以减少无谓调度。
func (pl *NodeDeclaredFeatures) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	oldNode, newNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}
	if oldNode != nil && slices.Equal(oldNode.Status.DeclaredFeatures, newNode.Status.DeclaredFeatures) {
		logger.V(5).Info("node's declared features didn't change, skipping queueing", "pod", klog.KObj(pod), "node", klog.KObj(newNode))
		return fwk.QueueSkip, nil
	}
	logger.V(4).Info("Node declared features updated, queueing", "pod", klog.KObj(pod), "node", klog.KObj(newNode))
	return fwk.Queue, nil
}
