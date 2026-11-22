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

// Package noderesources 提供节点资源调度插件，负责检查节点资源是否够用并按资源使用率打分。
package noderesources

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/resource"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

var _ fwk.PreFilterPlugin = &Fit{}
var _ fwk.FilterPlugin = &Fit{}
var _ fwk.EnqueueExtensions = &Fit{}
var _ fwk.PreScorePlugin = &Fit{}
var _ fwk.ScorePlugin = &Fit{}
var _ fwk.SignPlugin = &Fit{}
var _ fwk.PlacementScorePlugin = &Fit{}

const (
	// Name 是插件在注册表和配置中使用的名字。
	Name = names.NodeResourcesFit

	// preFilterStateKey 是 CycleState 中存放 NodeResourcesFit 预计算数据的键。
	// 键名带上插件名，可避免与其他插件冲突。
	preFilterStateKey = "PreFilter" + Name

	// preScoreStateKey 是 CycleState 中存放 NodeResourcesFit 打分预计算数据的键。
	preScoreStateKey = "PreScore" + Name
)

// nodeResourceStrategyTypeMap 把评分策略类型映射到对应的打分器构造函数。
var nodeResourceStrategyTypeMap = map[config.ScoringStrategyType]scorer{
	config.LeastAllocated: func(strategy *config.ScoringStrategy) *resourceAllocationScorer {
		resources := strategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.LeastAllocated),
			scorer:    leastResourceScorer(resources),
			resources: resources,
		}
	},
	config.MostAllocated: func(strategy *config.ScoringStrategy) *resourceAllocationScorer {
		resources := strategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.MostAllocated),
			scorer:    mostResourceScorer(resources),
			resources: resources,
		}
	},
	config.RequestedToCapacityRatio: func(strategy *config.ScoringStrategy) *resourceAllocationScorer {
		resources := strategy.Resources
		return &resourceAllocationScorer{
			Name:      string(config.RequestedToCapacityRatio),
			scorer:    requestedToCapacityRatioScorer(resources, strategy.RequestedToCapacityRatio.Shape),
			resources: resources,
		}
	},
}

// Fit 是检查节点资源是否充足的插件，同时提供基于资源使用率的打分能力。
type Fit struct {
	ignoredResources                                   sets.Set[string]
	ignoredResourceGroups                              sets.Set[string]
	enableInPlacePodVerticalScaling                    bool
	enablePodLevelResources                            bool
	enableDRAExtendedResource                          bool
	enableInPlacePodLevelResourcesVerticalScaling      bool
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	handle                                             fwk.Handle
	*resourceAllocationScorer
	placementScorer *resourceAllocationScorer
}

// ScoreExtensions 返回 nil：本插件不需要调整归一化前的分数。
func (f *Fit) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// preFilterState 在 PreFilter 阶段计算、在 Filter 阶段使用的 Pod 资源请求快照。
type preFilterState struct {
	framework.Resource
}

// Clone 返回自身；该状态只读，无需深拷贝。
func (s *preFilterState) Clone() fwk.StateData {
	return s
}

// preScoreState 在 PreScore 阶段计算、在 Score 阶段使用的状态。
type preScoreState struct {
	// podRequests 的顺序与 NodeResourcesBalancedAllocationArgs.Resources 中定义的资源顺序一致，
	// 其他存放此类列表的地方也遵循同样约定。
	podRequests []int64
	// DRA 扩展资源打分相关信息。
	*draPreScoreState
}

// Clone 实现必需的 Clone 接口。状态是只读的，无需真正拷贝数据。
func (s *preScoreState) Clone() fwk.StateData {
	return s
}

// PreScore 计算待调度 Pod 的资源请求列表并写入 cycle state，供 Score 阶段使用。
func (f *Fit) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	podRequests := f.calculatePodResourceRequestList(pod, f.resources)
	state := &preScoreState{
		podRequests: podRequests,
	}
	if f.enableDRAExtendedResource {
		draPreScoreState, status := getDRAPreScoredParams(f.draManager, f.resources)
		if status != nil {
			return status
		}
		if draPreScoreState != nil {
			state.draPreScoreState = draPreScoreState
		}
	}

	cycleState.Write(preScoreStateKey, state)
	return nil
}

// getPreScoreState 从 cycle state 读取 PreScore 阶段写入的状态；类型不符时返回错误。
func getPreScoreState(cycleState fwk.CycleState) (*preScoreState, error) {
	c, err := cycleState.Read(preScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %q from cycleState: %w", preScoreStateKey, err)
	}

	s, ok := c.(*preScoreState)
	if !ok {
		return nil, fmt.Errorf("invalid PreScore state, got type %T", c)
	}
	return s, nil
}

// Name 返回插件名，用于日志等场景。
func (f *Fit) Name() string {
	return Name
}

// SignPod 基于容器资源请求与 overhead 生成签名片段。
// 集群中使用 DRA 扩展资源时，资源需求无法仅用静态请求描述，因此禁用该签名。
func (pl *Fit) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	opts := ResourceRequestsOptions{
		EnablePodLevelResources:   pl.enablePodLevelResources,
		EnableDRAExtendedResource: pl.enableDRAExtendedResource,
	}
	if pl.enableDRAExtendedResource {
		podRequest := computePodResourceRequest(pod, opts)
		for rName, rQuant := range podRequest.ScalarResources {
			// 请求量为 0 的资源跳过。
			if rQuant == 0 {
				continue
			}
			if shouldDelegateResourceToDRA(rName, nil, pl.draManager, opts) {
				return nil, fwk.NewStatus(fwk.Unschedulable, "signature disabled when dra extended resources are used in the cluster")
			}
		}
	}

	return []fwk.SignFragment{
		{Key: fwk.ResourcesSignerName, Value: computePodResourceRequest(pod, opts)},
	}, nil
}

// getScorer 根据评分策略类型从映射表中取得对应的打分器构造函数并构造打分器。
func getScorer(strategy *config.ScoringStrategy) (*resourceAllocationScorer, error) {
	if strategy == nil {
		return nil, fmt.Errorf("scoring strategy not specified")
	}
	scorerFactory, exists := nodeResourceStrategyTypeMap[strategy.Type]
	if !exists {
		return nil, fmt.Errorf("scoring strategy %s is not supported", strategy.Type)
	}
	return scorerFactory(strategy), nil
}

// NewFit 校验参数并按评分策略与特性开关初始化插件。
func NewFit(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	args, ok := plArgs.(*config.NodeResourcesFitArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NodeResourcesFitArgs, got %T", plArgs)
	}
	if err := validation.ValidateNodeResourcesFitArgs(nil, args); err != nil {
		return nil, err
	}

	scorer, err := getScorer(args.ScoringStrategy)
	if err != nil {
		return nil, err
	}
	if fts.EnableDRAExtendedResource {
		scorer.enableDRAExtendedResource = true
		scorer.draManager = h.SharedDRAManager()
		scorer.draFeatures = dynamicresources.AllocatorFeatures(fts)
		// 为 DeviceClass 选择器编译创建 CEL 缓存，避免重复编译相同的 CEL 表达式。
		scorer.DRACaches.celCache = cel.NewCache(10, cel.Features{
			EnableConsumableCapacity: fts.EnableDRAConsumableCapacity,
			EnableListTypeAttributes: fts.EnableDRAListTypeAttributes,
		})
	}

	pl := &Fit{
		ignoredResources:                sets.New(args.IgnoredResources...),
		ignoredResourceGroups:           sets.New(args.IgnoredResourceGroups...),
		enableInPlacePodVerticalScaling: fts.EnableInPlacePodVerticalScaling,
		handle:                          h,
		enablePodLevelResources:         fts.EnablePodLevelResources,
		enableDRAExtendedResource:       fts.EnableDRAExtendedResource,

		enableInPlacePodLevelResourcesVerticalScaling: fts.EnableInPlacePodLevelResourcesVerticalScaling,

		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,

		resourceAllocationScorer: scorer,
	}

	if fts.EnableTopologyAwareWorkloadScheduling {
		placementScorer, err := getScorer(&config.ScoringStrategy{
			Type:      config.MostAllocated,
			Resources: args.ScoringStrategy.Resources,
		})
		if err != nil {
			return nil, err
		}
		pl.placementScorer = placementScorer
	}

	return pl, nil
}

// ResourceRequestsOptions 汇总影响资源请求计算的特性开关。
type ResourceRequestsOptions struct {
	EnablePodLevelResources                            bool
	EnableDRAExtendedResource                          bool
	EnableDRANodeAllocatableResources                  bool
	EnableInPlacePodVerticalScalingSchedulerPreemption bool
}

// shouldDelegateResourceToDRA 判断给定资源是否应交给 DRA 插件处理。满足以下条件时返回 true：
//  1. 该资源不在节点 Allocatable 的标量资源中（即不由 device plugin 提供）；
//  2. 缓存中存在该资源到 DeviceClass 的映射（前提是有 draManager）。
func shouldDelegateResourceToDRA(rName v1.ResourceName, nodeInfo fwk.NodeInfo, draManager fwk.SharedDRAManager, opts ResourceRequestsOptions) bool {
	if !opts.EnableDRAExtendedResource {
		return false
	}

	if nodeInfo != nil {
		if allocatable := nodeInfo.GetAllocatable().GetScalarResources()[rName]; allocatable > 0 {
			return false
		}
	}

	if draManager == nil {
		return false
	}

	// draManager 可用时，检查缓存中是否存在资源到 DeviceClass 的映射。
	cache := draManager.DeviceClassResolver()
	return cache.GetDeviceClass(rName) != nil
}

// computePodResourceRequest 返回一个在各资源维度上覆盖峰值需求的 framework.Resource。
// init 容器顺序执行，因此逐维度取最大值；普通容器并行执行，因此累加各容器资源向量。
// Pod 的 Overhead 资源也会计入最终请求总量。
//
// 示例：
//
// Pod：
//
//	InitContainers
//	  IC1:
//	    CPU: 2
//	    Memory: 1G
//	  IC2:
//	    CPU: 2
//	    Memory: 3G
//	Containers
//	  C1:
//	    CPU: 2
//	    Memory: 1G
//	  C2:
//	    CPU: 1
//	    Memory: 1G
//
// 结果：CPU: 3, Memory: 3G
func computePodResourceRequest(pod *v1.Pod, opts ResourceRequestsOptions) *preFilterState {
	// Pod 尚未调度，无需考虑原地垂直扩缩容（InPlacePodVerticalScaling）。
	reqs := resource.PodRequests(pod, resource.PodResourcesOptions{
		// PodLevelResources 特性开启时才不跳过 Pod 级资源。
		SkipPodLevelResources:                    !opts.EnablePodLevelResources,
		UseDRANodeAllocatableResourceClaimStatus: opts.EnableDRANodeAllocatableResources,
	})
	result := &preFilterState{}
	result.SetMaxResource(reqs)
	return result
}

// PreFilter 在 PreFilter 扩展点计算 Pod 的资源请求并写入 cycle state。
func (f *Fit) PreFilter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	result := computePodResourceRequest(pod, ResourceRequestsOptions{EnablePodLevelResources: f.enablePodLevelResources})

	cycleState.Write(preFilterStateKey, result)
	return nil, nil
}

// PreFilterExtensions 返回 nil：本插件不需要 Pod 增删的增量更新钩子。
func (f *Fit) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// getPreFilterState 从 cycle state 读取 PreFilter 阶段写入的 Pod 资源请求状态。
func getPreFilterState(cycleState fwk.CycleState) (*preFilterState, error) {
	c, err := cycleState.Read(preFilterStateKey)
	if err != nil {
		// 读不到状态说明 PreFilter 可能未被调用。
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preFilterStateKey, err)
	}

	s, ok := c.(*preFilterState)
	if !ok {
		return nil, fmt.Errorf("%+v  convert to NodeResourcesFit.preFilterState error", c)
	}
	return s, nil
}

// EventsToRegister 返回可能让此前被本插件判定不可调度的 Pod 重新变得可调度的集群事件。
func (f *Fit) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	events := []fwk.ClusterEventWithHint{
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: f.isSchedulableAfterAssignedPodDelete},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeAllocatable}, QueueingHintFn: f.isSchedulableAfterNodeChange},
	}
	if f.enableDRAExtendedResource {
		events = append(events,
			// Pod 可能在等待某 DeviceClass 关联的扩展资源被创建或修改。
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.DeviceClass, ActionType: fwk.Add | fwk.Update}, QueueingHintFn: f.isSchedulableAfterDeviceClassEvent})
	}
	if f.enableInPlacePodVerticalScaling {
		// 开启原地垂直扩缩容（KEP 1287）时需注册 UpdatePodScaleDown：Pod 缩容会释放资源，
		// 可能让其他 Pod 变得可调度。
		events = append(events,
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.UpdatePodScaleDown}, QueueingHintFn: f.isSchedulableAfterAssignedPodScaleDown},
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodScaleDown}, QueueingHintFn: f.isSchedulableAfterTargetPodScaleDown})
	}
	if f.enableInPlacePodVerticalScalingSchedulerPreemption {
		events = append(events,
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.UpdatePodScaleUp}, QueueingHintFn: f.isSchedulableAfterAssignedPodScaleUp},
			fwk.ClusterEventWithHint{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodScaleUp}, QueueingHintFn: f.isSchedulableAfterTargetPodScaleUp})
	}
	return events, nil
}

// isSchedulableAfterAssignedPodDelete 在已调度 Pod 被删除时调用，
// 判断该变化是否可能让此前不可调度的 Pod 变得可调度。
func (f *Fit) isSchedulableAfterAssignedPodDelete(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	deletedPod, _, err := schedutil.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if deletedPod.Spec.NodeName == "" && deletedPod.Status.NominatedNodeName == "" {
		logger.V(5).Info("the deleted pod was unscheduled and it wouldn't make the unscheduled pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
		return fwk.QueueSkip, nil
	}

	if f.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		if deletedPod.Spec.NodeName != pod.Spec.NodeName {
			logger.V(5).Info("another scheduled pod was deleted, but it's on a different node than the deferred resize pod", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
			return fwk.QueueSkip, nil
		}
	}

	// 已调度 Pod 的任何删除事件都可能释放资源，因此一律提示重新入队。
	logger.V(5).Info("another scheduled pod was deleted, and it may make the unscheduled pod schedulable", "pod", klog.KObj(pod), "deletedPod", klog.KObj(deletedPod))
	return fwk.Queue, nil
}

// isSchedulableAfterTargetPodScaleDown 在目标 Pod 自身缩容时调用，缩容后它可能已可调度，直接提示重新入队。
func (f *Fit) isSchedulableAfterTargetPodScaleDown(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	logger.V(5).Info("the target pod got scaled down and it may be schedulable now", "pod", klog.KObj(pod))
	return fwk.Queue, nil
}

// isSchedulableAfterTargetPodScaleUp 在目标 Pod 自身扩容时调用。需要重新入队：
// 原本因自然可容纳而处于 UnschedulableAndUnresolvable 状态的 Pod，扩容后可能超出剩余空间，
// 需要通过抢占腾出资源。
func (f *Fit) isSchedulableAfterTargetPodScaleUp(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	if !resource.IsPodResizeDeferred(pod) {
		logger.V(5).Info("the target pod got scaled up, but it is not a deferred resize pod", "pod", klog.KObj(pod))
		return fwk.QueueSkip, nil
	}
	logger.V(5).Info("the target pod got scaled up, re-evaluating", "pod", klog.KObj(pod))
	return fwk.Queue, nil
}

// isSchedulableAfterAssignedPodScaleUp 检查是否有同节点上的其他已调度 Pod 扩容。
// 需要重新入队：竞争性的扩容会消耗延迟调度 Pod 正在等待的空余资源，使其重新进入评估以发起抢占。
func (f *Fit) isSchedulableAfterAssignedPodScaleUp(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, modifiedPod, err := schedutil.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}
	if !resource.IsPodResizeDeferred(pod) {
		logger.V(5).Info("another assigned pod got scaled up, but target pod is not a deferred resize pod", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}
	if modifiedPod.Spec.NodeName != pod.Spec.NodeName {
		logger.V(5).Info("another assigned pod got scaled up, but on a different node than the deferred resize pod", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}
	logger.V(5).Info("another assigned pod got scaled up on the same node as the deferred resize pod, re-evaluating", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
	return fwk.Queue, nil
}

// isSchedulableAfterAssignedPodScaleDown 在已调度 Pod 缩容时调用，
// 检查释放的资源是否正是此前不可调度 Pod 所请求的资源。
func (f *Fit) isSchedulableAfterAssignedPodScaleDown(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalPod, modifiedPod, err := schedutil.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if !f.haveEnoughRelevantResourcesDecreased(pod, originalPod, modifiedPod) {
		if loggerV := logger.V(10); loggerV.Enabled() {
			// 详细日志下输出对象 diff，便于定位变更细节。
			loggerV.Info("pod got scaled down, but the modification isn't related to the resource requests of the target pod", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod), "diff", diff.Diff(originalPod, modifiedPod))
		} else {
			logger.V(5).Info("pod got scaled down, but the modification isn't related to the resource requests of the target pod", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		}
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("another scheduled pod got scaled down, and it may make the unscheduled pod schedulable", "pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
	return fwk.Queue, nil
}

// haveEnoughRelevantResourcesDecreased 检查已调度 Pod 缩容后，目标 Pod 也请求的相关资源是否确有减少。
func (f *Fit) haveEnoughRelevantResourcesDecreased(targetPod *v1.Pod, originalPod, modifiedPod *v1.Pod) bool {
	if modifiedPod.Spec.NodeName == "" {
		// 未调度 Pod 的更新不占用节点资源，不会让目标 Pod 变得可调度。
		return false
	}

	if f.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(targetPod) {
		// 延迟扩容的 Pod 已绑定到特定节点，其他节点的变化不影响其可调度性。
		if modifiedPod.Spec.NodeName != targetPod.Spec.NodeName {
			return false
		}
	}

	// 另一个 Pod 已调度到节点上，其修改或删除可能释放资源。
	originalMaxResourceReq, modifiedMaxResourceReq := &framework.Resource{}, &framework.Resource{}
	opts := resource.PodResourcesOptions{UseStatusResources: f.enableInPlacePodVerticalScaling, InPlacePodLevelResourcesVerticalScalingEnabled: f.enableInPlacePodLevelResourcesVerticalScaling}
	originalMaxResourceReq.SetMaxResource(resource.PodRequests(originalPod, opts))
	modifiedMaxResourceReq.SetMaxResource(resource.PodRequests(modifiedPod, opts))

	// 逐个检查目标 Pod 请求的资源，判断修改后的请求是否比原来少。
	podRequests := resource.PodRequests(targetPod, opts)
	for rName, rValue := range podRequests {
		if rValue.IsZero() {
			// 只关心待调度 Pod 实际请求的资源。
			continue
		}
		switch rName {
		case v1.ResourceCPU:
			if originalMaxResourceReq.MilliCPU > modifiedMaxResourceReq.MilliCPU {
				return true
			}
		case v1.ResourceMemory:
			if originalMaxResourceReq.Memory > modifiedMaxResourceReq.Memory {
				return true
			}
		case v1.ResourceEphemeralStorage:
			if originalMaxResourceReq.EphemeralStorage > modifiedMaxResourceReq.EphemeralStorage {
				return true
			}
		default:
			if schedutil.IsScalarResourceName(rName) && originalMaxResourceReq.ScalarResources[rName] > modifiedMaxResourceReq.ScalarResources[rName] {
				return true
			}
		}
	}
	return false
}

// isSchedulableAfterNodeChange 在节点新增或变更时调用，
// 检查该变化是否可能让此前不可调度的 Pod 变得可调度。
func (f *Fit) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := schedutil.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}
	if f.enableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		// 延迟扩容的 Pod 已绑定到特定节点，其他节点的变化不影响其可调度性。
		if modifiedNode.Name != pod.Spec.NodeName {
			logger.V(5).Info("node was updated, but it is a different node than the deferred resize pod's node", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.QueueSkip, nil
		}
	}
	// 事件处理时使用 DRA manager 的扩展资源缓存。
	var draManager fwk.SharedDRAManager
	if f.enableDRAExtendedResource {
		draManager = f.handle.SharedDRAManager()
	}

	opts := ResourceRequestsOptions{
		EnablePodLevelResources:   f.enablePodLevelResources,
		EnableDRAExtendedResource: f.enableDRAExtendedResource,
	}

	// 变更后的节点仍容不下 Pod，无需重新入队。
	if !isFit(pod, modifiedNode, draManager, opts) {
		logger.V(5).Info("node was created or updated, but it doesn't have enough resource(s) to accommodate this pod", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.QueueSkip, nil
	}
	// 新增节点且能容纳 Pod，解除阻塞、允许重新调度。
	if originalNode == nil {
		logger.V(5).Info("node was added and it might fit the pod's resource requests", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.Queue, nil
	}
	// 节点虽能容纳 Pod，但可用资源未增加，本次变更不会改变调度结论。
	if !haveAnyRequestedResourcesIncreased(pod, originalNode, modifiedNode, draManager, opts) {
		logger.V(5).Info("node was updated, but haven't changed the pod's resource requestments fit assessment", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("node was updated, and may now fit the pod's resource requests", "pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return fwk.Queue, nil
}

// isSchedulableAfterDeviceClassEvent 在 DeviceClass 新增或变更时调用，
// 检查其扩展资源名是否与 Pod 请求匹配，从而判断是否值得重新入队。
func (f *Fit) isSchedulableAfterDeviceClassEvent(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalClass, modifiedClass, err := schedutil.As[*resourceapi.DeviceClass](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}
	if originalClass != nil {
		if ptr.Deref(originalClass.Spec.ExtendedResourceName, "") == ptr.Deref(modifiedClass.Spec.ExtendedResourceName, "") {
			logger.V(5).Info("device class has identical extended resource name", "pod", klog.KObj(pod), "deviceclass", klog.KObj(modifiedClass))
			return fwk.QueueSkip, nil
		}
	} else {
		// 只有新增时才检查隐式扩展资源名：Update 不会改变 DeviceClass 的名字。
		reqs := resource.PodRequests(pod, resource.PodResourcesOptions{})
		if _, ok := reqs[v1.ResourceName(resourceapi.ResourceDeviceClassPrefix+modifiedClass.Name)]; ok {
			logger.V(5).Info("device class was added, and may now fit the pod's resource requests", "pod", klog.KObj(pod), "deviceclass", klog.KObj(modifiedClass))
			return fwk.Queue, nil
		}
	}
	if modifiedClass.Spec.ExtendedResourceName != nil {
		reqs := resource.PodRequests(pod, resource.PodResourcesOptions{})
		if _, ok := reqs[v1.ResourceName(*modifiedClass.Spec.ExtendedResourceName)]; ok {
			logger.V(5).Info("device class was created or updated, and may fit the pod's resoruce requests", "pod", klog.KObj(pod), "deviceclass", klog.KObj(modifiedClass))
			return fwk.Queue, nil
		}
	}
	logger.V(5).Info("created or updated deivce class extended resource name is either nil, or does not match pod's resource request", "pod", klog.KObj(pod), "deviceclass", klog.KObj(modifiedClass))
	return fwk.QueueSkip, nil
}

// haveAnyRequestedResourcesIncreased 判断变更后的节点是否增加了 Pod 请求的任一资源（或可容纳 Pod 数上限）。
func haveAnyRequestedResourcesIncreased(pod *v1.Pod, originalNode, modifiedNode *v1.Node, draManager fwk.SharedDRAManager, opts ResourceRequestsOptions) bool {
	podRequest := computePodResourceRequest(pod, opts)
	originalNodeInfo := framework.NewNodeInfo()
	originalNodeInfo.SetNode(originalNode)
	modifiedNodeInfo := framework.NewNodeInfo()
	modifiedNodeInfo.SetNode(modifiedNode)

	if modifiedNodeInfo.Allocatable.GetAllowedPodNumber() > originalNodeInfo.Allocatable.GetAllowedPodNumber() {
		return true
	}

	if podRequest.MilliCPU == 0 &&
		podRequest.Memory == 0 &&
		podRequest.EphemeralStorage == 0 &&
		len(podRequest.ScalarResources) == 0 {
		return false
	}

	if (podRequest.MilliCPU > 0 && modifiedNodeInfo.Allocatable.GetMilliCPU() > originalNodeInfo.Allocatable.GetMilliCPU()) ||
		(podRequest.Memory > 0 && modifiedNodeInfo.Allocatable.GetMemory() > originalNodeInfo.Allocatable.GetMemory()) ||
		(podRequest.EphemeralStorage > 0 && modifiedNodeInfo.Allocatable.GetEphemeralStorage() > originalNodeInfo.Allocatable.GetEphemeralStorage()) {
		return true
	}

	for rName, rQuant := range podRequest.ScalarResources {
		// 请求量为 0 的资源跳过。
		if rQuant == 0 {
			continue
		}

		if modifiedNodeInfo.Allocatable.GetScalarResources()[rName] > originalNodeInfo.Allocatable.GetScalarResources()[rName] {
			return true
		}

		if shouldDelegateResourceToDRA(rName, modifiedNodeInfo, draManager, opts) {
			return true
		}
	}
	return false
}

// isFit 检查 Pod 是否能落到节点上；节点为 nil 时返回 false。
// 该方法构造一个临时的 NodeInfo 并调用 Fits 完成检查。
func isFit(pod *v1.Pod, node *v1.Node, draManager fwk.SharedDRAManager, opts ResourceRequestsOptions) bool {
	if node == nil {
		return false
	}
	nodeInfo := framework.NewNodeInfo()
	nodeInfo.SetNode(node)

	return len(Fits(pod, nodeInfo, draManager, opts)) == 0
}

// Filter 在 Filter 扩展点检查节点资源（CPU、内存、GPU、扩展资源等）是否足够运行 Pod。
// 返回不足资源列表；列表为空表示节点满足 Pod 的全部资源请求。
func (f *Fit) Filter(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	s, err := getPreFilterState(cycleState)
	if err != nil {
		return fwk.AsStatus(err)
	}

	var draManager fwk.SharedDRAManager
	if f.enableDRAExtendedResource {
		draManager = f.handle.SharedDRAManager()
	}

	opts := ResourceRequestsOptions{
		EnablePodLevelResources:                            f.enablePodLevelResources,
		EnableDRAExtendedResource:                          f.enableDRAExtendedResource,
		EnableInPlacePodVerticalScalingSchedulerPreemption: f.enableInPlacePodVerticalScalingSchedulerPreemption,
	}

	insufficientResources := fitsRequest(s, nodeInfo, f.ignoredResources, f.ignoredResourceGroups, draManager, opts, pod)

	if len(insufficientResources) != 0 {
		// 汇总所有不足资源的原因，便于诊断。
		failureReasons := make([]string, 0, len(insufficientResources))
		statusCode := fwk.Unschedulable
		for i := range insufficientResources {
			failureReasons = append(failureReasons, insufficientResources[i].Reason)

			if insufficientResources[i].Unresolvable {
				statusCode = fwk.UnschedulableAndUnresolvable
			}
		}

		return fwk.NewStatus(statusCode, failureReasons...)
	}

	return nil
}

// InsufficientResource 描述导致 Pod 无法落到节点上的资源瓶颈。
type InsufficientResource struct {
	ResourceName v1.ResourceName
	// 显式保存原因字符串，避免对常见资源在运行时格式化消息——集群自动扩缩容模拟中这很昂贵。
	Reason    string
	Requested int64
	Used      int64
	Capacity  int64
	// Unresolvable 表示抢占也无法让该节点容纳 Pod，判断依据是节点容量与 Pod 请求量的对比。
	Unresolvable bool
}

// Fits 检查节点是否有足够资源容纳 Pod，返回不足资源列表。
func Fits(pod *v1.Pod, nodeInfo fwk.NodeInfo, draManager fwk.SharedDRAManager, opts ResourceRequestsOptions) []InsufficientResource {
	return fitsRequest(computePodResourceRequest(pod, opts), nodeInfo, nil, nil, draManager, opts, pod)
}

// fitsRequest 逐资源维度比较 Pod 请求与节点剩余量，返回所有不足资源；
// 会跳过用户配置的忽略资源、交由 DRA 处理的资源，并处理原地扩缩容场景下的缓存差异。
func fitsRequest(podRequest *preFilterState, nodeInfo fwk.NodeInfo, ignoredExtendedResources, ignoredResourceGroups sets.Set[string], draManager fwk.SharedDRAManager, opts ResourceRequestsOptions, pod *v1.Pod) []InsufficientResource {
	insufficientResources := make([]InsufficientResource, 0, 4)

	allowedPodNumber := nodeInfo.GetAllocatable().GetAllowedPodNumber()
	if len(nodeInfo.GetPods())+1 > allowedPodNumber {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourcePods,
			Reason:       "Too many pods",
			Requested:    1,
			Used:         int64(len(nodeInfo.GetPods())),
			Capacity:     int64(allowedPodNumber),
		})
	}

	if podRequest.MilliCPU == 0 &&
		podRequest.Memory == 0 &&
		podRequest.EphemeralStorage == 0 &&
		len(podRequest.ScalarResources) == 0 {
		return insufficientResources
	}

	deltaMilliCPU, deltaMemory, deltaEphemeralStorage, deltaScalarResources := adjustDeltasToAccomodateCacheDiscrepancy(opts, podRequest, nodeInfo, pod)

	if podRequest.MilliCPU > 0 && deltaMilliCPU > (nodeInfo.GetAllocatable().GetMilliCPU()-nodeInfo.GetRequested().GetMilliCPU()) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceCPU,
			Reason:       "Insufficient cpu",
			Requested:    podRequest.MilliCPU,
			Used:         nodeInfo.GetRequested().GetMilliCPU(),
			Capacity:     nodeInfo.GetAllocatable().GetMilliCPU(),
			Unresolvable: podRequest.MilliCPU > nodeInfo.GetAllocatable().GetMilliCPU(),
		})
	}
	if podRequest.Memory > 0 && deltaMemory > (nodeInfo.GetAllocatable().GetMemory()-nodeInfo.GetRequested().GetMemory()) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceMemory,
			Reason:       "Insufficient memory",
			Requested:    podRequest.Memory,
			Used:         nodeInfo.GetRequested().GetMemory(),
			Capacity:     nodeInfo.GetAllocatable().GetMemory(),
			Unresolvable: podRequest.Memory > nodeInfo.GetAllocatable().GetMemory(),
		})
	}
	if podRequest.EphemeralStorage > 0 &&
		deltaEphemeralStorage > (nodeInfo.GetAllocatable().GetEphemeralStorage()-nodeInfo.GetRequested().GetEphemeralStorage()) {
		insufficientResources = append(insufficientResources, InsufficientResource{
			ResourceName: v1.ResourceEphemeralStorage,
			Reason:       "Insufficient ephemeral-storage",
			Requested:    podRequest.EphemeralStorage,
			Used:         nodeInfo.GetRequested().GetEphemeralStorage(),
			Capacity:     nodeInfo.GetAllocatable().GetEphemeralStorage(),
			Unresolvable: podRequest.GetEphemeralStorage() > nodeInfo.GetAllocatable().GetEphemeralStorage(),
		})
	}

	for rName, rQuant := range deltaScalarResources {
		// 请求量为 0 的资源跳过。
		if rQuant == 0 {
			continue
		}

		if v1helper.IsExtendedResourceName(rName) {
			// 属于忽略列表的扩展资源跳过检查。API 校验保证 rName 一定包含斜杠，可安全拆分前缀。
			var rNamePrefix string
			if ignoredResourceGroups.Len() > 0 {
				rNamePrefix = strings.Split(string(rName), "/")[0]
			}
			if ignoredExtendedResources.Has(string(rName)) || ignoredResourceGroups.Has(rNamePrefix) {
				continue
			}
		}

		if shouldDelegateResourceToDRA(rName, nodeInfo, draManager, opts) {
			continue
		}
		if podRequest.ScalarResources[rName] > 0 && rQuant > (nodeInfo.GetAllocatable().GetScalarResources()[rName]-nodeInfo.GetRequested().GetScalarResources()[rName]) {
			insufficientResources = append(insufficientResources, InsufficientResource{
				ResourceName: rName,
				Reason:       fmt.Sprintf("Insufficient %v", rName),
				Requested:    podRequest.ScalarResources[rName],
				Used:         nodeInfo.GetRequested().GetScalarResources()[rName],
				Capacity:     nodeInfo.GetAllocatable().GetScalarResources()[rName],
				Unresolvable: rQuant > nodeInfo.GetAllocatable().GetScalarResources()[rName],
			})
		}
	}

	return insufficientResources
}

// adjustDeltasToAccomodateCacheDiscrepancy 计算用于检查 Pod 的资源增量。
// 对已分配节点上的 Pod，节点缓存已计入它的期望资源（取 desired、allocated、actual 的最大值），
// 理论上只需检查节点是否超卖。此函数用于抹平调度队列中的 Pod 信息与节点快照缓存之间的异步差异。
func adjustDeltasToAccomodateCacheDiscrepancy(opts ResourceRequestsOptions, podRequest *preFilterState, nodeInfo fwk.NodeInfo, pod *v1.Pod) (int64, int64, int64, map[v1.ResourceName]int64) {
	deltaMilliCPU := podRequest.MilliCPU
	deltaMemory := podRequest.Memory
	deltaEphemeralStorage := podRequest.EphemeralStorage
	deltaScalarResources := podRequest.ScalarResources

	if !opts.EnableInPlacePodVerticalScalingSchedulerPreemption || pod == nil || len(pod.Spec.NodeName) == 0 || pod.Spec.NodeName != nodeInfo.Node().Name {
		return deltaMilliCPU, deltaMemory, deltaEphemeralStorage, deltaScalarResources
	}

	var cachedPodInfo fwk.PodInfo
	for _, pInfo := range nodeInfo.GetPods() {
		if pInfo.GetPod().UID == pod.UID {
			cachedPodInfo = pInfo
			break
		}
	}
	if cachedPodInfo == nil {
		return deltaMilliCPU, deltaMemory, deltaEphemeralStorage, deltaScalarResources
	}

	cachedRes := cachedPodInfo.CalculateResource().Resource
	// 用 max(0, ...) 防止 podRequest < cachedRes 时出现负增量（如缩容过程或缓存异步滞后）。
	// 负增量会在 kubelet 真正释放资源前错误地减少节点已用量；若陈旧的较大 cachedRes 导致抢占失败，
	// 缓存最终收敛后会发出缩容事件重新唤醒该 Pod。
	deltaMilliCPU = max(0, podRequest.MilliCPU-cachedRes.GetMilliCPU())
	deltaMemory = max(0, podRequest.Memory-cachedRes.GetMemory())
	deltaEphemeralStorage = max(0, podRequest.EphemeralStorage-cachedRes.GetEphemeralStorage())

	adjustedScalars := make(map[v1.ResourceName]int64)
	cachedScalars := cachedRes.GetScalarResources()
	for rName, rQuant := range podRequest.ScalarResources {
		adjustedScalars[rName] = max(0, rQuant-cachedScalars[rName])
	}
	deltaScalarResources = adjustedScalars

	return deltaMilliCPU, deltaMemory, deltaEphemeralStorage, deltaScalarResources
}

// Score 在 Score 扩展点根据节点资源使用率打分。
func (f *Fit) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	s, err := getPreScoreState(state)
	if err != nil {
		s = &preScoreState{
			podRequests: f.calculatePodResourceRequestList(pod, f.resources),
		}
		if f.enableDRAExtendedResource {
			draPreScoreState, status := getDRAPreScoredParams(f.draManager, f.resources)
			if status != nil {
				return 0, status
			}
			if draPreScoreState != nil {
				s.draPreScoreState = draPreScoreState
			}
		}
	}

	return f.score(ctx, pod, nodeInfo, s.podRequests, s.draPreScoreState)
}

// PlacementScoreExtensions 返回 nil：本插件不使用放置打分扩展。
func (f *Fit) PlacementScoreExtensions() fwk.PlacementScoreExtensions {
	return nil
}

// ScorePlacement 对放置方案中涉及的所有节点按容量比打分，包含该 PodGroup 内已分配 Pod 的资源请求。
func (f *Fit) ScorePlacement(ctx context.Context, state fwk.PlacementCycleState, podGroup fwk.PodGroupInfo, placement *fwk.PodGroupAssignments) (int64, *fwk.Status) {
	return f.placementScorer.scorePlacement(ctx, podGroup, placement)
}
