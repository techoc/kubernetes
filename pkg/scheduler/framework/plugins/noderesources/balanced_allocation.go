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

package noderesources

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/dynamic-resource-allocation/structured"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// BalancedAllocation 是打分插件，计算 CPU 与内存占容量比例之间的差异，
// 并优先选择这两个指标更接近的节点。
type BalancedAllocation struct {
	handle fwk.Handle
	resourceAllocationScorer
}

var _ fwk.PreScorePlugin = &BalancedAllocation{}
var _ fwk.ScorePlugin = &BalancedAllocation{}
var _ fwk.SignPlugin = &BalancedAllocation{}

// BalancedAllocationName 是插件在插件注册表与配置中使用的名称。
const (
	BalancedAllocationName = names.NodeResourcesBalancedAllocation

	// balancedAllocationPreScoreStateKey 是 CycleState 中存放
	// NodeResourcesBalancedAllocation 打分预计算数据的键。
	balancedAllocationPreScoreStateKey = "PreScore" + BalancedAllocationName
)

// draPreScoreState 保存 DRA 扩展资源打分所需的预计算数据。
type draPreScoreState struct {
	// allocatedState 保存 DRA 扩展资源打分所需的 DRA 分配状态。
	allocatedState *structured.AllocatedState
	// resourceSlices 保存 DRA 扩展资源打分所需的资源分片列表。
	resourceSlices []*resourceapi.ResourceSlice
}

// balancedAllocationPreScoreState 在 PreScore 阶段计算，供 Score 阶段使用。
type balancedAllocationPreScoreState struct {
	// podRequests 的元素顺序与 NodeResourcesFitArgs.Resources 中定义的资源顺序一致，
	// 其他保存此类列表的地方也是如此。
	podRequests []int64
	// DRA 扩展资源打分状态。
	*draPreScoreState
}

// Clone 实现必需的 Clone 接口。这里无需复制数据，直接返回自身即可。
func (s *balancedAllocationPreScoreState) Clone() fwk.StateData {
	return s
}

// PreScore 计算入站 Pod 的资源请求，并写入本次调度周期的状态供后续使用。
func (ba *BalancedAllocation) PreScore(ctx context.Context, cycleState fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	podRequests := ba.calculatePodResourceRequestList(pod, ba.resources)
	if ba.isBestEffortPod(podRequests) {
		// BestEffort Pod 没有资源请求，若参与均衡打分会把大量 Pod 引向同一节点。
		// 详见 https://github.com/kubernetes/kubernetes/issues/129138。
		return fwk.NewStatus(fwk.Skip)
	}
	state := &balancedAllocationPreScoreState{
		podRequests: podRequests,
	}
	if ba.enableDRAExtendedResource {
		draPreScoreState, status := getDRAPreScoredParams(ba.draManager, ba.resources)
		if status != nil {
			return status
		}
		if draPreScoreState != nil {
			state.draPreScoreState = draPreScoreState
		}
	}
	cycleState.Write(balancedAllocationPreScoreStateKey, state)
	return nil
}

// getBalancedAllocationPreScoreState 从调度周期状态中取出均衡打分的预计算结果。
func getBalancedAllocationPreScoreState(cycleState fwk.CycleState) (*balancedAllocationPreScoreState, error) {
	c, err := cycleState.Read(balancedAllocationPreScoreStateKey)
	if err != nil {
		return nil, fmt.Errorf("reading %q from cycleState: %w", balancedAllocationPreScoreStateKey, err)
	}

	s, ok := c.(*balancedAllocationPreScoreState)
	if !ok {
		return nil, fmt.Errorf("invalid PreScore state, got type %T", c)
	}
	return s, nil
}

// Name 返回插件名称，用于日志等场景。
func (ba *BalancedAllocation) Name() string {
	return BalancedAllocationName
}

// SignPod 根据容器资源及开销生成 Pod 签名片段。
func (pl *BalancedAllocation) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	opts := ResourceRequestsOptions{
		EnablePodLevelResources:   pl.enablePodLevelResources,
		EnableDRAExtendedResource: pl.enableDRAExtendedResource,
	}
	if pl.enableDRAExtendedResource {
		podRequest := computePodResourceRequest(pod, opts)
		for rName, rQuant := range podRequest.ScalarResources {
			// 请求量为 0 的资源不影响均衡性，无需检查是否需要交给 DRA 处理。
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

// Score 在 Score 扩展点调用，为节点计算均衡打分。
func (ba *BalancedAllocation) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	s, err := getBalancedAllocationPreScoreState(state)
	if err != nil {
		// PreScore 未写入状态时在此兜底重新计算，避免插件在未启用 PreScore 的路径上失效。
		s = &balancedAllocationPreScoreState{podRequests: ba.calculatePodResourceRequestList(pod, ba.resources)}
		if ba.isBestEffortPod(s.podRequests) {
			return 0, nil
		}
		if ba.enableDRAExtendedResource {
			draPreScoreState, status := getDRAPreScoredParams(ba.draManager, ba.resources)
			if status != nil {
				return 0, status
			}
			if draPreScoreState != nil {
				s.draPreScoreState = draPreScoreState
			}
		}
	}

	// ba.score 偏好资源使用率均衡的节点：计算各资源使用率的标准差，
	// 使用率越接近得分越高。公式为 score = (1 - std) * MaxNodeScore，
	// 其中 std = √Σ((fraction(i)-mean)^2)/len(resources)。
	// 算法部分借鉴了：
	// "Wei Huang et al. An Energy Efficient Virtual Machine Placement Algorithm with Balanced Resource Utilization"
	return ba.score(ctx, pod, nodeInfo, s.podRequests, s.draPreScoreState)
}

// ScoreExtensions 返回 Score 插件的扩展接口；本插件不需要扩展，返回 nil。
func (ba *BalancedAllocation) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// NewBalancedAllocation 校验参数并初始化一个新的均衡分配插件。
func NewBalancedAllocation(_ context.Context, baArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	args, ok := baArgs.(*config.NodeResourcesBalancedAllocationArgs)
	if !ok {
		return nil, fmt.Errorf("want args to be of type NodeResourcesBalancedAllocationArgs, got %T", baArgs)
	}

	if err := validation.ValidateNodeResourcesBalancedAllocationArgs(nil, args); err != nil {
		return nil, err
	}

	return &BalancedAllocation{
		handle: h,
		resourceAllocationScorer: resourceAllocationScorer{
			Name:                            BalancedAllocationName,
			enableInPlacePodVerticalScaling: fts.EnableInPlacePodVerticalScaling,
			enablePodLevelResources:         fts.EnablePodLevelResources,
			enableDRAExtendedResource:       fts.EnableDRAExtendedResource,
			scorer:                          balancedResourceScorer,
			useRequested:                    true,
			resources:                       args.Resources,
			enableInPlacePodLevelResourcesVerticalScaling: fts.EnableInPlacePodLevelResourcesVerticalScaling,
			draManager: h.SharedDRAManager(),
		},
	}, nil
}

// balancedResourceScorer 根据加入待调度 Pod 前后节点均衡度的改善程度打分。
func balancedResourceScorer(requested, allocated, allocatable []int64) int64 {
	// 计算加入待调度 Pod 请求后节点的得分。
	scoreWithPod := balancedResourceScore(requested, allocatable)
	// 计算未加入待调度 Pod 请求时节点的得分。
	scoreWithoutPod := balancedResourceScore(allocated, allocatable)
	// 用加入前后得分之差衡量均衡性的变化。两种得分都在 [MaxNodeScore/2, MaxNodeScore] 区间，
	// 差值落在 [-MaxNodeScore/2, MaxNodeScore/2]，这里把结果平移回 [MaxNodeScore/2, MaxNodeScore]，
	// 以贴近只考虑加入 Pod 后得分的旧实现：变化小约为 75，均衡改善趋近 100，恶化趋近 50。
	return fwk.MaxNodeScore/2 + (fwk.MaxNodeScore/2+scoreWithPod-scoreWithoutPod)/2
}

// balancedResourceScore 根据各资源请求占比的标准差计算节点均衡得分。
func balancedResourceScore(requested, allocatable []int64) int64 {
	var resourceToFractions []float64
	var totalFraction float64
	for i := range requested {
		// 无可分配容量的资源无法计算占比，跳过以免影响均衡度判断。
		if allocatable[i] == 0 {
			continue
		}
		fraction := float64(requested[i]) / float64(allocatable[i])
		// 占比封顶为 1，避免超额请求放大标准差。
		if fraction > 1 {
			fraction = 1
		}
		totalFraction += fraction
		resourceToFractions = append(resourceToFractions, fraction)
	}

	std := 0.0

	// 常见情况下只有 CPU 和内存两种资源，标准差可简化为 (fraction1-fraction2)/2；
	// 多于两种时套用标准公式 √(Σ((fraction(i)-mean)^2)/len(fractions))；
	// 其他情况（0 或 1 种资源）无差异可言，标准差置 0 即可。
	if len(resourceToFractions) == 2 {
		std = math.Abs((resourceToFractions[0] - resourceToFractions[1]) / 2)
	} else if len(resourceToFractions) > 2 {
		mean := totalFraction / float64(len(resourceToFractions))
		var sum float64
		for _, fraction := range resourceToFractions {
			sum = sum + (fraction-mean)*(fraction-mean)
		}
		std = math.Sqrt(sum / float64(len(resourceToFractions)))
	}

	// 标准差非负，1-std 使偏差最小的节点得分最高，再乘 MaxNodeScore 完成分数缩放。
	return int64((1 - std) * float64(fwk.MaxNodeScore))
}
