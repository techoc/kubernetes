/*
Copyright The Kubernetes Authors.

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

// Package topologyaware 实现拓扑感知的放置生成插件：根据 PodGroup 的拓扑约束，
// 把父放置（parent placement）按拓扑域拆分成多个候选放置。
package topologyaware

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

const (
	// Name 是插件在注册表和配置中使用的名称。
	Name = names.TopologyPlacementGenerator
)

// TopologyPlacement 根据 PodGroup 的拓扑约束为其生成放置方案。
// 已调度 Pod 所在的拓扑域会约束后续 Pod 的可选域，保证同一拓扑约束下的 Pod 落在同一域。
type TopologyPlacement struct {
	handle fwk.Handle
	fts    feature.Features
}

var _ fwk.PlacementGeneratePlugin = &TopologyPlacement{}

// New 初始化并返回一个 TopologyPlacement 插件实例。
func New(_ context.Context, _ runtime.Object, fh fwk.Handle, fts feature.Features) (*TopologyPlacement, error) {
	return &TopologyPlacement{
		handle: fh,
		fts:    fts,
	}, nil
}

// Name 返回插件名称。
func (pl *TopologyPlacement) Name() string {
	return Name
}

// GeneratePlacements 根据 PodGroup 规范中的拓扑约束生成放置方案。
// 它以父放置提供的节点为候选集，按拓扑域分组；若组内已有 Pod 被调度，
// 则只保留这些 Pod 所在的域，避免跨域放置。
func (pl *TopologyPlacement) GeneratePlacements(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, parentPlacement *fwk.Placement) (*fwk.GeneratePlacementsResult, *fwk.Status) {
	topologyKey, ok := pl.getTopologyKey(podGroup)
	if !ok {
		// 没有拓扑约束时直接沿用父放置，无需按域拆分。
		return &fwk.GeneratePlacementsResult{Placements: []*fwk.Placement{parentPlacement}}, nil
	}

	var requiredDomain *string
	scheduledPods, err := pl.getScheduledPods(podGroup)
	if err != nil {
		return nil, fwk.AsStatus(err)
	}
	// 已有 Pod 被调度时，其所在拓扑域成为硬性约束：新 Pod 必须落在同一个域。
	if len(scheduledPods) > 0 {
		scheduledDomain, err := pl.getScheduledPodsTopologyDomain(topologyKey, scheduledPods)
		if err != nil {
			return nil, fwk.AsStatus(fmt.Errorf("cannot determine domain for already scheduled pods: %w", err))
		}
		requiredDomain = &scheduledDomain
	}

	nodesPerTopologyDomain := make(map[string][]fwk.NodeInfo)
	for _, node := range parentPlacement.Nodes {
		// 缺少拓扑标签的节点无法归入任何域，直接忽略。
		if domain, ok := node.Node().Labels[topologyKey]; ok {
			if requiredDomain == nil || *requiredDomain == domain {
				nodesPerTopologyDomain[domain] = append(nodesPerTopologyDomain[domain], node)
			}
		}
	}

	placements := make([]*fwk.Placement, 0, len(nodesPerTopologyDomain))
	for topologyDomain, nodes := range nodesPerTopologyDomain {
		if len(nodes) > 0 {
			placements = append(placements, &fwk.Placement{
				Name:  topologyDomain,
				Nodes: nodes,
			})
		}
	}

	return &fwk.GeneratePlacementsResult{Placements: placements}, nil
}

// getScheduledPodsTopologyDomain 返回已调度 Pod 共同所在的拓扑域。
// 若 Pod 所在节点缺失拓扑标签或不同 Pod 落在不同域，则返回错误。
func (pl *TopologyPlacement) getScheduledPodsTopologyDomain(topologyKey string, scheduledPods []*v1.Pod) (string, error) {
	topologyDomain := ""
	for _, pod := range scheduledPods {
		node, err := pl.handle.SnapshotSharedLister().NodeInfos().Get(pod.Spec.NodeName)
		if err != nil {
			return "", fmt.Errorf("getting node for pod %v: %w", klog.KObj(pod), err)
		}
		domain, ok := node.Node().Labels[topologyKey]
		if !ok {
			return "", fmt.Errorf("no topology domain found for pod %v", klog.KObj(pod))
		}
		// 同一拓扑约束下的已调度 Pod 必须位于同一个域，跨域说明状态异常。
		if topologyDomain != "" && topologyDomain != domain {
			return "", fmt.Errorf("more than 1 domain found for pod group: %v, %v", topologyDomain, domain)
		}
		topologyDomain = domain
	}
	return topologyDomain, nil
}

// getTopologyKey 返回 PodGroup 中指定的拓扑键（若存在）。
// 同时兼容普通 PodGroup 和 CompositePodGroup；当前 API 层面只支持单个拓扑约束。
func (pl *TopologyPlacement) getTopologyKey(pgi fwk.PodGroupInfo) (string, bool) {
	if pg := pgi.GetPodGroup(); pg != nil {
		if schedulingConstraints := pg.Spec.SchedulingConstraints; schedulingConstraints != nil && len(schedulingConstraints.Topology) > 0 {
			// 目前 API 层面仅支持单个拓扑约束，取第一个即可。
			return schedulingConstraints.Topology[0].Key, true
		}
	} else if cpg := pgi.GetCompositePodGroup(); cpg != nil {
		if schedulingConstraints := cpg.Spec.SchedulingConstraints; schedulingConstraints != nil && len(schedulingConstraints.Topology) > 0 {
			// 目前 API 层面仅支持单个拓扑约束，取第一个即可。
			return schedulingConstraints.Topology[0].Key, true
		}
	}
	return "", false
}

// getScheduledPods 返回 PodGroup 中已调度的 Pod。
// 启用 CompositePodGroup 时需遍历组合内各子 PodGroup 的状态并汇总。
func (pl *TopologyPlacement) getScheduledPods(podGroup fwk.PodGroupInfo) ([]*v1.Pod, error) {
	if pl.fts.EnableCompositePodGroup {
		var results []*v1.Pod
		for podGroupState, err := range helper.GetPodGroupStates(pl.handle.SnapshotSharedLister(), fwk.MustParseEntityKey(podGroup.GetKey())) {
			if err != nil {
				return nil, err
			}
			results = append(results, podGroupState.ScheduledPods()...)
		}
		return results, nil
	}
	name := podGroup.GetName()
	podGroupState, err := pl.handle.SnapshotSharedLister().PodGroupStates().Get(podGroup.GetNamespace(), name)
	if err != nil {
		return nil, err
	}
	return podGroupState.ScheduledPods(), nil
}
