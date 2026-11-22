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

package podgrouppodscount

import (
	"context"
	"errors"

	"k8s.io/apimachinery/pkg/runtime"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// PodGroupPodsCount 是一个放置评分插件，倾向于选择能容纳所考察 PodGroup 中更多 pod 的放置方案。
type PodGroupPodsCount struct {
	handle fwk.Handle
	fts    feature.Features
}

// 编译期断言，确保插件实现了放置评分接口，接口签名变更时能尽早暴露。
var _ fwk.PlacementScorePlugin = &PodGroupPodsCount{}

// Name 是本插件注册到框架时使用的名字。
const Name = names.PodGroupPodsCount

// New 初始化一个新插件并返回。
func New(_ context.Context, _ runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	return &PodGroupPodsCount{handle: h, fts: fts}, nil
}

// Name 返回插件名，供日志等场景使用。
func (pl *PodGroupPodsCount) Name() string {
	return Name
}

// ScorePlacement 为给定的 Placement 计算分数。
// 打分时同时把已调度（assumed/assigned）的 pod 与本次提议的分配计算在内，
// 目的是缩小各方案间的相对差距，使 pod 总数的微小变化只带来分数的微小变化。
func (pl *PodGroupPodsCount) ScorePlacement(ctx context.Context, state fwk.PlacementCycleState, podGroup fwk.PodGroupInfo, placement *fwk.PodGroupAssignments) (int64, *fwk.Status) {
	scheduledPodsCount, err := pl.getScheduledPodsCount(podGroup)
	if err != nil {
		return 0, fwk.AsStatus(err)
	}

	return int64(scheduledPodsCount + len(placement.ProposedAssignments)), nil
}

// PlacementScoreExtensions 在插件实现了该接口时返回 PlacementScoreExtensions，否则返回 nil。
// PodGroupPodsCount 实现了该接口。
func (pl *PodGroupPodsCount) PlacementScoreExtensions() fwk.PlacementScoreExtensions {
	return pl
}

// NormalizePlacementScore 把分数归一化到 [MinScore, MaxScore] 区间。
// 归一化以所有候选放置方案中的最大 pod 数为基准。
// 这里刻意不把 MinCount（group 所需的最小 pod 数）纳入计算，
// 以免 pod 数仅有微小差异时分数出现巨大落差。
func (pl *PodGroupPodsCount) NormalizePlacementScore(ctx context.Context, state fwk.PodGroupCycleState, podGroup fwk.PodGroupInfo, scores []fwk.PlacementScore) *fwk.Status {
	maxCount := int64(0)

	for _, score := range scores {
		maxCount = max(maxCount, score.Score)
	}

	if maxCount == 0 {
		// TODO: 本插件假定 gang scheduling 插件不会放行已调度 pod 数为 0 的 pod group。
		// 可以通过把它变成核心算法假设，或在插件里优雅处理来解决。
		// 归一化公式以 maxCount 为分母，为 0 时无法计算，故直接报错而非除零。
		return fwk.AsStatus(errors.New("no pods from pod group are assigned to any of the candidate placements"))
	}

	for i := range scores {
		scores[i].Score = fwk.MinScore + scores[i].Score*(fwk.MaxScore-fwk.MinScore)/maxCount
	}

	return nil
}

// getScheduledPodsCount 统计该 pod group 已调度的 pod 数；组合 pod group 的状态分散在多个实体中，需累加。
func (pl *PodGroupPodsCount) getScheduledPodsCount(podGroup fwk.PodGroupInfo) (int, error) {
	if pl.fts.EnableCompositePodGroup {
		count := 0
		for podGroupState, err := range helper.GetPodGroupStates(pl.handle.SnapshotSharedLister(), fwk.MustParseEntityKey(podGroup.GetKey())) {
			if err != nil {
				return 0, err
			}
			count += podGroupState.ScheduledPodsCount()
		}
		return count, nil
	}
	pgState, err := pl.handle.SnapshotSharedLister().PodGroupStates().Get(podGroup.GetNamespace(), podGroup.GetName())
	if err != nil {
		return 0, err
	}
	return pgState.ScheduledPodsCount(), nil
}
