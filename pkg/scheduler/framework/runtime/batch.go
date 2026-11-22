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

package runtime

import (
	"bytes"
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// batchHandle 是 OpportunisticBatch 所需的 framework.Framework 子集，
// 只暴露批量打分复用会用到的扩展点，便于测试时替换。
type batchHandle interface {
	ProfileName() string
	SnapshotSharedLister() fwk.SharedLister
	RunFilterPlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status
	RunPreScorePlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status
	RunRawScorePlugins(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) ([]fwk.PluginScore, *fwk.Status)
	NormalizeScores(ctx context.Context, state fwk.CycleState, pod *v1.Pod, scores []fwk.NodePluginScores) *fwk.Status
}

// OpportunisticBatch 在可能的情况下缓存过滤和打分结果，
// 以优化一批特征相同的普通 Pod 的调度速度。
type OpportunisticBatch struct {
	state     *batchState
	lastCycle schedulingCycle
	handle    batchHandle

	// 主要用于测试：统计成功复用批量结果的 Pod 总数。
	batchedPods int64

	genericWorkloadEnabled bool
}

// batchState 保存一次调度产生的、可复用给后续 Pod 的候选节点及打分结果。
type batchState struct {
	signature    fwk.PodSignature
	sortedNodes  framework.SortedScoredNodes
	creationTime time.Time
}

// schedulingCycle 记录上一个调度周期的结果，用于判断缓存是否还能接上。
type schedulingCycle struct {
	cycleCount int64
	chosenNode string
	succeeded  bool
}

const (
	// maxBatchAge 是批量缓存的最大有效期，超过后节点状态可能已过期，不能再复用。
	maxBatchAge = 500 * time.Millisecond
)

// GetNodeHint 根据上一周期的过滤与打分结果，为 Pod 提供一个候选节点提示。
// 缓存只对签名相同、且按“每节点一个 Pod”方式连续调度的 Pod 有效——否则旧的打分无法复用。
// 这一点通过检查上次排名最高的节点已不可行来间接保证：说明上一个 Pod 已成功调度，
// 而当前 Pod 已放不进那个节点。
func (b *OpportunisticBatch) GetNodeHint(ctx context.Context, pod *v1.Pod, signature fwk.PodSignature, state fwk.CycleState, cycleCount int64) string {
	logger := klog.FromContext(ctx)
	var hint string

	startTime := time.Now()
	defer func() {
		hinted := "hint"
		if hint == "" {
			hinted = "no_hint"
			metrics.BatchAttemptStats.WithLabelValues(b.handle.ProfileName(), metrics.BatchAttemptNoHint).Inc()
			logger.V(4).Info("OpportunisticBatch no node hint available for pod",
				"profile", b.handle.ProfileName(), "pod", klog.KObj(pod), "cycleCount", cycleCount)
		}
		metrics.GetNodeHintDuration.WithLabelValues(hinted, b.handle.ProfileName()).Observe(metrics.SinceInSeconds(startTime))
	}()

	// 没有可复用的缓存状态时返回空提示。
	if !b.batchStateCompatible(ctx, pod, signature, cycleCount) {
		return ""
	}

	// 重新检查上次选中的节点：若仍可行则重新打分，让它继续参与本轮提示的竞争。
	if !b.refreshHintCandidates(ctx, pod, cycleCount, state) {
		return ""
	}

	// 否则弹出缓存列表的头部作为提示；下一次存储结果时会用它比对提示是否被采纳。
	hint = b.state.sortedNodes.Pop().Name
	logger.V(3).Info("OpportunisticBatch provided node hint",
		"profile", b.handle.ProfileName(), "pod", klog.KObj(pod), "cycleCount", cycleCount, "hint", hint,
		"remainingNodes", b.state.sortedNodes.Len())

	return hint
}

// StoreScheduleResults 保存一次调度的结果，供后续 Pod 复用为批量缓存。
func (b *OpportunisticBatch) StoreScheduleResults(ctx context.Context, signature fwk.PodSignature, hintedNode, chosenNode string, otherNodes framework.SortedScoredNodes, cycleCount int64) {
	logger := klog.FromContext(ctx)

	defer metrics.StoreScheduleResultsDuration.ObserveSince(time.Now(), b.handle.ProfileName())()
	// 记录本周期信息，供下一个 Pod 判断缓存是否可以接续。
	b.lastCycle = schedulingCycle{
		cycleCount: cycleCount,
		chosenNode: chosenNode,
		succeeded:  true,
	}
	logger.V(4).Info("OpportunisticBatch set cycle state",
		"profile", b.handle.ProfileName(), "cycleCount", cycleCount, "hintedNode", hintedNode, "chosenNode", chosenNode)

	if hintedNode == chosenNode {
		logger.V(4).Info("OpportunisticBatch skipping set state because hint was provided",
			"profile", b.handle.ProfileName(), "cycleCount", cycleCount, "chosenNode", chosenNode)
		metrics.BatchAttemptStats.WithLabelValues(b.handle.ProfileName(), metrics.BatchAttemptHintUsed).Inc()
		b.batchedPods++
		return
	}

	// 只在没给提示、或提示未被采纳时保存新结果。
	// 顺便记录“给了提示却没用”这种相对少见的情形。
	if hintedNode != "" {
		metrics.BatchAttemptStats.WithLabelValues(b.handle.ProfileName(), metrics.BatchAttemptHintNotUsed).Inc()
		logger.V(4).Info("OpportunisticBatch hint not used",
			"profile", b.handle.ProfileName(), "cycleCount", b.lastCycle.cycleCount, "hint", hintedNode, "chosen", chosenNode)
	}

	// 只有这个 Pod 可批量化时才保存结果；下一次给提示时会拿它与新 Pod 做兼容性检查。
	if signature != nil && otherNodes != nil && otherNodes.Len() > 0 {
		b.state = &batchState{
			signature:    signature,
			sortedNodes:  otherNodes,
			creationTime: time.Now(),
		}
		if loggerV := logger.V(6); loggerV.Enabled() {
			loggerV.Info("OpportunisticBatch set batch information",
				"profile", b.handle.ProfileName(), "signature", b.state.signature, "nodes", otherNodes.Len(), "cycleCount", cycleCount)
		} else {
			logger.V(4).Info("OpportunisticBatch set batch information",
				"profile", b.handle.ProfileName(), "nodes", otherNodes.Len(), "cycleCount", cycleCount)
		}
	} else {
		reason := metrics.BatchFlushPodNotBatchable
		if otherNodes == nil || otherNodes.Len() == 0 {
			reason = metrics.BatchFlushEmptyList
		}

		b.logUnusableState(logger, cycleCount, reason)
		b.state = nil
	}
}

// logUnusableState 记录缓存失效的原因和指标：状态已无法保持最新，必须丢弃。
func (b *OpportunisticBatch) logUnusableState(logger klog.Logger, cycleCount int64, reason string) {
	metrics.BatchCacheFlushed.WithLabelValues(b.handle.ProfileName(), reason).Inc()
	logger.V(4).Info("OpportunisticBatch found unusable state",
		"profile", b.handle.ProfileName(), "cycleCount", cycleCount, "reason", reason)
}

// batchStateCompatible 检查缓存的批量状态对新 Pod 是否仍然有效。
func (b *OpportunisticBatch) batchStateCompatible(ctx context.Context, pod *v1.Pod, signature fwk.PodSignature,
	cycleCount int64) bool {
	// 没有可用状态时直接返回。
	if b.stateEmpty() {
		return false
	}
	logger := klog.FromContext(ctx)

	// 周期号不连续说明上一个 Pod 是由另一个 profile 调度的，缓存状态不能再用。
	if cycleCount != b.lastCycle.cycleCount+1 {
		// PodGroup 调度周期中多个 Pod 会共享同一个周期号，此时可以复用缓存。
		if !b.genericWorkloadEnabled || cycleCount != b.lastCycle.cycleCount {
			b.logUnusableState(logger, cycleCount, metrics.BatchFlushPodSkipped)
			return false
		}
	}

	// 上一个 Pod 调度失败时，缓存状态不可复用。
	if !b.lastCycle.succeeded {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushPodFailed)
		return false
	}

	// 带提名节点的 Pod 绕过批量提示机制。
	if pod.Status.NominatedNodeName != "" {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushPodNominated)
		return false
	}

	// 新 Pod 与缓存状态不兼容时丢弃缓存。
	if signature == nil || !bytes.Equal(signature, b.state.signature) {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushPodIncompatible)
		return false
	}

	// 缓存太旧时丢弃：避免连续兼容 Pod 太多、或两个 Pod 之间等待过久导致状态失真。
	if time.Now().After((b.state.creationTime.Add(maxBatchAge))) {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushExpired)
		return false
	}

	// 缓存与新 Pod 匹配且未过期，可以复用。
	return true
}

// refreshHintCandidates 检查上次选中的节点对新 Pod 是否仍可行；若可行则重新打分并放回候选列表，
// 使其能参与下一次提示的竞争。节点丢失或重新打分失败时返回 false。
func (b *OpportunisticBatch) refreshHintCandidates(ctx context.Context, pod *v1.Pod, cycleCount int64,
	state fwk.CycleState) bool {
	logger := klog.FromContext(ctx)
	lastChosenNode, err := b.handle.SnapshotSharedLister().NodeInfos().Get(b.lastCycle.chosenNode)
	if lastChosenNode == nil || err != nil {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushNodeMissing)
		return false
	}

	status := b.handle.RunFilterPlugins(ctx, state, pod, lastChosenNode)
	if status.IsSuccess() {
		// 上次选中的节点仍然可行，重新打分后让它与缓存候选一起竞争下一次提示。
		return b.rescoreHintedNode(ctx, pod, state, cycleCount, lastChosenNode)
	}
	if !status.IsRejected() {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushFilterError)
		return false
	}
	return true
}

// rescoreHintedNode 对上次选中的节点重新执行 Score()，以新分数放回候选列表，
// 在全部候选上重新归一化后重建排序列表。
func (b *OpportunisticBatch) rescoreHintedNode(ctx context.Context, pod *v1.Pod, state fwk.CycleState, cycleCount int64,
	lastChosenNodeInfo fwk.NodeInfo) bool {
	logger := klog.FromContext(ctx)
	defer metrics.BatchRescoreDuration.ObserveSince(time.Now(), b.handle.ProfileName())()
	metrics.BatchRescoreAttempts.WithLabelValues(b.handle.ProfileName()).Inc()

	nodeInfos, filteredScores, ok := b.resolveCachedCandidates()
	if !ok {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushNodeMissing)
		return false
	}
	// PreScore 必须看到完整候选集，包括正在重新打分的这个节点。
	nodeInfos = append(nodeInfos, lastChosenNodeInfo)
	// 克隆当前 CycleState，避免重新打分路径中的写入泄漏回主调度状态。
	rescoreState := state.Clone()
	status := b.handle.RunPreScorePlugins(ctx, rescoreState, pod, nodeInfos)
	if !status.IsSuccess() {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushPreScoreError)
		return false
	}

	freshScores, status := b.handle.RunRawScorePlugins(ctx, rescoreState, pod, lastChosenNodeInfo)
	if !status.IsSuccess() {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushRescoreError)
		return false
	}

	// 把上次选中的节点及其新分数加入 filteredScores，使其与 nodeInfos 保持一一对应，
	// 然后在全部候选上重新做归一化。
	filteredScores = append(filteredScores,
		fwk.NodePluginScores{
			Name:      lastChosenNodeInfo.Node().Name,
			RawScores: freshScores,
		},
	)

	if status := b.handle.NormalizeScores(ctx, rescoreState, pod, filteredScores); !status.IsSuccess() {
		b.logUnusableState(logger, cycleCount, metrics.BatchFlushNormalizeError)
		return false
	}

	b.state.sortedNodes = framework.NewSortedScoredNodes(filteredScores)
	return true
}

// resolveCachedCandidates 把缓存中的排序节点解析为 NodeInfo 及其 NodePluginScores。
// 任一缓存节点在快照中已不存在时返回 false。
func (b *OpportunisticBatch) resolveCachedCandidates() ([]fwk.NodeInfo, []fwk.NodePluginScores, bool) {
	cached := b.state.sortedNodes.UnorderedList()
	nodeInfos := make([]fwk.NodeInfo, 0, len(cached)+1)
	scores := make([]fwk.NodePluginScores, 0, len(cached)+1)
	lister := b.handle.SnapshotSharedLister().NodeInfos()
	for _, ns := range cached {
		ni, err := lister.Get(ns.Name)
		if err != nil || ni == nil {
			return nil, nil, false
		}
		nodeInfos = append(nodeInfos, ni)
		scores = append(scores, ns)
	}
	return nodeInfos, scores, true
}

// stateEmpty 判断缓存状态是否为空。
// 空状态有多种表现形式，与其在产生时统一规范化，不如在这里一次性都检查掉。
func (b *OpportunisticBatch) stateEmpty() bool {
	return b.state == nil || b.state.sortedNodes == nil || b.state.sortedNodes.Len() == 0
}

// newOpportunisticBatch 创建批量调度器，genericWorkloadEnabled 表示是否启用了通用工作负载调度周期。
func newOpportunisticBatch(h batchHandle, genericWorkloadEnabled bool) *OpportunisticBatch {
	return &OpportunisticBatch{
		handle:                 h,
		genericWorkloadEnabled: genericWorkloadEnabled,
	}
}
