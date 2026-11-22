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
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// nodeInfoLister 是测试用的 fwk.NodeInfo 集合，实现 fwk.NodeInfoLister。
type nodeInfoLister []fwk.NodeInfo

// Get 按节点名返回假节点对象。
func (nodes nodeInfoLister) Get(nodeName string) (fwk.NodeInfo, error) {
	for _, node := range nodes {
		if node != nil && node.Node().Name == nodeName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("unable to find node: %s", nodeName)
}

// List 返回全部假节点。
func (nodes nodeInfoLister) List() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithAffinityList 本应只列出带亲和性 Pod 的节点，
// 假实现直接返回全部节点，测试不依赖此过滤效果。
func (nodes nodeInfoLister) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredAntiAffinityList 本应只列出带必需反亲和性 Pod 的节点，
// 假实现直接返回全部节点。
func (nodes nodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredNonHostScopedAntiAffinityList 本应只列出带非主机域必需反亲和性 Pod 的节点，
// 假实现直接返回全部节点。
func (nodes nodeInfoLister) HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// sharedLister 是测试用的 fwk.SharedLister 实现。
type sharedLister struct {
	nodes nodeInfoLister
}

// NodeInfos 返回节点 lister。
func (s sharedLister) NodeInfos() fwk.NodeInfoLister {
	return s.nodes
}

// storageInfoListerContract 是测试用的最小存储 lister，恒报 PVC 未被使用。
type storageInfoListerContract struct{}

// IsPVCUsedByPods 恒返回 false，测试不涉及 PVC 引用统计。
func (c *storageInfoListerContract) IsPVCUsedByPods(_ string) bool {
	return false
}

// StorageInfos 返回存储 lister。
func (s sharedLister) StorageInfos() fwk.StorageInfoLister {
	return &storageInfoListerContract{}
}

// PodGroupStates 返回 nil，测试不涉及 pod group 状态。
func (s sharedLister) PodGroupStates() fwk.PodGroupStateLister {
	return nil
}

// PodGroups 返回 nil，测试不涉及 pod group。
func (s sharedLister) PodGroups() fwk.PodGroupLister {
	return nil
}

// CompositePodGroupStates 返回 nil，测试不涉及 composite pod group 状态。
func (s sharedLister) CompositePodGroupStates() fwk.CompositePodGroupStateLister {
	return nil
}

// CompositePodGroups 返回 nil，测试不涉及 composite pod group。
func (s sharedLister) CompositePodGroups() fwk.CompositePodGroupLister {
	return nil
}

// BatchTestPlugin 是测试用过滤插件，只允许节点上存在特定前缀的 Pod。
type BatchTestPlugin struct{}

// Name 返回插件名。
func (pl *BatchTestPlugin) Name() string {
	return "batchTest"
}

// blockingPodPrefix 标记会与同类 Pod 互相阻塞的 Pod ID 前缀。
const blockingPodPrefix = 'b'

// blockingPodID 构造一个阻塞型 Pod ID。
func blockingPodID(suffix string) string {
	return fmt.Sprintf("%c-%s", blockingPodPrefix, suffix)
}

// nonBlockingPodPrefix 标记不会与其他 Pod 冲突的 Pod ID 前缀。
const nonBlockingPodPrefix = 'a'

// nonBlockingPodID 构造一个非阻塞型 Pod ID。
func nonBlockingPodID(suffix string) string {
	return fmt.Sprintf("%c-%s", nonBlockingPodPrefix, suffix)
}

// Filter 假设每个节点只能容纳一个 ID 以 blockingPodPrefix 开头的 Pod，
// 这样可以构造"是否阻塞后续同类 Pod"的测试场景。
func (pl *BatchTestPlugin) Filter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	podID := pod.GetUID()
	for _, nodePod := range nodeInfo.GetPods() {
		npid := nodePod.GetPod().GetUID()
		if podID[0] == blockingPodPrefix && npid[0] == blockingPodPrefix {
			return fwk.NewStatus(fwk.Unschedulable, "unsched")
		}
	}
	return nil
}

// newBatchTestPlugin 是 BatchTestPlugin 的工厂。
func newBatchTestPlugin(_ context.Context, injArgs runtime.Object, f fwk.Handle) (fwk.Plugin, error) {
	return &BatchTestPlugin{}, nil
}

// newBatchTestFramework 构造带批量测试插件的 framework，
// 可选择注入可配置的打分/过滤插件，并返回供测试修改的 lister。
func newBatchTestFramework(ctx context.Context, scorePl *configurableScorePlugin, filterPl *configurableFilterPlugin) (framework.Framework, *sharedLister, error) {
	r := Registry{
		"batchTest":     newBatchTestPlugin,
		queueSortPlugin: newQueueSortPlugin,
		bindPlugin:      newBindPlugin,
	}
	plugins := &config.Plugins{}
	profile := config.KubeSchedulerProfile{Plugins: plugins}
	profile.Plugins.QueueSort.Enabled = []config.Plugin{{Name: queueSortPlugin}}
	profile.Plugins.Bind.Enabled = []config.Plugin{{Name: bindPlugin}}
	profile.Plugins.Filter.Enabled = []config.Plugin{{Name: "batchTest"}}
	if filterPl != nil {
		r["configurableFilter"] = func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) { return filterPl, nil }
		profile.Plugins.Filter.Enabled = append(profile.Plugins.Filter.Enabled, config.Plugin{Name: "configurableFilter"})
	}
	if scorePl != nil {
		r["configurableScore"] = func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) { return scorePl, nil }
		profile.Plugins.PreScore.Enabled = []config.Plugin{{Name: "configurableScore"}}
		profile.Plugins.Score.Enabled = []config.Plugin{{Name: "configurableScore", Weight: 1}}
	}

	lister := &sharedLister{nodes: nodeInfoLister{}}

	ret, err := NewFramework(ctx, r, &profile, WithSnapshotSharedLister(lister))

	return ret, lister, err
}

// testSortedScoredNodes 是仅按节点名排序的测试用 SortedScoredNodes 实现。
type testSortedScoredNodes struct {
	Nodes []string
}

var _ framework.SortedScoredNodes = &testSortedScoredNodes{}

// Pop 弹出首个节点名。
func (t *testSortedScoredNodes) Pop() fwk.NodePluginScores {
	ret := fwk.NodePluginScores{Name: t.Nodes[0]}
	t.Nodes = t.Nodes[1:]
	return ret
}

// Len 返回剩余节点数。
func (t *testSortedScoredNodes) Len() int {
	return len(t.Nodes)
}

// UnorderedList 返回所有节点名对应的 NodePluginScores。
func (t *testSortedScoredNodes) UnorderedList() []fwk.NodePluginScores {
	result := make([]fwk.NodePluginScores, len(t.Nodes))
	for i, name := range t.Nodes {
		result[i] = fwk.NodePluginScores{Name: name}
	}
	return result
}

// newTestNodes 由节点名列表构造测试用 SortedScoredNodes。
func newTestNodes(n []string) *testSortedScoredNodes {
	return &testSortedScoredNodes{Nodes: n}
}

// TestBatchBasic 先让 OpportunisticBatch 处理第一个 Pod 并存储结果，
// 再检查第二个 Pod 是否按预期获得节点提示（hint）。
func TestBatchBasic(t *testing.T) {
	// 本测试先让 OpportunisticBatch 处理第一个 Pod，再观察它对第二个 Pod 的行为。
	tests := []struct {
		name                          string
		firstPodID                    string
		firstSig                      string
		firstPodScheduledSuccessfully bool
		// 仅在 firstPodScheduledSuccessfully 为 true 时设置。
		firstChosenNode string
		// 仅在 firstPodScheduledSuccessfully 为 true 时设置。
		firstOtherNodes framework.SortedScoredNodes
		// 为 true 时模拟两个 Pod 在同一个 PodGroup 调度周期内处理。
		sameCycle bool
		// 为 true 时模拟两个 Pod 之间夹着另一个 profile 处理的 Pod。
		skipPod                    bool
		secondPodID                string
		secondPodNominatedNodeName string
		secondSig                  string
		secondChosenNode           string
		secondOtherNodes           framework.SortedScoredNodes
		genericWorkloadEnabled     bool
		expectedHint               string
		expectedState              *batchState
	}{
		{
			name:                          "a second pod with the same signature gets a hint",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			expectedHint:                  "n1",
		},
		{
			name:                          "a second pod with a different signature doesn't get a hint",
			firstPodID:                    nonBlockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: true,
			secondPodID:                   nonBlockingPodID("2"),
			secondSig:                     "sig2",
			secondChosenNode:              "n1",
			expectedHint:                  "",
		},
		{
			name:                          "pod doesn't get hint if previous pod didn't scheduled",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: false,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			expectedHint:                  "",
		},
		{
			name:                          "empty list",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{}),
			firstPodScheduledSuccessfully: true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n4",
			expectedHint:                  "",
		},
		{
			name:                          "nil list",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               nil,
			firstPodScheduledSuccessfully: true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n4",
			expectedHint:                  "",
		},
		{
			name:                          "pod doesn't get hint because the previous pod is to a different profile",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: true,
			skipPod:                       true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			expectedHint:                  "",
		},
		{
			name:                          "pod doesn't use batch from preceding pod when they are from the same cycle state, but GenericWorkload is disabled",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: true,
			sameCycle:                     true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			genericWorkloadEnabled:        false,
			expectedHint:                  "",
		},
		{
			name:                          "pod uses batch from preceding pod when they are from the same cycle state and GenericWorkload is enabled",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: true,
			sameCycle:                     true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			genericWorkloadEnabled:        true,
			expectedHint:                  "n1",
		},
		{
			name:                          "pod uses batch from preceding pod and leaves remaining batch to next pod",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1", "n2"}),
			firstPodScheduledSuccessfully: true,
			secondPodID:                   blockingPodID("2"),
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			expectedHint:                  "n1",
			expectedState: &batchState{
				signature:   []byte("sig"),
				sortedNodes: newTestNodes([]string{"n2"}),
			},
		},
		{
			name:                          "a second pod with a nominated node does not get a hint",
			firstPodID:                    blockingPodID("1"),
			firstSig:                      "sig",
			firstChosenNode:               "n3",
			firstOtherNodes:               newTestNodes([]string{"n1"}),
			firstPodScheduledSuccessfully: true,
			secondPodID:                   blockingPodID("2"),
			secondPodNominatedNodeName:    "n1",
			secondSig:                     "sig",
			secondChosenNode:              "n1",
			expectedHint:                  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			testFwk, lister, err := newBatchTestFramework(ctx, nil, nil)
			if err != nil {
				t.Fatalf("Failed to create framework for testing: %v", err)
			}

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod",
					UID:  types.UID(tt.firstPodID),
				},
			}

			signature := fwk.PodSignature(tt.firstSig)
			batch := newOpportunisticBatch(testFwk, tt.genericWorkloadEnabled)
			state := framework.NewCycleState()

			// 先让第一个"Pod"走一遍 GetNodeHint
			hint := batch.GetNodeHint(ctx, pod, signature, state, 1)
			if hint != "" {
				t.Fatalf("Got unexpected hint %s", hint)
			}
			if tt.firstPodScheduledSuccessfully {
				batch.StoreScheduleResults(ctx, []byte(tt.firstSig), hint, tt.firstChosenNode, tt.firstOtherNodes, 1)
			}

			var cycleCount int64 = 2
			if tt.skipPod {
				cycleCount = 3
			} else if tt.sameCycle {
				cycleCount = 1
			}

			// 再处理第二个 Pod
			pod2 := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "pod2",
					UID:  types.UID(tt.secondPodID),
				},
				Status: v1.PodStatus{
					NominatedNodeName: tt.secondPodNominatedNodeName,
				},
			}

			lastChosenNode := framework.NewNodeInfo(pod)
			lastChosenNode.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{
				Name: tt.firstChosenNode,
				UID:  types.UID(tt.firstChosenNode),
			}})
			lister.nodes = nodeInfoLister{lastChosenNode}

			signature = fwk.PodSignature(tt.secondSig)
			hint = batch.GetNodeHint(ctx, pod2, signature, state, cycleCount)

			if hint != tt.expectedHint {
				t.Fatalf("Got hint '%s' expected '%s' for test '%s'", hint, tt.expectedHint, tt.name)
			}

			batch.StoreScheduleResults(ctx, []byte(tt.secondSig), hint, tt.secondChosenNode, tt.secondOtherNodes, cycleCount)

			batchEmpty := batch.state == nil || batch.state.sortedNodes == nil || batch.state.sortedNodes.Len() == 0
			expectedEmpty := tt.expectedState == nil

			if batchEmpty != expectedEmpty {
				t.Fatalf("Expected empty %t, got empty %t for %s", expectedEmpty, batchEmpty, tt.name)
			}
			if !expectedEmpty {
				if !bytes.Equal(batch.state.signature, []byte(tt.expectedState.signature)) {
					t.Fatalf("Got state signature '%s' expected '%s' for test '%s'", batch.state.signature, tt.expectedState.signature, tt.name)
				}
				nodesDiff := cmp.Diff(tt.expectedState.sortedNodes, batch.state.sortedNodes)
				if nodesDiff != "" {
					t.Fatalf("Diff between sortedNodes (-want,+got):\n%s", nodesDiff)
				}
			}
		})
	}
}

// preScoreStateKey 是 configurableScorePlugin 使用的 CycleState key。
const preScoreStateKey fwk.StateKey = "configurableScorePlugin/preScoreSeenNodes"

// preScoreSeenNodes 记录 PreScore 收到的节点名，
// 便于 NormalizeScore 校验自己拿到的是完整候选集。
type preScoreSeenNodes map[string]struct{}

// Clone 复制状态数据，避免跨周期共享可变 map。
func (s preScoreSeenNodes) Clone() fwk.StateData {
	clone := make(preScoreSeenNodes, len(s))
	for k := range s {
		clone[k] = struct{}{}
	}
	return clone
}

// configurableFilterPlugin 是可注入错误的测试用过滤插件。
type configurableFilterPlugin struct {
	filterErr bool
}

// Name 返回插件名。
func (pl *configurableFilterPlugin) Name() string { return "configurableFilter" }

// Filter 按配置返回注入的错误或通过。
func (pl *configurableFilterPlugin) Filter(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) *fwk.Status {
	if pl.filterErr {
		return fwk.AsStatus(fmt.Errorf("injected filter error"))
	}
	return nil
}

// configurableScorePlugin 是可配置得分与各阶段错误的测试用打分插件。
type configurableScorePlugin struct {
	score       int64
	preScoreErr bool
	scoreErr    bool
	normErr     bool
}

// Name 返回插件名。
func (pl *configurableScorePlugin) Name() string { return "configurableScore" }

// PreScore 记录传入的节点名，供 NormalizeScore 校验候选集一致性。
func (pl *configurableScorePlugin) PreScore(_ context.Context, state fwk.CycleState, _ *v1.Pod, nodes []fwk.NodeInfo) *fwk.Status {
	if pl.preScoreErr {
		return fwk.AsStatus(fmt.Errorf("injected prescore error"))
	}
	seen := make(preScoreSeenNodes, len(nodes))
	for _, n := range nodes {
		seen[n.Node().Name] = struct{}{}
	}
	state.Write(preScoreStateKey, seen)
	return nil
}

// Score 返回配置的得分或注入的错误。
func (pl *configurableScorePlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, _ fwk.NodeInfo) (int64, *fwk.Status) {
	if pl.scoreErr {
		return 0, fwk.AsStatus(fmt.Errorf("injected score error"))
	}
	return pl.score, nil
}

// ScoreExtensions 返回自身作为 ScoreExtensions 实现。
func (pl *configurableScorePlugin) ScoreExtensions() fwk.ScoreExtensions { return pl }

// NormalizeScore 校验每个待归一化节点都出现在 PreScore 记录的候选集中。
func (pl *configurableScorePlugin) NormalizeScore(_ context.Context, state fwk.CycleState, _ *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	if pl.normErr {
		return fwk.AsStatus(fmt.Errorf("injected normalize error"))
	}
	c, err := state.Read(preScoreStateKey)
	if err != nil {
		return fwk.AsStatus(fmt.Errorf("NormalizeScore: missing PreScore state: %w", err))
	}
	seen := c.(preScoreSeenNodes)
	for _, score := range scores {
		if _, ok := seen[score.Name]; !ok {
			return fwk.AsStatus(fmt.Errorf("NormalizeScore called for node %q which was not passed to PreScore",
				score.Name))
		}
	}
	return nil
}

// TestBatchRescore 验证重评分（rescore）流程：根据 n2 的新得分与 n1 的缓存得分比较，
// 正确给出提示；任一插件出错或 lister 缺少必要节点时都不给出提示。
func TestBatchRescore(t *testing.T) {
	const n1CachedScore = 50 // 所有用例中 n1 的固定缓存得分

	tests := []struct {
		name                   string
		score                  int64 // 重评分后 n2 的新得分
		filterErr              bool
		preScoreErr            bool
		scoreErr               bool
		normErr                bool
		chosenNodeMissing      bool
		cachedNodeMissing      bool
		expectedHint           string
		expectedRemainingNodes []string
	}{
		{
			name:                   "rescored node wins",
			score:                  100,
			expectedHint:           "n2",
			expectedRemainingNodes: []string{"n1"},
		},
		{
			name:                   "rescored node loses",
			score:                  10,
			expectedHint:           "n1",
			expectedRemainingNodes: []string{"n2"},
		},
		{
			name:                   "filter error gives no hint",
			score:                  100,
			filterErr:              true,
			expectedHint:           "",
			expectedRemainingNodes: []string{"n1"},
		},
		{
			name:                   "prescore error gives no hint",
			preScoreErr:            true,
			expectedHint:           "",
			expectedRemainingNodes: []string{"n1"},
		},
		{
			name:                   "score error gives no hint",
			scoreErr:               true,
			expectedHint:           "",
			expectedRemainingNodes: []string{"n1"},
		},
		{
			name:                   "normalize error gives no hint",
			normErr:                true,
			expectedHint:           "",
			expectedRemainingNodes: []string{"n1"},
		},
		{
			name:                   "chosen node missing from lister gives no hint",
			chosenNodeMissing:      true,
			expectedHint:           "",
			expectedRemainingNodes: []string{"n1"},
		},
		{
			name:                   "cached node missing from lister gives no hint",
			score:                  100,
			cachedNodeMissing:      true,
			expectedHint:           "",
			expectedRemainingNodes: []string{"n1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			scorePl := &configurableScorePlugin{
				score:       tt.score,
				preScoreErr: tt.preScoreErr,
				scoreErr:    tt.scoreErr,
				normErr:     tt.normErr,
			}
			filterPl := &configurableFilterPlugin{
				filterErr: tt.filterErr,
			}
			testFwk, lister, err := newBatchTestFramework(ctx, scorePl, filterPl)
			if err != nil {
				t.Fatalf("Failed to create framework: %v", err)
			}

			// 第一个 Pod：非阻塞型，选中 n2，n1 作为其他候选留下
			pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", UID: types.UID(nonBlockingPodID("1"))}}
			sig := fwk.PodSignature("sig")
			batch := newOpportunisticBatch(testFwk, false)

			cachedNodes := framework.NewSortedScoredNodes([]fwk.NodePluginScores{
				{Name: "n1", RawScores: []fwk.PluginScore{{Name: "configurableScore", Score: n1CachedScore}}},
			})
			batch.GetNodeHint(ctx, pod1, sig, framework.NewCycleState(), 1)
			batch.StoreScheduleResults(ctx, []byte("sig"), "", "n2", cachedNodes, 1)

			// n2（上次选中的节点）必须在 lister 中，refreshHintCandidates 才能继续。
			// cachedNodeMissing 为真时省略 n1，以触发 resolveCachedCandidates 的提前返回路径。
			if !tt.chosenNodeMissing {
				n2Info := framework.NewNodeInfo(pod1)
				n2Info.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2", UID: "n2"}})
				if tt.cachedNodeMissing {
					lister.nodes = nodeInfoLister{n2Info}
				} else {
					n1Info := framework.NewNodeInfo()
					n1Info.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "n1"}})
					lister.nodes = nodeInfoLister{n1Info, n2Info}
				}
			}

			pod2 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: types.UID(nonBlockingPodID("2"))}}
			hint := batch.GetNodeHint(ctx, pod2, sig, framework.NewCycleState(), 2)

			if hint != tt.expectedHint {
				t.Fatalf("got hint %q, expected %q", hint, tt.expectedHint)
			}

			if tt.expectedRemainingNodes != nil {
				if batch.state == nil || batch.state.sortedNodes == nil {
					t.Fatal("expected non-nil batch state after hint")
				}
				if got, want := batch.state.sortedNodes.Len(), len(tt.expectedRemainingNodes); got != want {
					t.Fatalf("remaining node count: got %d, want %d", got, want)
				}
				for i, want := range tt.expectedRemainingNodes {
					if got := batch.state.sortedNodes.Pop().Name; got != want {
						t.Fatalf("remaining node[%d]: got %q, want %q", i, got, want)
					}
				}
			}
		})
	}
}

// TestBatchRescoreChain 验证三个 Pod 连续调度时，
// 每个周期都触发重评分，且批量状态在整个过程中保持正确。
func TestBatchRescoreChain(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	const n1CachedScore = 50
	// n2 重评分得 100，始终高于 n1 的 50
	scorePl := &configurableScorePlugin{score: 100}
	testFwk, lister, err := newBatchTestFramework(ctx, scorePl, nil)
	if err != nil {
		t.Fatalf("Failed to create framework: %v", err)
	}

	sig := fwk.PodSignature("sig")
	batch := newOpportunisticBatch(testFwk, false)

	// Pod1：选中 n2，n1 作为唯一的其他候选保存
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod1", UID: types.UID(nonBlockingPodID("1"))}}
	otherNodes := framework.NewSortedScoredNodes([]fwk.NodePluginScores{
		{Name: "n1", RawScores: []fwk.PluginScore{{Name: "configurableScore", Score: n1CachedScore}}},
	})
	batch.GetNodeHint(ctx, pod1, sig, framework.NewCycleState(), 1)
	batch.StoreScheduleResults(ctx, []byte("sig"), "", "n2", otherNodes, 1)

	n1Info := framework.NewNodeInfo()
	n1Info.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", UID: "n1"}})
	n2Info := framework.NewNodeInfo(pod1)
	n2Info.SetNode(&v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n2", UID: "n2"}})
	lister.nodes = nodeInfoLister{n1Info, n2Info}

	// Pod2：触发重评分（n2 仍可行），n2 胜出
	pod2 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: types.UID(nonBlockingPodID("2"))}}
	hint2 := batch.GetNodeHint(ctx, pod2, sig, framework.NewCycleState(), 2)
	if hint2 != "n2" {
		t.Fatalf("pod2: got hint %q, want %q", hint2, "n2")
	}
	batch.StoreScheduleResults(ctx, []byte("sig"), hint2, "n2", nil, 2)

	// Pod3：再次触发重评分，n2 再次胜出。
	// n1 必须保留在状态中，供可能的 Pod4 使用。
	pod3 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod3", UID: types.UID(nonBlockingPodID("3"))}}
	hint3 := batch.GetNodeHint(ctx, pod3, sig, framework.NewCycleState(), 3)
	if hint3 != "n2" {
		t.Fatalf("pod3: got hint %q, want %q", hint3, "n2")
	}
	if batch.state == nil || batch.state.sortedNodes == nil || batch.state.sortedNodes.Len() != 1 {
		t.Fatal("expected 1 remaining node in state after pod3 hint")
	}
	if got := batch.state.sortedNodes.Pop().Name; got != "n1" {
		t.Fatalf("remaining node after pod3: got %q, want %q", got, "n1")
	}
}
