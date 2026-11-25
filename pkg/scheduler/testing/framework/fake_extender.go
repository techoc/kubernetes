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
	"sort"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// FitPredicate 是 fake extender 使用的过滤函数类型。
type FitPredicate func(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status

// PriorityFunc 是 fake extender 使用的打分函数类型。
type PriorityFunc func(pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.NodeScoreList, error)

// PriorityConfig 配置 fake extender 打分所用的函数及权重。
type PriorityConfig struct {
	Function PriorityFunc
	Weight   int64
}

// ErrorPredicateExtender 总是返回错误状态的 FitPredicate 实现。
func ErrorPredicateExtender(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Error, "some error")
}

// FalsePredicateExtender 总是返回不可调度状态的 FitPredicate 实现。
func FalsePredicateExtender(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("pod is unschedulable on the node %q", node.Node().Name))
}

// TruePredicateExtender 总是返回成功状态的 FitPredicate 实现。
func TruePredicateExtender(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
	return fwk.NewStatus(fwk.Success)
}

// Node1PredicateExtender 仅在节点名为 "node1" 时返回成功，否则返回不可调度。
func Node1PredicateExtender(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
	if node.Node().Name == "node1" {
		return fwk.NewStatus(fwk.Success)
	}
	return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
}

// Node2PredicateExtender 仅在节点名为 "node2" 时返回成功，否则返回不可调度。
func Node2PredicateExtender(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
	if node.Node().Name == "node2" {
		return fwk.NewStatus(fwk.Success)
	}
	return fwk.NewStatus(fwk.Unschedulable, fmt.Sprintf("node %q is not allowed", node.Node().Name))
}

// ErrorPrioritizerExtender 总是返回错误的 PriorityFunc 实现。
func ErrorPrioritizerExtender(pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.NodeScoreList, error) {
	return &fwk.NodeScoreList{}, fmt.Errorf("some error")
}

// Node1PrioritizerExtender 对 "node1" 给 10 分，其余节点给 1 分。
func Node1PrioritizerExtender(pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.NodeScoreList, error) {
	result := fwk.NodeScoreList{}
	for _, node := range nodes {
		score := 1
		if node.Node().Name == "node1" {
			score = 10
		}
		result = append(result, fwk.NodeScore{Name: node.Node().Name, Score: int64(score)})
	}
	return &result, nil
}

// Node2PrioritizerExtender 对 "node2" 给 10 分，其余节点给 1 分。
func Node2PrioritizerExtender(pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.NodeScoreList, error) {
	result := fwk.NodeScoreList{}
	for _, node := range nodes {
		score := 1
		if node.Node().Name == "node2" {
			score = 10
		}
		result = append(result, fwk.NodeScore{Name: node.Node().Name, Score: int64(score)})
	}
	return &result, nil
}

// node2PrioritizerPlugin 是测试用的打分插件，偏好 node2。
type node2PrioritizerPlugin struct{}

// NewNode2PrioritizerPlugin 返回构建 node2PrioritizerPlugin 的工厂函数。
func NewNode2PrioritizerPlugin() frameworkruntime.PluginFactory {
	return func(_ context.Context, _ runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
		return &node2PrioritizerPlugin{}, nil
	}
}

// Name 返回插件名称。
func (pl *node2PrioritizerPlugin) Name() string {
	return "Node2Prioritizer"
}

// Score 对 "node2" 返回 100 分，其余节点返回 10 分。
func (pl *node2PrioritizerPlugin) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	score := 10
	if nodeInfo.Node().Name == "node2" {
		score = 100
	}
	return int64(score), nil
}

// ScoreExtensions 返回 nil，本插件不使用打分扩展。
func (pl *node2PrioritizerPlugin) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// FakeExtender 是 Extender 接口的测试用实现，过滤、打分、抢占行为均可注入。
type FakeExtender struct {
	// ExtenderName 是该 fake extender 的名称，配置中名称应唯一。
	ExtenderName     string
	Predicates       []FitPredicate
	Prioritizers     []PriorityConfig
	Weight           int64
	NodeCacheCapable bool
	FilteredNodes    []fwk.NodeInfo
	UnInterested     bool
	Ignorable        bool
	Binder           func() error

	// CachedNodeNameToInfo 是 fake extender 持有的节点信息缓存。
	CachedNodeNameToInfo map[string]*framework.NodeInfo
}

const defaultFakeExtenderName = "defaultFakeExtender"

// Name 返回 extender 名称；未设置时使用默认名称。
func (f *FakeExtender) Name() string {
	if f.ExtenderName == "" {
		// 未显式命名时回退到默认名称，便于断言。
		return defaultFakeExtenderName
	}
	return f.ExtenderName
}

// IsIgnorable 返回内部错误是否可被忽略。
func (f *FakeExtender) IsIgnorable() bool {
	return f.Ignorable
}

// SupportsPreemption 返回该 extender 是否支持抢占；测试中假定总是支持。
func (f *FakeExtender) SupportsPreemption() bool {
	// 测试用 extender 假定已定义 preempt verb。
	return true
}

// ProcessPreemption 实现 extender 的抢占逻辑。
func (f *FakeExtender) ProcessPreemption(
	pod *v1.Pod,
	nodeNameToVictims map[string]*extenderv1.Victims,
	nodeInfos fwk.NodeInfoLister,
) (map[string]*extenderv1.Victims, error) {
	nodeNameToVictimsCopy := map[string]*extenderv1.Victims{}
	// 不修改入参 map，后续增删只作用于副本。
	for k, v := range nodeNameToVictims {
		// 真实实现中 extender 需自行按名称获取节点对象（如查询 kube-apiserver）。
		// 测试中直接使用传入的 nodeInfos 即可。
		nodeNameToVictimsCopy[k] = v
	}

	// Extender.ProcessPreemption 没有 context 参数，无法从中取 logger；
	// 为保持接口不变，这里用 klog.TODO()。
	logger := klog.TODO()
	for nodeName, victims := range nodeNameToVictimsCopy {
		// 尝试在 extender 侧执行抢占。
		nodeInfo, _ := nodeInfos.Get(nodeName)
		extenderVictimPods, extenderPDBViolations, fits, err := f.selectVictimsOnNodeByExtender(logger, pod, nodeInfo)
		if err != nil {
			return nil, err
		}
		// 若 extender 抢占后仍放不下 Pod，该节点无法通过抢占解决，从候选中剔除。
		if !fits {
			delete(nodeNameToVictimsCopy, nodeName)
		} else {
			// 把 extender 选出的牺牲者追加到原结果中。
			nodeNameToVictimsCopy[nodeName].Pods = append(victims.Pods, extenderVictimPods...)
			nodeNameToVictimsCopy[nodeName].NumPDBViolations = victims.NumPDBViolations + int64(extenderPDBViolations)
		}
	}
	return nodeNameToVictimsCopy, nil
}

// selectVictimsOnNodeByExtender 用 extender 侧的过滤条件检查抢占后的节点。
// 返回：extender 新增的牺牲者列表、违反 PDB 的牺牲者数（用于计分）、抢占后是否放得下，以及错误。
func (f *FakeExtender) selectVictimsOnNodeByExtender(logger klog.Logger, pod *v1.Pod, node fwk.NodeInfo) ([]*v1.Pod, int, bool, error) {
	// 支持抢占但没有节点缓存时，只跑一次过滤，确认默认调度器的判断在 extender 侧同样成立。
	if !f.NodeCacheCapable {
		err := f.runPredicate(pod, node)
		if err.IsSuccess() {
			return []*v1.Pod{}, 0, true, nil
		} else if err.IsRejected() {
			return nil, 0, false, nil
		} else {
			return nil, 0, false, err.AsError()
		}
	}

	// 支持抢占且有节点缓存时，假定 CachedNodeNameToInfo 可用，按节点名取出缓存快照。
	nodeInfoCopy := f.CachedNodeNameToInfo[node.Node().Name].SnapshotConcrete()

	var potentialVictims []*v1.Pod

	removePod := func(rp *v1.Pod) error {
		return nodeInfoCopy.RemovePod(logger, rp)
	}
	addPod := func(ap *v1.Pod) {
		nodeInfoCopy.AddPod(ap)
	}
	// 第一步：移除节点上所有低优先级 Pod，再看目标 Pod 能否放下。
	podPriority := corev1helpers.PodPriority(pod)
	for _, p := range nodeInfoCopy.GetPods() {
		if corev1helpers.PodPriority(p.GetPod()) < podPriority {
			potentialVictims = append(potentialVictims, p.GetPod())
			if err := removePod(p.GetPod()); err != nil {
				return nil, 0, false, err
			}
		}
	}
	sort.Slice(potentialVictims, func(i, j int) bool { return util.MoreImportantPod(potentialVictims[i], potentialVictims[j]) })

	// 若移走所有低优先级 Pod 后仍放不下，该节点不适合抢占，直接判失败。
	status := f.runPredicate(pod, nodeInfoCopy)
	if status.IsSuccess() {
		// 通过过滤。
	} else if status.IsRejected() {
		// 仍放不下。
		return nil, 0, false, nil
	} else {
		// 内部错误。
		return nil, 0, false, status.AsError()
	}

	var victims []*v1.Pod

	// TODO(harry): 后续处理 PDB；目前测试实现暂不处理 PDB。
	numViolatingVictim := 0

	reprievePod := func(p *v1.Pod) bool {
		addPod(p)
		status := f.runPredicate(pod, nodeInfoCopy)
		if !status.IsSuccess() {
			if err := removePod(p); err != nil {
				return false
			}
			victims = append(victims, p)
		}
		return status.IsSuccess()
	}

	// 当前假定所有潜在牺牲者都不违反 PDB，逐一尝试恢复以尽量少驱逐 Pod。
	for _, p := range potentialVictims {
		reprievePod(p)
	}

	return victims, numViolatingVictim, true, nil
}

// runPredicate 依次运行 extender 的过滤函数，返回 Pod 是否适合该节点。
func (f *FakeExtender) runPredicate(pod *v1.Pod, node fwk.NodeInfo) *fwk.Status {
	for _, predicate := range f.Predicates {
		status := predicate(pod, node)
		if !status.IsSuccess() {
			return status
		}
	}
	return fwk.NewStatus(fwk.Success)
}

// Filter 实现 extender 的过滤逻辑，按状态把节点分为通过、失败与不可恢复三类。
func (f *FakeExtender) Filter(pod *v1.Pod, nodes []fwk.NodeInfo) ([]fwk.NodeInfo, extenderv1.FailedNodesMap, extenderv1.FailedNodesMap, error) {
	var filtered []fwk.NodeInfo
	failedNodesMap := extenderv1.FailedNodesMap{}
	failedAndUnresolvableMap := extenderv1.FailedNodesMap{}
	for _, node := range nodes {
		status := f.runPredicate(pod, node)
		if status.IsSuccess() {
			filtered = append(filtered, node)
		} else if status.Code() == fwk.Unschedulable {
			failedNodesMap[node.Node().Name] = fmt.Sprintf("FakeExtender: node %q failed", node.Node().Name)
		} else if status.Code() == fwk.UnschedulableAndUnresolvable {
			failedAndUnresolvableMap[node.Node().Name] = fmt.Sprintf("FakeExtender: node %q failed and unresolvable", node.Node().Name)
		} else {
			return nil, nil, nil, status.AsError()
		}
	}

	// 记录过滤结果，供后续 Bind 校验目标节点是否在其中。
	f.FilteredNodes = filtered
	if f.NodeCacheCapable {
		return filtered, failedNodesMap, failedAndUnresolvableMap, nil
	}
	return filtered, failedNodesMap, failedAndUnresolvableMap, nil
}

// Prioritize 实现 extender 的打分逻辑，按各打分器的权重累加节点分数。
func (f *FakeExtender) Prioritize(pod *v1.Pod, nodes []fwk.NodeInfo) (*extenderv1.HostPriorityList, int64, error) {
	result := extenderv1.HostPriorityList{}
	combinedScores := map[string]int64{}
	for _, prioritizer := range f.Prioritizers {
		weight := prioritizer.Weight
		// 权重为 0 的打分器对总分无贡献，跳过。
		if weight == 0 {
			continue
		}
		priorityFunc := prioritizer.Function
		prioritizedList, err := priorityFunc(pod, nodes)
		if err != nil {
			return &extenderv1.HostPriorityList{}, 0, err
		}
		for _, hostEntry := range *prioritizedList {
			combinedScores[hostEntry.Name] += hostEntry.Score * weight
		}
	}
	for host, score := range combinedScores {
		result = append(result, extenderv1.HostPriority{Host: host, Score: score})
	}
	return &result, f.Weight, nil
}

// Bind 实现 extender 的绑定逻辑；若记录了过滤结果，则校验目标节点是否在其中。
func (f *FakeExtender) Bind(binding *v1.Binding) error {
	if f.Binder != nil {
		return f.Binder()
	}
	if len(f.FilteredNodes) != 0 {
		for _, node := range f.FilteredNodes {
			if node.Node().Name == binding.Target.Name {
				f.FilteredNodes = nil
				return nil
			}
		}
		err := fmt.Errorf("Node %v not in filtered nodes %v", binding.Target.Name, f.FilteredNodes)
		f.FilteredNodes = nil
		return err
	}
	return nil
}

// IsBinder 返回 true，表示此 extender 实现了绑定功能。
func (f *FakeExtender) IsBinder() bool {
	return true
}

// IsPrioritizer 返回是否配置了打分器。
func (f *FakeExtender) IsPrioritizer() bool {
	return len(f.Prioritizers) > 0
}

// IsFilter 返回是否配置了过滤函数。
func (f *FakeExtender) IsFilter() bool {
	return len(f.Predicates) > 0
}

// IsInterested 返回此 extender 是否对该 Pod 感兴趣。
func (f *FakeExtender) IsInterested(pod *v1.Pod) bool {
	return !f.UnInterested
}

var _ fwk.Extender = &FakeExtender{}
