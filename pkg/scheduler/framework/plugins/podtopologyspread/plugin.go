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

package podtopologyspread

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

const (
	// ErrReasonConstraintsNotMatch 是拓扑分布约束不满足时的过滤失败原因。
	ErrReasonConstraintsNotMatch = "node(s) didn't match pod topology spread constraints"
	// ErrReasonNodeLabelNotMatch 用于节点缺少约束所需标签的场景。
	ErrReasonNodeLabelNotMatch = ErrReasonConstraintsNotMatch + " (missing required label)"
)

// systemDefaultConstraints 是未配置默认约束时使用的系统默认值：
// 主机名维度最大倾斜 3，可用区维度最大倾斜 5，均为软约束。
var systemDefaultConstraints = []v1.TopologySpreadConstraint{
	{
		TopologyKey:       v1.LabelHostname,
		WhenUnsatisfiable: v1.ScheduleAnyway,
		MaxSkew:           3,
	},
	{
		TopologyKey:       v1.LabelTopologyZone,
		WhenUnsatisfiable: v1.ScheduleAnyway,
		MaxSkew:           5,
	},
}

// Package podtopologyspread 提供 Pod 拓扑分布约束插件，
// 在过滤和打分阶段保证 Pod 按拓扑域均匀分布。

// PodTopologySpread 确保 Pod 的 topologySpreadConstraints 得到满足。
type PodTopologySpread struct {
	systemDefaulted                                    bool
	parallelizer                                       fwk.Parallelizer // 并行执行器，供打分阶段并行处理节点
	defaultConstraints                                 []v1.TopologySpreadConstraint
	sharedLister                                       fwk.SharedLister // 快照共享 lister，提供节点与 Pod 信息
	services                                           corelisters.ServiceLister
	replicationCtrls                                   corelisters.ReplicationControllerLister
	replicaSets                                        appslisters.ReplicaSetLister
	statefulSets                                       appslisters.StatefulSetLister
	enableNodeInclusionPolicyInPodTopologySpread       bool // 是否启用节点纳入策略（NodeTaintsPolicy）
	enableMatchLabelKeysInPodTopologySpread            bool // 是否支持 matchLabelKeys 按标签选取拓扑域
	enableTaintTolerationComparisonOperators           bool // 是否支持污点容忍比较运算符
	enableInPlacePodVerticalScalingSchedulerPreemption bool // 原地垂直扩缩容场景是否参与抢占判断
}

var _ fwk.PreFilterPlugin = &PodTopologySpread{}
var _ fwk.FilterPlugin = &PodTopologySpread{}
var _ fwk.PreScorePlugin = &PodTopologySpread{}
var _ fwk.ScorePlugin = &PodTopologySpread{}
var _ fwk.EnqueueExtensions = &PodTopologySpread{}
var _ fwk.SignPlugin = &PodTopologySpread{}

// Name 是插件在注册表和配置中使用的名称。
const Name = names.PodTopologySpread

// Name 返回插件名称，用于日志等场景。
func (pl *PodTopologySpread) Name() string {
	return Name
}

// 拓扑分布不是 Pod 与节点间的局部关系，无法用签名缓存判定；
// 无论是显式配置还是默认约束，含拓扑约束的 Pod 都不支持签名。
func (pl *PodTopologySpread) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	if len(pod.Spec.TopologySpreadConstraints) > 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "pods with topology constraints are not signable")
	}

	if len(pl.defaultConstraints) > 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "pods with default topology constraints are not signable")
	}

	return nil, nil
}

// New 初始化并返回一个 PodTopologySpread 插件实例。
func New(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	if h.SnapshotSharedLister() == nil {
		return nil, fmt.Errorf("SnapshotSharedlister is nil")
	}
	args, err := getArgs(plArgs)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidatePodTopologySpreadArgs(nil, &args); err != nil {
		return nil, err
	}
	pl := &PodTopologySpread{
		parallelizer:       h.Parallelizer(),
		sharedLister:       h.SnapshotSharedLister(),
		defaultConstraints: args.DefaultConstraints,
		enableNodeInclusionPolicyInPodTopologySpread:       fts.EnableNodeInclusionPolicyInPodTopologySpread,
		enableMatchLabelKeysInPodTopologySpread:            fts.EnableMatchLabelKeysInPodTopologySpread,
		enableTaintTolerationComparisonOperators:           fts.EnableTaintTolerationComparisonOperators,
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
	}
	if args.DefaultingType == config.SystemDefaulting {
		pl.defaultConstraints = systemDefaultConstraints
		pl.systemDefaulted = true
	}
	if len(pl.defaultConstraints) != 0 {
		if h.SharedInformerFactory() == nil {
			return nil, fmt.Errorf("SharedInformerFactory is nil")
		}
		pl.setListers(h.SharedInformerFactory())
	}
	return pl, nil
}

// getArgs 把运行时配置对象断言为 PodTopologySpreadArgs 具体类型。
func getArgs(obj runtime.Object) (config.PodTopologySpreadArgs, error) {
	ptr, ok := obj.(*config.PodTopologySpreadArgs)
	if !ok {
		return config.PodTopologySpreadArgs{}, fmt.Errorf("want args to be of type PodTopologySpreadArgs, got %T", obj)
	}
	return *ptr, nil
}

// setListers 从 informer factory 初始化计算默认约束所需的各类 lister。
func (pl *PodTopologySpread) setListers(factory informers.SharedInformerFactory) {
	pl.services = factory.Core().V1().Services().Lister()
	pl.replicationCtrls = factory.Core().V1().ReplicationControllers().Lister()
	pl.replicaSets = factory.Apps().V1().ReplicaSets().Lister()
	pl.statefulSets = factory.Apps().V1().StatefulSets().Lister()
}

// EventsToRegister 返回可能让被本插件判为不可调度的 Pod 变为可调度的集群事件。
func (pl *PodTopologySpread) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// AssignedPod 包含以下事件：
		//   - Add：新增已分配 Pod 可能改变分布计数，使原不可调度 Pod 可调度
		//   - UpdatePodLabel：Pod 标签变化可能改变其是否匹配约束选择器
		//   - Delete：删除已分配 Pod 可能解除分布约束冲突
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add | fwk.UpdatePodLabel | fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodChange},
		// TargetPod 包含以下事件：
		//   - UpdatePodLabel：标签变化可能改变约束匹配结果
		//   - UpdatePodToleration：当约束设置 NodeTaintsPolicy:Honor 时，
		//     新增 toleration 可能让之前被拒绝的 Pod 变得可调度
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodToleration | fwk.UpdatePodLabel}, QueueingHintFn: pl.isSchedulableAfterTargetPodChange},
		// 节点的增删或标签/污点更新可能改变拓扑键归属，进而影响 Pod 的可调度性。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.Delete | fwk.UpdateNodeLabel | fwk.UpdateNodeTaint}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
	}, nil
}

// involvedInTopologySpreading 判断 incomingPod 是否参与 podWithSpreading 的拓扑分布计算：
// 只有已分配（或已提名）且同命名空间的 Pod 才会计入分布。
func involvedInTopologySpreading(incomingPod, podWithSpreading *v1.Pod) bool {
	if incomingPod.Spec.NodeName == "" && incomingPod.Status.NominatedNodeName == "" {
		return false
	}
	return incomingPod.Namespace == podWithSpreading.Namespace
}

// hasConstraintWithNodeTaintsPolicyHonor 在任一约束设置了 NodeTaintsPolicy: Honor 时返回 true。
func hasConstraintWithNodeTaintsPolicyHonor(constraints []topologySpreadConstraint) bool {
	for _, c := range constraints {
		if c.NodeTaintsPolicy == v1.NodeInclusionPolicyHonor {
			return true
		}
	}
	return false
}

// isSchedulableAfterTargetPodChange 在目标 Pod 自身变更时判断是否需要重新入队。
func (pl *PodTopologySpread) isSchedulableAfterTargetPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalPod, modifiedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	constraints, err := pl.getConstraints(pod)
	if err != nil {
		return fwk.Queue, err
	}

	if !equality.Semantic.DeepEqual(modifiedPod.Spec.Tolerations, originalPod.Spec.Tolerations) && hasConstraintWithNodeTaintsPolicyHonor(constraints) {
		// 存在 NodeTaintsPolicy: Honor 约束时，新增 toleration 可能让 Pod 落到之前被污点排除的节点。
		logger.V(5).Info("the unschedulable pod has got a new toleration, which could make it schedulable",
			"pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.Queue, nil
	}

	return pl.isSchedulableAfterPodLabelChange(logger, originalPod, modifiedPod, constraints)
}

// isSchedulableAfterAssignedPodChange 在已分配 Pod 增删或标签更新时判断是否需要重新入队。
func (pl *PodTopologySpread) isSchedulableAfterAssignedPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalPod, modifiedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	if (modifiedPod != nil && !involvedInTopologySpreading(modifiedPod, pod)) || (originalPod != nil && !involvedInTopologySpreading(originalPod, pod)) {
		logger.V(5).Info("the added/updated/deleted pod is unscheduled or has different namespace with target pod, so it doesn't make the target pod schedulable",
			"pod", klog.KObj(pod), "originalPod", klog.KObj(originalPod))
		return fwk.QueueSkip, nil
	}

	constraints, err := pl.getConstraints(pod)
	if err != nil {
		return fwk.Queue, err
	}

	// Pod 被修改：只有影响约束选择器匹配结果的标签变化才需要入队。
	if modifiedPod != nil && originalPod != nil {
		return pl.isSchedulableAfterPodLabelChange(logger, originalPod, modifiedPod, constraints)
	}

	// Pod 被新增：只有标签匹配约束选择器时才需要入队。
	if modifiedPod != nil {
		if podLabelsMatchSpreadConstraints(constraints, modifiedPod.Labels) {
			logger.V(5).Info("a scheduled pod was created and it matches with the pod's topology spread constraints",
				"pod", klog.KObj(pod), "createdPod", klog.KObj(modifiedPod))
			return fwk.Queue, nil
		}
		logger.V(5).Info("a scheduled pod was created, but it doesn't matches with the pod's topology spread constraints",
			"pod", klog.KObj(pod), "createdPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}

	// Pod 被删除：只有被删 Pod 标签匹配约束选择器时才需要入队。
	if podLabelsMatchSpreadConstraints(constraints, originalPod.Labels) {
		logger.V(5).Info("a scheduled pod which matches with the pod's topology spread constraints was deleted, and the pod may be schedulable now",
			"pod", klog.KObj(pod), "deletedPod", klog.KObj(originalPod))
		return fwk.Queue, nil
	}
	logger.V(5).Info("a scheduled pod was deleted, but it's unrelated to the pod's topology spread constraints",
		"pod", klog.KObj(pod), "deletedPod", klog.KObj(originalPod))

	return fwk.QueueSkip, nil
}

// isSchedulableAfterPodLabelChange 判断标签变化是否改变了 Pod 对任一约束选择器的匹配结果。
func (pl *PodTopologySpread) isSchedulableAfterPodLabelChange(logger klog.Logger, originalPod, modifiedPod *v1.Pod, constraints []topologySpreadConstraint) (fwk.QueueingHint, error) {
	if equality.Semantic.DeepEqual(modifiedPod.Labels, originalPod.Labels) {
		logger.V(5).Info("the pod's update doesn't include the label update, which doesn't make the target pod schedulable",
			"pod", klog.KObj(originalPod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}
	for _, c := range constraints {
		if c.Selector.Matches(labels.Set(originalPod.Labels)) != c.Selector.Matches(labels.Set(modifiedPod.Labels)) {
			// 标签变化使 Pod 从匹配变为不匹配（或反之），可能改变分布约束的调度结果。
			logger.V(5).Info("a scheduled or target pod's label was updated and it makes the updated pod match or unmatch the pod's topology spread constraints",
				"pod", klog.KObj(originalPod), "modifiedPod", klog.KObj(modifiedPod))
			return fwk.Queue, nil
		}
	}
	// 标签变化不影响任何约束的选择器匹配结果，无需入队。
	logger.V(5).Info("a scheduled or target pod's label was updated, but it's a change unrelated to the pod's topology spread constraints",
		"pod", klog.KObj(originalPod), "modifiedPod", klog.KObj(modifiedPod))
	return fwk.QueueSkip, nil
}

// getConstraints 从 Pod spec 提取硬约束（DoNotSchedule）；
// Pod 未显式配置时返回默认约束。
func (pl *PodTopologySpread) getConstraints(pod *v1.Pod) ([]topologySpreadConstraint, error) {
	var constraints []topologySpreadConstraint
	var err error
	if len(pod.Spec.TopologySpreadConstraints) > 0 {
		// APIServer 已按特性门控清理了 spec，这里无需重复检查门控，直接看约束数量即可。
		constraints, err = pl.filterTopologySpreadConstraints(
			pod.Spec.TopologySpreadConstraints,
			pod.Labels,
			v1.DoNotSchedule,
		)
		if err != nil {
			return nil, fmt.Errorf("obtaining pod's hard topology spread constraints: %w", err)
		}
	} else {
		constraints, err = pl.buildDefaultConstraints(pod, v1.DoNotSchedule)
		if err != nil {
			return nil, fmt.Errorf("setting default hard topology spread constraints: %w", err)
		}
	}
	return constraints, nil
}

// isSchedulableAfterNodeChange 在节点的标签或污点变化可能影响拓扑分布时返回 Queue。
func (pl *PodTopologySpread) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	constraints, err := pl.getConstraints(pod)
	if err != nil {
		return fwk.Queue, err
	}

	var originalNodeMatching, modifiedNodeMatching bool
	if originalNode != nil {
		originalNodeMatching = nodeLabelsMatchSpreadConstraints(originalNode.Labels, constraints)
	}
	if modifiedNode != nil {
		modifiedNodeMatching = nodeLabelsMatchSpreadConstraints(modifiedNode.Labels, constraints)
	}

	// 以下情况需要入队：
	//  1. 节点标签更新：变更前后对约束的匹配结果不同，或匹配时拓扑键值发生变化
	//  2. 节点污点更新：节点仍匹配约束但污点有变化（影响 NodeTaintsPolicy: Honor）
	//  3. 新增节点匹配约束
	//  4. 删除的节点原本匹配约束
	if originalNode != nil && modifiedNode != nil {
		if originalNodeMatching != modifiedNodeMatching {
			logger.V(5).Info("the node is updated and now pod topology spread constraints has changed, and the pod may be schedulable now",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode), "originalMatching", originalNodeMatching, "newMatching", modifiedNodeMatching)
			return fwk.Queue, nil
		}
		if modifiedNodeMatching && (checkTopologyKeyLabelsChanged(originalNode.Labels, modifiedNode.Labels, constraints) || !equality.Semantic.DeepEqual(originalNode.Spec.Taints, modifiedNode.Spec.Taints)) {
			logger.V(5).Info("the node is updated and now has different taints or labels, and the pod may be schedulable now",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.Queue, nil
		}
		return fwk.QueueSkip, nil
	}

	if modifiedNode != nil {
		if !modifiedNodeMatching {
			logger.V(5).Info("the created node doesn't match pod topology spread constraints",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.QueueSkip, nil
		}
		logger.V(5).Info("the created node matches topology spread constraints, and the pod may be schedulable now",
			"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
		return fwk.Queue, nil
	}

	if !originalNodeMatching {
		logger.V(5).Info("the deleted node doesn't match pod topology spread constraints", "pod", klog.KObj(pod), "node", klog.KObj(originalNode))
		return fwk.QueueSkip, nil
	}
	logger.V(5).Info("the deleted node matches topology spread constraints, and the pod may be schedulable now",
		"pod", klog.KObj(pod), "node", klog.KObj(originalNode))
	return fwk.Queue, nil
}

// checkTopologyKeyLabelsChanged 检查约束中任一 topologyKey 对应的标签值是否发生变化。
func checkTopologyKeyLabelsChanged(originalLabels, modifiedLabels map[string]string, constraints []topologySpreadConstraint) bool {
	for _, constraint := range constraints {
		topologyKey := constraint.TopologyKey
		if originalLabels[topologyKey] != modifiedLabels[topologyKey] {
			return true
		}
	}
	return false
}
