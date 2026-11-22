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

package interpodaffinity

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Name 是插件在插件注册表和配置中使用的名字。
const Name = names.InterPodAffinity

var _ fwk.PreFilterPlugin = &InterPodAffinity{}
var _ fwk.FilterPlugin = &InterPodAffinity{}
var _ fwk.PreScorePlugin = &InterPodAffinity{}
var _ fwk.ScorePlugin = &InterPodAffinity{}
var _ fwk.EnqueueExtensions = &InterPodAffinity{}
var _ fwk.SignPlugin = &InterPodAffinity{}

// InterPodAffinity 是检查 Pod 间亲和性/反亲和性的插件。
type InterPodAffinity struct {
	parallelizer                                       fwk.Parallelizer
	args                                               config.InterPodAffinityArgs
	sharedLister                                       fwk.SharedLister
	nsLister                                           listersv1.NamespaceLister
	enableInPlacePodVerticalScalingSchedulerPreemption bool
	enableInterPodAffinityHostnameFastPath             bool
}

// Name 返回插件名称，用于日志等场景。
func (pl *InterPodAffinity) Name() string {
	return Name
}

// SignPod 为 Pod 生成签名片段。
// Pod 间亲和性使可行性和打分不仅取决于当前 Pod 与节点，还取决于其他 Pod 的放置，
// 因此带有这些约束的 Pod 无法被签名。
func (pl *InterPodAffinity) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	if pod.Spec.Affinity != nil && (pod.Spec.Affinity.PodAffinity != nil || pod.Spec.Affinity.PodAntiAffinity != nil) {
		return nil, fwk.NewStatus(fwk.Unschedulable, "pods with InterPodAffinity are not signable")
	}

	// 开启该选项时，只考虑配置了亲和性的 Pod 之间的亲和关系，
	// 因此未设置规则的 Pod 的标签可以忽略。
	// 否则必须带上该 Pod 的标签，才能捕获它与那些设置了亲和规则的 Pod 之间的亲和关系。
	if pl.args.IgnorePreferredTermsOfExistingPods {
		return nil, nil
	}

	return []fwk.SignFragment{
		{Key: fwk.LabelsSignerName, Value: pod.Labels},
	}, nil
}

// EventsToRegister 返回可能让本插件判定为不可调度的 Pod 变为可调度的事件。
func (pl *InterPodAffinity) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	return []fwk.ClusterEventWithHint{
		// 这些 ActionType 包含以下事件：
		// - Delete：不可调度的 Pod 可能因违反某个已有 Pod 的反亲和约束而失败，删除该 Pod 可能使其可调度。
		// - UpdatePodLabel：修改已有 Pod 的标签（例如移除）可能使不可调度的 Pod 变为可调度。
		// - Add：不可调度的 Pod 可能因违反 Pod 亲和约束而失败，新增一个已分配的 Pod 可能使其可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Add | fwk.UpdatePodLabel | fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterAssignedPodChange},
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodLabel}, QueueingHintFn: pl.isSchedulableAfterTargetPodUpdateLabel},
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel}, QueueingHintFn: pl.isSchedulableAfterNodeChange},
	}, nil
}

// New 初始化并返回一个新的插件实例。
func New(_ context.Context, plArgs runtime.Object, h fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	if h.SnapshotSharedLister() == nil {
		return nil, fmt.Errorf("SnapshotSharedlister is nil")
	}
	args, err := getArgs(plArgs)
	if err != nil {
		return nil, err
	}
	if err := validation.ValidateInterPodAffinityArgs(nil, &args); err != nil {
		return nil, err
	}
	pl := &InterPodAffinity{
		parallelizer: h.Parallelizer(),
		args:         args,
		sharedLister: h.SnapshotSharedLister(),
		nsLister:     h.SharedInformerFactory().Core().V1().Namespaces().Lister(),
		enableInPlacePodVerticalScalingSchedulerPreemption: fts.EnableInPlacePodVerticalScalingSchedulerPreemption,
		enableInterPodAffinityHostnameFastPath:             fts.EnableInterPodAffinityHostnameFastPath,
	}

	return pl, nil
}

// getArgs 从 runtime.Object 中取出 InterPodAffinityArgs，类型不符时返回错误。
func getArgs(obj runtime.Object) (config.InterPodAffinityArgs, error) {
	ptr, ok := obj.(*config.InterPodAffinityArgs)
	if !ok {
		return config.InterPodAffinityArgs{}, fmt.Errorf("want args to be of type InterPodAffinityArgs, got %T", obj)
	}
	return *ptr, nil
}

// mergeAffinityTermNamespacesIfNotEmpty 用 NamespaceSelector 选出的命名空间集合更新 Namespaces，
// 成功后将 NamespaceSelector 置空。
// 这里假设该 term 属于待调度的 Pod：此时 namespaceSelector 要么被展开进 Namespaces
// （选择器设为 Nothing()），要么是 Empty()（匹配所有）。因此在匹配该 term 时，
// 无需再查找已有 Pod 所在命名空间的标签来显式匹配 term 的 namespaceSelector。
func (pl *InterPodAffinity) mergeAffinityTermNamespacesIfNotEmpty(at *fwk.AffinityTerm) error {
	if at.NamespaceSelector.Empty() {
		return nil
	}
	ns, err := pl.nsLister.List(at.NamespaceSelector)
	if err != nil {
		return err
	}
	for _, n := range ns {
		at.Namespaces.Insert(n.Name)
	}
	at.NamespaceSelector = labels.Nothing()
	return nil
}

// GetNamespaceLabelsSnapshot 返回命名空间关联标签的快照。
func GetNamespaceLabelsSnapshot(logger klog.Logger, ns string, nsLister listersv1.NamespaceLister) (nsLabels labels.Set) {
	podNS, err := nsLister.Get(ns)
	if err == nil {
		// 复制一份标签快照，避免后续读到 lister 中的变化。
		return labels.Merge(podNS.Labels, nil)
	}
	logger.V(3).Info("getting namespace, assuming empty set of namespace labels", "namespace", ns, "err", err)
	return
}

// isSchedulableAfterAssignedPodChange 在已分配 Pod 发生变化后，判断目标 Pod 是否可能变为可调度。
func (pl *InterPodAffinity) isSchedulableAfterAssignedPodChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalPod, modifiedPod, err := util.As[*v1.Pod](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}
	// Pod 的 NodeName 或 NominatedNodeName 变化会触发 Add/Delete 事件，无需在此处理。
	if (modifiedPod != nil && modifiedPod.Spec.NodeName == "" && modifiedPod.Status.NominatedNodeName == "") ||
		(originalPod != nil && originalPod.Spec.NodeName == "" && originalPod.Status.NominatedNodeName == "") {
		logger.V(5).Info("the added/updated/deleted pod is unscheduled, so it doesn't make the target pod schedulable",
			"pod", klog.KObj(pod), "originalPod", klog.KObj(originalPod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}

	terms, err := fwk.GetAffinityTerms(pod, fwk.GetPodAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		return fwk.Queue, err
	}

	antiTerms, err := fwk.GetAffinityTerms(pod, fwk.GetPodAntiAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		return fwk.Queue, err
	}

	// Pod 被更新：当更新后的 Pod 匹配目标 Pod 的亲和性、或不再匹配反亲和性时入队。
	// 注意，即使 Pod 有多个亲和项也无需逐项检查，
	// 因为当前的 PodAffinity 要求**单个**已有 Pod 就能满足待调度 Pod 的**全部**亲和项。
	if modifiedPod != nil && originalPod != nil {
		if !podMatchesAllAffinityTerms(terms, originalPod) && podMatchesAllAffinityTerms(terms, modifiedPod) {
			logger.V(5).Info("a scheduled pod was updated to match the target pod's affinity, and the pod may be schedulable now",
				"pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
			return fwk.Queue, nil
		}
		if podMatchesAnyAffinityTerms(antiTerms, originalPod) && !podMatchesAnyAffinityTerms(antiTerms, modifiedPod) {
			logger.V(5).Info("a scheduled pod was updated not to match the target pod's anti affinity, and the pod may be schedulable now",
				"pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
			return fwk.Queue, nil
		}
		logger.V(5).Info("a scheduled pod was updated but it doesn't match the target pod's affinity or does match the target pod's anti-affinity",
			"pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}

	// Pod 被添加：当新增的 Pod 匹配目标 Pod 的亲和性时入队。
	if modifiedPod != nil {
		if podMatchesAllAffinityTerms(terms, modifiedPod) {
			logger.V(5).Info("a scheduled pod was added and it matches the target pod's affinity",
				"pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
			return fwk.Queue, nil
		}
		logger.V(5).Info("a scheduled pod was added and it doesn't match the target pod's affinity",
			"pod", klog.KObj(pod), "modifiedPod", klog.KObj(modifiedPod))
		return fwk.QueueSkip, nil
	}

	// Pod 被删除：当被删除的 Pod 匹配目标 Pod 的反亲和性（或反之）时入队。

	if podMatchesAnyAffinityTerms(antiTerms, originalPod) {
		logger.V(5).Info("a scheduled pod was deleted and it matches the target pod's anti-affinity. The pod may be schedulable now",
			"pod", klog.KObj(pod), "originalPod", klog.KObj(originalPod))
		return fwk.Queue, nil
	}

	originalPodAntiTerms, err := fwk.GetAffinityTerms(originalPod, fwk.GetPodAntiAffinityTerms(originalPod.Spec.Affinity))
	if err != nil {
		return fwk.Queue, err
	}
	if podMatchesAnyAffinityTerms(originalPodAntiTerms, pod) {
		logger.V(5).Info("a scheduled pod was deleted and the target pod matches the deleted pod's anti-affinity. The pod may be schedulable now",
			"pod", klog.KObj(pod), "originalPod", klog.KObj(originalPod))
		return fwk.Queue, nil
	}

	logger.V(5).Info("a scheduled pod was deleted but it doesn't match the target pod's anti-affinity, nor vice versa",
		"pod", klog.KObj(pod), "originalPod", klog.KObj(originalPod))
	return fwk.QueueSkip, nil
}

// isSchedulableAfterTargetPodUpdateLabel 在目标 Pod 标签变化后返回入队提示。
func (pl *InterPodAffinity) isSchedulableAfterTargetPodUpdateLabel(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	// 标签更新后 Pod 可能变为可调度：例如其他 Pod 可能对旧标签有反亲和性，但对新标签没有。
	logger.V(5).Info("the target pod labels have changed and it may be schedulable now",
		"pod", klog.KObj(pod))
	return fwk.Queue, nil
}

// isSchedulableAfterNodeChange 在节点发生变化后，判断目标 Pod 是否可能变为可调度。
func (pl *InterPodAffinity) isSchedulableAfterNodeChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	originalNode, modifiedNode, err := util.As[*v1.Node](oldObj, newObj)
	if err != nil {
		return fwk.Queue, err
	}

	terms, err := fwk.GetAffinityTerms(pod, fwk.GetPodAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		return fwk.Queue, err
	}

	// 以下情况需要将该 Pod 入队：
	// - 1. 新增的节点带有 Pod 亲和性的 topologyKey，Pod 可能变为可调度。
	// - 2. 原节点没有 Pod 亲和性的 topologyKey，而更新后的节点有，Pod 可能变为可调度。
	// - 3. 原节点与更新后的节点都有 Pod 亲和性的 topologyKey 且值不同，Pod 可能变为可调度。
	for _, term := range terms {
		if originalNode == nil {
			if _, ok := modifiedNode.Labels[term.TopologyKey]; ok {
				// 情况 1：新增的节点带有 Pod 亲和性的 topologyKey。
				logger.V(5).Info("A node with a matched pod affinity topologyKey was added and it may make the pod schedulable",
					"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
				return fwk.Queue, nil
			}
			continue
		}
		originalTopologyValue, originalHasKey := originalNode.Labels[term.TopologyKey]
		modifiedTopologyValue, modifiedHasKey := modifiedNode.Labels[term.TopologyKey]

		if !originalHasKey && modifiedHasKey {
			// 情况 2：原节点没有 Pod 亲和性的 topologyKey，而更新后的节点有。
			logger.V(5).Info("A node got updated to have the topology key of pod affinity, which may make the pod schedulable",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.Queue, nil
		}

		if originalHasKey && modifiedHasKey && (originalTopologyValue != modifiedTopologyValue) {
			// 情况 3：两个节点都有 Pod 亲和性的 topologyKey，但值不同。
			logger.V(5).Info("A node is moved to a different domain of pod affinity, which may make the pod schedulable",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.Queue, nil
		}
	}

	antiTerms, err := fwk.GetAffinityTerms(pod, fwk.GetPodAntiAffinityTerms(pod.Spec.Affinity))
	if err != nil {
		return fwk.Queue, err
	}

	// 以下情况需要将该 Pod 入队：
	// - 1. 新增了节点，Pod 可能变为可调度。
	// - 2. 原节点有 Pod 反亲和性的 topologyKey，而更新后的节点没有，Pod 可能变为可调度。
	// - 3. 原节点与更新后的节点都有 Pod 反亲和性的 topologyKey 且值不同，Pod 可能变为可调度。
	for _, term := range antiTerms {
		if originalNode == nil {
			// 情况 1：新增了节点。
			// 带反亲和性的 Pod 一律重新入队，因为：
			// - 没有 topologyKey 的节点总是允许放置带反亲和性的 Pod。
			// - 带 topologyKey 的新节点只有在它所加入的拓扑域中没有该 Pod 排斥的 Pod 时才使其可调度。
			//   而检查该节点所在拓扑域中有哪些 Pod 超出了本 QueueingHint 的职责范围。
			logger.V(5).Info("A node was added and it may make the pod schedulable",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.Queue, nil
		}

		originalTopologyValue, originalHasKey := originalNode.Labels[term.TopologyKey]
		modifiedTopologyValue, modifiedHasKey := modifiedNode.Labels[term.TopologyKey]

		if originalHasKey && !modifiedHasKey {
			// 情况 2：原节点有 Pod 反亲和性的 topologyKey，而更新后的节点没有。
			// 注意无需检查相反情况（!originalHasKey && modifiedHasKey），
			// 因为没有该拓扑标签的节点总能接受带 Pod 反亲和性的 Pod。
			logger.V(5).Info("A node got updated to not have the topology key of pod anti-affinity, which may make the pod schedulable",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.Queue, nil
		}

		if originalHasKey && modifiedHasKey && (originalTopologyValue != modifiedTopologyValue) {
			// 情况 3：两个节点都有 Pod 反亲和性的 topologyKey，但值不同。
			logger.V(5).Info("A node is moved to a different domain of pod anti-affinity, which may make the pod schedulable",
				"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
			return fwk.Queue, nil
		}
	}
	logger.V(5).Info("a node is added/updated but doesn't have any topologyKey which matches pod affinity/anti-affinity",
		"pod", klog.KObj(pod), "node", klog.KObj(modifiedNode))
	return fwk.QueueSkip, nil
}
