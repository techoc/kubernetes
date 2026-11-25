/*
Copyright 2017 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"strconv"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// GetTemplateGeneration 从已废弃的注解中提取 v1.DaemonSet 关联的模板版本号。
// 注解不存在时返回 nil；注解存在但解析失败时返回 nil 及错误；
// 解析成功则返回指向该 int64 值的指针。
func GetTemplateGeneration(ds *apps.DaemonSet) (*int64, error) {
	annotation, found := ds.Annotations[apps.DeprecatedTemplateGeneration]
	if !found {
		return nil, nil
	}
	generation, err := strconv.ParseInt(annotation, 10, 64)
	if err != nil {
		return nil, err
	}
	return &generation, nil
}

// AddOrUpdateDaemonPodTolerations 为 DaemonSet 的 Pod 添加或更新必要的容忍规则，
// 例如 node.kubernetes.io/not-ready:NoExecute。
func AddOrUpdateDaemonPodTolerations(spec *v1.PodSpec) {
	// 无限期容忍 not-ready 污点，避免节点变为 NotReady 时
	// DaemonSet 的 Pod 被 NodeController 基于污点的驱逐机制删除。
	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodeNotReady,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoExecute,
	})

	// 同理，无限期容忍 unreachable 污点，应对节点变为不可达的情况。
	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodeUnreachable,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoExecute,
	})

	// TaintNodesByCondition 特性会根据节点状况自动打污点，
	// 所有 DaemonSet Pod 都必须容忍这些 NoSchedule 污点，否则无法在受影响节点上运行。
	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodeDiskPressure,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodeMemoryPressure,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodePIDPressure,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
		Key:      v1.TaintNodeUnschedulable,
		Operator: v1.TolerationOpExists,
		Effect:   v1.TaintEffectNoSchedule,
	})

	if spec.HostNetwork {
		v1helper.AddOrUpdateTolerationInPodSpec(spec, &v1.Toleration{
			Key:      v1.TaintNodeNetworkUnavailable,
			Operator: v1.TolerationOpExists,
			Effect:   v1.TaintEffectNoSchedule,
		})
	}
}

// CreatePodTemplate 返回给定模板的副本，并添加模板版本号标签（向后兼容）、
// 模板哈希标签，以及 DaemonSet 默认的容忍规则。
func CreatePodTemplate(template v1.PodTemplateSpec, generation *int64, hash string) v1.PodTemplateSpec {
	// 深拷贝后再修改，避免改动调用方传入的原始模板。
	newTemplate := *template.DeepCopy()

	AddOrUpdateDaemonPodTolerations(&newTemplate.Spec)

	if newTemplate.ObjectMeta.Labels == nil {
		newTemplate.ObjectMeta.Labels = make(map[string]string)
	}
	// 保留基于版本号的标签以兼容旧版本的更新判定逻辑。
	if generation != nil {
		newTemplate.ObjectMeta.Labels[extensions.DaemonSetTemplateGenerationKey] = fmt.Sprint(*generation)
	}
	// TODO: 是否需要校验 DaemonSet 是否采用 RollingUpdate 策略？
	if len(hash) > 0 {
		newTemplate.ObjectMeta.Labels[extensions.DefaultDaemonSetUniqueLabelKey] = hash
	}
	return newTemplate
}

// AllowsSurge 判断 DaemonSet 是否允许在单个节点上运行多个 Pod（即滚动更新时超量创建）。
func AllowsSurge(ds *apps.DaemonSet) bool {
	// 单个 Pod 场景下计算出的 maxSurge 大于 0，即表示允许 surge。
	maxSurge, err := SurgeCount(ds, 1)
	return err == nil && maxSurge > 0
}

// SurgeCount 返回滚动更新允许超量创建的 Pod 数：未配置 surge 时返回 0；
// 已配置时按 numberToSchedule 计算允许的数量；百分比非法时返回错误。
func SurgeCount(ds *apps.DaemonSet, numberToSchedule int) (int, error) {
	if ds.Spec.UpdateStrategy.Type != apps.RollingUpdateDaemonSetStrategyType {
		return 0, nil
	}

	r := ds.Spec.UpdateStrategy.RollingUpdate
	if r == nil {
		return 0, nil
	}
	// 未配置 maxSurge 时默认不允许超量创建，即 surge 数量为 0。
	if r.MaxSurge == nil {
		return 0, nil
	}
	return intstrutil.GetScaledValueFromIntOrPercent(r.MaxSurge, numberToSchedule, true)
}

// UnavailableCount 返回滚动更新允许不可用的 Pod 数：未配置时返回 0；
// 已配置时按 numberToSchedule 计算允许的数量；百分比非法时返回错误。
func UnavailableCount(ds *apps.DaemonSet, numberToSchedule int) (int, error) {
	if ds.Spec.UpdateStrategy.Type != apps.RollingUpdateDaemonSetStrategyType {
		return 0, nil
	}
	r := ds.Spec.UpdateStrategy.RollingUpdate
	if r == nil {
		return 0, nil
	}
	return intstrutil.GetScaledValueFromIntOrPercent(r.MaxUnavailable, numberToSchedule, true)
}

// IsPodUpdated 判断 Pod 的标签是否匹配模板版本号或模板哈希，二者任一匹配即视为已更新。
func IsPodUpdated(pod *v1.Pod, hash string, dsTemplateGeneration *int64) bool {
	// 同时支持按版本号和按哈希判断，是为了兼容旧版本 DaemonSet 创建的 Pod。
	templateMatches := dsTemplateGeneration != nil &&
		pod.Labels[extensions.DaemonSetTemplateGenerationKey] == fmt.Sprint(*dsTemplateGeneration)
	hashMatches := len(hash) > 0 && pod.Labels[extensions.DefaultDaemonSetUniqueLabelKey] == hash
	return hashMatches || templateMatches
}

// ReplaceDaemonSetPodNodeNameNodeAffinity 用选中给定 nodeName 的新 NodeAffinity
// 替换给定 affinity 中的 RequiredDuringSchedulingIgnoredDuringExecution 字段。
// 注意：本函数假定原有 NodeAffinity 与所选 nodeName 不存在冲突。
func ReplaceDaemonSetPodNodeNameNodeAffinity(affinity *v1.Affinity, nodename string) *v1.Affinity {
	// 通过 matchFields 按对象名（metadata.name）精确选中目标节点，
	// 这是调度器将 DaemonSet Pod 固定到指定节点的依据。
	nodeSelReq := v1.NodeSelectorRequirement{
		Key:      metav1.ObjectNameField,
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{nodename},
	}

	nodeSelector := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchFields: []v1.NodeSelectorRequirement{nodeSelReq},
			},
		},
	}

	if affinity == nil {
		return &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: nodeSelector,
			},
		}
	}

	if affinity.NodeAffinity == nil {
		affinity.NodeAffinity = &v1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: nodeSelector,
		}
		return affinity
	}

	nodeAffinity := affinity.NodeAffinity

	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nodeSelector
		return affinity
	}

	// 直接整体替换节点选择器条款，确保旧的其他选择条件不会把目标节点排除在外。
	nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = []v1.NodeSelectorTerm{
		{
			MatchFields: []v1.NodeSelectorRequirement{nodeSelReq},
		},
	}

	return affinity
}

// GetTargetNodeName 获取 DaemonSet Pod 的目标节点名称：若 .spec.NodeName 非空则直接返回；
// 否则从 NodeAffinity 中提取待调度 Pod 的目标节点；两者都无法获取时返回错误。
func GetTargetNodeName(pod *v1.Pod) (string, error) {
	// 已调度的 Pod 其 NodeName 由调度器填写，是最直接的目标节点来源。
	if len(pod.Spec.NodeName) != 0 {
		return pod.Spec.NodeName, nil
	}

	// 未调度的 Pod 目标节点记录在 NodeAffinity 中，由控制器预先写入。
	if pod.Spec.Affinity == nil ||
		pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return "", fmt.Errorf("no spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution for pod %s/%s",
			pod.Namespace, pod.Name)
	}

	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) < 1 {
		return "", fmt.Errorf("no nodeSelectorTerms in requiredDuringSchedulingIgnoredDuringExecution of pod %s/%s",
			pod.Namespace, pod.Name)
	}

	for _, term := range terms {
		for _, exp := range term.MatchFields {
			if exp.Key == metav1.ObjectNameField &&
				exp.Operator == v1.NodeSelectorOpIn {
				if len(exp.Values) != 1 {
					return "", fmt.Errorf("the matchFields value of '%s' is not unique for pod %s/%s",
						metav1.ObjectNameField, pod.Namespace, pod.Name)
				}

				return exp.Values[0], nil
			}
		}
	}

	return "", fmt.Errorf("no node name found for pod %s/%s", pod.Namespace, pod.Name)
}
