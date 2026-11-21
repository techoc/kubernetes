/*
Copyright 2015 The Kubernetes Authors.

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

package pod

import (
	"iter"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// ContainerType 表示容器类型，用位掩码表示，便于组合成可同时匹配的容器集合。
type ContainerType int

const (
	// Containers 表示普通容器。
	Containers ContainerType = 1 << iota
	// InitContainers 表示初始化容器。
	InitContainers
	// EphemeralContainers 表示临时容器。
	EphemeralContainers
)

// AllContainers 指定访问所有类型的容器。
const AllContainers ContainerType = InitContainers | Containers | EphemeralContainers

// AllFeatureEnabledContainers 返回包含所有不受特性开关保护的容器类型的 ContainerType 掩码。
// 当前所有容器类型均已默认启用，因此直接返回 AllContainers；保留函数是为了让调用方
// 不必关心哪些类型受开关保护。
func AllFeatureEnabledContainers() ContainerType {
	return AllContainers
}

// ContainerVisitor 在每个容器 spec 上被调用，返回值表示是否继续访问后续容器。
type ContainerVisitor func(container *v1.Container, containerType ContainerType) (shouldContinue bool)

// Visitor 在每个对象名称上被调用，返回值表示是否继续访问后续名称。
type Visitor func(name string) (shouldContinue bool)

// skipEmptyNames 包装 visitor，过滤掉空名称。
// 空引用在 spec 中允许存在（如未指定的 secret 名），不应触发依赖方的误判。
func skipEmptyNames(visitor Visitor) Visitor {
	return func(name string) bool {
		if len(name) == 0 {
			// 跳过空名称，继续访问。
			return true
		}
		// 交给真正的 visitor 处理。
		return visitor(name)
	}
}

// VisitContainers 用 mask 指定的容器类型过滤，对给定 pod spec 中的每个容器调用 visitor。
// 若 visitor 返回 false，则提前终止遍历。全部访问完成返回 true，被提前终止返回 false。
func VisitContainers(podSpec *v1.PodSpec, mask ContainerType, visitor ContainerVisitor) bool {
	for c, t := range ContainerIter(podSpec, mask) {
		if !visitor(c, t) {
			return false
		}
	}
	return true
}

// ContainerIter 返回一个迭代器，遍历给定 pod spec 中类型被 mask 选中的所有容器。
// 遍历顺序固定为 InitContainers、普通 Containers，最后是 EphemeralContainers，
// 保证依赖该顺序的逻辑行为稳定。
func ContainerIter(podSpec *v1.PodSpec, mask ContainerType) iter.Seq2[*v1.Container, ContainerType] {
	return func(yield func(*v1.Container, ContainerType) bool) {
		if mask&InitContainers != 0 {
			for i := range podSpec.InitContainers {
				if !yield(&podSpec.InitContainers[i], InitContainers) {
					return
				}
			}
		}
		if mask&Containers != 0 {
			for i := range podSpec.Containers {
				if !yield(&podSpec.Containers[i], Containers) {
					return
				}
			}
		}
		if mask&EphemeralContainers != 0 {
			// 临时容器结构体嵌在 EphemeralContainerCommon 里，这里做指针转换以复用同一个 visitor。
			for i := range podSpec.EphemeralContainers {
				if !yield((*v1.Container)(&podSpec.EphemeralContainers[i].EphemeralContainerCommon), EphemeralContainers) {
					return
				}
			}
		}
	}
}

// VisitPodSecretNames 对 pod spec 引用的每个 secret 名称调用 visitor。
// 若 visitor 返回 false，则提前终止遍历。不递归访问传递引用（如 pod -> pvc -> pv -> secret）。
// 全部访问完成返回 true，被提前终止返回 false。
func VisitPodSecretNames(pod *v1.Pod, visitor Visitor) bool {
	// 先过滤空名称，避免镜像拉取密钥等字段为空时产生无意义的回调。
	visitor = skipEmptyNames(visitor)
	for _, reference := range pod.Spec.ImagePullSecrets {
		if !visitor(reference.Name) {
			return false
		}
	}
	VisitContainers(&pod.Spec, AllContainers, func(c *v1.Container, containerType ContainerType) bool {
		return visitContainerSecretNames(c, visitor)
	})
	var source *v1.VolumeSource

	for i := range pod.Spec.Volumes {
		source = &pod.Spec.Volumes[i].VolumeSource
		switch {
		case source.AzureFile != nil:
			if len(source.AzureFile.SecretName) > 0 && !visitor(source.AzureFile.SecretName) {
				return false
			}
		case source.CephFS != nil:
			if source.CephFS.SecretRef != nil && !visitor(source.CephFS.SecretRef.Name) {
				return false
			}
		case source.Cinder != nil:
			if source.Cinder.SecretRef != nil && !visitor(source.Cinder.SecretRef.Name) {
				return false
			}
		case source.FlexVolume != nil:
			if source.FlexVolume.SecretRef != nil && !visitor(source.FlexVolume.SecretRef.Name) {
				return false
			}
		case source.Projected != nil:
			// Projected 卷可同时挂载多个来源，需逐个检查其中的 secret。
			for j := range source.Projected.Sources {
				if source.Projected.Sources[j].Secret != nil {
					if !visitor(source.Projected.Sources[j].Secret.Name) {
						return false
					}
				}
			}
		case source.RBD != nil:
			if source.RBD.SecretRef != nil && !visitor(source.RBD.SecretRef.Name) {
				return false
			}
		case source.Secret != nil:
			if !visitor(source.Secret.SecretName) {
				return false
			}
		case source.ScaleIO != nil:
			if source.ScaleIO.SecretRef != nil && !visitor(source.ScaleIO.SecretRef.Name) {
				return false
			}
		case source.ISCSI != nil:
			if source.ISCSI.SecretRef != nil && !visitor(source.ISCSI.SecretRef.Name) {
				return false
			}
		case source.StorageOS != nil:
			if source.StorageOS.SecretRef != nil && !visitor(source.StorageOS.SecretRef.Name) {
				return false
			}
		case source.CSI != nil:
			// CSI 只关心节点发布阶段的密钥，其他阶段的引用由驱动自行处理。
			if source.CSI.NodePublishSecretRef != nil && !visitor(source.CSI.NodePublishSecretRef.Name) {
				return false
			}
		}
	}
	return true
}

// visitContainerSecretNames 访问容器通过 envFrom 和 env.valueFrom 引用的 secret 名称，
// 仅当 visitor 返回 false 时才返回 false。
func visitContainerSecretNames(container *v1.Container, visitor Visitor) bool {
	for _, env := range container.EnvFrom {
		if env.SecretRef != nil {
			if !visitor(env.SecretRef.Name) {
				return false
			}
		}
	}
	for _, envVar := range container.Env {
		// 只有 valueFrom 形式的 secret 引用才是间接引用，直接写死的 value 不涉及 secret。
		if envVar.ValueFrom != nil && envVar.ValueFrom.SecretKeyRef != nil {
			if !visitor(envVar.ValueFrom.SecretKeyRef.Name) {
				return false
			}
		}
	}
	return true
}

// VisitPodConfigmapNames 对 pod spec 引用的每个 configmap 名称调用 visitor。
// 若 visitor 返回 false，则提前终止遍历。不递归访问传递引用（如 pod -> pvc -> pv -> secret）。
// 全部访问完成返回 true，被提前终止返回 false。
func VisitPodConfigmapNames(pod *v1.Pod, visitor Visitor) bool {
	visitor = skipEmptyNames(visitor)
	VisitContainers(&pod.Spec, AllContainers, func(c *v1.Container, containerType ContainerType) bool {
		return visitContainerConfigmapNames(c, visitor)
	})
	var source *v1.VolumeSource
	for i := range pod.Spec.Volumes {
		source = &pod.Spec.Volumes[i].VolumeSource
		switch {
		case source.Projected != nil:
			// Projected 卷可同时挂载多个来源，需逐个检查其中的 configmap。
			for j := range source.Projected.Sources {
				if source.Projected.Sources[j].ConfigMap != nil {
					if !visitor(source.Projected.Sources[j].ConfigMap.Name) {
						return false
					}
				}
			}
		case source.ConfigMap != nil:
			if !visitor(source.ConfigMap.Name) {
				return false
			}
		}
	}
	return true
}

// visitContainerConfigmapNames 访问容器通过 envFrom 和 env.valueFrom 引用的 configmap 名称，
// 仅当 visitor 返回 false 时才返回 false。
func visitContainerConfigmapNames(container *v1.Container, visitor Visitor) bool {
	for _, env := range container.EnvFrom {
		if env.ConfigMapRef != nil {
			if !visitor(env.ConfigMapRef.Name) {
				return false
			}
		}
	}
	for _, envVar := range container.Env {
		if envVar.ValueFrom != nil && envVar.ValueFrom.ConfigMapKeyRef != nil {
			if !visitor(envVar.ValueFrom.ConfigMapKeyRef.Name) {
				return false
			}
		}
	}
	return true
}

// GetContainerStatus 从 statuses 中提取名为 name 的容器状态。
// 找到返回对应状态及 true，否则返回零值和 false。
func GetContainerStatus(statuses []v1.ContainerStatus, name string) (v1.ContainerStatus, bool) {
	for i := range statuses {
		if statuses[i].Name == name {
			return statuses[i], true
		}
	}
	return v1.ContainerStatus{}, false
}

// GetExistingContainerStatus 从 statuses 中提取名为 name 的容器状态，不关心其是否存在。
// 不存在时返回零值状态，调用方无需处理第二个返回值。
func GetExistingContainerStatus(statuses []v1.ContainerStatus, name string) v1.ContainerStatus {
	status, _ := GetContainerStatus(statuses, name)
	return status
}

// GetIndexOfContainerStatus 从 statuses 中获取名为 name 的容器状态的下标。
// 找到返回 (下标, true)，否则返回 (0, false)。
func GetIndexOfContainerStatus(statuses []v1.ContainerStatus, name string) (int, bool) {
	for i := range statuses {
		if statuses[i].Name == name {
			return i, true
		}
	}
	return 0, false
}

// IsPodAvailable 判断 pod 是否可用，可用需满足两个条件：pod 必须 Ready，且
// 在此基础上满足以下之一：1. minReadySeconds == 0；
// 2. Ready 条件的 LastTransitionTime（若已设置）+ minReadySeconds <= 当前时间。
func IsPodAvailable(pod *v1.Pod, minReadySeconds int32, now metav1.Time) bool {
	if !IsPodReady(pod) {
		return false
	}

	c := GetPodReadyCondition(pod.Status)
	minReadySecondsDuration := time.Duration(minReadySeconds) * time.Second
	// LastTransitionTime 为零表示从未就绪过，此时不能仅凭 Ready 就认定稳定可用。
	if minReadySeconds == 0 || (!c.LastTransitionTime.IsZero() && c.LastTransitionTime.Add(minReadySecondsDuration).Compare(now.Time) <= 0) {
		return true
	}
	return false
}

// IsPodReady 判断 pod 是否处于 Ready 状态。
func IsPodReady(pod *v1.Pod) bool {
	return IsPodReadyConditionTrue(pod.Status)
}

// IsPodTerminal 判断 pod 是否处于终态：所有容器都已停止且状态不会再回退。
func IsPodTerminal(pod *v1.Pod) bool {
	return IsPodPhaseTerminal(pod.Status.Phase)
}

// IsPodPhaseTerminal 判断给定的 pod phase 是否为终态。
func IsPodPhaseTerminal(phase v1.PodPhase) bool {
	return phase == v1.PodFailed || phase == v1.PodSucceeded
}

// IsPodReadyConditionTrue 判断给定状态中的 PodReady 条件是否为 True。
func IsPodReadyConditionTrue(status v1.PodStatus) bool {
	condition := GetPodReadyCondition(status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// IsContainersReadyConditionTrue 判断给定状态中的 ContainersReady 条件是否为 True。
func IsContainersReadyConditionTrue(status v1.PodStatus) bool {
	condition := GetContainersReadyCondition(status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// GetPodReadyCondition 从给定状态中提取 PodReady 条件，不存在时返回 nil。
func GetPodReadyCondition(status v1.PodStatus) *v1.PodCondition {
	_, condition := GetPodCondition(&status, v1.PodReady)
	return condition
}

// GetContainersReadyCondition 从给定状态中提取 ContainersReady 条件，不存在时返回 nil。
func GetContainersReadyCondition(status v1.PodStatus) *v1.PodCondition {
	_, condition := GetPodCondition(&status, v1.ContainersReady)
	return condition
}

// GetPodCondition 从给定状态中提取指定类型的条件，返回其下标和条件指针。
// 条件不存在时返回 -1 和 nil。
func GetPodCondition(status *v1.PodStatus, conditionType v1.PodConditionType) (int, *v1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	return GetPodConditionFromList(status.Conditions, conditionType)
}

// GetPodConditionFromList 从条件列表中提取指定类型的条件，返回其下标和条件指针。
// 条件不存在时返回 -1 和 nil。
func GetPodConditionFromList(conditions []v1.PodCondition, conditionType v1.PodConditionType) (int, *v1.PodCondition) {
	if conditions == nil {
		return -1, nil
	}
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return i, &conditions[i]
		}
	}
	return -1, nil
}

// UpdatePodCondition 更新已有的 pod 条件或新增一个条件。状态发生变化时把 LastTransitionTime 设为当前时间。
// 条件发生变更或新增时返回 true。
func UpdatePodCondition(status *v1.PodStatus, condition *v1.PodCondition) bool {
	condition.LastTransitionTime = metav1.Now()
	// 先查找同类型的已有条件，决定是新增还是更新。
	conditionIndex, oldCondition := GetPodCondition(status, condition.Type)

	if oldCondition == nil {
		// 新条件直接追加。
		status.Conditions = append(status.Conditions, *condition)
		return true
	}
	// 状态未变时沿用旧的转换时间，避免仅因为重新上报就刷新时间、扰乱事件语义。
	if condition.Status == oldCondition.Status {
		condition.LastTransitionTime = oldCondition.LastTransitionTime
	}

	isEqual := condition.Status == oldCondition.Status &&
		condition.Reason == oldCondition.Reason &&
		condition.Message == oldCondition.Message &&
		condition.LastProbeTime.Equal(&oldCondition.LastProbeTime) &&
		condition.LastTransitionTime.Equal(&oldCondition.LastTransitionTime)

	status.Conditions[conditionIndex] = *condition
	// 只要任一字段变化就认为条件发生变更。
	return !isEqual
}

// IsRestartableInitContainer 判断容器是否设置了 ContainerRestartPolicyAlways。
// 本函数不校验传入的是否确实是 init 容器，只看重启策略字段本身。
func IsRestartableInitContainer(initContainer *v1.Container) bool {
	if initContainer == nil || initContainer.RestartPolicy == nil {
		return false
	}
	return *initContainer.RestartPolicy == v1.ContainerRestartPolicyAlways
}

// IsContainerRestartable 判断容器是否可重启。满足以下任一条件即可重启：
// pod 级重启策略为 Always 或 OnFailure 且未被容器级策略覆盖；容器级重启策略为 Always 或 OnFailure；
// 或者容器级重启规则中存在动作为 Restart 的规则。
func IsContainerRestartable(pod v1.PodSpec, container v1.Container) bool {
	if container.RestartPolicy != nil {
		// 显式规则优先于容器级重启策略，命中 Restart 即认为可重启。
		for _, rule := range container.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestart {
				return true
			}
		}
		return *container.RestartPolicy != v1.ContainerRestartPolicyNever
	}
	// 未设置容器级策略时回落到 pod 级策略。
	return pod.RestartPolicy != v1.RestartPolicyNever
}

// ContainerShouldRestart 根据重启策略判断容器是否应重启。
// 先按顺序匹配容器级的 restartPolicyRules，命中任一规则即采取对应动作；再使用容器级重启策略；
// 最后在未指定容器级策略时回落到 pod 级策略。
func ContainerShouldRestart(container v1.Container, pod v1.PodSpec, exitCode int32) bool {
	if container.RestartPolicy != nil {
		rule, ok := FindMatchingContainerRestartRule(container, exitCode)
		if ok {
			switch rule.Action {
			case v1.ContainerRestartRuleActionRestart:
				return true
			case v1.ContainerRestartRuleActionRestartAllContainers:
				// 该动作依赖特性开关，未开启时按容器级策略处理。
				if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
					return true
				}
				// 特性未启用，回落到容器级策略。
			default:
				// 未知动作不做处理，回落到容器级重启策略。
			}
		}

		// 没有规则命中时才检查容器级重启策略。
		switch *container.RestartPolicy {
		case v1.ContainerRestartPolicyAlways:
			return true
		case v1.ContainerRestartPolicyOnFailure:
			return exitCode != 0
		case v1.ContainerRestartPolicyNever:
			return false
		default:
			// 未知策略不做处理，回落到 pod 级策略。
		}
	}

	switch pod.RestartPolicy {
	case v1.RestartPolicyAlways:
		return true
	case v1.RestartPolicyOnFailure:
		return exitCode != 0
	case v1.RestartPolicyNever:
		return false
	default:
		// 未指定时默认策略为 Always，因此返回 true。
		return true
	}
}

// FindMatchingContainerRestartRule 若 exitCode 命中给定容器的某条重启规则，
// 返回该规则和 true；没有规则命中时返回空规则和 false。
func FindMatchingContainerRestartRule(container v1.Container, exitCode int32) (rule v1.ContainerRestartRule, found bool) {
	for _, rule := range container.RestartPolicyRules {
		if rule.ExitCodes != nil {
			exitCodeMatched := false
			for _, code := range rule.ExitCodes.Values {
				if code == exitCode {
					exitCodeMatched = true
				}
			}
			switch rule.ExitCodes.Operator {
			case v1.ContainerRestartRuleOnExitCodesOpIn:
				if exitCodeMatched {
					return rule, true
				}
			case v1.ContainerRestartRuleOnExitCodesOpNotIn:
				if !exitCodeMatched {
					return rule, true
				}
			default:
				// 未知操作符不做处理，继续检查下一条规则。
			}
		}
	}
	return v1.ContainerRestartRule{}, false
}

// AllContainersCouldRestart 判断给定 pod 中的所有容器是否都有可能触发全体容器重启。
// 只要任一容器配置了 RestartAllContainers 动作即返回 true。
func AllContainersCouldRestart(pod *v1.PodSpec) bool {
	if pod == nil {
		return false
	}
	for _, container := range pod.InitContainers {
		for _, rule := range container.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	for _, container := range pod.Containers {
		for _, rule := range container.RestartPolicyRules {
			if rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
				return true
			}
		}
	}
	return false
}

// CalculatePodStatusObservedGeneration 计算 pod status 的 observedGeneration，用于记录 kubelet
// 实际观察到的 pod generation。当特性开关 PodObservedGenerationTracking 开启，或
// status.observedGeneration 已有值时，取 pod 的 generation。
// 这样可避免死循环：开关关闭后 kubelet 试图清空该值，而 API server 又在更新中保留原值。
func CalculatePodStatusObservedGeneration(pod *v1.Pod) int64 {
	if pod.Status.ObservedGeneration != 0 || utilfeature.DefaultFeatureGate.Enabled(features.PodObservedGenerationTracking) {
		return pod.Generation
	}
	return 0
}

// CalculatePodConditionObservedGeneration 计算某条 pod 条件的 observedGeneration。
// 当特性开关 PodObservedGenerationTracking 开启，或 condition[].observedGeneration 已有值时，
// 取 pod 的 generation。
// 这样可避免死循环：开关关闭后 kubelet 试图清空该值，而 API server 又在更新中保留原值。
func CalculatePodConditionObservedGeneration(podStatus *v1.PodStatus, generation int64, conditionType v1.PodConditionType) int64 {
	if podStatus == nil {
		return 0
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.PodObservedGenerationTracking) {
		return generation
	}
	for _, condition := range podStatus.Conditions {
		if condition.Type == conditionType && condition.ObservedGeneration != 0 {
			return generation
		}
	}
	return 0
}
