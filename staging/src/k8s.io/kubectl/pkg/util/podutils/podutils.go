/*
Copyright 2014 The Kubernetes Authors.

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

package podutils

import (
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Pod 可用时返回 true。
// 可用 Pod 必须先就绪，此外满足以下任一条件即可：
// 1. minReadySeconds == 0；或
// 2. LastTransitionTime + minReadySeconds 早于当前时间。
func IsPodAvailable(pod *corev1.Pod, minReadySeconds int32, now metav1.Time) bool {
	if !IsPodReady(pod) {
		return false
	}

	c := getPodReadyCondition(pod.Status)
	minReadySecondsDuration := time.Duration(minReadySeconds) * time.Second
	if minReadySeconds == 0 || !c.LastTransitionTime.IsZero() && c.LastTransitionTime.Add(minReadySecondsDuration).Before(now.Time) {
		return true
	}
	return false
}

// Pod 就绪时返回 true。
func IsPodReady(pod *corev1.Pod) bool {
	return isPodReadyConditionTrue(pod.Status)
}

func isPodDeleting(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

// Pod 的 Ready Condition 为 true 时返回 true。
func isPodReadyConditionTrue(status corev1.PodStatus) bool {
	condition := getPodReadyCondition(status)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

// 从给定 status 中提取 Pod 的 Ready Condition。
// 不存在时返回 nil。
func getPodReadyCondition(status corev1.PodStatus) *corev1.PodCondition {
	_, condition := getPodCondition(&status, corev1.PodReady)
	return condition
}

// 从给定 status 中提取指定 Condition。
// 不存在时返回 nil 和 -1，否则返回 Condition 及其索引。
func getPodCondition(status *corev1.PodStatus, conditionType corev1.PodConditionType) (int, *corev1.PodCondition) {
	if status == nil {
		return -1, nil
	}
	return getPodConditionFromList(status.Conditions, conditionType)
}

// 从给定 Condition 列表中提取指定 Condition，
// 返回索引和 Condition；不存在时返回 -1 和 nil。
func getPodConditionFromList(conditions []corev1.PodCondition, conditionType corev1.PodConditionType) (int, *corev1.PodCondition) {
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

// 对 Pod 排序，以便选出最适合取日志的一个。
type ByLogging []*corev1.Pod

func (s ByLogging) Len() int      { return len(s) }
func (s ByLogging) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s ByLogging) Less(i, j int) bool {
	// 1. 已调度 < 未调度；
	if s[i].Spec.NodeName != s[j].Spec.NodeName && (len(s[i].Spec.NodeName) == 0 || len(s[j].Spec.NodeName) == 0) {
		return len(s[i].Spec.NodeName) > 0
	}
	// 2. Running < Unknown < Pending；
	m := map[corev1.PodPhase]int{corev1.PodRunning: 0, corev1.PodUnknown: 1, corev1.PodPending: 2}
	if m[s[i].Status.Phase] != m[s[j].Status.Phase] {
		return m[s[i].Status.Phase] < m[s[j].Status.Phase]
	}
	// 3. 就绪 < 未就绪；
	if IsPodReady(s[i]) != IsPodReady(s[j]) {
		return IsPodReady(s[i])
	}
	// TODO: 将 Deployment 的 minReadySeconds 下推到 Pod 后，应把可用性纳入排序。
	//       see https://github.com/kubernetes/kubernetes/issues/22065
	// 4. 就绪时长较长 < 较短 < 空；
	if IsPodReady(s[i]) && IsPodReady(s[j]) && !podReadyTime(s[i]).Equal(podReadyTime(s[j])) {
		return afterOrZero(podReadyTime(s[j]), podReadyTime(s[i]))
	}
	// 5. 容器重启次数多 < 少；
	if res := compareMaxContainerRestarts(s[i], s[j]); res != nil {
		return *res
	}
	// 6. 创建较早 < 较晚 < 时间为空。
	if !s[i].CreationTimestamp.Equal(&s[j].CreationTimestamp) {
		return afterOrZero(&s[j].CreationTimestamp, &s[i].CreationTimestamp)
	}
	return false
}

// 对 Pod 排序，供控制器挑选最适合删除的一个。
type ActivePods []*corev1.Pod

func (s ActivePods) Len() int      { return len(s) }
func (s ActivePods) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s ActivePods) Less(i, j int) bool {
	// 1. 未调度 < 已调度；
	// 只有一个未调度时，它排在前面。
	if s[i].Spec.NodeName != s[j].Spec.NodeName && (len(s[i].Spec.NodeName) == 0 || len(s[j].Spec.NodeName) == 0) {
		return len(s[i].Spec.NodeName) == 0
	}
	// 2. Pending < Unknown < Running；
	m := map[corev1.PodPhase]int{corev1.PodPending: 0, corev1.PodUnknown: 1, corev1.PodRunning: 2}
	if m[s[i].Status.Phase] != m[s[j].Status.Phase] {
		return m[s[i].Status.Phase] < m[s[j].Status.Phase]
	}
	// 3. 未就绪 < 就绪；
	// 只有一个未就绪时，它排在前面。
	if IsPodReady(s[i]) != IsPodReady(s[j]) {
		return !IsPodReady(s[i])
	}
	// 4. 正在删除 < 未删除；
	if isPodDeleting(s[i]) != isPodDeleting(s[j]) {
		return isPodDeleting(s[i])
	}
	// 5. 删除时间较早 < 较晚；
	if isPodDeleting(s[i]) && isPodDeleting(s[j]) && !s[i].ObjectMeta.DeletionTimestamp.Equal(s[j].ObjectMeta.DeletionTimestamp) {
		return s[i].ObjectMeta.DeletionTimestamp.Before(s[j].ObjectMeta.DeletionTimestamp)
	}
	// TODO: 将 Deployment 的 minReadySeconds 下推到 Pod 后，应把可用性纳入排序。
	//       see https://github.com/kubernetes/kubernetes/issues/22065
	// 6. 就绪时长为空 < 较短 < 较长；
	// 都就绪时，最近就绪的排在前面。
	if IsPodReady(s[i]) && IsPodReady(s[j]) && !podReadyTime(s[i]).Equal(podReadyTime(s[j])) {
		return afterOrZero(podReadyTime(s[i]), podReadyTime(s[j]))
	}
	// 7. 容器重启次数多 < 少；
	if res := compareMaxContainerRestarts(s[i], s[j]); res != nil {
		return *res
	}
	// 8. 创建时间为空 < 较晚 < 较早。
	if !s[i].CreationTimestamp.Equal(&s[j].CreationTimestamp) {
		return afterOrZero(&s[i].CreationTimestamp, &s[j].CreationTimestamp)
	}
	return false
}

// 比较 t1 是否晚于 t2；其中一个为零值时，
// 零值反而视为更晚。
func afterOrZero(t1, t2 *metav1.Time) bool {
	if t1.Time.IsZero() || t2.Time.IsZero() {
		return t1.Time.IsZero()
	}
	return t1.After(t2.Time)
}

func podReadyTime(pod *corev1.Pod) *metav1.Time {
	for _, c := range pod.Status.Conditions {
		// 只关心 Pod 的 Ready Condition。
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return &c.LastTransitionTime
		}
	}
	return &metav1.Time{}
}

func maxContainerRestarts(pod *corev1.Pod) (regularRestarts, sidecarRestarts int) {
	for _, c := range pod.Status.ContainerStatuses {
		regularRestarts = max(regularRestarts, int(c.RestartCount))
	}
	names := sets.New[string]()
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			names.Insert(c.Name)
		}
	}
	for _, c := range pod.Status.InitContainerStatuses {
		if names.Has(c.Name) {
			sidecarRestarts = max(sidecarRestarts, int(c.RestartCount))
		}
	}
	return
}

// 用 *bool 表示比较结果：
// true：pi 的容器重启次数更多；
// false：pj 更多；
// nil：两者相同。
func compareMaxContainerRestarts(pi *corev1.Pod, pj *corev1.Pod) *bool {
	regularRestartsI, sidecarRestartsI := maxContainerRestarts(pi)
	regularRestartsJ, sidecarRestartsJ := maxContainerRestarts(pj)
	if regularRestartsI != regularRestartsJ {
		res := regularRestartsI > regularRestartsJ
		return &res
	}
	// 重启次数相同时，再尝试比较 sidecar 容器的重启次数。
	if sidecarRestartsI != sidecarRestartsJ {
		res := sidecarRestartsI > sidecarRestartsJ
		return &res
	}
	return nil
}

// ContainerType 和 VisitContainers 取自 Kubernetes 上游，
// https://github.com/kubernetes/kubernetes/blob/master/pkg/api/v1/pod/util.go
// 受项目目标限制，kubectl 不能直接导入。

// 表示容器类型。
type ContainerType int

const (
	// 普通容器。
	Containers ContainerType = 1 << iota
	// 初始化容器。
	InitContainers
	// 临时容器。
	EphemeralContainers
)

// 访问所有容器。
const AllContainers ContainerType = (InitContainers | Containers | EphemeralContainers)

// 对每个容器 spec 调用，返回 true
// 表示继续访问。
type ContainerVisitor func(container *corev1.Container, containerType ContainerType) (shouldContinue bool)

// 对 PodSpec 中类型匹配 mask 的每个容器调用 visitor；
// visitor 返回 false 时提前结束；
// 全部访问完成返回 true，提前结束返回 false。
func VisitContainers(podSpec *corev1.PodSpec, mask ContainerType, visitor ContainerVisitor) bool {
	if mask&InitContainers != 0 {
		for i := range podSpec.InitContainers {
			if !visitor(&podSpec.InitContainers[i], InitContainers) {
				return false
			}
		}
	}
	if mask&Containers != 0 {
		for i := range podSpec.Containers {
			if !visitor(&podSpec.Containers[i], Containers) {
				return false
			}
		}
	}
	if mask&EphemeralContainers != 0 {
		for i := range podSpec.EphemeralContainers {
			if !visitor((*corev1.Container)(&podSpec.EphemeralContainers[i].EphemeralContainerCommon), EphemeralContainers) {
				return false
			}
		}
	}
	return true
}
