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

package deletion

import (
	"fmt"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
)

// NamespaceConditionUpdater 将命名空间清理器（namespace deleter）在删除过程中
// 遇到的各类错误转换为 namespace 的 status conditions。
// 每类错误对应一种 condition；Update 负责把累积的结果一次性写回 status。
type NamespaceConditionUpdater interface {
	ProcessDiscoverResourcesErr(e error)
	ProcessGroupVersionErr(e error)
	ProcessDeleteContentErr(e error)
	Update(*v1.Namespace) bool
}

// namespaceConditionUpdater 是 NamespaceConditionUpdater 的默认实现。
// newConditions 暂存本轮新生成的 condition；deleteContentErrors 单独累积删除内容的错误，
// 待所有资源类型处理完毕后在 Update 中统一汇总为一个 condition。
type namespaceConditionUpdater struct {
	newConditions       []v1.NamespaceCondition
	deleteContentErrors []error
}

var _ NamespaceConditionUpdater = &namespaceConditionUpdater{}

var (
	// conditionTypes 由 namespace_deleter 控制器维护的 namespace condition 类型列表。
	// 顺序固定：Update 会按此顺序遍历，未报告错误的类型会被置为“成功”状态。
	conditionTypes = []v1.NamespaceConditionType{
		v1.NamespaceDeletionDiscoveryFailure,
		v1.NamespaceDeletionGVParsingFailure,
		v1.NamespaceDeletionContentFailure,
		v1.NamespaceContentRemaining,
		v1.NamespaceFinalizersRemaining,
	}
	// okMessages 定义各 condition 处于“成功”（Status=False）状态时的说明信息。
	okMessages = map[v1.NamespaceConditionType]string{
		v1.NamespaceDeletionDiscoveryFailure: "All resources successfully discovered",
		v1.NamespaceDeletionGVParsingFailure: "All legacy kube types successfully parsed",
		v1.NamespaceDeletionContentFailure:   "All content successfully deleted, may be waiting on finalization",
		v1.NamespaceContentRemaining:         "All content successfully removed",
		v1.NamespaceFinalizersRemaining:      "All content-preserving finalizers finished",
	}
	// okReasons 定义各 condition 处于“成功”（Status=False）状态时的原因码，
	// 与 okMessages 配套使用，保证成功状态的 Reason/Message 稳定可比对。
	okReasons = map[v1.NamespaceConditionType]string{
		v1.NamespaceDeletionDiscoveryFailure: "ResourcesDiscovered",
		v1.NamespaceDeletionGVParsingFailure: "ParsedGroupVersions",
		v1.NamespaceDeletionContentFailure:   "ContentDeleted",
		v1.NamespaceContentRemaining:         "ContentRemoved",
		v1.NamespaceFinalizersRemaining:      "ContentHasNoFinalizers",
	}
)

// ProcessGroupVersionErr 在资源 GroupVersion 解析失败时记录一个错误 condition。
// 解析失败意味着无法确定资源对应的 REST 映射，后续删除可能无法覆盖该类资源。
func (u *namespaceConditionUpdater) ProcessGroupVersionErr(err error) {
	d := v1.NamespaceCondition{
		Type:               v1.NamespaceDeletionGVParsingFailure,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "GroupVersionParsingFailed",
		Message:            err.Error(),
	}
	u.newConditions = append(u.newConditions, d)
}

// ProcessDiscoverResourcesErr 根据资源发现阶段的错误生成错误 condition。
// 对 ErrGroupDiscoveryFailed 单独统计失败的 API group 数量，便于用户定位问题范围；
// 其他类型的错误则直接使用其错误文本。
func (u *namespaceConditionUpdater) ProcessDiscoverResourcesErr(err error) {
	var msg string
	if derr, ok := err.(*discovery.ErrGroupDiscoveryFailed); ok {
		msg = fmt.Sprintf("Discovery failed for some groups, %d failing: %v", len(derr.Groups), err)
	} else {
		msg = err.Error()
	}
	d := v1.NamespaceCondition{
		Type:               v1.NamespaceDeletionDiscoveryFailure,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "DiscoveryFailed",
		Message:            msg,
	}
	u.newConditions = append(u.newConditions, d)

}

// ProcessContentTotals 根据删除汇总信息，按需生成 NamespaceContentRemaining
// 和 NamespaceFinalizersRemaining 两个 condition。
// 遍历 map 时顺序不确定，因此需先排序，避免相同状态产生不同的 Message 导致无谓的 status 更新。
func (u *namespaceConditionUpdater) ProcessContentTotals(contentTotals allGVRDeletionMetadata) {
	if len(contentTotals.gvrToNumRemaining) != 0 {
		remainingResources := []string{}
		for gvr, numRemaining := range contentTotals.gvrToNumRemaining {
			if numRemaining == 0 {
				continue
			}
			remainingResources = append(remainingResources, fmt.Sprintf("%s.%s has %d resource instances", gvr.Resource, gvr.Group, numRemaining))
		}
		// 排序保证 Message 稳定，避免在 map 遍历顺序变化时反复触发 status 更新。
		sort.Strings(remainingResources)
		u.newConditions = append(u.newConditions, v1.NamespaceCondition{
			Type:               v1.NamespaceContentRemaining,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "SomeResourcesRemain",
			Message:            fmt.Sprintf("Some resources are remaining: %s", strings.Join(remainingResources, ", ")),
		})
	}

	if len(contentTotals.finalizersToNumRemaining) != 0 {
		remainingByFinalizer := []string{}
		for finalizer, numRemaining := range contentTotals.finalizersToNumRemaining {
			if numRemaining == 0 {
				continue
			}
			remainingByFinalizer = append(remainingByFinalizer, fmt.Sprintf("%s in %d resource instances", finalizer, numRemaining))
		}
		// 同上：排序后再拼接，保证 finalizer 列表顺序稳定。
		sort.Strings(remainingByFinalizer)
		u.newConditions = append(u.newConditions, v1.NamespaceCondition{
			Type:               v1.NamespaceFinalizersRemaining,
			Status:             v1.ConditionTrue,
			LastTransitionTime: metav1.Now(),
			Reason:             "SomeFinalizersRemain",
			Message:            fmt.Sprintf("Some content in the namespace has finalizers remaining: %s", strings.Join(remainingByFinalizer, ", ")),
		})
	}
}

// ProcessDeleteContentErr 累积删除内容时的错误。
// 这里不立即生成 condition：所有资源类型的删除错误会在 Update 中合并为一个
// NamespaceDeletionContentFailure，避免同类 condition 重复出现。
func (u *namespaceConditionUpdater) ProcessDeleteContentErr(err error) {
	u.deleteContentErrors = append(u.deleteContentErrors, err)
}

// Update 将命名空间删除过程中累积的错误汇总为 status conditions 并写入 namespace。
// 返回值表示 status 是否发生变化，调用方可据此决定是否真正发起 API 更新。
func (u *namespaceConditionUpdater) Update(ns *v1.Namespace) bool {
	if c := getCondition(u.newConditions, v1.NamespaceDeletionContentFailure); c == nil {
		if c := makeDeleteContentCondition(u.deleteContentErrors); c != nil {
			u.newConditions = append(u.newConditions, *c)
		}
	}
	return updateConditions(&ns.Status, u.newConditions)
}

// makeDeleteContentCondition 将多个删除内容错误合并为一个 NamespaceDeletionContentFailure
// condition；没有错误时返回 nil。错误消息排序后拼接，保证输出稳定可比对。
func makeDeleteContentCondition(err []error) *v1.NamespaceCondition {
	if len(err) == 0 {
		return nil
	}
	msgs := make([]string, 0, len(err))
	for _, e := range err {
		msgs = append(msgs, e.Error())
	}
	sort.Strings(msgs)
	return &v1.NamespaceCondition{
		Type:               v1.NamespaceDeletionContentFailure,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "ContentDeletionFailed",
		Message:            fmt.Sprintf("Failed to delete all resource types, %d remaining: %v", len(err), strings.Join(msgs, ", ")),
	}
}

// updateConditions 将 newConditions 合并进 status.Conditions，并报告是否发生变化。
// 对每种受维护的 condition 类型：本轮没有报告错误时，用“成功”版本覆盖旧状态；
// 仅在 Status/Message/Reason 有差异时才写回，减少不必要的 API 更新。
// LastTransitionTime 只在 Status 真正翻转时刷新，保持其语义（状态转换时间）。
func updateConditions(status *v1.NamespaceStatus, newConditions []v1.NamespaceCondition) (hasChanged bool) {
	for _, conditionType := range conditionTypes {
		newCondition := getCondition(newConditions, conditionType)
		// 本轮没有该类型的错误 condition，说明已恢复正常，需写入对应的“成功”版本。
		if newCondition == nil {
			newCondition = newSuccessfulCondition(conditionType)
		}
		oldCondition := getCondition(status.Conditions, conditionType)

		// 旧 status 中不存在该类型的 condition，直接追加。
		if oldCondition == nil {
			status.Conditions = append(status.Conditions, *newCondition)
			hasChanged = true

		} else if oldCondition.Status != newCondition.Status || oldCondition.Message != newCondition.Message || oldCondition.Reason != newCondition.Reason {
			// 旧 condition 内容有差异，需要更新；其中 Status 翻转才刷新转换时间。
			if oldCondition.Status != newCondition.Status {
				oldCondition.LastTransitionTime = metav1.Now()
			}
			oldCondition.Type = newCondition.Type
			oldCondition.Status = newCondition.Status
			oldCondition.Reason = newCondition.Reason
			oldCondition.Message = newCondition.Message
			hasChanged = true
		}
	}
	return
}

// newSuccessfulCondition 为指定类型构造一个“成功”condition：Status=False，
// Reason/Message 取自 okReasons/okMessages，表示本轮未检测到该类错误。
func newSuccessfulCondition(conditionType v1.NamespaceConditionType) *v1.NamespaceCondition {
	return &v1.NamespaceCondition{
		Type:               conditionType,
		Status:             v1.ConditionFalse,
		LastTransitionTime: metav1.Now(),
		Reason:             okReasons[conditionType],
		Message:            okMessages[conditionType],
	}
}

// getCondition 按类型查找 condition，返回其指针以便调用方原地修改；不存在时返回 nil。
func getCondition(conditions []v1.NamespaceCondition, conditionType v1.NamespaceConditionType) *v1.NamespaceCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &(conditions[i])
		}
	}
	return nil
}
