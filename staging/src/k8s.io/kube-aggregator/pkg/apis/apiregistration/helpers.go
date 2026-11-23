/*
Copyright 2016 The Kubernetes Authors.

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

package apiregistration

import (
	"sort"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/version"
)

// SortedByGroupAndVersion sorts APIServices into their different groups, and then sorts them based on their versions.
// For example, the first element of the first array contains the APIService with the highest version number, in the
// group with the highest priority; while the last element of the last array contains the APIService with the lowest
// version number, in the group with the lowest priority.
// 聚合层把 APIService 按 group 分组、组内按版本排序，供 discovery 和路由按优先级消费。
// 用二维数组而不是扁平切片，是为了把“先选高优先级 group、再在其内选高优先级版本”的
// 两级决策直接体现在数据结构里，调用方顺序遍历即可得到正确优先级。
func SortedByGroupAndVersion(servers []*APIService) [][]*APIService {
	serversByGroupPriorityMinimum := ByGroupPriorityMinimum(servers)
	sort.Sort(serversByGroupPriorityMinimum)

	ret := [][]*APIService{}
	for _, curr := range serversByGroupPriorityMinimum {
		// check to see if we already have an entry for this group
		// 已经全局按 groupPriority 排好序，所以同 group 的 APIService 必然相邻出现。
		// 这里线性查找只需要在已有分组中比对，分组数量通常很小，不值得再建索引。
		existingIndex := -1
		for j, groupInReturn := range ret {
			if groupInReturn[0].Spec.Group == curr.Spec.Group {
				existingIndex = j
				break
			}
		}

		if existingIndex >= 0 {
			ret[existingIndex] = append(ret[existingIndex], curr)
			// 每加入一个同组版本就重排，而不是最后统一排一次：这样 ret 中每个分组
			// 在任何时刻都保持有序，逻辑更简单，代价可忽略（单组版本数很少）。
			sort.Sort(ByVersionPriority(ret[existingIndex]))
			continue
		}

		ret = append(ret, []*APIService{curr})
	}

	return ret
}

// ByGroupPriorityMinimum sorts with the highest group number first, then by name.
// This is not a simple reverse, because we want the name sorting to be alpha, not
// reverse alpha.
// 用 sort.Interface 实现排序而非直接 sort.Slice，是因为这两个比较器还会被
// 其他包复用（例如作为 sort.Sort 的参数传入），在类型上显式声明可以避免重复定义比较逻辑。
// 注意：优先级数值倒序，但同名冲突时的 name 仍保持正序，因为 name 只是稳定输出的 workaround，
// 并非优先级的一部分；对整个比较器做 reverse 会连 name 一起反转，破坏这一约定。
type ByGroupPriorityMinimum []*APIService

func (s ByGroupPriorityMinimum) Len() int      { return len(s) }
func (s ByGroupPriorityMinimum) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByGroupPriorityMinimum) Less(i, j int) bool {
	if s[i].Spec.GroupPriorityMinimum != s[j].Spec.GroupPriorityMinimum {
		return s[i].Spec.GroupPriorityMinimum > s[j].Spec.GroupPriorityMinimum
	}
	return s[i].Name < s[j].Name
}

// ByVersionPriority sorts with the highest version number first, then by name.
// This is not a simple reverse, because we want the name sorting to be alpha, not
// reverse alpha.
// 版本优先级先按 VersionPriority 数值倒序；数值相同时再用 Kubernetes 风格的版本比较函数
// 比较版本号本身，从而让 v10 排在 v9 之后而不是按字典序排在 v1 旁边。
// 和 ByGroupPriorityMinimum 一样不整体 reverse，是为了保持 name 比较的正序语义。
type ByVersionPriority []*APIService

func (s ByVersionPriority) Len() int      { return len(s) }
func (s ByVersionPriority) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByVersionPriority) Less(i, j int) bool {
	if s[i].Spec.VersionPriority != s[j].Spec.VersionPriority {
		return s[i].Spec.VersionPriority > s[j].Spec.VersionPriority
	}
	return version.CompareKubeAwareVersionStrings(s[i].Spec.Version, s[j].Spec.Version) > 0
}

// NewLocalAvailableAPIServiceCondition returns a condition for an available local APIService.
// 本地 APIService（Spec.Service 为 nil，由聚合层自身提供）不需要走远端健康检查，
// 因此直接构造一个可用的状态；Reason/Message 固定为 Local，便于在 status 中区分来源。
func NewLocalAvailableAPIServiceCondition() APIServiceCondition {
	return APIServiceCondition{
		Type:               Available,
		Status:             ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "Local",
		Message:            "Local APIServices are always available",
	}
}

// GetAPIServiceConditionByType gets an *APIServiceCondition by APIServiceConditionType if present
// 返回切片元素的地址而不是值副本，是为了让调用方（如 SetAPIServiceCondition）能原地修改
// condition 的 Status、Reason 等字段，同时保留切片中其他 condition 不被改动。
func GetAPIServiceConditionByType(apiService *APIService, conditionType APIServiceConditionType) *APIServiceCondition {
	for i := range apiService.Status.Conditions {
		if apiService.Status.Conditions[i].Type == conditionType {
			return &apiService.Status.Conditions[i]
		}
	}
	return nil
}

// SetAPIServiceCondition sets the status condition.  It either overwrites the existing one or
// creates a new one
// 只在 Status 真正翻转时才更新 LastTransitionTime，因为该字段语义是“状态最后一次变化的时间”，
// 而 Reason/Message 每次都覆盖以反映最新的探测结果。这样状态稳定但消息变化时不会误导调用方
// 以为发生过状态转换。
func SetAPIServiceCondition(apiService *APIService, newCondition APIServiceCondition) {
	existingCondition := GetAPIServiceConditionByType(apiService, newCondition.Type)
	if existingCondition == nil {
		apiService.Status.Conditions = append(apiService.Status.Conditions, newCondition)
		return
	}

	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		existingCondition.LastTransitionTime = newCondition.LastTransitionTime
	}

	existingCondition.Reason = newCondition.Reason
	existingCondition.Message = newCondition.Message
}

// IsAPIServiceConditionTrue indicates if the condition is present and strictly true
// 缺席的 condition 视为 false，而不是 Unknown：调用方通常只关心“是否已确定可用”，
// 未设置状态自然不满足这一条件。注意这里严格比较 ConditionTrue，
// ConditionUnknown 不会被误判为可用。
func IsAPIServiceConditionTrue(apiService *APIService, conditionType APIServiceConditionType) bool {
	condition := GetAPIServiceConditionByType(apiService, conditionType)
	return condition != nil && condition.Status == ConditionTrue
}
