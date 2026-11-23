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

package helper

import (
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// SortedByGroupAndVersion sorts APIServices into their different groups, and then sorts them based on their versions.
// For example, the first element of the first array contains the APIService with the highest version number, in the
// group with the highest priority; while the last element of the last array contains the APIService with the lowest
// version number, in the group with the lowest priority.
//
// 中文补充：发现文档既要决定"哪个组排在前面"，也要决定"组内哪个版本是首选"，
// 所以这里先按组优先级把 APIService 排好序，再把同组的连续段收拢成子切片，
// 每个子切片内部按版本优先级排序。返回值是二维结构而不是一维，正是为了让调用方
// 一次拿到"组的顺序"和"组内版本的顺序"两层信息。
func SortedByGroupAndVersion(servers []*v1.APIService) [][]*v1.APIService {
	serversByGroupPriorityMinimum := ByGroupPriorityMinimum(servers)
	sort.Sort(serversByGroupPriorityMinimum)

	ret := [][]*v1.APIService{}
	for _, curr := range serversByGroupPriorityMinimum {
		// check to see if we already have an entry for this group
		// 中文补充：输入已按组优先级排好，同组的项必然相邻；但返回结构按组聚合，
		// 所以还是要查一次已有分组，把同组的版本并进去。
		existingIndex := -1
		for j, groupInReturn := range ret {
			if groupInReturn[0].Spec.Group == curr.Spec.Group {
				existingIndex = j
				break
			}
		}

		if existingIndex >= 0 {
			ret[existingIndex] = append(ret[existingIndex], curr)
			sort.Sort(ByVersionPriority(ret[existingIndex]))
			continue
		}

		ret = append(ret, []*v1.APIService{curr})
	}

	return ret
}

// ByGroupPriorityMinimum sorts with the highest group number first, then by name.
// This is not a simple reverse, because we want the name sorting to be alpha, not
// reverse alpha.
//
// 中文补充：主排序键（优先级）要降序，次排序键（名字）却要升序，
// 单纯把升序结果反转会连名字顺序也一起反过来，所以必须在 Less 里分别处理两个键。
// 名字作为 tie-breaker 让同优先级的组顺序稳定可复现。
type ByGroupPriorityMinimum []*v1.APIService

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
//
// 中文补充：和组排序同理，但次级键用的是 Kubernetes 风格的版本号比较，
// 而不是字典序——v10 必须排在 v9 后面，字典序会把 v10 排到 v2 前面。
type ByVersionPriority []*v1.APIService

func (s ByVersionPriority) Len() int      { return len(s) }
func (s ByVersionPriority) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s ByVersionPriority) Less(i, j int) bool {
	if s[i].Spec.VersionPriority != s[j].Spec.VersionPriority {
		return s[i].Spec.VersionPriority > s[j].Spec.VersionPriority
	}
	return version.CompareKubeAwareVersionStrings(s[i].Spec.Version, s[j].Spec.Version) > 0
}

// APIServiceNameToGroupVersion returns the GroupVersion for a given apiServiceName.  The name
// must be valid, but any object you get back from an informer will be valid.
//
// 中文补充：APIService 的名字约定就是 "<version>.<group>"（legacy 例外是 "v1."），
// 所以路由反查不需要读对象，直接从名字拆两段即可。SplitN 限制为 2，
// 是为了让 group 里若带点号也能整体保留。名字的合法性由 API server 创建时校验，
// 这里不再重复校验。
func APIServiceNameToGroupVersion(apiServiceName string) schema.GroupVersion {
	tokens := strings.SplitN(apiServiceName, ".", 2)
	return schema.GroupVersion{Group: tokens[1], Version: tokens[0]}
}

// NewLocalAvailableAPIServiceCondition returns a condition for an available local APIService.
//
// 中文补充：不指向任何后端 Service 的 APIService 由聚合层自己处理，路由天然存在，
// 没有探测的必要；因此状态是恒真的，Reason 标 "Local" 让排查的人一眼看出
// 这不是探测出来的结果。
func NewLocalAvailableAPIServiceCondition() v1.APIServiceCondition {
	return v1.APIServiceCondition{
		Type:               v1.Available,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             "Local",
		Message:            "Local APIServices are always available",
	}
}

// SetAPIServiceCondition sets the status condition.  It either overwrites the existing one or
// creates a new one
//
// 中文补充：LastTransitionTime 只在状态真的翻转时才更新，否则每次 Reason/Message
// 的微小变化都会让"上次状态变更时间"失真。Reason 和 Message 则无条件覆盖，
// 因为它们描述的是当前探测结果，属于最新信息。
func SetAPIServiceCondition(apiService *v1.APIService, newCondition v1.APIServiceCondition) {
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
//
// 中文补充：用指针返回找到的 condition，是为了让"没有这个类型"和"有但状态不是 true"
// 都自然落到 false；调用方不需要先确认存在性再判断状态。
func IsAPIServiceConditionTrue(apiService *v1.APIService, conditionType v1.APIServiceConditionType) bool {
	condition := GetAPIServiceConditionByType(apiService, conditionType)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// GetAPIServiceConditionByType gets an *APIServiceCondition by APIServiceConditionType if present
//
// 中文补充：返回切片元素的地址而不是副本，调用方才能原地修改 match 到的 condition；
// SetAPIServiceCondition 正是靠这一点避免重建整个 Status.Conditions。
func GetAPIServiceConditionByType(apiService *v1.APIService, conditionType v1.APIServiceConditionType) *v1.APIServiceCondition {
	for i := range apiService.Status.Conditions {
		if apiService.Status.Conditions[i].Type == conditionType {
			return &apiService.Status.Conditions[i]
		}
	}
	return nil
}
