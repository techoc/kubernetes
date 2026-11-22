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

package v1

import (
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Equals returns true if the two lists are equivalent
// 使用 Quantity.Cmp 而非 ==，因为 resource.Quantity 的内部表示（后缀/缓存字符串）
// 可能不同但数值相等。
func Equals(a corev1.ResourceList, b corev1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}

	for key, value1 := range a {
		value2, found := b[key]
		if !found {
			return false
		}
		if value1.Cmp(value2) != 0 {
			return false
		}
	}

	return true
}

// LessThanOrEqual returns true if a < b for each key in b
// If false, it returns the keys in a that exceeded b
// 只比较 b 中出现的 key：a 独有的资源不受 b 约束。
func LessThanOrEqual(a corev1.ResourceList, b corev1.ResourceList) (bool, []corev1.ResourceName) {
	result := true
	resourceNames := []corev1.ResourceName{}
	for key, value := range b {
		if other, found := a[key]; found {
			if other.Cmp(value) > 0 {
				result = false
				resourceNames = append(resourceNames, key)
			}
		}
	}
	return result, resourceNames
}

// Max returns the result of Max(a, b) for each named resource
// 返回新对象且逐项 DeepCopy，避免调用方修改结果时污染输入。
func Max(a corev1.ResourceList, b corev1.ResourceList) corev1.ResourceList {
	result := corev1.ResourceList{}
	for key, value := range a {
		if other, found := b[key]; found {
			if value.Cmp(other) <= 0 {
				result[key] = other.DeepCopy()
				continue
			}
		}
		result[key] = value.DeepCopy()
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			result[key] = value.DeepCopy()
		}
	}
	return result
}

// Add returns the result of a + b for each named resource
// 第二个循环负责补上仅存在于 b 的资源；DeepCopy 保证输入不被复用。
func Add(a corev1.ResourceList, b corev1.ResourceList) corev1.ResourceList {
	result := corev1.ResourceList{}
	for key, value := range a {
		quantity := value.DeepCopy()
		if other, found := b[key]; found {
			quantity.Add(other)
		}
		result[key] = quantity
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			result[key] = value.DeepCopy()
		}
	}
	return result
}

// SubtractWithNonNegativeResult - subtracts and returns result of a - b but
// makes sure we don't return negative values to prevent negative resource usage.
// 结果中保留值为 0 的条目，便于状态对比时识别“曾经有、现在为 0”的资源。
func SubtractWithNonNegativeResult(a corev1.ResourceList, b corev1.ResourceList) corev1.ResourceList {
	zero := resource.MustParse("0")

	result := corev1.ResourceList{}
	for key, value := range a {
		quantity := value.DeepCopy()
		if other, found := b[key]; found {
			quantity.Sub(other)
		}
		if quantity.Cmp(zero) > 0 {
			result[key] = quantity
		} else {
			result[key] = zero
		}
	}

	for key := range b {
		if _, found := result[key]; !found {
			result[key] = zero
		}
	}
	return result
}

// Subtract returns the result of a - b for each named resource
// 与 SubtractWithNonNegativeResult 不同，这里允许负数结果；仅存在于 b 的资源取负值。
func Subtract(a corev1.ResourceList, b corev1.ResourceList) corev1.ResourceList {
	result := corev1.ResourceList{}
	for key, value := range a {
		quantity := value.DeepCopy()
		if other, found := b[key]; found {
			quantity.Sub(other)
		}
		result[key] = quantity
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			quantity := value.DeepCopy()
			quantity.Neg()
			result[key] = quantity
		}
	}
	return result
}

// Mask returns a new resource list that only has the values with the specified names
// 用于把聚合后的用量裁剪到当前 quota 关心的资源集合。
func Mask(resources corev1.ResourceList, names []corev1.ResourceName) corev1.ResourceList {
	nameSet := ToSet(names)
	result := corev1.ResourceList{}
	for key, value := range resources {
		if nameSet.Has(string(key)) {
			result[key] = value.DeepCopy()
		}
	}
	return result
}

// ResourceNames returns a list of all resource names in the ResourceList
// 顺序不保证稳定（来自 map 迭代），需要确定性时调用方应自行排序。
func ResourceNames(resources corev1.ResourceList) []corev1.ResourceName {
	result := []corev1.ResourceName{}
	for resourceName := range resources {
		result = append(result, resourceName)
	}
	return result
}

// Contains returns true if the specified item is in the list of items
func Contains(items []corev1.ResourceName, item corev1.ResourceName) bool {
	for _, i := range items {
		if i == item {
			return true
		}
	}
	return false
}

// ContainsPrefix returns true if the specified item has a prefix that contained in given prefix Set
// 常用于判断 count/<resource> 一类带前缀的扩展资源。
func ContainsPrefix(prefixSet []string, item corev1.ResourceName) bool {
	for _, prefix := range prefixSet {
		if strings.HasPrefix(string(item), prefix) {
			return true
		}
	}
	return false
}

// Intersection returns the intersection of both list of resources, deduped and sorted
// 排序与去重使结果可直接用于比较或作为 map key 集合。
func Intersection(a []corev1.ResourceName, b []corev1.ResourceName) []corev1.ResourceName {
	result := make([]corev1.ResourceName, 0, len(a))
	for _, item := range a {
		if Contains(result, item) {
			continue
		}
		if !Contains(b, item) {
			continue
		}
		result = append(result, item)
	}
	slices.Sort(result)
	return result
}

// Difference returns the list of resources resulting from a-b, deduped and sorted
func Difference(a []corev1.ResourceName, b []corev1.ResourceName) []corev1.ResourceName {
	result := make([]corev1.ResourceName, 0, len(a))
	for _, item := range a {
		if Contains(b, item) || Contains(result, item) {
			continue
		}
		result = append(result, item)
	}
	slices.Sort(result)
	return result
}

// IsZero returns true if each key maps to the quantity value 0
func IsZero(a corev1.ResourceList) bool {
	zero := resource.MustParse("0")
	for _, v := range a {
		if v.Cmp(zero) != 0 {
			return false
		}
	}
	return true
}

// RemoveZeros returns a new resource list that only has no zero values
// 注意：直接复用 Quantity 值而非深拷贝，调用方不应修改返回的条目。
func RemoveZeros(a corev1.ResourceList) corev1.ResourceList {
	result := corev1.ResourceList{}
	for key, value := range a {
		if !value.IsZero() {
			result[key] = value
		}
	}
	return result
}

// IsNegative returns the set of resource names that have a negative value.
// 负用量意味着控制器记账出错，调用方据此拒绝更新而不是把负值写回状态。
func IsNegative(a corev1.ResourceList) []corev1.ResourceName {
	results := []corev1.ResourceName{}
	zero := resource.MustParse("0")
	for k, v := range a {
		if v.Cmp(zero) < 0 {
			results = append(results, k)
		}
	}
	return results
}

// ToSet takes a list of resource names and converts to a string set
func ToSet(resourceNames []corev1.ResourceName) sets.String {
	result := sets.NewString()
	for _, resourceName := range resourceNames {
		result.Insert(string(resourceName))
	}
	return result
}

// CalculateUsage calculates and returns the requested ResourceList usage.
// If an error is returned, usage only contains the resources which encountered no calculation errors.
//
// 流程：先求 hardLimits 与各 evaluator 可统计资源的交集，再逐个 evaluator 汇总用量；
// 某个 evaluator 失败时将其资源从结果中剔除并记录错误，保证返回的用量不会因部分失败而偏小。
func CalculateUsage(namespaceName string, scopes []corev1.ResourceQuotaScope, hardLimits corev1.ResourceList, registry Registry, scopeSelector *corev1.ScopeSelector) (corev1.ResourceList, error) {
	// find the intersection between the hard resources on the quota
	// and the resources this controller can track to know what we can
	// look to measure updated usage stats for
	hardResources := ResourceNames(hardLimits)
	potentialResources := []corev1.ResourceName{}
	evaluators := registry.List()
	for _, evaluator := range evaluators {
		potentialResources = append(potentialResources, evaluator.MatchingResources(hardResources)...)
	}
	// NOTE: the intersection just removes duplicates since the evaluator match intersects with hard
	matchedResources := Intersection(hardResources, potentialResources)

	errors := []error{}

	// sum the observed usage from each evaluator
	newUsage := corev1.ResourceList{}
	for _, evaluator := range evaluators {
		// only trigger the evaluator if it matches a resource in the quota, otherwise, skip calculating anything
		intersection := evaluator.MatchingResources(matchedResources)
		if len(intersection) == 0 {
			continue
		}

		usageStatsOptions := UsageStatsOptions{Namespace: namespaceName, Scopes: scopes, Resources: intersection, ScopeSelector: scopeSelector}
		stats, err := evaluator.UsageStats(usageStatsOptions)
		if err != nil {
			// remember the error
			errors = append(errors, err)
			// exclude resources which encountered calculation errors
			// 剔除失败资源，避免把它们的用量当作 0 返回。
			matchedResources = Difference(matchedResources, intersection)
			continue
		}
		newUsage = Add(newUsage, stats.Used)
	}

	// mask the observed usage to only the set of resources tracked by this quota
	// merge our observed usage with the quota usage status
	// if the new usage is different than the last usage, we will need to do an update
	newUsage = Mask(newUsage, matchedResources)
	return newUsage, utilerrors.NewAggregate(errors)
}
