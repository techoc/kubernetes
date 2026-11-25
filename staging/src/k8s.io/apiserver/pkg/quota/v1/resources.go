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

// Equals 在两个列表等价时返回 true。
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

// LessThanOrEqual 在 a 中每个出现在 b 的资源都不超过 b 时返回 true；
// 否则返回 a 中超出 b 的资源名。
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

// Max 对每个指定资源返回 Max(a, b) 的结果。
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

// Add 对每个指定资源返回 a + b 的结果。
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

// SubtractWithNonNegativeResult 返回 a - b 的结果，并将负值归零，避免出现负资源用量。
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

// Subtract 对每个指定资源返回 a - b 的结果。
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

// Mask 返回仅包含指定资源名及其值的新资源列表。
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

// ResourceNames 返回资源列表中的所有资源名。
// 顺序不保证稳定（来自 map 迭代），需要确定性时调用方应自行排序。
func ResourceNames(resources corev1.ResourceList) []corev1.ResourceName {
	result := []corev1.ResourceName{}
	for resourceName := range resources {
		result = append(result, resourceName)
	}
	return result
}

// Contains 在列表包含指定对象时返回 true。
func Contains(items []corev1.ResourceName, item corev1.ResourceName) bool {
	for _, i := range items {
		if i == item {
			return true
		}
	}
	return false
}

// ContainsPrefix 在对象以给定前缀集合中的任一前缀开头时返回 true。
// 常用于判断 count/<resource> 一类带前缀的扩展资源。
func ContainsPrefix(prefixSet []string, item corev1.ResourceName) bool {
	for _, prefix := range prefixSet {
		if strings.HasPrefix(string(item), prefix) {
			return true
		}
	}
	return false
}

// Intersection 返回两个资源列表的交集，并去重和排序。
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

// Difference 返回 a - b 得到的资源列表，并去重和排序。
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

// IsZero 在每个资源名的数量均为零时返回 true。
func IsZero(a corev1.ResourceList) bool {
	zero := resource.MustParse("0")
	for _, v := range a {
		if v.Cmp(zero) != 0 {
			return false
		}
	}
	return true
}

// RemoveZeros 返回仅包含非零值的新资源列表。
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

// IsNegative 返回数量为负的资源名集合。
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

// ToSet 把资源名列表转换为字符串集合。
func ToSet(resourceNames []corev1.ResourceName) sets.String {
	result := sets.NewString()
	for _, resourceName := range resourceNames {
		result.Insert(string(resourceName))
	}
	return result
}

// CalculateUsage 计算并返回请求的资源列表用量。
// 返回错误时，用量中仅包含未遇到计算错误的资源。
//
// 流程：先求 hardLimits 与各 evaluator 可统计资源的交集，再逐个 evaluator 汇总用量；
// 某个 evaluator 失败时将其资源从结果中剔除并记录错误，保证返回的用量不会因部分失败而偏小。
func CalculateUsage(namespaceName string, scopes []corev1.ResourceQuotaScope, hardLimits corev1.ResourceList, registry Registry, scopeSelector *corev1.ScopeSelector) (corev1.ResourceList, error) {
	// 求配额硬限制与控制器可跟踪资源的交集，确定需要重新测量的用量。
	hardResources := ResourceNames(hardLimits)
	potentialResources := []corev1.ResourceName{}
	evaluators := registry.List()
	for _, evaluator := range evaluators {
		potentialResources = append(potentialResources, evaluator.MatchingResources(hardResources)...)
	}
	// 注意：求交集同时会去重，因为评估器的匹配结果已与硬限制取过交集。
	matchedResources := Intersection(hardResources, potentialResources)

	errors := []error{}

	// 汇总每个评估器观察到的用量。
	newUsage := corev1.ResourceList{}
	for _, evaluator := range evaluators {
		// 评估器未匹配配额中的任何资源时跳过，避免无谓计算。
		intersection := evaluator.MatchingResources(matchedResources)
		if len(intersection) == 0 {
			continue
		}

		usageStatsOptions := UsageStatsOptions{Namespace: namespaceName, Scopes: scopes, Resources: intersection, ScopeSelector: scopeSelector}
		stats, err := evaluator.UsageStats(usageStatsOptions)
		if err != nil {
			// 记录错误。
			errors = append(errors, err)
			// 剔除遇到计算错误的资源。
			// 剔除失败资源，避免把它们的用量当作 0 返回。
			matchedResources = Difference(matchedResources, intersection)
			continue
		}
		newUsage = Add(newUsage, stats.Used)
	}

	// 将观察到的用量裁剪到此配额跟踪的资源集合。
	newUsage = Mask(newUsage, matchedResources)
	return newUsage, utilerrors.NewAggregate(errors)
}
