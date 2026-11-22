/*
Copyright 2020 The Kubernetes Authors.

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

package meta

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetStatusCondition 将 conditions 中对应类型的条件更新为 newCondition，并返回本次调用是否改变了条件。
// conditions 必须非 nil（nil 时直接返回 false，不 panic）。
// 两种情形：
//  1. 指定类型的条件已存在：用 newCondition 更新其所有字段；仅当 Status 发生变化时，
//     才把 LastTransitionTime 设为当前时间（或采用 newCondition 显式给出的时间）——
//     这样"状态未变"的重复写入不会不断刷新转换时间，避免触发无意义的事件与 watch。
//  2. 指定类型的条件不存在：若 LastTransitionTime 为空则补为当前时间，然后追加 newCondition。
func SetStatusCondition(conditions *[]metav1.Condition, newCondition metav1.Condition) (changed bool) {
	if conditions == nil {
		return false
	}
	existingCondition := FindStatusCondition(*conditions, newCondition.Type)
	if existingCondition == nil {
		// 新条件必须带上 LastTransitionTime，否则下游按时间排序/判定的逻辑会失真。
		if newCondition.LastTransitionTime.IsZero() {
			newCondition.LastTransitionTime = metav1.NewTime(time.Now())
		}
		*conditions = append(*conditions, newCondition)
		return true
	}

	// 只有 Status 变化才算一次真正的状态转换，并据此刷新 LastTransitionTime。
	if existingCondition.Status != newCondition.Status {
		existingCondition.Status = newCondition.Status
		// 调用方显式提供了转换时间就尊重它（用于回放/测试），否则取当前时间。
		if !newCondition.LastTransitionTime.IsZero() {
			existingCondition.LastTransitionTime = newCondition.LastTransitionTime
		} else {
			existingCondition.LastTransitionTime = metav1.NewTime(time.Now())
		}
		changed = true
	}

	// Reason/Message/ObservedGeneration 的变化只需标记 changed，不影响 LastTransitionTime 语义。
	if existingCondition.Reason != newCondition.Reason {
		existingCondition.Reason = newCondition.Reason
		changed = true
	}
	if existingCondition.Message != newCondition.Message {
		existingCondition.Message = newCondition.Message
		changed = true
	}
	if existingCondition.ObservedGeneration != newCondition.ObservedGeneration {
		existingCondition.ObservedGeneration = newCondition.ObservedGeneration
		changed = true
	}

	return changed
}

// RemoveStatusCondition 从 conditions 中移除指定 conditionType 的条件（若存在），
// 返回是否确实移除过。conditions 必须非 nil。
// 实现上总是重建切片，而不是原地移动元素，避免调用方持有的其他切片别名被意外修改。
func RemoveStatusCondition(conditions *[]metav1.Condition, conditionType string) (removed bool) {
	if conditions == nil || len(*conditions) == 0 {
		return false
	}
	// 预先按"最多少一个元素"分配容量；即使目标不存在也只是少用一点容量。
	newConditions := make([]metav1.Condition, 0, len(*conditions)-1)
	for _, condition := range *conditions {
		if condition.Type != conditionType {
			newConditions = append(newConditions, condition)
		}
	}

	// 长度变化即说明有元素被过滤掉；没有变化时保留原内容。
	removed = len(*conditions) != len(newConditions)
	*conditions = newConditions

	return removed
}

// FindStatusCondition 在 conditions 中查找指定类型的条件，返回指向切片元素的指针（便于原地修改），
// 未找到时返回 nil。注意：返回的指针指向切片底层数组，切片被扩容或重新赋值后会失效。
func FindStatusCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}

	return nil
}

// IsStatusConditionTrue 在指定条件存在且为 metav1.ConditionTrue 时返回 true。
func IsStatusConditionTrue(conditions []metav1.Condition, conditionType string) bool {
	return IsStatusConditionPresentAndEqual(conditions, conditionType, metav1.ConditionTrue)
}

// IsStatusConditionFalse 在指定条件存在且为 metav1.ConditionFalse 时返回 true。
func IsStatusConditionFalse(conditions []metav1.Condition, conditionType string) bool {
	return IsStatusConditionPresentAndEqual(conditions, conditionType, metav1.ConditionFalse)
}

// IsStatusConditionPresentAndEqual 在指定条件存在且其 Status 等于给定值时返回 true；
// 条件不存在与状态不等都返回 false。
func IsStatusConditionPresentAndEqual(conditions []metav1.Condition, conditionType string, status metav1.ConditionStatus) bool {
	for _, condition := range conditions {
		if condition.Type == conditionType {
			return condition.Status == status
		}
	}
	return false
}
