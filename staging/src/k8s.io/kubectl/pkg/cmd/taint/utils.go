/*
Copyright 2018 The Kubernetes Authors.

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

// Package taints 提供处理 taint 的工具函数
package taint

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
)

// 导出的 taint 常量字符串
const (
	MODIFIED  = "modified"
	TAINTED   = "tainted"
	UNTAINTED = "untainted"
)

// parseTaints 接收一个数组形式的 spec，创建待添加 taint 和待删除 taint 的切片。
// 它还会校验 spec。例如，`<key>` 形式可用于删除 taint，但不能用于添加 taint。
func parseTaints(spec []string) ([]corev1.Taint, []corev1.Taint, error) {
	var taints, taintsToRemove []corev1.Taint
	uniqueTaints := map[corev1.TaintEffect]sets.Set[string]{}

	for _, taintSpec := range spec {
		if strings.HasSuffix(taintSpec, "-") {
			taintToRemove, err := parseTaint(strings.TrimSuffix(taintSpec, "-"))
			if err != nil {
				return nil, nil, err
			}
			taintsToRemove = append(taintsToRemove, corev1.Taint{Key: taintToRemove.Key, Effect: taintToRemove.Effect})
		} else {
			newTaint, err := parseTaint(taintSpec)
			if err != nil {
				return nil, nil, err
			}
			// 校验 taint 是否带有 effect，添加 taint 时必须指定 effect
			if len(newTaint.Effect) == 0 {
				return nil, nil, fmt.Errorf("invalid taint spec: %v", taintSpec)
			}
			// 按 <key, effect> 校验 taint 是否唯一
			if len(uniqueTaints[newTaint.Effect]) > 0 && uniqueTaints[newTaint.Effect].Has(newTaint.Key) {
				return nil, nil, fmt.Errorf("duplicated taints with the same key and effect: %v", newTaint)
			}
			// 将 taint 加入现有 taint 集合，以便进行唯一性检查
			if len(uniqueTaints[newTaint.Effect]) == 0 {
				uniqueTaints[newTaint.Effect] = sets.Set[string]{}
			}
			uniqueTaints[newTaint.Effect].Insert(newTaint.Key)

			taints = append(taints, newTaint)
		}
	}
	return taints, taintsToRemove, nil
}

// parseTaint 从字符串中解析 taint，字符串格式必须为
// '<key>=<value>:<effect>'、'<key>:<effect>' 或 '<key>' 之一。
func parseTaint(st string) (corev1.Taint, error) {
	var taint corev1.Taint

	var key string
	var value string
	var effect corev1.TaintEffect

	parts := strings.Split(st, ":")
	switch len(parts) {
	case 1:
		key = parts[0]
	case 2:
		effect = corev1.TaintEffect(parts[1])
		if err := validateTaintEffect(effect); err != nil {
			return taint, err
		}

		partsKV := strings.Split(parts[0], "=")
		if len(partsKV) > 2 {
			return taint, fmt.Errorf("invalid taint spec: %v", st)
		}
		key = partsKV[0]
		if len(partsKV) == 2 {
			value = partsKV[1]
			if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
				return taint, fmt.Errorf("invalid taint spec: %v, %s", st, strings.Join(errs, "; "))
			}
		}
	default:
		return taint, fmt.Errorf("invalid taint spec: %v", st)
	}

	if errs := validation.IsQualifiedName(key); len(errs) > 0 {
		return taint, fmt.Errorf("invalid taint spec: %v, %s", st, strings.Join(errs, "; "))
	}

	taint.Key = key
	taint.Value = value
	taint.Effect = effect

	return taint, nil
}

func validateTaintEffect(effect corev1.TaintEffect) error {
	if effect != corev1.TaintEffectNoSchedule && effect != corev1.TaintEffectPreferNoSchedule && effect != corev1.TaintEffectNoExecute {
		return fmt.Errorf("invalid taint effect: %v, unsupported taint effect", effect)
	}

	return nil
}

// reorganizeTaints 返回更新后的 taint 集合，综合考虑未更新的旧 taint、
// 已更新的旧 taint、已删除的旧 taint 以及新增的 taint。
func reorganizeTaints(node *corev1.Node, overwrite bool, taintsToAdd []corev1.Taint, taintsToRemove []corev1.Taint) (string, []corev1.Taint, error) {
	newTaints := append([]corev1.Taint{}, taintsToAdd...)
	oldTaints := node.Spec.Taints
	// 将已存在但未更新的 taint 加入 newTaints
	added := addTaints(oldTaints, &newTaints)
	allErrs, deleted := deleteTaints(taintsToRemove, &newTaints)
	if (added && deleted) || overwrite {
		return MODIFIED, newTaints, utilerrors.NewAggregate(allErrs)
	} else if added {
		return TAINTED, newTaints, utilerrors.NewAggregate(allErrs)
	}
	return UNTAINTED, newTaints, utilerrors.NewAggregate(allErrs)
}

// deleteTaints 从节点的 taint 列表中删除给定的 taint。
func deleteTaints(taintsToRemove []corev1.Taint, newTaints *[]corev1.Taint) ([]error, bool) {
	allErrs := []error{}
	var removed bool
	for _, taintToRemove := range taintsToRemove {
		if len(taintToRemove.Effect) > 0 {
			*newTaints, removed = deleteTaint(*newTaints, &taintToRemove)
		} else {
			*newTaints, removed = deleteTaintsByKey(*newTaints, taintToRemove.Key)
		}
		if !removed {
			allErrs = append(allErrs, fmt.Errorf("taint %q not found", taintToRemove.ToString()))
		}
	}
	return allErrs, removed
}

// addTaints 将 newTaints 列表加入现有 taint 列表，并更新 newTaints 列表。
// TODO: 需要重写此函数，使其只接收新值，而不是追加到 newTaints 列表，以保持一致。
func addTaints(oldTaints []corev1.Taint, newTaints *[]corev1.Taint) bool {
	for _, oldTaint := range oldTaints {
		existsInNew := false
		for _, taint := range *newTaints {
			if taint.MatchTaint(&oldTaint) {
				existsInNew = true
				break
			}
		}
		if !existsInNew {
			*newTaints = append(*newTaints, oldTaint)
		}
	}
	return len(oldTaints) != len(*newTaints)
}

// checkIfTaintsAlreadyExists 检查节点上是否已存在我们要添加的 taint，并返回由 taint key 组成的字符串。
func checkIfTaintsAlreadyExists(oldTaints []corev1.Taint, taints []corev1.Taint) string {
	var existingTaintList = make([]string, 0)
	for _, taint := range taints {
		for _, oldTaint := range oldTaints {
			if taint.Key == oldTaint.Key && taint.Effect == oldTaint.Effect {
				existingTaintList = append(existingTaintList, taint.Key)
			}
		}
	}
	return strings.Join(existingTaintList, ",")
}

// deleteTaintsByKey 删除所有 key 与给定 taintKey 相同的 taint
func deleteTaintsByKey(taints []corev1.Taint, taintKey string) ([]corev1.Taint, bool) {
	newTaints := []corev1.Taint{}
	for i := range taints {
		if taintKey == taints[i].Key {
			continue
		}
		newTaints = append(newTaints, taints[i])
	}
	return newTaints, len(taints) != len(newTaints)
}

// deleteTaint 删除所有 key 和 effect 与给定 taintToDelete 相同的 taint。
func deleteTaint(taints []corev1.Taint, taintToDelete *corev1.Taint) ([]corev1.Taint, bool) {
	newTaints := []corev1.Taint{}
	for i := range taints {
		if taintToDelete.MatchTaint(&taints[i]) {
			continue
		}
		newTaints = append(newTaints, taints[i])
	}
	return newTaints, len(taints) != len(newTaints)
}
