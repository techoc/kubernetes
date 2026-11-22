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

// Package taints 提供处理污点（taint）的工具函数。
package taints

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/kubernetes/pkg/apis/core/helper"
)

// 污点操作结果的类型常量。
const (
	MODIFIED  = "modified"
	TAINTED   = "tainted"
	UNTAINTED = "untainted"
)

// parseTaint 从字符串中解析污点，格式必须为
// '<key>=<value>:<effect>'、'<key>:<effect>' 或 '<key>' 之一。
func parseTaint(st string) (v1.Taint, error) {
	var taint v1.Taint

	var key string
	var value string
	var effect v1.TaintEffect

	parts := strings.Split(st, ":")
	switch len(parts) {
	case 1:
		key = parts[0]
	case 2:
		effect = v1.TaintEffect(parts[1])
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

// validateTaintEffect 校验污点效果是否为受支持的取值。
func validateTaintEffect(effect v1.TaintEffect) error {
	if effect != v1.TaintEffectNoSchedule && effect != v1.TaintEffectPreferNoSchedule && effect != v1.TaintEffectNoExecute {
		return fmt.Errorf("invalid taint effect: %v, unsupported taint effect", effect)
	}

	return nil
}

// ParseTaints 接受字符串数组形式的 spec，生成待添加和待删除污点的切片，并校验 spec。
// 例如 `<key>` 形式只能用于删除污点，不能用于添加。
func ParseTaints(spec []string) ([]v1.Taint, []v1.Taint, error) {
	var taints, taintsToRemove []v1.Taint
	uniqueTaints := map[v1.TaintEffect]sets.String{}

	for _, taintSpec := range spec {
		if strings.HasSuffix(taintSpec, "-") {
			// 以 "-" 结尾表示删除污点，此时无需指定 value。
			taintToRemove, err := parseTaint(strings.TrimSuffix(taintSpec, "-"))
			if err != nil {
				return nil, nil, err
			}
			taintsToRemove = append(taintsToRemove, v1.Taint{Key: taintToRemove.Key, Effect: taintToRemove.Effect})
		} else {
			newTaint, err := parseTaint(taintSpec)
			if err != nil {
				return nil, nil, err
			}
			// 添加污点必须指定 effect。
			if len(newTaint.Effect) == 0 {
				return nil, nil, fmt.Errorf("invalid taint spec: %v", taintSpec)
			}
			// 按 <key, effect> 校验污点唯一性。
			if len(uniqueTaints[newTaint.Effect]) > 0 && uniqueTaints[newTaint.Effect].Has(newTaint.Key) {
				return nil, nil, fmt.Errorf("duplicated taints with the same key and effect: %v", newTaint)
			}
			// 记录已添加的污点，供后续重复检查使用。
			if len(uniqueTaints[newTaint.Effect]) == 0 {
				uniqueTaints[newTaint.Effect] = sets.String{}
			}
			uniqueTaints[newTaint.Effect].Insert(newTaint.Key)

			taints = append(taints, newTaint)
		}
	}
	return taints, taintsToRemove, nil
}

// CheckIfTaintsAlreadyExists 检查节点上是否已存在待添加的污点，返回以逗号分隔的污点 key 列表。
func CheckIfTaintsAlreadyExists(oldTaints []v1.Taint, taints []v1.Taint) string {
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

// DeleteTaintsByKey 删除所有 key 与给定 taintKey 相同的污点。
func DeleteTaintsByKey(taints []v1.Taint, taintKey string) ([]v1.Taint, bool) {
	newTaints := []v1.Taint{}
	deleted := false
	for i := range taints {
		if taintKey == taints[i].Key {
			deleted = true
			continue
		}
		newTaints = append(newTaints, taints[i])
	}
	return newTaints, deleted
}

// DeleteTaint 删除所有 key 和 effect 与给定 taintToDelete 相同的污点。
func DeleteTaint(taints []v1.Taint, taintToDelete *v1.Taint) ([]v1.Taint, bool) {
	newTaints := []v1.Taint{}
	deleted := false
	for i := range taints {
		if taintToDelete.MatchTaint(&taints[i]) {
			deleted = true
			continue
		}
		newTaints = append(newTaints, taints[i])
	}
	return newTaints, deleted
}

// RemoveTaint 尝试从节点污点列表中移除指定污点。
// 若有变更则返回更新后节点的副本和 true，否则返回 false。
func RemoveTaint(node *v1.Node, taint *v1.Taint) (*v1.Node, bool, error) {
	// 深拷贝避免修改调用方的对象。
	newNode := node.DeepCopy()
	nodeTaints := newNode.Spec.Taints
	if len(nodeTaints) == 0 {
		return newNode, false, nil
	}

	if !TaintExists(nodeTaints, taint) {
		return newNode, false, nil
	}

	newTaints, _ := DeleteTaint(nodeTaints, taint)
	newNode.Spec.Taints = newTaints
	return newNode, true, nil
}

// AddOrUpdateTaint 尝试向节点的污点列表添加或更新指定污点。
// 若有变更则返回更新后节点的副本和 true，否则返回 false。
func AddOrUpdateTaint(node *v1.Node, taint *v1.Taint) (*v1.Node, bool, error) {
	// 深拷贝避免修改调用方的对象。
	newNode := node.DeepCopy()
	nodeTaints := newNode.Spec.Taints

	var newTaints []v1.Taint
	updated := false
	for i := range nodeTaints {
		if taint.MatchTaint(&nodeTaints[i]) {
			// 已存在且完全相同，无需变更。
			if helper.Semantic.DeepEqual(*taint, nodeTaints[i]) {
				return newNode, false, nil
			}
			newTaints = append(newTaints, *taint)
			updated = true
			continue
		}

		newTaints = append(newTaints, nodeTaints[i])
	}

	// 没有匹配项说明是新污点，直接追加。
	if !updated {
		newTaints = append(newTaints, *taint)
	}

	newNode.Spec.Taints = newTaints
	return newNode, true, nil
}

// TaintExists 检查给定污点是否存在于污点列表中。
func TaintExists(taints []v1.Taint, taintToFind *v1.Taint) bool {
	for _, taint := range taints {
		if taint.MatchTaint(taintToFind) {
			return true
		}
	}
	return false
}

// TaintKeyExists 检查给定污点 key 是否存在于污点列表中。
func TaintKeyExists(taints []v1.Taint, taintKeyToMatch string) bool {
	for _, taint := range taints {
		if taint.Key == taintKeyToMatch {
			return true
		}
	}
	return false
}

// TaintSetDiff 求两个污点切片的差异，返回新切片相对于旧切片新增和删除的元素。示例：
// 输入：taintsNew=[a b]，taintsOld=[a c]
// 输出：taintsToAdd=[b]，taintsToRemove=[c]
func TaintSetDiff(taintsNew, taintsOld []v1.Taint) (taintsToAdd []*v1.Taint, taintsToRemove []*v1.Taint) {
	for _, taint := range taintsNew {
		if !TaintExists(taintsOld, &taint) {
			t := taint
			taintsToAdd = append(taintsToAdd, &t)
		}
	}

	for _, taint := range taintsOld {
		if !TaintExists(taintsNew, &taint) {
			t := taint
			taintsToRemove = append(taintsToRemove, &t)
		}
	}

	return
}

// TaintSetFilter 按给定函数 fn 过滤污点切片，返回满足条件的污点。
func TaintSetFilter(taints []v1.Taint, fn func(*v1.Taint) bool) []v1.Taint {
	res := []v1.Taint{}

	for _, taint := range taints {
		if fn(&taint) {
			res = append(res, taint)
		}
	}

	return res
}

// CheckTaintValidation 校验给定污点是否合法，非法时返回错误。
func CheckTaintValidation(taint v1.Taint) error {
	if errs := validation.IsQualifiedName(taint.Key); len(errs) > 0 {
		return fmt.Errorf("invalid taint key: %s", strings.Join(errs, "; "))
	}
	if taint.Value != "" {
		if errs := validation.IsValidLabelValue(taint.Value); len(errs) > 0 {
			return fmt.Errorf("invalid taint value: %s", strings.Join(errs, "; "))
		}
	}
	if taint.Effect != "" {
		if err := validateTaintEffect(taint.Effect); err != nil {
			return err
		}
	}

	return nil
}
