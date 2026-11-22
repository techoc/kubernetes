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

package labels

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CloneAndAddLabel 克隆给定的 map，并加入给定的 key 和 value 后返回。
// 若 labelKey 为空则直接返回原 map。
func CloneAndAddLabel(labels map[string]string, labelKey, labelValue string) map[string]string {
	if labelKey == "" {
		// 无需添加标签。
		return labels
	}
	// 克隆原 map，避免修改调用方的数据。
	newLabels := map[string]string{}
	for key, value := range labels {
		newLabels[key] = value
	}
	newLabels[labelKey] = labelValue
	return newLabels
}

// CloneAndRemoveLabel 克隆给定的 map，并删除给定的 key 后返回。
// 若 labelKey 为空则直接返回原 map。
func CloneAndRemoveLabel(labels map[string]string, labelKey string) map[string]string {
	if labelKey == "" {
		// 无需删除标签。
		return labels
	}
	// 克隆原 map，避免修改调用方的数据。
	newLabels := map[string]string{}
	for key, value := range labels {
		newLabels[key] = value
	}
	delete(newLabels, labelKey)
	return newLabels
}

// AddLabel 在给定的 map 中加入给定的 key 和 value 并返回。
// 会原地修改传入的 map；若 labelKey 为空则直接返回原 map。
func AddLabel(labels map[string]string, labelKey, labelValue string) map[string]string {
	if labelKey == "" {
		// 无需添加标签。
		return labels
	}
	if labels == nil {
		labels = make(map[string]string)
	}
	labels[labelKey] = labelValue
	return labels
}

// CloneSelectorAndAddLabel 克隆给定的 selector，并加入给定的 key 和 value 后返回。
// 若 labelKey 为空则直接返回原 selector。
func CloneSelectorAndAddLabel(selector *metav1.LabelSelector, labelKey, labelValue string) *metav1.LabelSelector {
	if labelKey == "" {
		// 无需添加标签。
		return selector
	}

	// 克隆 selector。
	newSelector := new(metav1.LabelSelector)

	// TODO(madhusudancs): 检查这里能否改用 deepCopy_extensions_LabelSelector。
	newSelector.MatchLabels = make(map[string]string)
	if selector.MatchLabels != nil {
		for key, val := range selector.MatchLabels {
			newSelector.MatchLabels[key] = val
		}
	}
	newSelector.MatchLabels[labelKey] = labelValue

	if selector.MatchExpressions != nil {
		newMExps := make([]metav1.LabelSelectorRequirement, len(selector.MatchExpressions))
		for i, me := range selector.MatchExpressions {
			newMExps[i].Key = me.Key
			newMExps[i].Operator = me.Operator
			if me.Values != nil {
				newMExps[i].Values = make([]string, len(me.Values))
				copy(newMExps[i].Values, me.Values)
			} else {
				newMExps[i].Values = nil
			}
		}
		newSelector.MatchExpressions = newMExps
	} else {
		newSelector.MatchExpressions = nil
	}

	return newSelector
}

// AddLabelToSelector 在给定 selector 的 MatchLabels 中加入 key 和 value 并返回。
// 会原地修改传入的 selector；若 labelKey 为空则直接返回原 selector。
func AddLabelToSelector(selector *metav1.LabelSelector, labelKey, labelValue string) *metav1.LabelSelector {
	if labelKey == "" {
		// 无需添加标签。
		return selector
	}
	if selector.MatchLabels == nil {
		selector.MatchLabels = make(map[string]string)
	}
	selector.MatchLabels[labelKey] = labelValue
	return selector
}

// SelectorHasLabel 检查给定 selector 的 MatchLabels 是否包含给定的 label key。
func SelectorHasLabel(selector *metav1.LabelSelector, labelKey string) bool {
	return len(selector.MatchLabels[labelKey]) > 0
}
