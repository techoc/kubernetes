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

// Clones the given selector and returns a new selector with the given key and value added.
// Returns the given selector, if labelKey is empty.
// 中文补充：先深拷贝再修改，避免修改调用方传入的对象；labelKey 为空表示不需要加标签，
// 此时直接返回原对象而不拷贝。
func CloneSelectorAndAddLabel(selector *LabelSelector, labelKey, labelValue string) *LabelSelector {
	if labelKey == "" {
		// Don't need to add a label.
		return selector
	}

	// Clone.
	newSelector := selector.DeepCopy()

	if newSelector.MatchLabels == nil {
		newSelector.MatchLabels = make(map[string]string)
	}

	newSelector.MatchLabels[labelKey] = labelValue

	return newSelector
}

// AddLabelToSelector returns a selector with the given key and value added to the given selector's MatchLabels.
// 中文补充：就地修改传入的 selector（与 CloneSelectorAndAddLabel 不同），
// 调用方需自行确认可以修改原对象。
func AddLabelToSelector(selector *LabelSelector, labelKey, labelValue string) *LabelSelector {
	if labelKey == "" {
		// Don't need to add a label.
		return selector
	}
	if selector.MatchLabels == nil {
		selector.MatchLabels = make(map[string]string)
	}
	selector.MatchLabels[labelKey] = labelValue
	return selector
}

// SelectorHasLabel checks if the given selector contains the given label key in its MatchLabels
// 中文补充：只判断取值非空，因此 key 存在但值为空字符串时返回 false。
func SelectorHasLabel(selector *LabelSelector, labelKey string) bool {
	return len(selector.MatchLabels[labelKey]) > 0
}
