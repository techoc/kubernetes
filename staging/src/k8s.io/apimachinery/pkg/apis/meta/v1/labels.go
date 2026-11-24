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

// CloneSelectorAndAddLabel 深拷贝 selector，并在副本的 MatchLabels 中添加指定键值对。
// labelKey 为空时无需添加标签，直接返回原 selector。
// 中文补充：先深拷贝再修改，避免修改调用方传入的对象；labelKey 为空表示不需要加标签，
// 此时直接返回原对象而不拷贝。
func CloneSelectorAndAddLabel(selector *LabelSelector, labelKey, labelValue string) *LabelSelector {
	if labelKey == "" {
		// labelKey 为空，无需修改 selector。
		return selector
	}

	// 深拷贝，避免修改调用方持有的对象。
	newSelector := selector.DeepCopy()

	if newSelector.MatchLabels == nil {
		newSelector.MatchLabels = make(map[string]string)
	}

	newSelector.MatchLabels[labelKey] = labelValue

	return newSelector
}

// AddLabelToSelector 在传入 selector 的 MatchLabels 中添加指定键值对，并返回同一个 selector。
// 中文补充：就地修改传入的 selector（与 CloneSelectorAndAddLabel 不同），
// 调用方需自行确认可以修改原对象。
func AddLabelToSelector(selector *LabelSelector, labelKey, labelValue string) *LabelSelector {
	if labelKey == "" {
		// labelKey 为空，无需修改 selector。
		return selector
	}
	if selector.MatchLabels == nil {
		selector.MatchLabels = make(map[string]string)
	}
	selector.MatchLabels[labelKey] = labelValue
	return selector
}

// SelectorHasLabel 判断 selector 的 MatchLabels 中指定 key 对应的值是否非空。
// 中文补充：只判断取值非空，因此 key 存在但值为空字符串时返回 false。
func SelectorHasLabel(selector *LabelSelector, labelKey string) bool {
	return len(selector.MatchLabels[labelKey]) > 0
}
