/*
Copyright 2025 The Kubernetes Authors.

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

package architecture

import (
	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// compareObjects 检查所有预期字段都按预期设置。
// 实际对象可能有额外字段，其值会被忽略。
func compareObjects(expected, actual *unstructured.Unstructured) string {
	diff := cmp.Diff(expected.Object, actual.Object,
		// 预期对象中没有的字段可以忽略。
		// 只需比较存在的字段。
		//
		// 也许（？）更简单的方法是裁剪实际对象，
		// 再用 go-cmp 比较。但让 go-cmp 忽略字段的好处是
		// 这些字段会在 diff 中显示为截断（"..."），
		// 这样更准确一些。
		cmp.FilterPath(func(path cmp.Path) bool {
			return fieldIsMissing(expected.Object, path)
		}, cmp.Ignore()),
	)
	return diff
}

// fieldIsMissing 返回 path 标识的字段是否
// 不在对象中。它沿 path 递归下降，并检查沿途
// 对象的相应内容。
func fieldIsMissing(obj map[string]any, path cmp.Path) bool {
	// 第一项是空操作。
	missing := fieldIsMissingStep(obj, path[1:])
	// 取消注释以调试...
	// fmt.Printf("fieldIsMissing: %s %v\n", path.GoString(), missing)
	return missing
}

func fieldIsMissingStep(value any, path []cmp.PathStep) bool {
	if len(path) == 0 {
		// 完成，已检查完整路径。
		return false
	}
	// 只需对某些查找步骤下降，
	// 其余一律视为"不缺失"，
	// 因而参与比较。
	switch pathElement := path[0].(type) {
	case cmp.MapIndex:
		key := pathElement.Key().String()
		value, ok := value.(map[string]any)
		if !ok {
			// 类型不匹配。
			return false
		}
		entry, found := value[key]
		if !found {
			return true
		}
		return fieldIsMissingStep(entry, path[1:])
	case cmp.SliceIndex:
		key := pathElement.Key()
		value, ok := value.([]any)
		if !ok {
			// 类型不匹配。
			return false
		}
		if key < 0 {
			// 不知道为什么 go-cmp 会使用负索引，直接比较它。
			return false
		}
		if key >= len(value) {
			// 切片更小 -> 缺少条目。
			return true
		}
		entry := value[key]
		return fieldIsMissingStep(entry, path[1:])
	case cmp.TypeAssertion:
		// 实际值的类型会在需要时检查，
		// 这里跳过断言。
		return fieldIsMissingStep(value, path[1:])
	default:
		return false
	}
}
