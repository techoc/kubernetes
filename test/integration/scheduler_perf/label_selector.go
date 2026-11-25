/*
Copyright 2023 The Kubernetes Authors.

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

package benchmark

// enabled 检查标签过滤器，其行为与 GitHub 中的一致：
// - 空字符串表示启用
// - 各个标签以逗号分隔
// - [+]<label> 表示工作负载必须带有该标签
// - -<label> 表示工作负载不得带有该标签
func enabled(labelFilters []string, labels ...string) bool {
	for _, label := range labelFilters {
		if label == "" {
			continue
		}
		mustHaveLabel := label[0] != '-'
		if label[0] == '-' || label[0] == '+' {
			label = label[1:]
		}
		haveLabel := containsStr(labels, label)
		if haveLabel != mustHaveLabel {
			return false
		}
	}
	return true
}

func containsStr(hay []string, needle string) bool {
	for _, item := range hay {
		if item == needle {
			return true
		}
	}
	return false
}
