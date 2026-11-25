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

package image

import "regexp"

var (
	// tagMatcher 是匹配标签的正则表达式。
	// 假定镜像格式为 `[domain][:port][path]<name>[:tag][@sha256:digest]`。
	// 为正确匹配标签，也匹配可能存在的摘要；标签前所有内容由一个非贪婪组匹配。
	// 除标签组外的匹配子组均丢弃，因此 FindStringSubmatch 返回匹配时，标签位于第二个元素（完整匹配之后）。
	tagMatcher = regexp.MustCompile(`^(?U:.*)(?::([[:word:]][[:word:].-]*))?(?:@sha256:[a-fA-F\d]{64})?$`)
)

// TagFromImage 从镜像提取标签；未找到时返回空字符串。
func TagFromImage(image string) string {
	matches := tagMatcher.FindStringSubmatch(image)
	if len(matches) >= 2 {
		return matches[1]
	}
	return ""
}
