/*
Copyright The Kubernetes Authors.

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

package parse

import "strings"

// ParseSELinuxLabel 把 SELinux 标签字符串拆分为各组成部分。
// 格式为 "user:role:type:level"，依次对应 [user, role, type, level]。
// 缺失的组成部分以空字符串表示。
func ParseSELinuxLabel(label string) [4]string {
	// 固定长度数组保证即使标签缺字段，调用方也能按下标安全访问。
	var parts [4]string
	if label == "" {
		return parts
	}
	// 最多切四段：level 本身可能含冒号（如 s0:c123,c456），必须保留在原段中。
	split := strings.SplitN(label, ":", 4)
	copy(parts[:], split)
	return parts
}
