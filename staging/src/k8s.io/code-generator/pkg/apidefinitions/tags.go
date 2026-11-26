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

package apidefinitions

import (
	"fmt"

	"k8s.io/gengo/v2/codetags"
)

// tagGroupName 是唯一被所有生成器共享、且不带 "-gen" 后缀的包级标签。
const tagGroupName = "groupName"

// extractGeneratorTag 返回注释中 "+<tagName>" 的取值。
// 若某个值为 "false"，则所有值都必须是 "false"：重复禁用允许，
// 但禁用与其他取值混用是错误；此时 optedOut 置为 true。
func extractGeneratorTag(comments []string, tagName string) (values []string, optedOut bool, err error) {
	if tagName == "" {
		return nil, false, nil
	}
	values, err = tagValues(comments, tagName)
	if err != nil {
		return nil, false, err
	}
	hasFalse, hasOther := false, false
	for _, v := range values {
		if v == "false" {
			hasFalse = true
		} else {
			hasOther = true
		}
	}
	if hasFalse && hasOther {
		return nil, false, fmt.Errorf("+%s: cannot mix \"false\" with other values; got %v", tagName, values)
	}
	if hasFalse {
		return nil, true, nil
	}
	return values, false, nil
}

// tagValues 返回某个标签的全部值；标签缺失时返回 nil 而不是空切片，
// 便于调用方区分“没有标签”和“标签值为空”。
func tagValues(lines []string, name string) ([]string, error) {
	tags, err := extractTags(lines, name)
	if err != nil {
		return nil, err
	}
	if len(tags[name]) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(tags[name]))
	for _, t := range tags[name] {
		out = append(out, t.Value)
	}
	return out, nil
}

// extractTags 先按名字粗筛原始行，再逐条解析；RawValues(true) 允许
// 带引号等原始字面量，因为这里只做值透传，不做语义解释。
func extractTags(lines []string, names ...string) (map[string][]codetags.Tag, error) {
	raw := codetags.Extract("+", lines)
	out := map[string][]codetags.Tag{}
	for _, name := range names {
		for _, line := range raw[name] {
			t, err := codetags.Parse(line, codetags.RawValues(true))
			if err != nil {
				return nil, err
			}
			out[name] = append(out[name], t)
		}
	}
	return out, nil
}

// GroupNameForPackage 返回注释中 +groupName= 标签的值；标签不存在时 ok 为 false。
//
// 只取第一个值：group 名在 client-gen/lister-gen 中用于目录命名，
// 重复声明没有合理语义。
func GroupNameForPackage(comments []string) (group string, ok bool, err error) {
	values, err := tagValues(comments, tagGroupName)
	if err != nil {
		return "", false, err
	}
	if len(values) == 0 {
		return "", false, nil
	}
	return values[0], true, nil
}
