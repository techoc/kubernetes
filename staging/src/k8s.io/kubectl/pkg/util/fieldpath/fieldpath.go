/*
Copyright 2015 The Kubernetes Authors.

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

package fieldpath

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
)

// TODO(yue9944882): 复制到 k/apimachinery 后删除这个辅助包。

// 把 map[string]string 格式化为字符串。
func FormatMap(m map[string]string) (fmtStr string) {
	// 键按排序输出，保证结果稳定。
	keys := sets.New[string]()
	for key := range m {
		keys.Insert(key)
	}
	for _, key := range sets.List(keys) {
		fmtStr += fmt.Sprintf("%v=%q\n", key, m[key])
	}
	fmtStr = strings.TrimSuffix(fmtStr, "\n")

	return
}

// 从给定对象中提取字段并以字符串返回；
// 对象必须是指向 API 类型的指针。
func ExtractFieldPathAsString(obj interface{}, fieldPath string) (string, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", nil
	}

	if path, subscript, ok := SplitMaybeSubscriptedPath(fieldPath); ok {
		switch path {
		case "metadata.annotations":
			if errs := validation.IsQualifiedName(strings.ToLower(subscript)); len(errs) != 0 {
				return "", fmt.Errorf("invalid key subscript in %s: %s", fieldPath, strings.Join(errs, ";"))
			}
			return accessor.GetAnnotations()[subscript], nil
		case "metadata.labels":
			if errs := validation.IsQualifiedName(subscript); len(errs) != 0 {
				return "", fmt.Errorf("invalid key subscript in %s: %s", fieldPath, strings.Join(errs, ";"))
			}
			return accessor.GetLabels()[subscript], nil
		default:
			return "", fmt.Errorf("fieldPath %q does not support subscript", fieldPath)
		}
	}

	switch fieldPath {
	case "metadata.annotations":
		return FormatMap(accessor.GetAnnotations()), nil
	case "metadata.labels":
		return FormatMap(accessor.GetLabels()), nil
	case "metadata.name":
		return accessor.GetName(), nil
	case "metadata.namespace":
		return accessor.GetNamespace(), nil
	case "metadata.uid":
		return string(accessor.GetUID()), nil
	}

	return "", fmt.Errorf("unsupported fieldPath: %v", fieldPath)
}

// 判断给定 fieldPath 是否带下标；
// 带下标时
// 拆分为路径和下标记，返回 (path, subscript, true)；
// 不带下标时返回 (fieldPath, "", false)。
//
// 输入输出示例：
//
//	"metadata.annotations['myKey']" --> ("metadata.annotations", "myKey", true)
//	"metadata.annotations['a[b]c']" --> ("metadata.annotations", "a[b]c", true)
//	"metadata.labels['']"           --> ("metadata.labels", "", true)
//	"metadata.labels"               --> ("metadata.labels", "", false)
func SplitMaybeSubscriptedPath(fieldPath string) (string, string, bool) {
	if !strings.HasSuffix(fieldPath, "']") {
		return fieldPath, "", false
	}
	s := strings.TrimSuffix(fieldPath, "']")
	parts := strings.SplitN(s, "['", 2)
	if len(parts) < 2 {
		return fieldPath, "", false
	}
	if len(parts[0]) == 0 {
		return fieldPath, "", false
	}
	return parts[0], parts[1], true
}
