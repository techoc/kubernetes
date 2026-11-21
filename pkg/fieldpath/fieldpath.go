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
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/validation"
)

// FormatMap 把 map[string]string 格式化为字符串。
func FormatMap(m map[string]string) (fmtStr string) {
	// 按 key 排序输出，保证结果稳定可比较。
	keys := make([]string, 0, len(m))
	var grow int
	for k, v := range m {
		keys = append(keys, k)
		// 每个 key/value 额外预留 4 字节，分别对应 =、\n 和一对引号。
		grow += len(k) + len(v) + 4
	}
	sort.Strings(keys)
	// 预分配足够容量，避免 append 过程中反复扩容。
	dst := make([]byte, 0, grow)
	for _, key := range keys {
		if len(dst) > 0 {
			dst = append(dst, '\n')
		}
		dst = append(dst, key...)
		dst = append(dst, '=')
		dst = strconv.AppendQuote(dst, m[key])
	}
	return string(dst)
}

// ExtractFieldPathAsString 从给定对象中提取指定字段并以字符串返回。
// obj 必须是指向 API 类型的指针。
func ExtractFieldPathAsString(obj interface{}, fieldPath string) (string, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return "", err
	}

	if path, subscript, ok := SplitMaybeSubscriptedPath(fieldPath); ok {
		switch path {
		case "metadata.annotations":
			// annotation 的 key 不区分大小写合法性，按小写形式校验。
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

// SplitMaybeSubscriptedPath 判断字段路径是否带有下标：
//   - 带下标时拆成 path 与 subscript 并返回 (path, subscript, true)；
//   - 不带下标时原样返回 (fieldPath, "", false)。
//
// 输入与输出示例：
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
