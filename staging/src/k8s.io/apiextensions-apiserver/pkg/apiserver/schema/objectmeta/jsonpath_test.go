/*
Copyright 2018 The Kubernetes Authors.

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

package objectmeta

import (
	"bytes"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

type (
	// jsonPathNode 是 JSONPath 中的一段：要么是对象字段名，要么是数组下标。
	jsonPathNode struct {
		index *int
		field string
	}
	// JSONPath 表示 JSON 结构中的一条路径，由字段名和数组下标组成。
	JSONPath []jsonPathNode
)

// String 以 `.field` 和 `[i]` 的形式渲染路径。
func (p JSONPath) String() string {
	var buf bytes.Buffer
	for _, n := range p {
		if n.index == nil {
			buf.WriteString("." + n.field)
		} else {
			buf.WriteString(fmt.Sprintf("[%d]", *n.index))
		}
	}
	return buf.String()
}

// jsonPaths 枚举 j 中的所有路径，包含中间节点和叶子节点。
func jsonPaths(base JSONPath, j map[string]interface{}) []JSONPath {
	res := make([]JSONPath, 0, len(j))
	for k, old := range j {
		kPth := append(append([]jsonPathNode(nil), base...), jsonPathNode{field: k})
		res = append(res, kPth)

		switch old := old.(type) {
		case map[string]interface{}:
			res = append(res, jsonPaths(kPth, old)...)
		case []interface{}:
			res = append(res, jsonIterSlice(kPth, old)...)
		}
	}
	return res
}

// jsonIterSlice 枚举数组 j 中的所有路径。
func jsonIterSlice(base JSONPath, j []interface{}) []JSONPath {
	res := make([]JSONPath, 0, len(j))
	for i, old := range j {
		// 复制下标值：&i 会随循环变化，不可直接取地址。
		index := i
		iPth := append(append([]jsonPathNode(nil), base...), jsonPathNode{index: &index})
		res = append(res, iPth)

		switch old := old.(type) {
		case map[string]interface{}:
			res = append(res, jsonPaths(iPth, old)...)
		case []interface{}:
			res = append(res, jsonIterSlice(iPth, old)...)
		}
	}
	return res
}

// JSONPathValue 返回 j 中路径 pth 处的值，从下标 base 开始解析。
func JSONPathValue(j map[string]interface{}, pth JSONPath, base int) (interface{}, error) {
	if len(pth) == base {
		return nil, fmt.Errorf("empty json path is invalid for object")
	}
	if pth[base].index != nil {
		return nil, fmt.Errorf("index json path is invalid for object")
	}
	field, ok := j[pth[base].field]
	if !ok || len(pth) == base+1 {
		if len(pth) > base+1 {
			return nil, fmt.Errorf("invalid non-terminal json path %q for non-existing field", pth)
		}
		return j[pth[base].field], nil
	}
	switch field := field.(type) {
	case map[string]interface{}:
		return JSONPathValue(field, pth, base+1)
	case []interface{}:
		return jsonPathValueSlice(field, pth, base+1)
	default:
		return nil, fmt.Errorf("invalid non-terminal json path %q for field", pth[:base+1])
	}
}

// jsonPathValueSlice 返回数组 j 中路径 pth 处的值，从下标 base 开始解析。
func jsonPathValueSlice(j []interface{}, pth JSONPath, base int) (interface{}, error) {
	if len(pth) == base {
		return nil, fmt.Errorf("empty json path %q is invalid for object", pth)
	}
	if pth[base].index == nil {
		return nil, fmt.Errorf("field json path %q is invalid for object", pth[:base+1])
	}
	if *pth[base].index >= len(j) {
		return nil, fmt.Errorf("invalid index %q for array of size %d", pth[:base+1], len(j))
	}
	if len(pth) == base+1 {
		return j[*pth[base].index], nil
	}
	switch item := j[*pth[base].index].(type) {
	case map[string]interface{}:
		return JSONPathValue(item, pth, base+1)
	case []interface{}:
		return jsonPathValueSlice(item, pth, base+1)
	default:
		return nil, fmt.Errorf("invalid non-terminal json path %q for index", pth[:base+1])
	}
}

// SetJSONPath 把 value 写入 j 中路径 pth 处，从下标 base 开始解析。
func SetJSONPath(j map[string]interface{}, pth JSONPath, base int, value interface{}) error {
	if len(pth) == base {
		return fmt.Errorf("empty json path is invalid for object")
	}
	if pth[base].index != nil {
		return fmt.Errorf("index json path is invalid for object")
	}
	field, ok := j[pth[base].field]
	if !ok || len(pth) == base+1 {
		if len(pth) > base+1 {
			return fmt.Errorf("invalid non-terminal json path %q for non-existing field", pth)
		}
		j[pth[base].field] = runtime.DeepCopyJSONValue(value)
		return nil
	}
	switch field := field.(type) {
	case map[string]interface{}:
		return SetJSONPath(field, pth, base+1, value)
	case []interface{}:
		return setJSONPathSlice(field, pth, base+1, value)
	default:
		return fmt.Errorf("invalid non-terminal json path %q for field", pth[:base+1])
	}
}

// setJSONPathSlice 把 value 写入数组 j 中路径 pth 处，从下标 base 开始解析。
func setJSONPathSlice(j []interface{}, pth JSONPath, base int, value interface{}) error {
	if len(pth) == base {
		return fmt.Errorf("empty json path %q is invalid for object", pth)
	}
	if pth[base].index == nil {
		return fmt.Errorf("field json path %q is invalid for object", pth[:base+1])
	}
	if *pth[base].index >= len(j) {
		return fmt.Errorf("invalid index %q for array of size %d", pth[:base+1], len(j))
	}
	if len(pth) == base+1 {
		j[*pth[base].index] = runtime.DeepCopyJSONValue(value)
		return nil
	}
	switch item := j[*pth[base].index].(type) {
	case map[string]interface{}:
		return SetJSONPath(item, pth, base+1, value)
	case []interface{}:
		return setJSONPathSlice(item, pth, base+1, value)
	default:
		return fmt.Errorf("invalid non-terminal json path %q for index", pth[:base+1])
	}
}

// DeleteJSONPath 删除 j 中路径 pth 处的键/元素，从下标 base 开始解析。
func DeleteJSONPath(j map[string]interface{}, pth JSONPath, base int) error {
	if len(pth) == base {
		return fmt.Errorf("empty json path is invalid for object")
	}
	if pth[base].index != nil {
		return fmt.Errorf("index json path is invalid for object")
	}
	field, ok := j[pth[base].field]
	if !ok || len(pth) == base+1 {
		if len(pth) > base+1 {
			return fmt.Errorf("invalid non-terminal json path %q for non-existing field", pth)
		}
		delete(j, pth[base].field)
		return nil
	}
	switch field := field.(type) {
	case map[string]interface{}:
		return DeleteJSONPath(field, pth, base+1)
	case []interface{}:
		if len(pth) == base+2 {
			if pth[base+1].index == nil {
				return fmt.Errorf("field json path %q is invalid for object", pth)
			}
			j[pth[base].field] = append(field[:*pth[base+1].index], field[*pth[base+1].index+1:]...)
			return nil
		}
		return deleteJSONPathSlice(field, pth, base+1)
	default:
		return fmt.Errorf("invalid non-terminal json path %q for field", pth[:base+1])
	}
}

// deleteJSONPathSlice 删除数组 j 中路径 pth 处的元素，从下标 base 开始解析。
func deleteJSONPathSlice(j []interface{}, pth JSONPath, base int) error {
	if len(pth) == base {
		return fmt.Errorf("empty json path %q is invalid for object", pth)
	}
	if pth[base].index == nil {
		return fmt.Errorf("field json path %q is invalid for object", pth[:base+1])
	}
	if *pth[base].index >= len(j) {
		return fmt.Errorf("invalid index %q for array of size %d", pth[:base+1], len(j))
	}
	if len(pth) == base+1 {
		return fmt.Errorf("cannot delete item at index %q in-place", pth[:base])
	}
	switch item := j[*pth[base].index].(type) {
	case map[string]interface{}:
		return DeleteJSONPath(item, pth, base+1)
	case []interface{}:
		if len(pth) == base+2 {
			if pth[base+1].index == nil {
				return fmt.Errorf("field json path %q is invalid for object", pth)
			}
			j[*pth[base].index] = append(item[:*pth[base+1].index], item[*pth[base+1].index+1:])
			return nil
		}
		return deleteJSONPathSlice(item, pth, base+1)
	default:
		return fmt.Errorf("invalid non-terminal json path %q for index", pth[:base+1])
	}
}
