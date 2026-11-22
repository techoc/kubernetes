/*
Copyright 2017 The Kubernetes Authors.

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

package jsonmergepatch

import (
	"fmt"
	"reflect"

	"gopkg.in/evanphx/json-patch.v4"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/mergepatch"
)

// Create a 3-way merge patch based-on JSON merge patch.
// Calculate addition-and-change patch between current and modified.
// Calculate deletion patch between original and modified.
//
// 中文补充：三方补丁把“新增/修改”和“删除”分开计算，是为了让客户端只提交自己的意图，
// 从而在并发更新时不会误删其他写入者新增的字段。任一方存在冲突或前置条件不满足则返回错误。
func CreateThreeWayJSONMergePatch(original, modified, current []byte, fns ...mergepatch.PreconditionFunc) ([]byte, error) {
	// 空输入按空对象处理，避免 jsonpatch 把 nil 视为非法 JSON。
	if len(original) == 0 {
		original = []byte(`{}`)
	}
	if len(modified) == 0 {
		modified = []byte(`{}`)
	}
	if len(current) == 0 {
		current = []byte(`{}`)
	}

	addAndChangePatch, err := jsonpatch.CreateMergePatch(current, modified)
	if err != nil {
		return nil, err
	}
	// Only keep addition and changes
	// keepNull=false：剔除值为 null 的项，只保留新增与修改。
	addAndChangePatch, addAndChangePatchObj, err := keepOrDeleteNullInJsonPatch(addAndChangePatch, false)
	if err != nil {
		return nil, err
	}

	deletePatch, err := jsonpatch.CreateMergePatch(original, modified)
	if err != nil {
		return nil, err
	}
	// Only keep deletion
	// keepNull=true：只保留 null 项，即相对于 original 被删除的字段。
	deletePatch, deletePatchObj, err := keepOrDeleteNullInJsonPatch(deletePatch, true)
	if err != nil {
		return nil, err
	}

	// 若同一字段既被删除又被新增/修改，则无法安全合并，直接报冲突。
	hasConflicts, err := mergepatch.HasConflicts(addAndChangePatchObj, deletePatchObj)
	if err != nil {
		return nil, err
	}
	if hasConflicts {
		return nil, mergepatch.NewErrConflict(mergepatch.ToYAMLOrError(addAndChangePatchObj), mergepatch.ToYAMLOrError(deletePatchObj))
	}
	patch, err := jsonpatch.MergePatch(deletePatch, addAndChangePatch)
	if err != nil {
		return nil, err
	}

	var patchMap map[string]interface{}
	err = json.Unmarshal(patch, &patchMap)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal patch for precondition check: %s", patch)
	}
	meetPreconditions, err := meetPreconditions(patchMap, fns...)
	if err != nil {
		return nil, err
	}
	if !meetPreconditions {
		return nil, mergepatch.NewErrPreconditionFailed(patchMap)
	}

	return patch, nil
}

// keepOrDeleteNullInJsonPatch takes a json-encoded byte array and a boolean.
// It returns a filtered object and its corresponding json-encoded byte array.
// It is a wrapper of func keepOrDeleteNullInObj
// 中文补充：同时返回字节与 map 两种形式，调用方按需取用，避免重复反序列化。
func keepOrDeleteNullInJsonPatch(patch []byte, keepNull bool) ([]byte, map[string]interface{}, error) {
	var patchMap map[string]interface{}
	err := json.Unmarshal(patch, &patchMap)
	if err != nil {
		return nil, nil, err
	}
	filteredMap, err := keepOrDeleteNullInObj(patchMap, keepNull)
	if err != nil {
		return nil, nil, err
	}
	o, err := json.Marshal(filteredMap)
	return o, filteredMap, err
}

// keepOrDeleteNullInObj will keep only the null value and delete all the others,
// if keepNull is true. Otherwise, it will delete all the null value and keep the others.
// 中文补充：递归处理嵌套对象，保证 null 过滤在各层级一致生效。
func keepOrDeleteNullInObj(m map[string]interface{}, keepNull bool) (map[string]interface{}, error) {
	filteredMap := make(map[string]interface{})
	var err error
	for key, val := range m {
		switch {
		case keepNull && val == nil:
			// 保留删除标记。
			filteredMap[key] = nil
		case val != nil:
			switch typedVal := val.(type) {
			case map[string]interface{}:
				// Explicitly-set empty maps are treated as values instead of empty patches
				// 空 map 表示“显式设置为空”，不能被当作无操作而丢弃。
				if len(typedVal) == 0 {
					if !keepNull {
						filteredMap[key] = typedVal
					}
					continue
				}

				var filteredSubMap map[string]interface{}
				filteredSubMap, err = keepOrDeleteNullInObj(typedVal, keepNull)
				if err != nil {
					return nil, err
				}

				// If the returned filtered submap was empty, this is an empty patch for the entire subdict, so the key
				// should not be set
				if len(filteredSubMap) != 0 {
					filteredMap[key] = filteredSubMap
				}

			case []interface{}, string, float64, bool, int64, nil:
				// Lists are always replaced in Json, no need to check each entry in the list.
				if !keepNull {
					filteredMap[key] = val
				}
			default:
				return nil, fmt.Errorf("unknown type: %v", reflect.TypeOf(typedVal))
			}
		}
	}
	return filteredMap, nil
}

// meetPreconditions 依次执行所有前置条件函数；任一返回 false 即视为补丁不适用。
// 中文补充：返回 (false, nil) 与返回 error 的区别在于：前者是可预期的拒绝，
// 由调用方转换为 PreconditionFailed 错误。
func meetPreconditions(patchObj map[string]interface{}, fns ...mergepatch.PreconditionFunc) (bool, error) {
	// Apply the preconditions to the patch, and return an error if any of them fail.
	for _, fn := range fns {
		if !fn(patchObj) {
			return false, fmt.Errorf("precondition failed for: %v", patchObj)
		}
	}
	return true, nil
}
