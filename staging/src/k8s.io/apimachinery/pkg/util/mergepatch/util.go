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

package mergepatch

import (
	"fmt"
	"reflect"

	"k8s.io/utils/dump"
	"sigs.k8s.io/yaml"
)

// PreconditionFunc 断言补丁中不存在不兼容的改动。
// 返回 true 表示前置条件成立（补丁可接受），false 表示违反。
type PreconditionFunc func(interface{}) bool

// RequireKeyUnchanged 返回一个前置条件函数：若 patch 中出现指定的顶层 key，
// 则判定失败（说明该 key 的值已被改动）。
func RequireKeyUnchanged(key string) PreconditionFunc {
	return func(patch interface{}) bool {
		patchMap, ok := patch.(map[string]interface{})
		// 补丁不是对象形状时无从判断该 key，视为未改动以保持宽松。
		if !ok {
			return true
		}

		// The presence of key means that its value has been changed, so the test fails.
		// key 存在即意味着其值被改动，因此判定失败。
		_, ok = patchMap[key]
		return !ok
	}
}

// RequireMetadataKeyUnchanged 创建一个前置条件函数：若 patch 中出现
// metadata.key，则判定失败（说明该值已被改动）。
//
// 与 RequireKeyUnchanged 的区别在于它只检查 metadata 子对象，
// 用于保护 name、namespace 这类元数据字段。
func RequireMetadataKeyUnchanged(key string) PreconditionFunc {
	return func(patch interface{}) bool {
		patchMap, ok := patch.(map[string]interface{})
		if !ok {
			return true
		}
		patchMap1, ok := patchMap["metadata"]
		if !ok {
			return true
		}
		patchMap2, ok := patchMap1.(map[string]interface{})
		// metadata 存在但不是对象（例如被显式置为 null）时，无法按字段名判定，
		// 交由后续的完整校验处理。
		if !ok {
			return true
		}
		_, ok = patchMap2[key]
		return !ok
	}
}

// ToYAMLOrError 把任意值序列化为 YAML；失败时把错误文本本身作为字符串返回。
//
// 这是为了方便在错误消息或日志中展示对象内容：即使序列化失败也不能让
// 原本的错误处理路径再次出错，因此把失败信息降级为普通文本。
func ToYAMLOrError(v interface{}) string {
	y, err := toYAML(v)
	if err != nil {
		return err.Error()
	}

	return y
}

// toYAML 序列化为 YAML，失败时附带对象的格式化转储，便于定位不可序列化的值。
func toYAML(v interface{}) (string, error) {
	y, err := yaml.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("yaml marshal failed:%v\n%v\n", err, dump.Pretty(v))
	}

	return string(y), nil
}

// HasConflicts 判断左右两个 JSON 结构体对象是否在任一 key 上存在取值不同的重叠。
// 所有 key 必须是字符串。由于同类型的补丁具有一致的 key 结构，该判定适用于多种补丁类型。
// 本方法支持 JSON merge patch 语义。
//
// 注意：类型不同的数字（如 int(0) 与 int64(0)）会被判定为冲突。
// 请确保左右两侧的反序列化方式一致（例如使用同一个库）。
func HasConflicts(left, right interface{}) (bool, error) {
	switch typedLeft := left.(type) {
	case map[string]interface{}:
		switch typedRight := right.(type) {
		case map[string]interface{}:
			for key, leftValue := range typedLeft {
				rightValue, ok := typedRight[key]
				// 仅一侧存在的 key 不算冲突：merge patch 允许新增或删除字段。
				if !ok {
					continue
				}
				if conflict, err := HasConflicts(leftValue, rightValue); err != nil || conflict {
					return conflict, err
				}
			}

			return false, nil
		default:
			// 一侧是对象、另一侧不是，形状不兼容即为冲突。
			return true, nil
		}
	case []interface{}:
		switch typedRight := right.(type) {
		case []interface{}:
			// 数组按位置逐个比较，长度不同直接视为冲突（merge patch 语义下数组整体替换）。
			if len(typedLeft) != len(typedRight) {
				return true, nil
			}

			for i := range typedLeft {
				if conflict, err := HasConflicts(typedLeft[i], typedRight[i]); err != nil || conflict {
					return conflict, err
				}
			}

			return false, nil
		default:
			return true, nil
		}
	case string, float64, bool, int64, nil:
		// 标量直接比较；reflect.DeepEqual 能区分 nil 与零值。
		return !reflect.DeepEqual(left, right), nil
	default:
		// 出现未知类型说明输入不是标准 JSON 解码结果，无法可靠比较，报错而非猜测。
		return true, fmt.Errorf("unknown type: %v", reflect.TypeOf(left))
	}
}
