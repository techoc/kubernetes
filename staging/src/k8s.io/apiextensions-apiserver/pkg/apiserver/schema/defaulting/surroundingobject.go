/*
Copyright 2019 The Kubernetes Authors.

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

package defaulting

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AccessorFunc 在 obj 上按固定的（隐式编码的）JSON 路径取出节点 x；
// 路径存在时 found 为 true，否则为 false。路径上某字段类型不对时返回错误。
type AccessorFunc func(obj map[string]interface{}) (x interface{}, found bool, err error)

// SurroundingObjectFunc 是一个“外层对象”构造器：把 x 放在某个叶子节点上，
// 具体位置由一系列 Index() 和 Child(k) 调用决定。
// 它同时返回逆操作，即从测试对象中取出 x 的 accessor。
//
// 对 obj, acc, _ := someSurroundingObjectFunc(x) 有：
//
//	acc(obj) == x
//	reflect.DeepEqual(acc(DeepCopy(obj), x) == x
//
// 其中对切片和 map 而言 x 是原始实例。
//
// 如果 acc 计算完成后 obj 中持有 x 的节点被修改（例如被 prune），
// accessor 会返回修改后的节点值（例如被 prune 后的 x）。
//
// 示例（忽略最后两个返回值）：
//
//	NewRootObjectFunc()(x) == x
//	NewRootObjectFunc().Index()(x) == [x]
//	NewRootObjectFunc().Index().Child("foo") == [{"foo": x}]
//	NewRootObjectFunc().Index().Child("foo").Child("bar") == [{"foo": {"bar":x}}]
//	NewRootObjectFunc().Index().Child("foo").Child("bar").Index() == [{"foo": {"bar":[x]}}]
//
// 以及：
//
//	NewRootObjectFunc(), then acc(x) == x
//	NewRootObjectFunc().Index(), then acc([x]) == x
//	NewRootObjectFunc().Index().Child("foo"), then acc([{"foo": x}]) == x
//	NewRootObjectFunc().Index().Child("foo").Child("bar"), then acc([{"foo": {"bar":x}}]) == x
//	NewRootObjectFunc().Index().Child("foo").Child("bar").Index(), then acc([{"foo": {"bar":[x]}}]) == x
type SurroundingObjectFunc func(focus interface{}) (map[string]interface{}, AccessorFunc, error)

// NewRootObjectFunc 返回恒等函数。传入的 focus 值必须是对象。
func NewRootObjectFunc() SurroundingObjectFunc {
	return func(x interface{}) (map[string]interface{}, AccessorFunc, error) {
		obj, ok := x.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("object root default value must be of object type")
		}
		return obj, func(root map[string]interface{}) (interface{}, bool, error) {
			return root, true, nil
		}, nil
	}
}

// WithTypeMeta 返回一个闭包：若 TypeMeta 字段未定义则补上。
// 该操作会修改 f(x) 的结果。
func (f SurroundingObjectFunc) WithTypeMeta(meta metav1.TypeMeta) SurroundingObjectFunc {
	return func(x interface{}) (map[string]interface{}, AccessorFunc, error) {
		obj, acc, err := f(x)
		if err != nil {
			return nil, nil, err
		}
		if obj == nil {
			obj = map[string]interface{}{}
		}
		// 只在缺失时填充，避免覆盖调用方已有的值。
		if _, found := obj["kind"]; !found {
			obj["kind"] = meta.Kind
		}
		if _, found := obj["apiVersion"]; !found {
			obj["apiVersion"] = meta.APIVersion
		}
		return obj, acc, err
	}
}

// Child 返回函数 x => f({k: x}) 及其对应的 accessor。
func (f SurroundingObjectFunc) Child(k string) SurroundingObjectFunc {
	return func(x interface{}) (map[string]interface{}, AccessorFunc, error) {
		obj, acc, err := f(map[string]interface{}{k: x})
		if err != nil {
			return nil, nil, err
		}
		return obj, func(obj map[string]interface{}) (interface{}, bool, error) {
			x, found, err := acc(obj)
			if err != nil {
				return nil, false, fmt.Errorf(".%s%v", k, err)
			}
			if !found {
				return nil, false, nil
			}
			if x, ok := x.(map[string]interface{}); !ok {
				return nil, false, fmt.Errorf(".%s must be of object type", k)
			} else if v, found := x[k]; !found {
				return nil, false, nil
			} else {
				return v, true, nil
			}
		}, err
	}
}

// Index 返回函数 x => f([x]) 及其对应的 accessor。
func (f SurroundingObjectFunc) Index() SurroundingObjectFunc {
	return func(focus interface{}) (map[string]interface{}, AccessorFunc, error) {
		obj, acc, err := f([]interface{}{focus})
		if err != nil {
			return nil, nil, err
		}
		return obj, func(obj map[string]interface{}) (interface{}, bool, error) {
			x, found, err := acc(obj)
			if err != nil {
				return nil, false, fmt.Errorf("[]%v", err)
			}
			if !found {
				return nil, false, nil
			}
			if x, ok := x.([]interface{}); !ok {
				return nil, false, fmt.Errorf("[] must be of array type")
			} else if len(x) == 0 {
				return nil, false, nil
			} else {
				return x[0], true, nil
			}
		}, err
	}
}
