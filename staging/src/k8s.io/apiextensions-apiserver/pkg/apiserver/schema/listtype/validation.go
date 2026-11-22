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

// Package listtype 校验 x-kubernetes-list-type 为 map/set 的数组是否满足唯一性约束。
package listtype

import (
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
)

// ValidateListSetsAndMaps 校验 x-kubernetes-list-type 为 "map" 与 "set" 的数组
// 是否满足唯一性不变量：map 按 key 唯一，set 按整体元素唯一。
func ValidateListSetsAndMaps(fldPath *field.Path, s *schema.Structural, obj map[string]interface{}) field.ErrorList {
	if s == nil || obj == nil {
		return nil
	}

	var errs field.ErrorList

	if s.AdditionalProperties != nil && s.AdditionalProperties.Structural != nil {
		for k, v := range obj {
			errs = append(errs, validationListSetAndMaps(fldPath.Key(k), s.AdditionalProperties.Structural, v)...)
		}
	}
	if s.Properties != nil {
		for k, v := range obj {
			if sub, ok := s.Properties[k]; ok {
				errs = append(errs, validationListSetAndMaps(fldPath.Child(k), &sub, v)...)
			}
		}
	}

	return errs
}

// validationListSetAndMaps 按值的实际类型分派：数组走数组校验，对象则递归。
func validationListSetAndMaps(fldPath *field.Path, s *schema.Structural, obj interface{}) field.ErrorList {
	switch obj := obj.(type) {
	case []interface{}:
		return validateListSetsAndMapsArray(fldPath, s, obj)
	case map[string]interface{}:
		return ValidateListSetsAndMaps(fldPath, s, obj)
	}
	return nil
}

// validateListSetsAndMapsArray 校验数组自身的 list-type 约束，并递归校验各元素。
func validateListSetsAndMapsArray(fldPath *field.Path, s *schema.Structural, obj []interface{}) field.ErrorList {
	var errs field.ErrorList

	if s.XListType != nil {
		switch *s.XListType {
		case "set":
			nonUnique, err := validateListSet(fldPath, obj)
			if err != nil {
				errs = append(errs, err)
			} else {
				for _, i := range nonUnique {
					errs = append(errs, field.Duplicate(fldPath.Index(i), obj[i]))
				}
			}
		case "map":
			errs = append(errs, validateListMap(fldPath, s, obj)...)
		}
		// 若将来在此新增分支，需同步修改 pkg/apiserver/schema/cel/values.go
	}

	if s.Items != nil {
		for i := range obj {
			errs = append(errs, validationListSetAndMaps(fldPath.Index(i), s.Items, obj[i])...)
		}
	}

	return errs
}

// validateListSet 校验非结构化对象（标量与复合类型）的唯一性，返回所有重复项的下标。
//
// 特殊处理：为区分“键未定义”与“值为 null”，允许 unspecifiedKeyValue 与 nullObjectValue，
// 两者都会被 Golang 当作标量做正确的比较。
func validateListSet(fldPath *field.Path, obj []interface{}) ([]int, *field.Error) {
	if len(obj) <= 1 {
		return nil, nil
	}

	seenScalars := make(map[interface{}]int, len(obj))
	seenCompounds := make(map[string]int, len(obj))
	var nonUniqueIndices []int
	for i, x := range obj {
		switch x.(type) {
		case map[string]interface{}, []interface{}:
			// 复合类型按 JSON 序列化结果判等，避免依赖 map 不可比较的语义。
			bs, err := json.Marshal(x)
			if err != nil {
				return nil, field.Invalid(fldPath.Index(i), x, "internal error")
			}
			s := string(bs)
			if times, seen := seenCompounds[s]; !seen {
				seenCompounds[s] = 1
			} else {
				seenCompounds[s]++
				if times == 1 {
					nonUniqueIndices = append(nonUniqueIndices, i)
				}
			}
		default:
			// 标量可直接作为 map 键判等。
			if times, seen := seenScalars[x]; !seen {
				seenScalars[x] = 1
			} else {
				seenScalars[x]++
				if times == 1 {
					nonUniqueIndices = append(nonUniqueIndices, i)
				}
			}
		}
	}

	return nonUniqueIndices, nil
}

// validateListMap 校验 list-type 为 map 的数组：所有键字段组合必须唯一。
func validateListMap(fldPath *field.Path, s *schema.Structural, obj []interface{}) field.ErrorList {
	// 仅允许 nil 或对象
	for i, x := range obj {
		if _, ok := x.(map[string]interface{}); x != nil && !ok {
			return field.ErrorList{field.Invalid(fldPath.Index(i), x, "must be an object for an array of list-type map")}
		}
	}

	if len(obj) <= 1 {
		return nil
	}

	// 单 key 是常见情形，单独优化。
	if len(s.XListMapKeys) == 1 {
		type unspecifiedKeyValue struct{}

		keyField := s.XListMapKeys[0]
		keys := make([]interface{}, 0, len(obj))
		for _, x := range obj {
			if x == nil {
				keys = append(keys, unspecifiedKeyValue{}) // nil 对象意味着 key 未指定
				continue
			}

			x := x.(map[string]interface{})

			// key 未定义？
			key, ok := x[keyField]
			if !ok {
				keys = append(keys, unspecifiedKeyValue{})
				continue
			}

			keys = append(keys, key)
		}

		nonUnique, err := validateListSet(fldPath, keys)
		if err != nil {
			return field.ErrorList{err}
		}

		var errs field.ErrorList
		for _, i := range nonUnique {
			switch keys[i] {
			case unspecifiedKeyValue{}:
				errs = append(errs, field.Duplicate(fldPath.Index(i), map[string]interface{}{}))
			default:
				errs = append(errs, field.Duplicate(fldPath.Index(i), map[string]interface{}{keyField: keys[i]}))
			}
		}

		return errs
	}

	// 多 key 字段：把每个元素的 key 组合收集为一个对象再判重。
	keys := make([]interface{}, 0, len(obj))
	for _, x := range obj {
		key := map[string]interface{}{}
		if x == nil {
			keys = append(keys, key)
			continue
		}

		x := x.(map[string]interface{})

		for _, keyField := range s.XListMapKeys {
			if k, ok := x[keyField]; ok {
				key[keyField] = k
			}
		}

		keys = append(keys, key)
	}

	nonUnique, err := validateListSet(fldPath, keys)
	if err != nil {
		return field.ErrorList{err}
	}

	var errs field.ErrorList
	for _, i := range nonUnique {
		errs = append(errs, field.Duplicate(fldPath.Index(i), keys[i]))
	}

	return errs
}
