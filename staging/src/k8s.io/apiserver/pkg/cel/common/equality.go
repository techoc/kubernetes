/*
Copyright 2023 The Kubernetes Authors.

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

// Package common 提供 CEL 校验器共用的 schema 抽象、无序/映射列表处理
// 以及基于新旧值关联的递归比较工具。
package common

import (
	"reflect"
	"time"
)

// CorrelatedObject represents a node in a tree of objects that are being
// validated. It is used to keep track of the old value of an object during
// traversal of the new value. It is also used to cache the results of
// DeepEqual comparisons between the old and new values of objects.
//
// All receiver functions support being called on `nil` to support ergonomic
// recursive descent. The nil `CorrelatedObject` represents an uncorrelatable
// node in the tree.
//
// CorrelatedObject is not thread-safe. It is the responsibility of the caller
// to handle concurrency, if any.
//
// 存在的意义是"棘轮校验（ratcheting）"：新对象只需在其未变更的部分上满足
// 旧规则。判断某部分是否变更，就要把它与旧对象中"对应的"部分配对——
// 对 map-key 列表不能按下标配对，而应按 map 键匹配，这就是 correlate 逻辑的由来。
type CorrelatedObject struct {
	// Currently correlated old value during traversal of the schema/object
	OldValue interface{}

	// Value being validated
	Value interface{}

	// Schema used for validation of this value. The schema is also used
	// to determine how to correlate the old object.
	Schema Schema

	// Duration 是该对象及其所有子节点做 ratcheting 校验所花的时间。
	// 用指针共享同一个累计器，子节点直接累加到父节点上，避免逐层回传。
	Duration *time.Duration

	// Scratch space below, may change during validation

	// Cached comparison result of DeepEqual of `value` and `thunk.oldValue`
	comparisonResult *bool

	// mapList 缓存 map 类型列表的 map 表示，不是 map 类型列表时为 nil。
	// 惰性构建：只有真正按索引访问 map 类型列表时才需要它。
	mapList MapList

	// Children spawned by a call to `Validate` on this object
	// key is either a string or an index, depending upon whether `value` is
	// a map or a list, respectively.
	//
	// The list of children may be incomplete depending upon if the internal
	// logic of kube-openapi's SchemaValidator short-circuited before
	// reaching all of the children.
	//
	// It should be expected to have an entry for either all of the children, or
	// none of them.
	children map[interface{}]*CorrelatedObject
}

// NewCorrelatedObject 创建关联树的根节点，并将计时累加器初始化为零。
func NewCorrelatedObject(new, old interface{}, schema Schema) *CorrelatedObject {
	d := time.Duration(0)
	return &CorrelatedObject{
		OldValue: old,
		Value:    new,
		Schema:   schema,
		Duration: &d,
	}
}

// If OldValue or Value is not a list, or the index is out of bounds of the
// Value list, returns nil
// If oldValue is a list, this considers the x-list-type to decide how to
// correlate old values:
//
// If listType is map, creates a map representation of the list using the designated
// map-keys, caches it for future calls, and returns the map value, or nil if
// the correlated key is not in the old map
//
// Otherwise, if the list type is not correlatable this funcion returns nil.
func (r *CorrelatedObject) correlateOldValueForChildAtNewIndex(index int) interface{} {
	oldAsList, ok := r.OldValue.([]interface{})
	if !ok {
		return nil
	}

	asList, ok := r.Value.([]interface{})
	if !ok {
		return nil
	} else if len(asList) <= index {
		// Cannot correlate out of bounds index
		return nil
	}

	listType := r.Schema.XListType()
	switch listType {
	case "map":
		// Look up keys for this index in current object
		currentElement := asList[index]

		// 旧列表的 map 表示只构建一次并缓存，因为同一列表内多次按下标访问很常见。
		oldList := r.mapList
		if oldList == nil {
			oldList = MakeMapList(r.Schema, oldAsList)
			r.mapList = oldList
		}
		return oldList.Get(currentElement)

	case "set":
		// Are sets correlatable? Only if the old value equals the current value.
		// We might be able to support this, but do not currently see a lot
		// of value
		// (would allow you to add/remove items from sets with ratcheting but not change them)
		//
		// set 无稳定元素标识，无法可靠配对，故一律视为不可关联，退回整体比较。
		return nil
	case "":
		fallthrough
	case "atomic":
		// Atomic lists are the default are not correlatable by item
		// Ratcheting is not available on a per-index basis
		return nil
	default:
		// Unrecognized list type. Assume non-correlatable.
		return nil
	}
}

// CachedDeepEqual is equivalent to reflect.DeepEqual, but caches the
// results in the tree of ratchetInvocationScratch objects on the way:
//
// For objects and arrays, this function will make a best effort to make
// use of past DeepEqual checks performed by this Node's children, if available.
//
// If a lazy computation could not be found for all children possibly due
// to validation logic short circuiting and skipping the children, then
// this function simply defers to reflect.DeepEqual.
//
// 缓存结果以指针写入 comparisonResult：同一节点可能在多条校验路径上被反复比较，
// 缓存把复杂度从指数级降到线性。
func (r *CorrelatedObject) CachedDeepEqual() (res bool) {
	start := time.Now()
	defer func() {
		if r != nil && r.Duration != nil {
			*r.Duration += time.Since(start)
		}
	}()

	if r == nil {
		// Uncorrelatable node is not considered equal to its old value
		return false
	} else if r.comparisonResult != nil {
		return *r.comparisonResult
	}

	defer func() {
		r.comparisonResult = &res
	}()

	if r.Value == nil && r.OldValue == nil {
		return true
	} else if r.Value == nil || r.OldValue == nil {
		return false
	}

	oldAsArray, oldIsArray := r.OldValue.([]interface{})
	newAsArray, newIsArray := r.Value.([]interface{})

	oldAsMap, oldIsMap := r.OldValue.(map[string]interface{})
	newAsMap, newIsMap := r.Value.(map[string]interface{})

	// If old and new are not the same type, they are not equal
	if (oldIsArray != newIsArray) || oldIsMap != newIsMap {
		return false
	}

	// Objects are known to be same type of (map, slice, or primitive)
	switch {
	case oldIsArray:
		// Both arrays case. oldIsArray == newIsArray
		if len(oldAsArray) != len(newAsArray) {
			return false
		}

		for i := range newAsArray {
			child := r.Index(i)
			if child == nil {
				if r.mapList == nil {
					// 无法关联的列表用 reflect.DeepEqual 当作整体比较，这是保守但正确的选择。
					return reflect.DeepEqual(oldAsArray, newAsArray)
				}

				// 列表可关联却找不到对应旧元素时直接短路比较：等价于新增元素，判不相等。
				return false

			} else if !child.CachedDeepEqual() {
				// If one child is not equal the entire object is not equal
				return false
			}
		}

		return true
	case oldIsMap:
		// Both maps case. oldIsMap == newIsMap
		if len(oldAsMap) != len(newAsMap) {
			return false
		}

		for k := range newAsMap {
			child := r.Key(k)
			if child == nil {
				// Un-correlatable child due to key change.
				// Objects are not equal.
				return false
			} else if !child.CachedDeepEqual() {
				// If one child is not equal the entire object is not equal
				return false
			}
		}

		return true

	default:
		// Primitive: use reflect.DeepEqual
		return reflect.DeepEqual(r.OldValue, r.Value)
	}
}

// Key returns the child of the receiver with the given name.
// Returns nil if the given name is does not exist in the new object, or its
// value is not correlatable to an old value.
// If receiver is nil or if the new value is not an object/map, returns nil.
func (r *CorrelatedObject) Key(field string) *CorrelatedObject {
	start := time.Now()
	defer func() {
		if r != nil && r.Duration != nil {
			*r.Duration += time.Since(start)
		}
	}()

	if r == nil || r.Schema == nil {
		return nil
	} else if existing, exists := r.children[field]; exists {
		return existing
	}

	// Find correlated old value
	oldAsMap, okOld := r.OldValue.(map[string]interface{})
	newAsMap, okNew := r.Value.(map[string]interface{})
	if !okOld || !okNew {
		return nil
	}

	oldValueForField, okOld := oldAsMap[field]
	newValueForField, okNew := newAsMap[field]
	if !okOld || !okNew {
		return nil
	}

	// 字段 schema 可能显式声明，也可能来自 additionalProperties（自由键的 map）。
	var propertySchema Schema
	if prop, exists := r.Schema.Properties()[field]; exists {
		propertySchema = prop
	} else if addP := r.Schema.AdditionalProperties(); addP != nil && addP.Schema() != nil {
		propertySchema = addP.Schema()
	} else {
		return nil
	}

	if r.children == nil {
		r.children = make(map[interface{}]*CorrelatedObject, len(newAsMap))
	}

	res := &CorrelatedObject{
		OldValue: oldValueForField,
		Value:    newValueForField,
		Schema:   propertySchema,
		Duration: r.Duration,
	}
	r.children[field] = res
	return res
}

// Index returns the child of the receiver at the given index.
// Returns nil if the given index is out of bounds, or its value is not
// correlatable to an old value.
// If receiver is nil or if the new value is not an array, returns nil.
func (r *CorrelatedObject) Index(i int) *CorrelatedObject {
	start := time.Now()
	defer func() {
		if r != nil && r.Duration != nil {
			*r.Duration += time.Since(start)
		}
	}()

	if r == nil || r.Schema == nil {
		return nil
	} else if existing, exists := r.children[i]; exists {
		return existing
	}

	asList, ok := r.Value.([]interface{})
	if !ok || len(asList) <= i {
		return nil
	}

	oldValueForIndex := r.correlateOldValueForChildAtNewIndex(i)
	if oldValueForIndex == nil {
		return nil
	}
	var itemSchema Schema
	if i := r.Schema.Items(); i != nil {
		itemSchema = i
	} else {
		// 没有 items schema 就无法继续递归校验，视为不可关联。
		return nil
	}

	if r.children == nil {
		r.children = make(map[interface{}]*CorrelatedObject, len(asList))
	}

	res := &CorrelatedObject{
		OldValue: oldValueForIndex,
		Value:    asList[i],
		Schema:   itemSchema,
		Duration: r.Duration,
	}
	r.children[i] = res
	return res
}
