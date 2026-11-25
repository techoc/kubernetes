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

// CorrelatedObject 表示正在被校验的对象树中的一个节点。它用于在遍历新值
// 的过程中跟踪对象的旧值，也用于缓存新旧值之间 DeepEqual 比较的结果。
//
// 所有接收者方法都支持在 `nil` 上被调用，以方便符合直觉的递归下降。
// nil 的 `CorrelatedObject` 表示树中一个不可关联的节点。
//
// CorrelatedObject 不是线程安全的。并发（如果有）由调用方负责处理。
//
// 存在的意义是"棘轮校验（ratcheting）"：新对象只需在其未变更的部分上满足
// 旧规则。判断某部分是否变更，就要把它与旧对象中"对应的"部分配对——
// 对 map-key 列表不能按下标配对，而应按 map 键匹配，这就是 correlate 逻辑的由来。
type CorrelatedObject struct {
	// 遍历 schema/对象过程中当前关联到的旧值
	OldValue interface{}

	// 正在被校验的值
	Value interface{}

	// 用于校验该值的 Schema。该 Schema 同时用于决定如何关联旧对象。
	Schema Schema

	// Duration 是该对象及其所有子节点做 ratcheting 校验所花的时间。
	// 用指针共享同一个累计器，子节点直接累加到父节点上，避免逐层回传。
	Duration *time.Duration

	// 以下的临时（scratch）空间，可能在校验过程中发生变化

	// 缓存的 `value` 与 `thunk.oldValue` 之间 DeepEqual 的比较结果
	comparisonResult *bool

	// mapList 缓存 map 类型列表的 map 表示，不是 map 类型列表时为 nil。
	// 惰性构建：只有真正按索引访问 map 类型列表时才需要它。
	mapList MapList

	// 由在本对象上调用 `Validate` 而生成的子节点。
	// key 是字符串或索引，取决于 `value` 是 map 还是列表。
	//
	// 子节点列表可能不完整：kube-openapi 的 SchemaValidator 内部逻辑
	// 可能在遍历到所有子节点之前就短路返回。
	//
	// 预期情况是：要么包含全部子节点的条目，要么一条都没有。
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

// 如果 OldValue 或 Value 不是列表，或索引超出 Value 列表的边界，返回 nil。
// 如果 oldValue 是列表，则由 x-list-type 决定如何关联旧值：
//
// 如果 listType 是 map，用指定的 map-key 为列表构建 map 表示，缓存以备后续
// 调用，并返回对应的 map 值；若关联键不在旧 map 中则返回 nil。
//
// 否则，如果列表类型不可关联，本函数返回 nil。
func (r *CorrelatedObject) correlateOldValueForChildAtNewIndex(index int) interface{} {
	oldAsList, ok := r.OldValue.([]interface{})
	if !ok {
		return nil
	}

	asList, ok := r.Value.([]interface{})
	if !ok {
		return nil
	} else if len(asList) <= index {
		// 越界的索引无法关联
		return nil
	}

	listType := r.Schema.XListType()
	switch listType {
	case "map":
		// 在当前对象中查出该索引位置元素对应的 map-key
		currentElement := asList[index]

		// 旧列表的 map 表示只构建一次并缓存，因为同一列表内多次按下标访问很常见。
		oldList := r.mapList
		if oldList == nil {
			oldList = MakeMapList(r.Schema, oldAsList)
			r.mapList = oldList
		}
		return oldList.Get(currentElement)

	case "set":
		// set 可关联吗？只有当旧值恰好等于当前值时才行。
		// 也许可以做到支持，但目前看不到多少价值
		// （那将允许借助 ratcheting 对 set 增删元素，但不能修改元素本身）
		//
		// set 无稳定元素标识，无法可靠配对，故一律视为不可关联，退回整体比较。
		return nil
	case "":
		fallthrough
	case "atomic":
		// atomic 列表是默认类型，无法按元素逐条关联。
		// ratcheting 不能按索引逐个进行
		return nil
	default:
		// 无法识别的列表类型，视为不可关联。
		return nil
	}
}

// CachedDeepEqual 等价于 reflect.DeepEqual，但会把结果沿途缓存到
// CorrelatedObject 对象树中（原英文注释写作 ratchetInvocationScratch，
// 该类型名在代码中并不存在）：
//
// 对于对象和数组，本函数会尽量利用本 Node 的子节点已经做过的 DeepEqual
// 检查结果（如果可用）。
//
// 如果并非所有子节点都能找到惰性计算的结果——可能由于校验逻辑短路而跳过了
// 这些子节点——那么本函数直接回退到 reflect.DeepEqual。
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
		// 不可关联的节点不视为与其旧值相等
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

	// 如果新旧值不是同一类型，则不相等
	if (oldIsArray != newIsArray) || oldIsMap != newIsMap {
		return false
	}

	// 到这一步，两个对象已知同为 map、slice 或原始值三种类型之一
	switch {
	case oldIsArray:
		// 双方都是数组的情形。oldIsArray == newIsArray
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
				// 只要有一个子节点不相等，整个对象就不相等
				return false
			}
		}

		return true
	case oldIsMap:
		// 双方都是 map 的情形。oldIsMap == newIsMap
		if len(oldAsMap) != len(newAsMap) {
			return false
		}

		for k := range newAsMap {
			child := r.Key(k)
			if child == nil {
				// 键发生变化导致子节点无法关联。
				// 两个对象不相等。
				return false
			} else if !child.CachedDeepEqual() {
				// 只要有一个子节点不相等，整个对象就不相等
				return false
			}
		}

		return true

	default:
		// 原始值：使用 reflect.DeepEqual
		return reflect.DeepEqual(r.OldValue, r.Value)
	}
}

// Key 返回接收者中给定名称对应的子节点。
// 如果给定名称在新对象中不存在，或其值无法与旧值关联，返回 nil。
// 如果接收者为 nil 或新值不是对象/map，返回 nil。
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

	// 查找与之关联的旧值
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

// Index 返回接收者中给定索引处的子节点。
// 如果给定索引越界，或其值无法与旧值关联，返回 nil。
// 如果接收者为 nil 或新值不是数组，返回 nil。
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
